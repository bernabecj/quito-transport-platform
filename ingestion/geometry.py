"""
Route geometry from OSM way members.

An OSM route relation lists the ways a bus drives along. Each way is a short
stretch of road with its own node list, drawn in whatever direction the mapper
happened to draw it, so a relation's ways do not form a polyline on their own:
some must be reversed, and some relations have ways listed out of order, or
have holes where a stretch of road was never added. This module turns that
member list plus the ways' geometry (as returned by Overpass `out geom`) into
the best single polyline it can, and says what it could not attach.

Why node ids, not coordinates
  Two consecutive ways in a well-formed relation share an end node, so joining
  on node id is exact. Coordinate proximity is only a fallback for pieces that
  do not share a node — a small gap between two ways the mapper never
  connected — and it is bounded by WAY_JOIN_TOLERANCE_M so that a piece
  kilometres away is never stitched on. A phantom segment across a long gap
  would be worse than the gap: stops near it would project onto a road that
  does not exist.

No third-party geometry dependency: the Lambda image stays small, and the
maths here is a few dozen lines.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

EARTH_RADIUS_KM = 6371.0

# Pieces of a route whose ends lie within this distance are joined even when
# they share no node. Well short of BRT stop spacing (500 m+), so a bridge this
# long cannot reorder stops; long enough to close the few-metre gaps that
# appear where mappers left two ways unconnected.
WAY_JOIN_TOLERANCE_M = 100.0

# Way members with these role prefixes are platforms — the kerb, not the road.
# Everything else (empty role in PTv2, "forward"/"backward" in older mapping)
# is treated as road.
PLATFORM_ROLE_PREFIX = "platform"


def haversine_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    """Great-circle distance between two (lon, lat) points."""
    (lon1, lat1), (lon2, lat2) = a, b
    rad = math.pi / 180
    inner = (
        math.sin((lat2 - lat1) * rad / 2) ** 2
        + math.cos(lat1 * rad) * math.cos(lat2 * rad) * math.sin((lon2 - lon1) * rad / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(inner))


def polyline_length_km(points: list[tuple[float, float]]) -> float:
    return sum(haversine_km(points[i], points[i + 1]) for i in range(len(points) - 1))


def mean_hop_km(points: list[tuple[float, float]]) -> float:
    """Average distance between consecutive points — the scrambling detector.

    A route whose stops are in geographic order has a smooth ~0.6 km hop; one
    whose members are out of order jumps kilometres between neighbours. Mean
    hop, unlike end-to-end distance, does not flag a genuine loop route. The
    same measure gates corridors in traffic_loader (CORRIDOR_MAX_MEAN_HOP_KM).
    """
    return polyline_length_km(points) / (len(points) - 1)


def project_onto_polyline(
    line: list[tuple[float, float]], points: list[tuple[float, float]]
) -> list[tuple[float, float]]:
    """Snap each point to its nearest position on `line`.

    Returns, per point, (distance along the line in metres, distance from the
    point to the line in metres). Points beyond either end snap to that end,
    so their along-distance is 0 or the line's length — and their snap
    distance says how far past it they sit.

    Works in a local equirectangular frame scaled at the line's mean latitude.
    Quito straddles the equator, so over a 20 km corridor the error against a
    true geodesic projection is well under a metre.
    """
    lat0 = sum(lat for _, lat in line) / len(line)
    scale_x = EARTH_RADIUS_KM * 1000 * math.cos(math.radians(lat0)) * math.pi / 180
    scale_y = EARTH_RADIUS_KM * 1000 * math.pi / 180

    def to_xy(p):
        return (p[0] * scale_x, p[1] * scale_y)

    vertices = [to_xy(p) for p in line]
    cumulative = [0.0]
    for i in range(1, len(vertices)):
        cumulative.append(cumulative[-1] + math.dist(vertices[i - 1], vertices[i]))

    results = []
    for point in points:
        px, py = to_xy(point)
        best_snap, best_along = math.inf, 0.0
        for i in range(len(vertices) - 1):
            (ax, ay), (bx, by) = vertices[i], vertices[i + 1]
            dx, dy = bx - ax, by - ay
            seg_len_sq = dx * dx + dy * dy
            t = 0.0 if seg_len_sq == 0 else ((px - ax) * dx + (py - ay) * dy) / seg_len_sq
            t = max(0.0, min(1.0, t))
            snap = math.dist((px, py), (ax + t * dx, ay + t * dy))
            if snap < best_snap:
                best_snap, best_along = snap, cumulative[i] + t * math.sqrt(seg_len_sq)
        results.append((best_along, best_snap))
    return results


@dataclass
class RouteGeometry:
    """The road shape of one route relation, as far as it could be chained.

    spine
        The longest connected run of ways, as (lon, lat) points, oriented in
        the relation's direction of travel (see `_orient`).
    spine_node_ids
        The OSM node id at each spine vertex, parallel to `spine`.
    unattached
        Other connected runs that could not be joined to the spine — holes in
        the mapped road, or stray fragments. Kept so a caller can report them;
        they are not part of the polyline.
    road_ways / missing_ways
        How many way members were treated as road, and how many of those had
        no geometry in the payload (a relation outside the geometry block).
    """

    route_id: str
    spine: list[tuple[float, float]]
    spine_node_ids: list[int]
    unattached: list[list[tuple[float, float]]] = field(default_factory=list)
    road_ways: int = 0
    missing_ways: int = 0

    @property
    def spine_length_km(self) -> float:
        return polyline_length_km(self.spine)

    @property
    def unattached_length_km(self) -> float:
        return sum(polyline_length_km(piece) for piece in self.unattached)


# A piece under construction: parallel lists of node ids and (lon, lat).
_Piece = tuple[list[int], list[tuple[float, float]]]


def chain_route_ways(relation: dict, ways: dict[int, dict]) -> RouteGeometry | None:
    """Chain a relation's road ways into a polyline.

    `ways` maps way id to the Overpass `way` element (with `nodes` and
    `geometry`). Returns None when the relation has no road way with geometry,
    which is the normal case for relations the query did not fetch geometry
    for.

    Ways are walked in member order. Each is attached to the piece being built
    wherever one of its end nodes matches an end of the piece, reversed if
    needed; a way touching nothing starts a new piece. Pieces are then merged
    with each other — first on shared node ids, which rescues ways listed out
    of order, then on end proximity within WAY_JOIN_TOLERANCE_M. The longest
    result is the spine.
    """
    road_ways = 0
    missing = 0
    member_ways: list[_Piece] = []
    for member in relation.get("members", []):
        if member["type"] != "way" or member.get("role", "").startswith(PLATFORM_ROLE_PREFIX):
            continue
        road_ways += 1
        way = ways.get(member["ref"])
        if way is None or not way.get("geometry") or len(way.get("nodes", [])) < 2:
            missing += 1
            continue
        member_ways.append(
            (list(way["nodes"]), [(p["lon"], p["lat"]) for p in way["geometry"]])
        )

    if not member_ways:
        return None

    pieces = _chain_in_order(member_ways)
    pieces = _merge_on_shared_nodes(pieces)
    pieces = _merge_on_proximity(pieces, WAY_JOIN_TOLERANCE_M / 1000)

    pieces.sort(key=lambda piece: polyline_length_km(piece[1]), reverse=True)
    spine_ids, spine = pieces[0]
    spine_ids, spine = _orient(spine_ids, spine, member_ways)

    return RouteGeometry(
        route_id=str(relation["id"]),
        spine=spine,
        spine_node_ids=spine_ids,
        unattached=[coords for _, coords in pieces[1:]],
        road_ways=road_ways,
        missing_ways=missing,
    )


def _attach(piece: _Piece, way: _Piece) -> _Piece | None:
    """Join `way` onto either end of `piece` on a shared end node, or None.

    The shared node is kept once. Attaching at the piece's start is allowed:
    when the first way of a relation was drawn against the direction of
    travel, the second way touches its start, and the chain simply grows
    backwards from there. `_orient` flips it at the end.
    """
    p_ids, p_xy = piece
    w_ids, w_xy = way
    if w_ids[0] == p_ids[-1]:
        return p_ids + w_ids[1:], p_xy + w_xy[1:]
    if w_ids[-1] == p_ids[-1]:
        return p_ids + w_ids[-2::-1], p_xy + w_xy[-2::-1]
    if w_ids[-1] == p_ids[0]:
        return w_ids[:-1] + p_ids, w_xy[:-1] + p_xy
    if w_ids[0] == p_ids[0]:
        return w_ids[:0:-1] + p_ids, w_xy[:0:-1] + p_xy
    return None


def _chain_in_order(member_ways: list[_Piece]) -> list[_Piece]:
    pieces: list[_Piece] = []
    current: _Piece | None = None
    for way in member_ways:
        if current is None:
            current = (list(way[0]), list(way[1]))
            continue
        joined = _attach(current, way)
        if joined is None:
            pieces.append(current)
            current = (list(way[0]), list(way[1]))
        else:
            current = joined
    if current is not None:
        pieces.append(current)
    return pieces


def _merge_on_shared_nodes(pieces: list[_Piece]) -> list[_Piece]:
    """Repeatedly join any two pieces that share an end node."""
    merged = True
    while merged and len(pieces) > 1:
        merged = False
        for i in range(len(pieces)):
            for j in range(i + 1, len(pieces)):
                joined = _attach(pieces[i], pieces[j])
                if joined is not None:
                    pieces = [p for k, p in enumerate(pieces) if k not in (i, j)] + [joined]
                    merged = True
                    break
            if merged:
                break
    return pieces


def _merge_on_proximity(pieces: list[_Piece], tolerance_km: float) -> list[_Piece]:
    """Join pieces whose ends nearly touch, closest pair first.

    The bridge between the two ends becomes a segment of the polyline; it is at
    most `tolerance_km` long. No node is shared, so the joined vertex keeps
    both node ids.
    """
    while len(pieces) > 1:
        best = None
        for i in range(len(pieces)):
            for j in range(i + 1, len(pieces)):
                a_ids, a_xy = pieces[i]
                b_ids, b_xy = pieces[j]
                # (distance, how to assemble): a then b, possibly reversing either.
                options = [
                    (haversine_km(a_xy[-1], b_xy[0]), (a_ids + b_ids, a_xy + b_xy)),
                    (haversine_km(a_xy[-1], b_xy[-1]), (a_ids + b_ids[::-1], a_xy + b_xy[::-1])),
                    (haversine_km(a_xy[0], b_xy[-1]), (b_ids + a_ids, b_xy + a_xy)),
                    (haversine_km(a_xy[0], b_xy[0]), (b_ids[::-1] + a_ids, b_xy[::-1] + a_xy)),
                ]
                dist, joined = min(options, key=lambda o: o[0])
                if dist <= tolerance_km and (best is None or dist < best[0]):
                    best = (dist, i, j, joined)
        if best is None:
            return pieces
        _, i, j, joined = best
        pieces = [p for k, p in enumerate(pieces) if k not in (i, j)] + [joined]
    return pieces


def _orient(
    spine_ids: list[int], spine: list[tuple[float, float]], member_ways: list[_Piece]
) -> _Piece:
    """Point the spine in the relation's direction of travel.

    PTv2 lists ways in driving order, so the earliest way member on the spine
    should come before the latest one. If it comes after, the whole chain was
    built backwards (ways attached by prepending) and is flipped. Only the
    member order decides this; stop order is not consulted, because a
    relation with scrambled stops is exactly the case this geometry exists to
    repair.
    """
    position = {node_id: i for i, node_id in enumerate(spine_ids)}
    on_spine = [
        min(position[n] for n in ids if n in position)
        for ids, _ in member_ways
        if any(n in position for n in ids)
    ]
    if len(on_spine) >= 2 and on_spine[0] > on_spine[-1]:
        return spine_ids[::-1], spine[::-1]
    return spine_ids, spine
