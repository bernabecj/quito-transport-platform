"""Tests for ingestion/geometry.py — chaining OSM way members into a polyline.

The payloads are built by hand in the shape Overpass returns for
`relation ... out body` plus `way ... out geom`: relations with an ordered
member list, ways with `nodes` and a parallel `geometry` list. Nodes sit on a
north-south line through Quito, 0.005 degrees (~556 m) apart, so the expected
chain is simply the node ids in numeric order.
"""

import pytest

from ingestion.geometry import (
    WAY_JOIN_TOLERANCE_M,
    chain_route_ways,
    haversine_km,
    polyline_length_km,
)
from ingestion.network_loader import build_route_geometries

LON = -78.50
STEP = 0.005  # degrees of latitude between consecutive nodes


def node_xy(node_id: int) -> tuple[float, float]:
    return (LON, -0.20 - STEP * node_id)


def way(way_id: int, node_ids: list[int], coords=None) -> dict:
    coords = coords or [node_xy(n) for n in node_ids]
    return {
        "type": "way",
        "id": way_id,
        "nodes": list(node_ids),
        "geometry": [{"lon": lon, "lat": lat} for lon, lat in coords],
    }


def relation(rel_id: int, members: list[tuple], name: str = "Ecovia test") -> dict:
    return {
        "type": "relation",
        "id": rel_id,
        "tags": {"name": name, "route": "bus"},
        "members": [{"type": t, "ref": ref, "role": role} for t, ref, role in members],
    }


def road(way_id: int) -> tuple:
    return ("way", way_id, "")


def chain(ways: list[dict], members=None):
    members = members or [road(w["id"]) for w in ways]
    return chain_route_ways(relation(1, members), {w["id"]: w for w in ways})


# ── joining ways ─────────────────────────────────────────────────────────────


def test_ways_in_driving_order_chain_into_one_polyline():
    geometry = chain([way(10, [1, 2, 3]), way(11, [3, 4, 5]), way(12, [5, 6])])

    assert geometry.spine_node_ids == [1, 2, 3, 4, 5, 6]
    assert geometry.spine == [node_xy(n) for n in range(1, 7)]
    assert geometry.unattached == []
    assert geometry.road_ways == 3
    assert geometry.missing_ways == 0
    assert geometry.spine_length_km == pytest.approx(5 * STEP * 111.19, rel=1e-3)


def test_way_drawn_against_travel_direction_is_reversed():
    # Way 11 was drawn 5 -> 3 but is driven 3 -> 5.
    geometry = chain([way(10, [1, 2, 3]), way(11, [5, 4, 3]), way(12, [5, 6])])
    assert geometry.spine_node_ids == [1, 2, 3, 4, 5, 6]


def test_first_way_drawn_backwards_takes_direction_from_the_second():
    geometry = chain([way(10, [3, 2, 1]), way(11, [3, 4, 5]), way(12, [5, 6])])
    assert geometry.spine_node_ids == [1, 2, 3, 4, 5, 6]


def test_shared_node_is_kept_once():
    geometry = chain([way(10, [1, 2]), way(11, [2, 3])])
    assert geometry.spine_node_ids == [1, 2, 3]
    assert len(geometry.spine) == 3


def test_ways_listed_out_of_order_are_still_joined_on_node_ids():
    geometry = chain([way(10, [1, 2, 3]), way(12, [5, 6, 7]), way(11, [3, 4, 5])])
    assert geometry.spine_node_ids == [1, 2, 3, 4, 5, 6, 7]
    assert geometry.unattached == []


def test_direction_follows_member_order_not_drawing_order():
    # Every way is drawn south-to-north (ascending ids) but the relation lists
    # them from the northern end: the bus travels 6 -> 1.
    geometry = chain([way(12, [5, 6]), way(11, [3, 4, 5]), way(10, [1, 2, 3])])
    assert geometry.spine_node_ids == [6, 5, 4, 3, 2, 1]


def test_closed_way_does_not_break_the_chain():
    # A roundabout: first node == last node. The chain goes around it and
    # continues from the same node.
    ring = [3, 30, 31, 3]
    ring_xy = [node_xy(3), (LON + 0.0005, node_xy(3)[1]), (LON + 0.0005, node_xy(3)[1] - 0.0005), node_xy(3)]
    geometry = chain([way(10, [1, 2, 3]), way(11, ring, ring_xy), way(12, [3, 4, 5])])
    assert geometry.spine_node_ids == [1, 2, 3, 30, 31, 3, 4, 5]


# ── what is not road ─────────────────────────────────────────────────────────


def test_platform_ways_and_node_members_are_not_part_of_the_road():
    ways = [way(10, [1, 2, 3]), way(11, [3, 4, 5]), way(90, [50, 51])]
    members = [
        ("node", 100, "stop"),
        road(10),
        ("way", 90, "platform"),
        ("node", 101, "platform"),
        road(11),
    ]
    geometry = chain(ways, members)

    assert geometry.spine_node_ids == [1, 2, 3, 4, 5]
    assert geometry.road_ways == 2
    assert geometry.unattached == []


def test_legacy_forward_backward_roles_count_as_road():
    members = [("way", 10, "forward"), ("way", 11, "backward")]
    geometry = chain([way(10, [1, 2, 3]), way(11, [3, 4, 5])], members)
    assert geometry.spine_node_ids == [1, 2, 3, 4, 5]


def test_ways_without_geometry_are_counted_as_missing():
    members = [road(10), road(11), road(12)]
    geometry = chain([way(10, [1, 2, 3]), way(12, [5, 6])], members)

    assert geometry.road_ways == 3
    assert geometry.missing_ways == 1
    # Without way 11 the two remaining ways do not touch: two pieces, no bridge.
    assert geometry.spine_node_ids == [1, 2, 3]
    assert geometry.unattached == [[node_xy(5), node_xy(6)]]


def test_relation_without_any_road_geometry_returns_none():
    assert chain([], [("node", 100, "stop")]) is None
    assert chain([], [road(10), road(11)]) is None  # ways not fetched: feeder route


# ── gaps ─────────────────────────────────────────────────────────────────────


def test_distant_fragment_is_reported_not_bridged():
    # Nodes 40-43 are ~15 km away from the main run.
    far = [(LON + 0.1, -0.35 - STEP * i) for i in range(4)]
    geometry = chain([way(10, [1, 2, 3]), way(11, [3, 4, 5]), way(20, [40, 41, 42, 43], far)])

    assert geometry.spine_node_ids == [1, 2, 3, 4, 5]
    assert geometry.unattached == [far]
    assert geometry.unattached_length_km == pytest.approx(polyline_length_km(far))
    # The spine is not inflated by a phantom segment to the fragment.
    assert geometry.spine_length_km == pytest.approx(polyline_length_km(geometry.spine))
    assert geometry.spine_length_km < 3


def test_longest_piece_is_the_spine_regardless_of_member_position():
    far = [(LON + 0.1, -0.35 - STEP * i) for i in range(6)]
    geometry = chain([way(10, [1, 2]), way(20, list(range(40, 46)), far)])
    assert geometry.spine == far
    assert geometry.unattached == [[node_xy(1), node_xy(2)]]


def test_small_gap_without_shared_node_is_closed():
    # Way 11 starts 30 m south of where way 10 ends, and they share no node.
    gap_start = (LON, node_xy(3)[1] - 0.00027)
    ways = [way(10, [1, 2, 3]), way(11, [4, 5], [gap_start, node_xy(5)])]
    geometry = chain(ways)

    assert geometry.unattached == []
    assert geometry.spine_node_ids == [1, 2, 3, 4, 5]
    assert haversine_km(node_xy(3), gap_start) * 1000 < WAY_JOIN_TOLERANCE_M


def test_gap_wider_than_tolerance_stays_open():
    gap_start = (LON, node_xy(3)[1] - 0.003)  # ~330 m
    ways = [way(10, [1, 2, 3]), way(11, [4, 5], [gap_start, node_xy(5)])]
    geometry = chain(ways)

    assert haversine_km(node_xy(3), gap_start) * 1000 > WAY_JOIN_TOLERANCE_M
    assert geometry.spine_node_ids == [1, 2, 3]
    assert len(geometry.unattached) == 1


# ── build_route_geometries ───────────────────────────────────────────────────


def test_build_route_geometries_only_covers_relations_with_way_geometry():
    payload = {
        "elements": [
            relation(1, [("node", 100, "stop"), road(10), road(11)], "Ecovia E9"),
            relation(2, [("node", 200, "stop"), road(30)], "Cooperativa feeder"),
            {"type": "node", "id": 100, "lat": -0.2, "lon": LON},
            {"type": "node", "id": 200, "lat": -0.3, "lon": LON},
            way(10, [1, 2, 3]),
            way(11, [3, 4]),
        ]
    }
    geometries = build_route_geometries(payload)

    assert set(geometries) == {"1"}
    assert geometries["1"].spine_node_ids == [1, 2, 3, 4]


def test_build_route_geometries_warns_on_holes(caplog):
    payload = {
        "elements": [
            relation(1, [road(10), road(11), road(12)], "Ecovia E9"),
            way(10, [1, 2, 3]),
            way(12, [5, 6]),
        ]
    }
    with caplog.at_level("WARNING", logger="ingestion.network_loader"):
        build_route_geometries(payload)

    messages = " | ".join(caplog.messages)
    assert "Ecovia E9: 1 of 3 road ways have no geometry" in messages
    assert "1 road piece(s)" in messages and "could not be attached" in messages


# ── distance helper ──────────────────────────────────────────────────────────


def test_haversine_matches_known_distance():
    # One degree of latitude on the equator is ~111.19 km.
    assert haversine_km((LON, 0.0), (LON, 1.0)) == pytest.approx(111.19, rel=1e-3)
    assert haversine_km((LON, 0.0), (LON, 0.0)) == 0.0
