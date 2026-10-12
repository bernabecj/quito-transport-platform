"""
Transit Network Loader
Queries the OpenStreetMap Overpass API for Quito's public transport network
(bus, trolleybus and metro route relations plus their stop nodes), normalises
the response into three tabular datasets, and writes Parquet to
s3://quito-transport-platform/raw/network/year=.../month=.../day=...

Datasets produced
  routes       one row per transit route (Trole, Ecovía, Metrobús, cooperatives)
  stops        one row per physical stop, with coordinates
  route_stops  bridge table: which stops a route serves, in order — taken
               from the OSM member list, or from the road geometry when the
               member list is not geographic (see _route_stop_records)

Scope note
  OpenStreetMap carries the *topology* of the network — routes, stops and their
  ordering — but no timetables. Quito publishes no GTFS feed (checked against
  transit.land's registry and the Mobility Database catalogue; neither indexes
  any Ecuadorian feed), so schedule-derived metrics such as delay are out of
  scope for this platform. See docs/PROJECT_PLAN.md for the questions this
  data does answer.
"""

import io
import logging
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from ingestion.constants import (
    OSM_ROUTE_TYPES,
    OVERPASS_BACKOFF_SECONDS,
    OVERPASS_HEADERS,
    OVERPASS_HTTP_TIMEOUT,
    OVERPASS_MAX_ATTEMPTS,
    OVERPASS_TIMEOUT,
    OVERPASS_URLS,
    QUITO_BBOX,
    S3_BUCKET_NAME,
    S3_NETWORK_PREFIX,
    TRUNK_ROUTE_PATTERN,
)
from ingestion.geometry import RouteGeometry, chain_route_ways, mean_hop_km, project_onto_polyline

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# PTv2 tags stop members with these roles — only nodes represent boardable
# points. Way members carry the road geometry, fetched for the trunk corridors
# (see build_query) and chained in build_route_geometries.
STOP_ROLE_PREFIXES = ("stop", "platform")

# Values of route_stops.sequence_source: where stop_sequence came from.
SEQUENCE_FROM_MEMBERS = "osm_member"  # the order OSM lists the stops in
SEQUENCE_FROM_GEOMETRY = "geometry"  # distance along the chained road ways


def handler(event, context):
    """Lambda entry point."""
    now = datetime.now(timezone.utc)

    payload = fetch_network()
    dfs = parse_network(payload, now)

    date_prefix = f"raw/{S3_NETWORK_PREFIX}/year={now.year}/month={now.month:02d}/day={now.day:02d}"
    written = write_to_s3(dfs, S3_BUCKET_NAME, date_prefix)

    return {
        "statusCode": 200,
        "files_written": len(written),
        "total_rows": sum(v["rows"] for v in written.values()),
        "prefix": date_prefix,
    }


def build_query(name_pattern: str | None = None) -> str:
    """Overpass QL: route relations in the Quito bbox, their member nodes, and
    the road geometry of the trunk corridors.

    Three output blocks, in order:

    1. ``.routes out body`` — every route relation, with its member list (way
       and node refs, in OSM order) but no coordinates.
    2. ``node(r.routes); out body`` — every member node with tags and
       coordinates. This is where stop names and positions come from.
    3. ``way(r.trunk); out geom`` — the ways of the trunk relations only, each
       with its ``nodes`` list and a ``geometry`` array of coordinates. This is
       the road shape used to derive stop order where the relation's member
       order is not geographic (see the `sample README`_). Restricted to trunk
       corridors because they are the only routes the traffic loader samples,
       and asking for the geometry of all ~500 feeder routes would multiply the
       payload for no consumer.

    ``name_pattern`` narrows block 1 to relations whose name matches it. The
    production query passes nothing; the fixture under data/samples/network is
    generated with ``name_pattern=TRUNK_ROUTE_PATTERN`` so it holds exactly the
    trunk corridors, in the same shape the Lambda receives.

    .. _sample README: ../data/samples/README.md
    """
    south, west, north, east = QUITO_BBOX
    route_types = "|".join(OSM_ROUTE_TYPES)
    # Overpass regexes are case-sensitive unless the `,i` flag is given; the
    # traffic loader matches the same pattern with case=False, and the two
    # must select the same relations.
    name_filter = f'["name"~"{name_pattern}",i]' if name_pattern else ""
    return (
        f"[out:json][timeout:{OVERPASS_TIMEOUT}];\n"
        f'relation["type"="route"]["route"~"^({route_types})$"]{name_filter}'
        f"({south},{west},{north},{east})->.routes;\n"
        ".routes out body;\n"
        "node(r.routes);\n"
        "out body;\n"
        f'rel.routes["name"~"{TRUNK_ROUTE_PATTERN}",i]->.trunk;\n'
        "way(r.trunk);\n"
        "out geom;\n"
    )


def fetch_network(query: str | None = None) -> dict:
    """Query Overpass, cycling through endpoints and backing off on refusals.

    Overpass is a free shared service that sheds load aggressively: it answers
    with 429 or 504 when busy, and sometimes with an HTML error page under
    HTTP 200. A non-JSON body is therefore treated as a failure rather than
    trusted, and each round of endpoints is retried with a widening delay.

    `query` defaults to the production query; scripts pass a narrower one.
    """
    query = query or build_query()
    failures = []

    for attempt in range(1, OVERPASS_MAX_ATTEMPTS + 1):
        if attempt > 1:
            time.sleep(OVERPASS_BACKOFF_SECONDS * 2 ** (attempt - 2))

        for url in OVERPASS_URLS:
            host = urlparse(url).netloc
            try:
                response = requests.post(
                    url,
                    data={"data": query},
                    headers=OVERPASS_HEADERS,
                    timeout=OVERPASS_HTTP_TIMEOUT,
                )
                response.raise_for_status()
                payload = response.json()
            except (requests.RequestException, ValueError) as exc:
                # Every endpoint's error is kept: reporting only the last one
                # hides which endpoint actually refused and why.
                reason = f"attempt {attempt} {host}: {type(exc).__name__}: {exc}"
                logger.warning(reason)
                failures.append(reason)
                continue

            if payload.get("elements"):
                logger.info(
                    "attempt %s %s: %s elements", attempt, host, len(payload["elements"])
                )
                return payload

            reason = f"attempt {attempt} {host}: HTTP 200 but no elements"
            logger.warning(reason)
            failures.append(reason)

    raise RuntimeError(
        f"Overpass unavailable after {OVERPASS_MAX_ATTEMPTS} attempts. "
        + " | ".join(failures)
    )


def parse_network(payload: dict, fetched_at: datetime) -> dict[str, pd.DataFrame]:
    elements = payload["elements"]
    relations = [e for e in elements if e["type"] == "relation"]
    nodes = {e["id"]: e for e in elements if e["type"] == "node"}
    timestamp = fetched_at.isoformat()

    geometries = build_route_geometries(payload)

    routes = [_route_record(rel, timestamp) for rel in relations]
    route_stops = [
        record
        for rel in relations
        for record in _route_stop_records(rel, timestamp, nodes, geometries.get(str(rel["id"])))
    ]

    # Only keep stops an actual route serves; Overpass also returns unrelated
    # member nodes such as route markers.
    served_ids = {int(rs["stop_id"]) for rs in route_stops}
    stops = [
        _stop_record(nodes[node_id], timestamp)
        for node_id in sorted(served_ids)
        if node_id in nodes
    ]

    return {
        "routes": pd.DataFrame(routes),
        "stops": pd.DataFrame(stops),
        "route_stops": pd.DataFrame(route_stops),
    }


def build_route_geometries(payload: dict) -> dict[str, RouteGeometry]:
    """Chain each relation's way members into a polyline, keyed by route id.

    Only relations whose ways came back with geometry get an entry — with the
    production query that is the trunk corridors (see build_query). Feeder
    routes have way members but no `way` elements in the payload, so they are
    absent here rather than present with an empty shape.
    """
    elements = payload["elements"]
    ways = {e["id"]: e for e in elements if e["type"] == "way"}
    geometries = {}
    for relation in (e for e in elements if e["type"] == "relation"):
        geometry = chain_route_ways(relation, ways)
        if geometry is None:
            continue
        if geometry.missing_ways:
            logger.warning(
                "%s: %s of %s road ways have no geometry in the payload",
                relation.get("tags", {}).get("name", relation["id"]),
                geometry.missing_ways,
                geometry.road_ways,
            )
        if geometry.unattached:
            logger.warning(
                "%s: %s road piece(s) totalling %.2f km could not be attached to the "
                "%.2f km spine",
                relation.get("tags", {}).get("name", relation["id"]),
                len(geometry.unattached),
                geometry.unattached_length_km,
                geometry.spine_length_km,
            )
        geometries[geometry.route_id] = geometry
    return geometries


def _route_record(relation: dict, fetched_at: str) -> dict:
    tags = relation.get("tags", {})
    return {
        "route_id": str(relation["id"]),
        "route_ref": tags.get("ref"),
        "route_name": tags.get("name"),
        "route_type": tags.get("route"),
        "operator": tags.get("operator"),
        "network": tags.get("network"),
        "origin": tags.get("from"),
        "destination": tags.get("to"),
        "colour": tags.get("colour"),
        "fetched_at": fetched_at,
    }


def _route_stop_records(
    relation: dict, fetched_at: str, nodes: dict, geometry: RouteGeometry | None
) -> list[dict]:
    """One record per stop the relation serves, with its position on the route.

    `stop_sequence` is the position along the route, not a timetable order.
    It comes from one of two sources, recorded in `sequence_source`:

    - the order OSM lists the stop members in, which is what PTv2 intends and
      is right for most relations; or
    - distance along the road geometry, when that is available and orders the
      stops more tightly (lower mean hop) than the member list does. This is
      what repairs relations whose member list is scrambled — consecutive
      "stops" kilometres apart — without touching relations that are fine.

    `member_index` keeps the OSM position either way, and when geometry exists
    every stop also carries `distance_along_m` and `snap_distance_m` so a
    consumer can see how well the stop fits the road, whichever order won.
    """
    records = []
    for index, member in enumerate(relation.get("members", [])):
        if member["type"] != "node":
            continue
        role = member.get("role", "")
        if not role.startswith(STOP_ROLE_PREFIXES):
            continue
        records.append(
            {
                "route_id": str(relation["id"]),
                "stop_id": str(member["ref"]),
                "stop_sequence": len(records),
                "member_role": role,
                "member_index": index,
                "sequence_source": SEQUENCE_FROM_MEMBERS,
                "distance_along_m": None,
                "snap_distance_m": None,
                "fetched_at": fetched_at,
            }
        )

    coords = [
        (nodes[int(r["stop_id"])]["lon"], nodes[int(r["stop_id"])]["lat"])
        for r in records
        if int(r["stop_id"]) in nodes
    ]
    # Without a road shape, or without every stop's coordinates, or with fewer
    # than two stops, there is nothing to compare against: keep the OSM order.
    if geometry is None or len(coords) != len(records) or len(records) < 2:
        return records

    for record, (along, snap) in zip(records, project_onto_polyline(geometry.spine, coords)):
        record["distance_along_m"] = round(along, 1)
        record["snap_distance_m"] = round(snap, 1)

    by_geometry = sorted(records, key=lambda r: (r["distance_along_m"], r["member_index"]))
    by_id = {r["stop_id"]: c for r, c in zip(records, coords)}
    member_hop = mean_hop_km(coords)
    geometry_hop = mean_hop_km([by_id[r["stop_id"]] for r in by_geometry])

    # Ties keep the OSM order: it is the mapper's intent, and the geometry
    # order can only claim to be better when it measurably is.
    if geometry_hop >= member_hop:
        return records

    logger.info(
        "%s: stop order taken from road geometry (mean hop %.2f km) instead of the "
        "member list (%.2f km)",
        relation.get("tags", {}).get("name", relation["id"]),
        geometry_hop,
        member_hop,
    )
    for sequence, record in enumerate(by_geometry):
        record["stop_sequence"] = sequence
        record["sequence_source"] = SEQUENCE_FROM_GEOMETRY
    _warn_if_direction_disagrees(relation, by_geometry, nodes)
    return by_geometry


def _warn_if_direction_disagrees(relation: dict, ordered: list[dict], nodes: dict) -> None:
    """Flag a geometry-ordered route whose ends do not match its from/to tags.

    The spine's direction comes from the order of the way members. If the
    first stop along it is named like the `to` tag, or the last like `from`,
    the ways are probably listed against the direction of travel and the
    corridor would be sampled backwards. A warning, not a flip: the tags are
    free text and a mismatch needs a human look.
    """
    tags = relation.get("tags", {})
    origin, destination = tags.get("from"), tags.get("to")
    first = nodes[int(ordered[0]["stop_id"])].get("tags", {}).get("name")
    last = nodes[int(ordered[-1]["stop_id"])].get("tags", {}).get("name")
    if not (origin and destination and first and last):
        return

    def alike(a: str, b: str) -> bool:
        a, b = a.casefold(), b.casefold()
        return a in b or b in a

    if (alike(first, destination) and not alike(first, origin)) or (
        alike(last, origin) and not alike(last, destination)
    ):
        logger.warning(
            "%s: geometry runs '%s' -> '%s' but the relation is tagged from='%s' to='%s'",
            tags.get("name", relation["id"]),
            first,
            last,
            origin,
            destination,
        )


def _stop_record(node: dict, fetched_at: str) -> dict:
    tags = node.get("tags", {})
    return {
        "stop_id": str(node["id"]),
        "stop_name": tags.get("name"),
        "latitude": node["lat"],
        "longitude": node["lon"],
        "stop_type": tags.get("public_transport"),
        "shelter": tags.get("shelter"),
        "fetched_at": fetched_at,
    }


def write_to_s3(dfs: dict[str, pd.DataFrame], bucket: str, date_prefix: str) -> dict:
    s3 = boto3.client("s3")
    written = {}
    for name, df in dfs.items():
        key = f"{date_prefix}/{name}.parquet"
        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
        written[name] = {"key": key, "rows": len(df)}
    return written


if __name__ == "__main__":
    result = handler({}, None)
    print(result)
