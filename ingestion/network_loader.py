"""
Transit Network Loader
Queries the OpenStreetMap Overpass API for Quito's public transport network
(bus, trolleybus and metro route relations plus their stop nodes), normalises
the response into three tabular datasets, and writes Parquet to
s3://quito-transport-platform/raw/network/year=.../month=.../day=...

Datasets produced
  routes       one row per transit route (Trole, Ecovía, Metrobús, cooperatives)
  stops        one row per physical stop, with coordinates
  route_stops  bridge table: which stops a route serves, in order

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
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# PTv2 tags stop members with these roles; ways carry the road geometry and
# are skipped — only nodes represent boardable points.
STOP_ROLE_PREFIXES = ("stop", "platform")


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


def build_query() -> str:
    """Overpass QL: route relations in the Quito bbox, plus their member nodes."""
    south, west, north, east = QUITO_BBOX
    route_types = "|".join(OSM_ROUTE_TYPES)
    return (
        f"[out:json][timeout:{OVERPASS_TIMEOUT}];\n"
        f'relation["type"="route"]["route"~"^({route_types})$"]'
        f"({south},{west},{north},{east})->.routes;\n"
        ".routes out body;\n"
        "node(r.routes);\n"
        "out body;\n"
    )


def fetch_network() -> dict:
    """Query Overpass, cycling through endpoints and backing off on refusals.

    Overpass is a free shared service that sheds load aggressively: it answers
    with 429 or 504 when busy, and sometimes with an HTML error page under
    HTTP 200. A non-JSON body is therefore treated as a failure rather than
    trusted, and each round of endpoints is retried with a widening delay.
    """
    query = build_query()
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

    routes = [_route_record(rel, timestamp) for rel in relations]
    route_stops = [
        record for rel in relations for record in _route_stop_records(rel, timestamp)
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


def _route_stop_records(relation: dict, fetched_at: str) -> list[dict]:
    records = []
    for member in relation.get("members", []):
        if member["type"] != "node":
            continue
        role = member.get("role", "")
        if not role.startswith(STOP_ROLE_PREFIXES):
            continue
        records.append(
            {
                "route_id": str(relation["id"]),
                "stop_id": str(member["ref"]),
                # Position along the route, not a timetable order.
                "stop_sequence": len(records),
                "member_role": role,
                "fetched_at": fetched_at,
            }
        )
    return records


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
