"""
Corridor Travel-Time Loader
Samples how long it currently takes to drive each trunk BRT corridor, using the
Mapbox Directions `driving-traffic` profile, and writes Parquet to
raw/traffic/year=.../month=.../day=.../hour=... — in the S3 bucket, or under
data/local/ when STORAGE_BACKEND=local (see ingestion/storage.py). The network
snapshot it starts from is read from that same backend.

Why this exists
  Quito publishes no schedules, so schedule adherence cannot be measured (see
  docs/PROJECT_PLAN.md). What can be measured is how long a corridor actually
  takes to traverse, sampled repeatedly. Accumulated over days, that yields the
  metrics the project originally wanted: peak hours, worst corridors, and how
  unpredictable each route is.

Why there is no free-flow baseline column
  The obvious approach — compare `driving-traffic` against the plain `driving`
  profile — does not work here. Measured on 2026-08-19, `driving-traffic` came
  back *faster* than `driving` on all three trunk corridors, because `driving`
  uses static assumed speeds rather than a real uncongested baseline. Comparing
  the two would report negative delay during rush hour.

  So this loader records the observed duration only, and baselines are derived
  in the warehouse from each corridor's own history. That is also half the API
  requests: one per corridor per sample instead of two.

Cost
  18 corridors x 1 request per sample — one per trunk relation. Sampling
  hourly for a month is roughly 13,000 requests against Mapbox's 100,000/month
  free tier. (Until 2026-10 four Ecovía relations were dropped by the
  CORRIDOR_MAX_MEAN_HOP_KM gate in build_corridors because OSM lists their
  stops out of order; network_loader now orders those from the road geometry.)

Partitioning caveat
  The key below is partitioned to the hour with no minute component, so two
  runs within the same hour overwrite each other — on S3 and on disk alike.
  Sampling faster than hourly requires adding a minute to the key first.
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd
import requests
from dotenv import load_dotenv

from ingestion.constants import (
    CORRIDOR_MAX_MEAN_HOP_KM,
    CORRIDOR_MAX_SNAP_M,
    MAPBOX_DIRECTIONS_URL,
    MAPBOX_HTTP_TIMEOUT,
    MAPBOX_MAX_WAYPOINTS,
    MAPBOX_PROFILE,
    MAPBOX_SECRET_KEY,
    S3_NETWORK_PREFIX,
    S3_TRAFFIC_PREFIX,
    TRAFFIC_SAMPLING_ENABLED_VAR,
    TRUNK_ROUTE_PATTERN,
)
from ingestion.geometry import mean_hop_km
from ingestion.network_loader import SEQUENCE_FROM_GEOMETRY, SEQUENCE_FROM_MEMBERS
from ingestion.storage import get_storage

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

QUITO_TZ = timezone(timedelta(hours=-5))
CONGESTION_LEVELS = ("low", "moderate", "heavy", "severe", "unknown")


def handler(event, context):
    """Lambda entry point.

    Refuses to run unless sampling is explicitly enabled, so a stray schedule or
    manual invoke cannot quietly consume API quota.
    """
    if os.environ.get(TRAFFIC_SAMPLING_ENABLED_VAR, "").lower() != "true":
        message = (
            f"Traffic sampling is disabled. Set {TRAFFIC_SAMPLING_ENABLED_VAR}=true "
            "to allow live Mapbox requests."
        )
        logger.warning(message)
        return {"statusCode": 200, "skipped": True, "reason": message}

    now = datetime.now(timezone.utc)
    # One backend for the whole run: the network snapshot is read from the
    # same place the sample is written to.
    storage = get_storage()
    routes, stops, route_stops = read_latest_network(storage)
    corridors = build_corridors(routes, stops, route_stops)

    df = sample_corridors(corridors, get_token(), now)

    key = (
        f"raw/{S3_TRAFFIC_PREFIX}/year={now.year}/month={now.month:02d}"
        f"/day={now.day:02d}/hour={now.hour:02d}/corridor_travel_time.parquet"
    )
    location = write_raw(df, key, storage)

    return {"statusCode": 200, "corridors_sampled": len(df), "s3_key": key, "location": location}


def get_token() -> str:
    """Resolve the Mapbox token.

    Same rule as weather_loader: in Lambda the value is fetched from Secrets
    Manager at runtime, never injected by Terraform. Resolving a secret in
    Terraform writes it in plaintext into terraform.tfstate and into every plan
    file — which is exactly how the OpenWeather key leaked to a public repo.
    Locally the token comes from .env, so development needs no AWS access.
    """
    secret_name = os.environ.get("MAPBOX_SECRET_NAME")
    if secret_name:
        response = boto3.client("secretsmanager").get_secret_value(SecretId=secret_name)
        return json.loads(response["SecretString"])[MAPBOX_SECRET_KEY]

    try:
        return os.environ[MAPBOX_SECRET_KEY]
    except KeyError:
        raise RuntimeError(
            f"No credentials: set MAPBOX_SECRET_NAME (Lambda) or "
            f"{MAPBOX_SECRET_KEY} (local .env)."
        ) from None


def build_corridors(
    routes: pd.DataFrame, stops: pd.DataFrame, route_stops: pd.DataFrame
) -> list[dict]:
    """Turn the OSM network snapshot into an ordered waypoint list per corridor.

    Passing the real stops as waypoints keeps the measurement on the road the
    bus actually uses. Querying endpoint-to-endpoint would let Mapbox choose its
    own optimal path, which would measure a different road entirely.
    """
    trunk = routes[
        routes["route_name"].fillna("").str.contains(TRUNK_ROUTE_PATTERN, case=False, regex=True)
    ]
    coords = stops.set_index("stop_id")[["longitude", "latitude"]]

    corridors = []
    for route in trunk.itertuples():
        members = route_stops[route_stops["route_id"] == route.route_id]
        members = members.sort_values("stop_sequence")
        members, excluded = _exclude_off_road_stops(members, route.route_name)
        points = [
            (coords.loc[s, "longitude"], coords.loc[s, "latitude"])
            for s in members["stop_id"]
            if s in coords.index
        ]
        if len(points) < 2:
            continue

        mean_hop = mean_hop_km(points)
        if mean_hop > CORRIDOR_MAX_MEAN_HOP_KM:
            logger.warning(
                "skipping %s: stops are %.2f km apart on average, so the relation's "
                "member order is not geographic and routing it would measure a zigzag",
                route.route_name,
                mean_hop,
            )
            continue

        corridors.append(
            {
                "route_id": route.route_id,
                "route_ref": route.route_ref,
                "route_name": route.route_name,
                "mean_hop_km": round(mean_hop, 2),
                "sequence_source": _sequence_source(members),
                "stops_excluded": excluded,
                "waypoints": _downsample(points, MAPBOX_MAX_WAYPOINTS),
            }
        )
    return corridors


def _sequence_source(members: pd.DataFrame) -> str:
    """Where this route's stop order came from; snapshots written before the
    column existed are all member order."""
    if "sequence_source" not in members.columns or members.empty:
        return SEQUENCE_FROM_MEMBERS
    return members["sequence_source"].iloc[0]


def _exclude_off_road_stops(members: pd.DataFrame, route_name: str) -> tuple[pd.DataFrame, int]:
    """Drop stops that are not on the corridor's road from the waypoint list.

    Only for routes whose order came from the geometry: there, a stop far from
    the road is a misplaced member (see CORRIDOR_MAX_SNAP_M). Routes that kept
    the OSM member order are left whole — for them a large snap distance means
    the mapped road has a hole where the stop is, and the stop is still right.
    Nothing is removed from route_stops itself; this only shapes the request.
    """
    if _sequence_source(members) != SEQUENCE_FROM_GEOMETRY:
        return members, 0
    off_road = members["snap_distance_m"] > CORRIDOR_MAX_SNAP_M
    for stop in members[off_road].itertuples():
        logger.warning(
            "%s: stop %s is %.0f m from the road, not used as a waypoint",
            route_name,
            stop.stop_id,
            stop.snap_distance_m,
        )
    return members[~off_road], int(off_road.sum())


def _downsample(points: list[tuple], limit: int) -> list[tuple]:
    """Evenly thin a waypoint list, always keeping both endpoints."""
    if len(points) <= limit:
        return points
    step = (len(points) - 1) / (limit - 1)
    return [points[round(i * step)] for i in range(limit)]


def sample_corridors(corridors: list[dict], token: str, sampled_at: datetime) -> pd.DataFrame:
    records = []
    for corridor in corridors:
        try:
            records.append(sample_corridor(corridor, token, sampled_at))
        except (requests.RequestException, RuntimeError) as exc:
            # One bad corridor must not lose the whole sample: the value of this
            # dataset is an unbroken time series.
            logger.warning("%s: %s", corridor["route_name"], exc)
    return pd.DataFrame(records)


def sample_corridor(corridor: dict, token: str, sampled_at: datetime) -> dict:
    coords = ";".join(f"{lon},{lat}" for lon, lat in corridor["waypoints"])
    response = requests.get(
        f"{MAPBOX_DIRECTIONS_URL}/{MAPBOX_PROFILE}/{coords}",
        params={
            "access_token": token,
            "overview": "full",
            "geometries": "geojson",
            "annotations": "congestion",
        },
        timeout=MAPBOX_HTTP_TIMEOUT,
    )
    if response.status_code != 200:
        # The token travels in the query string, so never surface the URL.
        raise RuntimeError(
            f"HTTP {response.status_code}: {response.json().get('message', '')}"
        )

    payload = response.json()
    if not payload.get("routes"):
        raise RuntimeError("no route returned")
    route = payload["routes"][0]

    congestion = [
        level for leg in route.get("legs", []) for level in leg.get("annotation", {}).get("congestion", [])
    ]
    counts = {level: congestion.count(level) for level in CONGESTION_LEVELS}
    known = len(congestion) - counts["unknown"]

    duration_s = route["duration"]
    distance_m = route["distance"]
    local = sampled_at.astimezone(QUITO_TZ)

    return {
        "route_id": corridor["route_id"],
        "route_ref": corridor["route_ref"],
        "route_name": corridor["route_name"],
        "sampled_at": sampled_at.isoformat(),
        # Local hour and weekday are what peak-hour analysis groups by; deriving
        # them here keeps every consumer from re-implementing the -05:00 offset.
        "local_hour": local.hour,
        "local_weekday": local.weekday(),
        "waypoint_count": len(corridor["waypoints"]),
        "mean_hop_km": corridor["mean_hop_km"],
        # Provenance of the waypoint order, so the four corridors recovered
        # from road geometry can be told apart from the rest in the series.
        "sequence_source": corridor["sequence_source"],
        "stops_excluded": corridor["stops_excluded"],
        "distance_meters": round(distance_m, 1),
        "duration_seconds": round(duration_s, 1),
        "mean_speed_kmh": round(distance_m / duration_s * 3.6, 2) if duration_s else None,
        "segments_total": len(congestion),
        "segments_known": known,
        "congestion_known_pct": round(known / len(congestion) * 100, 1) if congestion else 0.0,
        **{f"congestion_{level}": counts[level] for level in CONGESTION_LEVELS},
    }


def read_latest_network(storage=None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load the most recent network snapshot written by network_loader, from
    the selected backend (S3 or the local folder — see ingestion.storage)."""
    storage = storage or get_storage()
    keys = storage.list_keys(f"raw/{S3_NETWORK_PREFIX}/")
    if not keys:
        raise RuntimeError(f"no network snapshot under raw/{S3_NETWORK_PREFIX}/")

    latest = max(key.rsplit("/", 1)[0] for key in keys)
    return tuple(
        storage.get_parquet(f"{latest}/{name}.parquet")
        for name in ("routes", "stops", "route_stops")
    )


def write_raw(df: pd.DataFrame, key: str, storage=None) -> str:
    """Write one Parquet file under `key` to the selected backend (S3 or the
    local folder — see ingestion.storage) and return where it landed."""
    return (storage or get_storage()).put_parquet(df, key)


if __name__ == "__main__":
    print(handler({}, None))
