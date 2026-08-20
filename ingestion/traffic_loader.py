"""
Corridor Travel-Time Loader
Samples how long it currently takes to drive each trunk BRT corridor, using the
Mapbox Directions `driving-traffic` profile, and writes Parquet to
s3://quito-transport-platform/raw/traffic/year=.../month=.../day=.../hour=...

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
  18 trunk corridors x 1 request per sample. Hourly for a month is roughly
  13,000 requests against Mapbox's 100,000/month free tier.
"""

import io
import json
import logging
import math
import os
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from dotenv import load_dotenv

from ingestion.constants import (
    CORRIDOR_MAX_MEAN_HOP_KM,
    MAPBOX_DIRECTIONS_URL,
    MAPBOX_HTTP_TIMEOUT,
    MAPBOX_MAX_WAYPOINTS,
    MAPBOX_PROFILE,
    MAPBOX_SECRET_KEY,
    S3_BUCKET_NAME,
    S3_NETWORK_PREFIX,
    S3_TRAFFIC_PREFIX,
    TRAFFIC_SAMPLING_ENABLED_VAR,
    TRUNK_ROUTE_PATTERN,
)

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
    routes, stops, route_stops = read_latest_network(S3_BUCKET_NAME)
    corridors = build_corridors(routes, stops, route_stops)

    df = sample_corridors(corridors, get_token(), now)

    key = (
        f"raw/{S3_TRAFFIC_PREFIX}/year={now.year}/month={now.month:02d}"
        f"/day={now.day:02d}/hour={now.hour:02d}/corridor_travel_time.parquet"
    )
    write_to_s3(df, S3_BUCKET_NAME, key)

    return {"statusCode": 200, "corridors_sampled": len(df), "s3_key": key}


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
        points = [
            (coords.loc[s, "longitude"], coords.loc[s, "latitude"])
            for s in members["stop_id"]
            if s in coords.index
        ]
        if len(points) < 2:
            continue

        mean_hop = _mean_hop_km(points)
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
                "waypoints": _downsample(points, MAPBOX_MAX_WAYPOINTS),
            }
        )
    return corridors


def _haversine_km(a: tuple, b: tuple) -> float:
    (lon1, lat1), (lon2, lat2) = a, b
    rad = math.pi / 180
    inner = (
        math.sin((lat2 - lat1) * rad / 2) ** 2
        + math.cos(lat1 * rad) * math.cos(lat2 * rad) * math.sin((lon2 - lon1) * rad / 2) ** 2
    )
    return 2 * 6371 * math.asin(math.sqrt(inner))


def _mean_hop_km(points: list[tuple]) -> float:
    """Average distance between consecutive stops — the scrambling detector.

    Mean hop separates a genuine loop route (smooth ~0.6 km spacing, endpoints
    close together) from a relation whose members are out of order (erratic
    multi-kilometre jumps). End-to-end distance alone would flag both.
    """
    hops = [_haversine_km(points[i], points[i + 1]) for i in range(len(points) - 1)]
    return sum(hops) / len(hops)


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
        "distance_meters": round(distance_m, 1),
        "duration_seconds": round(duration_s, 1),
        "mean_speed_kmh": round(distance_m / duration_s * 3.6, 2) if duration_s else None,
        "segments_total": len(congestion),
        "segments_known": known,
        "congestion_known_pct": round(known / len(congestion) * 100, 1) if congestion else 0.0,
        **{f"congestion_{level}": counts[level] for level in CONGESTION_LEVELS},
    }


def read_latest_network(bucket: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load the most recent network snapshot written by network_loader."""
    s3 = boto3.client("s3")
    listing = s3.list_objects_v2(Bucket=bucket, Prefix=f"raw/{S3_NETWORK_PREFIX}/")
    keys = [obj["Key"] for obj in listing.get("Contents", [])]
    if not keys:
        raise RuntimeError(f"no network snapshot under raw/{S3_NETWORK_PREFIX}/")

    latest = max(key.rsplit("/", 1)[0] for key in keys)
    frames = []
    for name in ("routes", "stops", "route_stops"):
        body = s3.get_object(Bucket=bucket, Key=f"{latest}/{name}.parquet")["Body"].read()
        frames.append(pd.read_parquet(io.BytesIO(body)))
    return tuple(frames)


def write_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    table = pa.Table.from_pandas(df)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())


if __name__ == "__main__":
    print(handler({}, None))
