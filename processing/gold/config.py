"""
Gold-layer configuration.

Everything the build needs to know that is not data: where the raw zone is,
where the gold tables go, how the raw files are laid out, and the handful of
domain constants the transformations share. Imported by every other module
in processing/gold, and by nothing outside it except the tests.

Domain constants that already exist in ingestion/constants.py are re-exported
from there rather than duplicated, so the trunk-corridor pattern and the
storage root cannot drift between the loaders and the gold build.
"""

from pathlib import Path

from ingestion.constants import LOCAL_STORAGE_ROOT, TRUNK_ROUTE_PATTERN  # noqa: F401

# ── Time ─────────────────────────────────────────────────────────────────────
# Quito is UTC-5 all year; Ecuador has no daylight-saving time. Kept as a
# plain offset rather than a zone name so the arithmetic is explicit and the
# result cannot depend on the tz database of whichever machine runs the job.
UTC_OFFSET_HOURS = -5

# OpenWeather's forecast grid is every three hours (00, 03, ... 21 UTC).
# Traffic is hourly, so each traffic row is matched to the nearest grid point,
# which is at most 90 minutes away.
WEATHER_GRID_HOURS = 3

# ── Layouts ──────────────────────────────────────────────────────────────────
# The raw zone can be read from two shapes:
#
#   LAYOUT_RAW      the bucket layout — <root>/<dataset>/year=/month=/day=[/hour=]/<file>.parquet.
#                   This is s3://<bucket>/raw, an export of it (s3_raw/), or
#                   data/local/raw written by the loaders with STORAGE_BACKEND=local.
#   LAYOUT_SAMPLES  the committed samples — one flat file per table under
#                   data/samples/, so the build and its tests run with nothing
#                   but the repository checked out.
#
# Whatever the shape, the readers in io.py return the same columns, so every
# transformation downstream sees one schema.
LAYOUT_RAW = "raw"
LAYOUT_SAMPLES = "samples"
LAYOUTS = (LAYOUT_RAW, LAYOUT_SAMPLES)

# Dataset folder names under the raw root (same names as the S3 prefixes).
DATASET_NETWORK = "network"
DATASET_TRAFFIC = "traffic"
DATASET_WEATHER = "weather"
NETWORK_TABLES = ("routes", "stops", "route_stops")

# File names in the samples layout.
SAMPLE_FILES = {
    DATASET_TRAFFIC: "traffic/corridor_travel_time_series.parquet",
    DATASET_WEATHER: "weather/weather.parquet",
    "routes": "network/routes.parquet",
    "stops": "network/stops.parquet",
    "route_stops": "network/route_stops.parquet",
}

# Default locations for a local run: the same data/local/ folder the ingestion
# loaders write to, so `raw/` and `gold/` sit side by side exactly as they do
# in the bucket.
DEFAULT_RAW_ROOT = LOCAL_STORAGE_ROOT / "raw"
DEFAULT_GOLD_ROOT = LOCAL_STORAGE_ROOT / "gold"
DEFAULT_SAMPLES_ROOT = Path(__file__).resolve().parent.parent.parent / "data" / "samples"

# ── Schema evolution ─────────────────────────────────────────────────────────
# Columns added to the raw tables after collection started. Every snapshot
# written before the change lacks them, and the series cannot be re-fetched,
# so the reader adds them with these defaults. The values are what the loaders
# would have written had the column existed:
#
#   sequence_source   "osm_member" — before 2026-10 every stop order came from
#                     the OSM member list (ingestion.network_loader.SEQUENCE_FROM_MEMBERS).
#   stops_excluded    0 — no stop was ever dropped from a waypoint list before
#                     CORRIDOR_MAX_SNAP_M existed.
#   member_index, distance_along_m, snap_distance_m — unknown; left null.
#
# Each entry is (default value, Spark type name).
SEQUENCE_SOURCE_DEFAULT = "osm_member"

ROUTE_STOPS_OPTIONAL_COLUMNS = {
    "sequence_source": (SEQUENCE_SOURCE_DEFAULT, "string"),
    "member_index": (None, "long"),
    "distance_along_m": (None, "double"),
    "snap_distance_m": (None, "double"),
}

TRAFFIC_OPTIONAL_COLUMNS = {
    "sequence_source": (SEQUENCE_SOURCE_DEFAULT, "string"),
    "stops_excluded": (0, "long"),
}

# ── Gold tables ──────────────────────────────────────────────────────────────
# One folder per table under the gold root: <gold_root>/<table>/*.parquet.
# Facts that grow with time are partitioned by local_date; dimensions and
# aggregates are small enough to be a single file.
TABLE_DIM_DATE = "dim_date"
TABLE_DIM_ROUTE = "dim_route"
TABLE_DIM_STOP = "dim_stop"
TABLE_DIM_WEATHER_CONDITION = "dim_weather_condition"
TABLE_FCT_WEATHER = "fct_weather"
TABLE_FCT_CORRIDOR_TRAVEL_TIME = "fct_corridor_travel_time"
TABLE_FCT_ROUTE_STOP = "fct_route_stop"
TABLE_FCT_NETWORK_SNAPSHOT = "fct_network_snapshot"
TABLE_AGG_CORRIDOR_HOURLY_PROFILE = "agg_corridor_hourly_profile"
TABLE_AGG_CORRIDOR_RELIABILITY = "agg_corridor_reliability"

GOLD_TABLES = (
    TABLE_DIM_DATE,
    TABLE_DIM_ROUTE,
    TABLE_DIM_STOP,
    TABLE_DIM_WEATHER_CONDITION,
    TABLE_FCT_WEATHER,
    TABLE_FCT_CORRIDOR_TRAVEL_TIME,
    TABLE_FCT_ROUTE_STOP,
    TABLE_FCT_NETWORK_SNAPSHOT,
    TABLE_AGG_CORRIDOR_HOURLY_PROFILE,
    TABLE_AGG_CORRIDOR_RELIABILITY,
)
