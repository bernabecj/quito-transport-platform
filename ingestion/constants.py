from pathlib import Path

QUITO_LAT = -0.1807
QUITO_LON = -78.4678

OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5"
# JSON field inside the Secrets Manager entry, and the env var name used locally.
OPENWEATHER_SECRET_KEY = "OPENWEATHER_API_KEY"

# OpenStreetMap Overpass API — the transit network source. Quito publishes no
# GTFS feed, so route topology comes from OSM instead. The mirror is a fallback:
# the primary endpoint sheds load under contention.
OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]
# Server-side query budget. A healthy run answers in ~20s, so 90s is generous;
# keeping it low means a struggling endpoint fails fast and we move to the next
# one rather than burning the Lambda's whole budget on one hung request.
OVERPASS_TIMEOUT = 90
OVERPASS_HTTP_TIMEOUT = OVERPASS_TIMEOUT + 30

# Overpass answers 429/500/504 when busy, and does so often enough that a daily
# job must be patient. Worst case is
# OVERPASS_MAX_ATTEMPTS * len(OVERPASS_URLS) * OVERPASS_HTTP_TIMEOUT plus
# backoff — keep the Lambda timeout above that (see lambda.tf).
OVERPASS_MAX_ATTEMPTS = 3
OVERPASS_BACKOFF_SECONDS = 10

# Required, not cosmetic: overpass-api.de rejects the default python-requests
# User-Agent with HTTP 406 Not Acceptable. Any descriptive agent is accepted,
# and the OSM usage policy asks callers to identify their application anyway.
OVERPASS_HEADERS = {
    "User-Agent": (
        "quito-transport-platform/1.0 "
        "(+https://github.com/bernabecj/quito-transport-platform)"
    )
}

# (south, west, north, east) — Quito plus the surrounding valleys, wide enough
# to catch routes that terminate outside the urban core.
QUITO_BBOX = (-0.45, -78.65, 0.05, -78.30)

OSM_ROUTE_TYPES = ["bus", "trolleybus", "subway", "light_rail", "share_taxi", "tram"]

S3_BUCKET_NAME = "quito-transport-platform"
S3_WEATHER_PREFIX = "weather"
S3_NETWORK_PREFIX = "network"
S3_TRAFFIC_PREFIX = "traffic"

# Where the local storage backend (ingestion/storage.py) puts the raw zone
# when running without AWS. It stands in for the bucket: the same keys the
# loaders write to S3 land under this folder, so
# s3://<bucket>/raw/weather/year=.../weather.parquet becomes
# data/local/raw/weather/year=.../weather.parquet. Resolved from this file,
# not the working directory, so `python -m ingestion.weather_loader` lands in
# the same place wherever it is launched from.
LOCAL_STORAGE_ROOT = Path(__file__).resolve().parent.parent / "data" / "local"

# The one switch between the two backends. Unset means S3: the Lambdas set no
# such variable (see lambda.tf) and must keep writing to the bucket. Locally,
# STORAGE_BACKEND=local in .env sends everything to LOCAL_STORAGE_ROOT. Any
# other value is an error, not a fallback — a typo must not send data to S3.
STORAGE_BACKEND_VAR = "STORAGE_BACKEND"
STORAGE_BACKEND_S3 = "s3"
STORAGE_BACKEND_LOCAL = "local"

# ── Mapbox travel-time sampling ──────────────────────────────────────────────
# Verified 2026-08-19: Mapbox reports real congestion on Quito's BRT corridors
# (49-71% of segments carry observed data, not the "unknown" fallback).
# See scripts/verify_mapbox_traffic.py.
MAPBOX_DIRECTIONS_URL = "https://api.mapbox.com/directions/v5/mapbox"
MAPBOX_PROFILE = "driving-traffic"
# Hard API limit for this profile, confirmed empirically: 26 waypoints returns
# HTTP 422. Corridors with more stops are evenly downsampled to fit.
MAPBOX_MAX_WAYPOINTS = 25
MAPBOX_HTTP_TIMEOUT = 30
# JSON field inside the Secrets Manager entry, and the env var name used locally.
MAPBOX_SECRET_KEY = "MAPBOX_TOKEN"

# Only the trunk BRT systems are sampled — the three networks from the original
# project pitch. Sampling all 102 routes with stops would cost 5.7x more
# requests for corridors that are mostly low-frequency feeder services.
TRUNK_ROUTE_PATTERN = r"Trole|Ecov|MetroBus|Metrobus"

# Quality gate on corridor geometry. Measured 2026-08-19, four trunk relations
# listed their stops out of order in OSM, with consecutive "stops" up to
# 15.9 km apart, which no BRT has. Routing through scrambled waypoints measures
# a zigzag, not the corridor, so anything over this mean hop is not sampled.
# Healthy corridors sit at 0.53-0.81 km; those four sat at 1.18-2.49 km until
# network_loader started ordering them from the road geometry (2026-10), after
# which they measure 0.51-0.68 km. The gate stays as the safety net.
CORRIDOR_MAX_MEAN_HOP_KM = 1.0

# When network_loader took a corridor's stop order from the road geometry
# (route_stops.sequence_source == "geometry"), a stop this far from that road
# is not on the corridor: it is a member the relation should not have, or a
# stop beyond the end of the mapped road. Such stops stay in route_stops but
# are not sent to Mapbox as waypoints — routed through, they add a detour the
# corridor does not have (6.8 km on one E3 relation). Measured 2026-10-12:
# every on-road trunk stop snaps within 90 m; the misplaced ones sit at
# 1.15, 1.48 and 6.83 km. Not applied to routes that kept the OSM member
# order, where a large snap distance means a hole in the mapped road, not a
# misplaced stop.
CORRIDOR_MAX_SNAP_M = 200

# The live Mapbox call is opt-in. Left unset, the loader refuses to spend API
# quota; sampling only happens when this is explicitly set to "true".
TRAFFIC_SAMPLING_ENABLED_VAR = "TRAFFIC_SAMPLING_ENABLED"
