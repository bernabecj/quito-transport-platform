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

# Quality gate on corridor geometry. `stop_sequence` follows the order members
# appear in the OSM relation, which for some Quito routes is not geographic
# order — measured 2026-08-19, four trunk corridors had consecutive "stops" up
# to 15.9 km apart, which no BRT has. Routing through scrambled waypoints
# measures a zigzag, not the corridor, so those are excluded from sampling.
# Healthy corridors sit at 0.53-0.81 km mean hop; the broken ones at 1.18-2.49.
CORRIDOR_MAX_MEAN_HOP_KM = 1.0

# The live Mapbox call is opt-in. Left unset, the loader refuses to spend API
# quota; sampling only happens when this is explicitly set to "true".
TRAFFIC_SAMPLING_ENABLED_VAR = "TRAFFIC_SAMPLING_ENABLED"
