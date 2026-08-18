QUITO_LAT = -0.1807
QUITO_LON = -78.4678

OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5"

# OpenStreetMap Overpass API — the transit network source. Quito publishes no
# GTFS feed, so route topology comes from OSM instead. The mirror is a fallback:
# the primary endpoint sheds load under contention.
OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]
OVERPASS_TIMEOUT = 180
# Overpass answers 429/504 when busy; retry the whole endpoint list with a
# widening delay. Total worst case stays inside the Lambda's 300s timeout.
OVERPASS_MAX_ATTEMPTS = 3
OVERPASS_BACKOFF_SECONDS = 15

# (south, west, north, east) — Quito plus the surrounding valleys, wide enough
# to catch routes that terminate outside the urban core.
QUITO_BBOX = (-0.45, -78.65, 0.05, -78.30)

OSM_ROUTE_TYPES = ["bus", "trolleybus", "subway", "light_rail", "share_taxi", "tram"]

S3_BUCKET_NAME = "quito-transport-platform"
S3_WEATHER_PREFIX = "weather"
S3_NETWORK_PREFIX = "network"
