"""
GTFS Loader
Fetches the Quito GTFS static feed (routes.txt, stops.txt, trips.txt,
stop_times.txt) from the public endpoint, converts to Parquet, and writes
to s3://quito-transport-raw/gtfs/year=.../month=.../day=...
"""
