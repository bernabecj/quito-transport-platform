"""
DAG: ingest_gtfs
Triggers the Lambda function that pulls the latest Quito GTFS static feed
(routes, stops, trips) and lands it as Parquet in the S3 raw zone.
"""
