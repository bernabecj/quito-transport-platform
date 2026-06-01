"""
Glue Job: clean_trips
PySpark job that reads raw GTFS Parquet from S3, applies cleaning rules
(cast types, drop malformed rows, deduplicate), and writes the cleaned
dataset to s3://quito-transport-processed/trips/
"""
