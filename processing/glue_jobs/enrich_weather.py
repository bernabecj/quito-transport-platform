"""
Glue Job: enrich_weather
PySpark job that joins cleaned trip data with weather observations by
timestamp and stop coordinates, computes derived metrics (estimated delay,
trip count by hour, weather correlation index), and writes enriched Parquet
to s3://quito-transport-processed/processed/
"""
