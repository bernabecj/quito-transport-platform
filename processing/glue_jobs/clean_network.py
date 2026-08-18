"""
Glue Job: clean_network
PySpark job that reads raw OSM network Parquet from S3 (routes, stops,
route_stops), applies cleaning rules (cast types, validate coordinates against
the Quito bounding box, deduplicate stops shared across routes), and writes the
cleaned datasets to s3://quito-transport-platform/processed/
"""
