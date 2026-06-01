"""
DAG: full_pipeline
Orchestrates the complete daily pipeline:
  ingest_gtfs >> ingest_weather >> validate (Great Expectations)
  >> glue_transform >> dbt_run >> dbt_test
Fails fast on quality gate; alerts via CloudWatch on any task failure.
"""
