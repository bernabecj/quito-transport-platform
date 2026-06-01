# Data Governance

## Data Ownership

| Domain        | Dataset                 | Owner               | Contact          |
|---------------|-------------------------|---------------------|------------------|
| Transit       | GTFS routes/stops/trips | Platform team       | —                |
| Weather       | OpenWeather observations| Platform team       | —                |
| Warehouse     | All dbt models          | Platform team       | —                |

## Data Retention Policy

| Zone              | Location                                  | Retention |
|-------------------|-------------------------------------------|-----------|
| Raw               | `s3://quito-transport-raw/`               | 90 days   |
| Processed         | `s3://quito-transport-processed/`         | 1 year    |
| Quality reports   | `s3://quito-transport-quality-reports/`   | 90 days   |
| Redshift warehouse| `quito_transport` database                | 2 years   |

## PII Policy

This project contains **no personal data**. All data sources are public:

- GTFS feeds are anonymised schedule data (routes, stops, times) — no passenger identifiers.
- OpenWeather API responses contain only meteorological observations — no user data.
- OpenStreetMap geolocation data is public map data — no personal attributes.

No consent framework, anonymisation pipeline, or GDPR deletion mechanism is required.

## SLA

The full pipeline must complete by **06:00 Quito time (UTC-5)** each day so that
dashboards reflect the previous day's data before the morning commute.

Airflow alerts via CloudWatch if any DAG task exceeds its timeout or fails.

## Access Control

| Role                  | Access                                                      |
|-----------------------|-------------------------------------------------------------|
| `lambda_ingestion`    | S3 raw zone write, Secrets Manager read                     |
| `glue_processing`     | S3 raw zone read, S3 processed zone write, Glue Catalog R/W |
| `airflow_orchestration` | Lambda invoke, Glue job start, CloudWatch logs write      |
| `redshift_loader`     | S3 processed zone read (COPY), Redshift write               |
| `metabase_readonly`   | Redshift marts schema read-only                             |

All roles are defined in [infrastructure/iam_policies.json](infrastructure/iam_policies.json).

## Data Catalog

All datasets are registered in the **AWS Glue Data Catalog** automatically when
Glue jobs run. Each table entry records:

- Schema (column names, types, nullability)
- Source location (S3 path or Redshift table)
- Partition keys (`year`, `month`, `day` for S3 datasets)
- Last updated timestamp

## Data Lineage

Full lineage from raw S3 → staging → marts → dashboard is documented in
`dbt docs` (generated via `dbt docs generate && dbt docs serve`).

Every dbt model has a human-readable description and column-level annotations
serving as the project's **data dictionary**.

## Partitioning Strategy

| Layer       | Partition keys              | Rationale                                     |
|-------------|-----------------------------|-----------------------------------------------|
| S3 raw      | `year / month / day`        | Enables date-range pruning in Glue and Athena |
| S3 processed| `year / month / day / route`| Adds route-level pruning for peak-hour queries|
| Redshift    | `DISTKEY(route_id)` + `SORTKEY(departure_ts)` | Optimises the delay-by-route aggregation |

## Observability

- **Airflow UI** — task-level status, logs, and retry history.
- **CloudWatch** — DAG failure alarms, Lambda error rates, Glue job metrics.
- **Great Expectations** — per-batch HTML quality reports saved to S3; link surfaced in Airflow task logs.
- **dbt** — test results in `target/run_results.json`; failures block the CI pipeline.
