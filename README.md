# Quito Transport Platform

A public data engineering platform that ingests real transit and weather data for Quito's bus rapid transit network (Trole, Ecovía, Metrobús), processes it with an AWS-native stack, and surfaces delay patterns, peak-hour insights, and weather correlations to citizens through a live dashboard.

![CI](https://github.com/bernabecj/quito-transport-platform/actions/workflows/ci.yml/badge.svg)

See [docs/PROJECT_PLAN.md](docs/PROJECT_PLAN.md) for the original pitch, the 3-week build plan, and the interview narrative behind this project.

---

## Why this exists

Quito's public transport serves hundreds of thousands of daily commuters, yet reliable data about delays, worst routes, and peak congestion is not publicly accessible. This platform changes that by building an open, observable data pipeline that turns raw GTFS schedules and weather observations into actionable insights — delay heatmaps, peak-hour traffic by route, and weather-delay correlations — all freely accessible.

---

## Architecture

```
APIs (GTFS + OpenWeather + OpenStreetMap)
              │
       AWS Lambda (scheduled ingestion trigger)
              │
     S3 Raw Zone  ─── Parquet, partitioned by date
              │
       Airflow DAG (Amazon MWAA)
              │
   ┌──────────┴──────────┐
   │                     │
AWS Glue / PySpark    Great Expectations
(transform & enrich)  (data quality gate)
   │                     │
   └──────────┬──────────┘
              │  (quality pass)
     S3 Processed Zone
              │
  Amazon Redshift (warehouse)
              │
      dbt (star schema + tests)
              │
  Metabase on EC2 (dashboards + maps)
```

### Stack

| Layer          | Tool                   | AWS Service                         |
| -------------- | ---------------------- | ----------------------------------- |
| Ingestion      | Python + REST APIs     | AWS Lambda                          |
| Raw Storage    | Parquet files          | AWS S3 (raw + processed zones)      |
| Orchestration  | Apache Airflow         | Amazon MWAA                         |
| Processing     | PySpark                | AWS Glue / EMR Serverless           |
| Transformation | dbt                    | Runs against Amazon Redshift        |
| Warehouse      | SQL analytics          | Amazon Redshift                     |
| Data Quality   | Great Expectations     | Runs in Glue/Airflow, reports to S3 |
| Data Catalog   | Schema & lineage       | AWS Glue Data Catalog               |
| Visualization  | Metabase               | EC2 t2.micro (free tier)            |
| CI/CD          | GitHub Actions         | Deploys to AWS automatically        |
| Secrets        | API keys & credentials | AWS Secrets Manager                 |
| Monitoring     | Pipeline observability | AWS CloudWatch                      |

---

## Data Sources

| Source          | What it provides                               | Cost      |
| --------------- | ---------------------------------------------- | --------- |
| Quito GTFS feed | Static transit schedules: routes, stops, trips | Free      |
| OpenWeather API | Current + 5-day forecast by coordinates        | Free tier |
| OpenStreetMap   | Geolocation enrichment for stops and routes    | Free      |

---

## Data Model (Star Schema)

```
fct_trips
 ├── route_id     → dim_routes
 ├── stop_id      → dim_stops
 └── condition_id → dim_weather_conditions
```

All models and column descriptions live in the dbt docs site (see [Running dbt docs](#running-dbt-docs)).

---

## Testing Strategy

Testing happens at three layers:

### 1. PyTest — unit tests for Python transformation logic (TDD)

```python
def test_calculate_delay_returns_minutes():
    scheduled = datetime(2024, 1, 1, 8, 0)
    actual    = datetime(2024, 1, 1, 8, 15)
    assert calculate_delay(scheduled, actual) == 15
```

Run: `pytest tests/ -v`

### 2. Great Expectations — data quality gate at ingestion

Runs before any data enters the warehouse. The Airflow DAG halts and alerts if any expectation fails.

```python
df.expect_column_to_exist("trip_id")
df.expect_column_values_to_not_be_null("stop_id")
df.expect_column_values_to_be_between("duration_minutes", min_value=1, max_value=180)
```

### 3. dbt Tests — data integrity in the warehouse

```yaml
- name: trip_id
  tests:
      - unique
      - not_null
- name: route_id
  tests:
      - relationships:
            to: ref('dim_routes')
            field: route_id
```

Custom SQL test: `dbt/tests/assert_no_negative_delays.sql`

---

## Folder Structure

```
quito-transport-platform/
├── airflow/
│   └── dags/
│       ├── ingest_gtfs.py          # DAG: pull GTFS feed → S3 raw
│       ├── ingest_weather.py       # DAG: pull OpenWeather → S3 raw
│       └── full_pipeline.py        # DAG: end-to-end orchestration
├── ingestion/
│   ├── gtfs_loader.py              # Fetch + convert GTFS to Parquet
│   └── weather_loader.py           # Fetch + normalise OpenWeather
├── processing/
│   └── glue_jobs/
│       ├── clean_trips.py          # PySpark: clean raw GTFS trips
│       └── enrich_weather.py       # PySpark: join trips + weather
├── quality/
│   └── expectations/
│       ├── trips_suite.json        # GE expectations for trip data
│       └── weather_suite.json      # GE expectations for weather data
├── dbt/
│   ├── models/
│   │   ├── staging/                # stg_trips, stg_stops, stg_weather
│   │   └── marts/                  # fct_trips, dim_routes, dim_stops, dim_weather_conditions
│   ├── tests/
│   │   └── assert_no_negative_delays.sql
│   └── dbt_project.yml
├── metabase/
│   └── docker-compose.yml          # Local Metabase for development
├── infrastructure/
│   └── iam_policies.json           # Least-privilege IAM policy definitions
├── .github/
│   └── workflows/
│       └── ci.yml                  # GitHub Actions: test → dbt test → GE suite
├── DATA_GOVERNANCE.md              # Ownership, retention, PII, SLA, access control
├── requirements.txt
├── .gitignore
├── LICENSE
└── README.md
```

---

## Running Locally

### Prerequisites

- Python 3.11+
- Docker + Docker Compose (for Metabase)
- AWS credentials configured (`~/.aws/credentials` or environment variables)
- OpenWeather API key stored in AWS Secrets Manager or `.env`

### Setup

```bash
git clone https://github.com/bernabecj/quito-transport-platform.git
cd quito-transport-platform
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Run ingestion locally

```bash
python ingestion/gtfs_loader.py
python ingestion/weather_loader.py
```

### Run unit tests

```bash
pytest tests/ -v --cov=ingestion --cov=processing
```

### Run dbt models

```bash
cd dbt
dbt deps
dbt run
dbt test
```

### Running dbt docs

```bash
dbt docs generate
dbt docs serve   # opens at http://localhost:8080
```

### Start Metabase (local)

```bash
cd metabase
MB_DB_PASSWORD=secret docker compose up -d
# Open http://localhost:3000
```

---

## CI/CD

Every push runs the full CI pipeline via GitHub Actions (`.github/workflows/ci.yml`):

1. `pytest` — unit tests
2. `dbt test` — warehouse integrity checks
3. Great Expectations checkpoint — raw data quality suite

Merging to `main` also triggers a deploy of updated dbt models to Redshift.

---

## Data Governance

See [DATA_GOVERNANCE.md](DATA_GOVERNANCE.md) for:

- Data ownership per domain
- Retention policy (raw: 90 days, warehouse: 2 years)
- PII policy (no personal data — documented explicitly)
- SLA (pipeline completes by 06:00 Quito time)
- Access control matrix (IAM roles + Redshift roles)
- Partitioning strategy

---

## Live Links

| Resource                  | URL                              |
| ------------------------- | -------------------------------- |
| Metabase dashboard        | _Coming soon_                    |
| dbt docs site             | _Coming soon_                    |
| Great Expectations report | _Coming soon_ (S3 public bucket) |

---

## License

[MIT](LICENSE)
