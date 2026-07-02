# Project Plan

This is the original planning doc for the Quito Transport Platform — the pitch, the
day-by-day build plan, and the interview narrative. For the as-built architecture and
setup instructions, see the [README](../README.md).

**The idea:** Quito's public transport (Trole, Ecovía, Metrobús) is notoriously unpredictable. Build a platform that ingests real transit and weather data, processes it, and surfaces insights like delay patterns, peak hours, and worst routes — publicly accessible so citizens actually use it.

**Why this works for Thoughtworks:**

- Real social impact (Quito citizens — a consulting story that writes itself)
- Covers almost every concept in the job description
- Data mesh-friendly architecture you can explain in interviews
- Live demo-able during technical rounds

---

## Stack (AWS + Thoughtworks-aligned)

| Layer          | Tool                   | AWS Service                                    |
| -------------- | ---------------------- | ----------------------------------------------- |
| Ingestion      | Python + REST APIs     | **AWS Lambda** (serverless ingestion triggers) |
| Raw Storage    | Parquet files          | **AWS S3** (data lake, raw + processed zones)  |
| Orchestration  | **Apache Airflow**     | **Amazon MWAA** or self-hosted on EC2          |
| Processing     | **PySpark**            | **AWS Glue** or EMR Serverless                 |
| Transformation | **dbt**                | Runs against Redshift                          |
| Warehouse      | SQL analytics          | **Amazon Redshift** (free trial)               |
| Data Quality   | **Great Expectations** | Runs in Glue/Airflow, results to S3            |
| Data Catalog   | Schema & lineage       | **AWS Glue Data Catalog**                      |
| Visualization  | **Metabase**           | Deployed on **EC2** (t2.micro free tier)       |
| CI/CD          | GitHub Actions         | Deploys to AWS automatically                   |
| Secrets        | API keys & credentials | **AWS Secrets Manager**                        |
| Monitoring     | Pipeline observability | **AWS CloudWatch**                             |

---

## Data Sources (free, public)

- **Quito GTFS feed** — static transit schedules (routes, stops, trips)
- **OpenWeather API** — free tier, current + forecast weather by coordinates
- **OpenStreetMap** — geolocation enrichment for stops and routes

---

## Architecture Overview

```
APIs (GTFS + Weather)
        ↓
   AWS Lambda (trigger)
        ↓
   S3 Raw Zone (Parquet)
        ↓
   Airflow DAG (orchestration)
        ↓
   AWS Glue / PySpark (transformation)
        ↓
   Great Expectations (data quality gate)
        ↓
   S3 Processed Zone → Redshift (warehouse)
        ↓
   dbt (modeling + data governance)
        ↓
   Metabase on EC2 (dashboards + maps)
```

---

## Data Governance Layer

| Concept                 | Tool                       | What it does                                                    |
| ------------------------ | --------------------------- | ---------------------------------------------------------------- |
| **Data Catalog**        | AWS Glue Data Catalog      | Registers all datasets, schemas, and metadata automatically     |
| **Data Lineage**        | dbt docs                   | Shows exactly how each model was built, from source to dashboard |
| **Data Quality**        | Great Expectations         | Validates every batch before it reaches the warehouse           |
| **Schema contracts**    | dbt sources + tests        | Enforces expected columns, types, and relationships             |
| **Access control**      | AWS IAM + Redshift roles   | Who can read/write what, documented                              |
| **Data dictionary**     | dbt descriptions           | Every table and column has a human-readable description          |
| **Observability**       | CloudWatch + Airflow logs  | Pipeline health, failures, and alerting                          |
| **Partitioning strategy** | S3 + Redshift             | Data partitioned by date and route for efficiency                |

---

## Testing Strategy

Testing happens at **three layers** — each catching a different class of problem.

### 1. PyTest — Unit tests for Python logic (TDD approach)

Write the test first, then the function.

```python
def test_calculate_delay_returns_minutes():
    scheduled = datetime(2024, 1, 1, 8, 0)
    actual = datetime(2024, 1, 1, 8, 15)
    assert calculate_delay(scheduled, actual) == 15

def test_calculate_delay_with_no_delay():
    scheduled = datetime(2024, 1, 1, 8, 0)
    actual = datetime(2024, 1, 1, 8, 0)
    assert calculate_delay(scheduled, actual) == 0
```

### 2. Great Expectations — Data quality gate at ingestion

Runs before data enters the warehouse. If it fails, the Airflow DAG stops.

```python
df.expect_column_to_exist("trip_id")
df.expect_column_values_to_not_be_null("stop_id")
df.expect_column_values_to_be_between("duration_minutes", min_value=1, max_value=180)
```

### 3. dbt Tests — Data integrity in the warehouse

```yaml
models:
  - name: fct_trips
    columns:
      - name: trip_id
        tests:
          - unique
          - not_null
      - name: route_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_routes')
              field: route_id
```

Custom dbt test for business rules:

```sql
-- tests/assert_no_negative_delays.sql
select trip_id
from {{ ref('fct_trips') }}
where delay_minutes < 0
```

### How they fit together

```
Raw data arrives (S3)
        ↓
Great Expectations  ← "Is the raw data valid?"
        ↓ (pass)
PySpark transforms
        ↓
PyTest already validated  ← "Is my logic correct?"
the transformation functions
        ↓
dbt loads to Redshift
        ↓
dbt tests run  ← "Is the data in the warehouse correct?"
        ↓ (all pass)
Metabase dashboard updates
```

---

## 3-Week Plan

### Week 1 — Foundation & Ingestion

**Days 1–2: Project setup**

- Create S3 buckets: `raw/`, `processed/`, `quality-reports/`
- Set up AWS IAM roles and Secrets Manager for API keys
- Initialize GitHub repo with folder structure and GitHub Actions skeleton

**Days 3–4: Ingestion pipeline**

- Python script to pull GTFS static data (routes, stops, trips) and OpenWeather API
- Lambda function to trigger ingestion on schedule
- Store as Parquet in S3 raw zone, partitioned by `year/month/day`

**Days 5–7: Airflow orchestration**

- Build Airflow DAGs that chain: ingest → validate → transform → load
- Add failure alerting via CloudWatch
- DAG runs daily automatically

---

### Week 2 — Processing, Quality & Warehouse

**Days 8–9: PySpark processing with AWS Glue**

- Clean and enrich raw data: join weather + transit + geolocation
- Compute derived metrics: estimated delays by route, trip counts by hour, weather correlation
- Output to S3 processed zone as Parquet

**Days 10–11: Great Expectations — data quality gate**

- Define expectations per dataset:
  - No null stop IDs
  - Trip durations within realistic range (5–180 min)
  - Weather records have valid coordinates
  - Row counts above minimum threshold per day
- Generate HTML quality reports saved to S3
- Airflow DAG **stops and alerts** if quality checks fail — data never reaches warehouse dirty

**Days 12–13: Redshift + dbt**

- Load processed data from S3 into Redshift via COPY command
- Build dbt models:
  - `stg_trips`, `stg_stops`, `stg_weather` (staging)
  - `fct_trips` (fact table)
  - `dim_routes`, `dim_stops`, `dim_weather_conditions` (dimensions — star schema)
- Add dbt tests: `not_null`, `unique`, `accepted_values`, `relationships`

**Day 14: dbt docs + data catalog**

- Write descriptions for every model and column (data dictionary)
- Generate dbt docs site (`dbt docs generate`)
- Register all datasets in AWS Glue Data Catalog with proper metadata

---

### Week 3 — Visualization, Governance Polish & Deployment

**Days 15–16: Metabase dashboards**

- Deploy Metabase on EC2 (t2.micro), connect to Redshift
- Build dashboards:
  - Map of Quito with delay heatmap by stop
  - Peak hour traffic by route
  - Weather vs delay correlation chart
  - Top 10 most delayed routes
  - Daily/weekly trend lines

**Days 17–18: CI/CD with GitHub Actions**

- On every push: run dbt tests + Great Expectations suite automatically
- On merge to main: deploy updated dbt models to Redshift
- Pipeline badge in README (green = passing)

**Days 19–20: Data governance documentation**

- Write a `DATA_GOVERNANCE.md`:
  - Data ownership per domain
  - Data retention policy (how long raw data is kept in S3)
  - PII policy (no personal data in this project — document that explicitly)
  - SLA definition (pipeline must complete by 6am daily)
- Draw the full architecture diagram (use Excalidraw or draw.io)

**Day 21: Polish + README**

- Clean, professional README with:
  - Architecture diagram
  - How to run locally
  - Link to live Metabase dashboard
  - Link to dbt docs
  - Screenshot of Great Expectations quality report
- Push everything, make repo public

---

## Folder Structure

```
quito-transport-platform/
├── airflow/
│   └── dags/
│       ├── ingest_gtfs.py
│       ├── ingest_weather.py
│       └── full_pipeline.py
├── ingestion/
│   ├── gtfs_loader.py
│   └── weather_loader.py
├── processing/
│   └── glue_jobs/
│       ├── clean_trips.py
│       └── enrich_weather.py
├── quality/
│   └── expectations/
│       ├── trips_suite.json
│       └── weather_suite.json
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   ├── tests/
│   └── dbt_project.yml
├── metabase/
│   └── docker-compose.yml
├── infrastructure/
│   └── iam_policies.json
├── DATA_GOVERNANCE.md
├── .github/workflows/ci.yml
└── README.md
```

---

## What to say in the interview

> "I built a public transport analytics platform for Quito citizens. It ingests GTFS transit schedules and weather data, processes them with PySpark on AWS Glue, validates every batch with Great Expectations before it reaches the warehouse, transforms with dbt on Redshift, and surfaces insights in Metabase — delay heatmaps, peak hours, weather correlations. I also implemented a governance layer: AWS Glue Data Catalog for schema registration, dbt docs as a living data dictionary, IAM for access control, and a documented data retention and PII policy. Everything ships through GitHub Actions. I chose Quito's transport system because it affects thousands of people daily — especially those who depend on it most."
