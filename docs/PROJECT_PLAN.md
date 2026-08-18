# Project Plan

This is the planning doc for the Quito Transport Platform — the pitch, the day-by-day
build plan, and the interview narrative. For the as-built architecture and setup
instructions, see the [README](../README.md).

**The idea:** Quito's public transport (Trole, Ecovía, Metrobús, plus dozens of private cooperatives) sprawls across a high-altitude valley, and nobody publishes an answer to basic questions about it — where it reaches, where it doesn't, and how much of it leaves riders standing in the rain. Build a platform that ingests the city's transit network and weather data, processes it, and surfaces coverage, service gaps and rider exposure — publicly accessible so citizens actually use it.

**Why this works for Thoughtworks:**

- Real social impact (Quito citizens — a consulting story that writes itself)
- Covers almost every concept in the job description
- Data mesh-friendly architecture you can explain in interviews
- Live demo-able during technical rounds

---

## Scope decision: why network analysis, not delay analysis

This plan originally targeted **delay analysis** from a GTFS schedule feed. That target was not achievable, and the investigation behind the pivot is part of the project's story rather than an embarrassment to hide.

**What was checked:**

| Source | Result |
| ------ | ------ |
| `epmmop.gob.ec` GTFS URL (original) | 404; never captured by the Internet Archive; 7/7 pipeline runs failed, never once succeeded |
| [transit.land](https://www.transit.land/) registry | 786 feeds worldwide, **zero** from Ecuador |
| [Mobility Database](https://mobilitydatabase.org/) catalogue | 3,462 feeds worldwide, **zero** from Ecuador |
| EPMMOP / AMT / Secretaría de Movilidad portals | No open-data or GTFS endpoints published |
| GitHub, national open-data portal, city geoportal | No Quito GTFS mirror |

**Two conclusions:**

1. **Quito publishes no GTFS feed.** Not a moved URL — the data is not public in that format.
2. **Delay analysis needed GTFS-Realtime regardless.** A delay is actual departure minus scheduled departure. Static GTFS carries only the scheduled half. The original metric was unachievable even with a working feed, so the pivot corrects a design flaw rather than merely working around a dead link.

**The replacement source** is OpenStreetMap via the Overpass API: 515 route relations across Quito, with 93–99% completeness on `operator`, `ref`, `from` and `to`, covering Trole, Ecovía, Metrobús and the private cooperatives. OSM carries **no timetables** — verified directly: 0 of 515 routes have `interval`, `opening_hours`, `frequency` or `departures` tags.

So the platform analyses **network structure**, not schedule adherence. Every metric it produces comes from data that demonstrably exists.

**Interview value:** this is the strongest part of the narrative. Verifying a data source before building on it, and correcting a metric that was impossible by construction, is exactly the judgement a consultancy is hiring for.

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

- **OpenStreetMap via Overpass API** — transit routes, stops, stop ordering, operator, shelter tags. No credentials required. Rate-limits under load (HTTP 429/504), so the loader cycles two endpoints with exponential backoff.
- **OpenWeather API** — free tier, current + forecast weather by coordinates.

### Why ingest daily if the network is near-static?

Each run writes a dated snapshot. Accumulated, those snapshots become a longitudinal record of network change — routes added or withdrawn, stops relocated, shelters appearing. The change history is itself a dataset, and it justifies partitioning the raw zone by `year/month/day`.

---

## Architecture Overview

```
APIs (OpenStreetMap Overpass + Weather)
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

## Analytical Questions

The marts are built to answer these, all from data that exists:

| Question | Data needed |
| -------- | ----------- |
| Which parts of Quito are within walking distance of a stop? | `stops` coordinates |
| Where does stop density thin out? | `stops` + `route_stops` |
| Which corridors carry many redundant routes, and which carry one? | `route_stops` overlap |
| What share of stops offer shelter, and which routes expose riders most? | `stops.shelter` + `route_stops` |
| Which operators cover which parts of the city? | `routes.operator` |
| What has been added or withdrawn since the first snapshot? | dated partitions |

**Planned enhancement — weather joined geographically.** Weather is currently fetched for a single city-centre coordinate. Quito's valley has real microclimate variation, so fetching several points and joining them to stops by proximity would turn "which stops lack shelter" into "which unsheltered stops sit in the rainiest zones". That is a change to `weather_loader.py`, not to the network pipeline.

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
| **Partitioning strategy** | S3 + Redshift             | Data partitioned by snapshot date for efficiency                 |
| **Source licensing**    | ODbL attribution           | OSM data requires attribution in published derivatives           |

---

## Testing Strategy

Testing happens at **three layers** — each catching a different class of problem.

### 1. PyTest — Unit tests for Python logic (TDD approach)

Write the test first, then the function.

```python
def test_route_stops_are_sequenced_from_zero():
    relation = {"id": 1, "members": [
        {"type": "node", "ref": 10, "role": "stop"},
        {"type": "way",  "ref": 99, "role": ""},
        {"type": "node", "ref": 11, "role": "platform"},
    ]}
    records = _route_stop_records(relation, "2026-01-01T00:00:00Z")
    assert [r["stop_sequence"] for r in records] == [0, 1]


def test_ways_are_excluded_from_stops():
    relation = {"id": 1, "members": [{"type": "way", "ref": 99, "role": ""}]}
    assert _route_stop_records(relation, "2026-01-01T00:00:00Z") == []
```

### 2. Great Expectations — Data quality gate at ingestion

Runs before data enters the warehouse. If it fails, the Airflow DAG stops.

```python
df.expect_column_to_exist("route_id")
df.expect_column_values_to_not_be_null("stop_id")
df.expect_column_values_to_be_between("latitude", min_value=-0.45, max_value=0.05)
df.expect_column_values_to_be_between("longitude", min_value=-78.65, max_value=-78.30)
df.expect_table_row_count_to_be_between("routes", min_value=400)
```

That last one matters: OSM is community-edited, so a sudden collapse in route count means a bad extract, not a city that lost its buses.

### 3. dbt Tests — Data integrity in the warehouse

```yaml
models:
  - name: fct_route_stops
    columns:
      - name: stop_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_stops')
              field: stop_id
      - name: route_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_routes')
              field: route_id
```

Custom dbt test for business rules:

```sql
-- tests/assert_stops_within_quito_bbox.sql
select stop_id
from {{ ref('dim_stops') }}
where latitude not between -0.45 and 0.05
   or longitude not between -78.65 and -78.30
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

- Python module to query the Overpass API for Quito's route relations and stop nodes, plus the OpenWeather API
- Normalise into `routes`, `stops`, `route_stops`
- Lambda (container image — pandas/pyarrow exceed the zip limit) triggered on schedule
- Store as Parquet in S3 raw zone, partitioned by `year/month/day`

**Days 5–7: Airflow orchestration**

- Build Airflow DAGs that chain: ingest → validate → transform → load
- Add failure alerting via CloudWatch
- DAG runs daily automatically

---

### Week 2 — Processing, Quality & Warehouse

**Days 8–9: PySpark processing with AWS Glue**

- Clean and enrich raw snapshots: deduplicate stops shared across routes, validate coordinates, derive zone from lat/lon
- Compute derived metrics: stop density per zone, route overlap per corridor, shelter coverage per route
- Output to S3 processed zone as Parquet

**Days 10–11: Great Expectations — data quality gate**

- Define expectations per dataset:
  - No null stop or route IDs
  - Coordinates inside the Quito bounding box
  - Route count above a minimum threshold (guards against a truncated Overpass response)
  - Every `route_stops` row resolves to a known stop
- Generate HTML quality reports saved to S3
- Airflow DAG **stops and alerts** if quality checks fail — data never reaches warehouse dirty

**Days 12–13: Redshift + dbt**

- Load processed data from S3 into Redshift via COPY command
- Build dbt models:
  - `stg_routes`, `stg_stops`, `stg_weather` (staging)
  - `fct_route_stops` (fact — route × stop × snapshot date)
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
  - Map of Quito with stop coverage and walking-distance gaps
  - Stop density heatmap by neighbourhood
  - Shelter coverage by route — the "you will get wet here" map
  - Route overlap by corridor
  - Operator coverage comparison
  - Network change over time

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
  - ODbL attribution requirement for OSM-derived outputs
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

## What to say in the interview

> "I built a public transport analytics platform for Quito citizens. It ingests the city's transit network from OpenStreetMap and weather from OpenWeather, processes them with PySpark on AWS Glue, validates every batch with Great Expectations before it reaches the warehouse, transforms with dbt on Redshift, and surfaces insights in Metabase — coverage maps, service gaps, and which routes leave riders standing in the rain.
>
> The part I'd highlight is how I chose the data source. I started out planning delay analysis from a GTFS feed, but the feed 404'd. Instead of assuming the URL had moved, I checked transit.land and the Mobility Database — 786 and 3,462 feeds respectively, neither with a single Ecuadorian entry. Quito simply doesn't publish GTFS. Digging further, I realised delay analysis needed GTFS-Realtime anyway, since a delay is actual minus scheduled and static GTFS only carries the scheduled half — so the original metric was impossible by construction, not just blocked by a dead link.
>
> So I pivoted to OpenStreetMap, which has 515 well-tagged Quito routes, and rebuilt the marts around questions that data can actually answer. I verified OSM had no timetables before committing — 0 of 515 routes carry frequency tags — so I didn't repeat the same mistake. The governance layer covers Glue Data Catalog for schema registration, dbt docs as a living data dictionary, IAM for access control, and documented retention, PII and ODbL attribution policies. Everything ships through GitHub Actions."
