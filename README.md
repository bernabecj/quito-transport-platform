# Quito Transport Platform

A public data engineering platform that ingests Quito's transit network, corridor travel times and weather, processes it with an AWS-native stack, and surfaces network coverage, peak hours and the city's least predictable routes through a live dashboard.

![CI](https://github.com/bernabecj/quito-transport-platform/actions/workflows/ci.yml/badge.svg)

See [docs/PROJECT_PLAN.md](docs/PROJECT_PLAN.md) for the build plan and the interview narrative behind this project.

---

## Why this exists

Quito's public transport serves hundreds of thousands of daily commuters across a city stretched along a high-altitude valley. Which neighbourhoods the network actually reaches, where stops cluster or thin out, and which corridors carry redundant routes are all questions nobody publishes an answer to. This platform builds an open, observable pipeline that turns the city's transit topology into answerable questions — coverage maps, stop-density analysis, and route overlap by corridor.

### A note on data availability

This project originally aimed to analyse **delays** from a GTFS schedule feed. That turned out to be impossible, and the reason is worth stating plainly because it shaped the whole design:

- **Quito publishes no GTFS feed.** Verified against [transit.land](https://www.transit.land/)'s registry (786 feeds) and MobilityData's [Mobility Database](https://mobilitydatabase.org/) catalogue (3,462 feeds) — neither indexes a single Ecuadorian feed. The URL this repo originally pointed at returns 404 and was never captured by the Internet Archive.
- **Delay analysis needs GTFS-Realtime anyway.** Measuring a delay means comparing actual against scheduled departure. Static GTFS carries only the schedule half, so even a working static feed would not have supported the original metric.

The network topology comes instead from **OpenStreetMap**, which maps Quito's transit in genuine detail — 515 route relations covering Trole, Ecovía, Metrobús and the private cooperatives, with 93–99% completeness on operator, ref, origin and destination. OSM carries no timetables, so this platform analyses **network structure**, not schedule adherence. Every metric below is derived from data that actually exists.

---

## Architecture

```
APIs (OpenStreetMap Overpass + OpenWeather)
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
| Warehouse      | SQL analytics          | Amazon Redshift (separate AWS account) |
| Data Quality   | Great Expectations     | Runs in Glue/Airflow, reports to S3 |
| Data Catalog   | Schema & lineage       | AWS Glue Data Catalog               |
| Visualization  | Metabase               | EC2 t2.micro (free tier)            |
| CI/CD          | GitHub Actions         | Deploys to AWS automatically        |
| Secrets        | API keys & credentials | AWS Secrets Manager                 |
| Monitoring     | Pipeline observability | AWS CloudWatch                      |

---

## Data Sources

| Source                    | What it provides                                               | Cost |
| ------------------------- | -------------------------------------------------------------- | ---- |
| OpenStreetMap (Overpass)  | Transit routes, stops, stop ordering, operator                | Free |
| Mapbox Directions         | Observed travel time and congestion per BRT corridor           | Free tier (100k/mo) |
| OpenWeather API           | Current + 5-day forecast for Quito                             | Free tier |

The Overpass API needs no credentials and answers in seconds. It does rate-limit and shed load under contention (HTTP 429/504), so [`ingestion/network_loader.py`](ingestion/network_loader.py) cycles through two endpoints with exponential backoff.

Mapbox is the only one of these that is metered. Coverage for Quito was verified before committing to it — see [`scripts/verify_mapbox_traffic.py`](scripts/verify_mapbox_traffic.py), which confirmed real congestion data on 49-71% of segments across the trunk corridors rather than the silent free-flow fallback Mapbox returns for uncovered regions. **Traffic sampling is disabled by default**; see [Corridor travel time](#corridor-travel-time-disabled-by-default).

### Why ingest daily if the network is near-static?

Each run writes a dated snapshot. Over time those snapshots become a longitudinal record of how the network changes — routes added or withdrawn, stops relocated or newly mapped. That change history is itself a dataset, and it is the reason the raw zone is partitioned by `year/month/day`.

---

## Datasets

The ingestion Lambda produces three tables per snapshot:

| Dataset       | Grain                  | Key columns                                                       |
| ------------- | ---------------------- | ----------------------------------------------------------------- |
| `routes`      | one row per route      | `route_id`, `route_ref`, `route_name`, `route_type`, `operator`, `origin`, `destination` |
| `stops`       | one row per stop       | `stop_id`, `stop_name`, `latitude`, `longitude`, `stop_type`, `shelter` |
| `route_stops` | one row per route–stop | `route_id`, `stop_id`, `stop_sequence`, `member_role`             |

`stop_sequence` is the stop's position along the route. It is **not** a timetable ordering — there are no times in this data.

The traffic sampler adds a fourth, written once per run rather than per day:

| Dataset                | Grain                        | Key columns                                                     |
| ---------------------- | ---------------------------- | --------------------------------------------------------------- |
| `corridor_travel_time` | one row per corridor per sample | `route_id`, `sampled_at`, `local_hour`, `duration_seconds`, `mean_speed_kmh`, `congestion_*` |

Real samples of every dataset are committed under [`data/samples/`](data/samples/) so the Glue and dbt layers can be built without calling any API.

---

## Data Model (Star Schema)

```
fct_route_stops                 fct_corridor_travel_time      fct_daily_weather
 ├── route_id → dim_routes       ├── route_id → dim_routes      └── condition_id
 ├── stop_id  → dim_stops        └── sampled_at                      → dim_weather_conditions
 └── snapshot_date
```

Analytical questions this supports:

- **Coverage** — which parts of Quito are within walking distance of a stop, and which are not?
- **Service gaps** — where does stop density thin out relative to population?
- **Route overlap** — which corridors carry many redundant routes while others carry one?
- **Network change** — what has been added or withdrawn since the first snapshot?
- **Peak hours** — when does a corridor's travel time rise above its own daily median?
- **Worst corridors** — which routes show the largest peak-to-off-peak spread?
- **Unpredictability** — which corridors vary most in travel time, which is closer to what riders actually experience than an average?

The last three need accumulated samples. A single snapshot cannot express them, because every one compares a corridor against its own history.

All models and column descriptions live in the dbt docs site (see [Running dbt docs](#running-dbt-docs)).

### Known data limitations

OpenStreetMap is community-mapped, so coverage is uneven. Measured against a real extract on 2026-08-19:

| Observation | Count | Consequence |
| ----------- | ----- | ----------- |
| Routes with at least one stop mapped | 119 / 515 | Stop-level analysis covers ~23% of routes; the rest have geometry but no stop nodes |
| Stops carrying a `shelter` tag | 7 / 485 | Too sparse to support any shelter or weather-exposure metric |
| Routes with an `operator` tag | 93% | Operator coverage analysis is sound |
| Trunk corridors with usable stop order | 14 / 18 | Four Ecovía relations list stops non-geographically (consecutive "stops" up to 15.9 km apart), so they are excluded from travel-time sampling |

Coverage, stop density and route overlap are well supported. Anything depending on `shelter` is not, and is deliberately excluded from the marts rather than reported on thin data.

---

### Corridor travel time — collection window

Sampling is **live from 2026-08-20 for two weeks**. An EventBridge rule
(`quito-transport-traffic-6h`) invokes `quito-traffic-ingestion` every six hours, writing one
Parquet snapshot per run to `raw/traffic/year=/month=/day=/hour=`.

At 00/06/12/18 UTC those land at **19:00, 01:00, 07:00 and 13:00 Quito time** — evening peak,
overnight baseline, morning peak and midday. Four points per corridor per day, enough to
compare those times against each other, though not to locate exactly when a peak occurs.

Why a fixed window rather than always-on: Mapbox is the one metered source here. Fourteen
corridors every six hours for two weeks is about **780 requests against a 100,000/month free
tier**, and the window stops before becoming an open-ended dependency.

**To stop collection**, set `state = "DISABLED"` on `aws_cloudwatch_event_rule.traffic_schedule`
in [`infrastructure/terraform/lambda.tf`](infrastructure/terraform/lambda.tf) and apply. That
halts invocation entirely. `TRAFFIC_SAMPLING_ENABLED` on the function is a second, independent
guard: with it set to `"false"` the handler returns `{"skipped": true}` even if something does
invoke it. The accumulated history stays in S3 either way.

The `sample_traffic` Airflow DAG stays **paused on purpose**. Local Airflow only runs while
the dev container is up, so EventBridge — which runs regardless — owns the schedule.
Unpausing the DAG would double the spend to write the same S3 key twice.

**This history cannot be backfilled.** Mapbox reports current conditions only; there is no
endpoint for past traffic (that is TomTom Traffic Stats, which does not cover Ecuador). Every
unsampled hour is permanently lost, which is why the window runs unattended in AWS.

---

## Testing Strategy

Testing happens at three layers:

### 1. PyTest — unit tests for Python transformation logic (TDD)

```python
def test_route_stops_are_sequenced_from_zero():
    relation = {"id": 1, "members": [
        {"type": "node", "ref": 10, "role": "stop"},
        {"type": "way",  "ref": 99, "role": ""},
        {"type": "node", "ref": 11, "role": "platform"},
    ]}
    records = _route_stop_records(relation, "2026-01-01T00:00:00Z")
    assert [r["stop_sequence"] for r in records] == [0, 1]
```

Run: `pytest tests/ -v`

### 2. Great Expectations — data quality gate at ingestion

Runs before any data enters the warehouse. The Airflow DAG halts and alerts if any expectation fails.

```python
df.expect_column_to_exist("route_id")
df.expect_column_values_to_not_be_null("stop_id")
df.expect_column_values_to_be_between("latitude", min_value=-0.45, max_value=0.05)
df.expect_column_values_to_be_between("longitude", min_value=-78.65, max_value=-78.30)
```

### 3. dbt Tests — data integrity in the warehouse

```yaml
- name: stop_id
  tests:
      - unique
      - not_null
- name: route_id
  tests:
      - relationships:
            to: ref('dim_routes')
            field: route_id
```

Custom SQL test: `dbt/tests/assert_stops_within_quito_bbox.sql`

---

## Folder Structure

```
quito-transport-platform/
├── airflow/
│   └── dags/
│       ├── ingest_network.py       # DAG: pull OSM network → S3 raw
│       ├── ingest_weather.py       # DAG: pull OpenWeather → S3 raw
│       ├── sample_traffic.py       # DAG: hourly Mapbox sampling (paused)
│       └── full_pipeline.py        # DAG: end-to-end orchestration
├── ingestion/
│   ├── network_loader.py           # Query Overpass, normalise to Parquet
│   ├── traffic_loader.py           # Sample corridor travel time (off by default)
│   └── weather_loader.py           # Fetch + normalise OpenWeather
├── data/
│   └── samples/                    # Real captured data for offline development
├── scripts/
│   └── verify_mapbox_traffic.py    # Go/no-go check on Quito traffic coverage
├── processing/
│   └── glue_jobs/
│       ├── clean_network.py        # PySpark: clean raw network snapshots
│       └── enrich_weather.py       # PySpark: join network + weather
├── quality/
│   └── expectations/
│       ├── network_suite.json      # GE expectations for network data
│       └── weather_suite.json      # GE expectations for weather data
├── dbt/
│   ├── models/
│   │   ├── staging/                # stg_routes, stg_stops, stg_weather
│   │   └── marts/                  # fct_route_stops, dim_routes, dim_stops, dim_weather_conditions
│   ├── tests/
│   └── dbt_project.yml
├── metabase/
│   └── docker-compose.yml          # Local Metabase for development
├── infrastructure/
│   ├── terraform/                  # Lambda, Glue, Redshift, IAM
│   └── scripts/                    # Container image build + push
├── docs/
│   └── PROJECT_PLAN.md
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
- Docker + Docker Compose (for Metabase and local Airflow)
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
python -m ingestion.network_loader   # queries Overpass, writes to S3
python -m ingestion.weather_loader
```

To inspect the network data without writing to S3:

```bash
python -c "
from ingestion.network_loader import fetch_network, parse_network
from datetime import datetime, timezone
dfs = parse_network(fetch_network(), datetime.now(timezone.utc))
for name, df in dfs.items():
    print(name, len(df))
"
```

### Run local Airflow

```bash
cd airflow && astro dev start        # UI at http://localhost:8080
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

OpenStreetMap data is licensed under the [ODbL](https://www.openstreetmap.org/copyright) and requires attribution in any published derivative.

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
