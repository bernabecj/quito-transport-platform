# Quito Transport Platform

A public data engineering platform that ingests Quito's transit network, corridor travel times and weather, builds a star-schema gold layer in PySpark, and surfaces network coverage, peak hours and the city's least predictable routes through a live dashboard.

![CI](https://github.com/bernabecj/quito-transport-platform/actions/workflows/ci.yml/badge.svg)

See [docs/PROJECT_PLAN.md](docs/PROJECT_PLAN.md) for the build plan and the roadmap beyond it, including a real-time streaming extension.

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
     Great Expectations ─── data quality gate on the raw samples
              │  (quality pass)
   AWS Glue / PySpark ─── processing/gold: raw → star schema + marts, then verify
              │
     S3 Gold Zone ─── one Parquet folder per table
              │
  Amazon Redshift (COPY, or Spectrum over the gold prefix)
              │
  Metabase on EC2 (dashboards + maps)
```

There is no separate "silver" zone: the exploration ([`notebooks/01_exploring.ipynb`](notebooks/01_exploring.ipynb)) found no nulls and no corrupted values in the raw data, so the gold build reads raw directly. dbt was set aside for the same reason — the gold tables *are* the warehouse model, and Redshift loads them as-is. The reasoning and the objectives are in [`docs/ANALYSIS_PLAN.md`](docs/ANALYSIS_PLAN.md); the model and how to run it in [`docs/GOLD_LAYER.md`](docs/GOLD_LAYER.md).

### Stack

| Layer          | Tool                   | AWS Service                         |
| -------------- | ---------------------- | ----------------------------------- |
| Ingestion      | Python + REST APIs     | AWS Lambda                          |
| Raw Storage    | Parquet files          | AWS S3 (raw + processed zones)      |
| Orchestration  | Apache Airflow         | Amazon MWAA                         |
| Processing     | PySpark (`processing/gold/`) | AWS Glue / EMR Serverless — raw → gold, plus `verify` |
| Transformation | — (dbt set aside; the gold layer is the model) | |
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

Real samples of every dataset are committed under [`data/samples/`](data/samples/) so the gold layer can be built and tested without calling any API (`python -m processing.gold.build --layout samples`).

---

## Data Model (Star Schema)

Built by `python -m processing.gold.build` and documented table by table in [`docs/GOLD_LAYER.md`](docs/GOLD_LAYER.md).

```
                         dim_date                     dim_weather_condition
                      (date_key)                         (condition_key)
                          ▲  ▲                                ▲
       dim_route ◄──── fct_corridor_travel_time ──────► fct_weather ───────┘
      (route_id)         (route_id, utc_ts)            (weather_ts)
        ▲  ▲  ▲
        │  │  └──────────── fct_route_stop ──────────► dim_stop
        │  │             (route_id, stop_sequence)     (stop_id)
        │  └── agg_corridor_hourly_profile  (route_id, local_hour, day_type)
        └───── agg_corridor_reliability     (route_id)

       fct_network_snapshot  (snapshot_date)
```

`fct_corridor_travel_time` is the hub: one row per corridor per sampled hour, with local time, per-day coverage (`is_complete_day` replaces any hard-coded analysis window) and the nearest 3-hour weather slot denormalised on. The dimensions are built from the union of every daily network snapshot (SCD type 1 with `first_seen_date` / `last_seen_date`), because the snapshots flap — two routes drop out of the Overpass response on scattered days and come back. The two `agg_*` marts hold the answers to the questions below, pre-aggregated, so Metabase reads a row instead of recomputing.

Analytical questions this supports, and what the data has shown so far (one hourly week, 14 corridors — [`docs/ANALYSIS_PLAN.md`](docs/ANALYSIS_PLAN.md) has the pass/fail bars):

- **Coverage** — which parts of Quito are within walking distance of a stop? (`fct_route_stop`, `dim_stop`)
- **Service gaps** — where does stop density thin out? (`dim_stop.route_count`)
- **Route overlap** — which corridors carry many redundant routes? (`fct_route_stop`)
- **Network change** — what has been added or withdrawn since the first snapshot? (`fct_network_snapshot`, `dim_route.last_seen_date`)
- **Peak hours** — travel time sits at 1.47–1.58× the overnight baseline from 06:00 to 19:00, a 13-hour plateau rather than two rush-hour spikes; worst at 17:00. (`agg_corridor_hourly_profile`)
- **Worst corridors** — MetroBus Corredor Central Norte tops every ranking: +84 % duration at its peak. (`agg_corridor_reliability`)
- **Unpredictability** — within-hour variability, ranked under both duration and speed because Mapbox reroutes between calls (up to 20 % distance spread on C6) and a rerouting corridor looks falsely stable on duration. (`agg_corridor_reliability`)

Weekday-vs-weekend and weather effects are in the tables but flagged: two weekend days and one city-centre weather point are not enough for a claim.

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

One suite per raw table under [`quality/expectations/`](quality/expectations/) — three for the network snapshot (`routes`, `stops`, `route_stops`), one for weather, one for the corridor travel-time series. They check the columns each loader emits, null constraints, coordinates inside `QUITO_BBOX`, minimum row counts (a truncated Overpass response returns fewer routes, not an error), that every `route_stops` row resolves to a known stop, strictly positive travel times, and physical ranges on the weather measurements.

The checkpoint `raw_samples_checkpoint` runs all of them against the committed samples, with no credentials and no network:

```bash
great_expectations checkpoint run raw_samples_checkpoint
```

[`tests/test_quality_suites.py`](tests/test_quality_suites.py) pins every threshold that mirrors `ingestion/constants.py` and proves each suite fails on the corruption it exists to catch. Details, including how the Data Context is wired, in [`quality/README.md`](quality/README.md).

### 3. Gold-layer tests and `verify` — data integrity in the warehouse model

Redshift records primary and foreign keys but does not enforce them, so the guarantee that the star schema joins correctly lives in the build itself:

```bash
pytest tests/gold                                           # 44 tests: each transformation on hand-built frames,
                                                            # plus an end-to-end build on data/samples/
python -m processing.gold.verify --gold-root data/local/gold  # 32 checks: every table present, every PK unique,
                                                            # no orphan FK, no traffic hour without weather; exit 0/1
```

The end-to-end test pins the row count of every table, asserts a rebuild is byte-identical (idempotent), and proves `verify` returns 1 when an orphan key is injected. To watch the suite fail on purpose, corrupt the samples with the existing drills — `python scripts/break_samples.py overpass-truncated` then `pytest tests/gold/test_build.py` — and put them back with `python scripts/restore_samples.py`.

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
│   ├── geometry.py                 # Chain OSM ways, project stops onto the road
│   ├── traffic_loader.py           # Sample corridor travel time (off by default)
│   ├── weather_loader.py           # Fetch + normalise OpenWeather
│   └── storage.py                  # Raw-zone backends: S3 or data/local, chosen by STORAGE_BACKEND
├── data/
│   ├── samples/                    # Real captured data for offline development
│   └── local/                      # raw/ written by the loaders with STORAGE_BACKEND=local; gold/ by the build (gitignored)
├── scripts/
│   ├── verify_mapbox_traffic.py    # Go/no-go check on Quito traffic coverage
│   ├── check_corridors.py          # Which trunk corridors get sampled, and why (offline)
│   ├── break_samples.py            # Corrupt the samples so the quality gate fails (drills)
│   └── restore_samples.py          # Put the pristine samples back
├── processing/
│   ├── gold/                       # PySpark: raw zone → star schema + marts (see docs/GOLD_LAYER.md)
│   │   ├── config.py               #   roots, layouts, table names, schema-evolution defaults
│   │   ├── session.py              #   the one SparkSession configuration (UTC)
│   │   ├── io.py                   #   readers that normalise the raw zone; the gold writer
│   │   ├── time.py                 #   utc_ts / local_ts / local_date / date_key
│   │   ├── dimensions.py           #   dim_date, dim_route, dim_stop
│   │   ├── weather.py              #   fct_weather, dim_weather_condition
│   │   ├── facts.py                #   fct_corridor_travel_time, fct_route_stop, fct_network_snapshot
│   │   ├── marts.py                #   agg_corridor_hourly_profile, agg_corridor_reliability
│   │   ├── build.py                #   python -m processing.gold.build
│   │   └── verify.py               #   python -m processing.gold.verify
│   └── glue_jobs/                  # superseded by gold/; kept until the Glue job definition moves
├── gx/
│   └── great_expectations.yml      # GE Data Context: datasources over data/samples, stores in quality/
├── quality/
│   ├── expectations/               # One GE suite per raw table (network x3, weather, traffic)
│   ├── checkpoints/
│   │   └── raw_samples_checkpoint.yml  # Runs every suite; what CI runs
│   └── README.md
├── tests/
│   ├── gold/                       # Gold-layer unit tests + end-to-end build on data/samples/
│   └── test_quality_suites.py      # Suites track constants.py and fail on bad data
├── dbt/                            # set aside: the gold layer replaced it (docs/ANALYSIS_PLAN.md)
├── metabase/
│   └── docker-compose.yml          # Local Metabase for development
├── infrastructure/
│   ├── terraform/                  # Lambda, Glue, Redshift, IAM
│   └── scripts/                    # Container image build + push
├── docs/
│   ├── PROJECT_PLAN.md
│   ├── ANALYSIS_PLAN.md            # objectives, what the data supports, the gold model
│   └── GOLD_LAYER.md               # run the build anywhere; every table and column; Redshift load
├── notebooks/
│   └── 01_exploring.ipynb          # the data-quality record; runs on Databricks or plain PySpark
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
- Java 17 or 21 (JDK) — for the PySpark gold build; the dev container ships it
- Docker + Docker Compose (for Metabase and local Airflow)
- API keys in `.env` for the loaders you want to run (OpenWeather, Mapbox)
- AWS credentials (`~/.aws/credentials` or environment variables) — only to
  write to the real bucket; not needed for local runs, see below

### Setup

```bash
git clone https://github.com/bernabecj/quito-transport-platform.git
cd quito-transport-platform
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt                     # includes pyspark; a JDK must be on PATH
```

### Run ingestion locally

The loaders write the raw zone either to the S3 bucket or to a folder in this
repo. One environment variable chooses, read from `.env`:

```bash
cp .env.example .env                 # STORAGE_BACKEND=local, plus your API keys
python -m ingestion.weather_loader
python -m ingestion.network_loader   # queries Overpass
TRAFFIC_SAMPLING_ENABLED=true python -m ingestion.traffic_loader   # spends Mapbox quota
```

| `STORAGE_BACKEND` | Writes to | Needs AWS |
| ----------------- | --------- | --------- |
| `local`           | `data/local/` (gitignored) | no |
| `s3` or unset     | `s3://quito-transport-platform/` — what the Lambdas do | yes |

Any other value is an error, so a typo cannot send data to S3 by accident.

`data/local/` mirrors the bucket exactly, partition folders included, so
anything built against the S3 layout works on it unchanged:

```
data/local/raw/weather/year=2026/month=10/day=13/weather.parquet
data/local/raw/network/year=2026/month=10/day=13/{routes,stops,route_stops}.parquet
data/local/raw/traffic/year=2026/month=10/day=13/hour=16/corridor_travel_time.parquet
```

Same semantics as S3 too: a rerun in the same partition (UTC day, or UTC hour
for traffic) overwrites that file; a later day or hour adds a new one next to
it. Note the partitions are UTC — a run at 20:00 Quito time lands in the next
day's folder. The traffic loader reads the newest `raw/network/` snapshot from
the same backend it writes to, so run the network loader first.

To inspect the network data without writing anywhere:

```bash
python -c "
from ingestion.network_loader import fetch_network, parse_network
from datetime import datetime, timezone
dfs = parse_network(fetch_network(), datetime.now(timezone.utc))
for name, df in dfs.items():
    print(name, len(df))
"
```

### Build the gold layer

```bash
python -m processing.gold.build  --layout samples                      # from data/samples/, no raw zone needed
python -m processing.gold.build  --raw-root data/local/raw             # from what the loaders wrote locally
python -m processing.gold.verify --gold-root data/local/gold           # integrity checks, exit 0/1
```

Output lands in `data/local/gold/<table>/` as Parquet; `pd.read_parquet("data/local/gold/dim_route")` is the quickest look. Setup from scratch for the dev container, Windows natively and Databricks is in [`docs/GOLD_LAYER.md`](docs/GOLD_LAYER.md#1-running-the-build-from-scratch).

### Run local Airflow

```bash
cd airflow && astro dev start        # UI at http://localhost:8080
```

### Run unit tests

```bash
pytest tests/ -v --cov=ingestion --cov=processing
```

No internet or AWS account needed: the loader tests fake the three APIs and
run the handlers end-to-end against a temporary folder (`tests/test_loaders_local.py`),
and the S3 backend is exercised against botocore's Stubber (`tests/test_storage.py`).

### Run the data quality gate

```bash
great_expectations checkpoint run raw_samples_checkpoint
```

Validates every committed sample against its suite. Needs no credentials; see [`quality/README.md`](quality/README.md).

### Start Metabase (local)

```bash
cd metabase
MB_DB_PASSWORD=secret docker compose up -d
# Open http://localhost:3000
```

---

## CI/CD

Every push runs the full CI pipeline via GitHub Actions (`.github/workflows/ci.yml`):

1. `pytest` — unit tests, including `tests/gold/` (the end-to-end gold build on the committed samples)
2. Great Expectations checkpoint `raw_samples_checkpoint` — raw data quality suites against the committed samples
3. `dbt test` — still in the workflow file; to be removed or retargeted once the Redshift load is in place

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
| Gold model reference      | [docs/GOLD_LAYER.md](docs/GOLD_LAYER.md) |
| Great Expectations report | _Coming soon_ (S3 public bucket) |

---

## License

[MIT](LICENSE)
