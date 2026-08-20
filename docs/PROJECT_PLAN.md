# Project Plan

This is the planning doc for the Quito Transport Platform — the pitch, the day-by-day
build plan, and the roadmap beyond the initial 3-week build. For the as-built architecture
and setup instructions, see the [README](../README.md).

**The idea:** Quito's public transport (Trole, Ecovía, Metrobús, plus dozens of private cooperatives) is notoriously unpredictable, and nobody publishes an answer to basic questions about it — where it reaches, when it slows down, and which corridors are worst. Build a platform that ingests the city's transit network, corridor travel times and weather, processes it, and surfaces coverage, peak hours and the least predictable routes — publicly accessible so citizens actually use it.

**Why this works for Thoughtworks:**

- Real social impact (Quito citizens — a consulting story that writes itself)
- Covers almost every concept in the job description
- Data mesh-friendly architecture you can explain in interviews
- Live demo-able during technical rounds

---

## Scope decision: measured congestion, not schedule adherence

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

### Recovering the original idea

Deviation from a timetable is unmeasurable, but the *premise* — that Quito's transport is unpredictable — is really about traffic congestion, and that is measurable. Combining the OSM route geometry the pipeline already ingests with a traffic-aware routing API gives observed travel time per corridor. Sampled hourly, it yields exactly what the pitch asked for:

| Original metric | Reframed as | Status |
| --------------- | ----------- | ------ |
| Delay vs schedule | Travel time vs the corridor's own historical median | Measurable |
| Peak hours | When corridor travel time rises above that median | Measurable |
| Worst routes | Largest peak-to-off-peak spread, and highest variance | Measurable |

Provider selection was itself verified rather than assumed. TomTom publishes no Ecuador coverage for either Traffic Flow or Traffic Stats (it serves AR, BR, CL, CO, PE, UY only); Google's terms forbid storing results, which rules out building a historical dataset; Waze's speed data requires a government partnership. Mapbox was the only candidate, and its `driving-traffic` profile *silently* degrades to free-flow estimates where it lacks coverage, so [`scripts/verify_mapbox_traffic.py`](../scripts/verify_mapbox_traffic.py) checks the per-segment congestion annotations before any code depends on it. Measured 2026-08-19: 49-71% of segments carry real observations.

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
| Warehouse      | SQL analytics          | **Amazon Redshift** (separate AWS account)     |
| Data Quality   | **Great Expectations** | Runs in Glue/Airflow, results to S3            |
| Data Catalog   | Schema & lineage       | **AWS Glue Data Catalog**                      |
| Visualization  | **Metabase**           | Deployed on **EC2** (t2.micro free tier)       |
| CI/CD          | GitHub Actions         | Deploys to AWS automatically                   |
| Secrets        | API keys & credentials | **AWS Secrets Manager**                        |
| Monitoring     | Pipeline observability | **AWS CloudWatch**                             |

---

## Data Sources (free, public)

- **OpenStreetMap via Overpass API** — transit routes, stops, stop ordering, operator. No credentials required. Rate-limits under load (HTTP 429/504), so the loader cycles two endpoints with exponential backoff.
- **Mapbox Directions (`driving-traffic`)** — observed travel time and per-segment congestion for the trunk BRT corridors. Free tier of 100,000 requests/month. Sampling runs every six hours in a **fixed two-week window from 2026-08-20** (~780 requests), driven by EventBridge so it survives the laptop being closed. It cannot be backfilled — Mapbox reports current conditions only — so the window is collected unattended and then switched off via `TRAFFIC_SAMPLING_ENABLED`.
- **OpenWeather API** — free tier, current + forecast weather by coordinates.

### Why ingest daily if the network is near-static?

Each run writes a dated snapshot. Accumulated, those snapshots become a longitudinal record of network change — routes added or withdrawn, stops relocated or newly mapped. The change history is itself a dataset, and it justifies partitioning the raw zone by `year/month/day`.

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
| Which operators cover which parts of the city? | `routes.operator` |
| What has been added or withdrawn since the first snapshot? | dated partitions |
| When does a corridor slow down during the day? | `corridor_travel_time` accumulated over days |
| Which corridors have the worst peak-to-off-peak spread? | `corridor_travel_time` accumulated over days |
| Which corridors are least predictable? | variance of `duration_seconds` per corridor |

**Planned enhancement — weather joined geographically.** Weather is currently fetched for a single city-centre coordinate. Quito's valley has real microclimate variation, so fetching several points and joining them to stops by proximity would let coverage analysis account for local conditions. That is a change to `weather_loader.py`, not to the network pipeline.

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
- Compute derived metrics: stop density per zone, route overlap per corridor, operator coverage per zone
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

## Roadmap: Real-Time Extension

Extending the existing batch platform with a streaming layer, so the same data answers
both **"what are the historical patterns?"** (the daily batch pipeline — delay trends,
peak hours, worst corridors, run once at 6am) and **"what is happening right now?"**
(a citizen at a stop at 8:15am who cannot know their route is currently 20 minutes
behind).

Adding a streaming layer turns this into a **Lambda architecture** — batch and speed
layers serving different latency needs from the same underlying data. That is a
stronger architectural story than either layer alone, and it exercises two areas the
3-week plan above only touches lightly: real-time processing and data governance /
masking.

### Data source: extending the verified feed, not inventing one

The [scope decision](#scope-decision-measured-congestion-not-schedule-adherence) above
already established that Quito publishes **no GTFS feed, static or realtime** — checked
against transit.land's registry (786 feeds, zero from Ecuador) and the Mobility Database
catalogue (3,462 feeds, zero from Ecuador). That finding covers GTFS-Realtime as much as
static GTFS: there is no vehicle-position or trip-update feed to poll, and building this
extension around one anyway would repeat the exact mistake the scope decision exists to
avoid.

So this extension does not introduce GTFS-Realtime. It streams the one live signal this
project has already verified: **Mapbox Directions corridor travel time**, currently
sampled every six hours by [`ingestion/traffic_loader.py`](../ingestion/traffic_loader.py).
The streaming layer is the same, already-proven data source polled far more often, feeding
a low-latency path alongside the existing batch one — not a new, unverified integration.

Two consequences follow, worth stating plainly rather than discovering mid-build:

- **No vehicle-level data exists in this stream.** Mapbox Directions returns a
  route-level duration and per-segment congestion, not individual vehicle positions.
  There is no `vehicle_id` to mask. The masking design below is kept because it is good
  practice to design in advance, but it only becomes load-bearing if a genuine
  vehicle-position feed is added later (e.g. another city's public GTFS-RT, substituted
  in and labelled honestly, the same way the [data source options](#target-architecture)
  below note it as a fallback).
- **The Mapbox rate limit, not Kinesis throughput, is the real constraint.** Polling all
  14 trunk corridors every 30s is ~40,000 requests/day — burns the 100k/month free tier
  in under three days. A realistic polling interval is every 2–5 minutes: still a 70×+
  improvement on the current 6-hour cadence, and enough to drive a visibly live dashboard
  without paying for the API.

### Target architecture

```
Mapbox Directions (corridor travel time, polled every 2-5 min per route)
        ↓
   Producer (Lambda, scheduled via EventBridge)
        ↓
   Kinesis Data Streams  ← partition key: route_id
        ↓
   ┌────────────────────────────────┐
   │  Spark Structured Streaming    │
   │  (Glue Streaming job)          │
   │  - windowed aggregations       │
   │  - watermark for late events   │
   │  - checkpointing to S3         │
   └────────────────────────────────┘
        ↓                    ↓
   S3 bronze/realtime     DynamoDB
   (feeds batch layer)    (current state)
        ↓                    ↓
   existing daily DAG    Grafana dashboard
```

The same stream feeds both the historical lake — landing in the same
`corridor_travel_time` shape the batch pipeline already writes, just at higher
frequency — and a low-latency store for the live dashboard. That dual sink is what
makes this a genuine Lambda architecture rather than two disconnected systems.

If a real vehicle-position feed ever becomes available (Quito starts publishing
GTFS-Realtime, or another city's feed is substituted in for a clearly-labelled demo),
it slots into the same Kinesis → Spark → dual-sink shape; only the producer and the
payload schema change.

### New components

**1. Producer (`streaming/producer/`)**

A Lambda function on an EventBridge schedule (every 2–5 minutes, not 30s — see the rate
limit note above) that calls Mapbox Directions for each trunk corridor and publishes to
Kinesis.

Key decisions to document:

- **Partition key = `route_id`.** All events for a corridor land in the same shard,
  preserving per-route ordering for correct windowed aggregation. Watch for skew: a
  corridor sampled more often than others becomes a hot shard — the same skew problem
  already handled in the Glue/PySpark layer, one layer up.
- **Batching.** `put_records` (plural), not one call per corridor, to keep API calls —
  and cost — down.
- **Failure handling.** Kinesis partial failures are normal; `put_records` returns
  per-record status. Retry only the failed records, not the whole batch.

**2. Stream processor (`streaming/processor/`)**

A Glue Streaming job running Spark Structured Streaming. Aggregations to compute, all
extensions of metrics the batch layer already produces:

- Average speed per route, 5-minute tumbling window (`mean_speed_kmh`, already sampled)
- Travel time vs. the corridor's own rolling historical median — the same comparison
  the batch marts make daily, computed continuously instead
- Congestion flag when average speed drops below the route-specific threshold
  (`congestion_*`, already sampled)
- Sample count per window, as a data-quality signal rather than a "vehicle count" —
  there are no vehicles in this feed, only route-level samples

Concepts to implement deliberately:

- **Watermark** — accept events up to 10 minutes late, discard beyond that. Without it,
  Spark retains every open window indefinitely and eventually exhausts memory.
- **Checkpointing to S3** — stores processed offsets so the job resumes correctly after
  failure.
- **Idempotent writes** — overwrite by window + route key rather than appending, so a
  replay after failure cannot double-count. Same principle already used in the batch
  layer.

**3. Serving layer**

- **DynamoDB** for current state: key `route_id`, attributes for current average speed,
  congestion status, last-updated timestamp. Single-digit-millisecond reads, on-demand
  billing, TTL to auto-expire stale rows.
- **S3 bronze/realtime** for the raw event archive, date-partitioned Parquet, feeding
  the existing batch pipeline — the same partitioning strategy (`year/month/day`)
  already used for the raw zone.

**4. Dashboard**

Grafana for this layer, not Metabase — time-series data with frequent refresh is what
Grafana is built for, while Metabase stays the right tool for the historical analytical
dashboards already planned above. That split ("Grafana observes systems, Metabase
analyzes business") is worth stating explicitly rather than leaving implicit.

Panels: live corridor speed, congestion alerts, current travel time vs. the historical
median from the batch layer.

### Data masking layer

This is designed in advance for when it becomes load-bearing (see the data-source note
above — the current Mapbox-based stream carries no vehicle-level identifiers, so none of
this applies yet). Documented now so it is not an afterthought if a vehicle-position feed
is added later.

| Technique | Where | Implementation |
|---|---|---|
| Hashing | Producer, before publish | SHA-256 of the identifier with a salt from Secrets Manager |
| Tokenization | Producer | Map real IDs to surrogate tokens, mapping table access-restricted |
| Column-level access | Redshift | `GRANT SELECT (col1, col2)` — analysts see aggregates, not raw IDs |
| Aggregation threshold | Processor | Suppress any window with too few underlying samples to prevent re-identification |
| Retention policy | S3 lifecycle | Raw positional data expires after 30 days; aggregates retained indefinitely |

If this becomes active, the reasoning belongs in `DATA_GOVERNANCE.md`, which would need
updating either way — it currently states the project holds no personal data at all, a
claim that is still true for the Mapbox-based stream but would stop being true the moment
a vehicle-level feed is added. Masking should happen **at ingestion, before data lands**,
so raw identifiers never persist — masking after storage means the unmasked data already
existed somewhere.

### Cost control

Non-negotiable, set up before creating any resource:

- AWS Budgets alert at $20 and $50
- Kinesis **on-demand** mode, not provisioned
- Glue Streaming charges per DPU-hour while running — **stop the job when not actively
  developing**
- DynamoDB on-demand, with TTL enabled
- A `make stop-all` target that shuts down every billable resource

Realistic monthly cost if disciplined: $15–40. If a Glue job is left running: $200+. The
difference is entirely operational habit — the same lesson the fixed two-week Mapbox
sampling window above already applies to the batch layer.

### Repository additions

```
quito-transport-platform/
├── streaming/
│   ├── producer/
│   │   └── mapbox_stream_producer.py
│   ├── processor/
│   │   ├── stream_aggregations.py
│   │   └── watermark_config.py
│   └── tests/
│       └── test_windowing.py
├── infrastructure/
│   └── streaming_iam.json
├── grafana/
│   └── dashboards/
└── docs/
    ├── STREAMING_ARCHITECTURE.md
    └── LAMBDA_TRADEOFFS.md
```

### Build sequence

Rough ordering, not a schedule — adapt to available time.

1. **Infrastructure and cost guardrails.** Budgets, IAM roles, Kinesis stream. Verify
   the alerts fire before proceeding.
2. **Producer.** Poll Mapbox Directions for the trunk corridors, publish to Kinesis.
   Success criterion: events visible in the stream.
3. **Consumer, minimal.** Read the stream, write raw events to S3. No aggregation yet —
   just prove the read path works.
4. **Windowed aggregation.** Add tumbling windows, then the watermark, then
   checkpointing. One at a time, verifying each.
5. **DynamoDB sink.** Write current state, confirm idempotency by replaying events and
   checking for duplicates.
6. **Grafana.** Connect, build panels.
7. **Documentation.** Architecture diagram, trade-off write-up, README update. Revisit
   the masking section and `DATA_GOVERNANCE.md` only if a vehicle-level feed has
   actually been added by this point — otherwise leave them as forward-looking design.

Steps 3–5 are where the real learning happens. Do not rush them.
