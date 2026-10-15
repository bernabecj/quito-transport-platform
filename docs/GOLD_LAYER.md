# Gold layer

The gold layer turns the raw zone into a star schema plus two pre-aggregated
marts, in plain PySpark, with no intermediate "silver" zone (the exploration
found nothing to clean — see [ANALYSIS_PLAN.md](ANALYSIS_PLAN.md)). It is
one package, `processing/gold/`, with two entry points:

```
python -m processing.gold.build   --raw-root <raw> --gold-root <gold> [--layout raw|samples]
python -m processing.gold.verify  --gold-root <gold>
```

`build` reads the whole raw zone and rewrites every gold table (a full,
idempotent rebuild — seconds of compute, a minute or two of Spark overhead).
`verify` reads the result back and enforces the star schema's integrity
contract: every table present, every primary key unique, no orphan foreign
keys; exit code 0 or 1. Redshift records keys but does not enforce them, so
`verify` is the guarantee.

The model itself (tables, columns, relationships) is documented in the
second half of this file; the first half is how to run it, from nothing.

---

## 1. Running the build from scratch

Three environments are supported. All of them need the same two things —
**a JDK (17 or 21)** for Spark's JVM and **the Python dependencies** — and
then run the same two commands. Pick one:

| Environment | Best for | Spark writes Parquet? |
| --- | --- | --- |
| [Sandbox / any Linux](#11-sandbox-or-any-linux-box) | checking the build runs at all | yes |
| [Dev container](#12-dev-container-recommended-on-windows) | day-to-day development on Windows | yes |
| [Windows natively](#13-windows-natively) | if Docker is not an option | only with Hadoop's native shims |
| [Databricks](#14-databricks) | browsing the tables in a catalogue / SQL | yes |

Throughout, `<repo>` is the `quito-transport-platform` folder and
`<s3_raw>` is an export of the bucket's `raw/` prefix (the `network/`,
`traffic/`, `weather/` folders). Without an export, `--layout samples`
builds from the files committed under `data/samples/` instead.

**With internet, the Python side is one line:** `pip install -r requirements.txt`
(it pins `pyspark==3.5.8` along with everything else the repo uses) or
`poetry install --with dev --with processing`. The offline `--no-index`
install and the `chmod` in 1.1 exist only because the sandbox cannot reach
PyPI and its wheels were built on Windows; skip them anywhere else.

### 1.1 Sandbox, or any Linux box

The sandbox has no internet, so the Python packages come from wheels built
on a machine that has (see [1.5](#15-building-the-wheels-offline-install)).

```bash
# 0. Java
java -version                          # need 17 or 21; on Ubuntu: sudo apt install openjdk-17-jre-headless

# 1. A virtual environment, outside the repo so it is never downloaded with it.
#    --system-site-packages lets it see pandas/numpy if the box already has them.
python3 -m venv --system-site-packages ~/venv
source ~/venv/bin/activate

# 2. Dependencies
pip install -r <repo>/requirements.txt               # online
#    Offline (the sandbox): install from a folder of wheels instead — see 1.5 —
pip install --no-index --find-links ~/pyspark-wheels pyspark pyarrow boto3 pytest pytest-cov
#    and, only if the pyspark wheel was built on Windows (it loses the Linux executable bit
#    on Spark's launcher scripts, so the JVM cannot be started):
chmod +x ~/venv/lib/python3.*/site-packages/pyspark/bin/* ~/venv/lib/python3.*/site-packages/pyspark/sbin/*

# 3. Smoke test: a Spark session starts on the local JVM
python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[1]').getOrCreate(); print(s.version, s.range(3).count()); s.stop()"
#    -> 3.5.8 3

# 4. Build, verify, test
cd <repo>
python -m processing.gold.build  --raw-root ~/s3_raw --gold-root data/local/gold     # ~2 min
python -m processing.gold.verify --gold-root data/local/gold                          # 32 passed, 0 failed
pytest tests/gold -q --no-cov                                                         # 44 passed, ~4 min
```

Without the raw-zone export: `python -m processing.gold.build --layout samples`
(raw root defaults to `data/samples/`).

### 1.2 Dev container (recommended on Windows)

`.devcontainer/` now installs Java 17 and Poetry's `processing` group
(pyspark), so the container is a complete Spark environment and nothing is
installed on Windows itself.

1. Docker Desktop running; VS Code with the *Dev Containers* extension.
2. Open `<repo>` in VS Code → **Dev Containers: Reopen in Container**. If
   the container already existed before this change, run
   **Dev Containers: Rebuild Container** once (new image layers: JDK + pyspark).
3. In the container's terminal (`/opt/venv` is already on `PATH`):

```bash
java -version                                                   # openjdk 17
python -c "import pyspark; print(pyspark.__version__)"          # 3.5.8
python -m processing.gold.build  --raw-root /path/to/s3_raw --gold-root data/local/gold
python -m processing.gold.verify --gold-root data/local/gold
pytest tests/gold -q --no-cov
```

Put the `s3_raw` export anywhere inside the workspace (e.g. `<repo>/s3_raw/`
— add it to `.gitignore` if you do) so the container can see it, or use
`--layout samples`.

### 1.3 Windows natively

Works, but Spark on Windows needs Hadoop's native shims to **write** files;
without them the build fails at the first `write_table` with
`UnsatisfiedLinkError … NativeIO$Windows.access0`. Reading is fine without.

```powershell
# 1. Java 17 (Temurin from adoptium.net). Install to a path without spaces, then:
setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-17"     # adjust; no trailing slash
java -version

# 2. Hadoop shims: from https://github.com/cdarlint/winutils take hadoop-3.3.6/bin
#    (winutils.exe AND hadoop.dll) into C:\hadoop\bin, then:
setx HADOOP_HOME "C:\hadoop"
setx PATH "%PATH%;C:\hadoop\bin"
#    Open a NEW terminal so setx takes effect.

# 3. Virtual environment (the repo's own .venv; it is gitignored)
cd <repo>
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
pip install pyspark==3.5.8 pyarrow==18.1.0 pandas==2.3.3 boto3==1.43.19 python-dotenv requests pytest pytest-cov
#    or, with Poetry:  poetry install --with dev --with processing
$env:PYSPARK_PYTHON = "$PWD\.venv\Scripts\python.exe"

# 4. Smoke test, then build / verify / test — same commands as Linux
python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[1]').getOrCreate(); print(s.version, s.range(3).count()); s.stop()"
python -m processing.gold.build  --raw-root C:\data\s3_raw --gold-root data\local\gold
python -m processing.gold.verify --gold-root data\local\gold
pytest tests\gold -q --no-cov
```

Paths may be given with either slash; the build joins them with `/`
internally because they may also be S3 URIs.

### 1.4 Databricks

Databricks already has Spark and Java; `get_spark()` reuses the notebook's
session. What is needed is the code and the data.

1. **Data.** Upload `s3_raw/` to a Unity Catalog volume, e.g.
   `/Volumes/workspace/default/data-engineering/quito-transport-platform/s3_raw/`
   (the path `notebooks/01_exploring.ipynb` already uses).
2. **Code.** Either add the GitHub repo as a *Git folder* (Workspace → Create →
   Git folder), or upload `processing/` and `ingestion/` to the workspace.
   Notebooks inside a Git folder get the repo root on `sys.path`
   automatically; otherwise `sys.path.append("/Workspace/<path-to-repo>")`.
3. **Build and verify** in a notebook cell:

```python
from processing.gold.build import build_all
from processing.gold.verify import run_checks, report
from processing.gold.session import get_spark

base = "/Volumes/workspace/default/data-engineering/quito-transport-platform"
counts = build_all(get_spark(), raw_root=f"{base}/s3_raw", gold_root=f"{base}/gold", layout="raw")
print(counts)
print(report(run_checks(get_spark(), f"{base}/gold")))
```

4. **Browse.** `spark.read.parquet(f"{base}/gold/dim_route").display()`, or
   register the folders as tables so they show in the Catalog and in
   Databricks SQL — the closest preview of the Redshift tables:

```sql
CREATE SCHEMA IF NOT EXISTS quito_gold;
CREATE TABLE IF NOT EXISTS quito_gold.dim_route USING PARQUET
  LOCATION '/Volumes/workspace/default/data-engineering/quito-transport-platform/gold/dim_route';
-- one statement per table; the local_date= partitions of fct_corridor_travel_time are discovered automatically
```

`pytest` can also run on Databricks (`%pip install pytest`, then
`pytest.main(["tests/gold", "-q", "--no-cov"])` from a notebook in the Git
folder), though the unit tests are meant for a laptop or CI.

### 1.5 Building the wheels (offline install)

For a machine without internet, download the packages elsewhere as wheels
(`.whl` — pip's pre-built, install-without-network format) and copy the
folder over. On Windows, for a Linux x86-64 target with Python 3.12:

```powershell
pip download --only-binary=:all: --python-version 3.12 `
  --platform manylinux_2_28_x86_64 --platform manylinux2014_x86_64 `
  pyarrow==18.1.0 boto3==1.43.19 pytest==8.4.2 pytest-cov==5.0.0 py4j==0.10.9.7 -d wheels
pip wheel pyspark==3.5.8 --no-deps -w wheels      # pyspark is source-only on PyPI; build its wheel here
```

`pyspark`'s wheel is pure Python (`py2.py3-none-any`), so one built on any
OS or Python version installs on any other; `pyarrow`'s is compiled, hence
the platform flags.

---

## 2. Checking the result

After a build, the quickest look needs no Spark — pandas reads a gold
folder, partitions included:

```python
import pandas as pd
dim = pd.read_parquet("data/local/gold/dim_route")
fct = pd.read_parquet("data/local/gold/fct_corridor_travel_time")
print(dim.query("is_trunk")[["route_ref", "route_name", "is_sampled", "stop_count"]])
print(fct[fct.is_complete_day].groupby("local_hour").mean_speed_kmh.mean().round(1))
```

The second line reproduces the exploration's finding: ~34 km/h from
midnight to 04:00, ~21 km/h from 07:00 to 18:00.

Expected counts, so a run can be judged at a glance:

| Table | `s3_raw` export (52 snapshots, 13 days of traffic) | `data/samples` |
| --- | --- | --- |
| `dim_date` | 135 | 18 |
| `dim_route` | 515 | 515 |
| `dim_stop` | 486 | 485 |
| `dim_weather_condition` | 8 | 6 |
| `fct_weather` | 589 | 144 |
| `fct_corridor_travel_time` | 3,108 (2,352 complete-day) | 2,352 |
| `fct_route_stop` | 961 | 960 |
| `fct_network_snapshot` | 52 | 1 |
| `agg_corridor_hourly_profile` | 1,008 | 1,008 |
| `agg_corridor_reliability` | 14 | 14 |

`verify` prints 32 checks (10 exists, 10 unique, 11 orphans, 1 grain) and
exits 0 when all pass.

---

## 3. The model

Ten tables in a star schema. The **facts** hold measurements (one row per
event), the **dimensions** hold the context those measurements are taken in,
and the two **marts** hold the objectives' answers, pre-aggregated from the
facts. Every key is a natural key — the OSM id, a `yyyymmdd` integer, a
readable slug — never a generated surrogate, so a rebuild yields the same
keys every time.

```
                         dim_date                     dim_weather_condition
                      (date_key)                         (condition_key)
                          ▲  ▲                                ▲
                          │  │                                │
       dim_route ◄──── fct_corridor_travel_time ──────► fct_weather ───────┘
      (route_id)         (route_id, utc_ts)            (weather_ts)
        ▲  ▲  ▲
        │  │  └──────────── fct_route_stop ──────────► dim_stop
        │  │             (route_id, stop_sequence)     (stop_id)
        │  │
        │  └── agg_corridor_hourly_profile  (route_id, local_hour, day_type)
        └───── agg_corridor_reliability     (route_id)

       fct_network_snapshot  (snapshot_date → dim_date)
```

Arrows run from a foreign key to the table it references. The facts never
reference each other except `fct_corridor_travel_time → fct_weather`, which
is the one time-to-time join in the model.

Conventions: `*_ts` columns are UTC timestamps; `local_*` columns are Quito
time (UTC−5, no DST); `date_key` is `yyyymmdd` of the **local** date.
Columns named in *italics* below are foreign keys.

### 3.1 Dimensions

**`dim_date`** — one row per calendar day across the data's span. PK `date_key`.

| column | type | meaning |
| --- | --- | --- |
| `date_key` | int | `yyyymmdd` |
| `date` | date | |
| `year`, `month`, `day`, `day_of_year`, `iso_week` | int | calendar parts |
| `weekday` | int | Monday = 0 … Sunday = 6 (Python's `weekday()`, as the loaders use) |
| `weekday_name` | string | |
| `is_weekend` | bool | Saturday or Sunday |
| `day_type` | string | `weekday` / `weekend` — what the marts split on |

**`dim_route`** — one row per OSM route relation, from the union of every snapshot (SCD type 1: the newest attributes win). PK `route_id`.

| column | type | meaning |
| --- | --- | --- |
| `route_id` | string | OSM relation id |
| `route_ref`, `route_name`, `route_type`, `operator`, `network`, `origin`, `destination`, `colour` | string | OSM tags, from the latest snapshot the route appeared in |
| `is_trunk` | bool | name matches the BRT pattern (Trole, Ecovía, MetroBus) — 20 routes |
| `is_sampled` | bool | has at least one travel-time sample — 14 routes today; flips automatically when a recovered corridor lands |
| `stop_count` | int | member stops in the latest snapshot (0 for 396 of 515) |
| `sequence_source` | string | how the stop order was decided: `osm_member` or `geometry`; null without stops |
| `first_seen_date`, `last_seen_date` | date | first / last snapshot carrying the route |
| `snapshots_present` | long | how many snapshots carried it (52 for all but two flapping routes) |

**`dim_stop`** — one row per OSM stop node, same SCD type 1 treatment. PK `stop_id`.

| column | type | meaning |
| --- | --- | --- |
| `stop_id` | string | OSM node id |
| `stop_name`, `stop_type`, `shelter` | string | OSM tags (shelter is tagged on 7 stops only) |
| `latitude`, `longitude` | double | WGS84 |
| `route_count` | int | distinct routes serving the stop in the latest membership — the density / overlap signal |
| `first_seen_date`, `last_seen_date`, `snapshots_present` | | as in `dim_route` |

**`dim_weather_condition`** — one row per OpenWeather condition seen. PK `condition_key`.

| column | type | meaning |
| --- | --- | --- |
| `condition_key` | string | `rain/light_rain` — slug of the two columns below |
| `weather_main` | string | OpenWeather group: Clear, Clouds, Rain … |
| `weather_description` | string | the finer label |
| `is_precipitation` | bool | Rain, Drizzle, Snow or Thunderstorm |
| `raw_rows` | long | raw rows carrying it — how rare the condition is (2 for *heavy intensity rain*) |

### 3.2 Facts

**`fct_weather`** — one row per three-hour UTC slot, the best available source for it. PK `weather_ts`.

| column | type | meaning |
| --- | --- | --- |
| `weather_ts` | timestamp | the slot: 00, 03 … 21 UTC |
| `local_ts`, `local_date`, *`date_key`*, `local_hour` | | Quito time |
| *`condition_key`*, `weather_main`, `weather_description` | | condition |
| `record_type` | string | `current` (an observation rounded to the slot) or `forecast` (the shortest-lead forecast for it) |
| `lead_hours` | double | forecast horizon; 0 for observations. Median 16 h on the winning forecasts |
| `offset_minutes` | int | `observed_at − weather_ts`; ±90 max, 0 for forecasts |
| `observed_at`, `fetched_at` | timestamp | the row's own time, and when the API was called |
| `temp_c`, `feels_like_c`, `humidity_pct`, `pressure_hpa`, `wind_speed_ms`, `wind_deg`, `clouds_pct`, `visibility_m` | | measures |
| `rain_mm`, `rain_period_hours` | double, int | rain accumulation and its window — **1 h for observations, 3 h for forecasts** (the raw column `rain_1h_mm` hides this) |

**`fct_corridor_travel_time`** — the hub: one row per corridor per sampled hour. PK (`route_id`, `utc_ts`). Partitioned by `local_date`.

| column | type | meaning |
| --- | --- | --- |
| *`route_id`* | string | corridor |
| `utc_ts` | timestamp | the sampled hour (from the raw partition) |
| `sampled_at` | timestamp | the exact instant of the Mapbox call |
| `local_ts`, `local_date`, *`date_key`*, `local_hour`, `local_weekday` | | Quito time; weekday Monday = 0 |
| `waypoint_count`, `mean_hop_km` | long, double | how the corridor was routed |
| `sequence_source`, `stops_excluded` | string, long | provenance of the stop order (`osm_member` / `geometry`) and waypoints dropped for being off-road; defaults for files that predate them |
| `distance_meters`, `duration_seconds`, `mean_speed_kmh` | double | **the measures**. Duration is primary; distance varies up to 20 % per corridor as Mapbox reroutes, so speed partly reflects path choice |
| `segments_total`, `segments_known`, `congestion_known_pct` | | how much of the route had live congestion data — 2 % at 04:00, 56 % at 20:00, so… |
| `congestion_low` … `congestion_unknown` | long | …these are supporting detail, not an hourly signal |
| `hours_sampled_that_day` | int | distinct hours this corridor was sampled on this local date |
| `is_complete_day` | bool | all 24 — what the marts filter on; 2,352 rows today |
| `routes_sampled_that_hour` | int | corridors in this hour's sample (14 = the full set) |
| *`weather_ts`*, `weather_offset_minutes` | | the nearest 3-hour slot (−60 / 0 / +60 min) |
| *`condition_key`*, `weather_record_type`, `weather_lead_hours` | | the slot's condition and provenance |
| `temp_c` … `rain_period_hours` | | the slot's measures, copied on (13 columns, as in `fct_weather`) |

**`fct_route_stop`** — the bridge between routes and stops: one row per route × position, latest snapshot. PK (`route_id`, `stop_sequence`).

| column | type | meaning |
| --- | --- | --- |
| *`route_id`*, `stop_sequence` | string, int | position along the route, from 0 |
| *`stop_id`* | string | one relation lists the same node twice — count `stop_id` distinct when counting stops |
| `member_role` | string | OSM role: `stop`, `platform`, `stop_entry_only` … |
| `sequence_source` | string | `osm_member` or `geometry` |
| `member_index` | int | OSM's own position, when the order was recomputed |
| `distance_along_m`, `snap_distance_m` | double | distance along the road and stop-to-road distance, geometry-ordered routes only; large snap = misplaced member |
| `snapshot_date` | date | |

**`fct_network_snapshot`** — one row per snapshot day: the network's size and what changed. PK `snapshot_date`.

| column | type | meaning |
| --- | --- | --- |
| `snapshot_date`, *`date_key`* | | |
| `routes`, `stops`, `route_stops` | long | row counts in the snapshot |
| `routes_with_stops`, `trunk_routes` | long | 119 and 20, every day |
| `previous_snapshot_date`, `days_since_previous` | | a missed daily run shows as > 1 (none so far) |
| `routes_added`, `routes_removed`, `stops_added`, `stops_removed` | int | ids present now / before but not both; null on the first snapshot |

### 3.3 Marts

**`agg_corridor_hourly_profile`** — objectives 1 and 4. PK (`route_id`, `local_hour`, `day_type`); 14 × 24 × 3 rows.

| column | type | meaning |
| --- | --- | --- |
| *`route_id`*, `local_hour`, `day_type` | | `day_type` ∈ `all` / `weekday` / `weekend` |
| `n`, `sample_days` | int | complete-day samples in the cell (7 / 5 / 2 today) |
| `duration_median_s`, `duration_p10_s`, `duration_p90_s`, `duration_mean_s` | double | |
| `speed_median_kmh`, `speed_p10_kmh`, `speed_p90_kmh`, `speed_mean_kmh` | double | |
| `distance_median_m` | double | |
| `baseline_duration_s` | double | the corridor's overnight median (local 00–04) for this day type |
| `ratio_to_baseline` | double | `duration_median_s ÷ baseline_duration_s`; 1.55 network-wide by day |
| `corridor_duration_median_s` | double | median over all 24 hours of this day type |
| `is_peak` | bool | hourly median above `corridor_duration_median_s` |
| `meets_min_samples` | bool | `n ≥ 3`; weekend cells are `false` today |

**`agg_corridor_reliability`** — objectives 2 and 3, one row per sampled corridor, rank 1 = worst. PK `route_id`.

| column | type | meaning |
| --- | --- | --- |
| *`route_id`* | | |
| `samples`, `sample_days`, `first_sample_date`, `last_sample_date` | | complete-day history |
| `baseline_duration_s`, `peak_duration_s`, `peak_hour_local` | | overnight median; the slowest hour's median and which hour |
| `spread_duration_pct`, `rank_spread_duration` | | `(peak − baseline) ÷ baseline × 100` |
| `baseline_speed_kmh`, `trough_speed_kmh`, `trough_hour_local` | | the same under speed |
| `spread_speed_pct`, `rank_spread_speed`, `spread_ranks_agree` | | agree on 3 of 14 corridors — both are shown |
| `cv_within_hour_duration`, `cv_within_hour_speed` | double | mean over hours of stddev ÷ mean — unpredictability at a fixed time of day |
| `rank_cv_duration`, `rank_cv_speed`, `cv_ranks_agree` | | |
| `distance_min_m`, `distance_max_m`, `distance_spread_pct` | | the rerouting confound, in the open (0 % on E1R, 20.2 % on C6) |
| `is_ranked` | bool | `sample_days ≥ 5`; ranks are null otherwise |

### 3.4 Integrity contract

Enforced by `python -m processing.gold.verify` after every build and by
`tests/gold/test_build.py`:

- every primary key above is unique and non-null;
- every foreign key value exists in the referenced table (11 relationships);
- `fct_corridor_travel_time` has no row without a weather slot.

---

## 4. Loading into Redshift

The gold folder is already in the shape Redshift wants: one Parquet folder
per table, Hive-style partitions on the fact. Two ways in, both without dbt.

### 4.1 `COPY` (data lives in Redshift)

Best for the Metabase dashboards: fastest queries, and the distribution and
sort keys below are what make a star schema fast on Redshift's MPP
architecture. `DISTKEY` decides which node a row lives on — the fact and the
dimension it joins most share `route_id`, so that join never moves data —
and `DISTSTYLE ALL` copies a small dimension to every node so no join needs
it shipped. `SORTKEY` orders rows on disk so a date filter reads a slice.

```sql
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE gold.dim_date (
    date_key INTEGER NOT NULL, "date" DATE, "year" SMALLINT, "month" SMALLINT, "day" SMALLINT,
    day_of_year SMALLINT, iso_week SMALLINT, weekday SMALLINT, weekday_name VARCHAR(9),
    is_weekend BOOLEAN, day_type VARCHAR(7),
    PRIMARY KEY (date_key)
) DISTSTYLE ALL SORTKEY (date_key);

CREATE TABLE gold.dim_route (
    route_id VARCHAR(16) NOT NULL, route_ref VARCHAR(64), route_name VARCHAR(256), route_type VARCHAR(32),
    operator VARCHAR(128), network VARCHAR(128), origin VARCHAR(128), destination VARCHAR(128),
    colour VARCHAR(16), is_trunk BOOLEAN, is_sampled BOOLEAN, stop_count INTEGER,
    sequence_source VARCHAR(10), first_seen_date DATE, last_seen_date DATE, snapshots_present BIGINT,
    PRIMARY KEY (route_id)
) DISTSTYLE ALL SORTKEY (route_id);

CREATE TABLE gold.fct_corridor_travel_time (
    route_id VARCHAR(16) NOT NULL REFERENCES gold.dim_route (route_id),
    utc_ts TIMESTAMP NOT NULL, sampled_at TIMESTAMP, local_ts TIMESTAMP, local_date DATE,
    date_key INTEGER REFERENCES gold.dim_date (date_key), local_hour SMALLINT, local_weekday SMALLINT,
    waypoint_count INTEGER, mean_hop_km DOUBLE PRECISION, sequence_source VARCHAR(10), stops_excluded INTEGER,
    distance_meters DOUBLE PRECISION, duration_seconds DOUBLE PRECISION, mean_speed_kmh DOUBLE PRECISION,
    segments_total INTEGER, segments_known INTEGER, congestion_known_pct DOUBLE PRECISION,
    congestion_low INTEGER, congestion_moderate INTEGER, congestion_heavy INTEGER,
    congestion_severe INTEGER, congestion_unknown INTEGER,
    hours_sampled_that_day SMALLINT, is_complete_day BOOLEAN, routes_sampled_that_hour SMALLINT,
    weather_ts TIMESTAMP, weather_offset_minutes SMALLINT, condition_key VARCHAR(64),
    weather_record_type VARCHAR(8), weather_lead_hours DOUBLE PRECISION,
    temp_c DOUBLE PRECISION, feels_like_c DOUBLE PRECISION, humidity_pct INTEGER, pressure_hpa INTEGER,
    wind_speed_ms DOUBLE PRECISION, wind_deg INTEGER, clouds_pct INTEGER, visibility_m INTEGER,
    rain_mm DOUBLE PRECISION, rain_period_hours SMALLINT,
    PRIMARY KEY (route_id, utc_ts)
) DISTKEY (route_id) SORTKEY (local_date, utc_ts);
-- dim_stop, dim_weather_condition: DISTSTYLE ALL.  fct_weather, fct_network_snapshot,
-- the marts: DISTSTYLE ALL (hundreds of rows).  fct_route_stop: DISTKEY (route_id).

-- Full refresh, matching the build's own semantics:
TRUNCATE gold.fct_corridor_travel_time;
COPY gold.fct_corridor_travel_time
FROM 's3://quito-transport-platform/gold/fct_corridor_travel_time/'
IAM_ROLE 'arn:aws:iam::<account>:role/<redshift-s3-read>'
FORMAT AS PARQUET
SERIALIZETOJSON;                       -- harmless here; needed only for nested types
```

Two things to know. `COPY … FORMAT AS PARQUET` matches columns **by
position**, so the `CREATE TABLE` column order must equal the Parquet
column order above (it does; `local_date` is a partition column in the
folder and is read from the path). And Redshift's `PRIMARY KEY` /
`REFERENCES` are *informational* — the planner uses them, nothing enforces
them — which is why `verify` runs before `COPY`, never after.

### 4.2 Redshift Spectrum (data stays in S3)

No load step: an external schema over the gold prefix, queried in place.
Slower per query, zero copies, always current. Good while the dashboards are
being designed.

```sql
CREATE EXTERNAL SCHEMA gold_ext
FROM DATA CATALOG DATABASE 'quito_gold'
IAM_ROLE 'arn:aws:iam::<account>:role/<redshift-spectrum>'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

CREATE EXTERNAL TABLE gold_ext.dim_route (
    route_id VARCHAR(16), route_ref VARCHAR(64), route_name VARCHAR(256), /* … as above … */ snapshots_present BIGINT
) STORED AS PARQUET LOCATION 's3://quito-transport-platform/gold/dim_route/';

CREATE EXTERNAL TABLE gold_ext.fct_corridor_travel_time ( /* all columns except local_date */ )
PARTITIONED BY (local_date DATE)
STORED AS PARQUET LOCATION 's3://quito-transport-platform/gold/fct_corridor_travel_time/';
-- then, after each build:  ALTER TABLE … ADD IF NOT EXISTS PARTITION (local_date='2026-08-25') LOCATION '…/local_date=2026-08-25/'
-- or let a Glue crawler over s3://…/gold/ register tables and partitions automatically.
```

A Glue crawler pointed at the gold prefix is the least manual option: it
creates the catalogue tables with the right columns and keeps the
partitions registered, and both Spectrum and Athena read them.

### 4.3 Daily flow, deployed

```
EventBridge (daily)
  → ingestion Lambdas land raw/…/day=<today>
  → Glue job runs  processing/gold/build.py  --raw-root s3://…/raw --gold-root s3://…/gold
  → same job runs  processing/gold/verify.py --gold-root s3://…/gold     (non-zero exit fails the job)
  → Redshift: TRUNCATE + COPY each table   (or nothing, with Spectrum)
  → Metabase queries gold.*
```

The build script needs no Glue-specific API: a Glue *Spark* job runs it as
a plain Python script with `--additional-python-modules` unnecessary
(pyspark is Glue's own), and `get_spark()` reuses Glue's session. The
`ingestion/` and `processing/` packages go up as `--extra-py-files` (a zip).
