# Analysis Plan — from raw zone to gold layer

Traffic collection stopped on 2026-09-02 after 13 days. This document is the plan
for turning the raw zone into the platform's findings: the objectives, what the
data can and cannot support, and how the results reach the dashboard.

**Direction change (2026-10).** The exploration in
[`notebooks/01_exploring.ipynb`](../notebooks/01_exploring.ipynb) found no nulls
and no corrupted values in the raw data, so the cleaning/"silver" pass the
original plan assumed has nothing to do. The transformation layer is therefore
built **directly from raw to gold in plain PySpark** (`processing/gold/`),
written as Parquet that mirrors the bucket layout — the same way the ingestion
loaders write locally under `data/local/`. dbt is set aside: the gold tables are
the warehouse model, and Redshift loads them as-is. The design is documented in
[`docs/GOLD_LAYER.md`](GOLD_LAYER.md).

---

## What exists

| Dataset | Location | Coverage |
| --- | --- | --- |
| Corridor travel time | `raw/traffic/year=/month=/day=/hour=/corridor_travel_time.parquet` | 3,108 rows, 14 corridors, 222 samples, 2026-08-20 → 2026-09-02 UTC. 6-hourly through 08-24 12:00, hourly from 08-24 16:00 to 09-01 23:00, truncated at 09-02 01:00. |
| Network (`routes`, `stops`, `route_stops`) | `raw/network/year=/month=/day=/{routes,stops,route_stops}.parquet` | 52 daily snapshots, 2026-08-19 → 2026-10-09. 515 routes, 486 stops, ~960 route-stop rows per snapshot. Three files with three schemas per partition: read each by file name, never the folder. |
| Weather | `raw/weather/year=/month=/day=/weather.parquet` | 58 daily calls (gaps in June–August, then daily). Each call = 1 `current` row + 40 `forecast` rows at 3-hour steps. |

The raw zone (~9 MB) is exported for offline work as `s3_raw/`; the committed
`data/samples/` hold one snapshot of each table plus the 2,352-row analysis
window, enough to run the gold build and its tests without AWS.

### What the exploration established

Established in the notebook, not suspected. Do not rediscover these.

- **Timestamps are UTC.** Quito is UTC-5, no DST. `traffic.hour` is UTC and
  `traffic.local_hour` is Quito time; the two agree on every row.
- **Analysis window: 2026-08-25 → 2026-08-31 local.** Seven complete local days
  with hourly coverage. 2,352 rows = 14 routes × 24 h × 7 days, no gaps, no
  duplicates, no nulls in any measure. Earlier days are 6-hourly and would bias
  every hourly mean toward hours 0/6/12/18 UTC.
- **`duration_seconds` is the primary measure.** Plausible range 14–89 min.
- **`mean_speed_kmh` is trustworthy but reflects path choice.** Routed distance
  varies run to run because Mapbox reroutes between the fixed waypoints: 0.0 %
  spread on E1R, up to 20.2 % on one C6 relation; six corridors exceed 10 %.
  A corridor that reroutes shows artificially *lower* duration variance because
  the reroute is what avoided the delay.
- **`congestion_*` is not usable as an hourly signal.** Segment coverage is
  structured by time of day — 1.8 % of segments known at 04:00 local, 56.5 % at
  21:00 — so the missingness tracks the variable under study. The columns stay
  in the fact table as supporting detail with `congestion_known_pct` alongside.
- **Speed alone shows the rush-hour signal.** 33–34 km/h from midnight to
  04:00, 21–22 km/h for the whole 07:00–18:00 span, recovering through the
  evening. This is the finding the project was built to make.
- **Weather is one point for the whole city**, one real observation per day
  (midnight local). The hourly series is reconstructed from the shortest-lead
  forecast for each target time, with the lead time kept as a column.
- **The network snapshots flap, they do not change.** Only two routes are ever
  absent (`6433119` on 32 of 52 days, `6427512` on 11, the latter renamed
  *Estacion → Estación* mid-way) and both keep returning. Dimensions are built
  from the **union of all snapshots**, never from the latest one alone.
- **All 14 sampled routes exist in `routes`.** The fact-to-dimension join is clean.
- **Four Ecovía trunk corridors (E1, E1R, E3 both directions) were never
  sampled.** `traffic_loader` rejected them because OSM lists their stops out
  of geographic order. `network_loader` now recovers the order from the road
  geometry (2026-10), adding `sequence_source`, `member_index`,
  `distance_along_m`, `snap_distance_m` to `route_stops` and `sequence_source`,
  `stops_excluded` to traffic — but **no snapshot in the raw zone carries those
  columns yet**; the deployed Lambda predates the change. The gold reader must
  treat them as optional and default them (`sequence_source = "osm_member"`).

---

## Objectives

### Analytical objectives

Scoped to what one hourly week of 14 corridors supports. Each has a pass/fail
bar so "we found something" means the same thing every time, and each is
answered by a named gold table so the finding reaches Metabase rather than
staying in a notebook.

| # | Objective | Gold table | Bar |
| --- | --- | --- | --- |
| 1 | **Daily congestion curve.** Per corridor, how travel time and speed move across the 24-hour cycle. | `agg_corridor_hourly_profile` | Only cells with n ≥ 3 samples; hours below that are flagged, not interpolated. |
| 2 | **Peak-to-off-peak spread, ranked.** How much worse each corridor's peak is against its own overnight baseline. | `agg_corridor_reliability` | Ranking reported under both `duration_seconds` and `mean_speed_kmh`; if they disagree, both are shown. |
| 3 | **Least predictable corridors.** Variability at a fixed time of day, not pooled across hours. | `agg_corridor_reliability` | Coefficient of variation within (corridor, local_hour), averaged; distance spread reported next to it so the rerouting confound is visible. Minimum history (`sample_days`) before a corridor is ranked. |
| 4 | **Weekday vs weekend.** Whether any corridor's curve differs between the two. | `agg_corridor_hourly_profile` (`day_type`) | Two weekend days only — labelled preliminary in the table itself. |
| 5 | **Weather correlation (exploratory).** Whether rain or low visibility coincides with slower corridors. | `fct_weather`, denormalised onto `fct_corridor_travel_time` | Hypothesis only: one city-centre point, forecast-reconstructed, small n for rain. Provenance (`weather_lead_hours`, `weather_record_type`) travels with every row. |
| 6 | **Network coverage and change.** Which stops each route serves; what is added or withdrawn over time. | `fct_route_stop`, `fct_network_snapshot` | "Removed" means absent for N consecutive snapshots, not absent today. |

### Implementation objectives

What the gold build must satisfy regardless of the findings.

1. **Raw to gold in one PySpark job**, runnable the same way on a laptop, in
   this sandbox and on AWS Glue: `python -m processing.gold.build --raw-root
   <raw> --gold-root <gold>`. No Glue-only APIs.
2. **Idempotent full rebuild.** Every run reads the whole raw zone and
   overwrites every gold table. Running twice yields identical output; a failed
   run is simply re-run. The fact table is partitioned by `local_date` so the
   build can switch to per-day overwrite if volume ever warrants it.
3. **No hard-coded dates.** The analysis window is derived (complete local days
   with hourly coverage), so new traffic extends the marts without code changes.
4. **Schema evolution handled in the reader.** Old and new raw shapes (with and
   without the corridor-recovery columns) read into one uniform table. The four
   recovered corridors appear automatically the day their data lands.
5. **Star schema, natural keys.** Dimensions keyed on OSM ids and a
   deterministic `date_key`; no generated surrogate ids, so reruns are stable
   and Redshift `COPY` needs no key remapping.
6. **Tested.** Unit tests on small hand-built DataFrames per transformation,
   plus one end-to-end run on `data/samples/` asserting row counts and
   referential integrity (every fact key exists in its dimension). Redshift
   does not enforce keys, so these tests are the only guarantee.
7. **Documented.** `docs/GOLD_LAYER.md` (model, columns, run and verify),
   README and PROJECT_PLAN updated to the new direction.

---

## Gold model

Ten tables in a star schema: one row per *thing* in the dimensions, one row
per *measurement* in the facts, and two pre-aggregated marts that answer the
objectives directly. Every key is a natural key (the OSM id, a `yyyymmdd`
integer, a readable slug), never a generated surrogate, so a rebuild produces
the same keys every time and Redshift `COPY` needs no remapping.

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

       fct_network_snapshot  (snapshot_date)   ── stands alone: counts per day
```

Arrows point from the foreign key to the table it references. `fct_corridor_travel_time`
is the hub: every analytical question starts there and reaches the context it
needs in one hop.

### Dimensions

| Table | Grain / primary key | Built from | Notes |
| --- | --- | --- | --- |
| `dim_date` | one row per calendar day, `date_key` (`yyyymmdd`) | generated over the data's span | `weekday` (Mon=0), `weekday_name`, `is_weekend`, `day_type` (`weekday`/`weekend`), `iso_week`. Date only; hour lives on the facts. |
| `dim_route` | one row per OSM route relation, `route_id` | `routes` — all 52 snapshots | SCD type 1: newest snapshot's attributes win; `first_seen_date`, `last_seen_date`, `snapshots_present` keep the history. `is_trunk` (matches the BRT pattern, 20), `is_sampled` (has traffic, 14), `stop_count`, `sequence_source` (`osm_member` / `geometry`). |
| `dim_stop` | one row per OSM stop node, `stop_id` | `stops` — all snapshots | Same SCD type 1 treatment. `route_count` from the latest membership snapshot — the stop-density / overlap signal. |
| `dim_weather_condition` | one row per OpenWeather condition, `condition_key` (`rain/light_rain`) | `weather` | `weather_main`, `weather_description`, `is_precipitation`, `raw_rows` (how rare it is). 8 rows today. |

### Facts

| Table | Grain / primary key | Foreign keys | Notes |
| --- | --- | --- | --- |
| `fct_weather` | one row per 3-hour UTC slot, `weather_ts` | `date_key`, `condition_key` | Best source per slot: a `current` observation if one rounds to it, else the shortest-lead forecast. Provenance on every row: `record_type`, `lead_hours`, `offset_minutes`, `observed_at`, `fetched_at`. `rain_mm` + `rain_period_hours` (1 h for observations, 3 h for forecasts — the raw column name hides this). |
| `fct_corridor_travel_time` | one row per corridor per sampled hour, (`route_id`, `utc_ts`) | `route_id`, `date_key`, `weather_ts`, `condition_key` | The hub. Traffic measures (`duration_seconds`, `distance_meters`, `mean_speed_kmh`, `congestion_*`, `congestion_known_pct`), local time (`local_ts`, `local_date`, `local_hour`, `local_weekday`), provenance (`sequence_source`, `stops_excluded`), per-day coverage (`hours_sampled_that_day`, `is_complete_day`) that replaces the notebook's hard-coded window, and the weather for the nearest 3-hour slot denormalised onto the row. Partitioned by `local_date`. |
| `fct_route_stop` | one row per route × position, (`route_id`, `stop_sequence`) | `route_id`, `stop_id` | Latest snapshot (961 rows, 119 routes, 486 stops). `member_role` and the recovery columns (`sequence_source`, `member_index`, `distance_along_m`, `snap_distance_m`) when present. The grain is the position, not the stop: one OSM relation lists the same node twice, so count `stop_id` distinct when counting stops. Coverage and overlap analysis. |
| `fct_network_snapshot` | one row per snapshot day, `snapshot_date` | `date_key` | `routes`, `stops`, `route_stops`, `routes_with_stops`, `trunk_routes`, and against the previous snapshot: `days_since_previous`, `routes_added` / `routes_removed`, `stops_added` / `stops_removed`. Where the flapping shows as ±1–2 on 14 of 52 days. |

### Marts

| Table | Grain / primary key | Foreign keys | Answers |
| --- | --- | --- | --- |
| `agg_corridor_hourly_profile` | (`route_id`, `local_hour`, `day_type`), `day_type` ∈ `all` / `weekday` / `weekend` | `route_id` | Objectives 1 and 4: `n`, `sample_days`, median / p10 / p90 / mean of duration and speed, `distance_median_m`, the corridor's overnight baseline (local 00–04) and `ratio_to_baseline`, `is_peak` (hourly median above the corridor's median over all hours of that day type), `meets_min_samples` (n ≥ 3 — weekend cells have n = 2 and are flagged, not interpolated). 1,008 rows. |
| `agg_corridor_reliability` | `route_id` | `route_id` | Objectives 2 and 3, rank 1 = worst: `spread_duration_pct` / `spread_speed_pct` (peak vs overnight baseline) with `peak_hour_local`, `trough_hour_local` and both ranks plus `spread_ranks_agree`; `cv_within_hour_duration` / `cv_within_hour_speed` (mean over hours of stddev ÷ mean) with both ranks plus `cv_ranks_agree`; `distance_spread_pct` (the rerouting confound, in the open); `sample_days`, `is_ranked` (≥ 5 days; ranks null otherwise). 14 rows. |

### Relationships the tests must hold

Redshift records primary and foreign keys but does not enforce them, so the
build's tests are the only guarantee:

- every primary key above is unique and non-null;
- every `route_id` in the facts and marts exists in `dim_route`;
- every `stop_id` in `fct_route_stop` exists in `dim_stop`;
- every `date_key` in the facts exists in `dim_date`;
- every `weather_ts` in `fct_corridor_travel_time` exists in `fct_weather`, and every
  `condition_key` in `dim_weather_condition`;
- `fct_corridor_travel_time` has exactly one row per (`route_id`, `utc_ts`) — 3,108 on
  the raw-zone export, 2,352 of them with `is_complete_day = true`.

### Status

| Step | Tables | State |
| --- | --- | --- |
| 2 | `dim_date`, `dim_route`, `dim_stop` | built |
| 3 | `fct_weather`, `dim_weather_condition` | built |
| 4 | `fct_corridor_travel_time` | built |
| 5 | `fct_route_stop`, `fct_network_snapshot` | built |
| 6 | `agg_corridor_hourly_profile`, `agg_corridor_reliability` | built |
| 7 | `build` / `verify` CLIs, `tests/gold/` (44 tests) | built |
| 8 | docs: `GOLD_LAYER.md` model reference, README, PROJECT_PLAN, Redshift load | built |

---

## Workflow

```
raw zone (S3, or s3_raw/ / data/local/raw locally)
        │
        ▼
processing/gold/build.py  ── PySpark, full rebuild ──►  gold/<table>/*.parquet
        │                                               (S3, or data/local/gold)
        ▼
Redshift  COPY ... FORMAT AS PARQUET   (or Spectrum external schema over gold/)
        │
        ▼
Metabase dashboards
```

Daily, once deployed: the ingestion Lambdas land `day=<today>`, the gold build
re-reads history plus today and rewrites the tables, Redshift reloads them,
and every metric ("corridor vs its own median") is recomputed over the whole
accumulated history.

1. **Explore in the notebook.** `notebooks/01_exploring.ipynb` runs on
   Databricks or plain PySpark (`QUITO_RAW_ROOT=/path/to/s3_raw`). Findings are
   recorded in the notebook's **Finding** blocks; they are the data-quality
   record.
2. **Codify in the gold build**, one transformation per module, with tests.
   A notebook finding that never becomes a gold table does not reach the
   dashboard.
3. **Verify** (below), then load to Redshift and build the Metabase questions
   against the gold tables.
4. **Update `docs/PROJECT_PLAN.md`'s Analytical Questions table** as each
   objective is answered, replacing "data needed" with the finding and the
   table that computes it.

---

## Verifying the implementation

Three levels, cheapest first. All run without AWS.

**1. PySpark itself works** — a Spark session starts on the local JVM and
reads Parquet:

```bash
python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[1]').getOrCreate(); print(s.version, s.range(3).count()); s.stop()"
```
Expected: `3.5.8 3`. Java 17 or 21 must be on `PATH` (`java -version`).

**2. Tests are green:**

```bash
pytest tests/gold -v
```

**3. The build runs end to end and the output is sane:**

```bash
# against the committed samples (no raw zone needed)
python -m processing.gold.build --layout samples --gold-root data/local/gold
# against a raw-zone export
python -m processing.gold.build --raw-root s3_raw --gold-root data/local/gold
python -m processing.gold.verify --gold-root data/local/gold
```

`verify` prints row counts per table and the referential-integrity checks
(32 in all) and exits non-zero on any failure. Expected on the `s3_raw`
export: `fct_corridor_travel_time` 3,108 rows (2,352 inside the complete-day
window), `dim_route` 515, `dim_stop` 486, 14 sampled corridors, 0 orphan keys.

Environment setup from scratch — sandbox, dev container, Windows natively,
Databricks — is in [`docs/GOLD_LAYER.md`](GOLD_LAYER.md#1-running-the-build-from-scratch).
The dev container now ships Java 17 and pyspark, which is the easiest path on
Windows; native Windows additionally needs Hadoop's `winutils.exe` / `hadoop.dll`
to write Parquet.

---

## Out of scope for this pass

- Re-enabling traffic collection — the analysis works with the 13 days that exist.
- The per-stop weather enrichment (`docs/PROJECT_PLAN.md`) — a `weather_loader.py` change.
- SCD Type 2 history on the dimensions — `fct_network_snapshot` keeps enough to add it later.
- Redeploying the Lambda image so the corridor-recovery columns start landing —
  an infrastructure step, tracked separately; the gold build is ready for it.
- dbt models — superseded by the gold build; the `dbt/` folder stays until the
  Redshift load is in place and is then removed or retargeted at the gold tables.
