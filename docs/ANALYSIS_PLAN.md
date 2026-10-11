# Traffic Analysis Plan

Collection stopped on 2026-09-02 after 13 days (`chore: sample traffic hourly,
then stop collection after 13 days`). This is the plan for turning that data
into the platform's actual findings: objectives here, method and execution are
yours, in Databricks notebooks against the raw S3 zone, with results codified
into dbt once a pattern is confirmed.

---

## What exists

| Dataset | Location | Coverage |
| --- | --- | --- |
| Corridor travel time | `s3://quito-transport-platform/raw/traffic/year=.../month=.../day=.../hour=.../corridor_travel_time.parquet` | 3,108 rows, 14 corridors, 222 samples, 2026-08-20 → 2026-09-02. 6-hourly through 08-24, hourly from 08-25 to 09-01 (8 full hourly days), partial at both edges. |
| Network (routes/stops/route_stops) | `s3://quito-transport-platform/raw/network/year=.../month=.../day=...` | Daily since 2026-08-19. Static — join in the corridor's own snapshot, not the latest one, if route topology ever changes. |
| Weather | `s3://quito-transport-platform/raw/weather/year=.../month=.../day=.../weather.parquet` | Sparse before the schedule was regularized (gaps in June–August), then daily. `current` + `forecast` rows mixed in one file — filter `record_type == "current"` for observed conditions. |

**Cell density**: grouping by (corridor × local_hour × weekday-or-weekend), the
median cell has 5 observations, the minimum is 2 (edge hours from the 6-hourly
period). 12 weekdays, 6 weekend days. Treat any single-digit-n comparison as
suggestive, not a claim — say so in whatever you publish rather than smoothing
over it.

**Known data quirks** (don't rediscover these from scratch):
- `congestion_*` annotation columns are ~62% `unknown` and the rest almost
  entirely `low` — not usable as a signal. Use `duration_seconds` /
  `mean_speed_kmh` as the real payload.
- `distance_meters` varies run to run (Mapbox reroutes) even though waypoints
  are fixed. This correlates *negatively* with `duration_seconds` variance
  (Spearman ≈ −0.87) — a corridor that reroutes shows artificially *lower*
  duration variance because the reroute is what avoided the delay. If you rank
  corridors by predictability, check whether the ranking survives using
  `mean_speed_kmh` variance instead of raw duration variance.
- Route ordering in `route_stops` is what makes a corridor's waypoints
  geographic; `traffic_loader.py` already drops relations that fail its
  `CORRIDOR_MAX_MEAN_HOP_KM` scrambling check, so all 14 corridors in the
  traffic table are trustworthy on that front.

---

## Objectives

These replace the traffic-related rows in `docs/PROJECT_PLAN.md`'s Analytical
Questions table, scoped to what 13 days of this specific dataset can actually
support. Each has a concrete pass/fail bar so "we found something" means the
same thing every time.

1. **Daily congestion curve.** For each corridor, characterize how travel
   time/speed moves across the 24-hour cycle. Bar: a curve built from cells
   with n ≥ 3; call out which hours don't clear that bar instead of silently
   interpolating them.

2. **Peak-to-off-peak spread, ranked.** Quantify how much worse each
   corridor's peak is relative to its own overnight baseline, and rank
   corridors by it. Bar: state whether the ranking is stable under both
   `duration_seconds` and `mean_speed_kmh` variance — if it flips, say so
   rather than picking whichever version reads better.

3. **Least predictable corridors.** Rank corridors by variability at a fixed
   time of day (not pooled across hours, which conflates "unpredictable" with
   "measured at more different times"). Bar: control for the rerouting
   confound above before publishing a ranking.

4. **Weekday vs. weekend pattern.** Determine whether any corridor's daily
   curve differs meaningfully between weekdays and weekends. Bar: with only 6
   weekend days, treat this as a preliminary read, not a headline claim —
   flag it as such in whatever you produce.

5. **Weather correlation (exploratory only).** Check whether rain or low
   visibility coincides with slower corridors. Bar: this is a stretch goal.
   Weather is a single city-centre point (see the planned per-stop enrichment
   in `docs/PROJECT_PLAN.md`), sample size for rain events specifically will
   be small, and correlation here does not imply causation — treat a positive
   finding as a hypothesis for later validation, not a conclusion.

Each objective should end with: the finding, the corridors/hours it's
strongest and weakest for, and the caveats above applied to that specific
result — not just a chart.

---

## Suggested workflow

**1. Databricks notebook, reading raw S3 directly** — this is exploration
before the transformation layer exists, so there's no need to wait on dbt.
Point Databricks (or any Spark/pandas environment with S3 read access) at
`s3://quito-transport-platform/raw/{traffic,network,weather}/`. The Parquet
partitioning (`year=/month=/day=/hour=`) is Hive-style, so a Spark read of the
whole `raw/traffic/` prefix will expose `year`, `month`, `day`, `hour` as
columns automatically — filter/join on those rather than parsing the S3 key.

Credentials: same IAM access this AWS account already has (`adm_user` or an
equivalent role scoped to read the `quito-transport-platform` bucket) — no new
secret needed for read-only EDA.

**2. Explore freely, but keep the notebook reproducible.** Whatever ends up
matters — cell outputs, one-off `.head()` calls — but the transformations that
feed a finding you intend to keep should run top-to-bottom without manual
steps, since step 3 will lift them into SQL.

**3. Once a pattern is confirmed, formalize it in dbt, not just the
notebook.** The stub models already exist and describe their own grain:

- [`dbt/models/staging/stg_corridor_travel_time.sql`](../dbt/models/staging/stg_corridor_travel_time.sql) —
  casts raw types, derives local hour/weekday. Do **not** implement the
  congestion-coverage filter its current comment describes — that comment
  predates this analysis and would delete the overnight baseline (11% known
  vs. 58% at midday). Leave congestion filtering out entirely.
- [`dbt/models/marts/fct_corridor_travel_time.sql`](../dbt/models/marts/fct_corridor_travel_time.sql) —
  grain is `route_id x sampled_at`, meaningful once aggregated across the
  accumulated series, which it now has.

A notebook finding that never becomes a dbt model doesn't reach Metabase, so
it doesn't reach the public dashboard the README describes as the point of
the project. Treat the notebook as scratch work and dbt as the deliverable
for anything you want on the dashboard.

**4. Update `docs/PROJECT_PLAN.md`'s Analytical Questions table** once an
objective is answered, replacing "data needed" with the actual finding or a
link to the dbt model that computes it.

---

## Out of scope for this pass

- Re-enabling collection to fill weekend gaps — you've decided to work with
  what exists.
- The planned per-stop weather enrichment (`docs/PROJECT_PLAN.md`) — a
  separate `weather_loader.py` change, not a traffic analysis task.
- Great Expectations suites for the traffic dataset — none exist yet
  (`quality/expectations/` only covers network and weather). Worth doing
  once the staging model is real, not before.
