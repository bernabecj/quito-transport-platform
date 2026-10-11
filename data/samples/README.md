# Sample datasets

Real data, committed so the downstream layers — Glue jobs, dbt models,
dashboards — can be developed without calling any API. Nothing here is
synthetic or hand-edited.

Total size is under 200 KB.

## What is here

| Path | Rows | Source | Captured |
| ---- | ---- | ------ | -------- |
| `network/routes.parquet` | 515 | OpenStreetMap via Overpass | 2026-08-19 |
| `network/stops.parquet` | 485 | OpenStreetMap via Overpass | 2026-08-19 |
| `network/route_stops.parquet` | 960 | OpenStreetMap via Overpass | 2026-08-19 |
| `traffic/corridor_travel_time.parquet` | 14 | Mapbox Directions `driving-traffic` | 2026-08-19 ~19:30 local |
| `traffic/corridor_travel_time_series.parquet` | 2 352 | Mapbox Directions `driving-traffic`, exported from the raw S3 zone | hourly, 2026-08-25 → 2026-08-31 local |

## The two traffic files are not interchangeable

`corridor_travel_time.parquet` is a **single snapshot** — one row per corridor,
all sharing one `sampled_at`. It is enough to develop schemas and row-level
transformations against, but every metric the pipeline exists for compares a
corridor against *its own history*, and with one timestamp there is no history.
Peak hours, worst corridors and travel-time variance are all undefined here.

`corridor_travel_time_series.parquet` is the series those metrics need: 14
corridors x 24 hours x 7 days, one complete local week. Use it for anything
that aggregates across time — the dbt models, their tests, and the Great
Expectations suite.

It carries the raw ingested shape, not a cleaned one. `sampled_at` is still a
string, the `year` / `month` / `day` / `hour` partition columns are still
present, and `hour` is **UTC** while `local_hour` is Quito time (UTC-5, no
DST). Deriving local time is the staging model's job.

Two quirks will bite anyone developing against it. Both are established, not
suspected:

- **`congestion_*` coverage is structured by time of day.** A mean of 1.8 % of
  segments reported congestion at 04:00 local, against 56.5 % at 21:00. The
  missingness tracks the variable under study, so these columns cannot carry an
  hourly comparison. `mean_speed_kmh` and `duration_seconds` are the real
  payload.
- **`distance_meters` is not stable per corridor.** Mapbox reroutes between
  calls even though the waypoints are fixed — a min-to-max spread of 0.0 % on
  E1R up to 20.2 % on one C6 relation. Speed is therefore not directly
  comparable across time for the high-spread corridors, and a corridor that
  reroutes shows artificially *lower* duration variance because the reroute is
  what avoided the delay.

[`docs/ANALYSIS_PLAN.md`](../../docs/ANALYSIS_PLAN.md) is the spec for what this
data supports and what it does not. The evidence behind both points above is in
[`notebooks/01_exploring.ipynb`](../../notebooks/01_exploring.ipynb), sections 8
and 9. Read the plan before modelling.

## Only 14 of 18 trunk corridors are present

This applies to both traffic files. Four Ecovía relations (E1 Las Universidades
→ T. Sur, E1R Terminal El Recreo → T. Sur, and E3 in both directions) list
their stop members in non-geographic order in OpenStreetMap. Consecutive
"stops" on those routes are up to 15.9 km apart, so routing through them
measures a zigzag across the city rather than the corridor. The loader excludes
any corridor whose mean stop spacing exceeds 1 km; the surviving 14 sit between
0.53 and 0.81 km, which is normal BRT spacing.

The exclusion is a data-quality gate, not a bug being hidden. Fixing it
properly means either correcting the relations upstream in OSM, or deriving stop
order by projecting stops onto the route's way geometry, which this pipeline
does not currently ingest.

## Regenerating

The network sample is free to regenerate — Overpass needs no credentials:

```python
from datetime import datetime, timezone
from ingestion.network_loader import fetch_network, parse_network

for name, df in parse_network(fetch_network(), datetime.now(timezone.utc)).items():
    df.to_parquet(f"data/samples/network/{name}.parquet", index=False)
```

The traffic snapshot costs 14 Mapbox requests against a 100,000/month free
tier. It needs `MAPBOX_TOKEN` in `.env`, and re-running it replaces the
snapshot rather than appending, so the file stays a single point in time.

The traffic series **cannot be re-fetched.** Mapbox returns current conditions
only, so this hourly history exists solely because `ingestion/traffic_loader.py`
ran on a schedule for 13 days, and collection stopped on 2026-09-02. The file is
an export of `s3://quito-transport-platform/raw/traffic/`, reproducible from
`notebooks/01_exploring.ipynb` section 7 onward for as long as that raw zone
survives, and not otherwise. Treat it as the irreplaceable artifact it is.
