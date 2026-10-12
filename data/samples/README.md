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
| `network/overpass_trunk_geom.json` | 20 relations, 173 stops, road geometry | OpenStreetMap via overpass-turbo (GeoJSON export; see below) | 2026-10-12 |
| `network/overpass_trunk_geom.overpassql` | — | the query that regenerates the file above | — |
| `traffic/corridor_travel_time.parquet` | 14 | Mapbox Directions `driving-traffic` | 2026-08-19 ~19:30 local |
| `traffic/corridor_travel_time_series.parquet` | 2 352 | Mapbox Directions `driving-traffic`, exported from the raw S3 zone | hourly, 2026-08-25 → 2026-08-31 local |
| `weather/weather.parquet` | 574 | OpenWeather, exported from the raw S3 zone | daily call, 2026-08-20 → 2026-09-02 |

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

## The weather sample is mostly forecasts

`weather/weather.parquet` holds one API call per day at 05:00 Coordinated
Universal Time, which is midnight local. Each call wrote one observation plus a
five day forecast at three hour steps, so the file is 14 rows of
`record_type = "current"` and 560 rows of `record_type = "forecast"`. Filter on
`record_type` before treating anything as observed.

Two consequences. Only 14 real observations exist across 14 days, all at
midnight local, which is not enough for an hourly join. And the forecasts
recover an hourly series only if you keep the shortest lead forecast per target
time, which means retaining lead time as a column so the provenance stays
honest. Section 4 of
[`notebooks/01_exploring.ipynb`](../../notebooks/01_exploring.ipynb) works this
through.

Weather has no spatial dimension. It is a single observation point for the
whole city, so any join to traffic or network data is time only, and nothing
can be said about weather varying by route or stop.

## The traffic files hold 14 of 18 trunk corridors

Four Ecovía relations (E1 Las Universidades → T. Sur, E1R Terminal El Recreo →
T. Sur, and E3 in both directions) list their stop members in non-geographic
order in OpenStreetMap. Consecutive "stops" on those routes are up to 15.9 km
apart, so routing through them measures a zigzag across the city rather than
the corridor. `traffic_loader` excludes any corridor whose mean stop spacing
exceeds 1 km (`CORRIDOR_MAX_MEAN_HOP_KM`); the surviving 14 sit between 0.53
and 0.81 km, which is normal BRT spacing. The traffic series was collected
under that rule and cannot be backfilled, so it stays at 14.

Since 2026-10 `network_loader` repairs those relations instead of losing them.
It asks Overpass for the trunk corridors' road geometry as well, chains each
relation's ways into a polyline (`ingestion/geometry.py`), projects every stop
onto it, and uses the distance along the road as the stop order **when that
orders the stops more tightly than the OSM member list does**. Relations whose
member list is already geographic are left exactly as they were; that includes
the two MetroBus relations, whose mapped road has holes and would be ordered
worse by geometry. `route_stops` says which happened per route
(`sequence_source` = `osm_member` or `geometry`), keeps OSM's position in
`member_index`, and carries `distance_along_m` / `snap_distance_m` per stop.

Three stops are members of an E3 relation without being on its road — Teatro
México and Colegio Montúfar lie south of Playón de la Marín, the relation's
own terminus; "San Cristóbal" is an E4/E6 stop 6.8 km away. They stay in
`route_stops` but `traffic_loader` does not send them to Mapbox as waypoints
(`CORRIDOR_MAX_SNAP_M`). With that, all 18 trunk corridors pass the gate, the
four recovered ones at 0.51–0.68 km. The committed network parquet predates
this and has no provenance columns; readers must treat their absence as
`osm_member`, which `traffic_loader` does.

`python scripts/check_corridors.py --network-dir data/samples/network` shows
the 14-of-18 state the parquet is in; `--overpass
data/samples/network/overpass_trunk_geom.json` shows the 18-of-18 the loader
now produces from the same stops.

## The trunk geometry fixture

`network/overpass_trunk_geom.json` exists so the stop-ordering code can be
developed and tested against the real relations without calling Overpass. The
loader consumes the raw Overpass API shape (`{"elements": [...]}`) — relations
with their member lists, nodes with tags, and the trunk corridors' ways with
coordinates from `out geom`. `network/overpass_trunk_geom.overpassql` is the
query that produces exactly that; it is `build_query(name_pattern=TRUNK_ROUTE_PATTERN)`
verbatim, and `tests/test_network_loader.py` fails if the two drift apart.

The file committed on 2026-10-12 is **not yet in that shape**. It is an
overpass-turbo GeoJSON export (`{"features": [...]}`) of the same query:
overpass-turbo had already merged each relation's ways into one line per
relation, dropped the member lists and the node tags. `scripts/check_corridors.py`
and `tests/test_trunk_fixture.py` accept it by rebuilding an approximate
payload — stop order from `route_stops.parquet`, one pseudo-way per line part
in whatever orientation overpass-turbo left it. Everything about *which* stops
come in which order is faithful on that input; the *direction* of a route is
not, and the one test that depends on it is skipped until the raw response is
committed. The raw response needs no conversion and drops straight in.

## Regenerating

The network sample is free to regenerate — Overpass needs no credentials:

```python
from datetime import datetime, timezone
from ingestion.network_loader import fetch_network, parse_network

for name, df in parse_network(fetch_network(), datetime.now(timezone.utc)).items():
    df.to_parquet(f"data/samples/network/{name}.parquet", index=False)
```

Doing so adds the four provenance columns to `route_stops.parquet` and changes
the row counts in the table above; update them together.

The trunk geometry fixture is regenerated, in the raw API shape, by either of

```bash
python scripts/check_corridors.py --fetch --save data/samples/network/overpass_trunk_geom.json

curl -sS -H "User-Agent: quito-transport-platform/1.0" \
     --data-urlencode "data@data/samples/network/overpass_trunk_geom.overpassql" \
     https://overpass-api.de/api/interpreter > data/samples/network/overpass_trunk_geom.json
```

from a machine that can reach `overpass-api.de`. `tests/test_trunk_fixture.py`
pins the outcome to the 2026-10-12 snapshot by route and stop id; if OSM has
changed underneath (a relation fixed upstream, a stop moved), re-baseline the
test and this file together rather than loosening the assertions.

The traffic snapshot costs 14 Mapbox requests against a 100,000/month free
tier. It needs `MAPBOX_TOKEN` in `.env`, and re-running it replaces the
snapshot rather than appending, so the file stays a single point in time.

The traffic series **cannot be re-fetched.** Mapbox returns current conditions
only, so this hourly history exists solely because `ingestion/traffic_loader.py`
ran on a schedule for 13 days, and collection stopped on 2026-09-02. The file is
an export of `s3://quito-transport-platform/raw/traffic/`, reproducible from
`notebooks/01_exploring.ipynb` section 7 onward for as long as that raw zone
survives, and not otherwise. Treat it as the irreplaceable artifact it is.
