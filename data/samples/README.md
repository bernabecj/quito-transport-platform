# Sample datasets

Real data, captured once, committed so the downstream layers — Glue jobs, dbt
models, dashboards — can be developed without calling any API. Nothing here is
synthetic or hand-edited.

Total size is under 100 KB.

## What is here

| Path | Rows | Source | Captured |
| ---- | ---- | ------ | -------- |
| `network/routes.parquet` | 515 | OpenStreetMap via Overpass | 2026-08-19 |
| `network/stops.parquet` | 485 | OpenStreetMap via Overpass | 2026-08-19 |
| `network/route_stops.parquet` | 960 | OpenStreetMap via Overpass | 2026-08-19 |
| `traffic/corridor_travel_time.parquet` | 14 | Mapbox Directions `driving-traffic` | 2026-08-19 ~19:30 local |

## Read this before using the traffic sample

**It is a single snapshot, not a time series.** One row per corridor, all
sharing one `sampled_at`. That is enough to develop schemas, transformations and
tests against, but it cannot answer the questions the pipeline exists for.

Peak hours, worst corridors and travel-time variance all compare a corridor
against *its own history*. With one timestamp there is no history, so those
metrics are undefined here. They only become computable once sampling runs on a
schedule and snapshots accumulate — see `ingestion/traffic_loader.py`, which is
disabled by default.

**Only 14 of 18 trunk corridors are present.** Four Ecovía relations
(E1 Las Universidades → T. Sur, E1R Terminal El Recreo → T. Sur, and E3 in both
directions) list their stop members in non-geographic order in OpenStreetMap.
Consecutive "stops" on those routes are up to 15.9 km apart, so routing through
them measures a zigzag across the city rather than the corridor. The loader
excludes any corridor whose mean stop spacing exceeds 1 km; the surviving 14 sit
between 0.53 and 0.81 km, which is normal BRT spacing.

The exclusion is a data-quality gate, not a bug being hidden. Fixing it properly
means either correcting the relations upstream in OSM, or deriving stop order by
projecting stops onto the route's way geometry, which this pipeline does not
currently ingest.

## Regenerating

The network sample is free to regenerate — Overpass needs no credentials:

```python
from datetime import datetime, timezone
from ingestion.network_loader import fetch_network, parse_network

for name, df in parse_network(fetch_network(), datetime.now(timezone.utc)).items():
    df.to_parquet(f"data/samples/network/{name}.parquet", index=False)
```

The traffic sample costs 14 Mapbox requests against a 100,000/month free tier.
It needs `MAPBOX_TOKEN` in `.env`, and re-running it replaces the snapshot
rather than appending, so the file stays a single point in time.
