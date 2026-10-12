#!/usr/bin/env python3
"""
Which trunk corridors would the traffic loader sample, and why?

Runs the network snapshot through the same code the pipeline uses —
network_loader.parse_network, then traffic_loader.build_corridors — and prints
one line per trunk relation: where its stop order came from, the mean stop
spacing before and after, which stops were left out of the waypoints, and
whether it passes the CORRIDOR_MAX_MEAN_HOP_KM gate. No Mapbox requests.

Usage:
    # a saved Overpass response (raw API JSON, or an overpass-turbo GeoJSON
    # export — see "Two fixture shapes" below)
    python scripts/check_corridors.py --overpass data/samples/network/overpass_trunk_geom.json

    # query Overpass live for the trunk corridors, optionally saving the
    # response — this is how the fixture is regenerated
    python scripts/check_corridors.py --fetch
    python scripts/check_corridors.py --fetch --save data/samples/network/overpass_trunk_geom.json

    # an already-parsed snapshot (routes/stops/route_stops parquet), e.g. the
    # committed sample, to see what the loader did with it
    python scripts/check_corridors.py --network-dir data/samples/network

Exit code 0 when every trunk corridor with two or more stops passes the gate,
1 otherwise.

Two fixture shapes
  The pipeline consumes raw Overpass API JSON ({"elements": [...]}), which is
  what --fetch --save writes. The file committed on 2026-10-12 is instead an
  overpass-turbo GeoJSON export ({"features": [...]}): overpass-turbo has
  already merged each relation's ways into LineStrings, dropped the member
  list and the node tags. When given that shape, this script rebuilds an
  approximate payload so the check can run offline:

    - each LineString part becomes one pseudo-way (negative ids), in the
      orientation overpass-turbo left it — which is arbitrary, so direction
      warnings from this input mean nothing;
    - stop member order is taken from route_stops.parquet in the same folder
      (the OSM order captured 2026-08-19), stop names from stops.parquet;
    - ways are pre-merged, so joining on shared node ids is not exercised,
      only joining on proximity.

  Mean hops, the chosen order, excluded stops and the pass/fail verdict are
  meaningful on this input. Direction is not. The raw response, once fetched,
  replaces the approximation with the real thing and needs no conversion.
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.constants import (  # noqa: E402
    CORRIDOR_MAX_MEAN_HOP_KM,
    CORRIDOR_MAX_SNAP_M,
    TRUNK_ROUTE_PATTERN,
)
from ingestion.geometry import mean_hop_km  # noqa: E402
from ingestion.network_loader import (  # noqa: E402
    build_query,
    build_route_geometries,
    fetch_network,
    parse_network,
)
from ingestion.traffic_loader import build_corridors  # noqa: E402


def load_payload(args) -> dict | None:
    if args.fetch:
        payload = fetch_network(build_query(name_pattern=TRUNK_ROUTE_PATTERN))
        if args.save:
            Path(args.save).write_text(json.dumps(payload, ensure_ascii=False, indent=1))
            print(f"saved {len(payload['elements'])} elements to {args.save}")
        return payload
    if args.overpass:
        path = Path(args.overpass)
        payload = json.loads(path.read_text(encoding="utf-8"))
        if "elements" in payload:
            if not any(e["type"] == "way" for e in payload["elements"]):
                print("note: no way elements in the response, so no route has geometry", file=sys.stderr)
            return payload
        if payload.get("type") == "FeatureCollection":
            print(
                f"note: {path.name} is an overpass-turbo GeoJSON export, not a raw Overpass "
                "response. Rebuilding an approximate payload from it (stop order from "
                f"{path.parent / 'route_stops.parquet'}); way orientation is arbitrary, so "
                "ignore direction warnings. Regenerate with --fetch --save when online.",
                file=sys.stderr,
            )
            return payload_from_turbo_geojson(payload, path.parent)
        sys.exit(f"{args.overpass}: neither an Overpass response nor a GeoJSON export")
    return None


def payload_from_turbo_geojson(geojson: dict, snapshot_dir: Path) -> dict:
    """Approximate an Overpass `out body` + `out geom` response from an
    overpass-turbo GeoJSON export. See the module docstring for what is and
    is not faithful."""
    route_stops = pd.read_parquet(snapshot_dir / "route_stops.parquet")
    stop_names = pd.read_parquet(snapshot_dir / "stops.parquet").set_index("stop_id")["stop_name"]
    features = geojson["features"]
    points = {f["id"].split("/")[1]: f for f in features if f["id"].startswith("node/")}

    elements = []
    next_id = 0  # synthetic ids are negative so they can never be mistaken for OSM's

    for feature in (f for f in features if f["id"].startswith("relation/")):
        rel_id = feature["id"].split("/")[1]
        geometry = feature["geometry"]
        parts = [geometry["coordinates"]] if geometry["type"] == "LineString" else geometry["coordinates"]
        members = []
        for part in parts:
            next_id -= 1
            node_ids = list(range(next_id, next_id - len(part), -1))
            next_id -= len(part)
            elements.append(
                {
                    "type": "way",
                    "id": node_ids[0],
                    "nodes": node_ids,
                    "geometry": [{"lon": lon, "lat": lat} for lon, lat in part],
                }
            )
            members.append({"type": "way", "ref": node_ids[0], "role": ""})

        snapshot = route_stops[route_stops["route_id"] == rel_id].sort_values("stop_sequence")
        if not snapshot.empty:
            stop_members = [(s.stop_id, s.member_role) for s in snapshot.itertuples()]
        else:  # relation not in the snapshot: fall back to file order
            stop_members = [
                (node_id, rel["role"])
                for node_id, point in points.items()
                for rel in point["properties"].get("@relations", [])
                if str(rel["rel"]) == rel_id
            ]
        members += [
            {"type": "node", "ref": int(stop_id), "role": role}
            for stop_id, role in stop_members
            if stop_id in points
        ]
        tags = {k: v for k, v in feature["properties"].items() if not k.startswith("@")}
        elements.append({"type": "relation", "id": int(rel_id), "tags": tags, "members": members})

    for node_id, point in points.items():
        lon, lat = point["geometry"]["coordinates"]
        node = {"type": "node", "id": int(node_id), "lon": lon, "lat": lat}
        name = stop_names.get(node_id)
        if isinstance(name, str):
            node["tags"] = {"name": name}
        elements.append(node)

    return {"elements": elements}


def load_snapshot(args, payload: dict | None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if payload is not None:
        dfs = parse_network(payload, datetime.now(timezone.utc))
        return dfs["routes"], dfs["stops"], dfs["route_stops"]
    folder = Path(args.network_dir)
    return tuple(pd.read_parquet(folder / f"{name}.parquet") for name in ("routes", "stops", "route_stops"))


def describe(routes, stops, route_stops, geometries, corridors) -> list[dict]:
    trunk = routes[routes["route_name"].fillna("").str.contains(TRUNK_ROUTE_PATTERN, case=False)]
    coords = stops.set_index("stop_id")[["longitude", "latitude"]]
    sampled = {c["route_id"]: c for c in corridors}
    has_provenance = "sequence_source" in route_stops.columns

    rows = []
    for route in trunk.sort_values("route_name").itertuples():
        members = route_stops[route_stops["route_id"] == route.route_id]
        members = members[members["stop_id"].isin(coords.index)]
        if len(members) < 2:
            continue
        xy = lambda frame: [tuple(coords.loc[s]) for s in frame["stop_id"]]  # noqa: E731

        osm_order = members.sort_values("member_index" if has_provenance else "stop_sequence")
        final_order = members.sort_values("stop_sequence")
        source = members["sequence_source"].iloc[0] if has_provenance else "osm_member"
        excluded = []
        if has_provenance and source == "geometry":
            excluded = members[members["snap_distance_m"] > CORRIDOR_MAX_SNAP_M]["stop_id"].tolist()
        corridor = sampled.get(route.route_id)
        geometry = geometries.get(route.route_id)

        rows.append(
            {
                "ref": route.route_ref,
                "name": route.route_name,
                "stops": len(members),
                "source": source,
                "hop_osm": mean_hop_km(xy(osm_order)),
                "hop_used": corridor["mean_hop_km"] if corridor else mean_hop_km(xy(final_order)),
                "excluded": excluded,
                "road_km": geometry.spine_length_km if geometry else None,
                "unattached_km": geometry.unattached_length_km if geometry else None,
                "passes": corridor is not None,
            }
        )
    return rows


def print_table(rows: list[dict]) -> None:
    print(
        f"{'ref':<6} {'name':<44} {'stops':>5} {'source':<10} {'hop OSM':>7} {'hop used':>8} "
        f"{'road km':>7} {'holes':>6} {'excl':>4}  result"
    )
    for r in rows:
        road = f"{r['road_km']:7.1f}" if r["road_km"] is not None else f"{'-':>7}"
        holes = f"{r['unattached_km']:6.1f}" if r["unattached_km"] else f"{'-':>6}"
        print(
            f"{str(r['ref'])[:6]:<6} {r['name'][:44]:<44} {r['stops']:>5} {r['source']:<10} "
            f"{r['hop_osm']:7.2f} {r['hop_used']:8.2f} {road} {holes} {len(r['excluded']):>4}  "
            f"{'PASS' if r['passes'] else 'DROP'}"
        )
        for stop_id in r["excluded"]:
            print(f"{'':<6} {'':<44} excluded from waypoints: stop {stop_id}")
    passing = sum(r["passes"] for r in rows)
    print(
        f"\n{passing} of {len(rows)} trunk corridors pass the {CORRIDOR_MAX_MEAN_HOP_KM:.1f} km "
        f"mean-hop gate (stops > {CORRIDOR_MAX_SNAP_M} m from the road are not waypoints)"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--overpass", metavar="FILE", help="saved Overpass JSON response")
    source.add_argument("--fetch", action="store_true", help="query Overpass for the trunk corridors")
    source.add_argument("--network-dir", metavar="DIR", help="folder with routes/stops/route_stops.parquet")
    parser.add_argument("--save", metavar="FILE", help="with --fetch: write the response here")
    parser.add_argument("-q", "--quiet", action="store_true", help="hide loader warnings")
    args = parser.parse_args()

    logging.basicConfig(format="%(levelname)s %(message)s")
    # The loaders pin their own level at import, so set them, not just root.
    for name in ("ingestion.network_loader", "ingestion.traffic_loader"):
        logging.getLogger(name).setLevel(logging.ERROR if args.quiet else logging.INFO)

    payload = load_payload(args)
    routes, stops, route_stops = load_snapshot(args, payload)
    corridors = build_corridors(routes, stops, route_stops)
    # parse_network already chained and reported the geometry; this second
    # pass is only for the road-length columns, so keep it quiet.
    logging.getLogger("ingestion.network_loader").setLevel(logging.ERROR)
    geometries = build_route_geometries(payload) if payload else {}
    print()

    rows = describe(routes, stops, route_stops, geometries, corridors)
    print_table(rows)
    return 0 if all(r["passes"] for r in rows) else 1


if __name__ == "__main__":
    sys.exit(main())
