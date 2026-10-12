#!/usr/bin/env python3
"""
Corrupt the committed samples so the data quality gate fails.

A gate that only ever passes proves nothing. This script applies one specific,
minimal corruption per suite to the files under data/samples/ — the same edits
quality/README.md describes — so you can watch

    great_expectations checkpoint run raw_samples_checkpoint

fail on exactly the expectation built for it. Undo with
scripts/restore_samples.py.

Before touching a file it copies the pristine version to
data/samples/.backup/ (git-ignored). An existing backup is never overwritten,
so running this twice cannot replace the good copy with a broken one. The
samples are real captures and the traffic series cannot be re-fetched, which is
why the backup comes first and why nothing here should ever be committed.

Usage:
    python scripts/break_samples.py              # network, weather, traffic
    python scripts/break_samples.py traffic      # one drill
    python scripts/break_samples.py --all        # every drill below
    python scripts/break_samples.py --list
    python scripts/restore_samples.py            # put everything back

The three `corridor-*` drills edit the trunk geometry fixture instead of a
parquet sample. They are caught by pytest, not Great Expectations:

    python scripts/break_samples.py corridor-geometry-missing
    pytest tests/test_trunk_fixture.py           # fails on the named test
    python scripts/check_corridors.py --overpass data/samples/network/overpass_trunk_geom.json
"""

import argparse
import json
import shutil
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "data" / "samples"
BACKUP = SAMPLES / ".backup"

STOPS = SAMPLES / "network" / "stops.parquet"
ROUTES = SAMPLES / "network" / "routes.parquet"
WEATHER = SAMPLES / "weather" / "weather.parquet"
TRAFFIC = SAMPLES / "traffic" / "corridor_travel_time_series.parquet"
FIXTURE = SAMPLES / "network" / "overpass_trunk_geom.json"


# ── the drills ───────────────────────────────────────────────────────────────
# Each takes the sample as a DataFrame and returns the corrupted one.


def stop_outside_bbox(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[0, "latitude"] = 0.75  # north of QUITO_BBOX's 0.05 edge
    return df


def null_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[5, "timestamp"] = None
    return df


def zero_duration(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[100, "duration_seconds"] = 0
    return df


def truncated_routes(df: pd.DataFrame) -> pd.DataFrame:
    return df.iloc[:350]  # below the 400-route floor


def truncated_stop_nodes(df: pd.DataFrame) -> pd.DataFrame:
    return df.iloc[:-5]  # routes intact, five stop nodes never arrived


def missing_corridor(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["route_id"] != df["route_id"].iloc[0]]


# The fixture drills work on either shape the fixture can have — a raw
# Overpass response ({"elements": [...]}) or the overpass-turbo GeoJSON export
# ({"features": [...]}); see data/samples/README.md.

E1_SCRAMBLED = 12110323  # Ecovía E1 Las Universidades - T. Sur: order must come from geometry
E1_STOP_ON_ROAD = 1630983244  # "Casa de la Cultura", served by this E1 relation only


def _is_raw(payload: dict) -> bool:
    return "elements" in payload


def e1_without_road_geometry(payload: dict) -> dict:
    """Take the road away from the scrambled E1 relation.

    With no geometry to project onto, the loader falls back to OSM's member
    order — 2.49 km mean hop — and the corridor is dropped again: 17 of 18.
    """
    if _is_raw(payload):
        relation = next(e for e in payload["elements"] if e["type"] == "relation" and e["id"] == E1_SCRAMBLED)
        way_ids = {m["ref"] for m in relation["members"] if m["type"] == "way"}
        payload["elements"] = [
            e for e in payload["elements"] if not (e["type"] == "way" and e["id"] in way_ids)
        ]
    else:
        feature = next(f for f in payload["features"] if f["id"] == f"relation/{E1_SCRAMBLED}")
        feature["geometry"] = {"type": "MultiLineString", "coordinates": []}
    return payload


def e1_stop_pushed_off_the_road(payload: dict) -> dict:
    """Move one on-road E1 stop 1 km east of the corridor.

    The stop still orders correctly, but it now snaps > CORRIDOR_MAX_SNAP_M,
    so traffic_loader leaves it out of the waypoints and the fixture tests
    see a misplaced stop that is not one of the three known ones.
    """
    shift = 0.009  # degrees of longitude, ~1 km on the equator
    if _is_raw(payload):
        node = next(e for e in payload["elements"] if e["type"] == "node" and e["id"] == E1_STOP_ON_ROAD)
        node["lon"] += shift
    else:
        feature = next(f for f in payload["features"] if f["id"] == f"node/{E1_STOP_ON_ROAD}")
        feature["geometry"]["coordinates"][0] += shift
    return payload


def e1_relation_removed(payload: dict) -> dict:
    """Drop the scrambled E1 relation altogether: 17 trunk relations."""
    if _is_raw(payload):
        payload["elements"] = [
            e for e in payload["elements"] if not (e["type"] == "relation" and e["id"] == E1_SCRAMBLED)
        ]
    else:
        payload["features"] = [f for f in payload["features"] if f["id"] != f"relation/{E1_SCRAMBLED}"]
    return payload


DRILLS = {
    # name: (file, corruption, suite that must fail, expectation that catches it)
    "network": (
        STOPS,
        stop_outside_bbox,
        "network_stops_suite",
        "expect_column_values_to_be_between(latitude)",
    ),
    "weather": (
        WEATHER,
        null_timestamp,
        "weather_suite",
        "expect_column_values_to_not_be_null(timestamp)",
    ),
    "traffic": (
        TRAFFIC,
        zero_duration,
        "traffic_suite",
        "expect_column_values_to_be_between(duration_seconds)",
    ),
    "overpass-truncated": (
        ROUTES,
        truncated_routes,
        "network_routes_suite",
        "expect_table_row_count_to_be_between",
    ),
    "nodes-truncated": (
        STOPS,
        truncated_stop_nodes,
        "network_route_stops_suite",
        "expect_column_unique_value_count_to_be_between(stop_id)",
    ),
    "corridor-missing": (
        TRAFFIC,
        missing_corridor,
        "traffic_suite",
        "expect_column_unique_value_count_to_be_between(route_id)",
    ),
    "corridor-geometry-missing": (
        FIXTURE,
        e1_without_road_geometry,
        "tests/test_trunk_fixture.py",
        "test_every_trunk_corridor_is_sampled (E1 back to 2.49 km, 17 of 18)",
    ),
    "corridor-stop-off-road": (
        FIXTURE,
        e1_stop_pushed_off_the_road,
        "tests/test_trunk_fixture.py",
        "test_misplaced_e3_stops_are_identified_by_id (a fourth off-road stop, excluded from E1's waypoints)",
    ),
    "corridor-relation-missing": (
        FIXTURE,
        e1_relation_removed,
        "tests/test_trunk_fixture.py",
        "test_fixture_holds_the_eighteen_trunk_corridors",
    ),
}
DEFAULT_DRILLS = ["network", "weather", "traffic"]


def backup_path(sample: Path) -> Path:
    return BACKUP / sample.relative_to(SAMPLES)


def ensure_backup(sample: Path) -> bool:
    """Copy the pristine file aside. Returns True if a new backup was made."""
    target = backup_path(sample)
    if target.exists():
        return False
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(sample, target)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "drills",
        nargs="*",
        metavar="DRILL",
        help="which drills to apply (default: %s); see --list" % " ".join(DEFAULT_DRILLS),
    )
    parser.add_argument("--all", action="store_true", help="apply every drill")
    parser.add_argument("--list", action="store_true", help="describe the drills and exit")
    args = parser.parse_args()

    if args.list:
        width = max(len(name) for name in DRILLS)
        for name, (path, _, suite, expectation) in DRILLS.items():
            print(f"{name:<{width}}  {path.relative_to(ROOT)}  ->  {suite} fails on {expectation}")
        return 0

    unknown = [name for name in args.drills if name not in DRILLS]
    if unknown:
        parser.error(f"unknown drill(s): {', '.join(unknown)}. Choose from: {', '.join(DRILLS)}")
    selected = list(DRILLS) if args.all else (args.drills or DEFAULT_DRILLS)

    for name in selected:
        path, corrupt, suite, expectation = DRILLS[name]
        if not path.exists():
            print(f"{name}: {path.relative_to(ROOT)} is missing — restore it first", file=sys.stderr)
            return 1
        fresh = ensure_backup(path)
        if path.suffix == ".json":
            payload = corrupt(json.loads(path.read_text(encoding="utf-8")))
            path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        else:
            corrupt(pd.read_parquet(path)).to_parquet(path, index=False)
        print(
            f"{name}: edited {path.relative_to(ROOT)}"
            f"{' (backed up)' if fresh else ' (backup already present, kept)'}"
            f"\n    -> {suite} should now fail on {expectation}"
        )

    print(
        "\nRun:      great_expectations checkpoint run raw_samples_checkpoint   (parquet drills)"
        "\n          pytest tests/test_trunk_fixture.py                          (corridor-* drills)"
        "\nRestore:  python scripts/restore_samples.py"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
