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
"""

import argparse
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
        df = corrupt(pd.read_parquet(path))
        df.to_parquet(path, index=False)
        print(
            f"{name}: edited {path.relative_to(ROOT)}"
            f"{' (backed up)' if fresh else ' (backup already present, kept)'}"
            f"\n    -> {suite} should now fail on {expectation}"
        )

    print(
        "\nRun:      great_expectations checkpoint run raw_samples_checkpoint"
        "\nRestore:  python scripts/restore_samples.py"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
