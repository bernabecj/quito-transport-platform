"""
Verify a gold layer: the star schema's integrity contract, enforced.

    python -m processing.gold.verify --gold-root <gold>

Redshift records primary and foreign keys but does not enforce them, so the
only guarantee that the gold tables join correctly is a check run here,
after every build. The checks are the list in docs/ANALYSIS_PLAN.md,
"Relationships the tests must hold":

  exists        every table is present and non-empty
  unique        every primary key is unique and non-null
  orphans       every foreign key value exists in the table it references
  grain         fct_corridor_travel_time has one row per (route_id, utc_ts)
                and every row has weather

Exit code 0 when everything passes, 1 otherwise, so the Airflow task or the
CI step that runs it fails loudly. The report is printed either way.
"""

import argparse
import sys
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from processing.gold.config import (
    DEFAULT_GOLD_ROOT,
    TABLE_AGG_CORRIDOR_HOURLY_PROFILE,
    TABLE_AGG_CORRIDOR_RELIABILITY,
    TABLE_DIM_DATE,
    TABLE_DIM_ROUTE,
    TABLE_DIM_STOP,
    TABLE_DIM_WEATHER_CONDITION,
    TABLE_FCT_CORRIDOR_TRAVEL_TIME,
    TABLE_FCT_NETWORK_SNAPSHOT,
    TABLE_FCT_ROUTE_STOP,
    TABLE_FCT_WEATHER,
)
from processing.gold.io import read_table
from processing.gold.session import get_spark

# Primary key of every table.
PRIMARY_KEYS = {
    TABLE_DIM_DATE: ("date_key",),
    TABLE_DIM_ROUTE: ("route_id",),
    TABLE_DIM_STOP: ("stop_id",),
    TABLE_DIM_WEATHER_CONDITION: ("condition_key",),
    TABLE_FCT_WEATHER: ("weather_ts",),
    TABLE_FCT_CORRIDOR_TRAVEL_TIME: ("route_id", "utc_ts"),
    TABLE_FCT_ROUTE_STOP: ("route_id", "stop_sequence"),
    TABLE_FCT_NETWORK_SNAPSHOT: ("snapshot_date",),
    TABLE_AGG_CORRIDOR_HOURLY_PROFILE: ("route_id", "local_hour", "day_type"),
    TABLE_AGG_CORRIDOR_RELIABILITY: ("route_id",),
}

# (table, column) -> (referenced table, referenced column). Nullable foreign
# keys (weather on a traffic hour with no weather slot) are checked on their
# non-null values only.
FOREIGN_KEYS = {
    (TABLE_FCT_CORRIDOR_TRAVEL_TIME, "route_id"): (TABLE_DIM_ROUTE, "route_id"),
    (TABLE_FCT_CORRIDOR_TRAVEL_TIME, "date_key"): (TABLE_DIM_DATE, "date_key"),
    (TABLE_FCT_CORRIDOR_TRAVEL_TIME, "weather_ts"): (TABLE_FCT_WEATHER, "weather_ts"),
    (TABLE_FCT_CORRIDOR_TRAVEL_TIME, "condition_key"): (
        TABLE_DIM_WEATHER_CONDITION,
        "condition_key",
    ),
    (TABLE_FCT_WEATHER, "date_key"): (TABLE_DIM_DATE, "date_key"),
    (TABLE_FCT_WEATHER, "condition_key"): (TABLE_DIM_WEATHER_CONDITION, "condition_key"),
    (TABLE_FCT_ROUTE_STOP, "route_id"): (TABLE_DIM_ROUTE, "route_id"),
    (TABLE_FCT_ROUTE_STOP, "stop_id"): (TABLE_DIM_STOP, "stop_id"),
    (TABLE_FCT_NETWORK_SNAPSHOT, "date_key"): (TABLE_DIM_DATE, "date_key"),
    (TABLE_AGG_CORRIDOR_HOURLY_PROFILE, "route_id"): (TABLE_DIM_ROUTE, "route_id"),
    (TABLE_AGG_CORRIDOR_RELIABILITY, "route_id"): (TABLE_DIM_ROUTE, "route_id"),
}


@dataclass
class Check:
    kind: str
    table: str
    passed: bool
    detail: str


def _load(spark: SparkSession, gold_root: str) -> dict[str, DataFrame | None]:
    tables = {}
    for name in PRIMARY_KEYS:
        try:
            tables[name] = read_table(spark, gold_root, name).cache()
        except Exception:  # noqa: BLE001 — a missing table is a finding, not a crash
            tables[name] = None
    return tables


def run_checks(spark: SparkSession, gold_root: str) -> list[Check]:
    """Every check, whether or not earlier ones passed, so one run shows
    everything that is wrong."""
    gold_root = str(gold_root)
    tables = _load(spark, gold_root)
    checks: list[Check] = []

    for name, df in tables.items():
        rows = df.count() if df is not None else 0
        checks.append(Check("exists", name, rows > 0, f"{rows} rows"))

    for name, key in PRIMARY_KEYS.items():
        df = tables[name]
        if df is None:
            continue
        nulls = df.filter(" or ".join(f"{k} is null" for k in key)).count()
        dupes = df.groupBy(*key).count().filter("count > 1").count()
        checks.append(
            Check(
                "unique",
                name,
                nulls == 0 and dupes == 0,
                f"pk=({', '.join(key)}) null_rows={nulls} duplicate_keys={dupes}",
            )
        )

    for (table, column), (ref_table, ref_column) in FOREIGN_KEYS.items():
        df, ref = tables[table], tables[ref_table]
        if df is None or ref is None:
            continue
        orphans = (
            df.select(F.col(column).alias("_fk"))
            .filter("_fk is not null")
            .distinct()
            .join(ref.select(F.col(ref_column).alias("_fk")).distinct(), "_fk", "left_anti")
            .count()
        )
        checks.append(
            Check(
                "orphans",
                table,
                orphans == 0,
                f"{column} -> {ref_table}.{ref_column}: {orphans} orphan values",
            )
        )

    fct = tables[TABLE_FCT_CORRIDOR_TRAVEL_TIME]
    if fct is not None:
        no_weather = fct.filter("weather_ts is null").count()
        complete = fct.filter("is_complete_day").count()
        checks.append(
            Check(
                "grain",
                TABLE_FCT_CORRIDOR_TRAVEL_TIME,
                no_weather == 0,
                f"rows_without_weather={no_weather} complete_day_rows={complete}",
            )
        )

    for df in tables.values():
        if df is not None:
            df.unpersist()
    return checks


def report(checks: list[Check]) -> str:
    lines = []
    for c in checks:
        lines.append(f"{'PASS' if c.passed else 'FAIL'}  {c.kind:8s} {c.table:28s} {c.detail}")
    failed = sum(not c.passed for c in checks)
    lines.append(f"\n{len(checks) - failed} passed, {failed} failed")
    return "\n".join(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Verify the gold layer's integrity.")
    parser.add_argument("--gold-root", default=str(DEFAULT_GOLD_ROOT))
    parser.add_argument("--master", default=None, help="Spark master, e.g. local[2]")
    args = parser.parse_args(argv)

    checks = run_checks(get_spark(master=args.master), args.gold_root)
    print(report(checks))
    return 0 if all(c.passed for c in checks) else 1


if __name__ == "__main__":
    sys.exit(main())
