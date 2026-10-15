"""
The gold build: raw zone in, ten tables out.

    python -m processing.gold.build --raw-root <raw> --gold-root <gold> [--layout raw|samples]

One function, ``build_all``, wires the readers (io.py), the transformations
(dimensions.py, weather.py, facts.py, marts.py) and the writer together in
dependency order. The CLI around it only parses arguments. Anything that can
hold a SparkSession — a laptop, this repo's tests, a Databricks notebook, an
AWS Glue job — calls the same function with the same arguments.

Full rebuild, every run
  Every table is recomputed from the whole raw zone and overwritten. Running
  twice yields identical output; a failed run is re-run, nothing to repair.
  At today's volume (a few MB) this takes seconds, which is why there is no
  incremental mode — see docs/ANALYSIS_PLAN.md, implementation objective 2,
  for the one switch that would add it.

Defaults
  --raw-root    data/local/raw    what the ingestion loaders write with STORAGE_BACKEND=local
  --gold-root   data/local/gold   beside it, mirroring the bucket's raw/ and gold/ prefixes
  --layout      raw               the bucket layout; "samples" reads the flat files in data/samples/
"""

import argparse
import logging
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from processing.gold.config import (
    DEFAULT_GOLD_ROOT,
    DEFAULT_RAW_ROOT,
    DEFAULT_SAMPLES_ROOT,
    LAYOUT_RAW,
    LAYOUT_SAMPLES,
    LAYOUTS,
    NETWORK_TABLES,
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
from processing.gold.dimensions import build_dim_date, build_dim_route, build_dim_stop
from processing.gold.facts import (
    build_fct_corridor_travel_time,
    build_fct_network_snapshot,
    build_fct_route_stop,
)
from processing.gold.io import read_network_table, read_traffic, read_weather, write_table
from processing.gold.marts import (
    build_agg_corridor_hourly_profile,
    build_agg_corridor_reliability,
)
from processing.gold.session import get_spark
from processing.gold.weather import build_dim_weather_condition, build_fct_weather

logger = logging.getLogger(__name__)

FACT_PARTITION = ("local_date",)


def _span(*frames_and_columns) -> tuple:
    """The earliest and latest date across several (frame, column) pairs —
    the range dim_date must cover so every date_key in every fact resolves."""
    lows, highs = [], []
    for frame, column in frames_and_columns:
        low, high = frame.agg(F.min(column), F.max(column)).first()
        if low is not None:
            lows.append(low)
            highs.append(high)
    return min(lows), max(highs)


def build_all(
    spark: SparkSession, raw_root: str, gold_root: str, layout: str = LAYOUT_RAW
) -> dict[str, int]:
    """Build and write every gold table. Returns ``{table: row_count}`` in
    the order written."""
    if layout not in LAYOUTS:
        raise ValueError(f"layout must be one of {LAYOUTS}, got {layout!r}")
    raw_root, gold_root = str(raw_root), str(gold_root)
    logger.info("gold build: raw=%s layout=%s -> gold=%s", raw_root, layout, gold_root)

    # ── read ─────────────────────────────────────────────────────────────
    # The raw zone is hundreds of small files (one per hour, one per
    # snapshot) whose schemas must be merged on every read. Each input is
    # read once and cached: it is a few MB, and without the cache every
    # table below would re-list and re-merge all of them.
    traffic = read_traffic(spark, raw_root, layout).cache()
    weather = read_weather(spark, raw_root, layout).cache()
    routes, stops, route_stops = (
        read_network_table(spark, raw_root, name, layout).cache() for name in NETWORK_TABLES
    )

    # ── transform, in dependency order ───────────────────────────────────
    # The two frames several tables depend on are cached too: the hub feeds
    # both marts and dim_date's span, and would otherwise be computed four
    # times.
    fct_weather = build_fct_weather(weather).cache()
    fct_travel = build_fct_corridor_travel_time(traffic, fct_weather).cache()
    fct_snapshot = build_fct_network_snapshot(routes, stops, route_stops)

    start, end = _span(
        (fct_travel, "local_date"), (fct_weather, "local_date"), (fct_snapshot, "snapshot_date")
    )
    dim_date = build_dim_date(spark, start, end)
    hourly_profile = build_agg_corridor_hourly_profile(fct_travel, dim_date)
    reliability = build_agg_corridor_reliability(fct_travel, dim_date)

    tables = [
        (TABLE_DIM_DATE, dim_date, ()),
        (TABLE_DIM_ROUTE, build_dim_route(routes, route_stops, traffic), ()),
        (TABLE_DIM_STOP, build_dim_stop(stops, route_stops), ()),
        (TABLE_DIM_WEATHER_CONDITION, build_dim_weather_condition(weather), ()),
        (TABLE_FCT_WEATHER, fct_weather, ()),
        (TABLE_FCT_CORRIDOR_TRAVEL_TIME, fct_travel, FACT_PARTITION),
        (TABLE_FCT_ROUTE_STOP, build_fct_route_stop(route_stops), ()),
        (TABLE_FCT_NETWORK_SNAPSHOT, fct_snapshot, ()),
        (TABLE_AGG_CORRIDOR_HOURLY_PROFILE, hourly_profile, ()),
        (TABLE_AGG_CORRIDOR_RELIABILITY, reliability, ()),
    ]

    # ── write ────────────────────────────────────────────────────────────
    counts = {}
    for name, frame, partition_by in tables:
        started = time.time()
        path = write_table(frame, gold_root, name, partition_by)
        counts[name] = spark.read.parquet(path).count()  # what landed, not what was planned
        logger.info("%-28s %6d rows  %5.1fs  %s", name, counts[name], time.time() - started, path)

    for frame in (traffic, weather, routes, stops, route_stops, fct_weather, fct_travel):
        frame.unpersist()
    return counts


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the gold layer from the raw zone.")
    parser.add_argument(
        "--raw-root",
        default=None,
        help=f"raw zone root (default: {DEFAULT_RAW_ROOT}, or data/samples with --layout samples)",
    )
    parser.add_argument("--gold-root", default=str(DEFAULT_GOLD_ROOT), help="where to write gold/")
    parser.add_argument("--layout", choices=LAYOUTS, default=LAYOUT_RAW)
    parser.add_argument("--master", default=None, help="Spark master, e.g. local[2]")
    args = parser.parse_args(argv)
    if args.raw_root is None:
        default = DEFAULT_SAMPLES_ROOT if args.layout == LAYOUT_SAMPLES else DEFAULT_RAW_ROOT
        args.raw_root = str(default)
    return args


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    spark = get_spark(master=args.master)
    counts = build_all(spark, args.raw_root, args.gold_root, args.layout)
    print(f"\ngold layer written to {args.gold_root}")
    for name, rows in counts.items():
        print(f"  {name:28s} {rows:7d} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
