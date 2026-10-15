"""
Reading the raw zone and writing the gold tables.

The readers are the only code that knows how the raw files are laid out and
how their schemas have drifted over time. Each one returns a DataFrame with a
fixed, documented set of columns, so the transformations downstream
(dimensions.py, facts.py, marts.py) are written against one schema and never
see a partition path, a missing column or a file-name convention.

What the readers normalise
  Partition columns    In the bucket layout, year/month/day(/hour) come from
                       the directory names; in the samples layout they are
                       either baked into the file or absent. The readers
                       guarantee them as integers, deriving them from the
                       row's own UTC timestamp when they are missing.
  Schema evolution     Columns added after collection started (the corridor
                       recovery of 2026-10 — see config.py) are added with
                       their historical defaults when absent, and null-filled
                       rows from older files get the same defaults. One shape
                       out, whatever shapes came in.
  Network snapshots    The three network tables carry a `snapshot_date`
                       column in place of year/month/day, because "which
                       day's snapshot" is the only thing the partition means.
  Derived columns      `local_weekday` on traffic, which the sample series
                       lacks and the loader writes.

Paths are handled as strings joined with "/", not pathlib, because the raw
and gold roots may be S3 URIs when this runs on Glue.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from processing.gold.config import (
    DATASET_NETWORK,
    DATASET_TRAFFIC,
    DATASET_WEATHER,
    LAYOUT_RAW,
    LAYOUT_SAMPLES,
    LAYOUTS,
    NETWORK_TABLES,
    ROUTE_STOPS_OPTIONAL_COLUMNS,
    SAMPLE_FILES,
    TRAFFIC_OPTIONAL_COLUMNS,
    UTC_OFFSET_HOURS,
)

PARTITION_COLUMNS_DAY = ("year", "month", "day")
PARTITION_COLUMNS_HOUR = PARTITION_COLUMNS_DAY + ("hour",)


# ── Helpers ──────────────────────────────────────────────────────────────────


def join_path(root: str, *parts: str) -> str:
    """``root/part/part`` for local paths and S3 URIs alike."""
    return "/".join([str(root).rstrip("/"), *parts])


def ensure_columns(df: DataFrame, optional: dict[str, tuple]) -> DataFrame:
    """Guarantee each column in ``optional`` exists and carries its default.

    ``optional`` maps a column name to ``(default, spark_type)``. A missing
    column is added as a literal of that type. A column that exists but is
    null on some rows — what ``mergeSchema`` produces when old and new files
    are read together — is coalesced to the default, unless the default is
    itself None, in which case null is the honest value.
    """
    for name, (default, spark_type) in optional.items():
        if name not in df.columns:
            df = df.withColumn(name, F.lit(default).cast(spark_type))
        elif default is not None:
            df = df.withColumn(name, F.coalesce(F.col(name).cast(spark_type), F.lit(default)))
    return df


def _check_layout(layout: str) -> None:
    if layout not in LAYOUTS:
        raise ValueError(f"layout must be one of {LAYOUTS}, got {layout!r}")


def _ensure_partition_columns(df: DataFrame, timestamp_col: str, columns: tuple) -> DataFrame:
    """Partition columns as integers, derived from ``timestamp_col`` (a UTC
    ISO-8601 string) whenever the file does not already carry them."""
    ts = F.to_timestamp(F.col(timestamp_col))
    derive = {"year": F.year(ts), "month": F.month(ts), "day": F.dayofmonth(ts), "hour": F.hour(ts)}
    for name in columns:
        source = F.col(name) if name in df.columns else derive[name]
        df = df.withColumn(name, source.cast("int"))
    return df


def _python_weekday(ts_col):
    """Monday=0 ... Sunday=6, matching datetime.weekday(), which is what
    traffic_loader writes. Spark's dayofweek is Sunday=1 ... Saturday=7."""
    return ((F.dayofweek(ts_col) + 5) % 7).cast("long")


# ── Readers ──────────────────────────────────────────────────────────────────


def read_traffic(spark: SparkSession, raw_root: str, layout: str = LAYOUT_RAW) -> DataFrame:
    """Every corridor travel-time sample, one row per (route_id, sampled_at).

    Guaranteed columns, beyond what traffic_loader writes:
      year, month, day, hour   int, UTC
      local_weekday            long, Monday=0
      sequence_source          string, "osm_member" when the file predates it
      stops_excluded           long, 0 when the file predates it
    """
    _check_layout(layout)
    if layout == LAYOUT_SAMPLES:
        path = join_path(raw_root, SAMPLE_FILES[DATASET_TRAFFIC])
    else:
        path = join_path(raw_root, DATASET_TRAFFIC)

    df = spark.read.option("mergeSchema", "true").parquet(path)
    df = _ensure_partition_columns(df, "sampled_at", PARTITION_COLUMNS_HOUR)
    df = ensure_columns(df, TRAFFIC_OPTIONAL_COLUMNS)
    if "local_weekday" not in df.columns:
        local_ts = F.to_timestamp("sampled_at") + F.expr(f"INTERVAL {UTC_OFFSET_HOURS} HOURS")
        df = df.withColumn("local_weekday", _python_weekday(local_ts))
    return df


def read_weather(spark: SparkSession, raw_root: str, layout: str = LAYOUT_RAW) -> DataFrame:
    """Every weather row, observations and forecasts alike (`record_type`
    tells them apart). Guarantees year/month/day as the UTC date of the API
    call (`fetched_at`), which is what the raw partition encodes."""
    _check_layout(layout)
    if layout == LAYOUT_SAMPLES:
        path = join_path(raw_root, SAMPLE_FILES[DATASET_WEATHER])
    else:
        path = join_path(raw_root, DATASET_WEATHER)

    df = spark.read.option("mergeSchema", "true").parquet(path)
    return _ensure_partition_columns(df, "fetched_at", PARTITION_COLUMNS_DAY)


def read_network_table(
    spark: SparkSession, raw_root: str, name: str, layout: str = LAYOUT_RAW
) -> DataFrame:
    """One of routes / stops / route_stops, every snapshot stacked.

    In the bucket layout the three tables share a partition directory, so
    reading the directory would merge three unrelated schemas (the bug that
    notebooks/01_exploring.ipynb section 2 documents). The table is selected
    by file name with ``pathGlobFilter``: Spark still reads the *directory*,
    so partition discovery gives year/month/day for free, but only files
    matching the pattern are opened. (A path glob such as
    ``year=*/month=*/day=*/routes.parquet`` works too, but with
    ``mergeSchema`` it is five times slower — every matched file becomes its
    own root path.)

    Guaranteed columns: the table's own, plus
      snapshot_date    date, the UTC day of the snapshot
    and for route_stops the corridor-recovery columns with their defaults
    (see config.ROUTE_STOPS_OPTIONAL_COLUMNS). year/month/day are dropped in
    favour of snapshot_date.
    """
    _check_layout(layout)
    if name not in NETWORK_TABLES:
        raise ValueError(f"name must be one of {NETWORK_TABLES}, got {name!r}")

    if layout == LAYOUT_SAMPLES:
        df = spark.read.option("mergeSchema", "true").parquet(
            join_path(raw_root, SAMPLE_FILES[name])
        )
    else:
        df = (
            spark.read.option("mergeSchema", "true")
            .option("pathGlobFilter", f"{name}.parquet")
            .parquet(join_path(raw_root, DATASET_NETWORK))
        )

    df = _ensure_partition_columns(df, "fetched_at", PARTITION_COLUMNS_DAY)
    df = df.withColumn("snapshot_date", F.make_date("year", "month", "day")).drop(
        *PARTITION_COLUMNS_DAY
    )
    if name == "route_stops":
        df = ensure_columns(df, ROUTE_STOPS_OPTIONAL_COLUMNS)
    return df


# ── Writers ──────────────────────────────────────────────────────────────────


def write_table(
    df: DataFrame, gold_root: str, name: str, partition_by: tuple[str, ...] = ()
) -> str:
    """Write one gold table as Parquet under ``<gold_root>/<name>/``.

    Full refresh: whatever was there is replaced by exactly ``df``. The
    session's ``partitionOverwriteMode=dynamic`` (see session.py) would keep
    partitions that are absent from the new data — right for an incremental
    load, wrong for a rebuild, where a partition that no longer exists in the
    raw zone must disappear from gold too. The write therefore overrides the
    mode to ``static`` for itself; switching a table to incremental later is
    a matter of dropping that one option for that one table.

    Unpartitioned tables are written as a single file — they are dimensions
    and aggregates of a few hundred rows, and one file per table is what a
    Redshift COPY or a pandas reader wants. Partitioned tables get one file
    per partition value for the same reason.
    """
    path = join_path(gold_root, name)
    if partition_by:
        writer = df.repartition(*partition_by).write.partitionBy(*partition_by)
    else:
        writer = df.coalesce(1).write
    writer.mode("overwrite").option("partitionOverwriteMode", "static").parquet(path)
    return path


def read_table(spark: SparkSession, gold_root: str, name: str) -> DataFrame:
    return spark.read.parquet(join_path(gold_root, name))
