"""
Dimension tables: dim_date, dim_route, dim_stop.

Each function is a pure transformation — DataFrames in, one DataFrame out,
no reading or writing — so build.py can wire them together and the tests can
feed them hand-built frames.

Why the network dimensions are built from every snapshot, not the latest
  The raw network is re-fetched daily and the snapshots flap: two routes
  drop out of the Overpass response on scattered days and come back
  (notebooks/01_exploring.ipynb, section 10). A dimension built from "today's
  snapshot" would lose those routes on a bad day, and every fact row pointing
  at them would lose its context. So each entity is collapsed across all
  snapshots: the most recently seen version of its attributes wins (SCD
  type 1 — one row per entity, overwritten in place), and three columns keep
  the history honest:

    first_seen_date     the first snapshot the entity appeared in
    last_seen_date      the most recent one
    snapshots_present   how many snapshots carried it

  "Withdrawn from the network" is a question about last_seen_date, not about
  absence from today's file. A full version history (SCD type 2) is out of
  scope for this pass; fct_network_snapshot keeps enough to add it later.

Why dim_route knows about traffic
  `is_sampled` says whether the corridor has travel-time measurements. It is
  what separates the 14 sampled trunk corridors from the four Ecovía
  relations the sampler rejected (and the two Ruta de Integración relations
  with no stops). It is computed from the traffic data, not hard-coded, so
  the day the recovered corridors start landing they flip to sampled with no
  code change.
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from processing.gold.config import TRUNK_ROUTE_PATTERN
from processing.gold.time import date_key

ROUTE_ATTRIBUTES = (
    "route_ref",
    "route_name",
    "route_type",
    "operator",
    "network",
    "origin",
    "destination",
    "colour",
)
STOP_ATTRIBUTES = ("stop_name", "latitude", "longitude", "stop_type", "shelter")

WEEKDAY_NAMES = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")


# ── Shared ───────────────────────────────────────────────────────────────────


def latest_version(df: DataFrame, key: str, attributes: tuple[str, ...]) -> DataFrame:
    """One row per ``key``: the attributes from its most recent snapshot,
    plus first_seen_date / last_seen_date / snapshots_present.

    Ties within a snapshot (the same id twice in one file — not observed,
    but OSM could do it) are broken on the attributes themselves so the
    result is deterministic across runs.
    """
    newest_first = Window.partitionBy(key).orderBy(
        F.col("snapshot_date").desc(), *[F.col(a).desc_nulls_last() for a in attributes]
    )
    newest = (
        df.withColumn("_rank", F.row_number().over(newest_first))
        .filter(F.col("_rank") == 1)
        .select(key, *attributes)
    )
    presence = df.groupBy(key).agg(
        F.min("snapshot_date").alias("first_seen_date"),
        F.max("snapshot_date").alias("last_seen_date"),
        F.countDistinct("snapshot_date").alias("snapshots_present"),
    )
    return newest.join(presence, key)


def latest_snapshot(df: DataFrame) -> DataFrame:
    """Rows from the most recent snapshot only — for relationships such as
    route membership, where mixing snapshots would double-count."""
    newest = df.agg(F.max("snapshot_date")).collect()[0][0]
    return df.filter(F.col("snapshot_date") == F.lit(newest))


# ── dim_date ─────────────────────────────────────────────────────────────────


def build_dim_date(spark: SparkSession, start, end) -> DataFrame:
    """One row per calendar day from ``start`` to ``end`` inclusive (Python
    dates or ISO strings). ``date_key`` is the primary key.

    The only dimension that needs the session: it is generated, not derived
    from a raw table. Small on purpose — hour-of-day lives on the fact rows
    (local_hour), not here: a date × hour dimension would be 24× larger for
    no query it makes easier, and the marts group by local_hour directly.

    Columns
      date_key (PK)   yyyymmdd
      date            the day
      year, month, day, day_of_year, iso_week
      weekday         Monday=0 ... Sunday=6 (datetime.weekday(), as the loaders use)
      weekday_name
      is_weekend      Saturday or Sunday
      day_type        "weekday" / "weekend" — what the marts split on
    """
    days = F.explode(F.sequence(F.to_date(F.lit(str(start))), F.to_date(F.lit(str(end)))))
    weekday = (F.dayofweek("date") + 5) % 7
    return (
        spark.range(1)
        .select(days.alias("date"))
        .withColumn("date_key", date_key(F.col("date")))
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("day_of_year", F.dayofyear("date"))
        .withColumn("iso_week", F.weekofyear("date"))
        .withColumn("weekday", weekday.cast("int"))
        .withColumn("weekday_name", F.element_at(F.array(*map(F.lit, WEEKDAY_NAMES)), weekday + 1))
        .withColumn("is_weekend", weekday >= 5)
        .withColumn("day_type", F.when(weekday >= 5, "weekend").otherwise("weekday"))
        .select(
            "date_key",
            "date",
            "year",
            "month",
            "day",
            "day_of_year",
            "iso_week",
            "weekday",
            "weekday_name",
            "is_weekend",
            "day_type",
        )
    )


# ── dim_route ────────────────────────────────────────────────────────────────


def build_dim_route(routes: DataFrame, route_stops: DataFrame, traffic: DataFrame) -> DataFrame:
    """One row per route_id.

    Columns
      route_id (PK), route_ref, route_name, route_type, operator, network,
      origin, destination, colour          latest snapshot's values
      is_trunk          route_name matches TRUNK_ROUTE_PATTERN — the BRT
                        systems the traffic sampler targets
      is_sampled        at least one travel-time sample exists
      stop_count        stops in the latest snapshot's membership (0 when none)
      sequence_source   where that membership's order came from ("osm_member"
                        or "geometry"); null for routes with no stops
      first_seen_date, last_seen_date, snapshots_present
    """
    dim = latest_version(routes, "route_id", ROUTE_ATTRIBUTES)
    # Case-insensitive, as traffic_loader.build_corridors matches it.
    trunk_pattern = f"(?i){TRUNK_ROUTE_PATTERN}"

    membership = latest_snapshot(route_stops).groupBy("route_id").agg(
        F.count("*").alias("stop_count"),
        # All rows of a route share one source (traffic_loader reads the
        # first); max() is the deterministic way to pick it.
        F.max("sequence_source").alias("sequence_source"),
    )
    sampled = traffic.select("route_id").distinct().withColumn("is_sampled", F.lit(True))

    return (
        dim.join(membership, "route_id", "left")
        .join(sampled, "route_id", "left")
        .withColumn("is_trunk", F.coalesce(F.col("route_name"), F.lit("")).rlike(trunk_pattern))
        .withColumn("is_sampled", F.coalesce(F.col("is_sampled"), F.lit(False)))
        .withColumn("stop_count", F.coalesce(F.col("stop_count"), F.lit(0)).cast("int"))
        .select(
            "route_id",
            *ROUTE_ATTRIBUTES,
            "is_trunk",
            "is_sampled",
            "stop_count",
            "sequence_source",
            "first_seen_date",
            "last_seen_date",
            "snapshots_present",
        )
    )


# ── dim_stop ─────────────────────────────────────────────────────────────────


def build_dim_stop(stops: DataFrame, route_stops: DataFrame) -> DataFrame:
    """One row per stop_id.

    Columns
      stop_id (PK), stop_name, latitude, longitude, stop_type, shelter
                        latest snapshot's values
      route_count       routes serving the stop in the latest membership
                        snapshot — the stop-density / overlap signal
      first_seen_date, last_seen_date, snapshots_present
    """
    dim = latest_version(stops, "stop_id", STOP_ATTRIBUTES)
    served = (
        latest_snapshot(route_stops)
        .groupBy("stop_id")
        .agg(F.countDistinct("route_id").alias("route_count"))
    )
    return (
        dim.join(served, "stop_id", "left")
        .withColumn("route_count", F.coalesce(F.col("route_count"), F.lit(0)).cast("int"))
        .select(
            "stop_id",
            *STOP_ATTRIBUTES,
            "route_count",
            "first_seen_date",
            "last_seen_date",
            "snapshots_present",
        )
    )
