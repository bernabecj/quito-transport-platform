"""
Fact tables.

    build_fct_corridor_travel_time   one row per corridor per sampled hour — the hub
    build_fct_route_stop             one row per route × position, latest snapshot
    build_fct_network_snapshot       one row per snapshot day: counts and deltas

Pure transformations, like dimensions.py: frames in, one frame out.

Why the hub carries weather columns
  Objective 5 (docs/ANALYSIS_PLAN.md) asks whether rain coincides with slower
  corridors. Weather is one point for the whole city at three-hour steps, so
  the match is purely on time: each traffic hour takes the nearest three-hour
  slot (at most 90 minutes away). The measures are copied onto the traffic
  row — denormalised — so that question is one table in Metabase, not a
  join on a rounded timestamp that every dashboard author would have to get
  right. fct_weather stays the table of record; weather_ts on this row is the
  key back to it, and the provenance (record_type, lead_hours) comes along so
  a forecast is never mistaken for an observation.

Why the hub carries coverage columns
  The exploration locked its analysis window to 2026-08-25 → 08-31 by hand,
  because sampling was 6-hourly before 08-24 16:00 UTC and hourly after.
  Hard-coding that in the build would freeze the marts at one week forever.
  Instead every row says how well its own day was sampled:

    hours_sampled_that_day    distinct hours this corridor was sampled on
                              this local date
    is_complete_day           all 24 of them
    routes_sampled_that_hour  corridors in this hour's sample (14 = the full set)

  The marts filter on is_complete_day; today that reproduces the notebook's
  2,352 rows, and the day sampling resumes the window grows on its own.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from processing.gold.config import TRUNK_ROUTE_PATTERN
from processing.gold.dimensions import latest_snapshot
from processing.gold.time import date_key, with_time_columns
from processing.gold.weather import nearest_grid_slot

HOURS_PER_DAY = 24

TRAFFIC_MEASURES = (
    "distance_meters",
    "duration_seconds",
    "mean_speed_kmh",
    "segments_total",
    "segments_known",
    "congestion_known_pct",
    "congestion_low",
    "congestion_moderate",
    "congestion_heavy",
    "congestion_severe",
    "congestion_unknown",
)

# Weather columns copied onto the traffic row, and the names they get there.
# Provenance columns are prefixed so nobody reads "record_type" on a traffic
# fact and wonders what was recorded.
WEATHER_COLUMNS = {
    "condition_key": "condition_key",
    "record_type": "weather_record_type",
    "lead_hours": "weather_lead_hours",
    "temp_c": "temp_c",
    "feels_like_c": "feels_like_c",
    "humidity_pct": "humidity_pct",
    "pressure_hpa": "pressure_hpa",
    "wind_speed_ms": "wind_speed_ms",
    "wind_deg": "wind_deg",
    "clouds_pct": "clouds_pct",
    "visibility_m": "visibility_m",
    "rain_mm": "rain_mm",
    "rain_period_hours": "rain_period_hours",
}


def build_fct_corridor_travel_time(traffic: DataFrame, fct_weather: DataFrame) -> DataFrame:
    """One row per (route_id, utc_ts).

    Columns
      route_id, utc_ts (PK)      FK to dim_route; the sampled hour, UTC
      sampled_at                 the exact instant of the Mapbox call
      local_ts, local_date, date_key (FK to dim_date), local_hour, local_weekday
      waypoint_count, mean_hop_km, sequence_source, stops_excluded
                                 how the corridor was routed (provenance)
      distance_meters, duration_seconds, mean_speed_kmh
      segments_*, congestion_*   see docs/ANALYSIS_PLAN.md on their coverage
      hours_sampled_that_day, is_complete_day, routes_sampled_that_hour
      weather_ts (FK to fct_weather), weather_offset_minutes,
      condition_key (FK to dim_weather_condition), weather_record_type,
      weather_lead_hours, temp_c ... rain_period_hours

    The raw partition key already makes (route_id, hour) unique — a rerun in
    the same hour overwrites the file — but the grain is enforced here
    anyway, keeping the latest sampled_at, so the fact cannot carry a
    duplicate whatever the raw zone does.
    """
    t = with_time_columns(traffic).withColumn("sampled_at", F.to_timestamp("sampled_at"))

    latest_first = Window.partitionBy("route_id", "utc_ts").orderBy(F.col("sampled_at").desc())
    t = t.withColumn("_rank", F.row_number().over(latest_first)).filter("_rank = 1").drop("_rank")

    per_day = Window.partitionBy("route_id", "local_date")
    per_hour = Window.partitionBy("utc_ts")
    t = (
        t.withColumn("hours_sampled_that_day", F.size(F.collect_set("utc_ts").over(per_day)))
        .withColumn("is_complete_day", F.col("hours_sampled_that_day") == HOURS_PER_DAY)
        .withColumn("routes_sampled_that_hour", F.size(F.collect_set("route_id").over(per_hour)))
    )

    t = t.withColumn("weather_ts", nearest_grid_slot(F.col("utc_ts"))).withColumn(
        "weather_offset_minutes",
        ((F.unix_timestamp("utc_ts") - F.unix_timestamp("weather_ts")) / 60).cast("int"),
    )
    weather = fct_weather.select(
        "weather_ts", *[F.col(src).alias(dst) for src, dst in WEATHER_COLUMNS.items()]
    )
    # Left join: a traffic hour with no weather slot (a gap in the weather
    # schedule) keeps its measurements and gets nulls, never disappears.
    t = t.join(weather, "weather_ts", "left")

    return t.select(
        "route_id",
        "utc_ts",
        "sampled_at",
        "local_ts",
        "local_date",
        "date_key",
        "local_hour",
        "local_weekday",
        "waypoint_count",
        "mean_hop_km",
        "sequence_source",
        "stops_excluded",
        *TRAFFIC_MEASURES,
        "hours_sampled_that_day",
        "is_complete_day",
        "routes_sampled_that_hour",
        "weather_ts",
        "weather_offset_minutes",
        *WEATHER_COLUMNS.values(),
    )


# ── fct_route_stop ───────────────────────────────────────────────────────────


def build_fct_route_stop(route_stops: DataFrame) -> DataFrame:
    """One row per (route_id, stop_sequence), from the latest snapshot.

    A *bridge* between dim_route and dim_stop: which stops a route serves,
    in what order. Coverage ("which stops does E1 reach"), density ("how
    many routes call here") and overlap ("which corridors share stops") are
    all joins through this table.

    The grain is the position on the route, not the stop: OSM relations can
    list the same node twice (one does, at consecutive positions), and
    dropping the repeat would silently edit the relation. (route_id, stop_id)
    is therefore *almost* unique, and a consumer counting distinct stops per
    route should count stop_id, not rows.

    Columns
      route_id (FK dim_route), stop_sequence      PK
      stop_id (FK dim_stop)
      member_role           OSM's role: stop, platform, stop_entry_only, ...
      sequence_source       "osm_member" or "geometry" — how stop_sequence was decided
      member_index          OSM's own position, when the order was recomputed
      distance_along_m      distance along the road, geometry-ordered routes only
      snap_distance_m       stop-to-road distance; large values flag misplaced members
      snapshot_date         the snapshot this membership comes from
    """
    return latest_snapshot(route_stops).select(
        "route_id",
        F.col("stop_sequence").cast("int").alias("stop_sequence"),
        "stop_id",
        "member_role",
        "sequence_source",
        F.col("member_index").cast("int").alias("member_index"),
        "distance_along_m",
        "snap_distance_m",
        "snapshot_date",
    )


# ── fct_network_snapshot ─────────────────────────────────────────────────────


def build_fct_network_snapshot(
    routes: DataFrame, stops: DataFrame, route_stops: DataFrame
) -> DataFrame:
    """One row per snapshot day: what the network looked like, and what
    changed since the previous snapshot.

    The history table. dim_route and dim_stop collapse the snapshots to one
    row per entity; this keeps the per-day counts so "did the network
    change?" can be answered without the raw zone, and so the flapping
    documented in the exploration is visible as what it is — ±1 routes on
    scattered days, never a trend.

    Columns
      snapshot_date (PK), date_key (FK dim_date)
      routes, stops, route_stops       row counts in the snapshot
      routes_with_stops                routes that have at least one member stop
      trunk_routes                     routes matching the BRT pattern
      previous_snapshot_date           the snapshot compared against (null for the first)
      days_since_previous              gap in days — a missed daily run shows here
      routes_added, routes_removed     route_ids present now / before but not both
      stops_added, stops_removed       likewise for stop_id

    "Removed" is relative to the previous snapshot only. A route absent for
    one day and back the next counts as removed then added; whether that is
    real is a question for dim_route.last_seen_date over a longer span.
    """
    trunk = F.coalesce(F.col("route_name"), F.lit("")).rlike(f"(?i){TRUNK_ROUTE_PATTERN}")
    per_day_routes = routes.groupBy("snapshot_date").agg(
        F.count("*").alias("routes"),
        F.sum(trunk.cast("int")).alias("trunk_routes"),
        F.collect_set("route_id").alias("_route_ids"),
    )
    per_day_stops = stops.groupBy("snapshot_date").agg(
        F.count("*").alias("stops"), F.collect_set("stop_id").alias("_stop_ids")
    )
    per_day_members = route_stops.groupBy("snapshot_date").agg(
        F.count("*").alias("route_stops"),
        F.countDistinct("route_id").alias("routes_with_stops"),
    )

    chronological = Window.orderBy("snapshot_date")
    previous = lambda c: F.lag(c).over(chronological)  # noqa: E731
    diff = lambda now, before: F.size(F.array_except(now, before))  # noqa: E731
    empty = F.array().cast("array<string>")

    snapshots = (
        per_day_routes.join(per_day_stops, "snapshot_date", "outer")
        .join(per_day_members, "snapshot_date", "outer")
        .withColumn("previous_snapshot_date", previous("snapshot_date"))
        .withColumn("_prev_route_ids", F.coalesce(previous("_route_ids"), empty))
        .withColumn("_prev_stop_ids", F.coalesce(previous("_stop_ids"), empty))
    )
    first = F.col("previous_snapshot_date").isNull()
    now_r, prev_r = F.col("_route_ids"), F.col("_prev_route_ids")
    now_s, prev_s = F.col("_stop_ids"), F.col("_prev_stop_ids")
    since_previous = lambda c: F.when(first, None).otherwise(c)  # noqa: E731
    return snapshots.select(
        "snapshot_date",
        date_key(F.col("snapshot_date")).alias("date_key"),
        F.coalesce("routes", F.lit(0)).alias("routes"),
        F.coalesce("stops", F.lit(0)).alias("stops"),
        F.coalesce("route_stops", F.lit(0)).alias("route_stops"),
        F.coalesce("routes_with_stops", F.lit(0)).alias("routes_with_stops"),
        F.coalesce("trunk_routes", F.lit(0)).alias("trunk_routes"),
        "previous_snapshot_date",
        F.datediff("snapshot_date", "previous_snapshot_date").alias("days_since_previous"),
        since_previous(diff(now_r, prev_r)).alias("routes_added"),
        since_previous(diff(prev_r, now_r)).alias("routes_removed"),
        since_previous(diff(now_s, prev_s)).alias("stops_added"),
        since_previous(diff(prev_s, now_s)).alias("stops_removed"),
    ).orderBy("snapshot_date")
