"""
Marts: the objectives in docs/ANALYSIS_PLAN.md, pre-aggregated.

    build_agg_corridor_hourly_profile   objectives 1 and 4 — the daily curve, by day type
    build_agg_corridor_reliability      objectives 2 and 3 — spread and unpredictability, ranked

This is the first place rows are collapsed. Everything above it keeps every
hourly sample; here those samples are grouped so Metabase reads an answer
instead of recomputing one per refresh. Both marts read the hub alone —
fct_corridor_travel_time filtered to complete days — plus dim_date for the
weekday/weekend split. Nothing here can disagree with the facts, because
nothing here exists that the facts do not.

Definitions (each is a column, so the dashboard shows the rule it applies)
  complete day       is_complete_day on the fact: all 24 hours sampled. Today
                     that is 2026-08-25 → 08-31; it grows when sampling resumes.
  day_type           "weekday", "weekend", or "all". "all" is the plain daily
                     curve (objective 1); the other two are objective 4.
  baseline           the corridor's own overnight median duration — local
                     hours 00:00–04:59, where the exploration measured a flat
                     33–34 km/h on every corridor. Each corridor is compared
                     against itself, never against another corridor.
  ratio_to_baseline  hourly median duration ÷ baseline; 1.5 means the hour
                     takes 50 % longer than overnight.
  is_peak            the hour's median duration is above the corridor's median
                     over all hours of that day type — the README's "when does
                     a corridor's travel time rise above its own daily median".
  meets_min_samples  n ≥ MIN_SAMPLES_PER_CELL. A cell below it is kept, flagged,
                     and never interpolated (objective 1's bar).
  is_ranked          sample_days ≥ MIN_SAMPLE_DAYS. A corridor with a day of
                     data does not top a ranking by accident; its row is
                     present with its numbers and null ranks.

On the rerouting confound (objective 3's bar)
  distance_meters varies between calls on the same corridor because Mapbox
  chooses different paths. A corridor that reroutes can look *more*
  predictable on duration — the reroute is what avoided the delay. So the
  reliability mart reports the within-hour variability under both duration
  and speed, ranks both, says whether the ranks agree, and puts
  distance_spread_pct beside them so a reader can see which corridors the
  confound applies to.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

BASELINE_HOURS = (0, 1, 2, 3, 4)
MIN_SAMPLES_PER_CELL = 3
MIN_SAMPLE_DAYS = 5

DAY_TYPE_ALL = "all"


def _percentile(col: str, p: float):
    """Exact percentile — the groups are small (≤ a few hundred rows), so the
    approximate variant buys nothing."""
    return F.expr(f"percentile({col}, {p})")


def complete_days(fct: DataFrame, dim_date: DataFrame) -> DataFrame:
    """The hub restricted to complete days, with day_type attached."""
    return fct.filter("is_complete_day").join(
        dim_date.select("date_key", "day_type"), "date_key"
    )


# ── agg_corridor_hourly_profile ──────────────────────────────────────────────


def build_agg_corridor_hourly_profile(fct: DataFrame, dim_date: DataFrame) -> DataFrame:
    """One row per (route_id, local_hour, day_type).

    Columns
      route_id (FK dim_route), local_hour, day_type     PK
      n, sample_days
      duration_median_s, duration_p10_s, duration_p90_s, duration_mean_s
      speed_median_kmh, speed_p10_kmh, speed_p90_kmh, speed_mean_kmh
      distance_median_m
      baseline_duration_s      overnight median for this corridor and day type
      ratio_to_baseline
      corridor_duration_median_s   median over all hours of this day type
      is_peak
      meets_min_samples
    """
    rows = complete_days(fct, dim_date)
    rows = rows.withColumn("day_type", F.lit(DAY_TYPE_ALL)).unionByName(rows)

    cells = rows.groupBy("route_id", "local_hour", "day_type").agg(
        F.count("*").alias("n"),
        F.countDistinct("local_date").alias("sample_days"),
        _percentile("duration_seconds", 0.5).alias("duration_median_s"),
        _percentile("duration_seconds", 0.1).alias("duration_p10_s"),
        _percentile("duration_seconds", 0.9).alias("duration_p90_s"),
        F.avg("duration_seconds").alias("duration_mean_s"),
        _percentile("mean_speed_kmh", 0.5).alias("speed_median_kmh"),
        _percentile("mean_speed_kmh", 0.1).alias("speed_p10_kmh"),
        _percentile("mean_speed_kmh", 0.9).alias("speed_p90_kmh"),
        F.avg("mean_speed_kmh").alias("speed_mean_kmh"),
        _percentile("distance_meters", 0.5).alias("distance_median_m"),
    )

    baseline = (
        rows.filter(F.col("local_hour").isin(*BASELINE_HOURS))
        .groupBy("route_id", "day_type")
        .agg(_percentile("duration_seconds", 0.5).alias("baseline_duration_s"))
    )
    corridor = rows.groupBy("route_id", "day_type").agg(
        _percentile("duration_seconds", 0.5).alias("corridor_duration_median_s")
    )

    rounded = lambda c: F.round(F.col(c), 1).alias(c)  # noqa: E731
    return (
        cells.join(baseline, ["route_id", "day_type"], "left")
        .join(corridor, ["route_id", "day_type"], "left")
        .withColumn(
            "ratio_to_baseline",
            F.round(F.col("duration_median_s") / F.col("baseline_duration_s"), 3),
        )
        .withColumn("is_peak", F.col("duration_median_s") > F.col("corridor_duration_median_s"))
        .withColumn("meets_min_samples", F.col("n") >= MIN_SAMPLES_PER_CELL)
        .select(
            "route_id",
            F.col("local_hour").cast("int").alias("local_hour"),
            "day_type",
            F.col("n").cast("int").alias("n"),
            F.col("sample_days").cast("int").alias("sample_days"),
            rounded("duration_median_s"),
            rounded("duration_p10_s"),
            rounded("duration_p90_s"),
            rounded("duration_mean_s"),
            rounded("speed_median_kmh"),
            rounded("speed_p10_kmh"),
            rounded("speed_p90_kmh"),
            rounded("speed_mean_kmh"),
            rounded("distance_median_m"),
            rounded("baseline_duration_s"),
            "ratio_to_baseline",
            rounded("corridor_duration_median_s"),
            "is_peak",
            "meets_min_samples",
        )
    )


# ── agg_corridor_reliability ─────────────────────────────────────────────────


def build_agg_corridor_reliability(fct: DataFrame, dim_date: DataFrame) -> DataFrame:
    """One row per route_id — every sampled corridor, ranked where it has
    enough history. Rank 1 is the worst (largest spread, least predictable).

    Columns
      route_id (PK, FK dim_route)
      samples, sample_days, first_sample_date, last_sample_date
      baseline_duration_s        overnight median (all day types)
      peak_duration_s, peak_hour_local
                                 the slowest hour's median, and which hour
      spread_duration_pct        (peak − baseline) / baseline × 100
      rank_spread_duration
      baseline_speed_kmh, trough_speed_kmh, trough_hour_local
      spread_speed_pct           (baseline − trough) / baseline × 100
      rank_spread_speed
      spread_ranks_agree         both spread ranks equal
      cv_within_hour_duration    mean over hours of (stddev ÷ mean) of duration
      cv_within_hour_speed       likewise for speed
      rank_cv_duration, rank_cv_speed, cv_ranks_agree
      distance_min_m, distance_max_m, distance_spread_pct
                                 the rerouting confound, in the open
      is_ranked                  sample_days ≥ MIN_SAMPLE_DAYS; ranks are null otherwise
    """
    rows = complete_days(fct, dim_date)

    per_route = rows.groupBy("route_id").agg(
        F.count("*").alias("samples"),
        F.countDistinct("local_date").alias("sample_days"),
        F.min("local_date").alias("first_sample_date"),
        F.max("local_date").alias("last_sample_date"),
        F.min("distance_meters").alias("distance_min_m"),
        F.max("distance_meters").alias("distance_max_m"),
    )
    baseline = (
        rows.filter(F.col("local_hour").isin(*BASELINE_HOURS))
        .groupBy("route_id")
        .agg(
            _percentile("duration_seconds", 0.5).alias("baseline_duration_s"),
            _percentile("mean_speed_kmh", 0.5).alias("baseline_speed_kmh"),
        )
    )

    # Hourly medians and within-hour variability, then the worst hour of each.
    hourly = rows.groupBy("route_id", "local_hour").agg(
        _percentile("duration_seconds", 0.5).alias("duration_median_s"),
        _percentile("mean_speed_kmh", 0.5).alias("speed_median_kmh"),
        (F.stddev("duration_seconds") / F.avg("duration_seconds")).alias("cv_duration"),
        (F.stddev("mean_speed_kmh") / F.avg("mean_speed_kmh")).alias("cv_speed"),
    )
    slowest_first = Window.partitionBy("route_id").orderBy(
        F.col("duration_median_s").desc(), F.col("local_hour")
    )
    fastest_first = Window.partitionBy("route_id").orderBy(
        F.col("speed_median_kmh").asc(), F.col("local_hour")
    )
    peak = (
        hourly.withColumn("_r", F.row_number().over(slowest_first))
        .filter("_r = 1")
        .select(
            "route_id",
            F.col("duration_median_s").alias("peak_duration_s"),
            F.col("local_hour").cast("int").alias("peak_hour_local"),
        )
    )
    trough = (
        hourly.withColumn("_r", F.row_number().over(fastest_first))
        .filter("_r = 1")
        .select(
            "route_id",
            F.col("speed_median_kmh").alias("trough_speed_kmh"),
            F.col("local_hour").cast("int").alias("trough_hour_local"),
        )
    )
    variability = hourly.groupBy("route_id").agg(
        F.avg("cv_duration").alias("cv_within_hour_duration"),
        F.avg("cv_speed").alias("cv_within_hour_speed"),
    )

    out = (
        per_route.join(baseline, "route_id", "left")
        .join(peak, "route_id", "left")
        .join(trough, "route_id", "left")
        .join(variability, "route_id", "left")
        .withColumn(
            "spread_duration_pct",
            F.round(
                (F.col("peak_duration_s") - F.col("baseline_duration_s"))
                / F.col("baseline_duration_s")
                * 100,
                1,
            ),
        )
        .withColumn(
            "spread_speed_pct",
            F.round(
                (F.col("baseline_speed_kmh") - F.col("trough_speed_kmh"))
                / F.col("baseline_speed_kmh")
                * 100,
                1,
            ),
        )
        .withColumn(
            "distance_spread_pct",
            F.round(
                (F.col("distance_max_m") - F.col("distance_min_m")) / F.col("distance_min_m") * 100,
                1,
            ),
        )
        .withColumn("is_ranked", F.col("sample_days") >= MIN_SAMPLE_DAYS)
    )

    # Ranks only among corridors with enough history; worst first.
    def rank_desc(col: str):
        order = Window.partitionBy("is_ranked").orderBy(F.col(col).desc(), F.col("route_id"))
        return F.when(F.col("is_ranked"), F.rank().over(order)).cast("int")

    out = (
        out.withColumn("rank_spread_duration", rank_desc("spread_duration_pct"))
        .withColumn("rank_spread_speed", rank_desc("spread_speed_pct"))
        .withColumn("rank_cv_duration", rank_desc("cv_within_hour_duration"))
        .withColumn("rank_cv_speed", rank_desc("cv_within_hour_speed"))
        .withColumn(
            "spread_ranks_agree", F.col("rank_spread_duration") == F.col("rank_spread_speed")
        )
        .withColumn("cv_ranks_agree", F.col("rank_cv_duration") == F.col("rank_cv_speed"))
    )

    rounded = lambda c, d=1: F.round(F.col(c), d).alias(c)  # noqa: E731
    return out.select(
        "route_id",
        F.col("samples").cast("int").alias("samples"),
        F.col("sample_days").cast("int").alias("sample_days"),
        "first_sample_date",
        "last_sample_date",
        rounded("baseline_duration_s"),
        rounded("peak_duration_s"),
        "peak_hour_local",
        "spread_duration_pct",
        "rank_spread_duration",
        rounded("baseline_speed_kmh"),
        rounded("trough_speed_kmh"),
        "trough_hour_local",
        "spread_speed_pct",
        "rank_spread_speed",
        "spread_ranks_agree",
        rounded("cv_within_hour_duration", 4),
        rounded("cv_within_hour_speed", 4),
        "rank_cv_duration",
        "rank_cv_speed",
        "cv_ranks_agree",
        rounded("distance_min_m", 0),
        rounded("distance_max_m", 0),
        "distance_spread_pct",
        "is_ranked",
    )
