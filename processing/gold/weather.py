"""
Weather: the 3-hourly series and the condition dimension.

What the raw weather is
  One OpenWeather call per day, normally at 05:00 UTC (midnight in Quito).
  Each call writes one `current` observation and forty `forecast` rows — the
  next five days at three-hour steps, on the UTC grid 00, 03, ... 21. So a
  given target time (say 2026-08-27 15:00 UTC) was forecast up to five
  times, on five consecutive mornings, with lead times from about 10 hours
  to about 118 hours; and only one real observation exists per day, at an
  odd minute (04:46, 05:00, ...).

How the series is built (notebooks/01_exploring.ipynb, section 4)
  One row per three-hour UTC slot. For each slot the best available row is
  kept, in this order:

    1. a `current` observation whose timestamp rounds to the slot
       (at most 90 minutes away — the distance is kept as offset_minutes)
    2. otherwise the `forecast` for that slot with the shortest lead time,
       i.e. the one issued most recently before it

  Provenance travels with every row — record_type, lead_hours,
  offset_minutes, observed_at, fetched_at — so a consumer can always tell a
  measured value from a predicted one and weight it accordingly. Objective 5
  in docs/ANALYSIS_PLAN.md is exploratory for exactly this reason.

Rain
  weather_loader stores OpenWeather's `rain.1h` for observations but
  `rain.3h` for forecasts, both under the name rain_1h_mm. The gold column
  is rain_mm with rain_period_hours (1 or 3) beside it, so the accumulation
  window is explicit rather than wrong in the column name.

The condition dimension
  OpenWeather's (weather_main, weather_description) pairs — "Rain / light
  rain" — keyed by a readable natural key ("rain/light_rain"). A generated
  integer would renumber every time a new condition appeared in the data,
  which would break idempotent rebuilds; the natural key cannot.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from processing.gold.config import UTC_OFFSET_HOURS, WEATHER_GRID_HOURS
from processing.gold.time import date_key

RECORD_TYPE_CURRENT = "current"
RECORD_TYPE_FORECAST = "forecast"

MEASURES = (
    "temp_c",
    "feels_like_c",
    "humidity_pct",
    "pressure_hpa",
    "wind_speed_ms",
    "wind_deg",
    "clouds_pct",
    "visibility_m",
)

GRID_SECONDS = WEATHER_GRID_HOURS * 3600


def condition_key(weather_main, weather_description):
    """"Rain" + "light rain" -> "rain/light_rain". Lower-cased and
    underscored so it is safe as a partition value, a URL and a Redshift
    key alike."""
    slug = lambda c: F.regexp_replace(F.lower(F.trim(c)), r"\s+", "_")  # noqa: E731
    return F.concat_ws("/", slug(weather_main), slug(weather_description))


def nearest_grid_slot(ts_col):
    """The three-hour UTC slot closest to a timestamp: 04:46 -> 06:00,
    04:29 -> 03:00. Exactly halfway rounds up."""
    seconds = F.unix_timestamp(ts_col)
    return F.from_unixtime(
        F.floor((seconds + GRID_SECONDS / 2) / GRID_SECONDS) * GRID_SECONDS
    ).cast("timestamp")


# ── fct_weather ──────────────────────────────────────────────────────────────


def build_fct_weather(weather: DataFrame) -> DataFrame:
    """One row per three-hour UTC slot, best available source.

    Columns
      weather_ts (PK)      the slot, UTC
      local_ts, local_date, date_key, local_hour
      condition_key        FK to dim_weather_condition
      weather_main, weather_description
      record_type          "current" or "forecast"
      lead_hours           forecast horizon; 0 for observations
      offset_minutes       observed_at - weather_ts, in minutes (0 for forecasts)
      observed_at          the row's own timestamp
      fetched_at           when the API was called
      temp_c, feels_like_c, humidity_pct, pressure_hpa, wind_speed_ms,
      wind_deg, clouds_pct, visibility_m
      rain_mm, rain_period_hours
    """
    observed_at = F.to_timestamp("timestamp")
    fetched_at = F.to_timestamp("fetched_at")
    is_current = F.col("record_type") == RECORD_TYPE_CURRENT

    candidates = (
        weather.withColumn("observed_at", observed_at)
        .withColumn("fetched_at", fetched_at)
        .withColumn(
            "weather_ts",
            F.when(is_current, nearest_grid_slot(F.col("observed_at"))).otherwise(
                F.col("observed_at")
            ),
        )
        .withColumn(
            "lead_hours",
            F.when(is_current, F.lit(0.0)).otherwise(
                F.round(
                    (F.unix_timestamp("observed_at") - F.unix_timestamp("fetched_at")) / 3600, 2
                )
            ),
        )
        .withColumn(
            "offset_minutes",
            ((F.unix_timestamp("observed_at") - F.unix_timestamp("weather_ts")) / 60).cast("int"),
        )
        .withColumn("rain_mm", F.col("rain_1h_mm"))
        .withColumn("rain_period_hours", F.when(is_current, 1).otherwise(WEATHER_GRID_HOURS))
    )

    # Observations first, then the shortest lead; fetched_at last so that two
    # identical candidates (a re-run that wrote the same day twice) still
    # rank deterministically.
    best_first = Window.partitionBy("weather_ts").orderBy(
        F.when(is_current, 0).otherwise(1), F.col("lead_hours"), F.col("fetched_at").desc()
    )
    best = candidates.withColumn("_rank", F.row_number().over(best_first)).filter("_rank = 1")

    local_ts = F.col("weather_ts") + F.expr(f"INTERVAL {UTC_OFFSET_HOURS} HOURS")
    key = condition_key(F.col("weather_main"), F.col("weather_description"))
    return (
        best.withColumn("local_ts", local_ts)
        .withColumn("local_date", F.to_date(local_ts))
        .withColumn("date_key", date_key(F.to_date(local_ts)))
        .withColumn("local_hour", F.hour(local_ts))
        .withColumn("condition_key", key)
        .select(
            "weather_ts",
            "local_ts",
            "local_date",
            "date_key",
            "local_hour",
            "condition_key",
            "weather_main",
            "weather_description",
            "record_type",
            "lead_hours",
            "offset_minutes",
            "observed_at",
            "fetched_at",
            *MEASURES,
            "rain_mm",
            "rain_period_hours",
        )
    )


# ── dim_weather_condition ────────────────────────────────────────────────────


def build_dim_weather_condition(weather: DataFrame) -> DataFrame:
    """One row per (weather_main, weather_description) seen in the raw data.

    Columns
      condition_key (PK)   "rain/light_rain"
      weather_main         OpenWeather group: Clear, Clouds, Rain, ...
      weather_description  the finer label
      is_precipitation     Rain, Drizzle, Snow or Thunderstorm
      raw_rows             how many raw rows carried it — a hint of how rare
                           a condition is before anyone charts it
    """
    precipitation = ("Rain", "Drizzle", "Snow", "Thunderstorm")
    key = condition_key(F.col("weather_main"), F.col("weather_description"))
    return (
        weather.groupBy("weather_main", "weather_description")
        .agg(F.count("*").alias("raw_rows"))
        .withColumn("condition_key", key)
        .withColumn("is_precipitation", F.col("weather_main").isin(*precipitation))
        .select(
            "condition_key", "weather_main", "weather_description", "is_precipitation", "raw_rows"
        )
    )
