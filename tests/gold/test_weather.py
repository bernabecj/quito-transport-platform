"""Tests for processing/gold/weather.py, on hand-built frames."""

from pyspark.sql import functions as F

from processing.gold.weather import (
    build_dim_weather_condition,
    build_fct_weather,
    condition_key,
    nearest_grid_slot,
)
from tests.gold.conftest import frame, rows_by

WEATHER_SCHEMA = (
    "record_type string, fetched_at string, timestamp string, temp_c double, feels_like_c double, "
    "humidity_pct long, pressure_hpa long, weather_main string, weather_description string, "
    "wind_speed_ms double, wind_deg long, rain_1h_mm double, clouds_pct long, visibility_m long"
)


def forecast(fetched, target, temp, main="Clouds", desc="broken clouds", rain=0.0):
    return {
        "record_type": "forecast", "fetched_at": fetched, "timestamp": target, "temp_c": temp,
        "weather_main": main, "weather_description": desc, "rain_1h_mm": rain,
    }


def current(fetched, observed, temp, main="Rain", desc="light rain", rain=0.4):
    return {
        "record_type": "current", "fetched_at": fetched, "timestamp": observed, "temp_c": temp,
        "weather_main": main, "weather_description": desc, "rain_1h_mm": rain,
    }


def test_nearest_grid_slot_rounds_to_the_closest_three_hours(spark):
    cases = {
        "2026-08-25T04:46:00+00:00": "2026-08-25 06:00:00",  # 74 min to 06, 106 to 03
        "2026-08-25T04:29:00+00:00": "2026-08-25 03:00:00",
        "2026-08-25T12:00:00+00:00": "2026-08-25 12:00:00",
        "2026-08-25T22:31:00+00:00": "2026-08-26 00:00:00",  # rolls into the next day
    }
    df = spark.createDataFrame([(k,) for k in cases], ["ts"]).withColumn(
        "slot", nearest_grid_slot(F.to_timestamp("ts"))
    )
    assert {r.ts: str(r.slot) for r in df.collect()} == cases


def test_condition_key_is_a_lowercase_slug(spark):
    df = spark.createDataFrame([("Rain", "heavy intensity rain")], ["m", "d"])
    assert df.select(condition_key(F.col("m"), F.col("d"))).first()[0] == "rain/heavy_intensity_rain"


def test_fct_weather_prefers_observation_then_shortest_lead(spark):
    raw = frame(
        spark,
        [
            # Slot 12:00 on the 25th forecast twice; the later issue (shorter lead) must win.
            forecast("2026-08-24T05:00:00+00:00", "2026-08-25T12:00:00+00:00", temp=10.0),
            forecast("2026-08-25T05:00:00+00:00", "2026-08-25T12:00:00+00:00", temp=11.0),
            # Slot 06:00 on the 25th has a forecast AND an observation at 05:01 -> observation wins.
            forecast("2026-08-25T05:00:00+00:00", "2026-08-25T06:00:00+00:00", temp=8.0),
            current("2026-08-25T05:01:00+00:00", "2026-08-25T05:01:00+00:00", temp=9.5),
        ],
        WEATHER_SCHEMA,
    )
    got = rows_by(build_fct_weather(raw), "weather_ts")
    assert {str(k) for k in got} == {"2026-08-25 06:00:00", "2026-08-25 12:00:00"}

    noon = got[[k for k in got if str(k).endswith("12:00:00")][0]]
    assert (noon.record_type, noon.temp_c, noon.lead_hours, noon.offset_minutes) == ("forecast", 11.0, 7.0, 0)
    assert (noon.rain_period_hours, noon.local_hour, noon.date_key) == (3, 7, 20260825)

    six = got[[k for k in got if str(k).endswith("06:00:00")][0]]
    assert (six.record_type, six.temp_c, six.lead_hours, six.offset_minutes) == ("current", 9.5, 0.0, -59)
    assert (six.rain_period_hours, six.rain_mm, six.condition_key) == (1, 0.4, "rain/light_rain")


def test_dim_weather_condition_one_row_per_pair_with_precipitation_flag(spark):
    raw = frame(
        spark,
        [
            forecast("2026-08-24T05:00:00+00:00", "2026-08-25T12:00:00+00:00", 10.0),
            forecast("2026-08-24T05:00:00+00:00", "2026-08-25T15:00:00+00:00", 10.0),
            current("2026-08-25T05:01:00+00:00", "2026-08-25T05:01:00+00:00", 9.5),
        ],
        WEATHER_SCHEMA,
    )
    got = rows_by(build_dim_weather_condition(raw), "condition_key")
    assert set(got) == {"clouds/broken_clouds", "rain/light_rain"}
    assert (got["clouds/broken_clouds"].is_precipitation, got["clouds/broken_clouds"].raw_rows) == (False, 2)
    assert (got["rain/light_rain"].is_precipitation, got["rain/light_rain"].raw_rows) == (True, 1)
