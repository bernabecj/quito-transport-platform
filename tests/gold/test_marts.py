"""Tests for processing/gold/marts.py.

The hub is synthesised with a known shape: corridor A takes 1,000 s overnight
and 1,500 s by day, corridor B is flat, and corridor C has one day of data.
The marts must report exactly that.
"""

from processing.gold.dimensions import build_dim_date
from processing.gold.marts import (
    MIN_SAMPLE_DAYS,
    MIN_SAMPLES_PER_CELL,
    build_agg_corridor_hourly_profile,
    build_agg_corridor_reliability,
)
from processing.gold.time import date_key
from tests.gold.conftest import frame, rows_by

HUB_SCHEMA = (
    "route_id string, utc_ts timestamp, local_date date, date_key int, local_hour int, "
    "duration_seconds double, mean_speed_kmh double, distance_meters double, is_complete_day boolean"
)
# 2026-08-24 (Mon) .. 2026-08-30 (Sun): five weekdays, two weekend days.
DAYS = [f"2026-08-{d:02d}" for d in range(24, 31)]


def hub_rows(route_id, days, duration_for_hour, distance=20000.0, complete=True):
    rows = []
    for day in days:
        for hour in range(24):
            duration = duration_for_hour(hour)
            rows.append(
                {
                    "route_id": route_id,
                    "utc_ts": f"{day} {(hour + 5) % 24:02d}:00:00",
                    "local_date": day,
                    "date_key": int(day.replace("-", "")),
                    "local_hour": hour,
                    "duration_seconds": duration,
                    "mean_speed_kmh": distance / duration * 3.6,
                    "distance_meters": distance,
                    "is_complete_day": complete,
                }
            )
    return rows


def build_hub(spark, rows):
    df = spark.createDataFrame(
        [
            (
                r["route_id"], r["utc_ts"], r["local_date"], r["date_key"], r["local_hour"],
                r["duration_seconds"], r["mean_speed_kmh"], r["distance_meters"], r["is_complete_day"],
            )
            for r in rows
        ],
        "route_id string, utc_ts string, local_date string, date_key int, local_hour int, "
        "duration_seconds double, mean_speed_kmh double, distance_meters double, is_complete_day boolean",
    )
    return df.selectExpr(
        "route_id", "cast(utc_ts as timestamp) utc_ts", "cast(local_date as date) local_date",
        "date_key", "local_hour", "duration_seconds", "mean_speed_kmh", "distance_meters",
        "is_complete_day",
    )


def peaky(hour):
    return 1500.0 if 7 <= hour <= 18 else 1000.0


def flat(_hour):
    return 1200.0


def test_hourly_profile_baseline_ratio_peak_and_thin_cells(spark):
    hub = build_hub(spark, hub_rows("A", DAYS, peaky) + hub_rows("B", DAYS, flat))
    dim_date = build_dim_date(spark, DAYS[0], DAYS[-1])
    prof = build_agg_corridor_hourly_profile(hub, dim_date)

    assert prof.count() == 2 * 24 * 3
    got = rows_by(prof, "route_id", "local_hour", "day_type")

    night, day = got[("A", 2, "all")], got[("A", 12, "all")]
    assert (night.n, night.sample_days, night.baseline_duration_s) == (7, 7, 1000.0)
    assert (night.ratio_to_baseline, night.is_peak) == (1.0, False)
    assert (day.duration_median_s, day.ratio_to_baseline, day.is_peak) == (1500.0, 1.5, True)
    assert day.corridor_duration_median_s == 1250.0  # 12 hours at 1500, 12 at 1000 -> midpoint

    assert got[("B", 12, "all")].is_peak is False  # flat corridor never peaks

    weekday, weekend = got[("A", 12, "weekday")], got[("A", 12, "weekend")]
    assert (weekday.n, weekday.meets_min_samples) == (5, True)
    assert (weekend.n, weekend.meets_min_samples) == (2, False)
    assert MIN_SAMPLES_PER_CELL == 3


def test_hourly_profile_ignores_incomplete_days(spark):
    hub = build_hub(spark, hub_rows("A", DAYS[:3], peaky) + hub_rows("A", DAYS[3:], peaky, complete=False))
    prof = build_agg_corridor_hourly_profile(hub, build_dim_date(spark, DAYS[0], DAYS[-1]))
    assert rows_by(prof, "route_id", "local_hour", "day_type")[("A", 12, "all")].n == 3


def test_reliability_spreads_ranks_and_min_history(spark):
    hub = build_hub(
        spark,
        hub_rows("A", DAYS, peaky)
        + hub_rows("B", DAYS, flat, distance=22000.0)
        + hub_rows("C", DAYS[:1], lambda h: 3000.0 if h == 8 else 1000.0),
    )
    rel = build_agg_corridor_reliability(hub, build_dim_date(spark, DAYS[0], DAYS[-1]))
    got = rows_by(rel, "route_id")

    a, b, c = got["A"], got["B"], got["C"]
    assert (a.sample_days, a.is_ranked, a.baseline_duration_s, a.peak_duration_s) == (7, True, 1000.0, 1500.0)
    assert (a.spread_duration_pct, a.peak_hour_local) == (50.0, 7)  # first hour of the plateau
    assert a.spread_speed_pct == round((1 - 1000 / 1500) * 100, 1)  # speed is distance/duration
    assert (b.spread_duration_pct, b.cv_within_hour_duration) == (0.0, 0.0)

    # C has the biggest spread of all but one day of data: numbers present, ranks null.
    assert (c.sample_days, c.is_ranked, c.spread_duration_pct) == (1, False, 200.0)
    assert (c.rank_spread_duration, c.rank_cv_duration) == (None, None)
    assert MIN_SAMPLE_DAYS == 5

    # Among ranked corridors, 1 = worst.
    assert (a.rank_spread_duration, b.rank_spread_duration) == (1, 2)
    assert (a.rank_spread_speed, b.rank_spread_speed) == (1, 2)
    assert a.spread_ranks_agree and b.spread_ranks_agree
    assert a.distance_spread_pct == 0.0 and a.distance_min_m == 20000.0
