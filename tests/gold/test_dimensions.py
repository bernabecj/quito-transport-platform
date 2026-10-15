"""Tests for processing/gold/dimensions.py and time.py, on hand-built frames."""

from datetime import date

from pyspark.sql import functions as F

from processing.gold.dimensions import (
    build_dim_date,
    build_dim_route,
    build_dim_stop,
    latest_snapshot,
    latest_version,
)
from processing.gold.time import date_key, with_time_columns
from tests.gold.conftest import frame, rows_by

ROUTES_SCHEMA = (
    "route_id string, route_ref string, route_name string, route_type string, operator string, "
    "network string, origin string, destination string, colour string, snapshot_date date"
)
STOPS_SCHEMA = (
    "stop_id string, stop_name string, latitude double, longitude double, stop_type string, "
    "shelter string, snapshot_date date"
)
ROUTE_STOPS_SCHEMA = (
    "route_id string, stop_id string, stop_sequence long, member_role string, "
    "sequence_source string, snapshot_date date"
)
TRAFFIC_SCHEMA = "route_id string, year int, month int, day int, hour int"

D1, D2, D3 = date(2026, 8, 19), date(2026, 8, 20), date(2026, 8, 21)


def route(route_id, name, day, ref="C1"):
    return {"route_id": route_id, "route_ref": ref, "route_name": name, "snapshot_date": day}


# ── time.py ──────────────────────────────────────────────────────────────────


def test_with_time_columns_shifts_utc_to_quito_and_keys_the_local_date(spark):
    # 03:00 UTC on the 20th is 22:00 on the 19th in Quito.
    df = with_time_columns(
        frame(spark, [{"route_id": "1", "year": 2026, "month": 8, "day": 20, "hour": 3}], TRAFFIC_SCHEMA)
    )
    r = df.first()
    assert str(r.utc_ts) == "2026-08-20 03:00:00"
    assert str(r.local_ts) == "2026-08-19 22:00:00"
    assert (str(r.local_date), r.date_key) == ("2026-08-19", 20260819)


def test_date_key_is_yyyymmdd(spark):
    assert spark.range(1).select(date_key(F.lit("2026-01-05").cast("date"))).first()[0] == 20260105


# ── latest_version / latest_snapshot ─────────────────────────────────────────


def test_latest_version_keeps_newest_attributes_and_records_presence(spark):
    routes = frame(
        spark,
        [route("1", "Old name", D1), route("1", "New name", D3), route("2", "Only once", D2)],
        ROUTES_SCHEMA,
    )
    got = rows_by(latest_version(routes, "route_id", ("route_ref", "route_name")), "route_id")
    assert got["1"].route_name == "New name"
    assert (got["1"].first_seen_date, got["1"].last_seen_date, got["1"].snapshots_present) == (D1, D3, 2)
    assert (got["2"].first_seen_date, got["2"].last_seen_date, got["2"].snapshots_present) == (D2, D2, 1)


def test_latest_snapshot_returns_only_the_newest_day(spark):
    rs = frame(
        spark,
        [
            {"route_id": "1", "stop_id": "a", "stop_sequence": 0, "snapshot_date": D1},
            {"route_id": "1", "stop_id": "a", "stop_sequence": 0, "snapshot_date": D2},
            {"route_id": "1", "stop_id": "b", "stop_sequence": 1, "snapshot_date": D2},
        ],
        ROUTE_STOPS_SCHEMA,
    )
    out = latest_snapshot(rs)
    assert out.count() == 2 and out.select("snapshot_date").distinct().first()[0] == D2


# ── dim_date ─────────────────────────────────────────────────────────────────


def test_dim_date_covers_the_span_inclusive_with_weekday_semantics(spark):
    dd = build_dim_date(spark, "2026-08-29", "2026-08-31")  # Sat, Sun, Mon
    got = rows_by(dd, "date_key")
    assert sorted(got) == [20260829, 20260830, 20260831]
    assert [(got[k].weekday, got[k].weekday_name, got[k].day_type) for k in sorted(got)] == [
        (5, "Saturday", "weekend"),
        (6, "Sunday", "weekend"),
        (0, "Monday", "weekday"),
    ]
    assert got[20260829].is_weekend and not got[20260831].is_weekend


# ── dim_route ────────────────────────────────────────────────────────────────


def test_dim_route_keeps_a_flapping_route_and_flags_trunk_and_sampled(spark):
    routes = frame(
        spark,
        [
            route("1", "Trolebus C1 A => B", D1),
            route("1", "Trolebus C1 A => B", D3),  # absent on D2: flapping
            route("2", "Ecovía E3 X - Y", D1, ref="E3"),
            route("2", "Ecovía E3 X - Y", D2, ref="E3"),
            route("2", "Ecovía E3 X - Y", D3, ref="E3"),
            route("3", "Cooperativa feeder", D3, ref="144"),
        ],
        ROUTES_SCHEMA,
    )
    rs = frame(
        spark,
        [
            {"route_id": "1", "stop_id": "a", "stop_sequence": 0, "sequence_source": "osm_member", "snapshot_date": D3},
            {"route_id": "1", "stop_id": "b", "stop_sequence": 1, "sequence_source": "osm_member", "snapshot_date": D3},
            {"route_id": "2", "stop_id": "a", "stop_sequence": 0, "sequence_source": "geometry", "snapshot_date": D3},
            {"route_id": "1", "stop_id": "z", "stop_sequence": 0, "sequence_source": "osm_member", "snapshot_date": D1},
        ],
        ROUTE_STOPS_SCHEMA,
    )
    traffic = frame(spark, [{"route_id": "1", "year": 2026, "month": 8, "day": 20, "hour": 12}], TRAFFIC_SCHEMA)

    got = rows_by(build_dim_route(routes, rs, traffic), "route_id")
    assert set(got) == {"1", "2", "3"}
    assert (got["1"].is_trunk, got["1"].is_sampled, got["1"].snapshots_present) == (True, True, 2)
    assert (got["2"].is_trunk, got["2"].is_sampled, got["2"].sequence_source) == (True, False, "geometry")
    assert (got["3"].is_trunk, got["3"].is_sampled, got["3"].stop_count, got["3"].sequence_source) == (
        False, False, 0, None,
    )
    # stop_count comes from the latest snapshot only, not every snapshot stacked.
    assert got["1"].stop_count == 2


# ── dim_stop ─────────────────────────────────────────────────────────────────


def test_dim_stop_counts_routes_from_the_latest_membership(spark):
    stops = frame(
        spark,
        [
            {"stop_id": "a", "stop_name": "Estacion", "latitude": -0.2, "longitude": -78.5, "snapshot_date": D1},
            {"stop_id": "a", "stop_name": "Estación", "latitude": -0.2, "longitude": -78.5, "snapshot_date": D2},
            {"stop_id": "b", "stop_name": "Lonely", "latitude": -0.3, "longitude": -78.4, "snapshot_date": D2},
        ],
        STOPS_SCHEMA,
    )
    rs = frame(
        spark,
        [
            {"route_id": "1", "stop_id": "a", "stop_sequence": 0, "snapshot_date": D2},
            {"route_id": "2", "stop_id": "a", "stop_sequence": 0, "snapshot_date": D2},
            {"route_id": "2", "stop_id": "a", "stop_sequence": 5, "snapshot_date": D2},  # same stop twice
        ],
        ROUTE_STOPS_SCHEMA,
    )
    got = rows_by(build_dim_stop(stops, rs), "stop_id")
    assert got["a"].stop_name == "Estación" and got["a"].snapshots_present == 2
    assert got["a"].route_count == 2  # distinct routes, not membership rows
    assert got["b"].route_count == 0
