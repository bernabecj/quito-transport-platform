"""Tests for processing/gold/facts.py, on hand-built frames."""

from datetime import date, datetime

from processing.gold.facts import (
    build_fct_corridor_travel_time,
    build_fct_network_snapshot,
    build_fct_route_stop,
)
from tests.gold.conftest import frame, rows_by

TRAFFIC_SCHEMA = (
    "route_id string, sampled_at string, year int, month int, day int, hour int, "
    "local_hour long, local_weekday long, waypoint_count long, mean_hop_km double, "
    "sequence_source string, stops_excluded long, distance_meters double, duration_seconds double, "
    "mean_speed_kmh double, segments_total long, segments_known long, congestion_known_pct double, "
    "congestion_low long, congestion_moderate long, congestion_heavy long, congestion_severe long, "
    "congestion_unknown long"
)
WEATHER_FACT_SCHEMA = (
    "weather_ts timestamp, condition_key string, record_type string, lead_hours double, "
    "temp_c double, feels_like_c double, humidity_pct long, pressure_hpa long, wind_speed_ms double, "
    "wind_deg long, clouds_pct long, visibility_m long, rain_mm double, rain_period_hours int"
)
ROUTES_SCHEMA = "route_id string, route_name string, snapshot_date date"
STOPS_SCHEMA = "stop_id string, snapshot_date date"
ROUTE_STOPS_SCHEMA = (
    "route_id string, stop_id string, stop_sequence long, member_role string, "
    "sequence_source string, member_index long, distance_along_m double, snap_distance_m double, "
    "snapshot_date date"
)
D1, D2, D3 = date(2026, 8, 19), date(2026, 8, 20), date(2026, 8, 21)


def sample(route_id, day, hour, duration=3000.0, minute=0, **extra):
    row = {
        "route_id": route_id,
        "sampled_at": f"2026-08-{day:02d}T{hour:02d}:{minute:02d}:00+00:00",
        "year": 2026, "month": 8, "day": day, "hour": hour,
        "local_hour": (hour - 5) % 24, "local_weekday": 1,
        "sequence_source": "osm_member", "stops_excluded": 0,
        "distance_meters": 20000.0, "duration_seconds": duration, "mean_speed_kmh": 24.0,
    }
    row.update(extra)
    return row


def weather_slot(ts, temp, key="clouds/few_clouds"):
    return {
        "weather_ts": datetime.fromisoformat(ts), "condition_key": key,
        "record_type": "forecast", "lead_hours": 5.0, "temp_c": temp,
    }


def full_day(route_id, day):
    """24 hourly samples covering local day `day` (05:00 UTC that day to 04:00 UTC next)."""
    return [sample(route_id, day, h) for h in range(5, 24)] + [sample(route_id, day + 1, h) for h in range(0, 5)]


# ── fct_corridor_travel_time ─────────────────────────────────────────────────


def test_hub_enforces_one_row_per_route_hour_keeping_the_latest_call(spark):
    traffic = frame(
        spark,
        [sample("1", 25, 12, duration=100.0, minute=0), sample("1", 25, 12, duration=200.0, minute=30)],
        TRAFFIC_SCHEMA,
    )
    weather = frame(spark, [], WEATHER_FACT_SCHEMA)
    out = build_fct_corridor_travel_time(traffic, weather)
    assert out.count() == 1 and out.first().duration_seconds == 200.0


def test_hub_coverage_columns_derive_the_complete_day(spark):
    traffic = frame(spark, full_day("1", 25) + [sample("1", 27, 12), sample("2", 27, 12)], TRAFFIC_SCHEMA)
    weather = frame(spark, [], WEATHER_FACT_SCHEMA)
    out = build_fct_corridor_travel_time(traffic, weather)
    by_day = {str(r.local_date): r for r in out.filter("route_id = '1'").collect()}
    assert (by_day["2026-08-25"].hours_sampled_that_day, by_day["2026-08-25"].is_complete_day) == (24, True)
    assert (by_day["2026-08-27"].hours_sampled_that_day, by_day["2026-08-27"].is_complete_day) == (1, False)
    assert out.filter("utc_ts = '2026-08-27 12:00:00'").first().routes_sampled_that_hour == 2
    assert out.filter("is_complete_day").count() == 24


def test_hub_joins_the_nearest_weather_slot_and_keeps_rows_without_one(spark):
    traffic = frame(spark, [sample("1", 25, 13), sample("1", 25, 14), sample("1", 26, 2)], TRAFFIC_SCHEMA)
    weather = frame(
        spark,
        [weather_slot("2026-08-25 12:00:00", 10.0), weather_slot("2026-08-25 15:00:00", 20.0, "rain/light_rain")],
        WEATHER_FACT_SCHEMA,
    )
    got = {str(r.utc_ts): r for r in build_fct_corridor_travel_time(traffic, weather).collect()}
    assert (got["2026-08-25 13:00:00"].temp_c, got["2026-08-25 13:00:00"].weather_offset_minutes) == (10.0, 60)
    assert (got["2026-08-25 14:00:00"].temp_c, got["2026-08-25 14:00:00"].condition_key) == (20.0, "rain/light_rain")
    assert got["2026-08-25 14:00:00"].weather_record_type == "forecast"
    missing = got["2026-08-26 02:00:00"]  # slot 03:00 does not exist -> nulls, row kept
    assert str(missing.weather_ts) == "2026-08-26 03:00:00" and missing.temp_c is None


def test_hub_carries_provenance_from_old_and_new_traffic_shapes(spark):
    traffic = frame(
        spark,
        [sample("1", 25, 12), sample("2", 25, 12, sequence_source="geometry", stops_excluded=3)],
        TRAFFIC_SCHEMA,
    )
    got = rows_by(build_fct_corridor_travel_time(traffic, frame(spark, [], WEATHER_FACT_SCHEMA)), "route_id")
    assert (got["1"].sequence_source, got["1"].stops_excluded) == ("osm_member", 0)
    assert (got["2"].sequence_source, got["2"].stops_excluded) == ("geometry", 3)


# ── fct_route_stop ───────────────────────────────────────────────────────────


def test_route_stop_is_the_latest_snapshot_and_keeps_a_repeated_stop(spark):
    rs = frame(
        spark,
        [
            {"route_id": "1", "stop_id": "a", "stop_sequence": 0, "sequence_source": "osm_member", "snapshot_date": D1},
            {"route_id": "1", "stop_id": "a", "stop_sequence": 0, "sequence_source": "geometry", "snapshot_date": D2, "snap_distance_m": 12.0},
            {"route_id": "1", "stop_id": "a", "stop_sequence": 1, "sequence_source": "geometry", "snapshot_date": D2, "snap_distance_m": 12.0},
        ],
        ROUTE_STOPS_SCHEMA,
    )
    out = build_fct_route_stop(rs)
    assert out.count() == 2
    assert out.select("snapshot_date", "sequence_source").distinct().collect() == [(D2, "geometry")]
    assert sorted(r.stop_sequence for r in out.collect()) == [0, 1]


# ── fct_network_snapshot ─────────────────────────────────────────────────────


def test_network_snapshot_counts_and_deltas_show_flapping_as_plus_minus_one(spark):
    routes = frame(
        spark,
        [
            {"route_id": "1", "route_name": "Trolebus C1", "snapshot_date": D1},
            {"route_id": "2", "route_name": "Feeder", "snapshot_date": D1},
            {"route_id": "1", "route_name": "Trolebus C1", "snapshot_date": D2},  # route 2 missing
            {"route_id": "1", "route_name": "Trolebus C1", "snapshot_date": D3},
            {"route_id": "2", "route_name": "Feeder", "snapshot_date": D3},  # and back
        ],
        ROUTES_SCHEMA,
    )
    stops = frame(spark, [{"stop_id": "a", "snapshot_date": d} for d in (D1, D2, D3)], STOPS_SCHEMA)
    rs = frame(
        spark,
        [{"route_id": "1", "stop_id": "a", "stop_sequence": 0, "snapshot_date": d} for d in (D1, D2, D3)],
        ROUTE_STOPS_SCHEMA,
    )
    got = rows_by(build_fct_network_snapshot(routes, stops, rs), "snapshot_date")
    assert [got[d].routes for d in (D1, D2, D3)] == [2, 1, 2]
    assert [got[d].trunk_routes for d in (D1, D2, D3)] == [1, 1, 1]
    assert (got[D1].previous_snapshot_date, got[D1].routes_added, got[D1].routes_removed) == (None, None, None)
    assert (got[D2].routes_added, got[D2].routes_removed, got[D2].days_since_previous) == (0, 1, 1)
    assert (got[D3].routes_added, got[D3].routes_removed, got[D3].stops_added) == (1, 0, 0)
    assert got[D3].date_key == 20260821 and got[D3].routes_with_stops == 1
