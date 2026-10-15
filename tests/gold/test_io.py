"""Tests for processing/gold/io.py.

Raw zones are built on the fly under tmp_path in the exact bucket layout the
loaders write, so every reader is exercised against directory partitions,
mixed old/new schemas and the committed samples — the three shapes it meets
in practice. No AWS, no network.
"""

import pytest
from pyspark.sql import functions as F

from processing.gold.config import (
    DEFAULT_SAMPLES_ROOT,
    LAYOUT_RAW,
    LAYOUT_SAMPLES,
    SEQUENCE_SOURCE_DEFAULT,
)
from processing.gold.io import (
    ensure_columns,
    join_path,
    read_network_table,
    read_table,
    read_traffic,
    read_weather,
    write_table,
)
from tests.gold.conftest import write_parquet

# ── Fixtures: minimal raw rows in the loaders' shapes ────────────────────────


def traffic_row(route_id, sampled_at, **extra):
    """The columns traffic_loader wrote before 2026-10, plus any new ones."""
    row = {
        "route_id": route_id,
        "route_ref": "C1",
        "route_name": "Trolebus C1",
        "sampled_at": sampled_at,
        "local_hour": 7,
        "local_weekday": 1,
        "waypoint_count": 20,
        "mean_hop_km": 0.6,
        "distance_meters": 20000.0,
        "duration_seconds": 3000.0,
        "mean_speed_kmh": 24.0,
        "segments_total": 100,
        "segments_known": 40,
        "congestion_known_pct": 40.0,
        "congestion_low": 30,
        "congestion_moderate": 5,
        "congestion_heavy": 3,
        "congestion_severe": 2,
        "congestion_unknown": 60,
    }
    row.update(extra)
    return row


def route_stop_row(route_id, stop_id, seq, fetched_at, **extra):
    row = {
        "route_id": route_id,
        "stop_id": stop_id,
        "stop_sequence": seq,
        "member_role": "stop",
        "fetched_at": fetched_at,
    }
    row.update(extra)
    return row


@pytest.fixture
def raw_root(tmp_path):
    """A two-day raw zone. Day 25 is the old shape everywhere; day 26 carries
    the corridor-recovery columns in both traffic and route_stops."""
    root = tmp_path / "raw"
    t = root / "traffic" / "year=2026" / "month=08"
    write_parquet(
        t / "day=25" / "hour=12" / "corridor_travel_time.parquet",
        [traffic_row("1", "2026-08-25T12:00:00+00:00")],
    )
    write_parquet(
        t / "day=26" / "hour=12" / "corridor_travel_time.parquet",
        [
            traffic_row(
                "1", "2026-08-26T12:00:00+00:00", sequence_source="geometry", stops_excluded=3
            )
        ],
    )
    n = root / "network" / "year=2026" / "month=08"
    fetched = {"25": "2026-08-25T00:20:00+00:00", "26": "2026-08-26T00:20:00+00:00"}
    for day, at in fetched.items():
        write_parquet(
            n / f"day={day}" / "routes.parquet",
            [{"route_id": "1", "route_ref": "C1", "route_name": "Trolebus C1", "fetched_at": at}],
        )
        write_parquet(
            n / f"day={day}" / "stops.parquet",
            [
                {
                    "stop_id": "s1",
                    "stop_name": "A",
                    "latitude": -0.2,
                    "longitude": -78.5,
                    "fetched_at": at,
                }
            ],
        )
    write_parquet(
        n / "day=25" / "route_stops.parquet", [route_stop_row("1", "s1", 0, fetched["25"])]
    )
    write_parquet(
        n / "day=26" / "route_stops.parquet",
        [
            route_stop_row(
                "1", "s1", 0, fetched["26"],
                sequence_source="geometry",
                member_index=0,
                distance_along_m=0.0,
                snap_distance_m=12.5,
            )
        ],
    )
    w = root / "weather" / "year=2026" / "month=08"
    write_parquet(
        w / "day=25" / "weather.parquet",
        [
            {
                "record_type": "current",
                "fetched_at": "2026-08-25T05:00:00+00:00",
                "timestamp": "2026-08-25T05:00:00+00:00",
                "temp_c": 10.0,
            }
        ],
    )
    return str(root)


# ── ensure_columns ───────────────────────────────────────────────────────────


def test_ensure_columns_adds_missing_column_with_typed_default(spark):
    df = spark.createDataFrame([("1",)], ["route_id"])
    out = ensure_columns(
        df, {"stops_excluded": (0, "long"), "snap_distance_m": (None, "double")}
    )
    assert dict(out.dtypes)["stops_excluded"] == "bigint"
    assert dict(out.dtypes)["snap_distance_m"] == "double"
    assert out.collect()[0].asDict() == {
        "route_id": "1",
        "stops_excluded": 0,
        "snap_distance_m": None,
    }


def test_ensure_columns_fills_nulls_left_by_merge_schema(spark):
    df = spark.createDataFrame([("1", None), ("2", "geometry")], ["route_id", "sequence_source"])
    out = ensure_columns(df, {"sequence_source": (SEQUENCE_SOURCE_DEFAULT, "string")})
    assert {r.route_id: r.sequence_source for r in out.collect()} == {
        "1": SEQUENCE_SOURCE_DEFAULT,
        "2": "geometry",
    }


def test_ensure_columns_leaves_existing_values_alone_when_default_is_none(spark):
    df = spark.createDataFrame([("1", None), ("2", 4.0)], ["route_id", "snap_distance_m"])
    out = ensure_columns(df, {"snap_distance_m": (None, "double")})
    assert {r.route_id: r.snap_distance_m for r in out.collect()} == {"1": None, "2": 4.0}


# ── read_traffic ─────────────────────────────────────────────────────────────


def test_read_traffic_unifies_old_and_new_shapes(spark, raw_root):
    df = read_traffic(spark, raw_root)
    rows = {r.day: r for r in df.collect()}
    assert set(rows) == {25, 26}
    # The old file gets the historical defaults; the new file keeps its values.
    assert (rows[25].sequence_source, rows[25].stops_excluded) == (SEQUENCE_SOURCE_DEFAULT, 0)
    assert (rows[26].sequence_source, rows[26].stops_excluded) == ("geometry", 3)


def test_read_traffic_partition_columns_are_integers_from_directories(spark, raw_root):
    df = read_traffic(spark, raw_root)
    assert {c: t for c, t in df.dtypes if c in ("year", "month", "day", "hour")} == {
        "year": "int", "month": "int", "day": "int", "hour": "int"
    }
    assert df.select("year", "month", "hour").distinct().collect() == [(2026, 8, 12)]


def test_read_traffic_rejects_unknown_layout(spark, raw_root):
    with pytest.raises(ValueError, match="layout"):
        read_traffic(spark, raw_root, layout="bronze")


def test_read_traffic_samples_layout_derives_what_the_series_lacks(spark):
    df = read_traffic(spark, str(DEFAULT_SAMPLES_ROOT), layout=LAYOUT_SAMPLES)
    assert df.count() == 2352
    derived = ("local_weekday", "sequence_source", "stops_excluded", "year", "month", "day", "hour")
    assert set(derived) <= set(df.columns)
    # Derived local weekday must agree with the loader's own local_hour:
    # 2026-08-26 is a Wednesday (weekday 2), but a sample at 03:00 UTC that
    # day is still Tuesday evening (weekday 1) in Quito.
    got = {
        r.local_hour: r.local_weekday
        for r in df.filter("year=2026 and month=8 and day=26 and hour in (3, 12)").collect()
    }
    assert got == {22: 1, 7: 2}


# ── read_weather ─────────────────────────────────────────────────────────────


def test_read_weather_raw_layout_keeps_partition_columns(spark, raw_root):
    df = read_weather(spark, raw_root)
    assert df.select("year", "month", "day").collect() == [(2026, 8, 25)]


def test_read_weather_samples_layout(spark):
    df = read_weather(spark, str(DEFAULT_SAMPLES_ROOT), layout=LAYOUT_SAMPLES)
    assert df.count() == 574
    assert {r.record_type: r["count"] for r in df.groupBy("record_type").count().collect()} == {
        "current": 14,
        "forecast": 560,
    }


# ── read_network_table ───────────────────────────────────────────────────────


def test_read_network_table_selects_by_file_name_not_directory(spark, raw_root):
    routes = read_network_table(spark, raw_root, "routes")
    stops = read_network_table(spark, raw_root, "stops")
    assert "stop_name" not in routes.columns and "route_name" not in stops.columns
    assert routes.count() == 2 and stops.count() == 2


def test_read_network_table_replaces_partitions_with_snapshot_date(spark, raw_root):
    routes = read_network_table(spark, raw_root, "routes")
    assert not {"year", "month", "day"} & set(routes.columns)
    assert dict(routes.dtypes)["snapshot_date"] == "date"
    assert sorted(str(r.snapshot_date) for r in routes.collect()) == ["2026-08-25", "2026-08-26"]


def test_read_route_stops_unifies_recovery_columns(spark, raw_root):
    rs = read_network_table(spark, raw_root, "route_stops")
    rows = {str(r.snapshot_date): r for r in rs.collect()}
    old, new = rows["2026-08-25"], rows["2026-08-26"]
    assert (old.sequence_source, old.member_index, old.snap_distance_m) == (
        SEQUENCE_SOURCE_DEFAULT,
        None,
        None,
    )
    assert (new.sequence_source, new.member_index, new.snap_distance_m) == ("geometry", 0, 12.5)


def test_read_network_table_rejects_unknown_table(spark, raw_root):
    with pytest.raises(ValueError, match="name"):
        read_network_table(spark, raw_root, "trips")


def test_read_network_table_samples_layout_dates_from_fetched_at(spark):
    rs = read_network_table(spark, DEFAULT_SAMPLES_ROOT, "route_stops", layout=LAYOUT_SAMPLES)
    assert rs.count() == 960
    assert rs.select("snapshot_date").distinct().count() == 1
    assert rs.filter(F.col("sequence_source") == SEQUENCE_SOURCE_DEFAULT).count() == 960


# ── write_table / read_table ─────────────────────────────────────────────────


def test_write_table_round_trips_and_is_a_full_refresh(spark, tmp_path):
    gold = str(tmp_path / "gold")
    first = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["k", "v"])
    second = spark.createDataFrame([(9, "z")], ["k", "v"])
    write_table(first, gold, "dim_x")
    write_table(second, gold, "dim_x")
    assert read_table(spark, gold, "dim_x").collect() == [(9, "z")]


def test_write_table_unpartitioned_is_a_single_file(spark, tmp_path):
    gold = tmp_path / "gold"
    write_table(spark.range(1000).repartition(5), str(gold), "dim_x")
    assert len(list((gold / "dim_x").glob("*.parquet"))) == 1


def test_write_table_partitioned_drops_partitions_missing_from_the_rebuild(spark, tmp_path):
    gold = tmp_path / "gold"
    first = spark.createDataFrame([("2026-08-25", 1), ("2026-08-26", 2)], ["local_date", "v"])
    second = spark.createDataFrame([("2026-08-26", 3)], ["local_date", "v"])
    write_table(first, str(gold), "fct_x", partition_by=("local_date",))
    assert sorted(p.name for p in (gold / "fct_x").glob("local_date=*")) == [
        "local_date=2026-08-25",
        "local_date=2026-08-26",
    ]
    write_table(second, str(gold), "fct_x", partition_by=("local_date",))
    assert [p.name for p in (gold / "fct_x").glob("local_date=*")] == ["local_date=2026-08-26"]
    # Spark infers the partition value back from the directory name, as a date.
    assert [(r.v, str(r.local_date)) for r in read_table(spark, str(gold), "fct_x").collect()] == [
        (3, "2026-08-26")
    ]


def test_join_path_handles_local_and_s3_roots():
    assert join_path("/data/raw/", "traffic") == "/data/raw/traffic"
    assert join_path("s3://bucket/raw", "network", "year=*") == "s3://bucket/raw/network/year=*"
