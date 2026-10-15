"""End-to-end: the build CLI on the committed samples, then verify.

Slow by the standards of this suite (a full Spark build, about a minute),
so everything that needs the built output shares one module-scoped build.
"""

import pandas as pd
import pytest
from pyspark.sql import functions as F

from processing.gold import build, verify
from processing.gold.config import (
    DEFAULT_SAMPLES_ROOT,
    GOLD_TABLES,
    LAYOUT_SAMPLES,
    TABLE_AGG_CORRIDOR_HOURLY_PROFILE,
    TABLE_AGG_CORRIDOR_RELIABILITY,
    TABLE_DIM_DATE,
    TABLE_DIM_ROUTE,
    TABLE_DIM_STOP,
    TABLE_DIM_WEATHER_CONDITION,
    TABLE_FCT_CORRIDOR_TRAVEL_TIME,
    TABLE_FCT_NETWORK_SNAPSHOT,
    TABLE_FCT_ROUTE_STOP,
    TABLE_FCT_WEATHER,
)
from processing.gold.io import read_table, write_table

# What the committed samples must produce. Row counts are pinned: a change
# here is a change in the samples or in the model, and either deserves a
# deliberate edit of this table.
EXPECTED_ROWS = {
    TABLE_DIM_DATE: 18,
    TABLE_DIM_ROUTE: 515,
    TABLE_DIM_STOP: 485,
    TABLE_DIM_WEATHER_CONDITION: 6,
    TABLE_FCT_WEATHER: 144,
    TABLE_FCT_CORRIDOR_TRAVEL_TIME: 2352,
    TABLE_FCT_ROUTE_STOP: 960,
    TABLE_FCT_NETWORK_SNAPSHOT: 1,
    TABLE_AGG_CORRIDOR_HOURLY_PROFILE: 14 * 24 * 3,
    TABLE_AGG_CORRIDOR_RELIABILITY: 14,
}


@pytest.fixture(scope="module")
def gold_root(tmp_path_factory, spark):
    root = tmp_path_factory.mktemp("gold")
    counts = build.build_all(spark, str(DEFAULT_SAMPLES_ROOT), str(root), LAYOUT_SAMPLES)
    return root, counts


def test_build_writes_every_table_with_the_expected_rows(gold_root):
    root, counts = gold_root
    assert list(counts) == list(GOLD_TABLES)
    assert counts == EXPECTED_ROWS
    assert sorted(p.name for p in root.iterdir()) == sorted(GOLD_TABLES)


def test_verify_passes_on_a_fresh_build(spark, gold_root):
    root, _ = gold_root
    checks = verify.run_checks(spark, str(root))
    failed = [c for c in checks if not c.passed]
    assert not failed, verify.report(checks)
    assert {c.kind for c in checks} == {"exists", "unique", "orphans", "grain"}


def test_gold_is_readable_without_spark_and_reproduces_the_rush_hour(gold_root):
    root, _ = gold_root
    fct = pd.read_parquet(root / TABLE_FCT_CORRIDOR_TRAVEL_TIME)
    assert len(fct) == 2352 and fct.is_complete_day.all()
    curve = fct.groupby("local_hour").mean_speed_kmh.mean().round(1)
    # The exploration's finding: ~34 km/h overnight, ~21 km/h through the day.
    assert curve[1] == 34.0 and curve[7] == 21.4 and curve[12] == 21.3


def test_build_is_idempotent(spark, gold_root):
    root, counts = gold_root
    before = {name: read_table(spark, str(root), name).toPandas() for name in (TABLE_DIM_ROUTE, TABLE_AGG_CORRIDOR_RELIABILITY)}
    again = build.build_all(spark, str(DEFAULT_SAMPLES_ROOT), str(root), LAYOUT_SAMPLES)
    assert again == counts
    for name, frame in before.items():
        after = read_table(spark, str(root), name).toPandas()
        key = "route_id"
        pd.testing.assert_frame_equal(
            frame.sort_values(key).reset_index(drop=True),
            after.sort_values(key).reset_index(drop=True),
        )


def test_verify_fails_on_an_orphan_and_reports_it(spark, gold_root, tmp_path):
    root, _ = gold_root
    broken = tmp_path / "broken"
    for name in GOLD_TABLES:
        write_table(read_table(spark, str(root), name), str(broken), name)
    # A reliability row for a corridor that is not in dim_route. Read from
    # the pristine build, not from `broken`: Spark clears an overwrite target
    # before a lazy read of the same path has finished.
    rel = read_table(spark, str(root), TABLE_AGG_CORRIDOR_RELIABILITY)
    orphan = rel.limit(1).withColumn("route_id", F.lit("999999"))
    write_table(rel.unionByName(orphan), str(broken), TABLE_AGG_CORRIDOR_RELIABILITY)

    assert verify.main(["--gold-root", str(broken)]) == 1
    failed = [c for c in verify.run_checks(spark, str(broken)) if not c.passed]
    assert [(c.kind, c.table) for c in failed] == [("orphans", TABLE_AGG_CORRIDOR_RELIABILITY)]


def test_cli_parses_defaults_for_the_samples_layout():
    args = build.parse_args(["--layout", "samples", "--gold-root", "/tmp/x"])
    assert args.raw_root == str(DEFAULT_SAMPLES_ROOT) and args.gold_root == "/tmp/x"
    assert build.parse_args([]).layout == "raw"
