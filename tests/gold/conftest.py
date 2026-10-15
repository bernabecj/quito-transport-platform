"""Shared fixtures for the gold-layer tests.

One SparkSession per test session: starting a JVM costs a few seconds, and
Spark keeps one per process anyway. It goes through processing.gold.session
so the tests run under the same time zone and overwrite settings as the build.
"""

import pandas as pd
import pytest

from processing.gold.session import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark(app_name="quito-gold-tests", master="local[2]")
    yield session
    session.stop()


def write_parquet(path, rows: list[dict]) -> None:
    """Write a list of row dicts as one Parquet file, creating the directory.

    pandas + pyarrow, not Spark: the loaders write their raw files with
    pyarrow (ingestion/storage.py), so this is the shape the readers really
    meet, and it sidesteps Spark's one-directory-per-write output layout.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def frame(spark, rows: list[dict], schema: str):
    """A DataFrame from row dicts with an explicit DDL schema, so a None in
    the first row cannot make Spark guess a column's type."""
    names = [part.strip().split()[0] for part in schema.split(",")]
    return spark.createDataFrame([tuple(r.get(n) for n in names) for r in rows], schema)


def rows_by(df, *key):
    """``{key_tuple_or_value: Row}`` for quick lookups in assertions."""
    out = {}
    for r in df.collect():
        k = tuple(r[c] for c in key)
        out[k[0] if len(k) == 1 else k] = r
    return out
