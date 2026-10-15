"""
Time columns shared by the facts, the dimensions and the marts.

All raw timestamps are UTC (notebooks/01_exploring.ipynb, section 5), and
the research questions — rush hour, weekday effects — are local-time
questions. Every table therefore carries both, derived in exactly one place
so no two tables can disagree about what "local" means.

    utc_ts       timestamp   the sample's hour, from the partition columns
    local_ts     timestamp   utc_ts shifted by UTC_OFFSET_HOURS (-5, no DST)
    local_date   date        the Quito calendar day — what daily aggregates group by
    date_key     int         yyyymmdd of local_date, the foreign key to dim_date

utc_ts is built from the partition columns rather than parsed from
`sampled_at` on purpose: the partition is the hour the sample *represents*
(a run at 12:00:03 belongs to hour 12), and it is what the notebook's
findings were derived from. `sampled_at` stays on the fact row as the exact
instant. The session time zone is pinned to UTC (session.py), so
make_timestamp here is UTC on every machine.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from processing.gold.config import UTC_OFFSET_HOURS


def date_key(date_col) -> Column:
    """yyyymmdd as an integer: deterministic, sorts chronologically, and the
    conventional surrogate for a date dimension in a warehouse."""
    return (F.year(date_col) * 10000 + F.month(date_col) * 100 + F.dayofmonth(date_col)).cast(
        "int"
    )


def with_time_columns(df: DataFrame) -> DataFrame:
    """Add utc_ts, local_ts, local_date and date_key to a frame that carries
    integer year/month/day/hour columns (what io.read_traffic guarantees)."""
    utc_ts = F.expr("make_timestamp(year, month, day, hour, 0, 0)")
    local_ts = utc_ts + F.expr(f"INTERVAL {UTC_OFFSET_HOURS} HOURS")
    return (
        df.withColumn("utc_ts", utc_ts)
        .withColumn("local_ts", local_ts)
        .withColumn("local_date", F.to_date(local_ts))
        .withColumn("date_key", date_key(F.to_date(local_ts)))
    )
