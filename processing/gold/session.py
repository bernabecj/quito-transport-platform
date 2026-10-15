"""
Spark session for the gold build.

One place that decides how Spark is configured, so a laptop, this repo's
tests, the sandbox and AWS Glue all run the transformations under the same
settings. The transformations themselves never touch SparkSession.builder.

Three settings are not cosmetic:

  spark.sql.session.timeZone = UTC
      Every raw timestamp is UTC (verified in notebooks/01_exploring.ipynb,
      section 5). Spark parses and formats timestamps in the *session* time
      zone, which defaults to the machine's. Pinning it to UTC makes
      to_timestamp("2026-08-25T12:00:00+00:00") and make_timestamp(year,
      month, day, hour, 0, 0) mean the same instant on a laptop set to
      America/Guayaquil, in a container set to UTC, and on Glue — and makes
      the local-time derivation in the fact table an explicit -5 h, not an
      accident of where the job ran.

  spark.sql.sources.partitionOverwriteMode = dynamic
      With INSERT OVERWRITE on a partitioned table, "static" mode deletes
      every partition before writing; "dynamic" only replaces the partitions
      present in the new data. The build currently rewrites everything anyway,
      but this is the one switch needed to move the fact table to per-day
      incremental writes later (see docs/ANALYSIS_PLAN.md, implementation
      objective 2), so it is set now rather than discovered then.

  spark.sql.shuffle.partitions
      Defaults to 200, which for a 9 MB raw zone means 200 near-empty tasks
      on every join and groupBy. A small number keeps local runs and tests
      fast; Glue ignores it in favour of its own sizing when a session already
      exists.
"""

import os

from pyspark.sql import SparkSession

# Override the Spark master from the environment, e.g. GOLD_SPARK_MASTER=local[2]
# in CI to keep memory down. Unset means every local core.
SPARK_MASTER_VAR = "GOLD_SPARK_MASTER"
DEFAULT_MASTER = "local[*]"

LOCAL_SHUFFLE_PARTITIONS = 8


def get_spark(app_name: str = "quito-gold", master: str | None = None) -> SparkSession:
    """The session the build runs on.

    An already-running session — on Glue, on Databricks, in a notebook, or in
    the test fixture — is reused untouched, so the master and the shuffle
    setting only apply when this call is what starts Spark. The time zone
    and overwrite mode are SQL-level settings and are applied either way.
    """
    spark = SparkSession.getActiveSession()
    if spark is None:
        # Nothing running: this call starts Spark, so the master and the
        # local tuning apply. On Databricks or Glue a session already exists
        # (and may be a Spark Connect session, which has no master to set),
        # so it is taken as-is and only the SQL settings below are applied.
        master = master or os.environ.get(SPARK_MASTER_VAR) or DEFAULT_MASTER
        spark = (
            SparkSession.builder.appName(app_name)
            .master(master)
            .config("spark.sql.shuffle.partitions", str(LOCAL_SHUFFLE_PARTITIONS))
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        )
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.sparkContext.setLogLevel("WARN")
    return spark
