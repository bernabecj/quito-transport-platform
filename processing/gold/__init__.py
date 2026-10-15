"""
Gold layer: raw zone -> star schema, in plain PySpark.

    config.py      roots, layouts, table names, schema-evolution defaults
    session.py     the one SparkSession configuration (UTC, overwrite mode)
    io.py          readers that normalise the raw zone; the gold writer

Later steps add dimensions.py, weather.py, facts.py, marts.py, build.py and
verify.py. See docs/GOLD_LAYER.md for the model and docs/ANALYSIS_PLAN.md for
the objectives each table answers.
"""
