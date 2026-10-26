# Databricks notebook source
# MAGIC %md
# MAGIC # framework/05_gold
# MAGIC
# MAGIC Defines `publish_gold(flow_id)`.
# MAGIC
# MAGIC Builds the dissemination layer for a flow from its validated silver table and its
# MAGIC DSD. Nothing here is flow-specific: dimension order, attribute order and the
# MAGIC SDMX-CSV header are all derived from `dsd_components` and `flow_config`.
# MAGIC
# MAGIC Four objects per flow:
# MAGIC
# MAGIC | Object | Grain | Contents |
# MAGIC | --- | --- | --- |
# MAGIC | `<entity>_timeseries` | one row per observation | dimensions, period, value, status, all attributes |
# MAGIC | `<entity>_series` | one row per series key | the **latest** observation of each series |
# MAGIC | `<entity>_sdmx_csv` | view over `_timeseries` | SDMX-CSV shape: `STRUCTURE`, `STRUCTURE_ID`, `ACTION`, then DSD column order |
# MAGIC | `dq_overview` | one row per flow | latest validation run, rolled up from `dq_summary` |
# MAGIC
# MAGIC `STRUCTURE_ID` is `agency:flow_id(version)` straight from `flow_config`, e.g.
# MAGIC `BIS:WS_CBPOL(1.0)`, which is what the BIS API itself emits.
# MAGIC
# MAGIC Safe by default: `dry_run=True` writes nothing and returns the DataFrames.

# COMMAND ----------

# DBTITLE 1,Constants and imports
from pyspark.sql import functions as F, Window

CATALOG = "workspace"
BRONZE = "workspace.sdmx_bronze"
SILVER = "workspace.sdmx_silver"
GOLD = "workspace.sdmx_gold"

print(f"Constants loaded. Gold schema: {GOLD}")

# COMMAND ----------

# DBTITLE 1,publish_gold
def publish_gold(flow_id, dry_run=True):
    """Build the gold dissemination objects for one flow from silver + the DSD."""
    cfg = spark.sql(f"""SELECT agency, version, dsd_id FROM {BRONZE}.flow_config
                        WHERE flow_id = '{flow_id}' AND enabled = true""").collect()
    if not cfg:
        raise ValueError(f"Flow '{flow_id}' not found or not enabled in flow_config")
    agency, version, dsd_id = cfg[0]["agency"], cfg[0]["version"], cfg[0]["dsd_id"]

    entity = flow_id.replace("WS_", "").lower()
    silver_table = f"{SILVER}.{entity}"
    print(f"=== publish_gold('{flow_id}') [{'DRY RUN' if dry_run else 'WRITE'}] "
          f"source={silver_table} ===")

    comps = spark.sql(f"""
        SELECT component_id, component_type, attachment_level, ordinal
        FROM {SILVER}.dsd_components
        WHERE dsd_id = '{dsd_id}' AND dsd_version = '{version}'
        ORDER BY ordinal""").collect()
    dim_cols = [r["component_id"] for r in comps if r["component_type"] == "dimension"]
    time_col = [r["component_id"] for r in comps if r["component_type"] == "time_dimension"][0]
    measure_col = [r["component_id"] for r in comps if r["component_type"] == "measure"][0]

    # SDMX orders attributes by what they attach to, then by DSD position. This is what
    # makes a group-level attribute like BIS_TOTAL_CREDIT.DECIMALS sort after the
    # series-level ones even though its ordinal is lower, and it is why the BIS
    # sdmx-csv output differs from its plain csv output.
    LEVELS = {"dataset": 0, "series": 1, "group": 2, "observation": 3}
    attr_cols = [r["component_id"] for r in sorted(
        (r for r in comps if r["component_type"] == "attribute"),
        key=lambda r: (LEVELS.get(r["attachment_level"], 4), int(r["ordinal"])))]

    silver = spark.table(silver_table)
    # OBS_STATUS sits next to the value for dissemination; SDMX-CSV keeps DSD order.
    status = "OBS_STATUS" if "OBS_STATUS" in attr_cols else None
    rest = [a for a in attr_cols if a != status]

    ts_cols = dim_cols + [time_col, "period_start_date", measure_col]
    ts_cols += ([status] if status else []) + rest
    timeseries = silver.select(*ts_cols)

    # One row per series key, carrying its latest period.
    w = Window.partitionBy(*dim_cols).orderBy(F.col("period_start_date").desc(),
                                              F.col(time_col).desc())
    series = (timeseries.withColumn("_rnk", F.row_number().over(w))
              .filter("_rnk = 1").drop("_rnk")
              .select(*dim_cols, time_col, "period_start_date", measure_col,
                      *([status] if status else [])))

    structure_id = f"{agency}:{flow_id}({version})"
    csv_cols = ", ".join(dim_cols + [time_col, measure_col] + attr_cols)
    view_sql = (f"CREATE OR REPLACE VIEW {GOLD}.{entity}_sdmx_csv AS\n"
                f"SELECT 'dataflow' AS STRUCTURE, '{structure_id}' AS STRUCTURE_ID, "
                f"'I' AS ACTION, {csv_cols}\n"
                f"FROM {GOLD}.{entity}_timeseries")

    n_ts, n_series = timeseries.count(), series.count()
    print(f"  timeseries {n_ts} rows ({len(ts_cols)} cols) | series {n_series} rows "
          f"| STRUCTURE_ID {structure_id}")

    if not dry_run:
        (timeseries.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(f"{GOLD}.{entity}_timeseries"))
        (series.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(f"{GOLD}.{entity}_series"))
        spark.sql(view_sql)
        print(f"  WROTE {GOLD}.{entity}_timeseries, {GOLD}.{entity}_series, "
              f"{GOLD}.{entity}_sdmx_csv")
    else:
        print("  DRY RUN: nothing written.")

    return {"timeseries": timeseries, "series": series, "view_sql": view_sql,
            "rows_timeseries": n_ts, "rows_series": n_series,
            "structure_id": structure_id}

print("publish_gold(flow_id) defined.")

# COMMAND ----------

# DBTITLE 1,refresh_dq_overview
def refresh_dq_overview(dry_run=True):
    """Roll the latest validation run for every flow into gold.dq_overview.

    Grouped by run_id, not run_timestamp: the seven rule rows of one run each carry their
    own timestamp, so grouping on the timestamp collapses a run to a single rule and loses
    its warnings. run_id is the only stable identifier of a run.

    pass_rate is the MIN across rules - the worst rule governs the flow's headline
    figure. Inferred from the recovered table, which cannot distinguish MIN from AVG
    because every rule currently passes.
    """
    overview = spark.sql(f"""
        WITH runs AS (
            SELECT flow_id, run_id, MAX(run_timestamp) AS ts,
                   ROW_NUMBER() OVER (PARTITION BY flow_id
                                      ORDER BY MAX(run_timestamp) DESC) AS rn
            FROM {SILVER}.dq_summary
            GROUP BY flow_id, run_id
        )
        SELECT s.flow_id,
               s.run_id                        AS last_run_id,
               MAX(s.run_timestamp)            AS last_run_timestamp,
               ANY_VALUE(s.validator_version)  AS validator_version,
               MAX(s.rows_checked)             AS rows_checked,
               SUM(s.rows_failed)              AS total_errors,
               SUM(s.warning_count)            AS total_warnings,
               MIN(s.pass_rate)                AS pass_rate
        FROM {SILVER}.dq_summary s
        JOIN runs r ON s.run_id = r.run_id AND r.rn = 1
        GROUP BY s.flow_id, s.run_id
        ORDER BY s.flow_id""")

    n = overview.count()
    print(f"=== refresh_dq_overview [{'DRY RUN' if dry_run else 'WRITE'}] {n} flows ===")
    if not dry_run:
        (overview.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(f"{GOLD}.dq_overview"))
        print(f"  WROTE {GOLD}.dq_overview")
    else:
        print("  DRY RUN: nothing written.")
    return overview

print("refresh_dq_overview() defined.")
