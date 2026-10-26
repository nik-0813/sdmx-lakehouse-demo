# Databricks notebook source
# MAGIC %md
# MAGIC # framework/04_completeness
# MAGIC
# MAGIC Defines `completeness_check(flow_id)`.
# MAGIC
# MAGIC **Validation catches wrong data; completeness catches missing data.** `validate_flow`
# MAGIC can only inspect rows that arrived, so it is structurally incapable of seeing a gap.
# MAGIC This stage closes that hole.
# MAGIC
# MAGIC Driven by `sdmx_bronze.expected_series` (`flow_id`, `series_key` as JSON,
# MAGIC `first_period`, `frequency`). For each expected series it generates every period from
# MAGIC `first_period` to the latest period present anywhere in the flow, subtracts the periods
# MAGIC actually received, and raises a **warning** finding per gap.
# MAGIC
# MAGIC Design decisions, all deliberate:
# MAGIC
# MAGIC - **The horizon is flow-wide**, not per series. A series that stops reporting entirely
# MAGIC   still shows gaps up to the point other series reached - otherwise silence would look
# MAGIC   like completeness.
# MAGIC - **Only series listed in `expected_series` are checked.** An unexpected series (e.g.
# MAGIC   the unmapped `Atlantis` row in the collection fixture) is a *mapping* finding, not a
# MAGIC   completeness one.
# MAGIC - **Findings are warnings, never errors.** A gap may be legitimate - a series can start
# MAGIC   late or be discontinued. It is a question for the reporter, not a rejection.
# MAGIC - **Frequencies come from `freq_time_patterns`.** A frequency with no
# MAGIC   `date_parse_expr` (currently `W`) cannot be enumerated and raises a warning rather
# MAGIC   than silently reporting zero gaps.
# MAGIC
# MAGIC This is the one part of the pipeline that was rewritten rather than recovered: the
# MAGIC original was lost with an unsaved notebook. It is validated against the three findings
# MAGIC the original produced.

# COMMAND ----------

# DBTITLE 1,Constants and imports
import json
import uuid

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

BRONZE = "workspace.sdmx_bronze"
SILVER = "workspace.sdmx_silver"

COMPLETENESS_SCHEMA = StructType([StructField(n, StringType(), True) for n in [
    "run_id", "flow_id", "series_key", "missing_period", "frequency", "message"]])

# How to walk and render each frequency. Which frequencies are *known* comes from
# freq_time_patterns; this is the step and display format that metadata does not carry.
FREQ_STEP = {
    "A": ("INTERVAL 1 YEAR", "date_format({d}, 'yyyy')"),
    "H": ("INTERVAL 6 MONTHS", "concat(date_format({d}, 'yyyy'), '-S', "
                               "cast(ceil(month({d}) / 6) as int))"),
    "Q": ("INTERVAL 3 MONTHS", "concat(date_format({d}, 'yyyy'), '-Q', quarter({d}))"),
    "M": ("INTERVAL 1 MONTH", "date_format({d}, 'yyyy-MM')"),
    "D": ("INTERVAL 1 DAY", "date_format({d}, 'yyyy-MM-dd')"),
}

print(f"Constants loaded. Frequencies enumerable: {sorted(FREQ_STEP)}")

# COMMAND ----------

# DBTITLE 1,completeness_check
def completeness_check(flow_id, bronze_table=None, dry_run=True):
    """Report expected series-periods that never arrived, as warning findings."""
    run_id = uuid.uuid4().hex

    cfg = spark.sql(f"""SELECT dsd_id, version FROM {BRONZE}.flow_config
                        WHERE flow_id = '{flow_id}' AND enabled = true""").collect()
    if not cfg:
        raise ValueError(f"Flow '{flow_id}' not found or not enabled in flow_config")
    dsd_id, dsd_version = cfg[0]["dsd_id"], cfg[0]["version"]

    entity = flow_id.replace("WS_", "").lower()
    bronze_table = bronze_table or f"{BRONZE}.{entity}_raw"
    print(f"=== completeness_check('{flow_id}') [{'DRY RUN' if dry_run else 'WRITE'}] "
          f"source={bronze_table} ===")

    dim_cols = [r["component_id"] for r in spark.sql(f"""
        SELECT component_id FROM {SILVER}.dsd_components
        WHERE dsd_id = '{dsd_id}' AND dsd_version = '{dsd_version}'
          AND component_type = 'dimension'
        ORDER BY ordinal""").collect()]

    parse_expr = {r["freq_code"]: r["date_parse_expr"]
                  for r in spark.table(f"{SILVER}.freq_time_patterns").collect()}

    expected = spark.sql(f"""SELECT series_key, first_period, frequency
                             FROM {BRONZE}.expected_series
                             WHERE flow_id = '{flow_id}'""").collect()
    if not expected:
        print(f"  No expected_series rows for '{flow_id}' - nothing to check.")
        return spark.createDataFrame([], COMPLETENESS_SCHEMA)

    actual = spark.table(bronze_table)
    horizon = actual.agg(F.max("TIME_PERIOD").alias("p")).collect()[0]["p"]
    print(f"  {len(expected)} expected series | horizon (latest period in flow): {horizon}")

    rows = []
    for spec in expected:
        freq = spec["frequency"]
        key = json.loads(spec["series_key"])
        key_str = "|".join(f"{d}={key[d]}" for d in dim_cols if d in key)

        if not parse_expr.get(freq) or freq not in FREQ_STEP:
            rows.append((run_id, flow_id, key_str, None, freq,
                         f"Cannot enumerate periods for FREQ={freq}: no date parse "
                         f"expression defined, so gaps cannot be detected"))
            continue

        step, fmt = FREQ_STEP[freq]
        bounds = (spark.createDataFrame([(spec["first_period"],), (horizon,)], "TIME_PERIOD STRING")
                  .select(F.expr(parse_expr[freq]).alias("d")).collect())
        start, end = bounds[0]["d"], bounds[1]["d"]
        if start is None or end is None or start > end:
            continue

        wanted = [r["p"] for r in spark.sql(
            f"SELECT {fmt.format(d='d')} AS p FROM "
            f"(SELECT explode(sequence(DATE'{start}', DATE'{end}', {step})) AS d)").collect()]

        received = actual
        for dim, val in key.items():
            received = received.filter(F.col(dim) == val)
        got = {r["TIME_PERIOD"] for r in received.select("TIME_PERIOD").distinct().collect()}

        for period in wanted:
            if period not in got:
                rows.append((run_id, flow_id, key_str, period, freq,
                             f"Expected {key_str} for {period} \u2014 not received (gap)"))

    findings = spark.createDataFrame(rows, COMPLETENESS_SCHEMA)
    n = findings.count()
    print(f"  {n} completeness finding(s)")
    if n:
        display(findings.select("series_key", "missing_period", "message"))

    if not dry_run:
        (findings.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true")
         .saveAsTable(f"{BRONZE}.collection_completeness_findings"))
        print(f"  WROTE {BRONZE}.collection_completeness_findings")
    else:
        print("  DRY RUN: nothing written.")

    return findings

print("completeness_check(flow_id) defined.")
