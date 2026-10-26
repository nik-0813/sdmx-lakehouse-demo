# Databricks notebook source
# ===========================================================================
# validate_flow v2.1
#
# Metadata-driven SDMX validation. Rules come from dsd_components, codes,
# rule_config, freq_time_patterns and obs_status_missing.
#
# SAFE BY DEFAULT: dry_run=True writes NOTHING and returns DataFrames.
# Writes happen only with dry_run=False, and only for the flow's own bronze
# table (never for a test source_table).
#
# Known assumptions (documented, not hidden):
#   * the frequency dimension is called FREQ and the status attribute OBS_STATUS
#   * group-level attributes are checked per row like series-level ones;
#     dataset-level attributes are not checked per row
#   * date_parse_expr values in freq_time_patterns must use try_* functions
#     (e.g. try_to_date) and may reference the time column by name or {col}
#   * a STRUCT failure (missing column) aborts the run
#
# Independent personal project, not affiliated with the BIS.
# ===========================================================================

import uuid
from datetime import datetime

from pyspark.sql import functions as F, Window, Column
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType
)

VALIDATOR_VERSION = "v2.1"
SILVER = "workspace.sdmx_silver"
BRONZE = "workspace.sdmx_bronze"

RULES = ["STRUCT", "DIM_CODE", "ATTR_MANDATORY", "ATTR_CODE",
         "TIME_FORMAT", "OBS_VALUE", "DUPLICATE"]

FINDING_SCHEMA = StructType([StructField(n, t, True) for n, t in [
    ("row_id", LongType()),
    ("row_key", StringType()),
    ("run_id", StringType()),
    ("rule_id", StringType()),
    ("severity", StringType()),
    ("component", StringType()),
    ("offending_value", StringType()),
    ("message", StringType()),
    ("batch_id", StringType()),
]])

SUMMARY_SCHEMA = StructType([StructField(n, t, True) for n, t in [
    ("run_id", StringType()),
    ("run_timestamp", TimestampType()),
    ("flow_id", StringType()),
    ("rule_id", StringType()),
    ("rows_checked", LongType()),
    ("rows_failed", LongType()),
    ("pass_rate", DoubleType()),
    ("validator_version", StringType()),
    ("warning_count", LongType()),
]])


def _lc(x):
    return x if isinstance(x, Column) else F.lit(x)


def _finding(df, run_id, rule_id, severity, component, value, message):
    """Row-level findings. df must have row_id, row_key, batch_id."""
    return df.select(
        F.col("row_id").cast("long").alias("row_id"),
        F.col("row_key").alias("row_key"),
        F.lit(run_id).alias("run_id"),
        F.lit(rule_id).alias("rule_id"),
        F.lit(severity).alias("severity"),
        _lc(component).cast("string").alias("component"),
        _lc(value).cast("string").alias("offending_value"),
        _lc(message).cast("string").alias("message"),
        F.col("batch_id").alias("batch_id"))


def _batch_finding(run_id, rule_id, severity, component, message):
    """One finding for the whole batch (no row_id)."""
    return spark.createDataFrame(
        [(None, None, run_id, rule_id, severity, component, None, message, None)],
        FINDING_SCHEMA)


def _codes(cl):
    a, i, v = cl
    return (spark.table(f"{SILVER}.codes")
            .filter((F.col("agency") == a) & (F.col("codelist_id") == i) & (F.col("version") == v))
            .select(F.col("code_id").alias("_code")).distinct())


def validate_flow(flow_id, dry_run=True, source_table=None):
    if not dry_run and source_table:
        raise ValueError("Writes are only allowed for the flow's own bronze "
                         "table: use source_table=None with dry_run=False.")

    run_id = uuid.uuid4().hex
    run_ts = datetime.now()

    # --- 1. config ---------------------------------------------------------
    cfg = spark.sql(f"""SELECT agency, version, dsd_id FROM {BRONZE}.flow_config
                        WHERE flow_id = '{flow_id}' AND enabled = true""").collect()
    if not cfg:
        raise ValueError(f"Flow '{flow_id}' not found or not enabled in flow_config")
    dsd_id, dsd_version = cfg[0]["dsd_id"], cfg[0]["version"]

    entity = flow_id.replace("WS_", "").lower()
    bronze_table = source_table or f"{BRONZE}.{entity}_raw"
    silver_table = f"{SILVER}.{entity}"
    findings_table = f"{SILVER}.{entity}_dq_findings"
    print(f"=== validate_flow('{flow_id}') {VALIDATOR_VERSION} "
          f"[{'DRY RUN' if dry_run else 'WRITE'}] source={bronze_table} ===")

    # --- 2. structure metadata ----------------------------------------------
    comps = spark.sql(f"""
        SELECT component_id, component_type, assignment_status, attachment_level,
               codelist_agency, codelist_id, codelist_version
        FROM {SILVER}.dsd_components
        WHERE dsd_id = '{dsd_id}' AND dsd_version = '{dsd_version}'
        ORDER BY ordinal""").collect()

    dim_cols = [r["component_id"] for r in comps if r["component_type"] == "dimension"]
    time_col = [r["component_id"] for r in comps if r["component_type"] == "time_dimension"][0]
    attr_cols = [r["component_id"] for r in comps if r["component_type"] == "attribute"]
    measure_col = [r["component_id"] for r in comps if r["component_type"] == "measure"][0]
    attr_level = {r["component_id"]: r["attachment_level"]
                  for r in comps if r["component_type"] == "attribute"}
    mandatory = [r["component_id"] for r in comps
                 if r["component_type"] == "attribute" and r["assignment_status"] == "Mandatory"]

    cl_of = lambda r: (r["codelist_agency"], r["codelist_id"], r["codelist_version"])
    dim_cl = {r["component_id"]: cl_of(r) for r in comps
              if r["component_type"] == "dimension" and r["codelist_id"]}
    attr_cl = {r["component_id"]: cl_of(r) for r in comps
               if r["component_type"] == "attribute" and r["codelist_id"]}

    key_cols = dim_cols + [time_col]
    print(f"  Dimensions: {dim_cols}  Time: {time_col}  Measure: {measure_col}")
    print(f"  Mandatory attrs: {[(a, attr_level.get(a)) for a in mandatory]}")

    # --- 3. rule severity config ---------------------------------------------
    rule_sev = {(r["rule_id"], r["component"]): r["severity"]
                for r in spark.table(f"{SILVER}.rule_config").collect()}
    get_sev = lambda rule, comp="*": rule_sev.get((rule, comp),
                                                  rule_sev.get((rule, "*"), "error"))

    # --- 4. bronze + STRUCT -----------------------------------------------------
    bronze = spark.table(bronze_table)
    bronze_cols = set(bronze.columns)
    rows_in = bronze.count()
    expected = key_cols + attr_cols + [measure_col, "batch_id", "ingested_at"]
    missing = [c for c in expected if c not in bronze_cols]
    if missing:
        raise ValueError(f"STRUCT: columns missing from {bronze_table}: {missing}")
    print(f"  STRUCT: PASS ({len(expected)} components present). Bronze rows: {rows_in}")

    findings = []

    # --- 5. DUPLICATE (same key+period twice in the SAME batch) --------------------
    dup_keys = (bronze.groupBy(*key_cols, "batch_id")
                .agg(F.count("*").alias("dup_cnt")).filter("dup_cnt > 1"))

    # --- 6. cross-batch dedup: latest ingested_at wins (revisions) ---------------------
    other = [c for c in bronze.columns if c not in key_cols + ["ingested_at", "batch_id"]]
    w = Window.partitionBy(*key_cols).orderBy(
        F.col("ingested_at").desc(), F.col("batch_id").desc(),
        *[F.col(c) for c in other])
    deduped = bronze.withColumn("_rnk", F.row_number().over(w)).filter("_rnk = 1").drop("_rnk")
    deduped = (deduped
               .withColumn("row_key", F.concat_ws("|", *[F.col(c).cast("string") for c in key_cols]))
               .withColumn("row_id", F.row_number().over(
                   Window.orderBy(*[F.col(c) for c in key_cols], F.col("batch_id")))))
    rows_after = deduped.count()
    superseded = rows_in - rows_after
    print(f"  Dedup: {rows_in} -> {rows_after} ({superseded} superseded as revisions)")

    dups = deduped.join(dup_keys, key_cols + ["batch_id"], "inner")
    findings.append(_finding(dups, run_id, "DUPLICATE", get_sev("DUPLICATE"), "|".join(key_cols),
                             F.col("dup_cnt"), F.concat(F.lit("Same key+period delivered "),
                                                        F.col("dup_cnt").cast("string"),
                                                        F.lit(" times in one batch"))))

    # --- 7. DIM_CODE -----------------------------------------------------------------
    for dim, cl in dim_cl.items():
        bad = deduped.join(F.broadcast(_codes(cl)), deduped[dim] == F.col("_code"), "left_anti")
        findings.append(_finding(bad, run_id, "DIM_CODE", get_sev("DIM_CODE", dim), dim, F.col(dim),
                                 F.concat(F.lit("Value '"), F.coalesce(F.col(dim), F.lit("")),
                                          F.lit(f"' not in codelist {cl[1]}"))))

    # --- 8. ATTR_MANDATORY (dataset-level skipped; 100% empty = warning once) ---------
    for a in mandatory:
        if attr_level.get(a) == "dataset":
            continue
        blank = F.col(a).isNull() | (F.trim(F.col(a)) == "")
        n_blank = deduped.filter(blank).count()
        if n_blank == 0:
            continue
        if n_blank == rows_after:
            findings.append(_batch_finding(
                run_id, "ATTR_MANDATORY", "warning", a,
                f"Mandatory '{a}' empty in 100% of rows ({n_blank}); source never populates it"))
        else:
            findings.append(_finding(deduped.filter(blank), run_id, "ATTR_MANDATORY", "error",
                                     a, F.lit(None),
                                     f"Mandatory '{a}' empty in {n_blank} of {rows_after} rows"))

    # --- 9. ATTR_CODE ----------------------------------------------------------------------
    for a, cl in attr_cl.items():
        present = deduped.filter(F.col(a).isNotNull() & (F.trim(F.col(a)) != ""))
        bad = present.join(F.broadcast(_codes(cl)), present[a] == F.col("_code"), "left_anti")
        findings.append(_finding(bad, run_id, "ATTR_CODE", get_sev("ATTR_CODE", a), a, F.col(a),
                                 F.concat(F.lit("Value '"), F.col(a),
                                          F.lit(f"' not in codelist {cl[1]}"))))

    # --- 10. TIME_FORMAT: shape by FREQ, then strict calendar parse ----------------------
    pats = spark.table(f"{SILVER}.freq_time_patterns").collect()
    sub = lambda e: e.replace("{col}", f"`{time_col}`")
    cases = [(p["freq_code"], sub(p["date_parse_expr"])) for p in pats if p["date_parse_expr"]]
    case_sql = ("CASE " + " ".join(f"WHEN `FREQ` = '{f}' THEN {e}" for f, e in cases)
                + " ELSE NULL END") if cases else "CAST(NULL AS DATE)"
    print(f"  period_start_date expression built from freq_time_patterns for: "
          f"{[f for f, _ in cases]}")
    deduped = deduped.withColumn("period_start_date", F.expr(case_sql))

    pat_df = spark.table(f"{SILVER}.freq_time_patterns").select(
        F.col("freq_code").alias("_fc"), F.col("regex_pattern").alias("_rx"),
        F.col("date_parse_expr").alias("_dp"))
    tf = deduped.join(F.broadcast(pat_df), deduped["FREQ"] == F.col("_fc"), "left")
    shape_ok = F.col(time_col).isNotNull() & F.col(time_col).rlike(F.col("_rx"))

    findings += [
        _finding(tf.filter(F.col("_rx").isNull()), run_id, "TIME_FORMAT", "warning", time_col,
                 F.col(time_col), F.concat(F.lit("No time pattern defined for FREQ="),
                                           F.coalesce(F.col("FREQ"), F.lit("null")))),
        _finding(tf.filter(F.col("_rx").isNotNull() & ~shape_ok), run_id, "TIME_FORMAT",
                 get_sev("TIME_FORMAT"), time_col, F.col(time_col),
                 F.concat(F.lit("TIME_PERIOD does not match the pattern for FREQ="), F.col("FREQ"))),
        _finding(tf.filter(F.col("_rx").isNotNull() & shape_ok & F.col("_dp").isNull()),
                 run_id, "TIME_FORMAT", "warning", time_col, F.col(time_col),
                 F.concat(F.lit("No date parse defined for FREQ="), F.col("FREQ"))),
        _finding(tf.filter(F.col("_rx").isNotNull() & shape_ok & F.col("_dp").isNotNull()
                           & F.col("period_start_date").isNull()),
                 run_id, "TIME_FORMAT", get_sev("TIME_FORMAT"), time_col, F.col(time_col),
                 F.lit("TIME_PERIOD matches the pattern but is not a valid calendar period")),
    ]

    # --- 11. OBS_VALUE: numeric, or empty with a 'missing' OBS_STATUS --------------------
    missing_codes = [r["code_id"] for r in spark.table(f"{SILVER}.obs_status_missing").collect()]
    status = F.col("OBS_STATUS") if "OBS_STATUS" in bronze_cols else F.lit(None).cast("string")
    val = F.col(measure_col)
    num = F.expr(f"try_cast(`{measure_col}` AS DOUBLE)")
    non_blank = val.isNotNull() & (F.trim(val) != "")
    not_numeric = (num.isNull() | F.isnan(num) | (num == float("inf"))
                   | (num == float("-inf")) | (val != F.trim(val)))

    findings += [
        _finding(deduped.filter(non_blank & not_numeric), run_id, "OBS_VALUE",
                 get_sev("OBS_VALUE"), measure_col, val, F.lit("OBS_VALUE is not numeric")),
        _finding(deduped.filter(~non_blank & ~F.coalesce(status.isin(missing_codes), F.lit(False))),
                 run_id, "OBS_VALUE", get_sev("OBS_VALUE"), measure_col, val,
                 F.concat(F.lit("Empty OBS_VALUE but OBS_STATUS='"),
                          F.coalesce(status, F.lit("")), F.lit("' is not a missing-value code"))),
    ]

    # --- 12. materialise findings once (small data), then derive everything --------------
    all_f = findings[0]
    for f in findings[1:]:
        all_f = all_f.unionByName(f)
    findings_df = spark.createDataFrame(all_f.collect(), FINDING_SCHEMA)

    err_ids = (findings_df.filter((F.col("severity") == "error") & F.col("row_id").isNotNull())
               .select("row_id").distinct())
    warn_cnt = (findings_df.filter((F.col("severity") == "warning") & F.col("row_id").isNotNull())
                .groupBy("row_id").agg(F.count("*").cast("int").alias("dq_warning_count")))

    silver = (deduped.join(err_ids, "row_id", "left_anti")
              .join(warn_cnt, "row_id", "left")
              .withColumn("dq_warning_count", F.coalesce(F.col("dq_warning_count"), F.lit(0)))
              .withColumn(measure_col, F.expr(f"try_cast(`{measure_col}` AS DOUBLE)")))
    silver = silver.select(*dim_cols, time_col, "period_start_date", measure_col,
                           *attr_cols, "batch_id", "ingested_at", "dq_warning_count")
    rows_valid = silver.count()

    # --- 13. dq_summary rows ------------------------------------------------------------------
    errs = findings_df.filter(F.col("severity") == "error")
    warns = findings_df.filter(F.col("severity") == "warning")
    summary_rows = []
    for rule in RULES:
        e = errs.filter(F.col("rule_id") == rule)
        failed = (e.filter(F.col("row_id").isNotNull()).select("row_id").distinct().count()
                  + e.filter(F.col("row_id").isNull()).count())
        w_cnt = warns.filter(F.col("rule_id") == rule).count()
        pr = round(1.0 - (failed / rows_after), 4) if rows_after else 1.0
        summary_rows.append((run_id, run_ts, flow_id, rule, rows_after, failed,
                             float(pr), VALIDATOR_VERSION, w_cnt))
    summary_df = spark.createDataFrame(summary_rows, SUMMARY_SCHEMA)

    # --- 14. report ------------------------------------------------------------------------------
    print(f"\n  Rows in {rows_in} | superseded {superseded} | after dedup {rows_after} | "
          f"valid (silver) {rows_valid} | rejected (errors) {rows_after - rows_valid}")
    display(summary_df.select("rule_id", "rows_checked", "rows_failed", "pass_rate", "warning_count"))

    # --- 15. optional write ------------------------------------------------------------------------
    if not dry_run:
        (silver.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(silver_table))
        (findings_df.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(findings_table))
        summary_df.write.mode("append").insertInto(f"{SILVER}.dq_summary")  # positional
        print(f"  WROTE {silver_table}, {findings_table}; appended 7 rows to dq_summary")
    else:
        print("  DRY RUN: nothing written.")

    return {"run_id": run_id, "silver": silver, "findings": findings_df,
            "summary": summary_df, "key_cols": key_cols, "measure_col": measure_col,
            "rows_in": rows_in, "superseded": superseded,
            "rows_after": rows_after, "rows_valid": rows_valid}


def seeded_proof(result, fixture_table):
    """Per fixture row: expected_rule vs the rules that actually fired."""
    fx = spark.table(fixture_table).withColumn(
        "row_key", F.concat_ws("|", *[F.col(c).cast("string") for c in result["key_cols"]]))
    found = (result["findings"].filter(F.col("row_key").isNotNull())
             .groupBy("row_key")
             .agg(F.concat_ws(",", F.sort_array(F.collect_set("rule_id"))).alias("rules_found")))
    return fx.join(found, "row_key", "left").select("row_key", "expected_rule", "rules_found")


def compare_to_table(result, table):
    """Rows only in the new silver vs the existing table, and vice versa (0, 0 = identical)."""
    cols = result["key_cols"] + [result["measure_col"], "period_start_date"]
    a, b = result["silver"].select(*cols), spark.table(table).select(*cols)
    return a.exceptAll(b).count(), b.exceptAll(a).count()


print("validate_flow v2.1 defined (dry_run=True by default).")
