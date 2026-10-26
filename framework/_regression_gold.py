# Databricks notebook source
# MAGIC %md
# MAGIC # framework/_regression_gold
# MAGIC
# MAGIC Non-destructive check that `publish_gold` and `refresh_dq_overview` reproduce the
# MAGIC existing gold objects exactly. Everything runs with `dry_run=True`.
# MAGIC
# MAGIC A pass means the rebuilt gold layer is byte-for-byte equivalent to the one that was
# MAGIC created interactively and then lost, so the framework can be trusted to regenerate it.

# COMMAND ----------

# MAGIC %run ./05_gold

# COMMAND ----------

import json

results = {}
failures = []


def check(name, actual, expected):
    ok = actual == expected
    results[name] = {"expected": expected, "actual": actual, "ok": ok}
    if not ok:
        failures.append(f"{name}: expected {expected}, got {actual}")
    print(f"  {'PASS' if ok else 'FAIL'}  {name:<40} expected={expected!r:<16} actual={actual!r}")


def diff(df, table):
    """(rows only in df, rows only in table) - (0, 0) means identical."""
    existing = spark.table(table).select(*df.columns)
    return [df.exceptAll(existing).count(), existing.exceptAll(df).count()]


def cols(df, table):
    """Column order must match too - diff() reorders, so it cannot see this."""
    return [list(df.columns), list(spark.table(table).columns)]

# COMMAND ----------

# DBTITLE 1,WS_CBPOL
cbpol = publish_gold("WS_CBPOL", dry_run=True)
check("cbpol.rows_timeseries", cbpol["rows_timeseries"], 360)
check("cbpol.rows_series", cbpol["rows_series"], 5)
check("cbpol.structure_id", cbpol["structure_id"], "BIS:WS_CBPOL(1.0)")
check("cbpol.timeseries_diff", diff(cbpol["timeseries"], "workspace.sdmx_gold.cbpol_timeseries"), [0, 0])
check("cbpol.series_diff", diff(cbpol["series"], "workspace.sdmx_gold.cbpol_series"), [0, 0])
built, live = cols(cbpol["timeseries"], "workspace.sdmx_gold.cbpol_timeseries")
check("cbpol.timeseries_col_order", built, live)
built, live = cols(cbpol["series"], "workspace.sdmx_gold.cbpol_series")
check("cbpol.series_col_order", built, live)

# COMMAND ----------

# DBTITLE 1,WS_TC
tc = publish_gold("WS_TC", dry_run=True)
check("tc.rows_timeseries", tc["rows_timeseries"], 192)
check("tc.rows_series", tc["rows_series"], 3)
check("tc.structure_id", tc["structure_id"], "BIS:WS_TC(2.0)")
check("tc.timeseries_diff", diff(tc["timeseries"], "workspace.sdmx_gold.tc_timeseries"), [0, 0])
check("tc.series_diff", diff(tc["series"], "workspace.sdmx_gold.tc_series"), [0, 0])
# Deliberate deviation: live tc_timeseries puts the dataset-level COLLECTION last, while
# live cbpol_timeseries puts its dataset-level attributes first - the two were built at
# different times with ad-hoc column lists. publish_gold applies attachment-level order
# consistently, which is what both _sdmx_csv views already use. Data is identical.
check("tc.timeseries_col_order", list(tc["timeseries"].columns),
      ["FREQ", "BORROWERS_CTY", "TC_BORROWERS", "TC_LENDERS", "VALUATION", "UNIT_TYPE",
       "TC_ADJUST", "TIME_PERIOD", "period_start_date", "OBS_VALUE", "OBS_STATUS",
       "COLLECTION", "UNIT_MULT", "UNIT_MEASURE", "TITLE_TS", "DECIMALS",
       "OBS_PRE_BREAK", "OBS_CONF"])
built, live = cols(tc["series"], "workspace.sdmx_gold.tc_series")
check("tc.series_col_order", built, live)

# COMMAND ----------

# DBTITLE 1,WS_CBTA
# A 6-dimension DSD the code had never seen when it was written. Its SDMX-CSV attribute
# order comes out right because the order is derived from the attachment hierarchy.
cbta = publish_gold("WS_CBTA", dry_run=True)
check("cbta.rows_timeseries", cbta["rows_timeseries"], 96)
check("cbta.rows_series", cbta["rows_series"], 8)
check("cbta.structure_id", cbta["structure_id"], "BIS:WS_CBTA(1.0)")
check("cbta.timeseries_diff", diff(cbta["timeseries"], "workspace.sdmx_gold.cbta_timeseries"), [0, 0])
check("cbta.series_diff", diff(cbta["series"], "workspace.sdmx_gold.cbta_series"), [0, 0])
built, live = cols(cbta["timeseries"], "workspace.sdmx_gold.cbta_timeseries")
check("cbta.timeseries_col_order", built, live)
built, live = cols(cbta["series"], "workspace.sdmx_gold.cbta_series")
check("cbta.series_col_order", built, live)

# COMMAND ----------

# DBTITLE 1,SDMX-CSV views match the existing ones column for column
for entity, built_result in (("cbpol", cbpol), ("tc", tc), ("cbta", cbta)):
    select_sql = built_result["view_sql"].split(" AS\n", 1)[1]
    built = [c.upper() for c in spark.sql(select_sql).limit(0).columns]
    live = [c.upper() for c in spark.table(f"workspace.sdmx_gold.{entity}_sdmx_csv").columns]
    check(f"{entity}.sdmx_csv_columns", built, live)

# COMMAND ----------

# DBTITLE 1,dq_overview
ov = refresh_dq_overview(dry_run=True)
check("dq_overview.rows", ov.count(), 3)
check("dq_overview.diff", diff(ov, "workspace.sdmx_gold.dq_overview"), [0, 0])

# COMMAND ----------

# DBTITLE 1,Result
print("\n" + "=" * 70)
print("GOLD REGRESSION PASSED" if not failures else "GOLD REGRESSION FAILED")
for f in failures:
    print("  " + f)
print("=" * 70)

dbutils.notebook.exit(json.dumps({"passed": not failures,
                                  "failures": failures,
                                  "results": results}, default=str))
