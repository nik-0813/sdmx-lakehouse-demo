# Databricks notebook source
# MAGIC %md
# MAGIC # framework/_regression_completeness
# MAGIC
# MAGIC Checks the rewritten `completeness_check` against the three findings the original
# MAGIC (now lost) implementation produced. `dry_run=True` throughout.
# MAGIC
# MAGIC Fixture: `cbpol_raw_collection` holds US at 2024-01/02/03, DE at 2024-01/02,
# MAGIC JP at 2024-01 only, plus an unmapped `Atlantis` row. `expected_series` expects
# MAGIC US, DE and JP monthly from 2024-01. The flow-wide horizon is therefore 2024-03,
# MAGIC so DE is missing one period and JP two.

# COMMAND ----------

# MAGIC %run ./04_completeness

# COMMAND ----------

import json

results = {}
failures = []


def check(name, actual, expected):
    ok = actual == expected
    results[name] = {"expected": expected, "actual": actual, "ok": ok}
    if not ok:
        failures.append(f"{name}: expected {expected}, got {actual}")
    print(f"  {'PASS' if ok else 'FAIL'}  {name:<34} expected={expected!r:<14} actual={actual!r}")

# COMMAND ----------

# DBTITLE 1,Run against the collection fixture
found = completeness_check("WS_CBPOL",
                           bronze_table="workspace.sdmx_bronze.cbpol_raw_collection",
                           dry_run=True)
check("rows", found.count(), 3)

COMPARE = ["flow_id", "series_key", "missing_period", "frequency", "message"]
live = spark.table("workspace.sdmx_bronze.collection_completeness_findings").select(*COMPARE)
mine = found.select(*COMPARE)
check("diff_vs_original", [mine.exceptAll(live).count(), live.exceptAll(mine).count()], [0, 0])

# COMMAND ----------

# DBTITLE 1,The gaps are the expected ones
gaps = sorted((r["series_key"], r["missing_period"]) for r in found.collect())
check("gaps", gaps, [("FREQ=M|REF_AREA=DE", "2024-03"),
                     ("FREQ=M|REF_AREA=JP", "2024-02"),
                     ("FREQ=M|REF_AREA=JP", "2024-03")])

# US reported every period, and Atlantis is not an expected series, so neither appears.
check("us_has_no_gap", [g for g in gaps if "US" in g[0]], [])
check("atlantis_not_checked", [g for g in gaps if "Atlantis" in g[0]], [])

# COMMAND ----------

# DBTITLE 1,Result
print("\n" + "=" * 70)
print("COMPLETENESS REGRESSION PASSED" if not failures else "COMPLETENESS REGRESSION FAILED")
for f in failures:
    print("  " + f)
print("=" * 70)

dbutils.notebook.exit(json.dumps({"passed": not failures,
                                  "failures": failures,
                                  "results": results}, default=str))
