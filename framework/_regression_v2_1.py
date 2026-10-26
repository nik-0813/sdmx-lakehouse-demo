# Databricks notebook source
# MAGIC %md
# MAGIC # framework/_regression_v2_1
# MAGIC
# MAGIC Non-destructive regression for `validate_flow` v2.1. Every call uses
# MAGIC `dry_run=True`, so nothing is written to any table.
# MAGIC
# MAGIC Asserts the figures the README publishes, so a failure here means either the
# MAGIC validator or the README is wrong.

# COMMAND ----------

# MAGIC %run ./03_validate

# COMMAND ----------

import json

results = {}
failures = []


def check(name, actual, expected):
    ok = actual == expected
    results[name] = {"expected": expected, "actual": actual, "ok": ok}
    if not ok:
        failures.append(f"{name}: expected {expected}, got {actual}")
    print(f"  {'PASS' if ok else 'FAIL'}  {name:<34} expected={expected!r:<20} actual={actual!r}")

# COMMAND ----------

# DBTITLE 1,WS_CBPOL against live bronze
cbpol = validate_flow("WS_CBPOL", dry_run=True)
check("cbpol.rows_in", cbpol["rows_in"], 408)
check("cbpol.superseded", cbpol["superseded"], 48)
check("cbpol.rows_valid", cbpol["rows_valid"], 360)

# COMMAND ----------

# DBTITLE 1,WS_TC against live bronze
tc = validate_flow("WS_TC", dry_run=True)
check("tc.rows_valid", tc["rows_valid"], 192)
check("tc.superseded", tc["superseded"], 0)

# COMMAND ----------

# DBTITLE 1,Seeded-error fixture
seeded = validate_flow("WS_CBPOL", dry_run=True,
                       source_table="workspace.sdmx_bronze.cbpol_raw_seeded_test")

# US|2024-05 is deliberately delivered twice, so 9 rows dedupe to 8 and the 3 clean
# controls are the only rows that survive validation.
check("seeded.rows_in", seeded["rows_in"], 9)
check("seeded.rows_after", seeded["rows_after"], 8)
check("seeded.rows_valid", seeded["rows_valid"], 3)

proof = seeded_proof(seeded, "workspace.sdmx_bronze.cbpol_raw_seeded_test").collect()
caught, controls_dirty = [], []
for r in proof:
    found = (r["rules_found"] or "").split(",") if r["rules_found"] else []
    if r["expected_rule"] == "NONE":
        if found:
            controls_dirty.append((r["row_key"], found))
    elif r["expected_rule"] in found:
        caught.append(r["expected_rule"])

check("seeded.rules_caught", len(set(caught)), 6)
check("seeded.clean_controls_with_findings", len(controls_dirty), 0)
results["seeded.caught_rules"] = sorted(set(caught))
print("\n  caught:", sorted(set(caught)))
if controls_dirty:
    print("  controls with unexpected findings:", controls_dirty)

# COMMAND ----------

# DBTITLE 1,Compare against the existing silver tables
check("cbpol.compare_to_table", list(compare_to_table(cbpol, "workspace.sdmx_silver.cbpol")), [0, 0])
check("tc.compare_to_table", list(compare_to_table(tc, "workspace.sdmx_silver.tc")), [0, 0])

# COMMAND ----------

# DBTITLE 1,Result
print("\n" + "=" * 70)
print("REGRESSION PASSED" if not failures else "REGRESSION FAILED")
for f in failures:
    print("  " + f)
print("=" * 70)

dbutils.notebook.exit(json.dumps({"passed": not failures,
                                  "failures": failures,
                                  "results": results}, default=str))
