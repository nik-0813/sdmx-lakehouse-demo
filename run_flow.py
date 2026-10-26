# Databricks notebook source
# MAGIC %md
# MAGIC # run_flow
# MAGIC
# MAGIC Runs the whole SDMX pipeline for one dataflow. **This is the only notebook you run to
# MAGIC onboard a dataset.**
# MAGIC
# MAGIC Adding a new dataflow is one `INSERT` into `sdmx_bronze.flow_config` and then this
# MAGIC notebook. No new code, no new tables, no new validation rules - every stage reads its
# MAGIC behaviour from the DSD, the codelists and the reference tables.
# MAGIC
# MAGIC | Stage | Function | Writes |
# MAGIC | --- | --- | --- |
# MAGIC | 01 | `load_structures` | `dsd_components`, `codelists`, `codes` |
# MAGIC | 02 | `ingest_flow` | `sdmx_bronze.<entity>_raw` + raw file in the Volume |
# MAGIC | 03 | `validate_flow` | `sdmx_silver.<entity>`, `<entity>_dq_findings`, `dq_summary` |
# MAGIC | 04 | `completeness_check` | `collection_completeness_findings` |
# MAGIC | 05 | `publish_gold` | `sdmx_gold.<entity>_timeseries`, `_series`, `_sdmx_csv`, `dq_overview` |
# MAGIC
# MAGIC **Widgets**
# MAGIC
# MAGIC - `flow_id` - must already exist and be enabled in `flow_config`.
# MAGIC - `dry_run` - `true` computes and reports everything but writes nothing to silver or
# MAGIC   gold. Default `true`.
# MAGIC - `fetch` - `true` calls the BIS API (stages 01 and 02). These always write to bronze,
# MAGIC   so they are gated separately from `dry_run`. Set `false` to re-run the downstream
# MAGIC   stages against bronze data already held. Default `false`.
# MAGIC
# MAGIC Run `framework/00_bootstrap` once before the first flow.

# COMMAND ----------

dbutils.widgets.text("flow_id", "WS_CBPOL", "Flow ID")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry run (no silver/gold writes)")
dbutils.widgets.dropdown("fetch", "false", ["true", "false"], "Fetch from BIS API (writes bronze)")

# COMMAND ----------

# MAGIC %run ./framework/01_structures

# COMMAND ----------

# MAGIC %run ./framework/02_ingest

# COMMAND ----------

# MAGIC %run ./framework/03_validate

# COMMAND ----------

# MAGIC %run ./framework/04_completeness

# COMMAND ----------

# MAGIC %run ./framework/05_gold

# COMMAND ----------

# DBTITLE 1,Run the pipeline
import json

flow_id = dbutils.widgets.get("flow_id").strip()
dry_run = dbutils.widgets.get("dry_run") == "true"
fetch = dbutils.widgets.get("fetch") == "true"

cfg = spark.sql(f"""SELECT * FROM workspace.sdmx_bronze.flow_config
                    WHERE flow_id = '{flow_id}' AND enabled = true""").collect()
if not cfg:
    raise ValueError(f"'{flow_id}' is not an enabled row in flow_config. "
                     f"Add it there first - that is the whole onboarding step.")

print("=" * 78)
print(f"run_flow('{flow_id}')  dry_run={dry_run}  fetch={fetch}")
print(f"  key={cfg[0]['key']}  period={cfg[0]['start_period']}..{cfg[0]['end_period']}")
print("=" * 78)

summary = {"flow_id": flow_id, "dry_run": dry_run, "fetch": fetch}

if fetch:
    print("\n--- 01 load_structures ---")
    load_structures(flow_id)
    print("\n--- 02 ingest_flow ---")
    ingest_flow(flow_id)
else:
    print("\n--- 01/02 skipped (fetch=false): using bronze data already held ---")

print("\n--- 03 validate_flow ---")
validated = validate_flow(flow_id, dry_run=dry_run)
summary.update({k: validated[k] for k in
                ("run_id", "rows_in", "superseded", "rows_after", "rows_valid")})

print("\n--- 04 completeness_check ---")
summary["completeness_findings"] = completeness_check(flow_id, dry_run=dry_run).count()

print("\n--- 05 publish_gold ---")
# publish_gold reads the silver table on disk. On a dry run nothing was written, so for a
# brand-new flow there is nothing to publish yet - that is expected, not a failure.
entity = flow_id.replace("WS_", "").lower()
if dry_run and not spark.catalog.tableExists(f"workspace.sdmx_silver.{entity}"):
    print(f"  skipped: silver.{entity} does not exist yet and dry_run wrote nothing. "
          f"Re-run with dry_run=false to create it.")
    summary["gold"] = "skipped (no silver table)"
else:
    gold = publish_gold(flow_id, dry_run=dry_run)
    summary.update({"rows_timeseries": gold["rows_timeseries"],
                    "rows_series": gold["rows_series"],
                    "structure_id": gold["structure_id"]})
    refresh_dq_overview(dry_run=dry_run)

print("\n" + "=" * 78)
print(json.dumps(summary, indent=2, default=str))
print("=" * 78)

dbutils.notebook.exit(json.dumps(summary, default=str))
