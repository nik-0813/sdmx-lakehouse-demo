# Databricks notebook source
# DBTITLE 1,Status — code not reproducible
# MAGIC %md
# MAGIC > ## ⚠️ This notebook is documentation only — the code is missing
# MAGIC >
# MAGIC > The results below were produced in interactive cells that were never saved.
# MAGIC > This notebook contains **no executable cells**: the reporter-file transform,
# MAGIC > the mapping application and `completeness_check()` are **not** in this repo,
# MAGIC > and neither are the DDL for `collection_source`, `collection_mapping`,
# MAGIC > `collection_mapping_findings`, `expected_series` and
# MAGIC > `collection_completeness_findings`.
# MAGIC >
# MAGIC > The tables exist in the Databricks workspace and the figures below are real,
# MAGIC > but **this phase cannot currently be rebuilt from the repository.** It is
# MAGIC > listed as unchecked in the README Status section. Treat the "10/10" table as
# MAGIC > a record of what was run, not as reproducible evidence.

# COMMAND ----------

# DBTITLE 1,Phase L overview
# MAGIC %md
# MAGIC # Phase L — Collection Simulation
# MAGIC
# MAGIC **Goal:** Prove that the SDMX lakehouse can ingest a non-SDMX "reporter" file and validate it with the same `validate_flow`, using a metadata-driven mapping table.
# MAGIC
# MAGIC ## Design
# MAGIC
# MAGIC 1. **Source file** (`sdmx_bronze.collection_source`): Non-SDMX reporter with local country names, Jan-24 dates, comma decimals, missing rate, unknown country
# MAGIC 2. **Mapping table** (`sdmx_bronze.collection_mapping`): Source field/value → DSD component/code + transforms (data, not code)
# MAGIC 3. **Transformation**: Maps values, converts dates/decimals, sets constants, logs unmapped values as mapping findings, writes to bronze
# MAGIC 4. **Validation**: Run `validate_flow` with NO code changes on the transformed bronze data
# MAGIC 5. **Bronze lineage**: `source_file` and `source_row` columns for traceability
# MAGIC
# MAGIC ## Results
# MAGIC
# MAGIC All 8 expectations met:
# MAGIC - Mapping finding for unmapped 'Atlantis' ✅
# MAGIC - DIM_CODE catches REF_AREA='Atlantis' ✅
# MAGIC - OBS_VALUE catches null rate with OBS_STATUS='A' ✅
# MAGIC - ATTR_MANDATORY warns on TIME_FORMAT 100% null ✅
# MAGIC - Japan gap (no Feb-24 row) not detected ✅ (expected — validator only checks existing rows)
# MAGIC - Comma decimals correctly converted ✅
# MAGIC - Jan-24 dates correctly converted to 2024-01 ✅
# MAGIC - 0 findings on 5 clean rows ✅

# COMMAND ----------

# DBTITLE 1,Completeness check documentation
# MAGIC %md
# MAGIC ## Completeness Check (added)
# MAGIC
# MAGIC **Validation catches wrong data. Completeness catches missing data.**
# MAGIC
# MAGIC `validate_flow` checks existing rows — it cannot detect gaps (expected series-periods with no row). The completeness check fills this gap:
# MAGIC
# MAGIC 1. **`sdmx_bronze.expected_series`** — config-driven table: flow_id, series_key (JSON), first_period, frequency
# MAGIC 2. **`completeness_check(flow_id, bronze_table)`** — generic function, no flow-specific code:
# MAGIC    - Reads expected_series for the flow
# MAGIC    - Generates expected TIME_PERIODs from first_period to the latest period in actual data
# MAGIC    - Compares with actual periods per series
# MAGIC    - Produces WARNING findings for each missing series-period
# MAGIC
# MAGIC ### Results
# MAGIC
# MAGIC 3 completeness findings (WARNINGs):
# MAGIC - DE missing 2024-03 (gap)
# MAGIC - JP missing 2024-02 (gap)
# MAGIC - JP missing 2024-03 (gap)
# MAGIC
# MAGIC US has all 3 periods — no gap.
# MAGIC
# MAGIC ### Updated expected-vs-actual (10/10 ✅)
# MAGIC
# MAGIC | # | Issue | Source | Result |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 1 | Unmapped 'Atlantis' | Mapping | ✅ |
# MAGIC | 2 | REF_AREA='Atlantis' | validate_flow | ✅ |
# MAGIC | 3 | Null OBS_VALUE | validate_flow | ✅ |
# MAGIC | 4 | TIME_FORMAT 100% null | validate_flow | ✅ |
# MAGIC | 5 | JP missing Feb-24 | completeness | ✅ |
# MAGIC | 6 | DE missing Mar-24 | completeness | ✅ |
# MAGIC | 7 | JP missing Mar-24 | completeness | ✅ |
# MAGIC | 8 | Comma decimals | transform | ✅ |
# MAGIC | 9 | Jan-24 dates | transform | ✅ |
# MAGIC | 10 | Clean rows (5) | validate_flow | ✅ |
# MAGIC
# MAGIC **Tables created:** `sdmx_bronze.expected_series` (3 rows), `sdmx_bronze.collection_completeness_findings` (3 rows)

# COMMAND ----------

