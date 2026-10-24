# Databricks notebook source
# DBTITLE 1,Decision Log title
# MAGIC %md
# MAGIC # 99 — Decision Log
# MAGIC
# MAGIC Running log of design decisions, assumptions, and limitations across all phases of the SDMX Lakehouse project. Updated after each phase.

# COMMAND ----------

# DBTITLE 1,Phase F entry
# MAGIC %md
# MAGIC ---
# MAGIC ## Phase F — Fix shared codelist report query
# MAGIC
# MAGIC **What was built:** Fixed the report query in notebook 05 that counts codes per codelist for a given DSD.
# MAGIC
# MAGIC **Why (design decision):** The old query joined `dsd_components → codelists → codes` directly. When a codelist is shared across multiple components (e.g. `CL_BIS_UNIT` is used by both `UNIT_TYPE` and `UNIT_MEASURE` in `BIS_TOTAL_CREDIT`), each code row was counted once per component, inflating the count. The `codelists → codes` join also matched on `agency + codelist_id` only, missing `version`.
# MAGIC
# MAGIC **Fix:** (1) `SELECT DISTINCT codelist_id, codelist_agency, codelist_version` from `dsd_components` to deduplicate shared codelist references. (2) Added `version` to all joins.
# MAGIC
# MAGIC **Assumptions:** WS_CBPOL was checked and is unaffected (no shared codelists in BIS_CBPOL). All codelists currently use version 1.0, so the missing-version join didn't cause inflation yet — but it would if two versions ever coexist.
# MAGIC
# MAGIC **Known limitations:** None — this is a query fix only, no tables modified.
# MAGIC
# MAGIC **Tables/notebooks involved:**
# MAGIC - Read: `sdmx_silver.dsd_components`, `sdmx_silver.codelists`, `sdmx_silver.codes`
# MAGIC - Notebook: `05_Silver_Validation` (cells: before/after query, summary)
# MAGIC
# MAGIC **Result:** WS_TC total codes corrected from 2391 (inflated) to 1286 (actual). CL_BIS_UNIT corrected from 2210 to 1105.

# COMMAND ----------

# DBTITLE 1,Phase G entry
# MAGIC %md
# MAGIC ---
# MAGIC ## Phase G — WS_TC data ingest
# MAGIC
# MAGIC **What was built:** Ingested Total Credit (WS_TC) data into the bronze layer. First flow with more than 2 dimensions, validating the generic `ingest_flow` function.
# MAGIC
# MAGIC **Why (design decision):** Chose a small key (`Q.US+DE+JP.H.A.M.770.A`) with 3 countries, quarterly frequency, households sector, % of GDP, from 2010. This covers enough dimensions to test the validator later while keeping the dataset small (~192 rows). All 9 codes in the key were verified against `sdmx_silver.codes` before use — no guessing.
# MAGIC
# MAGIC **Assumptions:**
# MAGIC - DE (Germany) has total credit data at country level (unlike WS_CBPOL where Germany's policy rate is under XM/Euro area). Confirmed by API test.
# MAGIC - One sector (Households & NPISHs) is sufficient for validation testing.
# MAGIC - Quarterly data from 2010 is a reasonable test window.
# MAGIC
# MAGIC **Known limitations:**
# MAGIC - Only 3 countries, 1 sector, 1 unit — not comprehensive coverage of WS_TC.
# MAGIC - `ingest_flow` code was copied into notebook 05 to be self-contained; the canonical version remains in notebook 04.
# MAGIC
# MAGIC **Tables/notebooks involved:**
# MAGIC - Updated: `sdmx_bronze.flow_config` (WS_TC key, start_period, end_period, notes)
# MAGIC - Created: `sdmx_bronze.tc_raw` (192 rows, 17 source columns + 3 metadata columns, all strings)
# MAGIC - Raw file: `/Volumes/workspace/sdmx_bronze/raw_files/tc_20261024_132422.csv`
# MAGIC - Notebook: `05_Silver_Validation` (Phase G cells)
# MAGIC
# MAGIC **Result:** 192 rows, 3 unique series (US, DE, JP), 64 quarters each (2010-Q1 to 2025-Q4), no gaps.

# COMMAND ----------

# DBTITLE 1,Phase H entry
# MAGIC %md
# MAGIC ---
# MAGIC ## Phase H — Validation design (no code yet, awaiting approval)
# MAGIC
# MAGIC **What was built:** Design proposal for the metadata-driven validation framework. No code — just the rule catalogue, table schemas, and dedup rule.
# MAGIC
# MAGIC **Why (design decision):**
# MAGIC - All 7 rules derive from `dsd_components` and `codes` tables — no hard-coded column names or codelist IDs. Adding a new flow needs only `flow_config` + `load_structures` + `ingest_flow`.
# MAGIC - The FREQ → TIME_PERIOD pattern mapping uses a small lookup table (`freq_time_patterns`) rather than hard-coding patterns, so new frequencies can be added without touching validator code.
# MAGIC - Dedup is a pre-processing step (latest `ingested_at` wins), separate from the DUPLICATE validation rule which verifies no duplicates remain.
# MAGIC - OBS_VALUE rule allows null values when OBS_STATUS indicates missing (H, L, M, Q) — derived from `code_name LIKE 'Missing%'` in CL_OBS_STATUS, not hard-coded.
# MAGIC
# MAGIC **Assumptions:**
# MAGIC - BIS CSV API always returns columns matching DSD component_ids (verified for both flows).
# MAGIC - TIME_PERIOD formats follow standard SDMX conventions (verified for M and Q against actual data; A, H, D, W included but not yet seen).
# MAGIC - period_start_date can be parsed from TIME_PERIOD (e.g., 2020-Q1 → 2020-01-01).
# MAGIC
# MAGIC **Known limitations:**
# MAGIC - TIME_FORMAT attribute is NULL in all 408 cbpol_raw rows (BIS CSV doesn't populate it). ATTR_MANDATORY will flag all rows — this is a source limitation, not our bug.
# MAGIC - 48 duplicate keys in cbpol_raw from overlapping Phase 3 + Phase D ingests. Dedup will resolve.
# MAGIC - FREQ codes B and E have no pattern (marked 'not supported'); TIME_FORMAT rule will flag them.
# MAGIC
# MAGIC **Tables/notebooks involved:**
# MAGIC - Read: `sdmx_silver.dsd_components`, `sdmx_silver.codes`, `sdmx_bronze.cbpol_raw`, `sdmx_bronze.tc_raw`
# MAGIC - Proposed new: `sdmx_silver.freq_time_patterns` (lookup), `sdmx_silver.<flow>` (valid), `sdmx_silver.<flow>_rejects` (failures), `sdmx_silver.dq_summary` (shared)
# MAGIC - Notebook: `05_Silver_Validation` (Phase H design cells), `99_Decision_Log`
# MAGIC
# MAGIC **Result:** Design proposed, awaiting user approval.

# COMMAND ----------

# DBTITLE 1,Phase H updated entry
# MAGIC %md
# MAGIC ---
# MAGIC ## Phase H (updated) — Design changes per user approval
# MAGIC
# MAGIC **What changed:**
# MAGIC 1. Added `sdmx_silver.rule_config` (rule_id, component, severity) — severity is now configurable. ATTR_MANDATORY: 100% null in a batch → WARNING ("source never populates"); partial → ERROR.
# MAGIC 2. DUPLICATE rule: error only for same key+period within the SAME batch. Cross-batch repeats are revisions (latest ingested_at wins, reported as superseded, not failures).
# MAGIC 3. TIME_FORMAT: strict parse for period_start_date (rows that don't parse fail). No pattern for a FREQ → WARNING, not failure. Patterns for A/M/Q/D verified against actual BIS data; H/W from SDMX conventions (unverified).
# MAGIC 4. `sdmx_silver.obs_status_missing` table stores the 4 missing-value codes (H, L, M, Q) explicitly, derived from CL_OBS_STATUS. No `LIKE 'Missing%'` at validation time.
# MAGIC 5. Rejects table: added `run_id` and `severity` columns. Silver table: added `dq_warning_count`.
# MAGIC 6. `freq_time_patterns` is **reference data from SDMX conventions, not derived from the DSD** — patterns are based on the SDMX standard and verified against actual BIS API responses where data was available.
# MAGIC
# MAGIC **Investigation results:**
# MAGIC - sdmx-csv format: TIME_FORMAT still empty — BIS never populates it regardless of format.
# MAGIC - DSD attachment level: `attributeRelationship` in DSD JSON has 3 patterns (series/observation/dataset level). `dsd_components` does NOT currently record this — not changed yet.
# MAGIC - TIME_PERIOD patterns verified from actual data: A (WS_CBTA: 2020), M (WS_CBPOL: 2024-01), Q (WS_TC: 2010-Q1), D (WS_CBPOL daily: 2024-01-01). H, W, B: no data found.
# MAGIC
# MAGIC **Tables/notebooks involved:**
# MAGIC - Created: `sdmx_silver.rule_config`, `sdmx_silver.freq_time_patterns`, `sdmx_silver.obs_status_missing`
# MAGIC - Notebook: `05_Silver_Validation`, `99_Decision_Log`

# COMMAND ----------

# DBTITLE 1,Phase I entry
# MAGIC %md
# MAGIC ---
# MAGIC ## Phase I — Implement validate_flow for WS_CBPOL
# MAGIC
# MAGIC **What was built:** Implemented `validate_flow(flow_id)` — a metadata-driven validator that reads DSD components and codelists to validate bronze data and write typed results to silver. Ran it for WS_CBPOL.
# MAGIC
# MAGIC **Why (design decision):**
# MAGIC - The validator reads `dsd_components`, `codes`, `rule_config`, `freq_time_patterns`, and `obs_status_missing` at runtime — zero hard-coded column names or codelist IDs.
# MAGIC - Dedup (cross-batch, latest ingested_at wins) is separate from DUPLICATE rule (same-batch duplicates only).
# MAGIC - ATTR_MANDATORY uses the 100% null rule: TIME_FORMAT is NULL in all 360 deduped rows → all get WARNING, not ERROR. This correctly identifies a source limitation.
# MAGIC - period_start_date parsed from TIME_PERIOD using FREQ-specific logic (A→yyyy, M→yyyy-MM, Q→year+quarter→month, D→yyyy-MM-dd).
# MAGIC - OBS_VALUE checked via cast to double (null result = non-numeric). Missing OBS_STATUS codes from explicit table, not LIKE pattern.
# MAGIC
# MAGIC **Assumptions:**
# MAGIC - `.cast("double")` returns null for non-numeric strings (verified: works in Spark 4.2).
# MAGIC - All 5 BIS mandatory attributes for WS_CBPOL are present as columns; only TIME_FORMAT is empty (source limitation).
# MAGIC - No duplicates within the same batch (verified: both Phase 3 and Phase D batches have unique keys within each batch).
# MAGIC
# MAGIC **Known limitations:**
# MAGIC - `freq_time_patterns` includes unverified patterns for H, W (no BIS data available to confirm). If a flow uses these frequencies, the regex may not match.
# MAGIC - period_start_date parsing uses a CASE expression with FREQ values — frequencies without a matching CASE branch return NULL.
# MAGIC - DSD attachment level (series/observation/dataset) is not yet stored in `dsd_components`. This could affect attribute validation granularity in the future.
# MAGIC - The `row_key` in rejects uses full series key (all dims + TIME_PERIOD) for human readability.
# MAGIC
# MAGIC **Tables/notebooks involved:**
# MAGIC - Created: `sdmx_silver.cbpol` (360 rows, 19 cols), `sdmx_silver.cbpol_rejects` (360 rows), `sdmx_silver.dq_summary` (7 rows for WS_CBPOL)
# MAGIC - Read: `sdmx_bronze.cbpol_raw`, `sdmx_silver.dsd_components`, `sdmx_silver.codes`, `sdmx_silver.rule_config`, `sdmx_silver.freq_time_patterns`, `sdmx_silver.obs_status_missing`
# MAGIC - Notebook: `05_Silver_Validation` (Phase I cells)
# MAGIC
# MAGIC **Result:** 408 rows in → 48 superseded → 360 after dedup → 360 valid, 0 errors, 360 warnings (TIME_FORMAT source limitation). All 7 rules executed correctly.

# COMMAND ----------

