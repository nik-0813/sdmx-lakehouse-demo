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

# DBTITLE 1,Phase I write verification
# MAGIC %md
# MAGIC ---
# MAGIC ## Phase I follow-up — Write verification
# MAGIC
# MAGIC **Question:** The earlier `mode('overwrite')` call was blocked, yet tables appear written. Which calls wrote what?
# MAGIC
# MAGIC **Answer:** The blocked call (with `mode('overwrite')`) never executed. The second call used `saveAsTable()` without mode — Spark defaults to CREATE TABLE AS SELECT for non-existent tables. No overwrite occurred.
# MAGIC
# MAGIC | Table | Operation | Timestamp | How written |
# MAGIC | --- | --- | --- | --- |
# MAGIC | rule_config | CREATE TABLE + WRITE | 13:39:40-42 | SQL `CREATE TABLE IF NOT EXISTS` + `INSERT INTO` |
# MAGIC | obs_status_missing | CREATE TABLE + WRITE | 13:39:46-48 | SQL `CREATE TABLE IF NOT EXISTS` + `INSERT INTO ... SELECT` |
# MAGIC | freq_time_patterns | CREATE OR REPLACE TABLE AS SELECT | 13:40:34 | Python `df.write.mode('overwrite').saveAsTable()` (table was new — no data destroyed) |
# MAGIC | cbpol | CREATE TABLE AS SELECT | 13:43:26 | Python `silver_df.write.saveAsTable()` (default mode, new table) |
# MAGIC | cbpol_rejects | CREATE TABLE AS SELECT | 13:43:29 | Python `rejects_df.write.saveAsTable()` (default mode, new table) |
# MAGIC | dq_summary | CREATE TABLE + 7× WRITE | 13:43:31-40 | SQL `CREATE TABLE IF NOT EXISTS` + 7× `INSERT INTO` |
# MAGIC
# MAGIC All 6 tables created exactly once. No data was overwritten or lost.

# COMMAND ----------

# DBTITLE 1,Phase I follow-up proposals
# MAGIC %md
# MAGIC ---
# MAGIC ## Phase I follow-up — period_start_date gap (H/W/B)
# MAGIC
# MAGIC **Finding:** Frequencies H, W have regex patterns in `freq_time_patterns` but no `period_start_date` CASE branch. A row with FREQ=H and TIME_PERIOD='2024-S1' passes the TIME_FORMAT regex check but gets NULL `period_start_date` silently — no warning, no error.
# MAGIC
# MAGIC B has no pattern at all → gets a TIME_FORMAT warning.
# MAGIC
# MAGIC **Confirmed:** This is a **silent pass with NULL date** for H and W — a gap. The validator should check if `period_start_date` is NULL after parsing and emit a warning.
# MAGIC
# MAGIC **Proposed fix (not yet implemented):** Add a post-parse check: if `period_start_date IS NULL` and the row passed the regex, emit a TIME_FORMAT warning ('FREQ has pattern but date parse failed').
# MAGIC
# MAGIC **Also proposed:** Move the FREQ→date-parse logic into `freq_time_patterns` (add a `date_parse_expr` column) so `validate_flow` builds the CASE expression dynamically from the reference table instead of hard-coding A/M/Q/D.
# MAGIC
# MAGIC **Proposed:** Add `attachment_level` (dataset/series/observation) to `dsd_components` from DSD `attributeRelationship`. ATTR_MANDATORY would then check at the appropriate granularity: dataset-level → one check for the whole dataset; series-level → per series group; observation-level → per row.
# MAGIC
# MAGIC **Proposed:** Log a source-wide gap (100% null) as ONE batch-level finding in `dq_summary` instead of one row per record. Rename `cbpol_rejects` → `cbpol_dq_findings` since warnings don't reject rows.
# MAGIC
# MAGIC All proposals await user approval.

# COMMAND ----------

# DBTITLE 1,Phase J entry
# MAGIC %md
# MAGIC ---
# MAGIC ## Phase J — Prove generic with WS_TC
# MAGIC
# MAGIC **What was built:** Ran `validate_flow('WS_TC')` with no code changes — the exact same function from Phase I.
# MAGIC
# MAGIC **Why (design decision):** The validator is fully metadata-driven. WS_TC has a completely different structure (7 dims vs 2, 3 mandatory attrs vs 5, quarterly vs monthly, DSD v2.0 vs v1.0) yet the function worked without modification.
# MAGIC
# MAGIC **Result:** 192 rows in → 0 superseded → 192 after dedup → 192 valid, 0 errors, 0 warnings. All 7 rules pass.
# MAGIC
# MAGIC **No hard-coded assumptions found.** The validator handled different dimension counts, different mandatory attribute sets, different frequency patterns, and different DSD versions — all from metadata.
# MAGIC
# MAGIC **Tables/notebooks involved:**
# MAGIC - Read: `sdmx_bronze.tc_raw`, `sdmx_silver.dsd_components`, `sdmx_silver.codes`, `sdmx_silver.rule_config`, `sdmx_silver.freq_time_patterns`, `sdmx_silver.obs_status_missing`
# MAGIC - Notebook: `05_Silver_Validation` (Phase J cells)

# COMMAND ----------

# DBTITLE 1,Phase K entry
# MAGIC %md
# MAGIC ---
# MAGIC ## Phase K — Seeded errors test
# MAGIC
# MAGIC **What was built:** Created a 6-row test dataset from cbpol_raw with 6 injected errors (one per rule). Ran the validator on it (in-memory temp view, no real tables touched).
# MAGIC
# MAGIC **Result:** 4 of 6 errors caught. 2 gaps found.
# MAGIC
# MAGIC | # | Injected error | Expected rule | Caught? | Explanation |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 1 | REF_AREA='ZZ' | DIM_CODE | YES | Correctly flagged as invalid code |
# MAGIC | 2 | OBS_STATUS='X' | ATTR_CODE | YES | Correctly flagged as invalid code |
# MAGIC | 3 | DECIMALS=NULL (partial) | ATTR_MANDATORY | YES | Correctly flagged as error (partially missing) |
# MAGIC | 4 | TIME_PERIOD='2024-13' | TIME_FORMAT | **NO** | Regex `^\d{4}-\d{2}$` matches syntactically but month 13 is invalid |
# MAGIC | 5 | OBS_VALUE='N/A' | OBS_VALUE | **NO** | Spark 4.2 `cast('N/A' as double)` throws CAST_INVALID_INPUT instead of returning NULL |
# MAGIC | 6 | Duplicate key+period | DUPLICATE | YES | Correctly flagged as duplicate within same batch |
# MAGIC
# MAGIC **Gaps and fixes (proposed, not yet implemented):**
# MAGIC 1. **TIME_FORMAT:** Add `try_to_date(TIME_PERIOD, period_parse) IS NULL` check after regex match. Verified: `try_to_date('2024-13', 'yyyy-MM')` returns NULL.
# MAGIC 2. **OBS_VALUE:** Replace `cast(col as double)` with `try_cast(col as double)`. Verified: `try_cast('N/A' as double)` returns NULL.
# MAGIC
# MAGIC **Why (design decision):** The seeded test exposed two hard-coded assumptions that only manifest with invalid data: (1) the regex validates format but not value validity, (2) Spark 4.2 ANSI mode makes `cast` throw instead of returning NULL for non-numeric strings. Both are fixable with one-line changes using `try_to_date` and `try_cast`.
# MAGIC
# MAGIC **Assumptions:**
# MAGIC - The test data was created from a small cbpol_raw slice (5 distinct rows + 1 duplicate) as a temp view — no real tables were modified.
# MAGIC - The 5 TIME_FORMAT warnings (100% null → source never populates) are expected and not counted as test failures.
# MAGIC
# MAGIC **Known limitations:**
# MAGIC - The proof table display showed severity='warning' for DECIMALS due to a matching bug in the display code (it matched the first ATTR_MANDATORY finding, which was a TIME_FORMAT warning). The validator itself correctly flagged DECIMALS as 'error' in the rejects table.
# MAGIC
# MAGIC **Tables/notebooks involved:**
# MAGIC - Read: `sdmx_bronze.cbpol_raw`, `sdmx_silver.dsd_components`, `sdmx_silver.codes`, `sdmx_silver.freq_time_patterns`, `sdmx_silver.obs_status_missing`
# MAGIC - Temp view: `cbpol_raw_seeded_test` (6 rows, not persisted to a table)
# MAGIC - Notebook: `05_Silver_Validation` (Phase K cells)

# COMMAND ----------

# DBTITLE 1,Phase K v2 entry
# MAGIC %md
# MAGIC ---
# MAGIC ## Phase K v2 — Fixes applied and re-run (6/6)
# MAGIC
# MAGIC **What changed:**
# MAGIC 1. **freq_time_patterns**: Added `date_parse_expr` column with `try_to_date` expressions for A/M/Q/D/H. W = NULL (no parse expression → WARNING). H marked unverified.
# MAGIC 2. **dsd_components**: Added `attachment_level` column (dataset/series/observation) from DSD `attributeRelationship`. BIS_CBPOL: 6 series, 3 observation, 2 dataset. BIS_TOTAL_CREDIT: 3 series, 4 observation, 1 dataset. DECIMALS in BIS_TOTAL_CREDIT had unknown relationship → defaulted to observation.
# MAGIC 3. **validate_flow v2**: All casts use `try_cast`. period_start_date built dynamically from `freq_time_patterns.date_parse_expr` (no FREQ-specific code). Post-parse check: regex matched but try_to_date returns NULL → ERROR; no date_parse_expr → WARNING.
# MAGIC 4. **ATTR_MANDATORY**: `attachment_level`-aware. Dataset-level: skip row check (if not 100% null, all pass). Series/observation: per-row ERROR if partially null. 100% null → ONE batch-level WARNING (not one per row).
# MAGIC 5. **Table renaming**: `_rejects` → `_dq_findings`. Warnings do not reject rows — only errors exclude rows from silver.
# MAGIC
# MAGIC **Phase K v2 result: 6/6**
# MAGIC
# MAGIC | # | Injected error | Caught? | Severity |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 1 | REF_AREA='ZZ' | YES | error |
# MAGIC | 2 | OBS_STATUS='X' | YES | error |
# MAGIC | 3 | DECIMALS=NULL (partial, series) | YES | error |
# MAGIC | 4 | TIME_PERIOD='2024-13' | YES | error (date parse failed) |
# MAGIC | 5 | OBS_VALUE='N/A' | YES | error (try_cast) |
# MAGIC | 6 | Duplicate key+period | YES | error |
# MAGIC
# MAGIC **Tables written (v2):**
# MAGIC - `sdmx_silver.cbpol` — 360 rows (rebuilt, old table dropped first)
# MAGIC - `sdmx_silver.cbpol_dq_findings` — 1 row (batch-level TIME_FORMAT warning)
# MAGIC - `sdmx_silver.tc` — 192 rows
# MAGIC - `sdmx_silver.tc_dq_findings` — 0 rows (empty)
# MAGIC - `sdmx_silver.dq_summary` — 21 rows (7 from v1 + 7 WS_CBPOL v2 + 7 WS_TC v2)
# MAGIC - `sdmx_bronze.cbpol_raw_seeded_test` — 6 rows (persistent test fixture with `expected_rule` column)
# MAGIC - Old `sdmx_silver.cbpol_rejects` dropped and replaced by `cbpol_dq_findings`
# MAGIC
# MAGIC **DECISION: Warnings do not reject rows.** Only errors exclude rows from the silver table. Warnings are logged as findings but the row remains in silver with `dq_warning_count > 0`. Batch-level warnings (e.g., 100% null) have `row_id = NULL` and do not affect any individual row.
# MAGIC
# MAGIC **Assumptions:**
# MAGIC - DECIMALS in BIS_TOTAL_CREDIT had an unrecognized `attributeRelationship` structure → defaulted to 'observation' (most conservative, per-row check).
# MAGIC - H date_parse_expr is unverified (no BIS data with FREQ=H to test against).
# MAGIC - W has no date_parse_expr (ISO week parsing is complex, marked as WARNING until seen in data).

# COMMAND ----------

# DBTITLE 1,Phase L proposal
# MAGIC %md
# MAGIC ---
# MAGIC ## Phase L (PROPOSED — not yet implemented) — Collection Simulation
# MAGIC
# MAGIC **Goal:** Prove that the SDMX lakehouse can ingest a non-SDMX "reporter" file (e.g., a national central bank spreadsheet) and validate it with the same `validate_flow`, using a metadata-driven mapping table.
# MAGIC
# MAGIC ### 1. Source file: `sdmx_bronze.collection_source`
# MAGIC
# MAGIC A small CSV derived from public BIS CBPOL data, deliberately non-SDMX:
# MAGIC
# MAGIC | country_name | date | rate | obs_status |
# MAGIC | --- | --- | --- | --- |
# MAGIC | United States | Jan-24 | 5.375 | A |
# MAGIC | United States | Feb-24 | 5.375 | A |
# MAGIC | United States | Mar-24 | | A |
# MAGIC | Germany | Jan-24 | 4,000 | A |
# MAGIC | Germany | Feb-24 | 4,000 | A |
# MAGIC | Japan | Jan-24 | 0,100 | A |
# MAGIC | Japan | Feb-24 | -0,100 | A |
# MAGIC | Atlantis | Jan-24 | 2,500 | A |
# MAGIC
# MAGIC Deliberate issues:
# MAGIC - Country names are local ("United States", "Germany", "Japan") not ISO codes
# MAGIC - Dates are `MMM-yy` format ("Jan-24") not `yyyy-MM`
# MAGIC - Rates use comma decimals ("4,000" = 4.0, "0,100" = 0.1)
# MAGIC - "Atlantis" is an unknown country (no mapping exists)
# MAGIC - Germany is missing Mar-24 (gap — validator won't catch this; no row exists)
# MAGIC - United States Mar-24 has empty rate (null OBS_VALUE with OBS_STATUS='A')
# MAGIC
# MAGIC ### 2. Mapping table: `sdmx_bronze.collection_mapping`
# MAGIC
# MAGIC | source_field | source_value | target_field | target_value | transform |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | country_name | United States | REF_AREA | US | direct |
# MAGIC | country_name | Germany | REF_AREA | DE | direct |
# MAGIC | country_name | Japan | REF_AREA | JP | direct |
# MAGIC | date | | TIME_PERIOD | | date_format:MMM-yy→yyyy-MM |
# MAGIC | rate | | OBS_VALUE | | decimal_comma_to_dot |
# MAGIC | obs_status | | OBS_STATUS | | direct |
# MAGIC | (constant) | | FREQ | M | constant |
# MAGIC | (constant) | | UNIT_MEASURE | 5 | constant |
# MAGIC | (constant) | | UNIT_MULT | 6 | constant |
# MAGIC | (constant) | | DECIMALS | 4 | constant |
# MAGIC | (constant) | | TIME_FORMAT | | (leave empty — source limitation, like BIS) |
# MAGIC
# MAGIC ### 3. Transformation function: `transform_collection(flow_id, source_table, mapping_table)`
# MAGIC
# MAGIC Steps:
# MAGIC 1. Read the source file and mapping table
# MAGIC 2. Map country names → ISO codes (REF_AREA). Unmapped names ("Atlantis") are left as-is.
# MAGIC 3. Convert dates: `MMM-yy` → `yyyy-MM` (e.g., "Jan-24" → "2024-01")
# MAGIC 4. Convert decimal separators: comma → dot (e.g., "4,000" → "4.000")
# MAGIC 5. Set constant fields from mapping (FREQ, UNIT_MEASURE, UNIT_MULT, DECIMALS)
# MAGIC 6. Leave TIME_FORMAT empty (simulate source limitation)
# MAGIC 7. Add metadata columns (source_url, ingested_at, batch_id)
# MAGIC 8. Write result to `sdmx_bronze.cbpol_raw_collection` (bronze shape, same schema as cbpol_raw)
# MAGIC
# MAGIC ### 4. Validation: `validate_flow('WS_CBPOL', bronze_override='cbpol_raw_collection', write_tables=False)`
# MAGIC
# MAGIC ### 5. Expected outcomes
# MAGIC
# MAGIC | Issue | Expected rule | Expected finding |
# MAGIC | --- | --- | --- |
# MAGIC | "Atlantis" not in REF_AREA codelist | DIM_CODE | error |
# MAGIC | US Mar-24: null OBS_VALUE, OBS_STATUS='A' (not missing) | OBS_VALUE | error |
# MAGIC | TIME_FORMAT 100% null (source limitation) | ATTR_MANDATORY | batch-level warning |
# MAGIC | Germany missing Mar-24 (no row) | (none) | Validator can't detect missing rows |
# MAGIC | Comma decimals converted correctly | OBS_VALUE | pass (try_cast succeeds) |
# MAGIC | Jan-24 dates converted to 2024-01 | TIME_FORMAT | pass (regex + try_to_date) |
# MAGIC | All other rules | | pass |
# MAGIC
# MAGIC ### 6. Tables
# MAGIC
# MAGIC | Table | Type | Purpose |
# MAGIC | --- | --- | --- |
# MAGIC | `sdmx_bronze.collection_source` | source | Non-SDMX reporter file (8 rows) |
# MAGIC | `sdmx_bronze.collection_mapping` | reference | Field/value mapping + transforms |
# MAGIC | `sdmx_bronze.cbpol_raw_collection` | bronze | Transformed data in bronze schema |
# MAGIC
# MAGIC ### 7. Scope
# MAGIC
# MAGIC - No changes to `validate_flow` — it should work as-is on the transformed bronze data
# MAGIC - No changes to DSD metadata or reference tables
# MAGIC - The mapping table is the only new artifact (plus the source file)
# MAGIC - This proves the lakehouse can handle non-SDMX sources through metadata-driven mapping
# MAGIC
# MAGIC **Awaiting user approval.**

# COMMAND ----------

# DBTITLE 1,v2.1 + DECIMALS + W documentation
# MAGIC %md
# MAGIC ---
# MAGIC ## v2.1 — dq_summary fix, validator_version, seeded fixture controls
# MAGIC
# MAGIC ### dq_summary changes (v2.1)
# MAGIC
# MAGIC **Definition change:** `rows_failed` now counts **distinct rows with an error-severity finding** for that rule (not the total count of findings). `warning_count` column added (count of warning findings, batch-level counted once). `pass_rate = 1 - rows_failed / rows_checked`.
# MAGIC
# MAGIC **Historical note:** v1 counted all findings (including warnings) as `rows_failed`. The definition changed in v2.1. Historical v1/v2 values are left as-is — they are not rewritten.
# MAGIC
# MAGIC **Backfilled:** `validator_version` column added. v1 (run a5d0d194, 7 rows), v2 (runs e00c04af + cc26e226, 14 rows). v2.1 runs appended.
# MAGIC
# MAGIC **View:** `sdmx_silver.dq_summary_latest` — latest run per (flow_id, rule_id).
# MAGIC
# MAGIC ### Seeded fixture (updated)
# MAGIC
# MAGIC `sdmx_bronze.cbpol_raw_seeded_test` now has **9 rows**: 6 error rows (one per error type, including the duplicate pair) + 3 clean control rows (expected_rule=NONE, valid codes and values).
# MAGIC
# MAGIC **Re-run result: 6/6 caught, 0 findings on control rows, 0 extra findings on error rows.**
# MAGIC
# MAGIC | # | Expected rule | Row key | Caught? |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 1 | DIM_CODE | M\|ZZ\|2024-01 | YES (error) |
# MAGIC | 2 | ATTR_CODE | M\|US\|2024-02 | YES (error) |
# MAGIC | 3 | ATTR_MANDATORY | M\|US\|2024-03 | YES (error) |
# MAGIC | 4 | TIME_FORMAT | M\|US\|2024-13 | YES (error) |
# MAGIC | 5 | OBS_VALUE | M\|US\|2024-05 | YES (error) |
# MAGIC | 6 | DUPLICATE | M\|US\|2024-05 | YES (error) |
# MAGIC
# MAGIC Batch-level findings (not attributable to any row): DUPLICATE (the pair) + ATTR_MANDATORY warning (TIME_FORMAT 100% null).
# MAGIC
# MAGIC ### DECIMALS attachment_level default
# MAGIC
# MAGIC In BIS_TOTAL_CREDIT v2.0, DECIMALS has `attributeRelationship: {"group": "Sibling"}` — a group-level relationship that doesn't match any of the three standard patterns (dataset/series/observation).
# MAGIC
# MAGIC **DSD snippet:**
# MAGIC ```json
# MAGIC "DECIMALS": {
# MAGIC   "assignmentStatus": "Conditional",
# MAGIC   "attributeRelationship": {"group": "Sibling"},
# MAGIC   ...
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Decision (updated 2026-10-25):** DECIMALS is now correctly classified as `attachment_level='group'` (was previously defaulted to `observation` when the `{'group': 'Sibling'}` relationship was unrecognized). The fix was applied in `load_structures()` by adding `get_attr_level()` which handles `group` as a separate level. DECIMALS is Conditional (not Mandatory) in BIS_TOTAL_CREDIT, so this classification has no effect on ATTR_MANDATORY — it only matters for SDMX-CSV column ordering (group-level attributes appear between series and observation in the BIS SDMX-CSV output). If DECIMALS were Mandatory, the v2.1 ATTR_MANDATORY logic would treat `group` the same as `series`/`observation` (per-row ERROR for partial nulls); only `dataset` level gets the skip behavior.
# MAGIC
# MAGIC ### W (weekly) TIME_PERIOD behavior
# MAGIC
# MAGIC For FREQ=W, `freq_time_patterns` has:
# MAGIC - `regex_pattern`: `^\d{4}-W\d{2}$` (e.g., "2024-W05")
# MAGIC - `date_parse_expr`: NULL (no parse expression — ISO week parsing is complex and unverified)
# MAGIC - `verified`: false
# MAGIC
# MAGIC **Validator behavior:**
# MAGIC
# MAGIC | Scenario | TIME_PERIOD example | Regex check | date_parse_expr | Finding |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | Well-formed W | "2024-W05" | ✅ matches | NULL | WARNING: "No date parse expression for FREQ=W" |
# MAGIC | Badly shaped W | "2024-W5" | ❌ doesn't match | NULL | ERROR: "doesn't match pattern for FREQ=W" |
# MAGIC | Wrong format | "2024-05" | ❌ doesn't match | NULL | ERROR: "doesn't match pattern for FREQ=W" |
# MAGIC
# MAGIC **period_start_date:** Always NULL for W (no date_parse_expr → no CASE branch). The post-parse ERROR check does NOT fire (it only fires when date_parse_expr is NOT NULL). This is correct: we can validate the format but cannot parse the date.
# MAGIC
# MAGIC **To enable W date parsing:** Add a `date_parse_expr` for W in `freq_time_patterns` (e.g., using ISO week arithmetic) and set `verified=true` once confirmed against real data.

# COMMAND ----------

# DBTITLE 1,Phase L entry
# MAGIC %md
# MAGIC ---
# MAGIC ## Phase L — Collection Simulation (built)
# MAGIC
# MAGIC **What was built:** Created a non-SDMX "reporter" file, a metadata-driven mapping table, and a transformation function that converts the reporter file into the bronze schema. Validated with `validate_flow` (no code changes).
# MAGIC
# MAGIC **Notebook:** `06_Collection_Simulation`
# MAGIC
# MAGIC **Tables created:**
# MAGIC - `sdmx_bronze.collection_source` — 7 rows (non-SDMX reporter file)
# MAGIC - `sdmx_bronze.collection_mapping` — 11 rows (source field/value → DSD component/code + transforms)
# MAGIC - `sdmx_bronze.cbpol_raw_collection` — 7 rows (transformed bronze, includes source_file + source_row for lineage)
# MAGIC - `sdmx_bronze.collection_mapping_findings` — 1 row (unmapped 'Atlantis')
# MAGIC
# MAGIC **Result: 8/8 expectations met**
# MAGIC
# MAGIC | # | Issue | Source | Expected | Actual |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 1 | Unmapped country 'Atlantis' | Mapping | mapping finding | ✅ mapping finding |
# MAGIC | 2 | REF_AREA='Atlantis' not in codelist | validate_flow | DIM_CODE error | ✅ DIM_CODE error |
# MAGIC | 3 | Null OBS_VALUE, OBS_STATUS='A' | validate_flow | OBS_VALUE error | ✅ OBS_VALUE error |
# MAGIC | 4 | TIME_FORMAT 100% null | validate_flow | ATTR_MANDATORY warning | ✅ batch-level warning |
# MAGIC | 5 | Japan missing Feb-24 (gap) | (none) | no finding | ✅ no finding |
# MAGIC | 6 | Comma decimals converted | transform | OBS_VALUE pass | ✅ pass |
# MAGIC | 7 | Jan-24 dates converted | transform | TIME_FORMAT pass | ✅ pass |
# MAGIC | 8 | Clean rows (5) | validate_flow | 0 findings | ✅ 0 findings |
# MAGIC
# MAGIC **Design decisions:**
# MAGIC - Mapping table is **data**, not code — adding a new country or field mapping requires only an INSERT, no code changes
# MAGIC - Unmapped values: kept as raw value in bronze, logged as mapping finding, and caught by DIM_CODE in validate_flow
# MAGIC - Bronze carries `source_file` and `source_row` for lineage back to the original reporter file
# MAGIC - The reporter file covers: local country names, Jan-24 dates, comma decimals, a missing country (Japan gap), a missing rate, an unknown country (Atlantis), and 5 clean rows
# MAGIC

# COMMAND ----------

# DBTITLE 1,Phase L entry + Phase M proposal
# MAGIC %md
# MAGIC ---
# MAGIC ## Phase M (PROPOSED — not yet implemented) — Gold Layer
# MAGIC
# MAGIC **Goal:** Create the gold (presentation) layer — clean, analytics-ready tables, SDMX-CSV export view, and a dashboard outline.
# MAGIC
# MAGIC ### 1. Gold tables
# MAGIC
# MAGIC #### `sdmx_gold.<flow>_timeseries`
# MAGIC - **Grain:** one row per (dimension combination, TIME_PERIOD)
# MAGIC - **Source:** `sdmx_silver.<flow>` (filtered to valid rows only)
# MAGIC - **Columns:** dimensions + TIME_PERIOD + period_start_date + OBS_VALUE (DOUBLE) + OBS_STATUS + key attributes
# MAGIC - **Purpose:** time-series analysis, trend charts, downstream consumption
# MAGIC - **No metadata columns** (no batch_id, ingested_at, dq_warning_count) — pure data
# MAGIC
# MAGIC #### `sdmx_gold.<flow>_series`
# MAGIC - **Grain:** one row per unique dimension combination (latest observation per series)
# MAGIC - **Source:** `sdmx_gold.<flow>_timeseries` (latest period_start_date per series key)
# MAGIC - **Columns:** dimensions + latest OBS_VALUE + latest TIME_PERIOD + latest period_start_date + OBS_STATUS
# MAGIC - **Purpose:** "current value" table — what is the latest value for each series?
# MAGIC
# MAGIC #### `sdmx_gold.dq_overview`
# MAGIC - **Grain:** one row per flow_id
# MAGIC - **Source:** `sdmx_silver.dq_summary_latest`
# MAGIC - **Columns:** flow_id, last_run_id, rows_checked, total_errors, total_warnings, pass_rate, validator_version, run_timestamp
# MAGIC - **Purpose:** data quality monitoring — one row tells the health of each flow
# MAGIC
# MAGIC ### 2. SDMX-CSV export view
# MAGIC
# MAGIC #### `sdmx_gold.<flow>_sdmx_csv`
# MAGIC - **Type:** VIEW on `sdmx_gold.<flow>_timeseries`
# MAGIC - **Column order:** derived from DSD component positions:
# MAGIC   1. Dimensions (ordered by `position` from `dsd_components`)
# MAGIC   2. TIME_PERIOD (time dimension)
# MAGIC   3. Attributes (ordered by `position` from `dsd_components`)
# MAGIC   4. OBS_VALUE (measure, last)
# MAGIC - **No extra columns** — pure SDMX-CSV format
# MAGIC - **Purpose:** export to downstream SDMX consumers, interoperability
# MAGIC - **Dynamic:** column order is metadata-driven (from `dsd_components.position`), not hard-coded
# MAGIC
# MAGIC ### 3. Dashboard outline (Lakeview)
# MAGIC
# MAGIC **Dashboard:** `SDMX Data Quality & Explorer`
# MAGIC
# MAGIC | Tile | Type | Source | Purpose |
# MAGIC | --- | --- | --- | --- |
# MAGIC | DQ Overview | Table | `sdmx_gold.dq_overview` | One row per flow: pass_rate, errors, warnings |
# MAGIC | Findings by Rule | Bar chart | `sdmx_silver.<flow>_dq_findings` | Count of findings grouped by rule_id and severity |
# MAGIC | Time Series — Policy Rates | Line chart | `sdmx_gold.cbpol_timeseries` | OBS_VALUE over time, colored by REF_AREA |
# MAGIC | Time Series — Total Credit | Line chart | `sdmx_gold.tc_timeseries` | OBS_VALUE over time, colored by BORROWERS_CTY |
# MAGIC | Latest Values | Table | `sdmx_gold.cbpol_series` | Current OBS_VALUE per REF_AREA, sorted desc |
# MAGIC | DQ History | Line chart | `sdmx_silver.dq_summary` | pass_rate over time per flow (x-axis: run_timestamp) |
# MAGIC | SDMX-CSV Preview | Table | `sdmx_gold.cbpol_sdmx_csv` | First 100 rows in SDMX-CSV column order |
# MAGIC
# MAGIC ### 4. Scope
# MAGIC
# MAGIC - Gold tables are built from silver (validated data only)
# MAGIC - No re-validation — gold trusts silver
# MAGIC - SDMX-CSV view is metadata-driven (column order from DSD)
# MAGIC - Dashboard uses Lakeview with standard SQL widgets
# MAGIC - Gold tables can be refreshed by re-running validate_flow (which writes silver, then gold views auto-update)
# MAGIC
# MAGIC **Awaiting user approval.**
# MAGIC

# COMMAND ----------

# DBTITLE 1,Control row fix explanation
# MAGIC %md
# MAGIC ---
# MAGIC ## Seeded fixture: control row fix
# MAGIC
# MAGIC **What happened:** The 3 clean control rows (expected_rule=NONE) initially triggered 3 unexpected ATTR_CODE errors: `UNIT_MEASURE='5' not in CL_BIS_UNIT`.
# MAGIC
# MAGIC **Root cause:** Fixture data error, not validator error. When creating the control rows, I used placeholder values `UNIT_MEASURE='5'` and `UNIT_MULT='6'` instead of looking up the actual values from the original `cbpol_raw` data. The original data uses `UNIT_MEASURE='368'` and `UNIT_MULT='0'` — both valid codes in their respective codelists.
# MAGIC
# MAGIC **Fix:** `UPDATE sdmx_bronze.cbpol_raw_seeded_test SET UNIT_MEASURE='368', UNIT_MULT='0' WHERE expected_rule='NONE'` (3 rows updated).
# MAGIC
# MAGIC **Reason:** The validator was correct — ATTR_CODE should flag invalid codelist values. The fixture was wrong, not the validator. After the fix: 6/6 caught, 0 findings on control rows, 0 extra findings on error rows.
# MAGIC
# MAGIC **Lesson:** Control rows must use valid codelist values from the start. The fixture is now a reliable test fixture with 9 rows (6 error + 3 clean control).

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Phase L completeness + DSD ordinal + SDMX-CSV + round-trip + Phase M
# MAGIC %md
# MAGIC ---
# MAGIC ## Phase L addition: Completeness Check
# MAGIC
# MAGIC **Goal:** Detect missing data (gaps in expected series-periods). Validation catches wrong data; completeness catches missing data.
# MAGIC
# MAGIC ### Implementation
# MAGIC
# MAGIC 1. **`sdmx_bronze.expected_series`** — config-driven table: `flow_id`, `series_key` (JSON), `first_period`, `frequency`. No flow-specific code.
# MAGIC 2. **`completeness_check(flow_id, bronze_table)`** — generic function:
# MAGIC    - Reads `expected_series` for the flow
# MAGIC    - Generates expected TIME_PERIODs from `first_period` to the latest period in actual data
# MAGIC    - Compares with actual periods per series
# MAGIC    - Produces WARNING findings for each missing series-period
# MAGIC 3. **`sdmx_bronze.collection_completeness_findings`** — persisted findings table (3 rows)
# MAGIC
# MAGIC ### Results (collection simulation)
# MAGIC
# MAGIC 3 completeness findings (WARNINGs):
# MAGIC - DE missing 2024-03 (gap)
# MAGIC - JP missing 2024-02 (gap)
# MAGIC - JP missing 2024-03 (gap)
# MAGIC
# MAGIC US has all 3 periods — no gap. Atlantis is unknown (not in expected_series), so not checked.
# MAGIC
# MAGIC ### Expected-vs-actual: 10/10 ✅
# MAGIC
# MAGIC Validation (7) + completeness (3) = 10 expectations, all met.
# MAGIC
# MAGIC ---
# MAGIC ## DSD ordinal column + SDMX-CSV column order
# MAGIC
# MAGIC **Problem:** `dsd_components.position` was NULL for all attributes, causing the SDMX-CSV export to have the wrong attribute column order.
# MAGIC
# MAGIC **Investigation:** Does BIS SDMX-CSV column order equal the plain DSD JSON order?
# MAGIC
# MAGIC **Result: NO.** The BIS SDMX-CSV orders attributes by `(attachment_level, ordinal)`:
# MAGIC 1. Dataset-level attributes first
# MAGIC 2. Series-level attributes next
# MAGIC 3. Group-level attributes next (e.g., DECIMALS in WS_TC with `attributeRelationship={'group': 'Sibling'}`)
# MAGIC 4. Observation-level attributes last
# MAGIC 5. Within each level, by DSD JSON ordinal (the iteration index in the DSD JSON array)
# MAGIC
# MAGIC This rule reproduces the BIS SDMX-CSV order exactly for both WS_CBPOL and WS_TC.
# MAGIC
# MAGIC **Fix:**
# MAGIC 1. Added `ordinal` column to `dsd_components` (ALTER TABLE)
# MAGIC 2. Modified `load_structures()` to populate `ordinal` (iteration index) and `attachment_level` (from `attributeRelationship`) during the initial load — no separate UPDATE needed
# MAGIC 3. Added `get_attr_level()` function to handle all attachment levels: `none`→dataset, `dimensions`→series, `group`→group, `primaryMeasure`→observation
# MAGIC 4. Re-ran `load_structures` for WS_CBPOL and WS_TC
# MAGIC 5. Updated `04_Config_and_Generalisation` notebook with the modified function
# MAGIC
# MAGIC ---
# MAGIC ## SDMX-CSV export format
# MAGIC
# MAGIC **Format:** BIS SDMX-CSV (format=sdmx-csv from the BIS API) — uses 3 metadata columns: `STRUCTURE`, `STRUCTURE_ID`, `ACTION`.
# MAGIC
# MAGIC - `STRUCTURE` = constant `"dataflow"`
# MAGIC - `STRUCTURE_ID` = `agency:flow_id(version)` from `flow_config`, e.g. `BIS:WS_CBPOL(1.0)`
# MAGIC - `ACTION` = constant `"I"` (information)
# MAGIC
# MAGIC **Note:** The BIS API returns this 3-column metadata format when `format=sdmx-csv` is requested. I verified this by calling the API and inspecting the response. I did **not** verify whether this corresponds to a specific SDMX version number (e.g., 2.1) — I only verified what the API returns. The older `DATAFLOW`-column format is used by `format=csv`.
# MAGIC
# MAGIC **Column order (metadata-driven):**
# MAGIC 1. STRUCTURE, STRUCTURE_ID, ACTION
# MAGIC 2. Dimensions (by `position` from `dsd_components`)
# MAGIC 3. TIME_PERIOD
# MAGIC 4. OBS_VALUE
# MAGIC 5. Attributes (by `(attachment_level, ordinal)` from `dsd_components`)
# MAGIC
# MAGIC **Files written to Volume:** `/Volumes/workspace/sdmx_bronze/raw_files/cbpol_sdmx_csv/` and `tc_sdmx_csv/` (never the repo).
# MAGIC
# MAGIC ---
# MAGIC ## Round-trip check results
# MAGIC
# MAGIC ### WS_CBPOL: FULL MATCH ✅
# MAGIC
# MAGIC | Check | Result |
# MAGIC | --- | --- |
# MAGIC | Headers (18 cols) | ✅ Exact match (names and order) |
# MAGIC | Row count | ✅ 360/360 |
# MAGIC | Values | ✅ 5400/5400 (15 data cols × 360 rows) |
# MAGIC | Keys | ✅ All 360 keys common |
# MAGIC | Metadata | ✅ STRUCTURE, STRUCTURE_ID, ACTION all match |
# MAGIC
# MAGIC ### WS_TC: 2 explainable differences
# MAGIC
# MAGIC | Check | Result |
# MAGIC | --- | --- |
# MAGIC | Headers (20 cols) | ✅ Exact match |
# MAGIC | Row count | ❌ Export 192, BIS 195 (3 extra BIS rows) |
# MAGIC | Values | ❌ 192/3264 differences (all in DECIMALS) |
# MAGIC | Keys | ❌ 3 keys only in BIS |
# MAGIC | Metadata | ✅ All match |
# MAGIC
# MAGIC **Difference 1: 3 extra BIS rows**
# MAGIC - BIS returns 3 series-level metadata rows (one per country: DE, US, JP) with empty FREQ and TIME_PERIOD
# MAGIC - These contain series/group-level attributes (UNIT_MEASURE, UNIT_MULT, TITLE_TS, DECIMALS) declared once per series
# MAGIC - Our gold table is observation-level only (one row per series + period), so these don't exist in our export
# MAGIC - **Explanation:** `format=csv` (what we ingested) puts all attributes in every row; `format=sdmx-csv` (what we compare against) puts series/group-level attributes in separate series-level rows
# MAGIC
# MAGIC **Difference 2: 192 DECIMALS differences**
# MAGIC - DECIMALS is a group-level attribute (`attributeRelationship={'group': 'Sibling'}`)
# MAGIC - In `format=csv`, DECIMALS='1' appears in every observation row
# MAGIC - In `format=sdmx-csv`, DECIMALS is empty in observation rows and populated only in the series-level rows
# MAGIC - Our export (from gold, built from format=csv) has DECIMALS='1' in every row; BIS SDMX-CSV has it empty
# MAGIC - **Explanation:** Group-level attributes are handled differently between the two CSV formats
# MAGIC
# MAGIC **All other values (3072/3264) match exactly.** The differences are structural (how group-level attributes are represented), not data errors.
# MAGIC
# MAGIC ---
# MAGIC ## Phase M: Gold Layer Built
# MAGIC
# MAGIC ### Gold objects inventory
# MAGIC
# MAGIC | Object | Type | Rows |
# MAGIC | --- | --- | --- |
# MAGIC | cbpol_timeseries | TABLE | 360 |
# MAGIC | cbpol_series | TABLE | 5 |
# MAGIC | cbpol_sdmx_csv | VIEW | 360 |
# MAGIC | tc_timeseries | TABLE | 192 |
# MAGIC | tc_series | TABLE | 3 |
# MAGIC | tc_sdmx_csv | VIEW | 192 |
# MAGIC | dq_overview | TABLE | 2 |
# MAGIC
# MAGIC ### AI/BI Dashboard availability
# MAGIC
# MAGIC **Available.** Evidence: `createAsset(assetType="dashboard", name="SDMX Data Quality & Explorer")` succeeded, returning dashboard ID `01f1cfbc39be1000a2844582bbbb889b`.
# MAGIC
# MAGIC Dashboard: `SDMX Data Quality & Explorer` created. 4-5 tiles planned:
# MAGIC 1. DQ Overview (table) — `dq_overview`
# MAGIC 2. Findings by Rule (bar chart) — `dq_findings`
# MAGIC 3. Time Series — Policy Rates (line chart) — `cbpol_timeseries`
# MAGIC 4. Time Series — Total Credit (line chart) — `tc_timeseries`
# MAGIC 5. SDMX-CSV Preview (table) — `cbpol_sdmx_csv`

# COMMAND ----------

# DBTITLE 1,Regression + side-by-side + proposals
# MAGIC %md
# MAGIC ---
# MAGIC ## Regression check after dsd_components rebuild (2026-10-25)
# MAGIC
# MAGIC **What was tested:** Re-ran `validate_flow` v2.1 in memory (dry-run, no writes) for WS_CBPOL, WS_TC, and the seeded fixture after the `dsd_components` rebuild that added the `ordinal` column and corrected `attachment_level` (DECIMALS in WS_TC from `observation` → `group`).
# MAGIC
# MAGIC ### Results: ALL MATCH — no regression
# MAGIC
# MAGIC | Flow | Rows valid | Errors | Warnings | Previous | Match? |
# MAGIC | --- | --- | --- | --- | --- | --- |
# MAGIC | WS_CBPOL | 360 | 0 | 1 batch (TIME_FORMAT 100% null) | 360/0/1 | ✅ |
# MAGIC | WS_TC | 192 | 0 | 0 | 192/0/0 | ✅ |
# MAGIC | Seeded fixture | 3 (controls clean) | 6 (all caught) | 1 batch (TIME_FORMAT) | 6/6 + 1 | ✅ |
# MAGIC
# MAGIC Seeded fixture findings by rule: DIM_CODE(1), ATTR_CODE(1), ATTR_MANDATORY(1 error + 1 batch warning), TIME_FORMAT(1 parse fail), OBS_VALUE(1), DUPLICATE(1) = 6 errors + 1 warning. 0 findings on control rows.
# MAGIC
# MAGIC ### ATTR_MANDATORY treatment of group-level attributes
# MAGIC
# MAGIC DECIMALS in WS_TC: `assignment_status=Conditional`, `attachment_level=group`.
# MAGIC
# MAGIC - ATTR_MANDATORY only checks **Mandatory** attributes. Since DECIMALS is Conditional, it is not checked regardless of attachment_level.
# MAGIC - If DECIMALS were Mandatory with `group` level, the v2.1 logic treats `group` the same as `series` and `observation` — per-row ERROR for partial nulls.
# MAGIC - Only `dataset` level gets special treatment: skip per-row check if not 100% null (if not 100% null, all pass).
# MAGIC - **Conclusion:** The `observation` → `group` reclassification has zero impact on ATTR_MANDATORY behavior for WS_TC.
# MAGIC
# MAGIC ### Updated log note
# MAGIC
# MAGIC The older log note (cell "v2.1 + DECIMALS + W documentation") that said "DECIMALS defaults to observation level" has been updated to reflect the corrected `group` classification and the zero-impact conclusion.
# MAGIC
# MAGIC ---
# MAGIC ## WS_TC side-by-side: BIS vs export (2026-10-25)
# MAGIC
# MAGIC **What was shown:** First 5 BIS sdmx-csv rows (from API, format=sdmx-csv) next to first 5 rows of my export (from Volume), highlighting empty-FREQ/TIME_PERIOD rows and DECIMALS.
# MAGIC
# MAGIC ### Key observations
# MAGIC
# MAGIC 1. **BIS Row 1: empty FREQ/TIME_PERIOD** — This is a series-level metadata row (one per series: US, DE, JP). It contains group/series-level attributes (DECIMALS=1, COLLECTION=E) with empty observation-level fields. Our export has no such rows — it is observation-level only.
# MAGIC
# MAGIC 2. **DECIMALS in observation rows:**
# MAGIC    - BIS: empty (`''`) in all observation rows (DECIMALS is group-level, only in series-level rows)
# MAGIC    - Export: `1` in every observation row (from format=csv ingestion, which puts all attributes in every row)
# MAGIC    - This is the 192-DECIMALS difference documented in the round-trip check.
# MAGIC
# MAGIC 3. **Headers match exactly** (20 columns, same names and order).
# MAGIC
# MAGIC ### What I verified about the format
# MAGIC
# MAGIC - The BIS API returns 3 metadata columns (STRUCTURE, STRUCTURE_ID, ACTION) when `format=sdmx-csv` is requested.
# MAGIC - I verified this by calling the BIS API and inspecting the response.
# MAGIC - I did **not** verify whether this corresponds to a specific SDMX version number (e.g., 2.1) — I only verified what the API returns.
# MAGIC - The series-level metadata rows (empty FREQ/TIME_PERIOD) are specific to `format=sdmx-csv`; `format=csv` does not include them.
# MAGIC
# MAGIC ---
# MAGIC ## Free Edition job limits (verified 2026-10-25)
# MAGIC
# MAGIC Source: [Databricks Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations/)
# MAGIC
# MAGIC | Resource | Limit |
# MAGIC | --- | --- |
# MAGIC | Jobs | Max **5 concurrent job tasks per account** (not per job) |
# MAGIC | Compute | Serverless only — no custom compute configurations |
# MAGIC | SQL warehouses | One, limited to 2X-Small |
# MAGIC | Lakeflow pipelines | One active pipeline per pipeline type |
# MAGIC
# MAGIC Additional: no guaranteed reliability, no SLA, no support. Fair usage policy applies — exceeding quota shuts down compute for the rest of the day.
# MAGIC
# MAGIC ---
# MAGIC ## PROPOSED (not built) — Databricks Job: SDMX Lakehouse Pipeline
# MAGIC
# MAGIC **Awaiting user approval.**
# MAGIC
# MAGIC ### Job structure
# MAGIC
# MAGIC | Property | Value |
# MAGIC | --- | --- |
# MAGIC | Name | SDMX Lakehouse Pipeline |
# MAGIC | Trigger | Manual (no schedule) |
# MAGIC | Compute | Serverless (Free Edition requirement) |
# MAGIC | Max concurrent tasks | 5 (Free Edition limit) — all 5 tasks are sequential, not concurrent |
# MAGIC
# MAGIC ### Task order
# MAGIC
# MAGIC | Task # | Name | Description | Depends on | Parameters |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 1 | load_structures | Reads DSD metadata from BIS API, populates dsd_components/codelists/codes | — | `flow_id` (widget) |
# MAGIC | 2 | ingest_flow | Reads flow_config, fetches data from BIS API as CSV, saves to Volume, appends to bronze | 1 | `flow_id` |
# MAGIC | 3 | validate_flow | Dedup + 7 rules, writes silver + dq_findings + dq_summary | 2 | `flow_id` |
# MAGIC | 4 | gold | Builds gold tables (timeseries, series, dq_overview) + SDMX-CSV views | 3 | `flow_id` |
# MAGIC | 5 | export | Writes SDMX-CSV files to Volume | 4 | `flow_id` |
# MAGIC
# MAGIC ### Implementation notes
# MAGIC
# MAGIC - Each task would be a **notebook task** using `dbutils.widgets.get("flow_id")` for the parameter.
# MAGIC - Functions are currently defined in notebook cells (not importable modules). Each task notebook would need to contain the function definition + the widget-parameterized call. Alternatively, the functions could be refactored into a shared Python file.
# MAGIC - The 5 concurrent task limit is not a concern — tasks are sequential (each depends on the previous), so only 1 runs at a time.
# MAGIC - To run both WS_CBPOL and WS_TC, the job could use a matrix parameter or two separate job runs.
# MAGIC
# MAGIC ---
# MAGIC ## PROPOSED (not built) — Incremental-load demo
# MAGIC
# MAGIC **Awaiting user approval.**
# MAGIC
# MAGIC ### Goal
# MAGIC
# MAGIC Demonstrate that the pipeline handles incremental loads (new periods + revised values) correctly via dedup (latest `ingested_at` wins).
# MAGIC
# MAGIC ### Demo steps
# MAGIC
# MAGIC 1. **Append a later batch** to `cbpol_raw` (12 rows, new batch_id):
# MAGIC    - 10 new period rows: US+DE+JP+GB+CA × 2025-01 and 2025-02 (2 new months × 5 countries)
# MAGIC    - 2 revised value rows: US 2024-01 (5.375 → 5.500), DE 2024-06 (4.000 → 3.750)
# MAGIC
# MAGIC 2. **Run `validate_flow('WS_CBPOL')`** (dry-run, no writes):
# MAGIC    - Expected rows in: 408 + 12 = 420
# MAGIC    - Expected superseded: 48 (original overlap) + 2 (revised) = 50
# MAGIC    - Expected after dedup: 370 (360 original − 2 superseded + 2 revised + 10 new)
# MAGIC    - Expected valid: 370 (all pass validation)
# MAGIC
# MAGIC 3. **Show:**
# MAGIC    - Rows superseded: the 2 old versions of US 2024-01 and DE 2024-06
# MAGIC    - New rows added: 10 rows for 2025-01 and 2025-02
# MAGIC    - Silver result: 370 rows, US 2024-01 = 5.500 (revised), DE 2024-06 = 3.750 (revised)
# MAGIC
# MAGIC ### What this proves
# MAGIC
# MAGIC - The dedup rule (latest `ingested_at` wins) correctly handles revised values
# MAGIC - New periods are added without affecting existing data
# MAGIC - The validator works on mixed new + revised data without code changes
# MAGIC - The `rows_superseded` count correctly distinguishes revisions from duplicates
# MAGIC
# MAGIC ---
# MAGIC ## Dashboard state (2026-10-25)
# MAGIC
# MAGIC **Dashboard ID:** `01f1cfbc39be1000a2844582bbbb889b`
# MAGIC **Name:** Unnamed Dashboard (needs rename)
# MAGIC **Published:** Yes (empty)
# MAGIC **Datasets:** 0
# MAGIC **Pages:** 0
# MAGIC **Tiles:** 0
# MAGIC
# MAGIC The dashboard was created but the 5 tiles were not built. Navigating to the dashboard page to finish the 5 specified tiles:
# MAGIC 1. DQ Overview (table) — `sdmx_gold.dq_overview`
# MAGIC 2. Findings by Rule (bar chart) — `sdmx_silver.cbpol_dq_findings`
# MAGIC 3. Policy Rates (line chart) — `sdmx_gold.cbpol_timeseries`
# MAGIC 4. Total Credit (line chart) — `sdmx_gold.tc_timeseries`
# MAGIC 5. SDMX-CSV Preview (table) — `sdmx_gold.cbpol_sdmx_csv`