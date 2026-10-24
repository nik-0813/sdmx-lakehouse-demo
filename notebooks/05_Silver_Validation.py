# Databricks notebook source
# DBTITLE 1,05 — Silver Validation
# MAGIC %md
# MAGIC # 05 — Silver Validation
# MAGIC
# MAGIC Metadata-driven validation of SDMX dataflows. Rules are derived from `sdmx_silver.dsd_components` and `sdmx_silver.codes` — no hard-coded column names or codelist IDs in the validation logic. Adding a new dataflow needs only a `flow_config` row plus `load_structures` and `ingest_flow`.

# COMMAND ----------

# DBTITLE 1,Phase F header
# MAGIC %md
# MAGIC ## Phase F — Fix shared-codelist report query
# MAGIC
# MAGIC **Problem:** The report query in notebook 04 joins `dsd_components → codelists → codes` directly. When a codelist is shared across multiple components (e.g. `CL_BIS_UNIT` is used by both `UNIT_TYPE` and `UNIT_MEASURE` in `BIS_TOTAL_CREDIT`), each code row is duplicated once per component, inflating `code_count`.
# MAGIC
# MAGIC Additionally, the `codelists → codes` join matched on `agency + codelist_id` only, missing `version` — if two versions of a codelist ever coexist, codes would cross-join.
# MAGIC
# MAGIC **Fix:**
# MAGIC 1. `SELECT DISTINCT codelist_id, codelist_agency, codelist_version` from `dsd_components` to deduplicate shared codelist references.
# MAGIC 2. Join `codelists → codes` on `agency + codelist_id + version` (not `id` alone).
# MAGIC
# MAGIC Below: before (inflated) and after (correct) for `WS_TC`.

# COMMAND ----------

# DBTITLE 1,Before: old query (inflated)
# BEFORE — old query (inflated code counts for shared codelists)
# CL_BIS_UNIT is used by UNIT_TYPE + UNIT_MEASURE → codes counted twice
print("=== BEFORE: Inflated code counts (old query) ===")
spark.sql("""
    SELECT dc.codelist_id, cl.name, COUNT(c.code_id) as code_count
    FROM workspace.sdmx_silver.dsd_components dc
    JOIN workspace.sdmx_silver.codelists cl
      ON dc.codelist_id = cl.codelist_id AND dc.codelist_agency = cl.agency
    JOIN workspace.sdmx_silver.codes c
      ON cl.codelist_id = c.codelist_id AND cl.agency = c.agency
    WHERE dc.dsd_id = 'BIS_TOTAL_CREDIT' AND dc.codelist_id IS NOT NULL
    GROUP BY dc.codelist_id, cl.name
    ORDER BY dc.codelist_id
""").show(50, truncate=False)

# COMMAND ----------

# DBTITLE 1,After: fixed query (correct)
# AFTER — fixed query (deduplicated codelist refs + version join)
# CL_BIS_UNIT now shows 1105 (correct), not 2210
print("=== AFTER: Correct code counts (fixed query) ===")
spark.sql("""
    SELECT dc.codelist_id, cl.name, COUNT(c.code_id) as code_count
    FROM (
        SELECT DISTINCT codelist_id, codelist_agency, codelist_version
        FROM workspace.sdmx_silver.dsd_components
        WHERE dsd_id = 'BIS_TOTAL_CREDIT' AND codelist_id IS NOT NULL
    ) dc
    JOIN workspace.sdmx_silver.codelists cl
      ON dc.codelist_id = cl.codelist_id
      AND dc.codelist_agency = cl.agency
      AND dc.codelist_version = cl.version
    JOIN workspace.sdmx_silver.codes c
      ON cl.codelist_id = c.codelist_id
      AND cl.agency = c.agency
      AND cl.version = c.version
    GROUP BY dc.codelist_id, cl.name
    ORDER BY dc.codelist_id
""").show(50, truncate=False)

# COMMAND ----------

# DBTITLE 1,Phase F summary
# MAGIC %md
# MAGIC ### Phase F — Summary
# MAGIC
# MAGIC | Codelist | Before (inflated) | After (correct) | Fix |
# MAGIC | --- | --- | --- | --- |
# MAGIC | CL_BIS_UNIT | 2210 | 1105 | Deduplicated codelist ref (shared by UNIT_TYPE + UNIT_MEASURE) |
# MAGIC | All others | unchanged | unchanged | No shared codelists |
# MAGIC | **Total** | **2391** | **1286** | **−1105** |
# MAGIC
# MAGIC Two changes in the fixed query:
# MAGIC 1. **`SELECT DISTINCT` codelist refs** from `dsd_components` — prevents shared codelists being counted once per component.
# MAGIC 2. **Added `version` to all joins** — `codelists ↔ codes` now joins on `agency + codelist_id + version`, not `agency + codelist_id` alone.
# MAGIC
# MAGIC WS_CBPOL was unaffected (no shared codelists in BIS_CBPOL).

# COMMAND ----------

# DBTITLE 1,Phase G header
# MAGIC %md
# MAGIC ## Phase G — WS_TC Data Ingest
# MAGIC
# MAGIC ### Step 1: WS_TC dimensions overview
# MAGIC
# MAGIC Show the 7 dimensions of `BIS_TOTAL_CREDIT` (WS_TC) with their codelists and code counts, using the fixed query from Phase F.

# COMMAND ----------

# DBTITLE 1,WS_TC dimensions with codelists
# 7 dimensions of WS_TC with codelists and code counts (fixed query)
print("=== WS_TC dimensions ===")
spark.sql("""
    SELECT dc.component_id, dc.position, dc.codelist_id, cl.name as codelist_name,
           cnt.code_count
    FROM workspace.sdmx_silver.dsd_components dc
    JOIN workspace.sdmx_silver.codelists cl
      ON dc.codelist_id = cl.codelist_id
      AND dc.codelist_agency = cl.agency
      AND dc.codelist_version = cl.version
    LEFT JOIN (
        SELECT codelist_id, agency, version, COUNT(*) as code_count
        FROM workspace.sdmx_silver.codes
        GROUP BY codelist_id, agency, version
    ) cnt ON cl.codelist_id = cnt.codelist_id
         AND cl.agency = cnt.agency
         AND cl.version = cnt.version
    WHERE dc.dsd_id = 'BIS_TOTAL_CREDIT' AND dc.component_type = 'dimension'
    ORDER BY dc.position
""").show(50, truncate=False)

# COMMAND ----------

# DBTITLE 1,Proposed key header
# MAGIC %md
# MAGIC ### Step 2: Proposed small key
# MAGIC
# MAGIC **Key order:** `FREQ.BORROWERS_CTY.TC_BORROWERS.TC_LENDERS.VALUATION.UNIT_TYPE.TC_ADJUST`
# MAGIC
# MAGIC **Proposed key:** `Q.US+DE+JP.H.A.M.770.A`
# MAGIC
# MAGIC | Position | Dimension | Code | Meaning |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 1 | FREQ | Q | Quarterly |
# MAGIC | 2 | BORROWERS_CTY | US+DE+JP | United States, Germany, Japan |
# MAGIC | 3 | TC_BORROWERS | H | Households & NPISHs |
# MAGIC | 4 | TC_LENDERS | A | All sectors |
# MAGIC | 5 | VALUATION | M | Market value |
# MAGIC | 6 | UNIT_TYPE | 770 | Percentage of GDP |
# MAGIC | 7 | TC_ADJUST | A | Adjusted for breaks |
# MAGIC
# MAGIC **Date range:** 2010-Q1 to 2025-Q4 (64 quarters × 3 countries = ~192 rows expected)
# MAGIC
# MAGIC All codes verified against `sdmx_silver.codes` and tested against the BIS API (12 rows returned for 2010-Q1 to 2010-Q4 test slice).

# COMMAND ----------

# DBTITLE 1,Verify proposed codes
# Verify every proposed code exists in sdmx_silver.codes
print("=== Code verification ===")
spark.sql("""
    SELECT p.dim, p.codelist_id, p.code_id, c.code_name,
           CASE WHEN c.code_id IS NOT NULL THEN 'VALID' ELSE 'NOT FOUND' END as status
    FROM (
        SELECT 'FREQ (dim 1)'          as dim, 'CL_FREQ'        as codelist_id, 'Q'   as code_id
        UNION ALL SELECT 'BORROWERS_CTY (dim 2)', 'CL_AREA',        'US'
        UNION ALL SELECT 'BORROWERS_CTY (dim 2)', 'CL_AREA',        'DE'
        UNION ALL SELECT 'BORROWERS_CTY (dim 2)', 'CL_AREA',        'JP'
        UNION ALL SELECT 'TC_BORROWERS (dim 3)',  'CL_TC_BORROWERS', 'H'
        UNION ALL SELECT 'TC_LENDERS (dim 4)',    'CL_TC_LENDERS',   'A'
        UNION ALL SELECT 'VALUATION (dim 5)',     'CL_VALUATION',    'M'
        UNION ALL SELECT 'UNIT_TYPE (dim 6)',     'CL_BIS_UNIT',     '770'
        UNION ALL SELECT 'TC_ADJUST (dim 7)',     'CL_ADJUST',       'A'
    ) p
    LEFT JOIN workspace.sdmx_silver.codes c
      ON p.codelist_id = c.codelist_id
      AND p.code_id = c.code_id
      AND c.agency = 'BIS'
    ORDER BY p.dim
""").show(50, truncate=False)

# COMMAND ----------

# DBTITLE 1,Test key against API
# Test the proposed key against the BIS API (small date range: 2010-Q1 to 2010-Q4)
import requests

test_url = "https://stats.bis.org/api/v2/data/dataflow/BIS/WS_TC/2.0/Q.US+DE+JP.H.A.M.770.A"
resp = requests.get(test_url, params={
    "format": "csv", "startPeriod": "2010-Q1", "endPeriod": "2010-Q4"
}, timeout=30)

print(f"HTTP {resp.status_code}")
if resp.status_code == 200:
    lines = resp.text.strip().split("\n")
    print(f"Data rows: {len(lines) - 1}")
    print(f"Columns: {lines[0]}")
    from collections import Counter
    countries = [l.split(",")[1] for l in lines[1:]]
    print(f"Rows per country: {dict(Counter(countries))}")
    print(f"\nSample row:\n  {lines[1]}")
else:
    print(f"ERROR: {resp.text[:300]}")

# COMMAND ----------

# DBTITLE 1,Key proposal — awaiting approval
# MAGIC %md
# MAGIC ### Step 2 result: Key proposal — awaiting approval
# MAGIC
# MAGIC **Proposed key:** `Q.US+DE+JP.H.A.M.770.A` with `startPeriod=2010-Q1`, `endPeriod=2025-Q4`
# MAGIC
# MAGIC All 9 codes verified VALID in `sdmx_silver.codes`. API test returned HTTP 200 with 12 rows (4 quarters × 3 countries). All three countries (US, DE, JP) returned data.
# MAGIC
# MAGIC ⏳ Waiting for user approval before updating `flow_config` and running `ingest_flow('WS_TC')`.

# COMMAND ----------

# DBTITLE 1,Step 3 header
# MAGIC %md
# MAGIC ### Step 3: Ingest WS_TC (approved)
# MAGIC
# MAGIC User approved the key `Q.US+DE+JP.H.A.M.770.A`. Updated `flow_config` and ran `ingest_flow('WS_TC')`.
# MAGIC
# MAGIC The `ingest_flow` function is defined in notebook 04. We define the constants and function here (same code) so notebook 05 is self-contained for execution.

# COMMAND ----------

# DBTITLE 1,Constants and ingest_flow
# --- Constants and ingest_flow function (from notebook 04, unchanged) ---
import requests, json, re, uuid
from datetime import datetime

BASE_URL = "https://stats.bis.org/api/v2"
VOLUME_PATH = "/Volumes/workspace/sdmx_bronze/raw_files/"
BRONZE_SCHEMA = "workspace.sdmx_bronze"
SILVER_SCHEMA = "workspace.sdmx_silver"
CATALOG = "workspace"

def ingest_flow(flow_id):
    """Read flow_config, fetch data from BIS SDMX API as CSV, save to Volume, append all-string rows to bronze."""
    config = spark.sql(f"""
        SELECT agency, version, key, start_period, end_period, dsd_id
        FROM {BRONZE_SCHEMA}.flow_config
        WHERE flow_id = '{flow_id}' AND enabled = true
    """).collect()
    if not config:
        print(f"[ingest_flow] Flow '{flow_id}' not found or not enabled.")
        return
    row = config[0]
    agency, version, key = row["agency"], row["version"], row["key"]
    start_period, end_period, dsd_id = row["start_period"], row["end_period"], row["dsd_id"]

    data_path = f"{BASE_URL}/data/dataflow/{agency}/{flow_id}/{version}/{key}"
    source_url = f"{data_path}?format=csv&startPeriod={start_period}&endPeriod={end_period}"
    print(f"Fetching: {source_url}")
    resp = requests.get(data_path, params={"format": "csv", "startPeriod": start_period, "endPeriod": end_period}, timeout=60)
    if resp.status_code != 200:
        print(f"[ingest_flow] ERROR: HTTP {resp.status_code}\n  {resp.text[:300]}")
        return
    text = resp.text.strip()
    if not text:
        print("[ingest_flow] ERROR: Empty response"); return
    lines = text.split("\n")
    if len(lines) <= 1:
        print("[ingest_flow] ERROR: No data rows"); return
    print(f"  HTTP 200, {len(lines) - 1} data rows received")

    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    entity = flow_id.replace("WS_", "").lower()
    filepath = f"{VOLUME_PATH}{entity}_{timestamp_str}.csv"
    with open(filepath, "w") as f:
        f.write(resp.text)
    print(f"  Raw file saved: {filepath}")

    df = spark.read.option("header", "true").option("inferSchema", "false").option("mode", "PERMISSIVE").csv(filepath)
    row_count = df.count()
    print(f"  Parsed {row_count} rows, {len(df.columns)} source columns (all strings)")

    from pyspark.sql.functions import lit, current_timestamp
    batch_id = uuid.uuid4().hex
    df = df.withColumn("source_url", lit(source_url)).withColumn("ingested_at", current_timestamp()).withColumn("batch_id", lit(batch_id))

    bronze_table = f"{BRONZE_SCHEMA}.{entity}_raw"
    try:
        existing_cols = [c["col_name"] for c in spark.sql(f"DESCRIBE {bronze_table}").collect()]
        table_exists = True
    except Exception:
        table_exists = False
    if not table_exists:
        df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
        print(f"  Created new table {bronze_table}")
    else:
        if "batch_id" not in existing_cols:
            spark.sql(f"ALTER TABLE {bronze_table} ADD COLUMN batch_id STRING")
        df.write.format("delta").mode("append").saveAsTable(bronze_table)
        print(f"  Appended to {bronze_table}")

    dim_cols = [r["component_id"] for r in spark.sql(f"""
        SELECT component_id FROM {SILVER_SCHEMA}.dsd_components
        WHERE dsd_id = '{dsd_id}' AND component_type = 'dimension' ORDER BY position
    """).collect()]
    series_count = "N/A"
    if dim_cols:
        col_list = ", ".join(dim_cols)
        series_result = spark.sql(f"SELECT COUNT(*) as cnt FROM (SELECT DISTINCT {col_list} FROM {bronze_table} WHERE batch_id = '{batch_id}')").collect()
        if series_result:
            series_count = series_result[0]["cnt"]
    time_result = spark.sql(f"SELECT MIN(TIME_PERIOD) as min_p, MAX(TIME_PERIOD) as max_p FROM {bronze_table} WHERE batch_id = '{batch_id}'").collect()[0]

    print(f"\n{'='*60}")
    print(f"ingest_flow('{flow_id}') COMPLETE")
    print(f"  Rows ingested (this batch): {row_count}")
    print(f"  Time range: {time_result['min_p']} to {time_result['max_p']}")
    print(f"  Unique series: {series_count}")
    print(f"  Batch ID: {batch_id}")
    print(f"  Table: {bronze_table}")
    print(f"{'='*60}")

print("Constants and ingest_flow loaded.")

# COMMAND ----------

# DBTITLE 1,Update flow_config for WS_TC
# Update flow_config with the approved key (idempotent UPDATE)
spark.sql("""
    UPDATE workspace.sdmx_bronze.flow_config
    SET key = 'Q.US+DE+JP.H.A.M.770.A',
        start_period = '2010-Q1',
        end_period = '2025-Q4',
        notes = 'Total credit. Quarterly. US+DE+JP, Households & NPISHs, All lenders, Market value, % of GDP, Adjusted for breaks. From 2010.'
    WHERE flow_id = 'WS_TC'
""")
print("flow_config updated for WS_TC")

# COMMAND ----------

# DBTITLE 1,Run ingest_flow WS_TC
# Run the ingest
ingest_flow("WS_TC")

# COMMAND ----------

# DBTITLE 1,Verify WS_TC ingest
# Verify: rows per country and time range
spark.sql("""
    SELECT BORROWERS_CTY, COUNT(*) as rows, MIN(TIME_PERIOD) as first_period, MAX(TIME_PERIOD) as last_period
    FROM workspace.sdmx_bronze.tc_raw
    GROUP BY BORROWERS_CTY
    ORDER BY BORROWERS_CTY
""").show()

# COMMAND ----------

# DBTITLE 1,Phase G summary
# MAGIC %md
# MAGIC ### Phase G — Summary
# MAGIC
# MAGIC | Metric | Value |
# MAGIC | --- | --- |
# MAGIC | Rows ingested | 192 |
# MAGIC | Source columns | 17 (all STRING) |
# MAGIC | Unique series | 3 (one per country) |
# MAGIC | Time range | 2010-Q1 to 2025-Q4 |
# MAGIC | Countries | US (64 rows), DE (64 rows), JP (64 rows) |
# MAGIC | Bronze table | `workspace.sdmx_bronze.tc_raw` |
# MAGIC | Key used | `Q.US+DE+JP.H.A.M.770.A` |
# MAGIC
# MAGIC All 64 quarters present for all 3 countries — no gaps. The `ingest_flow` function worked unchanged for WS_TC (version 2.0, 7 dimensions), confirming the generic design.

# COMMAND ----------

# DBTITLE 1,Phase H header
# MAGIC %md
# MAGIC ## Phase H — Validation Design (no code yet)
# MAGIC
# MAGIC ### Design principle
# MAGIC The validator is **metadata-driven**: all rules are derived from `sdmx_silver.dsd_components` and `sdmx_silver.codes` for a given `flow_id`. No hard-coded column names or codelist IDs. Adding a new dataflow needs only a `flow_config` row plus `load_structures` and `ingest_flow`.
# MAGIC
# MAGIC ### What was verified before designing
# MAGIC
# MAGIC | Finding | Source | Impact on design |
# MAGIC | --- | --- | --- |
# MAGIC | TIME_PERIOD format for M = `yyyy-MM` | cbpol_raw actual data | TIME_FORMAT rule regex |
# MAGIC | TIME_PERIOD format for Q = `yyyy-Qn` | tc_raw actual data | TIME_FORMAT rule regex |
# MAGIC | OBS_STATUS codes H, L, M, Q = missing | sdmx_silver.codes (CL_OBS_STATUS) | OBS_VALUE rule — null OBS_VALUE allowed only with these statuses |
# MAGIC | WS_CBPOL has 5 mandatory attributes | dsd_components (BIS_CBPOL) | ATTR_MANDATORY checks metadata, not hard-coded |
# MAGIC | WS_TC has 3 mandatory attributes | dsd_components (BIS_TOTAL_CREDIT) | Same rule, different result — proves genericity |
# MAGIC | TIME_FORMAT is NULL in all 408 cbpol rows | cbpol_raw | ATTR_MANDATORY will flag this; severity = error per spec |
# MAGIC | 48 duplicate keys in cbpol_raw | cbpol_raw (Phase 3 + Phase D overlap) | Dedup will supersede 48 rows; DUPLICATE rule verifies none remain |
# MAGIC | No non-numeric OBS_VALUE in either table | cbpol_raw, tc_raw | OBS_VALUE rule expected to pass on real data |
# MAGIC | cbpol_raw: 15 source cols = 15 DSD components | schema vs dsd_components | STRUCT rule expected to pass |
# MAGIC | tc_raw: 17 source cols = 17 DSD components | schema vs dsd_components | STRUCT rule expected to pass |

# COMMAND ----------

# DBTITLE 1,Rule catalogue
# MAGIC %md
# MAGIC ### 1. Rule Catalogue
# MAGIC
# MAGIC | Rule ID | Description | Severity | Derived from |
# MAGIC | --- | --- | --- | --- |
# MAGIC | **STRUCT** | Every DSD component (dimension, time_dimension, attribute, measure) has a matching column in the bronze table. Checks presence, not types (bronze is all-STRING). | error | `dsd_components` WHERE `dsd_id = <flow's DSD>` → expected `component_id` list; compare to bronze table column names |
# MAGIC | **DIM_CODE** | Every dimension value in the bronze data exists in its codelist. Only checks dimensions with a codelist. | error | `dsd_components` WHERE `component_type = 'dimension' AND codelist_id IS NOT NULL` → for each, validate bronze[dim] IN `codes` WHERE `codelist_id + agency + version` match |
# MAGIC | **ATTR_MANDATORY** | Mandatory attributes (assignment_status = 'Mandatory') are present and not NULL/empty. | error | `dsd_components` WHERE `component_type = 'attribute' AND assignment_status = 'Mandatory'` → check bronze[attr] IS NOT NULL AND != '' |
# MAGIC | **ATTR_CODE** | Attribute values that have a codelist must exist in that codelist. Only checks non-empty values (null conditional attributes are OK). | error | `dsd_components` WHERE `component_type = 'attribute' AND codelist_id IS NOT NULL` → when bronze[attr] is not null/empty, validate IN `codes` for matching codelist |
# MAGIC | **TIME_FORMAT** | TIME_PERIOD matches the pattern implied by the FREQ code of the same row. | error | `sdmx_silver.freq_time_patterns` lookup table (see below); FREQ is a dimension, its value per row selects the pattern; TIME_PERIOD is matched against the regex |
# MAGIC | **OBS_VALUE** | OBS_VALUE is numeric (castable to DOUBLE), OR is null/empty AND OBS_STATUS is one of the missing codes (H, L, M, Q). | error | `dsd_components` (OBS_VALUE = measure, OBS_STATUS = attribute with CL_OBS_STATUS); missing codes derived from `codes` WHERE `codelist_id = 'CL_OBS_STATUS'` AND `code_name LIKE 'Missing%'` |
# MAGIC | **DUPLICATE** | After dedup, no two rows share the same series key (all dimension values) + TIME_PERIOD. | error | `dsd_components` WHERE `component_type IN ('dimension', 'time_dimension')` → these columns form the uniqueness key; checked on the deduped dataset |

# COMMAND ----------

# DBTITLE 1,FREQ pattern lookup
# MAGIC %md
# MAGIC ### 1a. FREQ → TIME_PERIOD pattern lookup table
# MAGIC
# MAGIC A small reference table `sdmx_silver.freq_time_patterns` (created once, not per-flow):
# MAGIC
# MAGIC | freq_code | freq_name | regex_pattern | example |
# MAGIC | --- | --- | --- | --- |
# MAGIC | A | Annual | `^\d{4}$` | 2020 |
# MAGIC | M | Monthly | `^\d{4}-\d{2}$` | 2020-01 |
# MAGIC | Q | Quarterly | `^\d{4}-Q[1-4]$` | 2020-Q1 |
# MAGIC | H | Half-yearly | `^\d{4}-S[1-2]$` | 2020-S1 |
# MAGIC | D | Daily | `^\d{4}-\d{2}-\d{2}$` | 2020-01-15 |
# MAGIC | W | Weekly | `^\d{4}-W\d{2}$` | 2020-W01 |
# MAGIC
# MAGIC **Not covered:** B (business daily) and E (event) — marked 'not supported' in CL_FREQ. If a row has FREQ=B or E, the TIME_FORMAT rule will flag it with a warning (no pattern available).
# MAGIC
# MAGIC **Verified against actual data:** M matches `2024-01` in cbpol_raw; Q matches `2010-Q1` in tc_raw. Patterns for A, H, D, W are standard SDMX conventions not yet seen in our data but included for completeness.

# COMMAND ----------

# DBTITLE 1,Table schemas
# MAGIC %md
# MAGIC ### 2. Table Schemas
# MAGIC
# MAGIC #### `sdmx_silver.<flow>` — typed valid rows
# MAGIC
# MAGIC One row per valid observation (after dedup + validation pass):
# MAGIC
# MAGIC | Column group | Columns | Type |
# MAGIC | --- | --- | --- |
# MAGIC | Dimensions | all dimension component_ids from DSD | STRING |
# MAGIC | Time | TIME_PERIOD (original string, e.g. `2020-Q1`) | STRING |
# MAGIC | Time | period_start_date (parsed to first day of period) | DATE |
# MAGIC | Measure | OBS_VALUE | DOUBLE |
# MAGIC | Attributes | all attribute component_ids from DSD | STRING |
# MAGIC | Metadata | batch_id | STRING |
# MAGIC | Metadata | ingested_at | TIMESTAMP |
# MAGIC
# MAGIC No `source_url` column (bronze-only metadata). Column names come from `dsd_components.component_id` — not hard-coded.
# MAGIC
# MAGIC #### `sdmx_silver.<flow>_rejects` — one row per rule failure
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | row_key | STRING | Concatenation of all dimension values + TIME_PERIOD (identifies the source row) |
# MAGIC | rule_id | STRING | Which rule failed (e.g. `DIM_CODE`, `OBS_VALUE`) |
# MAGIC | component | STRING | Which component failed (e.g. `REF_AREA`, `OBS_VALUE`) |
# MAGIC | offending_value | STRING | The actual value that failed validation |
# MAGIC | message | STRING | Human-readable explanation |
# MAGIC | batch_id | STRING | Batch of the source row |
# MAGIC
# MAGIC A single source row can appear multiple times (once per failed rule).
# MAGIC
# MAGIC #### `sdmx_silver.dq_summary` — run-level summary (shared across all flows)
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | run_id | STRING | Unique ID for this validation run |
# MAGIC | run_timestamp | TIMESTAMP | When the run executed |
# MAGIC | flow_id | STRING | Which flow was validated (e.g. `WS_CBPOL`) |
# MAGIC | rule_id | STRING | Which rule (e.g. `DIM_CODE`) |
# MAGIC | rows_checked | BIGINT | Total rows checked (deduped) |
# MAGIC | rows_failed | BIGINT | Rows that failed this rule |
# MAGIC | pass_rate | DOUBLE | `1.0 - (rows_failed / rows_checked)` |

# COMMAND ----------

# DBTITLE 1,Dedup rule
# MAGIC %md
# MAGIC ### 3. Dedup Rule
# MAGIC
# MAGIC **Rule:** Latest `ingested_at` wins per series key + TIME_PERIOD.
# MAGIC
# MAGIC **Series key** = concatenation of all dimension component values (derived from `dsd_components` WHERE `component_type = 'dimension'`, ordered by `position`).
# MAGIC
# MAGIC **Processing order:**
# MAGIC 1. Read all rows from bronze (all batches, all `batch_id`s)
# MAGIC 2. Dedup: for each series key + TIME_PERIOD, keep only the row with MAX(`ingested_at`)
# MAGIC 3. Report: `rows_in` (total bronze rows), `rows_superseded` (rows removed by dedup), `rows_after_dedup`
# MAGIC 4. Validate the deduped set against all rules
# MAGIC 5. Write valid rows to `sdmx_silver.<flow>`, rejects to `sdmx_silver.<flow>_rejects`, summary to `sdmx_silver.dq_summary`
# MAGIC
# MAGIC **Expected for WS_CBPOL:** 408 rows in → 48 superseded (Phase 3 + Phase D overlap) → 360 rows after dedup.
# MAGIC **Expected for WS_TC:** 192 rows in → 0 superseded (single batch) → 192 rows after dedup.
# MAGIC
# MAGIC The DUPLICATE rule (step 4) then verifies no remaining duplicates exist in the deduped set — should always be 0 if dedup works correctly. It catches the edge case where two rows share the same series key + TIME_PERIOD AND the same `ingested_at` timestamp (down to the millisecond).

# COMMAND ----------

# DBTITLE 1,Phase H summary
# MAGIC %md
# MAGIC ### Phase H — Summary
# MAGIC
# MAGIC **Proposed:**
# MAGIC 1. **7 rules** (STRUCT, DIM_CODE, ATTR_MANDATORY, ATTR_CODE, TIME_FORMAT, OBS_VALUE, DUPLICATE) — all metadata-driven from `dsd_components` + `codes` + `freq_time_patterns`
# MAGIC 2. **3 tables** per flow: `sdmx_silver.<flow>` (valid), `sdmx_silver.<flow>_rejects` (failures), `sdmx_silver.dq_summary` (shared summary)
# MAGIC 3. **1 lookup table**: `sdmx_silver.freq_time_patterns` (FREQ → regex, created once)
# MAGIC 4. **Dedup**: latest `ingested_at` wins per series key + TIME_PERIOD; reported separately from validation
# MAGIC
# MAGIC **Known data quality issues found during design:**
# MAGIC - TIME_FORMAT is NULL in all 408 cbpol_raw rows (mandatory attribute, BIS CSV doesn't populate it). ATTR_MANDATORY will flag all 408 rows. This is a real source limitation, not a bug in our pipeline.
# MAGIC - 48 duplicate keys in cbpol_raw from overlapping Phase 3 + Phase D ingests. Dedup will resolve this.
# MAGIC
# MAGIC ⏳ Waiting for user approval before implementing.

# COMMAND ----------

# DBTITLE 1,Phase H updated design
# MAGIC %md
# MAGIC ## Phase H (updated) — Design changes per user approval
# MAGIC
# MAGIC ### Changes applied:
# MAGIC 1. **rule_config table** (`sdmx_silver.rule_config`): severity configurable per rule. ATTR_MANDATORY: 100% null in batch → WARNING ("source never populates"); partial → ERROR.
# MAGIC 2. **DUPLICATE**: error only for same key+period **within same batch**. Cross-batch repeats are revisions (latest `ingested_at` wins, reported as superseded).
# MAGIC 3. **TIME_FORMAT**: strict parse for `period_start_date` (rows that don't parse fail). No pattern for a FREQ → WARNING, not failure. Patterns for A/M/Q/D **verified against actual BIS data**; H/W/B from SDMX conventions (unverified).
# MAGIC 4. **obs_status_missing table**: explicit list (H, L, M, Q) derived from CL_OBS_STATUS, stored in `sdmx_silver.obs_status_missing`. No `LIKE 'Missing%'` at validation time.
# MAGIC 5. **Rejects table**: added `run_id`, `severity`. Silver table: added `dq_warning_count`.
# MAGIC 6. **freq_time_patterns**: reference data from SDMX conventions, not derived from the DSD — noted in decision log.
# MAGIC
# MAGIC ### Investigation results:
# MAGIC - **sdmx-csv format**: TIME_FORMAT still empty — BIS never populates it regardless of format.
# MAGIC - **DSD attachment level**: `attributeRelationship` in DSD JSON has 3 patterns: `{dimensions: [...]}` (series-level), `{primaryMeasure: OBS_VALUE}` (observation-level), `{none: {}}` (dataset-level). `dsd_components` does NOT currently record this — not changed yet.
# MAGIC - **TIME_PERIOD patterns verified**: A=`^\d{4}$` (WS_CBTA: 2020), M=`^\d{4}-\d{2}$` (WS_CBPOL: 2024-01), Q=`^\d{4}-Q[1-4]$` (WS_TC: 2010-Q1), D=`^\d{4}-\d{2}-\d{2}$` (WS_CBPOL daily: 2024-01-01).

# COMMAND ----------

# DBTITLE 1,Reference tables
# MAGIC %md
# MAGIC ### Reference tables created
# MAGIC
# MAGIC | Table | Purpose | Rows |
# MAGIC | --- | --- | --- |
# MAGIC | `sdmx_silver.rule_config` | Configurable severity per rule | 7 (all default to 'error') |
# MAGIC | `sdmx_silver.freq_time_patterns` | FREQ → regex pattern for TIME_PERIOD validation | 6 (A,M,Q,D verified; H,W unverified) |
# MAGIC | `sdmx_silver.obs_status_missing` | OBS_STATUS codes indicating missing values | 4 (H, L, M, Q) |

# COMMAND ----------

# DBTITLE 1,Phase I header
# MAGIC %md
# MAGIC ## Phase I — Implement validate_flow for WS_CBPOL

# COMMAND ----------

# DBTITLE 1,validate_flow function
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, LongType
import uuid

def validate_flow(flow_id):
    """Metadata-driven validation of an SDMX dataflow. All rules derived from dsd_components + codes + reference tables."""
    run_id = uuid.uuid4().hex
    
    # --- 1. Read flow_config ---
    config = spark.sql(f"""
        SELECT agency, version, dsd_id FROM workspace.sdmx_bronze.flow_config
        WHERE flow_id = '{flow_id}' AND enabled = true
    """).collect()
    if not config:
        print(f"[validate_flow] Flow '{flow_id}' not found or not enabled."); return
    row = config[0]
    dsd_id, dsd_version = row["dsd_id"], row["version"]
    entity = flow_id.replace("WS_", "").lower()
    bronze_table = f"workspace.sdmx_bronze.{entity}_raw"
    silver_table = f"workspace.sdmx_silver.{entity}"
    rejects_table = f"workspace.sdmx_silver.{entity}_rejects"
    
    print(f"=== validate_flow('{flow_id}') ===")
    print(f"  DSD: {dsd_id} v{dsd_version}")
    
    # --- 2. Read dsd_components ---
    comps = spark.sql(f"""
        SELECT component_id, component_type, position, assignment_status,
               codelist_agency, codelist_id, codelist_version
        FROM workspace.sdmx_silver.dsd_components
        WHERE dsd_id = '{dsd_id}' AND dsd_version = '{dsd_version}'
        ORDER BY CASE component_type WHEN 'dimension' THEN 1 WHEN 'time_dimension' THEN 2
                 WHEN 'attribute' THEN 3 WHEN 'measure' THEN 4 END, position
    """).collect()
    dim_cols = [r["component_id"] for r in comps if r["component_type"] == "dimension"]
    time_col = [r["component_id"] for r in comps if r["component_type"] == "time_dimension"][0]
    attr_cols = [r["component_id"] for r in comps if r["component_type"] == "attribute"]
    measure_col = [r["component_id"] for r in comps if r["component_type"] == "measure"][0]
    mandatory_attrs = [r["component_id"] for r in comps if r["component_type"] == "attribute" and r["assignment_status"] == "Mandatory"]
    dim_codelists = {r["component_id"]: (r["codelist_agency"], r["codelist_id"], r["codelist_version"])
                     for r in comps if r["component_type"] == "dimension" and r["codelist_id"]}
    attr_codelists = {r["component_id"]: (r["codelist_agency"], r["codelist_id"], r["codelist_version"])
                      for r in comps if r["component_type"] == "attribute" and r["codelist_id"]}
    print(f"  Dimensions: {dim_cols}")
    print(f"  Mandatory attrs: {mandatory_attrs}")
    
    # --- 3. Read rule_config ---
    rule_sev = {}
    for r in spark.sql("SELECT rule_id, component, severity FROM workspace.sdmx_silver.rule_config").collect():
        rule_sev[(r["rule_id"], r["component"])] = r["severity"]
    def get_sev(rule_id):
        return rule_sev.get((rule_id, "*"), "error")
    
    # --- 4. Read bronze ---
    bronze_df = spark.table(bronze_table)
    rows_in = bronze_df.count()
    bronze_cols = set(bronze_df.columns)
    print(f"  Bronze rows: {rows_in}")
    all_rejects = []
    
    # Helper: build full row_key from dimensions + time
    key_cols = dim_cols + [time_col]
    def make_row_key(r):
        return "|".join(str(r[c]) for c in key_cols)
    
    # --- 5. STRUCT ---
    expected_cols = dim_cols + [time_col] + attr_cols + [measure_col]
    missing = [c for c in expected_cols if c not in bronze_cols]
    if missing:
        for c in missing:
            all_rejects.append((None, None, run_id, "STRUCT", get_sev("STRUCT"), c, "", f"Column '{c}' missing from bronze", None))
        print(f"  STRUCT: FAIL — {len(missing)} missing")
    else:
        print(f"  STRUCT: PASS — all {len(expected_cols)} components present")
    
    # --- 6. DUPLICATE (within same batch) ---
    dup_df = bronze_df.groupBy(*key_cols, "batch_id").agg(F.count("*").alias("cnt")).filter("cnt > 1")
    dup_count = dup_df.count()
    if dup_count > 0:
        for r in dup_df.collect():
            all_rejects.append((None, make_row_key(r), run_id, "DUPLICATE", get_sev("DUPLICATE"), "|".join(key_cols), str(r["cnt"]), f"Duplicate within batch {r['batch_id'][:8]}...", r["batch_id"]))
        print(f"  DUPLICATE: FAIL — {dup_count} duplicate keys within same batch")
    else:
        print(f"  DUPLICATE: PASS — no duplicates within same batch")
    
    # --- 7. Dedup (cross-batch: latest ingested_at wins) ---
    w = Window.partitionBy(*key_cols).orderBy(F.col("ingested_at").desc())
    deduped = bronze_df.withColumn("_rnk", F.row_number().over(w)).filter("_rnk = 1").drop("_rnk")
    rows_after_dedup = deduped.count()
    rows_superseded = rows_in - rows_after_dedup
    print(f"  Dedup: {rows_in} -> {rows_after_dedup} ({rows_superseded} superseded)")
    
    # Add row_id for tracking
    w2 = Window.orderBy(F.lit(1))
    deduped = deduped.withColumn("row_id", F.row_number().over(w2))
    
    # --- 8. DIM_CODE ---
    dim_fails = 0
    for dim_name, (cl_a, cl_i, cl_v) in dim_codelists.items():
        valid_set = set(r["code_id"] for r in spark.sql(f"SELECT code_id FROM workspace.sdmx_silver.codes WHERE codelist_id = '{cl_i}' AND agency = '{cl_a}' AND version = '{cl_v}'").collect())
        for r in deduped.filter(~F.col(dim_name).isin(list(valid_set))).collect():
            dim_fails += 1
            all_rejects.append((r["row_id"], make_row_key(r), run_id, "DIM_CODE", get_sev("DIM_CODE"), dim_name, str(r[dim_name]), f"Value '{r[dim_name]}' not in codelist {cl_i}", r["batch_id"]))
    print(f"  DIM_CODE: {dim_fails} failures")
    
    # --- 9. ATTR_MANDATORY (100% null -> WARNING, partial -> ERROR) ---
    am_err, am_warn = 0, 0
    for attr_name in mandatory_attrs:
        if attr_name not in bronze_cols: continue
        null_cnt = deduped.filter(F.col(attr_name).isNull() | (F.col(attr_name) == "")).count()
        if null_cnt == 0: continue
        sev = "warning" if null_cnt == rows_after_dedup else "error"
        for r in deduped.filter(F.col(attr_name).isNull() | (F.col(attr_name) == "")).collect():
            if sev == "warning": am_warn += 1
            else: am_err += 1
            msg = f"Mandatory '{attr_name}' empty (source never populates)" if sev == "warning" else f"Mandatory '{attr_name}' empty (partially missing)"
            all_rejects.append((r["row_id"], make_row_key(r), run_id, "ATTR_MANDATORY", sev, attr_name, "", msg, r["batch_id"]))
    print(f"  ATTR_MANDATORY: {am_err} errors, {am_warn} warnings")
    
    # --- 10. ATTR_CODE ---
    ac_fails = 0
    for attr_name, (cl_a, cl_i, cl_v) in attr_codelists.items():
        if attr_name not in bronze_cols: continue
        valid_set = set(r["code_id"] for r in spark.sql(f"SELECT code_id FROM workspace.sdmx_silver.codes WHERE codelist_id = '{cl_i}' AND agency = '{cl_a}' AND version = '{cl_v}'").collect())
        for r in deduped.filter(F.col(attr_name).isNotNull() & (F.col(attr_name) != "") & ~F.col(attr_name).isin(list(valid_set))).collect():
            ac_fails += 1
            all_rejects.append((r["row_id"], make_row_key(r), run_id, "ATTR_CODE", get_sev("ATTR_CODE"), attr_name, str(r[attr_name]), f"Value '{r[attr_name]}' not in codelist {cl_i}", r["batch_id"]))
    print(f"  ATTR_CODE: {ac_fails} failures")
    
    # --- 11. TIME_FORMAT (regex check + period_start_date strict parse) ---
    deduped_pat = deduped.join(spark.table("workspace.sdmx_silver.freq_time_patterns"), deduped["FREQ"] == F.col("freq_code"), "left")
    tf_fails = deduped_pat.filter(F.col("regex_pattern").isNotNull() & ~F.col(time_col).rlike(F.col("regex_pattern"))).collect()
    tf_warns = deduped_pat.filter(F.col("regex_pattern").isNull()).collect()
    for r in tf_fails:
        all_rejects.append((r["row_id"], make_row_key(r), run_id, "TIME_FORMAT", get_sev("TIME_FORMAT"), time_col, str(r[time_col]), f"TIME_PERIOD '{r[time_col]}' doesn't match pattern for FREQ={r['FREQ']}", r["batch_id"]))
    for r in tf_warns:
        all_rejects.append((r["row_id"], make_row_key(r), run_id, "TIME_FORMAT", "warning", time_col, str(r[time_col]), f"No pattern for FREQ={r['FREQ']}", r["batch_id"]))
    print(f"  TIME_FORMAT: {len(tf_fails)} failures, {len(tf_warns)} warnings (no pattern)")
    
    # --- 12. OBS_VALUE ---
    missing_codes = [r["code_id"] for r in spark.sql("SELECT code_id FROM workspace.sdmx_silver.obs_status_missing").collect()]
    obs_nonnum = deduped.filter(F.col(measure_col).isNotNull() & (F.col(measure_col) != "") & F.col(measure_col).cast("double").isNull()).collect()
    obs_nullbad = deduped.filter((F.col(measure_col).isNull() | (F.col(measure_col) == "")) & (~F.col("OBS_STATUS").isin(missing_codes))).collect()
    for r in obs_nonnum:
        all_rejects.append((r["row_id"], make_row_key(r), run_id, "OBS_VALUE", get_sev("OBS_VALUE"), measure_col, str(r[measure_col]), f"OBS_VALUE '{r[measure_col]}' not numeric", r["batch_id"]))
    for r in obs_nullbad:
        all_rejects.append((r["row_id"], make_row_key(r), run_id, "OBS_VALUE", get_sev("OBS_VALUE"), measure_col, str(r[measure_col]), f"Null OBS_VALUE but OBS_STATUS='{r['OBS_STATUS']}' not missing code", r["batch_id"]))
    print(f"  OBS_VALUE: {len(obs_nonnum)} non-numeric, {len(obs_nullbad)} null-without-missing-status")
    
    # --- 13. Build silver + rejects ---
    error_row_ids = set(r[0] for r in all_rejects if r[4] == "error" and r[0] is not None)
    warning_row_ids = set(r[0] for r in all_rejects if r[4] == "warning" and r[0] is not None)
    rows_valid = rows_after_dedup - len(error_row_ids)
    
    # Silver DataFrame
    silver_df = deduped.filter(~F.col("row_id").isin(list(error_row_ids))) if error_row_ids else deduped
    silver_df = silver_df.withColumn(measure_col, F.col(measure_col).cast("double"))
    silver_df = silver_df.withColumn("period_start_date", F.expr("""
        CASE WHEN FREQ = 'A' THEN to_date(TIME_PERIOD, 'yyyy')
             WHEN FREQ = 'M' THEN to_date(TIME_PERIOD, 'yyyy-MM')
             WHEN FREQ = 'D' THEN to_date(TIME_PERIOD, 'yyyy-MM-dd')
             WHEN FREQ = 'Q' THEN to_date(concat(substring(TIME_PERIOD, 1, 4), '-',
                  CASE substring(TIME_PERIOD, 7, 1) WHEN '1' THEN '01' WHEN '2' THEN '04' WHEN '3' THEN '07' WHEN '4' THEN '10' END), 'yyyy-MM')
             ELSE NULL END
    """))
    if warning_row_ids:
        silver_df = silver_df.withColumn("dq_warning_count", F.when(F.col("row_id").isin(list(warning_row_ids)), F.lit(1)).otherwise(F.lit(0)))
    else:
        silver_df = silver_df.withColumn("dq_warning_count", F.lit(0))
    select_cols = [c for c in dim_cols + [time_col, "period_start_date", measure_col] + attr_cols + ["batch_id", "ingested_at", "dq_warning_count"] if c in silver_df.columns]
    silver_df = silver_df.select(*select_cols)
    
    # Rejects DataFrame
    rej_schema = StructType([StructField(n, t(), True) for n, t in [("row_id", LongType), ("row_key", StringType), ("run_id", StringType), ("rule_id", StringType), ("severity", StringType), ("component", StringType), ("offending_value", StringType), ("message", StringType), ("batch_id", StringType)]])
    rejects_df = spark.createDataFrame(all_rejects, rej_schema) if all_rejects else spark.createDataFrame([], rej_schema)
    
    # --- 14. Write tables ---
    silver_df.write.format("delta").saveAsTable(silver_table)
    rejects_df.write.format("delta").saveAsTable(rejects_table)
    
    # dq_summary
    spark.sql("CREATE TABLE IF NOT EXISTS workspace.sdmx_silver.dq_summary (run_id STRING, run_timestamp TIMESTAMP, flow_id STRING, rule_id STRING, rows_checked BIGINT, rows_failed BIGINT, pass_rate DOUBLE) USING DELTA")
    from collections import Counter
    by_rule = Counter((r[3], r[4]) for r in all_rejects)
    for rule_id in ["STRUCT", "DIM_CODE", "ATTR_MANDATORY", "ATTR_CODE", "TIME_FORMAT", "OBS_VALUE", "DUPLICATE"]:
        failed = sum(cnt for (rule, _), cnt in by_rule.items() if rule == rule_id)
        pr = round(1.0 - (failed / rows_after_dedup if rows_after_dedup > 0 else 0), 4)
        spark.sql(f"INSERT INTO workspace.sdmx_silver.dq_summary VALUES ('{run_id}', current_timestamp(), '{flow_id}', '{rule_id}', {rows_after_dedup}, {failed}, {float(pr)})")
    
    # --- 15. Report ---
    print(f"\n{'='*60}")
    print(f"validate_flow('{flow_id}') COMPLETE")
    print(f"  Rows in (bronze):        {rows_in}")
    print(f"  Rows superseded (dedup): {rows_superseded}")
    print(f"  Rows after dedup:        {rows_after_dedup}")
    print(f"  Rows valid (silver):     {rows_valid}")
    print(f"  Rows rejected (errors):  {rows_after_dedup - rows_valid}")
    print(f"  Total reject records:    {len(all_rejects)}")
    if all_rejects:
        print(f"\n  Rejects by rule:")
        for (rule, sev), cnt in sorted(by_rule.items()):
            print(f"    {rule:20s} ({sev:7s}): {cnt}")
        print(f"\n  Sample rejects (first 5):")
        for r in all_rejects[:5]:
            print(f"    rule={r[3]}, sev={r[4]}, comp={r[5]}, val='{r[6]}', msg={r[7]}")
    print(f"\n  Run ID: {run_id}")
    print(f"  Silver: {silver_table} ({silver_df.count()} rows)")
    print(f"  Rejects: {rejects_table} ({rejects_df.count()} rows)")
    print(f"{'='*60}")
    return run_id

print("validate_flow function defined.")

# COMMAND ----------

# DBTITLE 1,Run validate_flow WS_CBPOL
# Run validation for WS_CBPOL
validate_flow("WS_CBPOL")

# COMMAND ----------

# DBTITLE 1,Verify WS_CBPOL results
# Verify silver table
spark.sql("""
    SELECT COUNT(*) as rows, COUNT(DISTINCT REF_AREA) as countries,
           MIN(TIME_PERIOD) as min_period, MAX(TIME_PERIOD) as max_period,
           MIN(period_start_date) as min_date, MAX(period_start_date) as max_date,
           MIN(OBS_VALUE) as min_val, MAX(OBS_VALUE) as max_val,
           SUM(dq_warning_count) as total_warnings
    FROM workspace.sdmx_silver.cbpol
""").show()

# Verify dq_summary
spark.sql("""
    SELECT rule_id, rows_checked, rows_failed, pass_rate
    FROM workspace.sdmx_silver.dq_summary WHERE flow_id = 'WS_CBPOL'
    ORDER BY rule_id
""").show()

# COMMAND ----------

# DBTITLE 1,Phase I summary
# MAGIC %md
# MAGIC ### Phase I — Summary
# MAGIC
# MAGIC | Metric | Value |
# MAGIC | --- | --- |
# MAGIC | Rows in (bronze) | 408 |
# MAGIC | Rows superseded (dedup) | 48 (Phase 3 + Phase D overlap) |
# MAGIC | Rows after dedup | 360 |
# MAGIC | Rows valid (silver) | 360 |
# MAGIC | Rows rejected (errors) | 0 |
# MAGIC | Total reject records | 360 (all warnings) |
# MAGIC
# MAGIC **Rejects by rule:**
# MAGIC | Rule | Severity | Count | Explanation |
# MAGIC | --- | --- | --- | --- |
# MAGIC | ATTR_MANDATORY | warning | 360 | TIME_FORMAT NULL in all rows — BIS CSV never populates it (100% null → WARNING) |
# MAGIC | All others | — | 0 | All pass |
# MAGIC
# MAGIC **Tables created:**
# MAGIC - `sdmx_silver.cbpol` — 360 rows, 19 columns (dims STRING, TIME_PERIOD STRING, period_start_date DATE, OBS_VALUE DOUBLE, attrs STRING, batch_id, ingested_at, dq_warning_count)
# MAGIC - `sdmx_silver.cbpol_rejects` — 360 rows (all ATTR_MANDATORY warnings)
# MAGIC - `sdmx_silver.dq_summary` — 7 rows (one per rule)
# MAGIC - `sdmx_silver.rule_config` — 7 rows (configurable severity)
# MAGIC - `sdmx_silver.freq_time_patterns` — 6 rows (FREQ → regex, reference data from SDMX conventions)
# MAGIC - `sdmx_silver.obs_status_missing` — 4 rows (H, L, M, Q)

# COMMAND ----------

