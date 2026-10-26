# Databricks notebook source
# MAGIC %md
# MAGIC # framework/00_bootstrap
# MAGIC
# MAGIC Creates every schema, volume, control table and reference table the SDMX
# MAGIC framework depends on, and seeds the configuration tables.
# MAGIC
# MAGIC **Idempotent.** `CREATE ... IF NOT EXISTS` for structure; reference tables are
# MAGIC overwritten from the literals below, because their contents *are* the config.
# MAGIC Tables produced by the pipeline (`*_raw`, `codes`, `dsd_components`, silver
# MAGIC `<flow>`, `<flow>_dq_findings`, everything in `sdmx_gold`) are deliberately
# MAGIC **not** created here - they are created by the stage that owns them.
# MAGIC
# MAGIC Generated from the live workspace, which is the source of truth.

# COMMAND ----------

from datetime import datetime

CATALOG = "workspace"
SCHEMAS = ["sdmx_bronze", "sdmx_silver", "sdmx_gold"]
VOLUME = "raw_files"

# COMMAND ----------

# DBTITLE 1,Schemas and volume
for s in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")
    print(f"  schema {CATALOG}.{s}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.sdmx_bronze.{VOLUME}")
print(f"  volume {CATALOG}.sdmx_bronze.{VOLUME}")

# COMMAND ----------

# DBTITLE 1,Control and reference tables
TABLE_DDL = [
    """CREATE TABLE IF NOT EXISTS workspace.sdmx_bronze.flow_config (
  flow_id STRING,
  agency STRING,
  version STRING,
  dsd_id STRING,
  key STRING,
  start_period STRING,
  end_period STRING,
  enabled BOOLEAN,
  notes STRING)
USING delta""",

    """CREATE TABLE IF NOT EXISTS workspace.sdmx_bronze.expected_series (
  flow_id STRING,
  series_key STRING,
  first_period STRING,
  frequency STRING)
USING delta""",

    """CREATE TABLE IF NOT EXISTS workspace.sdmx_bronze.collection_source (
  country_name STRING,
  date STRING,
  rate STRING,
  obs_status STRING)
USING delta""",

    """CREATE TABLE IF NOT EXISTS workspace.sdmx_bronze.collection_mapping (
  source_field STRING,
  source_value STRING,
  target_field STRING,
  target_value STRING,
  transform STRING)
USING delta""",

    """CREATE TABLE IF NOT EXISTS workspace.sdmx_bronze.collection_mapping_findings (
  source_value STRING,
  target_field STRING,
  message STRING,
  source_row BIGINT)
USING delta""",

    """CREATE TABLE IF NOT EXISTS workspace.sdmx_bronze.collection_completeness_findings (
  run_id STRING,
  flow_id STRING,
  series_key STRING,
  missing_period STRING,
  frequency STRING,
  message STRING)
USING delta""",

    """CREATE TABLE IF NOT EXISTS workspace.sdmx_bronze.cbpol_raw_seeded_test (
  FREQ STRING,
  REF_AREA STRING,
  UNIT_MEASURE STRING,
  UNIT_MULT STRING,
  TIME_FORMAT STRING,
  COMPILATION STRING,
  DECIMALS STRING,
  SOURCE_REF STRING,
  SUPP_INFO_BREAKS STRING,
  TITLE STRING,
  TIME_PERIOD STRING,
  OBS_VALUE STRING,
  OBS_STATUS STRING,
  OBS_CONF STRING,
  OBS_PRE_BREAK STRING,
  source_url STRING,
  ingested_at TIMESTAMP,
  batch_id STRING,
  expected_rule STRING)
USING delta""",

    """CREATE TABLE IF NOT EXISTS workspace.sdmx_bronze.cbpol_raw_collection (
  FREQ STRING,
  REF_AREA STRING,
  UNIT_MEASURE STRING,
  UNIT_MULT STRING,
  TIME_FORMAT STRING,
  COMPILATION STRING,
  DECIMALS STRING,
  SOURCE_REF STRING,
  SUPP_INFO_BREAKS STRING,
  TITLE STRING,
  TIME_PERIOD STRING,
  OBS_VALUE STRING,
  OBS_STATUS STRING,
  OBS_CONF STRING,
  OBS_PRE_BREAK STRING,
  source_url STRING,
  ingested_at TIMESTAMP,
  batch_id STRING,
  source_file STRING,
  source_row BIGINT)
USING delta""",

    """CREATE TABLE IF NOT EXISTS workspace.sdmx_silver.rule_config (
  rule_id STRING,
  component STRING,
  severity STRING)
USING delta""",

    """CREATE TABLE IF NOT EXISTS workspace.sdmx_silver.freq_time_patterns (
  freq_code STRING,
  freq_name STRING,
  regex_pattern STRING,
  period_parse STRING,
  example STRING,
  verified BOOLEAN,
  date_parse_expr STRING COMMENT 'Spark SQL expression to parse TIME_PERIOD to a date for this FREQ. NULL = no parse expression (WARNING).')
USING delta""",

    """CREATE TABLE IF NOT EXISTS workspace.sdmx_silver.obs_status_missing (
  code_id STRING,
  code_name STRING)
USING delta""",

    """CREATE TABLE IF NOT EXISTS workspace.sdmx_silver.dq_summary (
  run_id STRING,
  run_timestamp TIMESTAMP,
  flow_id STRING,
  rule_id STRING,
  rows_checked BIGINT,
  rows_failed BIGINT,
  pass_rate DOUBLE,
  validator_version STRING COMMENT 'Validator version: v1 (Phase I, cast-based), v2 (Phase K v2, try_cast + date_parse_expr + attachment_level)',
  warning_count BIGINT COMMENT 'Number of warning-severity findings for this rule (batch-level counted once)')
USING delta""",
]

for stmt in TABLE_DDL:
    spark.sql(stmt)
    print("  " + stmt.split("\n")[0].replace("CREATE TABLE IF NOT EXISTS ", ""))

# COMMAND ----------

# DBTITLE 1,Views
VIEW_DDL = [
    """CREATE OR REPLACE VIEW workspace.sdmx_silver.dq_summary_latest AS
SELECT * FROM workspace.sdmx_silver.dq_summary
WHERE (flow_id, rule_id, run_timestamp) IN (
    SELECT flow_id, rule_id, MAX(run_timestamp)
    FROM workspace.sdmx_silver.dq_summary
    GROUP BY flow_id, rule_id
)""",
]

for stmt in VIEW_DDL:
    spark.sql(stmt)
    print("  " + stmt.split("\n")[0].replace("CREATE OR REPLACE VIEW ", ""))

# COMMAND ----------

# DBTITLE 1,Seed flow_config
rows = [
    ('WS_CBPOL', 'BIS', '1.0', 'BIS_CBPOL', 'M.US+XM+JP+GB+CH', '2020-01', '2025-12', True, 'Central bank policy rates. Monthly. Areas: US, XM (Euro area), JP, GB, CH. Euro area covers Germany (DE has no individual series).'),
    ('WS_DSR', 'BIS', '1.0', 'BIS_DSR', '', '', '', False, 'Debt service ratios. Disabled for now.'),
    ('WS_TC', 'BIS', '2.0', 'BIS_TOTAL_CREDIT', 'Q.US+DE+JP.H.A.M.770.A', '2010-Q1', '2025-Q4', True, 'Total credit. Quarterly. US+DE+JP, Households & NPISHs, All lenders, Market value, % of GDP, Adjusted for breaks. From 2010.'),
]
df = spark.createDataFrame(rows, "flow_id STRING, agency STRING, version STRING, dsd_id STRING, key STRING, start_period STRING, end_period STRING, enabled BOOLEAN, notes STRING")
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.sdmx_bronze.flow_config")
print(f"  seeded workspace.sdmx_bronze.flow_config: {df.count()} rows")

# COMMAND ----------

# DBTITLE 1,Seed expected_series
rows = [
    ('WS_CBPOL', '{"FREQ": "M", "REF_AREA": "US"}', '2024-01', 'M'),
    ('WS_CBPOL', '{"FREQ": "M", "REF_AREA": "DE"}', '2024-01', 'M'),
    ('WS_CBPOL', '{"FREQ": "M", "REF_AREA": "JP"}', '2024-01', 'M'),
]
df = spark.createDataFrame(rows, "flow_id STRING, series_key STRING, first_period STRING, frequency STRING")
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.sdmx_bronze.expected_series")
print(f"  seeded workspace.sdmx_bronze.expected_series: {df.count()} rows")

# COMMAND ----------

# DBTITLE 1,Seed collection_source
rows = [
    ('United States', 'Jan-24', '5.375', 'A'),
    ('United States', 'Feb-24', '5.375', 'A'),
    ('United States', 'Mar-24', '', 'A'),
    ('Germany', 'Jan-24', '4,000', 'A'),
    ('Germany', 'Feb-24', '4,000', 'A'),
    ('Japan', 'Jan-24', '0,100', 'A'),
    ('Atlantis', 'Jan-24', '2,500', 'A'),
]
df = spark.createDataFrame(rows, "country_name STRING, date STRING, rate STRING, obs_status STRING")
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.sdmx_bronze.collection_source")
print(f"  seeded workspace.sdmx_bronze.collection_source: {df.count()} rows")

# COMMAND ----------

# DBTITLE 1,Seed collection_mapping
rows = [
    ('country_name', 'United States', 'REF_AREA', 'US', 'direct'),
    ('country_name', 'Germany', 'REF_AREA', 'DE', 'direct'),
    ('country_name', 'Japan', 'REF_AREA', 'JP', 'direct'),
    ('date', '', 'TIME_PERIOD', '', 'date_format:MMM-yyΓåÆyyyy-MM'),
    ('rate', '', 'OBS_VALUE', '', 'decimal_comma_to_dot'),
    ('obs_status', '', 'OBS_STATUS', '', 'direct'),
    ('(constant)', '', 'FREQ', 'M', 'constant'),
    ('(constant)', '', 'UNIT_MEASURE', '368', 'constant'),
    ('(constant)', '', 'UNIT_MULT', '0', 'constant'),
    ('(constant)', '', 'DECIMALS', '4', 'constant'),
    ('(constant)', '', 'TIME_FORMAT', '', 'constant'),
]
df = spark.createDataFrame(rows, "source_field STRING, source_value STRING, target_field STRING, target_value STRING, transform STRING")
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.sdmx_bronze.collection_mapping")
print(f"  seeded workspace.sdmx_bronze.collection_mapping: {df.count()} rows")

# COMMAND ----------

# DBTITLE 1,Seed cbpol_raw_seeded_test
rows = [
    ('M', 'ZZ', '368', '0', '', 'From 19 Dec 1985 onwards: mid-point of the Federal Reserve target rate; from 1 Jul 1954 to 18 Dec 1985: US Fed Funds effective rate.', '4', 'US Federal Reserve System', '', ' Central bank policy rates - United States - Monthly - End of period', '2024-01', '5.375', 'A', 'F', '', 'test://seeded', datetime.fromisoformat('2026-10-24T14:10:15.153Z'), 'fd039120d22445e396d1b6cfa342a2f2', 'DIM_CODE'),
    ('M', 'US', '368', '0', '', 'From 19 Dec 1985 onwards: mid-point of the Federal Reserve target rate; from 1 Jul 1954 to 18 Dec 1985: US Fed Funds effective rate.', '4', 'US Federal Reserve System', '', ' Central bank policy rates - United States - Monthly - End of period', '2024-02', '5.375', 'X', 'F', '', 'test://seeded', datetime.fromisoformat('2026-10-24T14:10:15.153Z'), 'fd039120d22445e396d1b6cfa342a2f2', 'ATTR_CODE'),
    ('M', 'US', '368', '0', '', 'From 19 Dec 1985 onwards: mid-point of the Federal Reserve target rate; from 1 Jul 1954 to 18 Dec 1985: US Fed Funds effective rate.', '', 'US Federal Reserve System', '', ' Central bank policy rates - United States - Monthly - End of period', '2024-03', '5.375', 'A', 'F', '', 'test://seeded', datetime.fromisoformat('2026-10-24T14:10:15.153Z'), 'fd039120d22445e396d1b6cfa342a2f2', 'ATTR_MANDATORY'),
    ('M', 'US', '368', '0', '', 'From 19 Dec 1985 onwards: mid-point of the Federal Reserve target rate; from 1 Jul 1954 to 18 Dec 1985: US Fed Funds effective rate.', '4', 'US Federal Reserve System', '', ' Central bank policy rates - United States - Monthly - End of period', '2024-13', '5.375', 'A', 'F', '', 'test://seeded', datetime.fromisoformat('2026-10-24T14:10:15.153Z'), 'fd039120d22445e396d1b6cfa342a2f2', 'TIME_FORMAT'),
    ('M', 'US', '368', '0', '', 'From 19 Dec 1985 onwards: mid-point of the Federal Reserve target rate; from 1 Jul 1954 to 18 Dec 1985: US Fed Funds effective rate.', '4', 'US Federal Reserve System', '', ' Central bank policy rates - United States - Monthly - End of period', '2024-05', 'N/A', 'A', 'F', '', 'test://seeded', datetime.fromisoformat('2026-10-24T14:10:15.153Z'), 'fd039120d22445e396d1b6cfa342a2f2', 'OBS_VALUE'),
    ('M', 'US', '368', '0', '', 'From 19 Dec 1985 onwards: mid-point of the Federal Reserve target rate; from 1 Jul 1954 to 18 Dec 1985: US Fed Funds effective rate.', '4', 'US Federal Reserve System', '', ' Central bank policy rates - United States - Monthly - End of period', '2024-05', 'N/A', 'A', 'F', '', 'test://seeded', datetime.fromisoformat('2026-10-24T14:10:15.153Z'), 'fd039120d22445e396d1b6cfa342a2f2', 'DUPLICATE'),
    ('M', 'US', '368', '0', '', '', '4', '', '', '', '2024-06', '5.25', 'A', '', '', 'test://seeded', datetime.fromisoformat('2026-10-24T14:36:01.757Z'), 'clean-control-batch', 'NONE'),
    ('M', 'DE', '368', '0', '', '', '4', '', '', '', '2024-07', '4.000', 'A', '', '', 'test://seeded', datetime.fromisoformat('2026-10-24T14:36:01.757Z'), 'clean-control-batch', 'NONE'),
    ('M', 'JP', '368', '0', '', '', '4', '', '', '', '2024-08', '0.100', 'A', '', '', 'test://seeded', datetime.fromisoformat('2026-10-24T14:36:01.757Z'), 'clean-control-batch', 'NONE'),
]
df = spark.createDataFrame(rows, "FREQ STRING, REF_AREA STRING, UNIT_MEASURE STRING, UNIT_MULT STRING, TIME_FORMAT STRING, COMPILATION STRING, DECIMALS STRING, SOURCE_REF STRING, SUPP_INFO_BREAKS STRING, TITLE STRING, TIME_PERIOD STRING, OBS_VALUE STRING, OBS_STATUS STRING, OBS_CONF STRING, OBS_PRE_BREAK STRING, source_url STRING, ingested_at TIMESTAMP, batch_id STRING, expected_rule STRING")
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.sdmx_bronze.cbpol_raw_seeded_test")
print(f"  seeded workspace.sdmx_bronze.cbpol_raw_seeded_test: {df.count()} rows")

# COMMAND ----------

# DBTITLE 1,Seed rule_config
rows = [
    ('STRUCT', '*', 'error'),
    ('DIM_CODE', '*', 'error'),
    ('ATTR_MANDATORY', '*', 'error'),
    ('ATTR_CODE', '*', 'error'),
    ('TIME_FORMAT', '*', 'error'),
    ('OBS_VALUE', '*', 'error'),
    ('DUPLICATE', '*', 'error'),
]
df = spark.createDataFrame(rows, "rule_id STRING, component STRING, severity STRING")
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.sdmx_silver.rule_config")
print(f"  seeded workspace.sdmx_silver.rule_config: {df.count()} rows")

# COMMAND ----------

# DBTITLE 1,Seed freq_time_patterns
rows = [
    ('H', 'Half-yearly', '^\\d{4}-S[1-2]$', 'yyyy-\\SS', '2024-S1', False, "try_to_date(concat(substring(TIME_PERIOD, 1, 4), '-', CASE substring(TIME_PERIOD, 7, 1) WHEN '1' THEN '01' WHEN '2' THEN '07' END), 'yyyy-MM')"),
    ('D', 'Daily', '^\\d{4}-\\d{2}-\\d{2}$', 'yyyy-MM-dd', '2024-01-01', True, "try_to_date(TIME_PERIOD, 'yyyy-MM-dd')"),
    ('M', 'Monthly', '^\\d{4}-\\d{2}$', 'yyyy-MM', '2024-01', True, "try_to_date(TIME_PERIOD, 'yyyy-MM')"),
    ('A', 'Annual', '^\\d{4}$', 'yyyy', '2020', True, "try_to_date(TIME_PERIOD, 'yyyy')"),
    ('Q', 'Quarterly', '^\\d{4}-Q[1-4]$', 'yyyy-\\QQ', '2024-Q1', True, "try_to_date(concat(substring(TIME_PERIOD, 1, 4), '-', CASE substring(TIME_PERIOD, 7, 1) WHEN '1' THEN '01' WHEN '2' THEN '04' WHEN '3' THEN '07' WHEN '4' THEN '10' END), 'yyyy-MM')"),
    ('W', 'Weekly', '^\\d{4}-W\\d{2}$', 'yyyy-\\ww', '2024-W01', False, None),
]
df = spark.createDataFrame(rows, "freq_code STRING, freq_name STRING, regex_pattern STRING, period_parse STRING, example STRING, verified BOOLEAN, date_parse_expr STRING")
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.sdmx_silver.freq_time_patterns")
print(f"  seeded workspace.sdmx_silver.freq_time_patterns: {df.count()} rows")

# COMMAND ----------

# DBTITLE 1,Seed obs_status_missing
rows = [
    ('H', 'Missing value; holiday or weekend'),
    ('L', 'Missing value; data exist but were not collected'),
    ('M', 'Missing value; data cannot exist'),
    ('Q', 'Missing value; suppressed'),
]
df = spark.createDataFrame(rows, "code_id STRING, code_name STRING")
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.sdmx_silver.obs_status_missing")
print(f"  seeded workspace.sdmx_silver.obs_status_missing: {df.count()} rows")

# COMMAND ----------

# DBTITLE 1,Verify
expected = {
    "sdmx_bronze.flow_config": 3,
    "sdmx_bronze.expected_series": 3,
    "sdmx_bronze.collection_source": 7,
    "sdmx_bronze.collection_mapping": 11,
    "sdmx_bronze.cbpol_raw_seeded_test": 9,
    "sdmx_silver.rule_config": 7,
    "sdmx_silver.freq_time_patterns": 6,
    "sdmx_silver.obs_status_missing": 4,
}
ok = True
for t, n in expected.items():
    actual = spark.table(f"{CATALOG}.{t}").count()
    flag = "OK " if actual == n else "BAD"
    ok &= actual == n
    print(f"  {flag} {t:<40} expected {n:>3}  actual {actual:>3}")
print("\nBOOTSTRAP OK" if ok else "\nBOOTSTRAP FAILED - row counts do not match")
