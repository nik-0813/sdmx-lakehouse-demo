# Databricks notebook source
# MAGIC %md
# MAGIC # framework/02_ingest
# MAGIC
# MAGIC Defines `ingest_flow(flow_id)`.
# MAGIC
# MAGIC Reads `flow_config`, fetches the data slice from the BIS SDMX API as CSV, saves the
# MAGIC raw file to the Volume, and appends all-string rows to bronze with `source_url`,
# MAGIC `ingested_at` and `batch_id`. Every source column stays a STRING - bronze is
# MAGIC faithful to the raw file; typing happens in silver.
# MAGIC
# MAGIC Extracted verbatim from notebook `04_Config_and_Generalisation` so the framework
# MAGIC owns one copy. Notebook 04 is kept as the narrative of how it was developed.

# COMMAND ----------

# DBTITLE 1,Constants and imports
import requests, json, re, uuid
from datetime import datetime

# --- API and storage constants ---
BASE_URL = "https://stats.bis.org/api/v2"
VOLUME_PATH = "/Volumes/workspace/sdmx_bronze/raw_files/"
BRONZE_SCHEMA = "workspace.sdmx_bronze"
SILVER_SCHEMA = "workspace.sdmx_silver"
CATALOG = "workspace"

print(f"Constants loaded. BIS API: {BASE_URL}")

# COMMAND ----------

# DBTITLE 1,ingest_flow
def ingest_flow(flow_id):
    """
    Read flow_config, fetch data from the BIS SDMX API as CSV, save the raw file
    to the Volume, and append all-string rows to the bronze table with source_url,
    ingested_at, and batch_id.

    Args:
        flow_id (str): The flow ID from flow_config (e.g. "WS_CBPOL").
    """
    # --- 1. Read flow_config ---
    config = spark.sql(f"""
        SELECT agency, version, key, start_period, end_period, dsd_id
        FROM {BRONZE_SCHEMA}.flow_config
        WHERE flow_id = '{flow_id}' AND enabled = true
    """).collect()

    if not config:
        print(f"[ingest_flow] Flow '{flow_id}' not found or not enabled.")
        return

    row = config[0]
    agency = row["agency"]
    version = row["version"]
    key = row["key"]
    start_period = row["start_period"]
    end_period = row["end_period"]
    dsd_id = row["dsd_id"]

    # --- 2. Build URL and fetch data ---
    data_path = f"{BASE_URL}/data/dataflow/{agency}/{flow_id}/{version}/{key}"
    source_url = f"{data_path}?format=csv&startPeriod={start_period}&endPeriod={end_period}"

    print(f"Fetching: {source_url}")
    resp = requests.get(data_path, params={
        "format": "csv", "startPeriod": start_period, "endPeriod": end_period
    }, timeout=60)

    if resp.status_code != 200:
        print(f"[ingest_flow] ERROR: HTTP {resp.status_code}")
        print(f"  Response: {resp.text[:300]}")
        return

    text = resp.text.strip()
    if not text:
        print("[ingest_flow] ERROR: Empty response")
        return

    lines = text.split("\n")
    if len(lines) <= 1:
        print("[ingest_flow] ERROR: No data rows returned (only header or empty)")
        return

    data_rows = len(lines) - 1
    print(f"  HTTP 200, {data_rows} data rows received")

    # --- 3. Save raw CSV to Volume ---
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    entity = flow_id.replace("WS_", "").lower()
    filename = f"{entity}_{timestamp_str}.csv"
    filepath = f"{VOLUME_PATH}{filename}"

    with open(filepath, "w") as f:
        f.write(resp.text)
    print(f"  Raw file saved: {filepath}")

    # --- 4. Read CSV with Spark as ALL STRINGS ---
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(filepath)
    )

    source_col_count = len(df.columns)
    row_count = df.count()
    print(f"  Parsed {row_count} rows, {source_col_count} source columns (all strings)")

    # --- 5. Add metadata columns ---
    from pyspark.sql.functions import lit, current_timestamp

    batch_id = uuid.uuid4().hex
    df = (
        df
        .withColumn("source_url", lit(source_url))
        .withColumn("ingested_at", current_timestamp())
        .withColumn("batch_id", lit(batch_id))
    )

    # --- 6. Determine bronze table name ---
    bronze_table = f"{BRONZE_SCHEMA}.{entity}_raw"

    # --- 7. Handle table creation / schema ---
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
            print(f"  Added batch_id column to {bronze_table}")
        df.write.format("delta").mode("append").saveAsTable(bronze_table)
        print(f"  Appended to {bronze_table}")

    # --- 8. Compute series count from dsd_components ---
    dim_cols = [r["component_id"] for r in spark.sql(f"""
        SELECT component_id FROM {SILVER_SCHEMA}.dsd_components
        WHERE dsd_id = '{dsd_id}' AND component_type = 'dimension'
        ORDER BY position
    """).collect()]

    series_count = "N/A"
    if dim_cols:
        col_list = ", ".join(dim_cols)
        series_result = spark.sql(f"""
            SELECT COUNT(*) as cnt FROM (
                SELECT DISTINCT {col_list} FROM {bronze_table} WHERE batch_id = '{batch_id}'
            )
        """).collect()
        if series_result:
            series_count = series_result[0]["cnt"]

    # --- 9. Run summary ---
    time_result = spark.sql(f"""
        SELECT MIN(TIME_PERIOD) as min_p, MAX(TIME_PERIOD) as max_p
        FROM {bronze_table} WHERE batch_id = '{batch_id}'
    """).collect()[0]

    print(f"\n{'='*60}")
    print(f"ingest_flow('{flow_id}') COMPLETE")
    print(f"  URL: {source_url}")
    print(f"  Rows ingested (this batch): {row_count}")
    print(f"  Time range: {time_result['min_p']} to {time_result['max_p']}")
    print(f"  Unique series: {series_count}")
    print(f"  Batch ID: {batch_id}")
    print(f"  Table: {bronze_table}")
    print(f"{'='*60}")

# COMMAND ----------

print("ingest_flow(flow_id) defined.")
