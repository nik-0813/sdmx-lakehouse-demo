# Databricks notebook source
# DBTITLE 1,03 — Ingest One Dataflow
# MAGIC %md
# MAGIC # 03 — Ingest One Dataflow
# MAGIC
# MAGIC Part of the **SDMX Lakehouse** project. This notebook:
# MAGIC 1. Retrieves a limited slice of the `WS_CBPOL` dataflow (Central Bank Policy Rates) in SDMX-CSV format
# MAGIC 2. Saves the raw response to the `sdmx_bronze.raw_files` Volume with a timestamp
# MAGIC 3. Loads it into a bronze Delta table with ingestion metadata (source URL, retrieval timestamp)
# MAGIC 4. Reports row counts, key dimensions, and time range
# MAGIC
# MAGIC > **Disclaimer:** Independent project using publicly available data. Not affiliated with the Bank for International Settlements.

# COMMAND ----------

# DBTITLE 1,Step 1: Discover data API
# MAGIC %md
# MAGIC ## Step 1: Discover the BIS data API endpoint
# MAGIC
# MAGIC Before ingesting, we test the correct URL pattern for retrieving actual data (not structures). The SDMX 2.1 data resource follows `/data/{dataflowRef}/{key}?startPeriod=...&endPeriod=...&format=...`.
# MAGIC
# MAGIC We test several dataflow reference formats and CSV format options to find what BIS supports.

# COMMAND ----------

# DBTITLE 1,Explore BIS data API endpoints
# --- 3.0: Explore the BIS data API endpoint ---
import requests

BASE = "https://stats.bis.org/api/v2"

# Test various data URL patterns for WS_CBPOL
# Key: M.US = Monthly frequency, US reference area
# Period: 2024-01 to 2025-12 (recent 2 years)

tests = [
    # (label, url)
    ("data/BIS,WS_CBPOL,1.0/all (csv)",
     f"{BASE}/data/BIS,WS_CBPOL,1.0/all?format=csv&startPeriod=2024-01&endPeriod=2025-12"),
    ("data/BIS,WS_CBPOL,1.0/M.US (csv)",
     f"{BASE}/data/BIS,WS_CBPOL,1.0/M.US?format=csv&startPeriod=2024-01&endPeriod=2025-12"),
    ("data/BIS,WS_CBPOL,1.0/M.US (sdmx-csv)",
     f"{BASE}/data/BIS,WS_CBPOL,1.0/M.US?format=sdmx-csv&startPeriod=2024-01&endPeriod=2025-12"),
    ("data/BIS,WS_CBPOL,1.0/M.US (jsondata)",
     f"{BASE}/data/BIS,WS_CBPOL,1.0/M.US?format=jsondata&startPeriod=2024-01&endPeriod=2025-12"),
    ("data/BIS:WS_CBPOL(1.0)/M.US (csv)",
     f"{BASE}/data/BIS:WS_CBPOL(1.0)/M.US?format=csv&startPeriod=2024-01&endPeriod=2025-12"),
]

for label, url in tests:
    print(f"\n{'='*60}")
    print(f"Testing: {label}")
    print(f"URL: {url}")
    try:
        resp = requests.get(url, timeout=30, headers={"Accept": "text/csv, application/json"})
        print(f"Status: {resp.status_code}")
        print(f"Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
        text = resp.text[:600] if resp.text else "(empty)"
        print(f"Response (first 600 chars):\n{text}")
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")

# COMMAND ----------

# DBTITLE 1,Explore data API patterns and OpenAPI spec
# --- 3.0b: Try more data URL patterns and extract OpenAPI spec ---
import requests
import re

BASE = "https://stats.bis.org/api/v2"

# 1) Try different path structures for data endpoint
tests = [
    ("data/BIS/WS_CBPOL/1.0/M.US (csv)",
     f"{BASE}/data/BIS/WS_CBPOL/1.0/M.US?format=csv&startPeriod=2024-01&endPeriod=2025-12"),
    ("data/BIS/WS_CBPOL/M.US (csv)",
     f"{BASE}/data/BIS/WS_CBPOL/M.US?format=csv&startPeriod=2024-01&endPeriod=2025-12"),
    ("data/WS_CBPOL/M.US (csv)",
     f"{BASE}/data/WS_CBPOL/M.US?format=csv&startPeriod=2024-01&endPeriod=2025-12"),
    ("data/WS_CBPOL/all (csv)",
     f"{BASE}/data/WS_CBPOL/all?format=csv&startPeriod=2024-01&endPeriod=2025-12"),
]

for label, url in tests:
    print(f"\n{'='*60}")
    print(f"Testing: {label}")
    try:
        resp = requests.get(url, timeout=30)
        print(f"Status: {resp.status_code}")
        print(f"Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
        text = resp.text[:600] if resp.text else "(empty)"
        print(f"Response:\n{text}")
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")

# 2) Extract OpenAPI spec URL from Swagger UI HTML
print(f"\n{'='*60}")
print("Extracting OpenAPI spec from Swagger UI page")
html_resp = requests.get("https://stats.bis.org/api-doc/v2/", timeout=15)
# Look for spec URL in the HTML
spec_matches = re.findall(r'(?:url|spec)\s*[:=]\s*["\']([^"\']+\.json)["\']', html_resp.text)
print(f"Spec URLs found: {spec_matches}")
# Try common spec locations
for path in ["openapi.json", "v2/openapi.json", "api-docs.json"]:
    url = f"https://stats.bis.org/api-doc/v2/{path}"
    r = requests.get(url, timeout=10)
    print(f"  {path}: {r.status_code}")
    if r.status_code == 200 and 'json' in r.headers.get('Content-Type', '').lower():
        print(f"  FOUND! First 1000 chars:\n{r.text[:1000]}")
        break

# COMMAND ----------

# DBTITLE 1,Fetch Swagger UI JS and spec URL
# --- 3.0c: Fetch full Swagger UI HTML and try sdmx1 library ---
import requests
import re

# 1) Print full HTML to find the spec URL or JS init code
print("=== Full Swagger UI HTML ===")
html = requests.get("https://stats.bis.org/api-doc/v2/", timeout=15).text
print(html[:3000])

# 2) Look for any JS files referenced
js_files = re.findall(r'<script[^>]*src=["\']([^"\']+)["\']', html)
print(f"\nJS files referenced: {js_files}")

# 3) Try fetching each JS file to find the spec URL
for js_file in js_files:
    if js_file.startswith('./'):
        js_file = js_file[2:]
    js_url = f"https://stats.bis.org/api-doc/v2/{js_file}"
    try:
        js_resp = requests.get(js_url, timeout=15)
        if js_resp.status_code == 200:
            # Look for spec URL or swagger config
            spec_refs = re.findall(r'(?:url|spec|config)\s*[:=]\s*["\']([^"\']+)["\']', js_resp.text[:5000])
            if spec_refs:
                print(f"\nFound in {js_file}: {spec_refs}")
    except:
        pass

# COMMAND ----------

# DBTITLE 1,Fetch BIS OpenAPI spec
# --- 3.0d: Fetch the BIS OpenAPI spec (YAML) to find correct data endpoint ---
import requests
import re

SPEC_URL = "https://stats.bis.org/api-doc/v2/bis-stats-api-latest.yaml"
resp = requests.get(SPEC_URL, timeout=30)
print(f"Status: {resp.status_code}")
print(f"Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
print(f"Content length: {len(resp.text)} chars")

# Show first 2000 chars to see the API structure
print(f"\n=== First 2000 chars ===")
print(resp.text[:2000])

# COMMAND ----------

# DBTITLE 1,Test correct data endpoint with dataflow context
# --- 3.0e: Test correct data endpoint with 'dataflow' context ---
import requests

BASE = "https://stats.bis.org/api/v2"

# Correct pattern: /data/{context}/{agencyID}/{resourceID}/{version}/{key}
# context = 'dataflow' (querying data via a dataflow)
# key = M.US (Monthly, US) or 'all' for all series

urls = [
    ("data/dataflow/BIS/WS_CBPOL/1.0/M.US?csv",
     f"{BASE}/data/dataflow/BIS/WS_CBPOL/1.0/M.US?format=csv&startPeriod=2024-01&endPeriod=2025-12"),
    ("data/dataflow/BIS/WS_CBPOL/1.0/all?csv",
     f"{BASE}/data/dataflow/BIS/WS_CBPOL/1.0/all?format=csv&startPeriod=2024-01&endPeriod=2025-12"),
    ("data/dataflow/BIS/WS_CBPOL/1.0/M.US?sdmx-csv",
     f"{BASE}/data/dataflow/BIS/WS_CBPOL/1.0/M.US?format=sdmx-csv&startPeriod=2024-01&endPeriod=2025-12"),
    ("data/dataflow/BIS/WS_CBPOL/1.0/M.US?json",
     f"{BASE}/data/dataflow/BIS/WS_CBPOL/1.0/M.US?format=jsondata&startPeriod=2024-01&endPeriod=2025-12"),
]

for label, url in urls:
    print(f"\n{'='*60}")
    print(f"Testing: {label}")
    try:
        resp = requests.get(url, timeout=30)
        print(f"Status: {resp.status_code}")
        print(f"Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
        text = resp.text[:800] if resp.text else "(empty)"
        print(f"Response:\n{text}")
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")

# COMMAND ----------

# DBTITLE 1,Data API Discovery Summary
# MAGIC %md
# MAGIC ### Data API Discovery Summary
# MAGIC
# MAGIC The correct BIS data endpoint pattern is:
# MAGIC
# MAGIC ```
# MAGIC /data/{context}/{agencyID}/{resourceID}/{version}/{key}?format={format}&startPeriod=...&endPeriod=...
# MAGIC ```
# MAGIC
# MAGIC | Parameter | Value | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `context` | `dataflow` | Query data via a dataflow |
# MAGIC | `agencyID` | `BIS` | The agency |
# MAGIC | `resourceID` | `WS_CBPOL` | The dataflow ID |
# MAGIC | `version` | `1.0` | The dataflow version |
# MAGIC | `key` | `M.US+DE+JP` | Series key: FREQ=M, REF_AREA=US+DE+JP |
# MAGIC | `format` | `csv` | Flat CSV (one row per observation) |
# MAGIC
# MAGIC **Note:** `jsondata` format is NOT supported for data queries. `csv` and `sdmx-csv` both work.

# COMMAND ----------

# DBTITLE 1,Step 2: Fetch and store data
# MAGIC %md
# MAGIC ## Step 2: Fetch data slice, save to Volume, and load into bronze Delta
# MAGIC
# MAGIC We retrieve a limited slice of WS_CBPOL: monthly policy rates for US, Germany, and Japan, for 2024-01 to 2025-12.
# MAGIC
# MAGIC The raw CSV response is saved to `workspace.sdmx_bronze.raw_files` with a timestamp, then loaded into a bronze Delta table with `source_url` and `ingested_at` metadata columns.

# COMMAND ----------

# DBTITLE 1,Fetch data and save to Volume
# --- 3.1: Fetch WS_CBPOL data slice and save raw CSV to Volume ---
import requests
from datetime import datetime

# Build the data query URL
# Key: M.US+DE+JP = Monthly frequency, US + Germany + Japan
# Period: 2024-01 to 2025-12 (recent ~2 years)
DATA_URL = (
    "https://stats.bis.org/api/v2/data/dataflow/BIS/WS_CBPOL/1.0/M.US+DE+JP"
    "?format=csv&startPeriod=2024-01&endPeriod=2025-12"
)

print(f"Fetching: {DATA_URL}")
resp = requests.get(DATA_URL, timeout=60)
print(f"HTTP status: {resp.status_code}")
print(f"Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
print(f"Response size: {len(resp.text)} chars")

# Save raw CSV to the Volume with timestamp in filename
timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
raw_filename = f"cbpol_monthly_us_de_jp_{timestamp}.csv"
volume_path = f"/Volumes/workspace/sdmx_bronze/raw_files/{raw_filename}"

with open(volume_path, "w", encoding="utf-8") as f:
    f.write(resp.text)

print(f"\nSaved raw CSV to: {volume_path}")

# Show first 5 lines of the CSV to verify
lines = resp.text.strip().split("\n")
print(f"Total lines (including header): {len(lines)}")
print(f"\nHeader:\n{lines[0]}")
print(f"\nFirst data row:\n{lines[1] if len(lines) > 1 else '(no data)'}")

# COMMAND ----------

# DBTITLE 1,Load into bronze Delta table
# --- 3.2: Load raw CSV into bronze Delta table with ingestion metadata ---
from pyspark.sql.functions import lit, current_timestamp

# Read the CSV from the Volume
raw_df = spark.read.csv(
    volume_path,
    header=True,
    inferSchema=True
)

print(f"Raw CSV columns: {raw_df.columns}")
print(f"Raw row count: {raw_df.count()}")

# Add ingestion metadata columns
bronze_df = raw_df \
    .withColumn("source_url", lit(DATA_URL)) \
    .withColumn("ingested_at", current_timestamp())

print(f"\nBronze table columns: {bronze_df.columns}")
print(f"Bronze row count: {bronze_df.count()}")

# Write to bronze Delta table (overwrite for idempotency on full refresh)
bronze_df.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.sdmx_bronze.cbpol_raw")

print(f"\nStored: workspace.sdmx_bronze.cbpol_raw")

# Verify
verify_count = spark.sql("SELECT COUNT(*) AS n FROM workspace.sdmx_bronze.cbpol_raw").collect()[0]["n"]
print(f"Verification: {verify_count} rows in table")

# COMMAND ----------

# DBTITLE 1,Report counts, dimensions, time range
# --- 3.3: Report row counts, key dimensions, and time range ---
print("=" * 60)
print("PHASE 3 INGESTION REPORT")
print("=" * 60)

# Row count
total_rows = spark.sql("SELECT COUNT(*) AS n FROM workspace.sdmx_bronze.cbpol_raw").collect()[0]["n"]
print(f"\n1. Row count: {total_rows}")

# Key dimensions: distinct FREQ and REF_AREA
print("\n2. Key dimensions:")
print(f"   FREQ values: {[r['FREQ'] for r in spark.sql('SELECT DISTINCT FREQ FROM workspace.sdmx_bronze.cbpol_raw ORDER BY FREQ').collect()]}")
print(f"   REF_AREA values: {[r['REF_AREA'] for r in spark.sql('SELECT DISTINCT REF_AREA FROM workspace.sdmx_bronze.cbpol_raw ORDER BY REF_AREA').collect()]}")

# Series count (distinct FREQ + REF_AREA combinations)
series_count = spark.sql("SELECT COUNT(DISTINCT FREQ || '-' || REF_AREA) AS n FROM workspace.sdmx_bronze.cbpol_raw").collect()[0]["n"]
print(f"   Distinct series: {series_count}")

# Time range
print("\n3. Time range:")
time_range = spark.sql("""
    SELECT MIN(TIME_PERIOD) AS min_period, MAX(TIME_PERIOD) AS max_period
    FROM workspace.sdmx_bronze.cbpol_raw
""").collect()[0]
print(f"   Start: {time_range['min_period']}")
print(f"   End:   {time_range['max_period']}")

# Rows per series
print("\n4. Rows per series:")
display(spark.sql("""
    SELECT FREQ, REF_AREA, COUNT(*) AS obs_count, 
           MIN(TIME_PERIOD) AS start_period, MAX(TIME_PERIOD) AS end_period,
           MIN(OBS_VALUE) AS min_rate, MAX(OBS_VALUE) AS max_rate
    FROM workspace.sdmx_bronze.cbpol_raw
    GROUP BY FREQ, REF_AREA
    ORDER BY REF_AREA
"""))

# Sample rows
print("\n5. Sample observations (first 10):")
display(spark.sql("""
    SELECT FREQ, REF_AREA, TIME_PERIOD, OBS_VALUE, OBS_STATUS, TITLE
    FROM workspace.sdmx_bronze.cbpol_raw
    ORDER BY REF_AREA, TIME_PERIOD
    LIMIT 10
"""))

# COMMAND ----------

# DBTITLE 1,Phase 3 Summary
# MAGIC %md
# MAGIC ## Phase 3 Summary
# MAGIC
# MAGIC ### What was ingested
# MAGIC
# MAGIC | Item | Value |
# MAGIC | --- | --- |
# MAGIC | Dataflow | `WS_CBPOL` (Central Bank Policy Rates) |
# MAGIC | Series key | `M.US+DE+JP` (Monthly, US + Germany + Japan) |
# MAGIC | Period | 2024-01 to 2025-12 (24 months) |
# MAGIC | Format | CSV (flat, one row per observation) |
# MAGIC | Total rows | 48 |
# MAGIC | Series returned | 2 (US, JP) — DE returned no data |
# MAGIC | Raw file | `cbpol_monthly_us_de_jp_{timestamp}.csv` in Volume |
# MAGIC | Bronze table | `workspace.sdmx_bronze.cbpol_raw` (17 columns) |
# MAGIC
# MAGIC ### Key findings
# MAGIC
# MAGIC * **Germany (DE)** returned no data — BIS may use a different reference area code or Germany has no data in this period for this dataflow. The codelist `CL_BIS_GL_REF_AREA` (239 codes) can be checked in Phase 2's Delta table to find the correct code.
# MAGIC * **US rates** ranged from 3.625% to 5.375% over the period — showing the Fed's rate decisions.
# MAGIC * **Japan rates** ranged from -0.1% to 0.75% — showing the BOJ's exit from negative rates.
# MAGIC * All observations have `OBS_STATUS = A` (normal) and `OBS_CONF = F` (free/confidentiality not applicable).
# MAGIC
# MAGIC ### Bronze table schema
# MAGIC
# MAGIC The bronze table has 17 columns: 15 from the CSV (2 dimensions, 11 attributes, 1 time period, 1 measure) plus 2 ingestion metadata columns (`source_url`, `ingested_at`).
# MAGIC
# MAGIC ### Next steps for improvement
# MAGIC
# MAGIC 1. Check the correct REF_AREA code for Germany in the `workspace.sdmx_silver.codes` table
# MAGIC 2. Expand to more countries and frequencies for a richer dataset
# MAGIC 3. Transform to silver: validate against codelists, enrich with code descriptions
# MAGIC 4. Transform to gold: create ready-to-query tables for dashboards