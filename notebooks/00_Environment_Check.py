# Databricks notebook source
# DBTITLE 1,00 — Environment Check
# MAGIC %md
# MAGIC # 00 — Environment Check
# MAGIC
# MAGIC Part of the **SDMX Lakehouse** project. This notebook verifies the Databricks Free Edition environment before building the pipeline:
# MAGIC 1. Current catalog, available schemas, and Python version
# MAGIC 2. Outbound connectivity to the BIS SDMX API
# MAGIC 3. Whether `sdmx1` and `pysdmx` Python packages can be installed
# MAGIC
# MAGIC > **Disclaimer:** Independent project using publicly available data. Not affiliated with the Bank for International Settlements.

# COMMAND ----------

# DBTITLE 1,Catalog, schemas, Python version
# --- 0.1: Catalog, schemas, and Python version ---
import sys

# Current catalog (via Spark SQL)
catalog_row = spark.sql("SELECT current_catalog() AS cat").collect()[0]
print(f"Current catalog : {catalog_row['cat']}")

# Available catalogs
print("\nAvailable catalogs:")
for row in spark.sql("SHOW CATALOGS").collect():
    print(f"  - {row['catalog']}")

# Schemas in the current catalog
print(f"\nSchemas in '{catalog_row['cat']}':")
for row in spark.sql(f"SHOW SCHEMAS IN {catalog_row['cat']}").collect():
    print(f"  - {row['databaseName']}")

# Python version
print(f"\nPython version  : {sys.version}")
print(f"Spark version   : {spark.version}")

# COMMAND ----------

# DBTITLE 1,BIS API connectivity test
# --- 0.2: Test outbound connectivity to BIS SDMX API ---
import requests

BIS_API = "https://stats.bis.org/api/v2"

try:
    resp = requests.get(BIS_API, timeout=15)
    print(f"URL            : {BIS_API}")
    print(f"HTTP status    : {resp.status_code}")
    print(f"Response (first 500 chars):")
    print(resp.text[:500] if resp.text else "(empty body)")
except requests.exceptions.Timeout:
    print(f"FAILED: Request to {BIS_API} timed out after 15 seconds.")
    print("Outbound internet may be blocked or restricted.")
except requests.exceptions.ConnectionError as e:
    print(f"FAILED: Connection error to {BIS_API}.")
    print(f"Error: {e}")
    print("Outbound internet appears to be blocked on this compute.")
except Exception as e:
    print(f"FAILED: Unexpected error.")
    print(f"Error type: {type(e).__name__}")
    print(f"Error: {e}")

# COMMAND ----------

# DBTITLE 1,pip install sdmx1 and pysdmx
# --- 0.3: Test pip install of SDMX Python packages ---
import subprocess

packages = ["sdmx1", "pysdmx"]

for pkg in packages:
    print(f"\n{'='*50}")
    print(f"Testing: pip install {pkg}")
    print(f"{'='*50}")
    try:
        result = subprocess.run(
            ["pip", "install", pkg, "--quiet"],
            capture_output=True, text=True, timeout=60
        )
        if result.returncode == 0:
            print(f"SUCCESS: {pkg} installed.")
            if result.stdout.strip():
                print(f"stdout: {result.stdout.strip()[:300]}")
        else:
            print(f"FAILED: pip install {pkg} returned code {result.returncode}")
            if result.stderr.strip():
                print(f"stderr: {result.stderr.strip()[:500]}")
    except subprocess.TimeoutExpired:
        print(f"FAILED: pip install {pkg} timed out after 60 seconds.")
    except Exception as e:
        print(f"FAILED: {type(e).__name__}: {e}")