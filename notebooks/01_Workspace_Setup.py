# Databricks notebook source
# DBTITLE 1,01 — Workspace Setup
# MAGIC %md
# MAGIC # 01 — Workspace Setup
# MAGIC
# MAGIC Part of the **SDMX Lakehouse** project. This notebook creates the medallion architecture in Unity Catalog:
# MAGIC
# MAGIC | Layer | Schema | Purpose |
# MAGIC | --- | --- | --- |
# MAGIC | Bronze | `sdmx_bronze` | Raw ingested data + metadata (source URL, retrieval timestamp) |
# MAGIC | Silver | `sdmx_silver` | Validated, typed data conforming to DSD structures |
# MAGIC | Gold | `sdmx_gold` | Dissemination-ready tables for dashboards & sharing |
# MAGIC
# MAGIC We also create a **Volume** (`sdmx_bronze.raw_files`) to store raw API responses before loading into Delta tables.
# MAGIC
# MAGIC > **Disclaimer:** Independent project using publicly available data. Not affiliated with the Bank for International Settlements.

# COMMAND ----------

# DBTITLE 1,Create schemas
# --- 1.1: Create schemas (idempotent) ---
# Create the three medallion schemas in the 'workspace' catalog.
# CREATE SCHEMA IF NOT EXISTS is safe to re-run.

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.sdmx_bronze")
print("Created (or confirmed): workspace.sdmx_bronze")

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.sdmx_silver")
print("Created (or confirmed): workspace.sdmx_silver")

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.sdmx_gold")
print("Created (or confirmed): workspace.sdmx_gold")

# COMMAND ----------

# DBTITLE 1,Create raw files volume
# --- 1.2: Create Volume for raw files (idempotent) ---
# Store raw API responses (CSV/XML/JSON) before loading into Delta.
# CREATE VOLUME IF NOT EXISTS is safe to re-run.

spark.sql("CREATE VOLUME IF NOT EXISTS workspace.sdmx_bronze.raw_files")
print("Created (or confirmed): workspace.sdmx_bronze.raw_files")
print(f"Volume path: /Volumes/workspace/sdmx_bronze/raw_files/")

# COMMAND ----------

# DBTITLE 1,Verify schemas and volume
# --- 1.3: Verify schemas and volume ---
print("=" * 50)
print("Schemas in 'workspace' catalog:")
print("=" * 50)
for row in spark.sql("SHOW SCHEMAS IN workspace").collect():
    print(f"  - {row['databaseName']}")

print()
print("=" * 50)
print("Volumes in workspace.sdmx_bronze:")
print("=" * 50)
for row in spark.sql("SHOW VOLUMES IN workspace.sdmx_bronze").collect():
    print(f"  - {row['volume_name']}")

# COMMAND ----------

# DBTITLE 1,Table Naming Convention
# MAGIC %md
# MAGIC ## Table Naming Convention
# MAGIC
# MAGIC All tables follow a consistent naming pattern so the project is self-documenting and publishable.
# MAGIC
# MAGIC | Pattern | Example | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `sdmx_bronze.<entity>_raw` | `sdmx_bronze.dataflows_raw` | Raw ingested data with ingestion metadata |
# MAGIC | `sdmx_silver.<entity>` | `sdmx_silver.dataflows` | Validated, typed data conforming to DSD |
# MAGIC | `sdmx_gold.<entity>` | `sdmx_gold.dataflows` | Dissemination-ready, business-friendly tables |
# MAGIC
# MAGIC **Rules:**
# MAGIC * Table names are lowercase, snake_case.
# MAGIC * Bronze tables always carry `_raw` suffix and include `source_url` and `ingested_at` columns.
# MAGIC * Silver tables drop the `_raw` suffix and add typed columns.
# MAGIC * Gold tables are aggregation/query-ready and may include denormalized columns.
# MAGIC * All structure tables (dataflows, dsd_components, codelists, codes) preserve `agency`, `id`, and `version` columns from the SDMX metadata.
# MAGIC * Raw files in the Volume use the pattern `{entity}_{YYYYMMDD_HHMMSS}.{format}` (e.g. `dataflows_20261020_143000.csv`).