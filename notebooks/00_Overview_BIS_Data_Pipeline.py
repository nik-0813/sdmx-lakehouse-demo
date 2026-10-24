# Databricks notebook source
# DBTITLE 1,SDMX Lakehouse — Overview
# MAGIC %md
# MAGIC # SDMX Lakehouse — BIS Data Pipeline
# MAGIC
# MAGIC Personal portfolio project: ingest public statistical data from the [BIS SDMX RESTful API](https://stats.bis.org/api-doc/v2/), validate it against SDMX structures (DSD and codelists), and prepare it for dissemination.
# MAGIC
# MAGIC > **Disclaimer:** This is an independent project using publicly available data. It is not affiliated with, endorsed by, or representative of the Bank for International Settlements. No BIS logos or branding are used.
# MAGIC
# MAGIC ## Project Structure
# MAGIC
# MAGIC Each phase is a separate notebook for maintainability and publishing:
# MAGIC
# MAGIC | Phase | Notebook | Status | Description |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 0 | [00_Environment_Check](#notebook-4209715797752200) | ✅ Done | Verify catalog, connectivity, Python packages |
# MAGIC | 1 | [01_Workspace_Setup](#notebook-4209715797752201) | ✅ Done | Create schemas, volume, naming conventions |
# MAGIC | 2 | [02_Structures](#notebook-4209715797752203) | ✅ Done | List dataflows, retrieve DSD & codelists |
# MAGIC | 3 | [03_Ingest_Dataflow](#notebook-4209715797752205) | ✅ Done | Ingest a dataflow slice into bronze Delta |
# MAGIC
# MAGIC ## Workspace Layout
# MAGIC
# MAGIC ```
# MAGIC SDMX_Lakehouse/
# MAGIC   SDMX_Lakehouse — BIS Data Pipeline   ← Overview (this notebook)
# MAGIC   00_Environment_Check                 ← Phase 0
# MAGIC   01_Workspace_Setup                   ← Phase 1
# MAGIC   02_Structures                        ← Phase 2 (future)
# MAGIC   03_Ingest_Dataflow                   ← Phase 3 (future)
# MAGIC ```
# MAGIC
# MAGIC ## Data Architecture (Medallion)
# MAGIC
# MAGIC | Layer | Schema | Purpose |
# MAGIC | --- | --- | --- |
# MAGIC | Bronze | `workspace.sdmx_bronze` | Raw ingested data + metadata (`source_url`, `ingested_at`) |
# MAGIC | Silver | `workspace.sdmx_silver` | Validated, typed data conforming to DSD structures |
# MAGIC | Gold | `workspace.sdmx_gold` | Dissemination-ready tables for dashboards & sharing |
# MAGIC
# MAGIC **Volume:** `workspace.sdmx_bronze.raw_files` — raw API responses before loading into Delta.
# MAGIC
# MAGIC ## Naming Convention
# MAGIC
# MAGIC | Pattern | Example | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `sdmx_bronze.<entity>_raw` | `sdmx_bronze.dataflows_raw` | Raw data + ingestion metadata |
# MAGIC | `sdmx_silver.<entity>` | `sdmx_silver.dataflows` | Validated, typed |
# MAGIC | `sdmx_gold.<entity>` | `sdmx_gold.dataflows` | Dissemination-ready |
# MAGIC
# MAGIC All structure tables preserve `agency`, `id`, `version` from SDMX metadata. Raw files in the Volume follow `{entity}_{YYYYMMDD_HHMMSS}.{format}`.
# MAGIC
# MAGIC ## Environment
# MAGIC
# MAGIC - **Databricks Free Edition** (serverless compute, CPU)
# MAGIC - **Python** 3.12.3 · **Spark** 4.2.0
# MAGIC - **BIS API**: reachable (HTTP 501 on bare root is expected)
# MAGIC - **Packages**: `sdmx1`, `pysdmx` both install successfully