# Databricks notebook source
# MAGIC %md
# MAGIC # framework/_discover_cbta
# MAGIC
# MAGIC Scratch notebook: loads the `WS_CBTA` structures and reports its dimensions and their
# MAGIC codelists, so a valid series key can be chosen before ingesting. Reads only the BIS
# MAGIC structure endpoints; no data is fetched.

# COMMAND ----------

# MAGIC %run ./01_structures

# COMMAND ----------

load_structures("WS_CBTA")

# COMMAND ----------

# DBTITLE 1,Dimensions in key order
spark.sql("""
    SELECT dc.ordinal, dc.component_id, dc.codelist_id, COUNT(c.code_id) AS code_count
    FROM workspace.sdmx_silver.dsd_components dc
    LEFT JOIN workspace.sdmx_silver.codes c
      ON dc.codelist_id = c.codelist_id
     AND dc.codelist_agency = c.agency
     AND dc.codelist_version = c.version
    WHERE dc.dsd_id = 'CBTA' AND dc.component_type = 'dimension'
    GROUP BY dc.ordinal, dc.component_id, dc.codelist_id
    ORDER BY dc.ordinal
""").show(50, truncate=False)

# COMMAND ----------

# DBTITLE 1,Attributes and their attachment levels
spark.sql("""
    SELECT ordinal, component_id, component_type, assignment_status, attachment_level
    FROM workspace.sdmx_silver.dsd_components
    WHERE dsd_id = 'CBTA' AND component_type IN ('attribute', 'measure', 'time_dimension')
    ORDER BY ordinal
""").show(50, truncate=False)

# COMMAND ----------

# DBTITLE 1,Candidate codes for the smaller dimensions
spark.sql("""
    SELECT dc.component_id, c.code_id, c.code_name
    FROM workspace.sdmx_silver.dsd_components dc
    JOIN workspace.sdmx_silver.codes c
      ON dc.codelist_id = c.codelist_id
     AND dc.codelist_agency = c.agency
     AND dc.codelist_version = c.version
    WHERE dc.dsd_id = 'CBTA' AND dc.component_type = 'dimension'
      AND dc.codelist_id NOT IN ('CL_AREA')
    ORDER BY dc.ordinal, c.code_id
""").show(100, truncate=False)

# COMMAND ----------

# DBTITLE 1,Is the euro area / US / JP available?
spark.sql("""
    SELECT c.code_id, c.code_name
    FROM workspace.sdmx_silver.dsd_components dc
    JOIN workspace.sdmx_silver.codes c
      ON dc.codelist_id = c.codelist_id
     AND dc.codelist_agency = c.agency
     AND dc.codelist_version = c.version
    WHERE dc.dsd_id = 'CBTA' AND dc.codelist_id = 'CL_AREA'
      AND c.code_id IN ('US', 'XM', 'JP', 'GB', 'CH', 'DE')
    ORDER BY c.code_id
""").show(20, truncate=False)
