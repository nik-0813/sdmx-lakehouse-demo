# Databricks notebook source
# DBTITLE 1,02 — Structures First
# MAGIC %md
# MAGIC # 02 — Structures First
# MAGIC
# MAGIC Part of the **SDMX Lakehouse** project. This notebook:
# MAGIC 1. Lists all available BIS dataflows from the SDMX RESTful API
# MAGIC 2. For a dataflow chosen by the user, retrieves its DSD (dimensions, attributes) and codelists
# MAGIC 3. Stores the structures as Delta tables in Unity Catalog
# MAGIC 4. Summarizes the DSD in plain English
# MAGIC
# MAGIC > **Disclaimer:** Independent project using publicly available data. Not affiliated with the Bank for International Settlements.

# COMMAND ----------

# DBTITLE 1,Install sdmx1
# Install sdmx1 library (needed on fresh serverless compute)
import subprocess
result = subprocess.run(["pip", "install", "sdmx1", "--quiet"], capture_output=True, text=True, timeout=60)
print("sdmx1 installed" if result.returncode == 0 else f"FAILED: {result.stderr[:300]}")

# COMMAND ----------

# DBTITLE 1,Explore BIS API dataflow endpoints
# --- 2.0: Explore the BIS SDMX API to discover correct endpoints ---
# Test several SDMX 2.1 REST API patterns for listing dataflows.
# We test, observe the response, and then use the working endpoint.

import requests
import json

BASE = "https://stats.bis.org/api/v2"

# Pattern 1: SDMX 2.1 REST - all dataflows, latest version, JSON format
urls_to_test = [
    ("dataflow/all/all/latest (jsondata)", f"{BASE}/dataflow/all/all/latest?references=none&format=jsondata"),
    ("dataflow/BIS/all/latest (jsondata)", f"{BASE}/dataflow/BIS/all/latest?references=none&format=jsondata"),
    ("dataflow (no params)", f"{BASE}/dataflow"),
]

for label, url in urls_to_test:
    print(f"\n{'='*60}")
    print(f"Testing: {label}")
    print(f"URL: {url}")
    try:
        resp = requests.get(url, timeout=15, headers={"Accept": "application/json"})
        print(f"Status: {resp.status_code}")
        print(f"Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
        # Show first 500 chars of response
        text = resp.text[:500] if resp.text else "(empty)"
        print(f"Response (first 500 chars):\n{text}")
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")

# COMMAND ----------

# DBTITLE 1,Explore alternative BIS API endpoints
# --- 2.0b: Try alternative resource types and sdmx1 library ---
import requests
import json

BASE = "https://stats.bis.org/api/v2"

# The 'dataflow' resource returned 501. Try other SDMX resource types.
# Also try the sdmx1 library which may know BIS endpoints.

# 1) Try 'structure' (catch-all for structural metadata in some APIs)
# 2) Try 'datastructure' directly
# 3) Try 'data' resource (the actual data endpoint)
# 4) Try fetching API docs page

tests = [
    ("structure/all/all/latest", f"{BASE}/structure/all/all/latest?references=none&format=sdmx-json"),
    ("structure/BIS/all/latest", f"{BASE}/structure/BIS/all/latest?references=none&format=sdmx-json"),
    ("datastructure/all/all/latest", f"{BASE}/datastructure/all/all/latest?references=none&format=sdmx-json"),
    ("data/BIS/all/all/latest", f"{BASE}/data/BIS/all/all/latest?format=sdmx-json"),
    ("API docs page", "https://stats.bis.org/api-doc/v2/"),
]

for label, url in tests:
    print(f"\n{'='*60}")
    print(f"Testing: {label}")
    print(f"URL: {url}")
    try:
        resp = requests.get(url, timeout=15, headers={"Accept": "application/json"})
        print(f"Status: {resp.status_code}")
        print(f"Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
        ct = resp.headers.get('Content-Type', '')
        if 'json' in ct.lower():
            print(f"Response (first 800 chars):\n{resp.text[:800]}")
        elif 'html' in ct.lower():
            # Extract title from HTML
            import re
            title = re.search(r'<title>(.*?)</title>', resp.text, re.DOTALL)
            print(f"HTML title: {title.group(1).strip() if title else 'N/A'}")
            print(f"Response (first 500 chars):\n{resp.text[:500]}")
        else:
            print(f"Response (first 500 chars):\n{resp.text[:500]}")
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")

# COMMAND ----------

# DBTITLE 1,Fetch OpenAPI spec and test structure types
# --- 2.0c: Fetch OpenAPI spec and try structure/{type}/... pattern ---
import requests
import json

BASE = "https://stats.bis.org/api/v2"

# 1) Try to fetch the OpenAPI/Swagger spec
spec_urls = [
    "https://stats.bis.org/api-doc/v2/openapi.json",
    "https://stats.bis.org/api-doc/v2/swagger.json",
    "https://stats.bis.org/api-doc/v2/api-docs",
]

for url in spec_urls:
    print(f"\nTesting: {url}")
    try:
        resp = requests.get(url, timeout=15)
        print(f"  Status: {resp.status_code}, Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
        if resp.status_code == 200 and 'json' in resp.headers.get('Content-Type', '').lower():
            print(f"  FOUND! Response (first 1500 chars):")
            print(resp.text[:1500])
            break
    except Exception as e:
        print(f"  ERROR: {e}")

print("\n" + "="*60)
print("Trying structure/{type}/... patterns")
print("="*60)

# 2) Try structure/{structureType}/{agency}/{id}/{version}
#    where structureType = datastructure, dataflow, codelist
structure_tests = [
    ("structure/dataflow/all/all/latest", f"{BASE}/structure/dataflow/all/all/latest?references=none&format=sdmx-json"),
    ("structure/datastructure/all/all/latest", f"{BASE}/structure/datastructure/all/all/latest?references=none&format=sdmx-json"),
    ("structure/codelist/all/all/latest", f"{BASE}/structure/codelist/all/all/latest?references=none&format=sdmx-json"),
]

for label, url in structure_tests:
    print(f"\nTesting: {label}")
    try:
        resp = requests.get(url, timeout=15, headers={"Accept": "application/json"})
        print(f"  Status: {resp.status_code}")
        print(f"  Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
        text = resp.text[:800] if resp.text else "(empty)"
        print(f"  Response: {text}")
    except Exception as e:
        print(f"  ERROR: {type(e).__name__}: {e}")

# COMMAND ----------

# DBTITLE 1,API Discovery Summary
# MAGIC %md
# MAGIC ### API Discovery Summary
# MAGIC
# MAGIC The BIS SDMX API v2 uses the `structure` resource with a **structure type** as the first path parameter:
# MAGIC
# MAGIC | Endpoint | Returns | Status |
# MAGIC | --- | --- | --- |
# MAGIC | `/structure/dataflow/all/all/latest` | All dataflows | ✅ 200 JSON |
# MAGIC | `/structure/datastructure/all/all/latest` | All DSDs | ✅ 200 JSON |
# MAGIC | `/structure/codelist/all/all/latest` | All codelists | ✅ 200 JSON |
# MAGIC
# MAGIC **Note:** The standard SDMX 2.1 resources (`dataflow`, `datastructure`, `codelist` as top-level paths) return 501 "not supported". BIS wraps them under `structure/{type}/...` instead.
# MAGIC
# MAGIC Format: `?format=sdmx-json` returns SDMX-JSON structure messages.

# COMMAND ----------

# DBTITLE 1,Fetch and display BIS dataflows
# --- 2.1: Fetch all BIS dataflows and display as a table ---
import requests
import json
from pyspark.sql.types import StructType, StructField, StringType

DATAFLOW_URL = "https://stats.bis.org/api/v2/structure/dataflow/all/all/latest?references=none&format=sdmx-json"

resp = requests.get(DATAFLOW_URL, timeout=30, headers={"Accept": "application/json"})
print(f"HTTP status: {resp.status_code}")

# Parse the SDMX-JSON response
data = resp.json()
dataflows = data.get("data", {}).get("dataflows", [])
print(f"Total dataflows found: {len(dataflows)}")

# Build a list of rows for Spark DataFrame
rows = []
for df in dataflows:
    rows.append((
        df.get("agencyID", ""),
        df.get("id", ""),
        df.get("version", ""),
        df.get("names", {}).get("en", df.get("name", "")),
        df.get("structure", "")  # URN of the linked DSD
    ))

# Create Spark DataFrame
schema = StructType([
    StructField("agency", StringType(), True),
    StructField("id", StringType(), True),
    StructField("version", StringType(), True),
    StructField("name", StringType(), True),
    StructField("dsd_urn", StringType(), True),
])

dataflows_df = spark.createDataFrame(rows, schema=schema)
print(f"\nDataFrame created with {dataflows_df.count()} dataflows")
print("\nFirst 20 dataflows:")
display(dataflows_df.limit(20))

# COMMAND ----------

# DBTITLE 1,Show remaining dataflows
# --- 2.1b: Show remaining dataflows (21-32) ---
print("Dataflows 21-32:")
display(dataflows_df.offset(20).limit(12))

# COMMAND ----------

# DBTITLE 1,Step 2: DSD for WS_CBPOL
# MAGIC %md
# MAGIC ## Step 2: Retrieve DSD for `WS_CBPOL` — Central Bank Policy Rates
# MAGIC
# MAGIC **Dataflow:** `WS_CBPOL` (agency: BIS, version: 1.0)
# MAGIC **DSD:** `BIS:BIS_CBPOL(1.0)`
# MAGIC
# MAGIC We fetch the Data Structure Definition with `references=all` so codelists and concepts are included in a single API call. From the DSD we extract:
# MAGIC - **Dimensions** — the key dimensions that identify each series
# MAGIC - **Attributes** — metadata attached to observations or series
# MAGIC - **Codelists** — the valid codes for each dimension/attribute
# MAGIC - **Measures** — the measured value (typically OBS_VALUE)

# COMMAND ----------

# DBTITLE 1,Fetch DSD for WS_CBPOL
# --- 2.2: Fetch the DSD for WS_CBPOL with all references ---
import requests
import json

# Fetch the datastructure with references=all (includes codelists, concepts)
DSD_URL = "https://stats.bis.org/api/v2/structure/datastructure/BIS/BIS_CBPOL/1.0?references=all&format=sdmx-json"

resp = requests.get(DSD_URL, timeout=30, headers={"Accept": "application/json"})
print(f"HTTP status: {resp.status_code}")

raw = resp.json()

# Inspect top-level keys
print(f"\nTop-level keys: {list(raw.keys())}")
data = raw.get("data", {})
print(f"Data keys: {list(data.keys())}")

# Count structures
for key in data:
    items = data[key]
    if isinstance(items, list):
        print(f"  {key}: {len(items)} items")
    else:
        print(f"  {key}: {type(items).__name__}")

# Get the DSD object
dsds = data.get("dataStructures", [])
if dsds:
    dsd = dsds[0]
    print(f"\nDSD id: {dsd.get('id')}")
    print(f"DSD name: {dsd.get('names', {}).get('en', 'N/A')}")
    print(f"DSD agency: {dsd.get('agencyID')}")
    print(f"DSD version: {dsd.get('version')}")
    
    # Inspect components
    comps = dsd.get("dataStructureComponents", {})
    print(f"\nComponent groups: {list(comps.keys())}")
    
    # Dimensions
    dims = comps.get("dimensionList", {}).get("dimensions", [])
    print(f"Dimensions: {len(dims)}")
    for d in dims:
        print(f"  - {d.get('id')} (position {d.get('position')})")
    
    # Attributes
    attrs = comps.get("attributeList", {}).get("attributes", [])
    print(f"Attributes: {len(attrs)}")
    for a in attrs:
        print(f"  - {a.get('id')} (usage: {a.get('assignmentLevel', 'N/A')})")
    
    # Measures
    measures = comps.get("measureList", {}).get("measures", [])
    print(f"Measures: {len(measures)}")
    for m in measures:
        print(f"  - {m.get('id')}: {m.get('names', {}).get('en', 'N/A')}")

# Codelists
cls = data.get("codelists", [])
print(f"\nCodelists: {len(cls)}")
for cl in cls:
    codes = cl.get("codes", [])
    print(f"  - {cl.get('id')} v{cl.get('version')} ({cl.get('agencyID')}): {len(codes)} codes")

# COMMAND ----------

# DBTITLE 1,Step 3: Parse and store structures
# MAGIC %md
# MAGIC ## Step 3: Parse DSD components and store as Delta tables
# MAGIC
# MAGIC We extract dimensions, attributes, codelists, and codes from the DSD response, then persist them as Delta tables in `sdmx_silver` (structural metadata is already clean from the API). We also store the 32 dataflows in `sdmx_bronze` with ingestion metadata.

# COMMAND ----------

# DBTITLE 1,Parse DSD dimensions and attributes
# --- 2.3: Parse dimensions and attributes from the DSD ---
# The 'raw' and 'dsd' variables from cell 2.2 are still in memory.

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime

# Extract concept names from conceptSchemes for richer display
concept_schemes = data.get("conceptSchemes", [])
concept_map = {}  # concept_urn -> name
for cs in concept_schemes:
    for concept in cs.get("concepts", []):
        urn = cs.get("urn", "")
        # SDMX URNs for concepts are like urn:sdmx:org.sdmx.infomodel.conceptscheme.ConceptScheme=BIS:CS_CBPOL(1.0).FREQ
        # We match by concept ID within the scheme
        concept_map[concept.get("id", "")] = concept.get("names", {}).get("en", concept.get("id", ""))

# Parse dimensions
dim_rows = []
for d in dims:
    codelist_urn = d.get("localRepresentation", {}).get("enumeration", "")
    # Extract codelist ID from URN (last part before version)
    cl_id = codelist_urn.split("=")[-1].split("(")[0] if codelist_urn else ""
    dim_rows.append((
        "BIS", dsd.get("id", ""), dsd.get("version", ""),
        "dimension", d.get("id", ""), d.get("position", 0),
        concept_map.get(d.get("id", ""), d.get("id", "")),
        codelist_urn, cl_id
    ))

# Parse attributes
attr_rows = []
for a in attrs:
    codelist_urn = a.get("localRepresentation", {}).get("enumeration", "")
    cl_id = codelist_urn.split("=")[-1].split("(")[0] if codelist_urn else ""
    attr_rows.append((
        "BIS", dsd.get("id", ""), dsd.get("version", ""),
        "attribute", a.get("id", ""), 0,  # attributes don't have a position
        concept_map.get(a.get("id", ""), a.get("id", "")),
        codelist_urn, cl_id
    ))

# Combine into one DataFrame
comp_schema = StructType([
    StructField("agency", StringType(), True),
    StructField("dsd_id", StringType(), True),
    StructField("dsd_version", StringType(), True),
    StructField("component_type", StringType(), True),  # 'dimension' or 'attribute'
    StructField("component_id", StringType(), True),
    StructField("position", IntegerType(), True),
    StructField("concept_name", StringType(), True),
    StructField("codelist_urn", StringType(), True),
    StructField("codelist_id", StringType(), True),
])

dsd_components_df = spark.createDataFrame(dim_rows + attr_rows, schema=comp_schema)

print(f"DSD components: {dsd_components_df.count()} rows ({len(dim_rows)} dimensions, {len(attr_rows)} attributes)")
print("\nDimensions and Attributes:")
display(dsd_components_df)

# COMMAND ----------

# DBTITLE 1,Parse codelists and codes
# --- 2.4: Parse codelists and codes from the DSD response ---

# Parse codelist metadata
cl_rows = []
code_rows = []

for cl in cls:
    cl_rows.append((
        cl.get("agencyID", ""),
        cl.get("id", ""),
        cl.get("version", ""),
        cl.get("names", {}).get("en", cl.get("name", "")),
    ))
    for code in cl.get("codes", []):
        code_rows.append((
            cl.get("agencyID", ""),
            cl.get("id", ""),
            cl.get("version", ""),
            code.get("id", ""),
            code.get("names", {}).get("en", code.get("name", "")),
        ))

# Codelists DataFrame
cl_schema = StructType([
    StructField("agency", StringType(), True),
    StructField("codelist_id", StringType(), True),
    StructField("version", StringType(), True),
    StructField("name", StringType(), True),
])
codelists_df = spark.createDataFrame(cl_rows, schema=cl_schema)

# Codes DataFrame
code_schema = StructType([
    StructField("agency", StringType(), True),
    StructField("codelist_id", StringType(), True),
    StructField("version", StringType(), True),
    StructField("code_id", StringType(), True),
    StructField("code_name", StringType(), True),
])
codes_df = spark.createDataFrame(code_rows, schema=code_schema)

print(f"Codelists: {codelists_df.count()}")
print(f"Total codes: {codes_df.count()}")
print("\nCodelists:")
display(codelists_df)

print("\nSample codes from CL_FREQ (Frequency):")
display(codes_df.filter("codelist_id = 'CL_FREQ'").orderBy("code_id"))

# COMMAND ----------

# DBTITLE 1,Store structures as Delta tables
# --- 2.5: Store all structures as Delta tables (idempotent) ---
from datetime import datetime
from pyspark.sql.functions import lit, current_timestamp

ingestion_ts = datetime.utcnow().isoformat()

# 1) dataflows_raw in sdmx_bronze — add ingestion metadata
dataflows_bronze = dataflows_df.withColumn("source_url", lit(DATAFLOW_URL)) \
    .withColumn("ingested_at", current_timestamp())

dataflows_bronze.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.sdmx_bronze.dataflows_raw")
print(f"Stored: workspace.sdmx_bronze.dataflows_raw ({dataflows_bronze.count()} rows)")

# 2) dsd_components in sdmx_silver
dsd_components_df.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.sdmx_silver.dsd_components")
print(f"Stored: workspace.sdmx_silver.dsd_components ({dsd_components_df.count()} rows)")

# 3) codelists in sdmx_silver
codelists_df.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.sdmx_silver.codelists")
print(f"Stored: workspace.sdmx_silver.codelists ({codelists_df.count()} rows)")

# 4) codes in sdmx_silver
codes_df.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.sdmx_silver.codes")
print(f"Stored: workspace.sdmx_silver.codes ({codes_df.count()} rows)")

# Verify tables exist
print("\n--- Verification: Tables created ---")
for tbl in ["workspace.sdmx_bronze.dataflows_raw",
            "workspace.sdmx_silver.dsd_components",
            "workspace.sdmx_silver.codelists",
            "workspace.sdmx_silver.codes"]:
    cnt = spark.sql(f"SELECT COUNT(*) AS n FROM {tbl}").collect()[0]["n"]
    print(f"  {tbl}: {cnt} rows")

# COMMAND ----------

# DBTITLE 1,DSD Summary in plain English
# MAGIC %md
# MAGIC ## DSD Summary — `BIS_CBPOL` (Central Bank Policy Rates)
# MAGIC
# MAGIC ### Dimensions (2)
# MAGIC
# MAGIC Each observation series in this dataflow is uniquely identified by two key dimensions:
# MAGIC
# MAGIC | Dimension | Description | Codelist | Codes |
# MAGIC | --- | --- | --- | --- |
# MAGIC | `FREQ` | Frequency of the data (e.g. monthly, quarterly, annual) | CL_FREQ | 8 |
# MAGIC | `REF_AREA` | Reference area / country (e.g. US, DE, JP) | CL_BIS_GL_REF_AREA | 239 |
# MAGIC
# MAGIC **Series key example:** `FREQ=M&REF_AREA=US` → Monthly US policy rate.
# MAGIC
# MAGIC ### Attributes (11)
# MAGIC
# MAGIC Attributes provide metadata about each observation or series. Six are backed by codelists (constrained values), five are free-text:
# MAGIC
# MAGIC | Attribute | Description | Codelist | Codes |
# MAGIC | --- | --- | --- | --- |
# MAGIC | `TIME_FORMAT` | How the time period is represented (e.g. YYYY-MM) | CL_TIME_FORMAT | 19 |
# MAGIC | `OBS_STATUS` | Quality/normality of the observation (e.g. normal, missing) | CL_OBS_STATUS | 16 |
# MAGIC | `OBS_CONF` | Confidentiality status of the observation | CL_CONF_STATUS | 5 |
# MAGIC | `DECIMALS` | Number of decimal places to display | CL_DECIMALS | 17 |
# MAGIC | `UNIT_MEASURE` | Unit of measurement (e.g. percent) | CL_BIS_UNIT | 1105 |
# MAGIC | `UNIT_MULT` | Unit multiplier (e.g. thousands, millions) | CL_UNIT_MULT | 11 |
# MAGIC | `OBS_PRE_BREAK` | Pre-break observation value (free text) | — | — |
# MAGIC | `COMPILATION` | Compilation notes (free text) | — | — |
# MAGIC | `SOURCE_REF` | Publication source reference (free text) | — | — |
# MAGIC | `SUPP_INFO_BREAKS` | Supplemental information and breaks (free text) | — | — |
# MAGIC | `TITLE` | Series title (free text) | — | — |
# MAGIC
# MAGIC ### Codelists (9, total 1,423 codes)
# MAGIC
# MAGIC The DSD references 9 codelists with a total of 1,423 codes. The largest is `CL_BIS_UNIT` (1,105 codes) covering all BIS units of measurement, and `CL_BIS_GL_REF_AREA` (239 codes) covering all reference areas/countries.
# MAGIC
# MAGIC ### Plain English Summary
# MAGIC
# MAGIC The **Central Bank Policy Rates** dataflow (`WS_CBPOL`) is a relatively simple 2-dimensional dataset. Each time series is identified by a **frequency** (monthly, quarterly, annual, etc.) and a **reference area** (country or region, e.g. US, Germany, Japan). The observed value is the policy rate set by each central bank. 11 attributes provide context: the unit of measurement, number of decimals, observation status (e.g. normal, missing), confidentiality, and free-text notes about compilation and data breaks. This structure is ideal for a portfolio demo because it has a small number of dimensions, a rich set of attributes, and manageable codelist sizes.