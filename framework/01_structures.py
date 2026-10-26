# Databricks notebook source
# MAGIC %md
# MAGIC # framework/01_structures
# MAGIC
# MAGIC Defines `load_structures(flow_id)`.
# MAGIC
# MAGIC Reads `flow_config`, fetches the DSD and its codelists from the BIS SDMX API, and
# MAGIC upserts them into the generic silver structure tables (`dsd_components`,
# MAGIC `codelists`, `codes`). Idempotent: existing rows for the DSD are deleted before
# MAGIC fresh ones are inserted, so re-running cannot duplicate structure metadata.
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

# DBTITLE 1,load_structures
def load_structures(flow_id):
    """
    Read flow_config, fetch the DSD and its codelists from the BIS SDMX API,
    and upsert them into the generic silver structure tables (dsd_components, codelists, codes).

    Args:
        flow_id (str): The flow ID from flow_config (e.g. "WS_CBPOL", "WS_TC").
    """
    # --- 1. Read flow_config ---
    config = spark.sql(f"""
        SELECT agency, version, dsd_id
        FROM {BRONZE_SCHEMA}.flow_config
        WHERE flow_id = '{flow_id}' AND enabled = true
    """).collect()

    if not config:
        print(f"[load_structures] Flow '{flow_id}' not found or not enabled.")
        return

    row = config[0]
    agency, version, dsd_id = row["agency"], row["version"], row["dsd_id"]

    # --- 2. Fetch the DSD ---
    url = f"{BASE_URL}/structure/datastructure/{agency}/{dsd_id}/{version}"
    resp = requests.get(url, params={"format": "sdmx-json"}, timeout=30)

    if resp.status_code != 200:
        print(f"[load_structures] ERROR fetching DSD: HTTP {resp.status_code}")
        print(f"  URL: {url}")
        print(f"  Response: {resp.text[:300]}")
        return

    data = resp.json()
    dsd = data["data"]["dataStructures"][0]
    dsc = dsd["dataStructureComponents"]

    # --- 3. Helpers ---
    def parse_codelist_urn(urn):
        """Parse a codelist URN into (agency, id, version)."""
        if not urn:
            return (None, None, None)
        m = re.match(r".*Codelist=([^:]+):([^(]+)\(([^)]+)\)", urn)
        return (m.group(1), m.group(2), m.group(3)) if m else (None, None, None)

    def get_code_name(code):
        """Extract the English name from a code object."""
        names = code.get("names", {})
        if isinstance(names, dict):
            return names.get("en", code.get("name", "?"))
        elif isinstance(names, list) and len(names) > 0:
            return names[0].get("text", "?")
        return code.get("name", "?")

    def get_attr_level(ar):
        """Derive attachment_level from attributeRelationship."""
        if "none" in ar:
            return "dataset"
        elif "dimensions" in ar:
            return "series"
        elif "group" in ar:
            return "group"
        elif "primaryMeasure" in ar:
            return "observation"
        else:
            return "other"

    from pyspark.sql import Row
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    # --- 3. Build component rows ---
    component_rows = []
    codelist_refs = set()

    # Dimensions
    for d in dsc["dimensionList"]["dimensions"]:
        cl_a, cl_i, cl_v = parse_codelist_urn(d.get("localRepresentation", {}).get("enumeration", ""))
        if cl_i:
            codelist_refs.add((cl_a, cl_i, cl_v))
        component_rows.append(Row(
            agency=agency, dsd_id=dsd_id, dsd_version=version,
            component_id=d["id"], component_type="dimension", position=d.get("position"),
            ordinal=len(component_rows),
            concept_name=d["id"], assignment_status=None,
            codelist_agency=cl_a, codelist_id=cl_i, codelist_version=cl_v,
            attachment_level=None,
        ))

    # Time dimensions
    for t in dsc["dimensionList"].get("timeDimensions", []):
        component_rows.append(Row(
            agency=agency, dsd_id=dsd_id, dsd_version=version,
            component_id=t["id"], component_type="time_dimension", position=t.get("position"),
            ordinal=len(component_rows),
            concept_name=t["id"], assignment_status=None,
            codelist_agency=None, codelist_id=None, codelist_version=None,
            attachment_level=None,
        ))

    # Attributes
    for a in dsc.get("attributeList", {}).get("attributes", []):
        cl_a, cl_i, cl_v = parse_codelist_urn(a.get("localRepresentation", {}).get("enumeration", ""))
        if cl_i:
            codelist_refs.add((cl_a, cl_i, cl_v))
        ar = a.get("attributeRelationship", {})
        component_rows.append(Row(
            agency=agency, dsd_id=dsd_id, dsd_version=version,
            component_id=a["id"], component_type="attribute", position=None,
            ordinal=len(component_rows),
            concept_name=a["id"], assignment_status=a.get("assignmentStatus"),
            codelist_agency=cl_a, codelist_id=cl_i, codelist_version=cl_v,
            attachment_level=get_attr_level(ar),
        ))

    # Primary measure
    pm = dsc.get("measureList", {}).get("primaryMeasure", {})
    if pm:
        component_rows.append(Row(
            agency=agency, dsd_id=dsd_id, dsd_version=version,
            component_id=pm["id"], component_type="measure", position=None,
            ordinal=len(component_rows),
            concept_name=pm["id"], assignment_status=None,
            codelist_agency=None, codelist_id=None, codelist_version=None,
            attachment_level=None,
        ))

    # --- 4. Upsert dsd_components (delete + insert) ---
    spark.sql(f"""
        DELETE FROM {SILVER_SCHEMA}.dsd_components
        WHERE dsd_id = '{dsd_id}' AND dsd_version = '{version}'
    """)

    comp_schema = StructType([
        StructField("agency", StringType(), True),
        StructField("dsd_id", StringType(), True),
        StructField("dsd_version", StringType(), True),
        StructField("component_id", StringType(), True),
        StructField("component_type", StringType(), True),
        StructField("position", IntegerType(), True),
        StructField("ordinal", IntegerType(), True),
        StructField("concept_name", StringType(), True),
        StructField("assignment_status", StringType(), True),
        StructField("codelist_agency", StringType(), True),
        StructField("codelist_id", StringType(), True),
        StructField("codelist_version", StringType(), True),
        StructField("attachment_level", StringType(), True),
    ])

    comp_df = spark.createDataFrame(component_rows, comp_schema)
    comp_df.write.format("delta").mode("append").saveAsTable(f"{SILVER_SCHEMA}.dsd_components")

    # --- 5. Fetch and upsert codelists + codes ---
    cl_schema = StructType([
        StructField("agency", StringType(), True),
        StructField("codelist_id", StringType(), True),
        StructField("version", StringType(), True),
        StructField("name", StringType(), True),
    ])

    code_schema = StructType([
        StructField("agency", StringType(), True),
        StructField("codelist_id", StringType(), True),
        StructField("version", StringType(), True),
        StructField("code_id", StringType(), True),
        StructField("code_name", StringType(), True),
    ])

    total_codes = 0
    codelist_count = 0

    for cl_agency, cl_id, cl_ver in sorted(codelist_refs):
        cl_url = f"{BASE_URL}/structure/codelist/{cl_agency}/{cl_id}/{cl_ver}"
        cl_resp = requests.get(cl_url, params={"format": "sdmx-json"}, timeout=30)

        if cl_resp.status_code != 200:
            print(f"  WARNING: codelist {cl_id} HTTP {cl_resp.status_code}, skipping")
            continue

        cl_data = cl_resp.json()
        codelists_json = cl_data.get("data", {}).get("codelists", [])
        if not codelists_json:
            print(f"  WARNING: codelist {cl_id} returned empty, skipping")
            continue

        cl_obj = codelists_json[0]
        cl_name = cl_obj.get("name", cl_id)
        codes = cl_obj.get("codes", [])

        # Delete existing codelist row, then insert
        spark.sql(f"""
            DELETE FROM {SILVER_SCHEMA}.codelists
            WHERE agency = '{cl_agency}' AND codelist_id = '{cl_id}' AND version = '{cl_ver}'
        """)
        cl_row = [Row(agency=cl_agency, codelist_id=cl_id, version=cl_ver, name=cl_name)]
        spark.createDataFrame(cl_row, cl_schema).write.format("delta").mode("append").saveAsTable(f"{SILVER_SCHEMA}.codelists")

        # Delete existing codes, then insert
        spark.sql(f"""
            DELETE FROM {SILVER_SCHEMA}.codes
            WHERE agency = '{cl_agency}' AND codelist_id = '{cl_id}' AND version = '{cl_ver}'
        """)
        code_rows = [Row(
            agency=cl_agency, codelist_id=cl_id, version=cl_ver,
            code_id=c["id"], code_name=get_code_name(c)
        ) for c in codes]
        spark.createDataFrame(code_rows, code_schema).write.format("delta").mode("append").saveAsTable(f"{SILVER_SCHEMA}.codes")

        total_codes += len(codes)
        codelist_count += 1
        print(f"  Codelist {cl_id}: {len(codes)} codes")

    # --- 6. Run summary ---
    n_dims = sum(1 for r in component_rows if r["component_type"] == "dimension")
    n_time = sum(1 for r in component_rows if r["component_type"] == "time_dimension")
    n_attrs = sum(1 for r in component_rows if r["component_type"] == "attribute")
    n_meas = sum(1 for r in component_rows if r["component_type"] == "measure")

    print(f"\n{'='*60}")
    print(f"load_structures('{flow_id}') COMPLETE")
    print(f"  DSD: {agency}:{dsd_id}({version})")
    print(f"  Components: {len(component_rows)} total")
    print(f"    Dimensions:      {n_dims}")
    print(f"    Time dimensions: {n_time}")
    print(f"    Attributes:      {n_attrs}")
    print(f"    Measures:         {n_meas}")
    print(f"  Codelists loaded:  {codelist_count}")
    print(f"  Total codes loaded: {total_codes}")
    print(f"{'='*60}")

# COMMAND ----------

print("load_structures(flow_id) defined.")
