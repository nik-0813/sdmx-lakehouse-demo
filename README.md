# SDMX Lakehouse

Independent personal project: an SDMX data pipeline on Databricks Free Edition. Ingests public statistical data from the [BIS SDMX RESTful API](https://stats.bis.org/api-doc/v2/), validates it against Data Structure Definitions (DSDs) and codelists, and prepares it for dissemination through a medallion architecture.

> **Disclaimer:** This is an independent project using publicly available data. It is not affiliated with, endorsed by, or representative of the Bank for International Settlements. No BIS logos or branding are used.

## Architecture

```
BIS SDMX REST API
        |
        v
  +-----------+     +-----------+     +-----------+
  |  Bronze    | --> |  Silver    | --> |  Gold      |
  | (raw CSV)  |     | (typed,   |     | (dashboards|
  | + metadata |     |  validated)|     |  sharing)  |
  +-----------+     +-----------+     +-----------+
  sdmx_bronze       sdmx_silver       sdmx_gold
```

| Layer | Schema | Purpose |
| --- | --- | --- |
| Bronze | `workspace.sdmx_bronze` | Raw ingested data + metadata (`source_url`, `ingested_at`) |
| Silver | `workspace.sdmx_silver` | Validated, typed data conforming to DSD structures |
| Gold | `workspace.sdmx_gold` | Dissemination-ready tables for dashboards & sharing |

**Volume:** `workspace.sdmx_bronze.raw_files` — raw API responses before loading into Delta.

## Notebooks

| Phase | Notebook | Status | Description |
| --- | --- | --- | --- |
| Overview | [`00_Overview_BIS_Data_Pipeline`](notebooks/00_Overview_BIS_Data_Pipeline.py) | ✅ Done | Project index with links to all phase notebooks |
| 0 | [`00_Environment_Check`](notebooks/00_Environment_Check.py) | ✅ Done | Verify catalog, connectivity, Python packages |
| 1 | [`01_Workspace_Setup`](notebooks/01_Workspace_Setup.py) | ✅ Done | Create schemas, volume, naming conventions |
| 2 | [`02_Structures`](notebooks/02_Structures.py) | ✅ Done | List dataflows, retrieve DSD & codelists |
| 3 | [`03_Ingest_Dataflow`](notebooks/03_Ingest_Dataflow.py) | ✅ Done | Ingest a dataflow slice into bronze Delta |
| 4 | _04_Config_and_Generalisation_ | 🔧 In Progress | Generalise ingestion to multiple dataflows |

## Key Learnings

- **BIS API uses `structure/{type}/...` pattern** (not standard SDMX 2.1 top-level resources)
- **Data endpoint:** `/data/dataflow/{agency}/{resourceID}/{version}/{key}?format=csv`
- **Euro-area countries** (DE, FR, IT) have no individual series in WS_CBPOL — data is under code **XM** (Euro area)
- **Bronze = all strings** (faithful to raw CSV); silver does typing/casting
- **DSD JSON structure:** `dimensionList`, `measureList` (with `primaryMeasure`), `attributeList` (with `assignmentStatus`)
- `file://` prefix fails on UC Volumes in serverless — use bare `/Volumes/...` path

## Dataflows (Priority Order)

| Priority | Dataflow ID | Name | DSD |
| --- | --- | --- | --- |
| 1 | WS_CBPOL | Central bank policy rates | BIS_CBPOL |
| 2 | WS_CBTA | Central bank total assets | CBTA |
| 3 | WS_TC | Total credit | BIS_TOTAL_CREDIT |
| 4 | WS_EER | Effective exchange rates | BIS_EER |
| 5 | WS_DSR | Debt service ratios | BIS_DSR |

## Repository Structure

```
 sdmx-lakehouse-demo/
   README.md                <- this file
   notebooks/               <- Databricks notebooks (00-0x, phase-based)
   src/                    <- shared validator code (future)
   tests/                  <- validator unit tests (future)
   docs/                   <- architecture diagrams, screenshots, SDMX glossary
   .gitignore
   LICENSE                 <- Apache 2.0
```

## Environment

- **Databricks Free Edition** (serverless compute, CPU)
- **Python** 3.12.3 · **Spark** 4.2.0
- **BIS API**: `https://stats.bis.org/api/v2`
- **Packages**: `sdmx1`, `pysdmx`

## License

Apache License 2.0 — see [LICENSE](LICENSE).
