# GBIF Bird Observation Geographic Sampling Bias Pipeline

> End-to-end automated data pipeline detecting geographic sampling imbalance in global bird occurrence data.

**Team:** Rutuja Rajendra Saste · Pragnaya Priyadarshini  
**Course:** DATA Warehousing (DATA 230) - San José State University, Spring 2026

---

## The Problem

GBIF aggregates over 1 billion biodiversity records from citizen scientists worldwide. Despite this scale, spatial coverage is deeply uneven:

| Continent | Observations | Avg obs / 5° cell | Bias Score |
|---|---|---|---|
| Europe | 50,337 | 762 | +53 (Oversampled) |
| North America | 28,131 | 327 | +11 |
| Asia | 7,639 | 100 | -21 (Severely undersampled) |
| South America | 4,550 | 84 | -19 |
| Africa | 2,522 | 79 | -14 |

**Europe accounts for 51% of all observations. Africa accounts for 2.5%.** This bias affects IUCN Red List classifications, habitat protection decisions, and climate research - all of which depend on GBIF data.

---

## Solution

An automated pipeline that embeds bias measurement as a **first-class output** - not a post-hoc analysis. Every weekly Airflow run produces a fresh z-score ranked report of the world's most undersampled regions.

---

## Architecture

```
GBIF API
    ↓  Python ingestion (pymongo, retry logic)
MongoDB Atlas  ← raw semi-structured documents
    ↓  Databricks Notebook 1
Bronze Delta Table  ← structured, unfiltered
    ↓  Databricks Notebook 2
Silver Delta Table  ← 6 quality filters + 5 grid cell columns
    ↓  dbt Core
Star Schema Warehouse
├── dim_species       (1,400 rows)
├── dim_geography     (462 grid cells)
├── dim_date          (12 rows)
├── fact_observations (98,951 rows)
├── bias_metrics      (462 rows - z-scores + rankings)
└── monthly_distribution (97 rows)
    ↓  Power BI
Geographic Bias Dashboard (5 visuals, live Databricks connection)
    ↑
Apache Airflow @weekly (9-task DAG)
```

---

## Tech Stack

| Layer | Tool | Why chosen |
|---|---|---|
| Ingestion | Python 3.12 + pymongo | GBIF records are semi-structured - MongoDB handles variable fields without null-column overhead |
| Raw storage | MongoDB Atlas M0 | Document store for semi-structured GBIF records |
| Processing | **Databricks Free Edition + PySpark** | New tool - Delta Lake ACID storage, medallion architecture, Unity Catalog |
| Transformation | dbt Core 1.11.6 | SQL-first warehouse modeling with automated testing and lineage |
| Orchestration | Apache Airflow 2.9+ (Docker) | 9-task DAG with branch logic and DatabricksRunNowOperator |
| Visualisation | **Power BI** (browser) | New tool - Azure Maps, live ODBC to Databricks, geographic bubble visual |
| Version control | GitHub | Public repo with branch strategy (main + pipeline-dev) |

---

## Repository Structure

```
gbif-bias-pipeline/
├── dags/
│   └── gbif_bias_pipeline.py          # Airflow DAG - 9 tasks, @weekly schedule
├── notebooks/
│   ├── 01_mongo_to_bronze.ipynb       # Databricks: MongoDB → Bronze Delta table
│   └── 02_bronze_to_silver.ipynb      # Databricks: Bronze → Silver (6 filters + grid cells)
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_occurrences.sql    # Column rename + row_number() dedup
│   │   │   └── sources.yml
│   │   ├── marts/
│   │   │   ├── dim_species.sql        # Full taxonomy hierarchy
│   │   │   ├── dim_geography.sql      # 5° grid cells + continent assignment
│   │   │   ├── dim_date.sql           # Year/month with season
│   │   │   └── fact_observations.sql  # Central fact table
│   │   └── metrics/
│   │       ├── bias_metrics.sql       # Z-scores, rankings, bias categories
│   │       └── monthly_distribution.sql
│   ├── schema.yml                     # 37 data quality tests
│   ├── dbt_project.yml
│   ├── profiles.yml.example           # Credentials template - copy to profiles.yml
│   └── packages.yml
├── gbif_to_mongo.py                   # GBIF ingestion script
├── .env.example                       # Environment variables template
├── .gitignore
└── README.md
```

---

## Setup

### Prerequisites

```
Python 3.12+  |  Docker Desktop  |  MongoDB Atlas account (free)
Databricks Free Edition workspace  |  Power BI account (free)
```

### Installation

```bash
git clone https://github.com/pragnayap/GBIF-Bias-Pipeline.git
cd GBIF-Bias-Pipeline

python3 -m venv venv
source venv/bin/activate
pip install pymongo[srv] dnspython requests dbt-databricks apache-airflow-providers-databricks

cp .env.example .env
# Edit .env with your real credentials

cp dbt_project/profiles.yml.example dbt_project/profiles.yml
# Edit profiles.yml with your Databricks credentials
```

### Environment Variables

```bash
# .env
MONGO_URI=mongodb+srv://USERNAME:PASSWORD@cluster.mongodb.net/gbif_birds
DATABRICKS_HOST=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your_warehouse_id
DATABRICKS_TOKEN=your_databricks_personal_access_token
```

---

## Running the Pipeline

### Step 1 - Ingest from GBIF (one-time, ~6 hours)

```bash
source venv/bin/activate
python3 gbif_to_mongo.py --total 100000
```

> GBIF API has a hard offset limit of 100,000 records. The script has built-in exponential backoff retry (30s, 60s, 90s) and a `--resume-from` flag to continue after interruption.

### Step 2 - Run Databricks notebooks

```
Databricks → Open notebooks/01_mongo_to_bronze.ipynb → Run All
Databricks → Open notebooks/02_bronze_to_silver.ipynb → Run All
```

### Step 3 - Run dbt

```bash
cd dbt_project
dbt deps          # install dbt_utils package
dbt run           # build all 7 models
dbt test          # run 37 data quality tests
```

Expected output:
```
dbt run:  Done. PASS=7  WARN=0  ERROR=0  TOTAL=7
dbt test: Done. PASS=37 WARN=0  ERROR=0  TOTAL=37
```

### Step 4 - Airflow (automated weekly)

```bash
# Add Databricks connection
docker exec airflow-airflow-scheduler-1 airflow connections add \
    databricks_default --conn-type http \
    --conn-host https://YOUR_WORKSPACE.cloud.databricks.com \
    --conn-password YOUR_DATABRICKS_TOKEN

# Start Airflow
cd /path/to/airflow
docker compose up -d

# Open localhost:8080 → trigger gbif_bias_pipeline DAG
```

---

## Airflow DAG - 9 Tasks

```
install_packages
    → ingest_gbif_to_mongo        PythonOperator  - fetches from GBIF API
    → check_mongo_count           BranchPythonOperator - validates ≥ 90k records
    → [databricks_bronze]         DatabricksRunNowOperator - triggers Notebook 1
    → [abort_low_record_count]    EmptyOperator - aborts if count too low
    → databricks_silver           DatabricksRunNowOperator - triggers Notebook 2
    → dbt_run_staging             BashOperator
    → dbt_run_marts               BashOperator
    → dbt_run_metrics             BashOperator
    → dbt_test                    BashOperator - fails pipeline if any test fails
    → pipeline_done               EmptyOperator
```

**Schedule:** `@weekly`  
**Branch logic:** aborts entire pipeline if MongoDB count < 90,000 records

---

## dbt Models

| Model | Type | Rows | Key design decision |
|---|---|---|---|
| stg_occurrences | View | 98,951 | row_number() dedup on occurrence_id |
| dim_species | Table | ~1,400 | MD5 surrogate key on species name |
| dim_geography | Table | 462 | GROUP BY + first() - not SELECT DISTINCT (multiple countries per 5° cell) |
| dim_date | Table | ~12 | year/month with season and decade |
| fact_observations | Table | 98,951 | FK to all 3 dims, row_number() dedup |
| bias_metrics | Table | 462 | Aggregates from fact only - no dim joins inside COUNT |
| monthly_distribution | Table | 97 | Observations by month × continent |

### 37 Data Quality Tests

| Type | Count | What it validates |
|---|---|---|
| unique | 10 | No duplicate primary keys |
| not_null | 20 | Key fields never missing |
| relationships | 3 | Every FK in fact resolves to a valid dimension row |
| accepted_values | 4 | bias_category and continent contain only valid values |

---

## Bias Metrics

### Z-Score Formula

```
sampling_zscore = (cell_observations − global_average) / global_standard_deviation
```

### Bias Categories - Percentile-Based Thresholds

Thresholds are derived from the actual observation distribution (p25=6, p50=27.5, p75=135) rather than fixed z-score cutoffs. The data has extreme right skew - the mean (214 obs/cell) is inflated by European outliers while the median is only 27.5 obs/cell. Fixed z-score thresholds would classify almost nothing as undersampled.

| Category | Threshold | Cells | Real-world meaning |
|---|---|---|---|
| Severely undersampled | ≤ 6 obs | 119 | A 550×400km area with 1–6 total bird sightings |
| Moderately undersampled | 7–27 obs | 120 | Some data but insufficient for analysis |
| Near average | 28–500 obs | 141 | Reasonable citizen science coverage |
| Oversampled | > 500 obs | 82 | European citizen science hotspots |

---

## Continent Boundaries

Non-overlapping bounding boxes derived from:
- **Natural Earth 1:110m cultural vectors** - naturalearthdata.com (public domain)
- **GBIF continent processing documentation** - techdocs.gbif.org/en/data-processing

**Limitation:** Approximate rectangles. ~5-10 edge cells near continental borders may be misassigned.  
**Future work:** Point-in-polygon lookup using Natural Earth shapefile.

---

## New Tools Introduced

### Databricks + Delta Lake
Not covered in any homework assignment. Key features used:
- Delta Lake - ACID-compliant columnar storage with time travel
- Medallion architecture - Bronze/Silver layered data quality progression
- Unity Catalog - workspace-level catalog management
- Serverless PySpark - distributed processing without cluster management

### Power BI
Not used in any homework assignment. Key features used:
- Azure Maps visual - geographic bubble map with bias category color coding
- Live ODBC connection to Databricks SQL Warehouse
- DatabricksRunNowOperator integration ensures dashboard reflects latest pipeline run

---

## Key Challenges and Fixes

| Challenge | Fix |
|---|---|
| GBIF API terminated at ~9k records | Exponential backoff retry (30s, 60s, 90s) + `--resume-from` flag |
| MongoDB SSL handshake failure from Databricks | `tlsAllowInvalidCertificates=True` + IP whitelist 0.0.0.0/0 |
| Spark Connector unavailable (no Maven on Free Edition) | pymongo → pandas → spark.createDataFrame() |
| dbt catalog error: 'main' not found | Fixed `~/.dbt/profiles.yml` catalog from `main` to `workspace` |
| Silver table produced 0 clean rows | Year filter upper bound was 2025 - fixed to 2026 |
| dim_geography: 40 duplicate rows per geo_key | GROUP BY grid_cell + first() instead of SELECT DISTINCT |
| bias_metrics 10x observation inflation | Count from fact_observations only - removed dim joins inside COUNT |
| GBIF API 100k hard limit | Date range filtering via `lastInterpreted` for incremental runs |

---

## Future Work

- **Incremental ingestion** - use GBIF `lastInterpreted` date filter to fetch only new records each week
- **Point-in-polygon continent assignment** - replace bounding boxes with Natural Earth shapefile
- **Real-time streaming** - Kafka ingestion for near-real-time bias detection
- **Automated Power BI refresh** - trigger via REST API at `pipeline_done` task
- **Expand to all taxa** - run the same pipeline for all GBIF species beyond birds

---

## References

- Callaghan, C. T., et al. (2021). Three Frontiers for the Future of Biodiversity Research Using Citizen Science Data. *BioScience*, 71(1), 55–63.
- Global Biodiversity Information Facility. (2026). GBIF Occurrence Search API. https://api.gbif.org/v1/occurrence/search
- Natural Earth. (2023). 1:110m Cultural Vectors. https://naturalearthdata.com
- GBIF Technical Documentation. (2024). Continent Processing. https://techdocs.gbif.org/en/data-processing
- Kimball, R. & Ross, M. (2013). *The Data Warehouse Toolkit* (3rd ed.). Wiley.

---

## Relevance - UN SDG 15

This project supports **UN Sustainable Development Goal 15 (Life on Land)**. GBIF data is used by the IUCN Red List, national conservation agencies, and climate researchers. A pipeline that systematically identifies the most undersampled regions gives field researchers a data-driven list of where to direct observation effort - making bias measurement automated, reproducible, and actionable.
