# GBIF Bird Observation Geographic Sampling Bias Pipeline

End-to-end data pipeline detecting geographic sampling imbalance in global bird occurrence data.

**Team:** Pragnaya Priyadarshini | Rutuja Rajendra Saste  
**Course:** Data Warehousing — San José State University, Spring 2026

---

## Problem

GBIF aggregates over 1 billion biodiversity records from citizen scientists worldwide. Despite this scale, spatial coverage is deeply uneven. Europe averages **196 observations per 5° grid cell** while Africa averages **10** — a **19-fold difference**. This geographic bias directly affects conservation decisions, species distribution models, and habitat protection policy.

---

## Key Finding

| Continent | Observations | Avg obs/cell | Bias Score |
|---|---|---|---|
| Europe | 50,337 | 762 | +53 (Oversampled) |
| North America | 28,131 | 327 | +11 |
| Asia | 7,639 | 100 | -21 (Severely undersampled) |
| South America | 4,550 | 84 | -19 |
| Africa | 2,522 | 79 | -14 |

---

## Architecture

```
GBIF API
    ↓
Python Ingestion (gbif_to_mongo.py)
    ↓
MongoDB Atlas (raw_occurrences)
    ↓
Databricks Bronze (PySpark Delta)
    ↓
Databricks Silver (cleaned + grid cells)
    ↓
dbt Star Schema (staging → marts → metrics)
    ↓
Power BI Dashboard
    ↑
Apache Airflow @weekly DAG (orchestrates all steps)
```

| Layer | Tool | Output | Rows |
|---|---|---|---|
| Raw storage | MongoDB Atlas M0 | gbif_birds.raw_occurrences | 99,967 |
| Bronze | Databricks PySpark | workspace.gbif.bronze_occurrences | 99,967 |
| Silver | Databricks PySpark | workspace.gbif.silver_occurrences | 98,951 |
| Staging | dbt view | marts_staging.stg_occurrences | 98,951 |
| Marts | dbt tables | dim_species, dim_geography, dim_date, fact_observations | — |
| Metrics | dbt tables | bias_metrics, monthly_distribution | 462, 97 |
| Visualisation | Power BI | Geographic bias dashboard | — |
| Orchestration | Apache Airflow | gbif_bias_pipeline DAG (9 tasks) | — |

---

## Repository Structure

```
gbif-bias-pipeline/
├── dags/
│   └── gbif_bias_pipeline.py      # Airflow DAG — 9 tasks
├── notebooks/
│   ├── 01_mongo_to_bronze.py      # Databricks: MongoDB → Bronze Delta
│   └── 02_bronze_to_silver.py     # Databricks: Bronze → Silver Delta
├── dbt_project/
│   ├── models/
│   │   ├── staging/               # stg_occurrences
│   │   ├── marts/                 # dim_*, fact_observations
│   │   └── metrics/               # bias_metrics, monthly_distribution
│   ├── schema.yml                 # 37 data quality tests
│   ├── dbt_project.yml
│   ├── profiles.yml.example       # credentials template
│   └── packages.yml
├── gbif_to_mongo.py               # GBIF ingestion script
├── .env.example                   # environment variables template
├── .gitignore
└── README.md
```

---

## Setup

### Prerequisites

```
Python 3.12  |  MongoDB Atlas account  |  Databricks Free Edition  |  Power BI Web
```

### Installation

```bash
git clone https://github.com/pragnayap/GBIF-Bias-Pipeline.git
cd GBIF-Bias-Pipeline

# Python 3.12 required — Airflow 3.x not compatible with Python 3.13
python3.12 -m venv venv
source venv/bin/activate

pip install apache-airflow==3.2.1
pip install pymongo requests pandas dbt-core dbt-databricks python-dotenv

cp .env.example .env
cp dbt_project/profiles.yml.example dbt_project/profiles.yml
# Fill in credentials in both files
```

### Environment Variables (.env)

```
MONGO_URI=mongodb+srv://USERNAME:PASSWORD@cluster.mongodb.net/gbif_birds
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your_databricks_personal_access_token
BRONZE_JOB_ID=your_bronze_job_id
SILVER_JOB_ID=your_silver_job_id
```

---

## Running the Pipeline

### Step 1 — Ingest from GBIF (one-time, ~6 hours due to GBIF 100k API limit)

```bash
source venv/bin/activate
python3 gbif_to_mongo.py --total 100000
```

> If MongoDB already has >= 90,000 records, the Airflow DAG skips ingestion automatically on subsequent runs.

### Step 2 — Run Databricks notebooks

Upload `notebooks/01_mongo_to_bronze.py` and `notebooks/02_bronze_to_silver.py` to your Databricks workspace and run them as jobs. Credentials are read from Databricks Secrets scope `gbif`.

```
Job 1: MongoDB → Bronze Delta table
Job 2: Bronze → Silver Delta table (6 quality filters + grid cells)
```

### Step 3 — Run dbt

```bash
cd dbt_project
dbt deps
dbt run    # builds 7 models
dbt test   # runs 37 data quality tests
```

### Step 4 — Airflow (automated @weekly)

```bash
cd /path/to/gbif-bias-pipeline
source venv/bin/activate
export AIRFLOW_HOME=~/airflow
airflow db migrate      # first time only
airflow standalone
```

Open `http://localhost:8080` and trigger the `gbif_bias_pipeline` DAG.

> Keep the terminal running — closing it stops Airflow. Open a new tab (`Cmd+T`) for other commands.

### Step 5 — Refresh Power BI

After pipeline completes, manually refresh the dashboard:

```
app.powerbi.com → GBIF Bird Observation report → click Refresh
```

> Power BI free tier does not support scheduled auto-refresh. The pipeline updates Databricks tables automatically — only the Power BI pull step requires a manual click.

---

## dbt Models

| Model | Type | Rows | Purpose |
|---|---|---|---|
| stg_occurrences | View | 98,951 | Column rename + deduplication |
| dim_species | Table | ~1,400 | Full taxonomy hierarchy |
| dim_geography | Table | 462 | 5° grid cells with continent assignment |
| dim_date | Table | ~12 | Year/month with season |
| fact_observations | Table | 98,951 | Central fact table with FK to all dims |
| bias_metrics | Table | 462 | Z-scores, rankings, bias categories |
| monthly_distribution | Table | 97 | Observations by month × continent |

### Data Quality — 37/37 Tests Passing

| Type | Count | Checks |
|---|---|---|
| unique | 10 | No duplicate primary keys |
| not_null | 20 | Key fields never missing |
| relationships | 3 | FK integrity — every fact row resolves to a valid dimension |
| accepted_values | 4 | bias_category and continent contain only valid values |

---

## Bias Metrics

### Sampling Z-Score

```
z = (cell_observations − global_average) / global_standard_deviation
```

### Bias Categories (percentile-based thresholds)

| Category | Threshold | Cells |
|---|---|---|
| Severely undersampled | ≤ 6 observations | 119 cells |
| Moderately undersampled | 7–27 observations | 120 cells |
| Near average | 28–500 observations | 141 cells |
| Oversampled | > 500 observations | 82 cells |

Percentile-based thresholds (p25=6, p50=27, p75=135) are used instead of fixed z-score cutoffs because the data has extreme right skew — the mean (214 obs/cell) is heavily inflated by European outliers while the median is only 27.5 obs/cell.

---

## Airflow DAG — 9 Tasks

```
check_mongo_count
      ↓                    ↓
run_databricks_bronze   abort_low_record_count
      ↓
run_databricks_silver
      ↓
dbt_run_staging
      ↓
dbt_run_marts
      ↓
dbt_run_metrics
      ↓
dbt_test
      ↓
pipeline_done
```

Schedule: `@weekly`  
Branch logic: skips to Databricks if MongoDB >= 90,000 records, aborts if below threshold.

---

## Implementation Challenges

| Challenge | Fix |
|---|---|
| GBIF 503 errors at ~9k records | Exponential backoff retry (30s, 60s, 90s) |
| MongoDB SSL handshake failure from Databricks | tlsAllowInvalidCertificates=True |
| Spark Connector unavailable on Free Edition | pymongo → pandas → spark.createDataFrame() |
| dbt Hive Metastore disabled | catalog: workspace in profiles.yml |
| Silver 0 rows | Year filter upper bound fixed from 2025 to 2026 |
| dim_geography duplicates (31 rows per cell) | GROUP BY grid_cell + first() aggregation |
| bias_metrics fan-out (10x inflation) | Removed dim joins — count directly from fact |
| Airflow Python incompatibility | Recreated venv with Python 3.12 via Homebrew |
| Power BI no auto-refresh | Manual refresh after each pipeline run (free tier) |

---

## Continent Boundaries

Non-overlapping bounding boxes derived from Natural Earth 1:110m cultural vectors (naturalearthdata.com) and cross-referenced with GBIF continent processing documentation (techdocs.gbif.org/en/data-processing).

**Limitation:** ~5-10 edge cells near continental borders may be misassigned. Future work: point-in-polygon lookup using Natural Earth shapefile.

---

## Future Work

- True incremental GBIF ingestion using `lastInterpreted` date filter (current: weekly full re-check)
- Migrate Databricks triggers to official `DatabricksRunNowOperator` (current: custom PythonOperator)
- Automated Power BI dashboard refresh via REST API triggered at pipeline completion (current: manual)
- Point-in-polygon continent assignment using Natural Earth shapefile (current: approximate bounding boxes)
- Real-time ingestion via Kafka streaming
- Expand to additional GBIF taxa beyond birds (requires upgraded compute tier)

---

## References

- Callaghan, C. T., et al. (2021). Three Frontiers for the Future of Biodiversity Research Using Citizen Science Data. *BioScience*, 71(1), 55–63.
- GBIF Secretariat. (2026). GBIF Occurrence Search API. https://api.gbif.org/v1/occurrence/search
- Natural Earth. (2023). 1:110m Cultural Vectors. https://naturalearthdata.com
- GBIF Technical Documentation. (2024). Continent Processing. https://techdocs.gbif.org/en/data-processing
- Kimball, R. & Ross, M. (2013). *The Data Warehouse Toolkit* (3rd ed.). Wiley.

---

## Relevance

Supports UN SDG 15 (Life on Land). GBIF data is used by the IUCN Red List, national conservation agencies, and climate researchers. A pipeline that systematically identifies the most undersampled regions gives field researchers a data-driven list of where to direct observation effort.