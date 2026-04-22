# GBIF Bird Observation Bias Pipeline

End-to-end data pipeline detecting geographic sampling bias in bird observations.

## Stack
- Apache Airflow — orchestration
- MongoDB Atlas — raw data storage
- Databricks — bronze/silver transformations
- dbt — data modeling
- Power BI — visualization

## Setup
1. Clone repo
2. Create venv: `python3.12 -m venv venv`
3. Activate: `source venv/bin/activate`
4. Install: `pip install -r requirements.txt`
5. Copy `.env.example` to `.env` and fill in credentials
6. Run: `airflow standalone`
