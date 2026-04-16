from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

MONGO_URI   = "mongodb+srv://pragnaya:REDACTED_PASSWORD@dwp-cluster.mhebkb7.mongodb.net/"
DB_NAME     = "gbif_birds"
COLLECTION  = "raw_occurrences"
GBIF_URL    = "https://api.gbif.org/v1/occurrence/search"
GBIF_PARAMS = {
    "taxonKey": 212,
    "occurrenceStatus": "PRESENT",
    "hasCoordinate": "true",
    "hasGeospatialIssue": "false",
    "limit": 300,
}
TARGET      = 100_000
MIN_RECORDS = 90_000
DBT_DIR     = "/opt/airflow/dags/dbt_project"

default_args = {
    "owner":            "gbif_bias_pipeline",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="gbif_bias_pipeline",
    description="End-to-end GBIF bird observation bias detection pipeline",
    default_args=default_args,
    start_date=datetime(2026, 4, 15),
    schedule="@weekly",
    catchup=False,
    tags=["gbif", "bias", "dbt", "mongodb"],
)


def install_packages():
    import subprocess
    subprocess.run(
        ["pip", "install", "pymongo[srv]", "dnspython", "requests", "dbt-databricks", "--quiet"],
        check=True
    )
    print("All packages installed")


def ingest_gbif_to_mongo():
    import requests, time
    from pymongo import MongoClient
    from pymongo.server_api import ServerApi

    client = MongoClient(
        MONGO_URI,
        server_api=ServerApi("1"),
        serverSelectionTimeoutMS=15_000,
        tlsAllowInvalidCertificates=True,
    )
    col = client[DB_NAME][COLLECTION]
    col.create_index("gbifID", unique=True)

    existing = col.count_documents({})
    print(f"Existing records: {existing:,}")

    inserted = 0
    offset = existing

    while offset < TARGET:
        params = {**GBIF_PARAMS, "offset": offset}
        for attempt in range(3):
            try:
                resp = requests.get(GBIF_URL, params=params, timeout=30)
                if resp.status_code == 400:
                    print(f"GBIF hard limit reached at offset {offset}")
                    client.close()
                    return
                resp.raise_for_status()
                break
            except Exception as e:
                wait = 30 * (attempt + 1)
                print(f"Retry {attempt+1} after {wait}s — {e}")
                time.sleep(wait)

        results = resp.json().get("results", [])
        if not results:
            print("No more results from GBIF")
            break

        docs = []
        for r in results:
            if not r.get("species"):
                continue
            docs.append({
                "gbifID":             str(r.get("gbifID", "")),
                "species":            r.get("species"),
                "genus":              r.get("genus"),
                "family":             r.get("family"),
                "order":              r.get("order"),
                "class":              r.get("class"),
                "kingdom":            r.get("kingdom"),
                "decimalLatitude":    r.get("decimalLatitude"),
                "decimalLongitude":   r.get("decimalLongitude"),
                "countryCode":        r.get("countryCode"),
                "stateProvince":      r.get("stateProvince"),
                "eventDate":          r.get("eventDate"),
                "year":               r.get("year"),
                "month":              r.get("month"),
                "day":                r.get("day"),
                "recordedBy":         r.get("recordedBy"),
                "institutionCode":    r.get("institutionCode"),
                "basisOfRecord":      r.get("basisOfRecord"),
                "occurrenceStatus":   r.get("occurrenceStatus"),
            })

        if docs:
            try:
                result = col.insert_many(docs, ordered=False)
                inserted += len(result.inserted_ids)
            except Exception:
                pass

        offset += len(results)
        print(f"Progress: {offset:,} / {TARGET:,} — inserted {inserted:,} new records")

    total = col.count_documents({})
    client.close()
    print(f"Ingestion complete. Total in MongoDB: {total:,}")


def check_mongo_count(**context):
    from pymongo import MongoClient
    from pymongo.server_api import ServerApi

    client = MongoClient(
        MONGO_URI,
        server_api=ServerApi("1"),
        serverSelectionTimeoutMS=15_000,
        tlsAllowInvalidCertificates=True,
    )
    count = client[DB_NAME][COLLECTION].count_documents({})
    client.close()
    print(f"MongoDB record count: {count:,}")
    context["ti"].xcom_push(key="record_count", value=count)
    if count >= MIN_RECORDS:
        return "dbt_run_staging"
    return "abort_low_record_count"


task_install = PythonOperator(
    task_id="install_packages",
    python_callable=install_packages,
    dag=dag,
)

task_ingest = PythonOperator(
    task_id="ingest_gbif_to_mongo",
    python_callable=ingest_gbif_to_mongo,
    dag=dag,
    execution_timeout=timedelta(hours=3),
)

task_check = BranchPythonOperator(
    task_id="check_mongo_count",
    python_callable=check_mongo_count,
    dag=dag,
)

task_abort = EmptyOperator(
    task_id="abort_low_record_count",
    dag=dag,
)

task_dbt_staging = BashOperator(
    task_id="dbt_run_staging",
    bash_command=f"dbt run --select staging --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    dag=dag,
)

task_dbt_marts = BashOperator(
    task_id="dbt_run_marts",
    bash_command=f"dbt run --select marts --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    dag=dag,
)

task_dbt_metrics = BashOperator(
    task_id="dbt_run_metrics",
    bash_command=f"dbt run --select metrics --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    dag=dag,
)

task_dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command=f"dbt test --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    dag=dag,
)

task_done = EmptyOperator(
    task_id="pipeline_done",
    dag=dag,
)

(
    task_install
    >> task_ingest
    >> task_check
    >> [task_dbt_staging, task_abort]
)

(
    task_dbt_staging
    >> task_dbt_marts
    >> task_dbt_metrics
    >> task_dbt_test
    >> task_done
)
