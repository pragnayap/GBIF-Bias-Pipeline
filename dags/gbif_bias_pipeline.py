from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
import sys
sys.path.insert(0, '/Users/ritchit/Desktop/Data Warehouse/DW Term Project/gbif-bias-pipeline')
from gbif_to_mongo import ingest
import os
from dotenv import load_dotenv
load_dotenv()

MONGO_URI        = os.getenv("MONGO_URI")
DB_NAME          = "gbif_birds"
COLLECTION       = "raw_occurrences"
MIN_RECORDS      = 90_000
DBT_DIR          = "/Users/ritchit/Desktop/Data Warehouse/DW Term Project/gbif-bias-pipeline/dbt_project"
DATABRICKS_HOST  = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
BRONZE_JOB_ID    = os.getenv("BRONZE_JOB_ID")
SILVER_JOB_ID    = os.getenv("SILVER_JOB_ID") 

default_args = {
    "owner": "gbif_bias_pipeline",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="gbif_bias_pipeline",
    description="GBIF bird observation bias detection pipeline",
    default_args=default_args,
    start_date=datetime(2026, 4, 15),
    schedule="@weekly",
    catchup=False,
    tags=["gbif", "bias", "dbt", "mongodb", "databricks"],
)


def check_mongo_count(**context):
    from pymongo import MongoClient
    from pymongo.server_api import ServerApi
    client = MongoClient(MONGO_URI, server_api=ServerApi("1"),
                         serverSelectionTimeoutMS=15_000, tlsAllowInvalidCertificates=True)
    count = client[DB_NAME][COLLECTION].count_documents({})
    client.close()
    print(f"MongoDB count: {count:,}")
    context["ti"].xcom_push(key="record_count", value=count)
    return "run_databricks_bronze" if count >= MIN_RECORDS else "ingest_gbif_to_mongo"


def run_databricks_job(job_id):
    import requests, time
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    run_id = requests.post(
        f"{DATABRICKS_HOST}/api/2.1/jobs/run-now",
        headers=headers, json={"job_id": int(job_id)}, timeout=30
    ).json()["run_id"]
    print(f"Databricks job {job_id} triggered — run_id: {run_id}")

    while True:
        time.sleep(30)
        state = requests.get(
            f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get?run_id={run_id}",
            headers=headers, timeout=30
        ).json()["state"]
        print(f"State: {state['life_cycle_state']}")
        if state["life_cycle_state"] in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            if state.get("result_state") != "SUCCESS":
                raise Exception(f"Databricks job {job_id} failed: {state.get('result_state')}")
            break


# ── Tasks ────────────────────────────────────────────────────────────────────

task_ingest = PythonOperator(
    task_id="ingest_gbif_to_mongo",
    python_callable=ingest,
    dag=dag,
    execution_timeout=timedelta(hours=3),
)

task_check = BranchPythonOperator(
    task_id="check_mongo_count",
    python_callable=check_mongo_count,
    dag=dag,
)

task_abort = EmptyOperator(task_id="abort_low_record_count", dag=dag)

task_bronze = PythonOperator(
    task_id="run_databricks_bronze",
    python_callable=lambda: run_databricks_job(BRONZE_JOB_ID),
    dag=dag,
    execution_timeout=timedelta(minutes=30),
    trigger_rule="none_failed_min_one_success",
)

task_silver = PythonOperator(
    task_id="run_databricks_silver",
    python_callable=lambda: run_databricks_job(SILVER_JOB_ID),
    dag=dag,
    execution_timeout=timedelta(minutes=30),
)

task_dbt_staging = BashOperator(
    task_id="dbt_run_staging",
    bash_command=f"dbt run --select staging --project-dir '{DBT_DIR}' --profiles-dir '{DBT_DIR}'",
    dag=dag,
)

task_dbt_marts = BashOperator(
    task_id="dbt_run_marts",
    bash_command=f"dbt run --select marts --project-dir '{DBT_DIR}' --profiles-dir '{DBT_DIR}'",
    dag=dag,
)

task_dbt_metrics = BashOperator(
    task_id="dbt_run_metrics",
    bash_command=f"dbt run --select metrics --project-dir '{DBT_DIR}' --profiles-dir '{DBT_DIR}'",
    dag=dag,
)

task_dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command=f"dbt test --project-dir '{DBT_DIR}' --profiles-dir '{DBT_DIR}'",
    dag=dag,
)

task_done = EmptyOperator(task_id="pipeline_done", dag=dag)

task_check >> [task_bronze, task_ingest]
task_ingest >> task_bronze
task_bronze >> task_silver >> task_dbt_staging >> task_dbt_marts >> task_dbt_metrics >> task_dbt_test >> task_done