from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

AIRFLOW_HOME = "/opt/airflow"
DAGS_DIR     = f"{AIRFLOW_HOME}/dags"
SECRETS_DIR  = f"{AIRFLOW_HOME}/secrets"
DATASETS_DIR = f"{AIRFLOW_HOME}/data/datasets"

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="kaggle_datagen_to_postgres",
    description="Pobiera dane z Kaggle i (opcjonalnie) ładuje do Postgres",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["kaggle", "postgres", "ingestion"],
) as dag:

    make_dirs = BashOperator(
        task_id="make_dirs",
        bash_command=(
            f"mkdir -p {SECRETS_DIR} {DATASETS_DIR} && "
            f"chmod 600 {SECRETS_DIR}/kaggle.json || true && "
            f"ls -la {SECRETS_DIR} {DATASETS_DIR}"
        ),
    )

    download_data = BashOperator(
        task_id="download_data",
        bash_command=f'python "{DAGS_DIR}/download_data.py"',
    )
    transform_to_postgresql = BashOperator(
        task_id="transform_to_postgresql",
        bash_command=f'python "{DAGS_DIR}/transform_to_postgresql.py"',
    )

    make_dirs >> download_data >>transform_to_postgresql
