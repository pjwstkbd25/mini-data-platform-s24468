from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

AIRFLOW_HOME = "/opt/airflow"
DAGS_DIR     = f"{AIRFLOW_HOME}/dags"
SECRETS_DIR  = f"{AIRFLOW_HOME}/secrets"
DATASETS_DIR = f"{AIRFLOW_HOME}/data/datasets"
SCRIPT_PATH  = f"{DAGS_DIR}/datagen.py"

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

    run_datagen = BashOperator(
        task_id="run_datagen",
        bash_command=f'python "{SCRIPT_PATH}"',
        env={
            "KAGGLE_CONFIG_DIR": SECRETS_DIR,
            "KAGGLE_DEST_DIR": DATASETS_DIR,
            "KAGGLE_DATASET": "sumansharmadataworld/depression-surveydataset-for-analysis",
            "KAGGLE_FILES": "",
            "PG_HOST": "host.docker.internal",
            "PG_PORT": "5433",
            "PG_USER": "Jarek",
            "PG_PASSWORD": "Jarek",
            "PG_DB": "data",
            "PG_SCHEMA": "public",
        },
    )

    make_dirs >> run_datagen
