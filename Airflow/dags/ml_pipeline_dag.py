from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

AIRFLOW_HOME = "/opt/airflow"
JOBS_DIR = f"{AIRFLOW_HOME}/jobs"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ml_training_pipeline",
    description="Wczytuje Gold -> Trenuje modele -> Loguje do MLflow -> Wybiera najlepszy",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["ml", "mlflow", "training"],
) as dag:

    train_and_evaluate = BashOperator(
        task_id="train_and_evaluate_models",
        bash_command=f"python {JOBS_DIR}/train_models.py"
    )

    train_and_evaluate