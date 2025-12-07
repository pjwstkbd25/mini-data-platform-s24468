from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

AIRFLOW_HOME = "/opt/airflow"
JOBS_DIR = f"{AIRFLOW_HOME}/jobs"

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
        dag_id="datalake_pipeline",
        description="Pipeline: Kafka -> Bronze -> Silver -> QC -> Gold -> QC",
        start_date=datetime(2025, 1, 1),
        schedule_interval=None,
        catchup=False,
        default_args=default_args,
        tags=["spark", "datalake", "kafka"],
) as dag:
    # 1. Kafka -> Bronze
    kafka_to_bronze = BashOperator(
        task_id="kafka_to_bronze",
        bash_command=f"python {JOBS_DIR}/kafka_to_bronze.py"
    )

    # 2. Bronze -> Silver
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"python {JOBS_DIR}/bronze_to_silver.py"
    )

    # 3. Silver Quality Check
    silver_quality_check = BashOperator(
        task_id="silver_quality_check",
        bash_command=f"python {JOBS_DIR}/silver_quality_check.py"
    )

    # 4. Silver -> Gold (Transformacja pod ML)
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"python {JOBS_DIR}/silver_to_gold.py"
    )

    # 5. Gold Quality Check (Finalna weryfikacja)
    gold_quality_check = BashOperator(
        task_id="gold_quality_check",
        bash_command=f"python {JOBS_DIR}/gold_quality_check.py"
    )

    # Definicja pełnego przepływu
    kafka_to_bronze >> bronze_to_silver >> silver_quality_check >> silver_to_gold >> gold_quality_check
