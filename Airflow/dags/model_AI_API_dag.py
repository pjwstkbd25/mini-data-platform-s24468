from datetime import datetime, timedelta
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def test_api_prediction():
    """
    Wysyła request testowy do API (zakładając, że API działa na hoście 'localhost' lub nazwie serwisu w dockerze)
    """
    url = "http://localhost:8000/predict"  # Jeśli API jest w tym samym kontenerze
    # Jeśli API jest w innym serwisie docker-compose, zmień na http://nazwa_serwisu:8000

    payload = {
        "Age": 25,
        "Gender": "Female",
        "City": "Cracow",
        "Working_Professional_or_Student": "Student",
        "Sleep_Duration": "5-6 hours",
        "Dietary_Habits": "Moderate",
        "Degree": "Bachelor",
        "Family_History_of_Mental_Illness": "Yes",
        "Have_you_ever_had_suicidal_thoughts": "No",
        "Financial_Stress": 4
    }

    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        response.raise_for_status()
        print(">>> API odpowiedziało sukcesem!")
        print(f">>> Wynik: {response.json()}")
    except Exception as e:
        print(f"!!! API nie odpowiada lub zwróciło błąd: {e}")
        pass


with DAG(
        dag_id="model_api_check",
        description="Sprawdza dostępność API predykcyjnego",
        start_date=datetime(2025, 1, 1),
        schedule_interval="@daily",
        catchup=False,
        default_args=default_args,
        tags=["ml", "api", "test"],
) as dag:
    # Opcja A: Uruchamiamy API w tle (tylko do demo, w prod powinno być osobnym serwisem)
    start_api = BashOperator(
        task_id="start_api_server_background",
        bash_command="nohup python /opt/airflow/jobs/model_serving_api.py > /opt/airflow/api.log 2>&1 & echo 'API started'",
    )

    # Opcja B: Czekamy chwilę aż wstanie
    wait_for_start = BashOperator(
        task_id="wait_for_start",
        bash_command="sleep 10"
    )

    # Opcja C: Testujemy requestem
    test_prediction = PythonOperator(
        task_id="test_api_prediction",
        python_callable=test_api_prediction
    )

    start_api >> wait_for_start >> test_prediction