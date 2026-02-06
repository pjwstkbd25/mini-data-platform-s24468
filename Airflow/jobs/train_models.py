import os
import sys
import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
from pathlib import Path
import traceback

from sklearn.model_selection import cross_validate, KFold
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.naive_bayes import GaussianNB
from sklearn.metrics import make_scorer, accuracy_score, f1_score
# Imputer nie jest tu użyty wprost, bo robimy dropna, ale warto mieć import
from sklearn.impute import SimpleImputer

# --- KONFIGURACJA ---
AIRFLOW_HOME = Path("/opt/airflow")
GOLD_PATH = AIRFLOW_HOME / "data/datalake/gold/mental_health"
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
EXPERIMENT_NAME = "Mental_Health_Prediction"

os.environ["GIT_PYTHON_REFRESH"] = "quiet"


def run_job():
    print(">>> [Start] Model Training & Evaluation")

    print(f">>> Łączenie z MLflow: {MLFLOW_URI}")
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    print(f">>> Wczytywanie danych z: {GOLD_PATH}")
    try:
        df = pd.read_parquet(GOLD_PATH)
    except Exception as e:
        print(f"!!! Błąd odczytu danych: {e}")
        sys.exit(1)

    print(f">>> Dane wczytane. Rozmiar początkowy: {df.shape}")

    nan_counts = df.isnull().sum()
    bad_cols = nan_counts[nan_counts == len(df)]  # Kolumny gdzie liczba pustych = liczba wierszy

    if not bad_cols.empty:
        cols_to_drop = bad_cols.index.tolist()
        print(f">>> Usuwanie całkowicie pustych kolumn: {cols_to_drop}")
        df = df.drop(columns=cols_to_drop)

    if df.isnull().values.any():
        print(f"!!! Wykryto NaN w wierszach. Usuwam wiersze z brakami...")
        df = df.dropna()

    print(f">>> Rozmiar danych po czyszczeniu: {df.shape}")

    if len(df) < 10:
        print("!!! Zbyt mało danych do treningu. Prerywam.")
        sys.exit(1)

    if "label" not in df.columns:
        print("!!! Brak kolumny 'label' w danych Gold.")
        sys.exit(1)

    X = df.drop(columns=["label"])
    y = df["label"]

    # 5. Modele
    cv = KFold(n_splits=5, shuffle=True, random_state=42)
    scoring = {
        "accuracy": make_scorer(accuracy_score),
        "f1_macro": make_scorer(f1_score, average="macro", zero_division=0)
    }

    models = {
        "LogisticRegression": LogisticRegression(solver='saga', max_iter=2000),
        "RandomForest": RandomForestClassifier(n_estimators=100, random_state=42),
        "GradientBoosting": GradientBoostingClassifier(n_estimators=100, random_state=42),
        "SVC": SVC(),
        "NaiveBayes": GaussianNB()
    }

    best_model_name = None
    best_f1_score = -1.0
    best_model_obj = None

    print(">>> Rozpoczynanie treningu modeli...")

    for name, model in models.items():
        with mlflow.start_run(run_name=f"Train_{name}"):
            print(f"   Trening: {name}...")

            try:
                results = cross_validate(model, X, y, cv=cv, scoring=scoring, return_train_score=False)
                mean_acc = results["test_accuracy"].mean()
                mean_f1 = results["test_f1_macro"].mean()

                print(f"      -> Accuracy: {mean_acc:.4f}, F1 Macro: {mean_f1:.4f}")

                mlflow.log_param("model_type", name)
                mlflow.log_metric("accuracy_mean", mean_acc)
                mlflow.log_metric("f1_macro_mean", mean_f1)

                model.fit(X, y)

                if mean_f1 > best_f1_score:
                    best_f1_score = mean_f1
                    best_model_name = name
                    best_model_obj = model

                try:
                    signature = mlflow.models.infer_signature(X, model.predict(X))
                    mlflow.sklearn.log_model(model, "model", signature=signature)
                except Exception as log_err:
                    print(f"!!! OSTRZEŻENIE: Nie udało się zapisać pliku modelu do MLflow. Błąd: {log_err}")
                    print("!!! Kontynuuję proces (metryki zostały zapisane).")

            except Exception as e:
                print(f"!!! Krytyczny błąd treningu {name}: {e}")
                traceback.print_exc()
                continue

    print("=" * 50)
    if best_model_name:
        print(f"🎉 ZWYCIĘZCA: {best_model_name} z F1 Score: {best_f1_score:.4f}")

        with mlflow.start_run(run_name="Best_Model_Registration"):
            mlflow.log_param("winner", best_model_name)
            mlflow.log_metric("best_f1", best_f1_score)

            try:
                signature = mlflow.models.infer_signature(X, best_model_obj.predict(X))
                mlflow.sklearn.log_model(
                    best_model_obj,
                    "model",
                    signature=signature,
                    registered_model_name="Mental_Health_Predictor_Prod"
                )
                print(f">>> Zarejestrowano model w MLflow Registry.")
            except Exception as e:
                print(f"!!! OSTRZEŻENIE: Nie udało się zarejestrować modelu. Błąd: {e}")

    else:
        print("!!! Nie udało się wytrenować żadnego modelu.")
        sys.exit(1)

    print("=" * 50)


if __name__ == "__main__":
    run_job()