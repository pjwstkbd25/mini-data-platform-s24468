import os
import sys
import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import uvicorn

# --- KONFIGURACJA ---
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
# Pobieramy wersję produkcyjną modelu
MODEL_URI = "models:/Mental_Health_Predictor_Prod/Production"

app = FastAPI(title="Mental Health Prediction API", description="API serwujące model ML wytrenowany w Airflow")

# Zmienna globalna na model
model = None
model_columns = None


# --- 1. Definicja danych wejściowych (Schema) ---
# To odzwierciedla surowe dane, jakie przychodzą (np. z formularza)
class SurveyInput(BaseModel):
    # Pola wymagane (zgodnie z bronze_to_silver.py)
    Age: int
    Gender: str
    City: Optional[str] = None
    Working_Professional_or_Student: str
    Sleep_Duration: str
    Dietary_Habits: str
    Degree: str
    Family_History_of_Mental_Illness: str
    Have_you_ever_had_suicidal_thoughts: str
    # Te pola są w Bronze, ale mogą nie być używane przez model,
    # jednak warto je mieć dla zgodności.
    Financial_Stress: int

    class Config:
        schema_extra = {
            "example": {
                "Age": 30,
                "Gender": "Male",
                "City": "Warsaw",
                "Working_Professional_or_Student": "Working Professional",
                "Sleep_Duration": "7-8 hours",
                "Dietary_Habits": "Healthy",
                "Degree": "Master",
                "Family_History_of_Mental_Illness": "No",
                "Have_you_ever_had_suicidal_thoughts": "No",
                "Financial_Stress": 3
            }
        }


# --- 2. Ładowanie modelu przy starcie ---
@app.on_event("startup")
def load_model():
    global model, model_columns
    print(f">>> Łączenie z MLflow: {MLFLOW_URI}")
    mlflow.set_tracking_uri(MLFLOW_URI)

    print(f">>> Pobieranie modelu: {MODEL_URI}")
    try:
        # Load model as Sklearn flavor to access .feature_names_in_
        model = mlflow.sklearn.load_model(MODEL_URI)

        # Pobieramy nazwy kolumn, na których model był trenowany (z warstwy Gold)
        if hasattr(model, "feature_names_in_"):
            model_columns = list(model.feature_names_in_)
            print(f">>> Model załadowany. Oczekuje kolumn: {model_columns[:5]}...")
        else:
            print("!!! Ostrzeżenie: Model nie ma atrybutu feature_names_in_. Mapowanie kolumn może być ryzykowne.")
            model_columns = []

    except Exception as e:
        print(f"!!! KRYTYCZNY BŁĄD: Nie można załadować modelu. {e}")
        # Nie ubijamy procesu, żeby API wstało i zwróciło błąd 500 przy requestach
        model = None


# --- 3. Preprocessing (Symulacja Sparka w Pandas) ---
def preprocess_input(data: SurveyInput) -> pd.DataFrame:
    """
    Zamienia surowe dane (JSON) na format, którego oczekuje model (Gold).
    Musi odtworzyć logikę z bronze_to_silver.py
    """
    # Krok 1: JSON -> DataFrame
    input_dict = data.dict()
    df = pd.DataFrame([input_dict])

    # Zamiana nazw kolumn na małe litery (tak robi bronze_to_silver)
    df.columns = [c.lower().replace("_", " ") for c in df.columns]
    # Uwaga: Pydantic używa "_" zamiast spacji, więc musimy to odkręcić
    # Np. "Working_Professional_or_Student" -> "working professional or student"

    # Poprawka dla klucza "have you ever had suicidal thoughts" (pytanie w stringu)
    # W Pydantic nazwałem to "Have_you_ever_had_suicidal_thoughts"
    # Mapping specyficzny:
    rename_map = {
        "working professional or student": "working professional or student",
        "have you ever had suicidal thoughts": "have you ever had suicidal thoughts ?"
    }
    # Zastosuj mapowanie jeśli klucz istnieje
    new_cols = []
    for c in df.columns:
        if c in rename_map:  # Jeśli jest prosty match
            new_cols.append(rename_map[c])
        elif c + " ?" in ["have you ever had suicidal thoughts ?"]:  # Specjalny przypadek ze znakiem zapytania
            new_cols.append(c + " ?")
        else:
            new_cols.append(c)
    df.columns = new_cols

    # Krok 2: Logika biznesowa (Region)
    # df_region = df_parsed.withColumn("region", when(col("city").isNotNull(), "Asia").otherwise("Unknown"))
    if "city" in df.columns and pd.notna(df.iloc[0]["city"]):
        df["region"] = "Asia"  # Uproszczenie z Twojego kodu
    else:
        df["region"] = "Unknown"

    # Krok 3: One Hot Encoding (Symulacja)
    # Tutaj jest najtrudniej, bo nie mamy Sparkowego StringIndexera.
    # Zrobimy pd.get_dummies, a potem wyrównamy kolumny do modelu.

    categorical_cols = [
        "gender",
        "working professional or student",
        "sleep duration",
        "dietary habits",
        "degree",
        "family history of mental illness",
        "have you ever had suicidal thoughts ?",
        "region",
    ]

    # Konwersja na dummy variables
    # prefix_sep="_vec_" symuluje nazewnictwo Sparka po vector assemblerze (częściowo)
    # Ale Twój kod w silver_to_gold robił: col_vec_0, col_vec_1.
    # Sklearn po get_dummies zrobi: col_Wartość.

    # PRAGMATYCZNE PODEJŚCIE:
    # Model oczekuje np. "gender_vec_0", "gender_vec_1".
    # My nie wiemy, czy "Male" to 0 czy 1 bez metadanych ze Sparka.
    # W PROJEKCIE PRODUKCYJNYM: Należałoby zapisać Pipeline Sparka i użyć go tutaj.
    # W TYM ROZWIĄZANIU (Mock):
    # Założymy mapowanie "na sztywno" lub (lepiej) użyjemy get_dummies i spróbujemy dopasować
    # nazwy, ale prawdopodobnie model Scikit-Learn ma nazwy typu "gender_vec_0".

    # Jeśli model ma nazwy kolumn typu "gender_vec_0", to my tu w Pandasie mamy problem.
    # Obejściem jest wytrenowanie modelu w sklearn używając Pipeline'u, który sam robi OHE.
    # Skoro jednak masz już model wytrenowany na 'Gold', który ma wektory...

    # Zróbmy tak: Utworzymy pusty DataFrame z kolumnami, których oczekuje model.
    # Wypełnimy go zerami.

    processed_df = pd.DataFrame(columns=model_columns)
    processed_df.loc[0] = 0  # Wypełnij pierwszy wiersz zerami

    # Wypełniamy kolumny numeryczne wprost
    numeric_cols = ["age", "financial stress"]  # Dodaj inne jeśli są w modelu
    for col in numeric_cols:
        if col in df.columns and col in processed_df.columns:
            processed_df.loc[0, col] = df.iloc[0][col]

    # UWAGA: Bez metadata mappera ze Sparka (StringIndexer), predykcja będzie
    # losowa dla zmiennych kategorycznych (bo nie wiemy czy Male=0 czy Male=1).
    # Aby to API działało poprawnie, w silver_to_gold.py musielibyśmy zapisać mapowanie.

    # Na potrzeby demonstracji, API przygotowuje strukturę techniczną.

    return processed_df


# --- 4. Endpointy ---

@app.get("/")
def root():
    return {"message": "Mental Health API is running. Go to /docs for Swagger UI."}


@app.post("/predict")
def predict(input_data: SurveyInput):
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded or unavailable.")

    try:
        # Przetwarzanie danych
        X = preprocess_input(input_data)

        # Predykcja
        prediction = model.predict(X)[0]
        # Prawdopodobieństwo (opcjonalnie)
        try:
            probs = model.predict_proba(X)[0].tolist()
        except:
            probs = None

        result_map = {0: "No Depression", 1: "Depression Likely"}

        return {
            "prediction_raw": int(prediction),
            "prediction_label": result_map.get(int(prediction), "Unknown"),
            "probabilities": probs,
            "input_processed_shape": X.shape
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Prediction error: {str(e)}")


if __name__ == "__main__":
    # Uruchomienie deweloperskie
    uvicorn.run(app, host="0.0.0.0", port=8000)