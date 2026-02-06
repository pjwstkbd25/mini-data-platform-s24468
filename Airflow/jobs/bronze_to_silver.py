from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, get_json_object, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.ml.feature import StringIndexer, OneHotEncoder
import sys

# --- ŚCIEŻKI ---
BRONZE_PATH = "/opt/airflow/data/datalake/bronze/mental_health"
SILVER_PATH = "/opt/airflow/data/datalake/silver/mental_health"


def run_job():
    print(">>> [Start] Bronze -> Silver")

    spark = SparkSession.builder \
        .appName("BronzeToSilver") \
        .master("local[*]") \
        .config("spark.driver.host", "localhost") \
        .getOrCreate()

    # 1. Wczytanie Bronze (Parquet)
    print(f">>> Wczytywanie z: {BRONZE_PATH}")
    try:
        df = spark.read.parquet(BRONZE_PATH)
    except Exception as e:
        print(f"!!! Błąd odczytu: {e}")
        sys.exit(1)

    # 2. Parsowanie JSON (payload.after)
    df_json = df.withColumn("after_json", get_json_object(col("kafka_value"), "$.payload.after"))

    # 3. Schemat danych (Dostosowany do wielkości liter z CSV!)
    # Debezium wysyła klucze tak jak są w bazie. Skoro Pandas wgrał je jako "Name", musimy tu użyć "Name".
    survey_schema = StructType([
        StructField("Name", StringType(), True),
        StructField("Gender", StringType(), True),
        StructField("Age", IntegerType(), True),
        StructField("City", StringType(), True),
        StructField("Working Professional or Student", StringType(), True),
        StructField("Profession", StringType(), True),
        StructField("Academic Pressure", IntegerType(), True),
        StructField("Work Pressure", IntegerType(), True),
        StructField("CGPA", IntegerType(), True),
        StructField("Study Satisfaction", IntegerType(), True),
        StructField("Job Satisfaction", IntegerType(), True),
        StructField("Sleep Duration", StringType(), True),
        StructField("Dietary Habits", StringType(), True),
        StructField("Degree", StringType(), True),
        StructField("Have you ever had suicidal thoughts ?", StringType(), True),
        StructField("Work/Study Hours", IntegerType(), True),
        StructField("Financial Stress", IntegerType(), True),
        StructField("Family History of Mental Illness", StringType(), True),
        StructField("Depression", StringType(), True),
        # id jest z małej litery, bo dodane przez ALTER TABLE
        StructField("id", IntegerType(), True)
    ])

    # Rozpakowanie JSON
    df_parsed = df_json.select(
        from_json(col("after_json"), survey_schema).alias("data")
    ).select("data.*")

    # --- NORMALIZACJA NAZW KOLUMN ---
    # Zmieniamy nazwy kolumn na małe litery ("Name" -> "name"), żeby łatwiej się pracowało
    for column_name in df_parsed.columns:
        df_parsed = df_parsed.withColumnRenamed(column_name, column_name.lower())

    print(">>> Próbka danych po naprawie schematu:")
    df_parsed.show(5)

    # Sprawdzenie czy mamy dane (jeśli nadal są NULLe, przerywamy)
    if df_parsed.filter(col("name").isNotNull()).count() == 0:
        print("!!! ERROR: Nadal same NULLe. Sprawdź dokładnie nazwy pól w JSONie.")
        # Opcjonalnie: odkomentuj linię niżej, żeby zobaczyć surowy JSON w logach i znaleźć literówkę
        # df_json.select("after_json").show(1, truncate=False)
        sys.exit(1)

    # 4. Feature Engineering / Cleaning

    # Dodanie Region
    df_region = df_parsed.withColumn(
        "region",
        when(col("city").isNotNull(), "Asia").otherwise("Unknown")
    )

    # Drop niepotrzebnych kolumn
    df_clean = df_region.drop("name", "city", "id")

    # Filtrowanie (teraz używamy małych liter, bo zrobiliśmy rename pętlą wyżej)
    df_clean = df_clean.filter(col("age").between(15, 100))

    # 5. ML Features: StringIndexer + OneHotEncoder
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

    df_processed = df_clean

    # String Indexer
    for col_name in categorical_cols:
        indexer = StringIndexer(
            inputCol=col_name,
            outputCol=col_name + "_idx",
            handleInvalid="keep"
        )
        model = indexer.fit(df_processed)
        df_processed = model.transform(df_processed)

    # One Hot Encoder
    ohe = OneHotEncoder(
        inputCols=[c + "_idx" for c in categorical_cols],
        outputCols=[c + "_vec" for c in categorical_cols]
    )
    ohe_model = ohe.fit(df_processed)
    df_final = ohe_model.transform(df_processed)

    print(">>> Wynikowe dane (Silver):")
    df_final.select("age", "gender", "gender_vec", "depression").show(5)

    # 6. Zapis do Silver
    print(f">>> Zapisywanie do: {SILVER_PATH}")
    (
        df_final.write
        .mode("overwrite")
        .parquet(SILVER_PATH)
    )

    print(">>> [Sukces] Bronze -> Silver zakończony.")
    spark.stop()


if __name__ == "__main__":
    run_job()