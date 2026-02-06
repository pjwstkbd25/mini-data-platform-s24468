from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.functions import vector_to_array
import sys

# --- ŚCIEŻKI ---
SILVER_PATH = "/opt/airflow/data/datalake/silver/mental_health"
GOLD_PATH = "/opt/airflow/data/datalake/gold/mental_health"


def run_job():
    print(">>> [Start] Silver -> Gold")

    spark = SparkSession.builder \
        .appName("SilverToGold") \
        .master("local[*]") \
        .config("spark.driver.host", "localhost") \
        .getOrCreate()

    # 1. Wczytaj Silver
    print(f">>> Wczytywanie z: {SILVER_PATH}")
    try:
        df = spark.read.parquet(SILVER_PATH)
    except Exception as e:
        print(f"!!! Błąd odczytu: {e}")
        sys.exit(1)

    # 2. Target -> numeric (Label Encoding)
    # W Silver kolumna nazywa się 'depression' (mała litera)
    # Wartości to zazwyczaj 'Yes'/'No'
    print(">>> Kodowanie targetu (depression -> label)...")
    df = df.withColumn(
        "label",
        when(col("depression") == "Yes", 1)
        .when(col("depression") == "No", 0)
        .otherwise(None)  # Odrzucamy dziwne wartości
    )

    # Usuwamy wiersze, gdzie label jest nullem (jeśli jakieś były)
    df = df.filter(col("label").isNotNull())

    # 3. Rozwijanie wektorów one-hot (Flattening vectors)
    # Szukamy kolumn kończących się na '_vec'
    vec_cols = [c for c in df.columns if c.endswith("_vec")]
    print(f">>> Rozwijanie wektorów: {vec_cols}")

    for vc in vec_cols:
        arr_col = vc + "_arr"
        # Zamiana VectorUDT na Array<Double>
        df = df.withColumn(arr_col, vector_to_array(col(vc)))

        # Pobieramy rozmiar wektora (ile ma kolumn po one-hot encoding)
        # Bierzemy pierwszy wiersz, żeby sprawdzić długość tablicy
        try:
            row = df.selectExpr(f"size({arr_col}) as size").first()
            if row is None:
                continue
            size = row["size"]
        except Exception as e:
            print(f"!!! Błąd przy pobieraniu rozmiaru wektora {vc}: {e}")
            continue

        # Rozbijanie tablicy na kolumny: gender_vec_0, gender_vec_1, ...
        for i in range(size):
            new_col_name = f"{vc}_{i}"
            df = df.withColumn(new_col_name, col(arr_col)[i])

    # 4. Usuń kolumny nie-numeryczne
    # Chcemy zostawić tylko int, double, float.
    # Wyrzucamy stringi (np. 'gender', 'depression') i wektory/tablice.

    # Najpierw identyfikujemy kolumny, które chcemy zachować
    # Typy numeryczne w Sparku: IntegerType, DoubleType, FloatType, LongType
    # Prościej: odrzucamy 'string', 'array', 'vector'

    final_cols = []
    for field in df.schema.fields:
        dtype = field.dataType.simpleString()
        name = field.name

        # Pomiń wektory, tablice i stringi
        if "string" in dtype or "array" in dtype or "vector" in dtype:
            continue

        final_cols.append(name)

    print(f">>> Wybrane kolumny numeryczne do Gold: {final_cols}")
    df_final = df.select(final_cols)

    # 5. Podgląd
    print(">>> Próbka danych Gold:")
    df_final.show(5)

    # 6. Zapis Gold
    print(f">>> Zapisywanie do: {GOLD_PATH}")
    (
        df_final.write
        .mode("overwrite")
        .parquet(GOLD_PATH)
    )

    print(">>> [Sukces] Silver -> Gold zakończony.")
    spark.stop()


if __name__ == "__main__":
    run_job()
# # # jobs/silver_to_gold.py
# # import os
# # from pathlib import Path
# # from dotenv import load_dotenv
# # from pyspark.sql import SparkSession
# # from pyspark.sql.functions import col, when
# # from pyspark.ml.functions import vector_to_array
# #
# # SILVER_PATH = "/data/datalake/silver/social_media_survey"
# # GOLD_PATH = "/data/datalake/gold/ml_dataset"
# #
# #
# # def main():
# #     spark = (
# #         SparkSession.builder
# #         .appName("SilverToGold")
# #         .getOrCreate()
# #     )
# #
# #     # 1. Wczytaj Silver
# #     df = spark.read.parquet(SILVER_PATH)
# #
# #     # 2. Target → numeric
# #     df = df.withColumn(
# #         "Depression_num",
# #         when(col("Depression") == "Yes", 1)
# #         .when(col("Depression") == "No", 0)
# #         .otherwise(col("Depression").cast("int")),
# #     )
# #
# #     # 3. Rozwijanie wektorów one-hot
# #     vec_cols = [c for c in df.columns if c.endswith("_vec")]
# #
# #     for vc in vec_cols:
# #         arr_col = vc + "_arr"
# #         df = df.withColumn(arr_col, vector_to_array(col(vc)))
# #
# #         # Pobierz rozmiar wektora z pierwszego nie-nullowego wiersza
# #         size_row = (
# #             df.selectExpr(f"size({arr_col}) as size")
# #             .filter(col("size").isNotNull())
# #             .first()
# #         )
# #         if size_row is None:
# #             continue
# #         size = size_row["size"]
# #
# #         for i in range(size):
# #             df = df.withColumn(f"{vc}_{i}", col(arr_col)[i])
# #
# #     # Usuń oryginalne wektory i tymczasowe arraye
# #     drop_cols = vec_cols + [c + "_arr" for c in vec_cols]
# #     df = df.drop(*drop_cols)
# #
# #     # 4. Usuń wszystkie kolumny string/array – Gold ma być numeryczny
# #     df_numeric = df.select(
# #         [c for c, t in df.dtypes if t not in ("string", "array")]
# #     )
# #
# #     # 5. Zmień nazwę targetu
# #     df_final = df_numeric.withColumnRenamed("Depression_num", "label")
# #
# #     # 6. Zapis Gold
# #     (
# #         df_final.write
# #         .mode("overwrite")
# #         .parquet(GOLD_PATH)
# #     )
# #
# #     spark.stop()
# #
# #
# # if __name__ == "__main__":
# #     main()
# # jobs/silver_to_gold.py
# import os
# from pathlib import Path
# from dotenv import load_dotenv
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, when
# from pyspark.ml.functions import vector_to_array
#
# # -------------------- Env / Paths --------------------
# # BASE_DIR -> folder "Airflow"
# BASE_DIR = Path(__file__).resolve().parents[1]
# ENV_PATH = BASE_DIR / ".env"
# if ENV_PATH.exists():
#     load_dotenv(dotenv_path=ENV_PATH)
#
# # W kontenerze Spark: /opt/airflow/data -> /data
# DATA_DIR = os.getenv("DATA_DIR", "/data")
# DATALAKE_DIR = os.path.join(DATA_DIR, "datalake")
#
# SILVER_PATH = os.path.join(DATALAKE_DIR, "silver", "social_media_survey")
# GOLD_PATH = os.path.join(DATALAKE_DIR, "gold", "ml_dataset")
#
#
# def main():
#     spark = (
#         SparkSession.builder
#         .appName("SilverToGold")
#         .getOrCreate()
#     )
#
#     # 1. Wczytaj Silver
#     df = spark.read.parquet(SILVER_PATH)
#
#     # 2. Target → numeric
#     df = df.withColumn(
#         "Depression_num",
#         when(col("Depression") == "Yes", 1)
#         .when(col("Depression") == "No", 0)
#         .otherwise(col("Depression").cast("int")),
#     )
#
#     # 3. Rozwijanie wektorów one-hot
#     vec_cols = [c for c in df.columns if c.endswith("_vec")]
#
#     for vc in vec_cols:
#         arr_col = vc + "_arr"
#         df = df.withColumn(arr_col, vector_to_array(col(vc)))
#
#         # Pobierz rozmiar wektora z pierwszego nie-nullowego wiersza
#         size_row = (
#             df.selectExpr(f"size({arr_col}) as size")
#             .filter(col("size").isNotNull())
#             .first()
#         )
#         if size_row is None:
#             continue
#         size = size_row["size"]
#
#         for i in range(size):
#             df = df.withColumn(f"{vc}_{i}", col(arr_col)[i])
#
#     # Usuń oryginalne wektory i tymczasowe arraye
#     drop_cols = vec_cols + [c + "_arr" for c in vec_cols]
#     df = df.drop(*drop_cols)
#
#     # 4. Usuń wszystkie kolumny string/array – Gold ma być numeryczny
#     df_numeric = df.select(
#         [c for c, t in df.dtypes if t not in ("string", "array")]
#     )
#
#     # 5. Zmień nazwę targetu
#     df_final = df_numeric.withColumnRenamed("Depression_num", "label")
#
#     # 6. Zapis Gold
#     (
#         df_final.write
#         .mode("overwrite")
#         .parquet(GOLD_PATH)
#     )
#
#     spark.stop()
#
#
# if __name__ == "__main__":
#     main()
