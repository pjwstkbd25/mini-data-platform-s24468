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

    print(f">>> Wczytywanie z: {SILVER_PATH}")
    try:
        df = spark.read.parquet(SILVER_PATH)
    except Exception as e:
        print(f"!!! Błąd odczytu: {e}")
        sys.exit(1)

    print(">>> Kodowanie targetu (depression -> label)...")
    df = df.withColumn(
        "label",
        when(col("depression") == "Yes", 1)
        .when(col("depression") == "No", 0)
        .otherwise(None)  # Odrzucamy dziwne wartości
    )

    df = df.filter(col("label").isNotNull())

    vec_cols = [c for c in df.columns if c.endswith("_vec")]
    print(f">>> Rozwijanie wektorów: {vec_cols}")

    for vc in vec_cols:
        arr_col = vc + "_arr"
        df = df.withColumn(arr_col, vector_to_array(col(vc)))

        try:
            row = df.selectExpr(f"size({arr_col}) as size").first()
            if row is None:
                continue
            size = row["size"]
        except Exception as e:
            print(f"!!! Błąd przy pobieraniu rozmiaru wektora {vc}: {e}")
            continue

        for i in range(size):
            new_col_name = f"{vc}_{i}"
            df = df.withColumn(new_col_name, col(arr_col)[i])


    final_cols = []
    for field in df.schema.fields:
        dtype = field.dataType.simpleString()
        name = field.name

        if "string" in dtype or "array" in dtype or "vector" in dtype:
            continue

        final_cols.append(name)

    print(f">>> Wybrane kolumny numeryczne do Gold: {final_cols}")
    df_final = df.select(final_cols)

    print(">>> Próbka danych Gold:")
    df_final.show(5)

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