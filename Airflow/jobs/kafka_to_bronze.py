from pyspark.sql import SparkSession
import sys

# --- KONFIGURACJA ---
# Adres brokera wewnątrz sieci Dockera
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"

# Nazwa tematu.
# Skoro Twój skrypt transform_to_postgresql.py tworzy tabele z prefiksem 'kaggle_',
# a schema to 'public', to Debezium utworzy temat wg wzorca: server.schema.table
# PRZYKŁAD: Jeśli plik CSV nazywał się "Mental Health.csv", tabela to "kaggle_mental_health".
# Wpisz tutaj dokładną nazwę tematu, którą widzisz w Kafka UI (localhost:8090).
TOPIC_NAME = "pg.public.kaggle_final_depression_dataset_1"

# Ścieżka wyjściowa (Bronze)
BRONZE_PATH = "/opt/airflow/data/datalake/bronze/mental_health"


def run_job():
    print(f">>> [Start] Kafka -> Bronze | Topic: {TOPIC_NAME}")

    # Tworzenie sesji Spark z paczką do Kafki
    spark = SparkSession.builder \
        .appName("KafkaToBronze") \
        .master("local[*]") \
        .config("spark.driver.host", "localhost") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

    # Mniej logów
    spark.sparkContext.setLogLevel("WARN")

    # 1. Odczyt z Kafki (Batch - czytamy stan obecny od początku do końca)
    print(">>> Czytanie danych z Kafki...")
    df_raw = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    # 2. Rzutowanie na String (Bronze przechowuje surowy JSON, nie parsujemy go jeszcze)
    # Zapisujemy key, value, topic, partition, offset, timestamp
    df_parsed = df_raw.selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS kafka_value",
        "topic",
        "partition",
        "offset",
        "timestamp"
    )

    # Walidacja - czy coś przyszło?
    count = df_parsed.count()
    print(f">>> Pobrano {count} rekordów.")

    if count > 0:
        df_parsed.show(5, truncate=True)

    # 3. Zapis do Bronze (Parquet)
    print(f">>> Zapisywanie do: {BRONZE_PATH}")
    (
        df_parsed.write
        .mode("overwrite")
        .parquet(BRONZE_PATH)
    )

    print(">>> [Sukces] Zakończono job Kafka -> Bronze.")
    spark.stop()


if __name__ == "__main__":
    run_job()

# # # jobs/kafka_to_bronze.py
# # from pyspark.sql import SparkSession
# # import os
# # from pathlib import Path
# # from dotenv import load_dotenv
# # # Kafka config
# #
# #
# # KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
# # DEBEZIUM_TOPIC = "pg-source.public.kaggle_final_depression_dataset_1"
# #
# # # Bronze output
# # OUTPUT_PATH = "/data/datalake/bronze/debezium_raw"
# #
# # def main():
# #     spark = (
# #         SparkSession.builder
# #         .appName("KafkaToBronze")
# #         .getOrCreate()
# #     )
# #
# #     # 1. Read from Kafka
# #     df_raw = (
# #         spark.read
# #         .format("kafka")
# #         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
# #         .option("subscribe", DEBEZIUM_TOPIC)
# #         .option("startingOffsets", "earliest")
# #         .option("endingOffsets", "latest")
# #         .load()
# #     )
# #
# #     # 2. Convert Kafka bytes → string JSON
# #     df_parsed = df_raw.selectExpr(
# #         "CAST(key AS STRING) AS key_str",
# #         "CAST(value AS STRING) AS value_str",
# #         "topic",
# #         "partition",
# #         "offset",
# #         "timestamp"
# #     )
# #
# #     # 3. Save as Parquet
# #     (
# #         df_parsed.write
# #         .mode("overwrite")
# #         .parquet(OUTPUT_PATH)
# #     )
# #
# #     spark.stop()
# #
# # if __name__ == "__main__":
# #     main()
# # jobs/kafka_to_bronze.py
#
# #
# # # -------------------- Env / Paths --------------------
# # # BASE_DIR → folder "Airflow"
# # BASE_DIR = Path(__file__).resolve().parents[1]
# # ENV_PATH = BASE_DIR / ".env"
# # if ENV_PATH.exists():
# #     load_dotenv(dotenv_path=ENV_PATH)
# #
# # # W kontenerze Spark: /opt/airflow/data → /data
# # DATA_DIR = os.getenv("DATA_DIR", "/data")
# # DATALAKE_DIR = os.path.join(DATA_DIR, "datalake")
# # BRONZE_PATH = os.path.join(DATALAKE_DIR, "bronze", "debezium_raw")
# #
# # # -------------------- Kafka config --------------------
# # KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
# # DEBEZIUM_TOPIC = "pg-source.public.kaggle_final_depression_dataset_1"
# # jobs/kafka_to_bronze.py
#
# from pyspark.sql import SparkSession
# import os
# # # jobs/kafka_to_bronze.py
#
# import os
# # ...
# #
# # # -------------------- Env / Paths --------------------
# # DATA_DIR = os.getenv("DATA_DIR", "/opt/spark/data") # <-- Zmieniamy na ścieżkę Sparka!
# # DATALAKE_DIR = os.path.join(DATA_DIR, "datalake")
# # BRONZE_PATH = os.path.join(DATALAKE_DIR, "bronze", "debezium_raw")
# # # -------------------- Kafka config --------------------
# # KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
# # DEBEZIUM_TOPIC = "pg-source.public.kaggle_final_depression_dataset_1"
#
#
# def main():
#     spark = (
#         SparkSession.builder
#         .appName("KafkaToBronze")
#         .getOrCreate()
#     )
#
#     # 1. Read from Kafka
#     df_raw = (
#         spark.read
#         .format("kafka")
#         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
#         .option("subscribe", DEBEZIUM_TOPIC)
#         .option("startingOffsets", "earliest")
#         .option("endingOffsets", "latest")
#         .load()
#     )
#
#     # 2. Convert Kafka bytes → string JSON
#     df_parsed = df_raw.selectExpr(
#         "CAST(key AS STRING) AS key_str",
#         "CAST(value AS STRING) AS value_str",
#         "topic",
#         "partition",
#         "offset",
#         "timestamp"
#     )
#
#     # 3. Save Bronze Parquet
#     (
#         df_parsed.write
#         .mode("overwrite")
#         .parquet(BRONZE_PATH)
#     )
#
#     spark.stop()
#
#
# if __name__ == "__main__":
#     main()
