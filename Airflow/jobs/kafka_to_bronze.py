from pyspark.sql import SparkSession
import sys

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC_NAME = "pg.public.kaggle_final_depression_dataset_1"

BRONZE_PATH = "/opt/airflow/data/datalake/bronze/mental_health"


def run_job():
    print(f">>> [Start] Kafka -> Bronze | Topic: {TOPIC_NAME}")

    spark = SparkSession.builder \
        .appName("KafkaToBronze") \
        .master("local[*]") \
        .config("spark.driver.host", "localhost") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

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

    df_parsed = df_raw.selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS kafka_value",
        "topic",
        "partition",
        "offset",
        "timestamp"
    )

    count = df_parsed.count()
    print(f">>> Pobrano {count} rekordów.")

    if count > 0:
        df_parsed.show(5, truncate=True)

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
