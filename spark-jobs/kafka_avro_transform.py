# """
# Spark Structured Streaming → Kafka (+ Avro decode)
# -------------------------------------------------
#  • Czyta topic:  data_source.public.educational_data
#  • Dekoduje Avro przy pomocy Confluent Schema Registry
#  • Prosta transformacja: wybiera kolumny + wypisuje na konsolę
#  • Batch trigger = co 10 s (łatwo zobaczyć w logach)
# """
#
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_avro, to_json, struct
#
# # ──────────────────────────────────────────────────────────────────────────────
# #  Parametry – zmieniaj tylko tu:
# TOPIC              = "data_source.public.educational_data"
# BOOTSTRAP_SERVERS   = "kafka:9092"               # w sieci docker-compose
# SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
# TRIGGER_SECONDS     = 10                         # jak często batch
# # ──────────────────────────────────────────────────────────────────────────────
#
# # „Stub” – musi się sparsować, ale faktyczna treść i tak zostanie
# # nadpisana prawdziwym schematem pobranym po ID z nagłówka confluent.
# DUMMY_AVRO_SCHEMA = """
# {
#   "type": "record",
#   "name": "Dummy",
#   "fields": []
# }
# """
#
# spark = (
#     SparkSession.builder
#     .appName("KafkaAvroTransform")
#     .getOrCreate()
# )
#
# # → DataFrame w surowym formacie (klucz/wartość = bytes)
# df_raw = (
#     spark.readStream
#          .format("kafka")
#          .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
#          .option("subscribe", TOPIC)
#          .option("startingOffsets", "earliest")
#          .load()
# )
#
# # Dekodowanie Avro
# avro_opts = {
#     "mode": "PERMISSIVE",
#     "schemaRegistryAddress": SCHEMA_REGISTRY_URL
# }
#
# df_decoded = (
#     df_raw
#       .select(from_avro(col("value"), DUMMY_AVRO_SCHEMA, avro_opts).alias("evt"))
#       .select("evt.*")              # rozpakowujemy pola rekordu
# )
#
# # 👉 prosta transformacja: policz ile insertów w batchu
# df_enriched = (
#     df_decoded
#       .withColumn("batch_tag", col("Full_Name"))   # przyklad: cokolwiek
# )
#
# query = (
#     df_enriched
#       .writeStream
#       .format("console")
#       .option("truncate", False)
#       .outputMode("append")
#       .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
#       .start()
# )
#
# query.awaitTermination()
# ./spark-jobs/kafka_avro_transform.py
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.avro.functions import from_avro        # ← tu jest from_avro
from pyspark.sql.functions import col, to_json, struct   # reszta funkcji

spark = (SparkSession.builder
         .appName("KafkaAvroTransform")
         .getOrCreate())

BOOTSTRAP = "kafka:9092"
TOPIC     = "data_source.public.educational_data"

# --- Avro schema (wzięte z Schema Registry → latestVersion) ---
avro_schema = """
{
 "type":"record",
 "name":"wrapper",
 "fields":[
   {"name":"before","type":["null","string"],"default":null},
   {"name":"after","type":["null",{
      "type":"record","name":"after_rec",
      "fields":[
        {"name":"Full_Name","type":["null","string"],"default":null},
        {"name":"Age","type":["null","int"],"default":null},
        {"name":"Education_Level","type":["null","string"],"default":null},
        {"name":"Major","type":["null","string"],"default":null},
        {"name":"Year_Started_Education","type":["null","int"],"default":null},
        {"name":"Year_Completed_Education","type":["null","int"],"default":null},
        {"name":"Type_of_Educational_Institution","type":["null","string"],"default":null},
        {"name":"Average_Grade","type":["null","double"],"default":null}
      ]
   }],"default":null}
 ]
}
"""

raw = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load())

decoded = (raw
           .select(from_avro(col("value"), avro_schema).alias("evt"))
           .select("evt.after.*")                 # tylko część „after”
           .filter(col("Average_Grade") >= 3.0))  # ← mała transformacja

query = (decoded
         .select(to_json(struct("*")).alias("value"))
         .writeStream
         .format("console")          # wystarczy do demonstracji
         .option("truncate", "false")
         .start())

query.awaitTermination()
