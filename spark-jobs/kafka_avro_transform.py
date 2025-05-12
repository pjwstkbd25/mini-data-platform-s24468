# from pyspark.sql import SparkSession, functions as F
# from pyspark.sql.avro.functions import from_avro
#
# BOOTSTRAP = "kafka:9092"
# TOPIC = "data_source.public.educational_data"
#
# AVRO_READER_SCHEMA = r"""
# {
#   "type": "record",
#   "name": "Envelope",
#   "namespace": "data_source.public.educational_data",
#   "fields": [
#     { "name": "after",
#       "type": [ "null",
#         { "type": "record",
#           "name": "Value",
#           "fields": [
#             {"name":"Full_Name",                     "type":["null","string"], "default":null},
#             {"name":"Age",                           "type":["null","int"],    "default":null},
#             {"name":"Education_Level",               "type":["null","string"], "default":null},
#             {"name":"Major",                         "type":["null","string"], "default":null},
#             {"name":"Year_Started_Education",        "type":["null","int"],    "default":null},
#             {"name":"Year_Completed_Education",      "type":["null","int"],    "default":null},
#             {"name":"Type_of_Educational_Institution","type":["null","string"],"default":null},
#             {"name":"Average_Grade",                 "type":["null","double"], "default":null}
#           ]
#         }
#       ],
#       "default": null
#     }
#   ]
# }
# """
#
# # ——— 1. Spark session ———
# spark = (
#     SparkSession.builder
#     .appName("Kafka-Avro-Educational-Stream")
#     .getOrCreate()
# )
#
# # ——— 2. Źródło: Kafka ———
# raw = (
#     spark.readStream
#     .format("kafka")
#     .option("kafka.bootstrap.servers", BOOTSTRAP)
#     .option("subscribe", TOPIC)
#     # zmień na "earliest", jeśli chcesz odczytać całą historię
#     .option("startingOffsets", "latest")
#     .load()
# )
#
# # ——— 3. Deserializacja Avro ———
# decoded = (
#     raw.select(
#         from_avro(F.col("value"), AVRO_READER_SCHEMA).alias("evt")
#     )
# )
#
# # ——— 4. Wyciągamy kolumny tabeli (sekcję `after`) ———
# records = decoded.select("evt.after.*")
#
# # ——— 5. Logujemy jako JSON na stdout (lub zamień na sink Parquet / Delta) ———
# query = (
#     records.select(F.to_json(F.struct("*")).alias("value"))
#     .writeStream
#     .format("console")  # <— na produkcji zwykle parquet / delta / sink kafka
#     .option("truncate", "false")
#     .start()
# )
#
# query.awaitTermination()


#
#
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, expr
# from pyspark.sql.avro.functions import from_avro
# from confluent_kafka.schema_registry import SchemaRegistryClient
#
# spark = SparkSession.builder \
#     .appName("DebeziumAvroStreaming") \
#     .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.0") \
#     .getOrCreate()
#
# # 1. Strumieniowe czytanie z Kafka
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:9092") \
#     .option("subscribe", "data_source.public.educational_data") \
#     .option("startingOffsets", "earliest") \
#     .load()
#
# # 2. Usunięcie 5-bajtowego nagłówka Confluent Avro (magic byte + schema ID)
# df = df.withColumn("avro_payload", expr("substring(value, 6, length(value) - 5)"))
#
# # 3. Pobranie schematu Avro z Confluent Schema Registry
# schema_registry_conf = {"url": "http://schema-registry:8081"}
# sr_client = SchemaRegistryClient(schema_registry_conf)
# schema_info = sr_client.get_latest_version("data_source.public.educational_data-value")
# avro_schema_str = schema_info.schema.schema_str
#
# # 4. Deserializacja Avro -> kolumna strukturalna 'evt'
# decoded = df.select(from_avro(col("avro_payload"), avro_schema_str, {"mode": "FAILFAST"}).alias("evt"))
#
# decoded.printSchema()  # opcjonalnie wypisanie schematu dla sprawdzenia
#
# # 5. Dostęp do zagnieżdżonych pól (przykład wyciągnięcia wybranych kolumn)
# events = decoded.select(
#     col("evt.op").alias("operation"),
#     col("evt.after.Full_Name").alias("full_name"),
#     col("evt.after.Age").alias("age"),
#     col("evt.source.db").alias("source_db")
# )


from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.avro.functions import from_avro
import urllib.request, json

spark = SparkSession.builder \
    .appName("DebeziumAvroStreaming") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0") \
    .getOrCreate()

# 1. Czytanie z Kafka i usunięcie 5-bajtowego nagłówka
df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "data_source.public.educational_data")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("substring(value, 6) as avro_payload"))

# 2. Pobranie schemy z Registry (HTTP + stdlib)
url = "http://schema-registry:8081/subjects/data_source.public.educational_data-value/versions/latest"
schema_str = json.loads(urllib.request.urlopen(url).read())["schema"]

# 3. Deserializacja Avro + wybór pól
events = (df
.select(from_avro("avro_payload", schema_str, {"mode": "FAILFAST"}).alias("evt"))
.selectExpr(
    "evt.op          as operation",
    "evt.after.Full_Name as full_name",
    "evt.after.Age  as age",
    "evt.source.db  as source_db"
)
)

query = events.writeStream.format("console").option("truncate", False).start()

query.awaitTermination()
