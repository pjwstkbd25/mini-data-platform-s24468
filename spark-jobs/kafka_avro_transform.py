from pyspark.sql import SparkSession, functions as F
from pyspark.sql.avro.functions import from_avro

BOOTSTRAP = "kafka:9092"
TOPIC = "data_source.public.educational_data"

AVRO_READER_SCHEMA = r"""
{
  "type": "record",
  "name": "Envelope",
  "namespace": "data_source.public.educational_data",
  "fields": [
    { "name": "after",
      "type": [ "null",
        { "type": "record",
          "name": "Value",
          "fields": [
            {"name":"Full_Name",                     "type":["null","string"], "default":null},
            {"name":"Age",                           "type":["null","int"],    "default":null},
            {"name":"Education_Level",               "type":["null","string"], "default":null},
            {"name":"Major",                         "type":["null","string"], "default":null},
            {"name":"Year_Started_Education",        "type":["null","int"],    "default":null},
            {"name":"Year_Completed_Education",      "type":["null","int"],    "default":null},
            {"name":"Type_of_Educational_Institution","type":["null","string"],"default":null},
            {"name":"Average_Grade",                 "type":["null","double"], "default":null}
          ]
        }
      ],
      "default": null
    }
  ]
}
"""

# ——— 1. Spark session ———
spark = (
    SparkSession.builder
    .appName("Kafka-Avro-Educational-Stream")
    .getOrCreate()
)

# ——— 2. Źródło: Kafka ———
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    # zmień na "earliest", jeśli chcesz odczytać całą historię
    .option("startingOffsets", "latest")
    .load()
)

# ——— 3. Deserializacja Avro ———
decoded = (
    raw.select(
        from_avro(F.col("value"), AVRO_READER_SCHEMA).alias("evt")
    )
)

# ——— 4. Wyciągamy kolumny tabeli (sekcję `after`) ———
records = decoded.select("evt.after.*")

# ——— 5. Logujemy jako JSON na stdout (lub zamień na sink Parquet / Delta) ———
query = (
    records.select(F.to_json(F.struct("*")).alias("value"))
    .writeStream
    .format("console")  # <— na produkcji zwykle parquet / delta / sink kafka
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
