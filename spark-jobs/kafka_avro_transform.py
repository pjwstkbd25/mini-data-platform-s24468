from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
import urllib.request, json
from pyspark.sql.functions import col
spark = SparkSession.builder \
    .appName("DebeziumAvroStreaming") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0") \
    .getOrCreate()

# 1. Czytamy **wszystkie** trzy topici:
raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers","kafka:9092")
       .option("subscribe",
               "data_source.public.educational_data,"
               "data_source.public.addiction_data,"
               "data_source.public.investment_data")
       .option("startingOffsets","earliest")
       .load()
       .selectExpr("topic","substring(value,6) as avro_payload"))

# 2. Pobieramy schemę za każdym razem (lub cachujemy w dict, jeśli chcesz):
schemas = {}
for table in ["educational_data","addiction_data","investment_data"]:
    subj = f"data_source.public.{table}-value"
    url = f"http://schema-registry:8081/subjects/{subj}/versions/latest"
    schemas[table] = json.loads(urllib.request.urlopen(url).read())["schema"]

# 3. Funkcja, która dostaje cały mikro-batch i rozbija go na trzech:
def route_and_write(batch_df, epoch_id):
    for table, schema_str in schemas.items():
        df_table = (batch_df
                    .filter(col("topic") == f"data_source.public.{table}")
                    .select(from_avro("avro_payload", schema_str, {"mode":"FAILFAST"}).alias("evt"))
                    .select("evt.after.*"))
        if df_table.rdd.isEmpty():
            continue
        df_table.write \
            .format("delta") \
            .mode("append") \
            .option("checkpointLocation", f"s3a://spark-output/_checkpoints/{table}") \
            .save(f"s3a://spark-output/{table}")

# 4. Uruchomienie:
query = raw.writeStream \
    .foreachBatch(route_and_write) \
    .start()

query.awaitTermination()