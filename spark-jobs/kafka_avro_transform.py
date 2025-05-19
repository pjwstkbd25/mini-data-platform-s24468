
from pyspark.sql.functions import expr

from pyspark.sql import SparkSession
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

# query = events.writeStream.format("console").option("truncate", False).start()
#
# query.awaitTermination()

# 4. Konfiguracja Spark do pisania do MinIO
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", "minioadmin")
hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# 5. Zapis do Delta Lake na MinIO
query = events.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://spark-output/_checkpoints/educational-data") \
    .start("s3a://spark-output/educational-data")

query.awaitTermination()
