from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

spark = SparkSession.builder \
    .appName("CryptoStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", DoubleType()),
    StructField("timestamp", StringType()),
    StructField("trade_time", LongType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto-trades") \
    .option("startingOffsets", "latest") \
    .load()

trades = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*") \
 .withColumn("processing_time", current_timestamp())

query = trades.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='5 seconds') \
    .start()

print("✅ Spark Streaming Console Mode")
print("📊 Reading from Kafka every 5 seconds")
print("Press Ctrl+C to stop")

query.awaitTermination()
