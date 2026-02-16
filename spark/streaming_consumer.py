from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import os

aws_key = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret = os.environ.get('AWS_SECRET_ACCESS_KEY')

spark = SparkSession.builder \
    .appName("CryptoStreamingConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.access.key", aws_key) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-west-1.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
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
    .option("startingOffsets", "earliest") \
    .load()

trades = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*") \
 .withColumn("processing_time", current_timestamp()) \
 .withColumn("date", date_format(col("processing_time"), "yyyy-MM-dd"))

query = trades.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3a://data-pipeline-bronze-e601c2c3/spark-streaming/") \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .partitionBy("date", "symbol") \
    .trigger(processingTime='30 seconds') \
    .start()

print("✅ Spark Streaming started!")
print(f"📊 AWS Key: {aws_key[:10]}...")
print("💾 Writing to S3 Bronze")

query.awaitTermination()
