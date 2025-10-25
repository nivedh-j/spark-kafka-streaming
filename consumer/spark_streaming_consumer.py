from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, DoubleType, IntegerType

# Create Spark session with Kafka package
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# Define schema for incoming JSON
schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("amount", DoubleType())

# Read stream from Kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stream-topic") \
    .load()

# Extract JSON data from Kafka value
json_df = df.selectExpr("CAST(value AS STRING) as json") \
             .select(from_json(col("json"), schema).alias("data")) \
             .select("data.*")

# Example processing: group by user_id and sum their amount
agg_df = json_df.groupBy("user_id").sum("amount")

# Output to console
query = agg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
