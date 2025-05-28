import os
import sys

print("HADOOP_HOME:", os.environ.get("HADOOP_HOME"))

os.environ["PYSPARK_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Define the schema for the incoming JSON data
schema = StructType([
    StructField("unique_key", StringType(), True),
    StructField("created_date", StringType(), True),  # Parse as string, convert to timestamp later
    StructField("complaint_type", StringType(), True),
    StructField("incident_zip", StringType(), True),
    StructField("incident_address", StringType(), True),
    StructField("status", StringType(), True),
    StructField("agency", StringType(), True),
    StructField("borough", StringType(), True),
    StructField("processing_timestamp", StringType(), True),
    StructField("data_source", StringType(), True),
    StructField("pipeline_version", StringType(), True)
])

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("NYC311")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.hadoop.hadoop.native.lib", "false")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint")
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
        .getOrCreate()
    )

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "nyc311-service-requests") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse the JSON messages
    json_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Convert created_date to timestamp
    json_df = json_df.withColumn("created_date_ts", col("created_date").cast("timestamp"))

    # Example 1: Print a running count of complaints by type in 1-hour windows
    complaint_counts = json_df \
        .withWatermark("created_date_ts", "1 hour") \
        .groupBy(
            window(col("created_date_ts"), "1 hour"),
            col("complaint_type")
        ).count()

    # Output to console
    query = complaint_counts.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Example 2: Print a sample of the raw data
    sample_query = json_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 5) \
        .start()

    query.awaitTermination()
    sample_query.awaitTermination() 