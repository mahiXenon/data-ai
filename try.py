from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Initialize Spark session with Delta Lake support
# producer command- spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 /home/xs532-mahjat/Downloads/data_ai/producer.py
# consumer command- spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,io.delta:delta-spark_2.12:3.3.0 /home/xs532-mahjat/Downloads/data_ai/consumer.py
#
# docker exec -it <container_id> /bin/bash
# cd opt/kafka 
# kafka cunsumer : bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic dataset --from-beginning

#




spark = SparkSession.builder \
    .appName("KafkaToDeltaStreaming") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.extraClassPath", "/opt/spark-3.5.0/jars/*") \
    .config("spark.executor.extraClassPath", "/opt/spark-3.5.0/jars/*") \
    .getOrCreate()

# Define Kafka configuration  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,io.delta:delta-spark_2.12:3.3.0 /home/xs533-jatket/DATA-DEMO/Data-AI/kafka/consumer.py
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "dataset") \
    .option("startingOffsets", "earliest") \
    .load()

# Define schema for the Kafka data
schema = StructType([
    StructField("id", StringType()),
    StructField("value", DoubleType())
])

# Parse JSON data from Kafka
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Debugging: Print schema and sample data to console
console_query = parsed_df \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

# Write to Delta table
delta_query = parsed_df \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/home/xs532-mahjat/Downloads/data_ai/checkpoint") \
    .start("/home/xs532-mahjat/Downloads/data_ai/delta/wind_turbine")

# Wait for the streaming query to terminate
delta_query.awaitTermination()
