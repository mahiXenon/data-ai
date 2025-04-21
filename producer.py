from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import to_json, struct, col
import time
 
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaStreamingProducer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
    .getOrCreate()
 
# Define the schema for the CSV file
schema = StructType([
    StructField("Date/Time", StringType(), True),
    StructField("LV ActivePower (kW)", DoubleType(), True),
    StructField("Wind Speed (m/s)", DoubleType(), True),
    StructField("Theoretical_Power_Curve (KWh)", DoubleType(), True),
    StructField("Wind Direction (°)", DoubleType(), True)
])
 
# Read the CSV file as a streaming source
df = spark.readStream.option("header", "true") \
    .schema(schema) \
    .csv("/home/xs532-mahjat/Downloads/data_ai")  # Input directory for new CSV files
 
# Convert DataFrame to JSON format
json_df = df.select(to_json(struct(*[col(column) for column in df.columns])).alias("value"))
 
# Kafka Configurations
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "dataset"
 
# Write Data to Kafka Topic in streaming mode
try:
    query = json_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", KAFKA_TOPIC) \
        .outputMode("append") \
        .option("checkpointLocation", "./checkpoint") \
        .start()
    
    print(f"================================Streaming data to Kafka topic '{KAFKA_TOPIC}'===============================================")
    time_limit_seconds = 4  # Set the desired time limit
    start_time = time.time()
    while query.isActive:
        if time.time() - start_time > time_limit_seconds:
            query.stop()
            print(f"================================Successfully pubished data to kafka topic '{KAFKA_TOPIC}'============================================")
            break
 
except Exception as e:
    print(f"Error streaming data to Kafka: {e}")
 
# Stop Spark Session (This will execute when the streaming stops)
finally:
    spark.stop()
 