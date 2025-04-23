from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, from_json, current_date, current_timestamp, to_timestamp, create_map, lit
from pyspark.sql.types import StructType, StructField, StringType
import logging
 
# Initialize Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaConsumer")
 
try:
    logger.info("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Initializing Spark session ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
 
    # Initializing Spark Session
    spark = SparkSession.builder \
        .appName("KafkaConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,io.delta:delta-spark_2.12:3.3.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
 
    logger.info("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Spark session successfully initialized ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
 
    # Defining the schema for Kafka messages
    schema = StructType([
        StructField("Date/Time", StringType()),
        StructField("LV ActivePower (kW)", StringType()),
        StructField("Wind Speed (m/s)", StringType()),
        StructField("Theoretical_Power_Curve (KWh)", StringType()),
        StructField("Wind Direction (°)", StringType())
    ])
 
    # Reading data from Kafka in streaming fashion
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "dataset") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("kafka.request.timeout.ms", "120000") \
        .option("kafka.session.timeout.ms", "120000") \
        .load()
 
    logger.info("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Successfully connected to Kafka topic ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
 
    # Printing raw Kafka data to the console
    raw_query = kafka_df.select(col("value").cast("string").alias("raw_data"))
    raw_query.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()
 
    logger.info("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Raw Kafka data is being printed to the console ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
 
    # Deserializing Kafka data and applying schema
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).filter(col("data").isNotNull())
 
    logger.info("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Kafka data successfully parsed and validated ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
 
    # Transforming the filtered data
    transformed_df = parsed_df.select(
        to_date(col("data.`Date/Time`"), "dd MM yyyy").alias("signal_date"),
        to_timestamp(col("data.`Date/Time`"), "dd MM yyyy HH:mm").alias("signal_ts"),
        current_date().alias("create_date"),
        current_timestamp().alias("create_ts"),
        create_map(
            lit("ActivePower_kW"), col("data.`LV ActivePower (kW)`"),
            lit("WindSpeed_m_s"), col("data.`Wind Speed (m/s)`"),
            lit("PowerCurve_kWh"), col("data.`Theoretical_Power_Curve (KWh)`"),
            lit("WindDirection_deg"), col("data.`Wind Direction (°)`")
        ).alias("signals")
    )
 
    # Printing transformed data to the console
    transformed_query = transformed_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()
 
    logger.info("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Transformed data is being printed to the console ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
 
    # Writing the transformed data to Delta table
    delta_stream = transformed_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/home/xs532-mahjat/Downloads/data_ai/checkpoints/delta_kafka_consumer") \
        .start("/home/xs532-mahjat/Downloads/data_ai/delta/tables/wind_data")
 
 
    logger.info("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Delta table write initialized successfully ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
 
    # Waiting for the stream to finish
    delta_stream.awaitTermination()
 
except Exception as e:
    logger.critical(f"Critical failure: {e}")
    spark.stop()
    exit()