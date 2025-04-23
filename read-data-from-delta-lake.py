from pyspark.sql import SparkSession
 
 
# Initialize Spark Session with Delta Lake configurations
spark = SparkSession.builder \
    .appName("Reading data from delta lake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.0") \
    .getOrCreate()
 
# Path to the Delta table
delta_table = "/home/xs532-mahjat/Downloads/data_ai/delta/tables/wind_data"
 
# Loading the data from Delta table
delta_df = spark.read.format("delta").load(delta_table)
 
# showing the Delta table
delta_df.show(truncate=False)