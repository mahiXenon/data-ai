from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import countDistinct,avg,hour,col,when,broadcast,cast

spark = SparkSession.builder \
    .appName("Analysis of data") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.0") \
    .getOrCreate()

path_to_data = "/home/xs532-mahjat/Downloads/data_ai/delta/tables/wind_data"
data = spark.read.format("delta").load(path_to_data)

distinct_signal_ts = data.groupBy("signal_date")\
                    .agg(countDistinct("signal_ts")\
                    .alias("distinct_signal_count"))
# print(f"Distinct signal_ts count: {distinct_signal_ts}")
print(f"^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^'Number of distinct signal_ts'^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
distinct_signal_ts.show(truncate=False)

avg_signal_value = data.groupBy("signal_date",hour("signal_ts").alias("hour"))\
                       .agg(avg(col("signals").getItem("ActivePower_kW").cast("float")).alias("avg_ActivePower_kW"),
                            avg(col("signals").getItem("WindSpeed_m_s").cast("float")).alias("avg_WindSpeed_m_s"),
                            avg(col("signals").getItem("PowerCurve_kWh").cast("float")).alias("avg_PowerCurve_kWh"),
                            avg(col("signals").getItem("WindDirection_deg").cast("float")).alias("avg_WindDirection_deg")
                        )
avg_signal_value.show(truncate=False)
print(f"^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^'AVG value of signal_ts'^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")


generation_indicator = avg_signal_value.withColumn("generation_indicator",
    when((col("avg_ActivePower_kW") < 200), "Low")
    .when((col("avg_ActivePower_kW") >= 200) & (col("avg_ActivePower_kW") < 600), "Medium")
    .when((col("avg_ActivePower_kW") >= 600) & (col("avg_ActivePower_kW") < 1000), "High")
    .otherwise("Exceptional")
)
generation_indicator.show(truncate=False)
print(f"^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^'Generation indicator'^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW mapping_table AS
    SELECT 'avg_ActivePower_kW' as sig_name, 'active_power_average' as sig_mapping_name
    UNION ALL
    SELECT 'avg_WindSpeed_m_s', 'wind_speed_average'
    UNION ALL   
    SELECT 'avg_PowerCurve_kWh', 'theo_power_curve_average'
    UNION ALL
    SELECT 'avg_WindDirection_deg', 'wind_direction_average'
    UNION ALL
    SELECT 'generation_indicator', 'generation_mapping'
""")

mapped_df = spark.table("mapping_table")
mapped_df.show(truncate=False)
print(f"^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^'Mapped signal name created using spark sql'^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")

renamed_mapped_df = generation_indicator

for row in broadcast(mapped_df).collect():
    sig_name = row["sig_name"]
    sig_mapping_name = row["sig_mapping_name"]
    if sig_name in renamed_mapped_df.columns:
        renamed_mapped_df = renamed_mapped_df.withColumnRenamed(sig_name, sig_mapping_name)

renamed_mapped_df.show(truncate=False)
print(f"^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^'Renamed signal name using broadcast join'^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")

