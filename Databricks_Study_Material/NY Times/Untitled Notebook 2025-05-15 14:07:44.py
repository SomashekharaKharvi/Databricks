# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when
from pyspark.sql.types import IntegerType, DoubleType, LongType, StringType, TimestampType, StructType, StructField

# COMMAND ----------

tripdata_06_df= spark.read.format('parquet').load('/FileStore/tables/yellow_tripdata_2024_06.parquet')
tripdata_07_df= spark.read.format('parquet').load('/FileStore/tables/yellow_tripdata_2024_07.parquet')
tripdata_08_df= spark.read.format('parquet').load('/FileStore/tables/yellow_tripdata_2024_08.parquet')
lookup_df=spark.read.format('csv').option('header', True).load('/FileStore/tables/taxi_zone_lookup.csv')

# COMMAND ----------

def trip_validation(df):
  df = tripdata_06_df.withColumn("is_valid", when(
    (col("tpep_pickup_datetime") < col("tpep_dropoff_datetime")) &
    (((col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")) / 60).between(1, 180)) &
    (col("passenger_count").between(1, 6)) &
    (col("trip_distance") > 0) & (col("trip_distance") <= 100) &
    (col("PULocationID").between(1, 263)) & (col("DOLocationID").between(1, 263)) &
    (col("fare_amount") >= 0) & (col("total_amount") >= 0) &
    (col("tip_amount") >= 0) & (col("tip_amount") <= col("total_amount")) &
    (col("payment_type").isin([1, 2, 3, 4, 5, 6])) &
    (col("VendorID").isin([1, 2])) &
    (col("RatecodeID").between(1, 6)) &
    (col("store_and_fwd_flag").isin(["Y", "N"]))
    , True).otherwise(False))
  
  return df

# COMMAND ----------

trip_df= tripdata_06_df.union(tripdata_07_df).union(tripdata_08_df)
trip_df = trip_validation(trip_df)

# COMMAND ----------

# JDBC connection properties
jdbc_url = "jdbc:postgresql://<host>:<port>/<database>"
connection_properties = {
    "user": "<your_username>",
    "password": "<your_password>",
    "driver": "org.postgresql.Driver"
}

# Convert 'is_valid' boolean column to string if needed
df_to_write = df.withColumn("valid", col("is_valid").cast("string"))

# Write to PostgreSQL table
df_to_write.write \
    .jdbc(
        url=jdbc_url,
        table="yellow_taxi_trips",
        mode="append",   # or "overwrite" if recreating table
        properties=connection_properties
    )
