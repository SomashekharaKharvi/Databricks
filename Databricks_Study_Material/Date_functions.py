# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# Sample data
data = [
    ("2023-01-15",),
    ("2023-02-20",),
    ("2023-03-25",),
    ("2023-04-30",),
    ("2023-05-05",)
]

# Create DataFrame with a date column
df = spark.createDataFrame(data, ["date_string"])

# Convert the string column to a date type
# df = df.withColumn("date", to_date("date_string", "yyyy-MM-dd"))

# Show the original DataFrame
print("Original DataFrame:")
df.display()

# COMMAND ----------

df1=df.withColumn("date", to_date(col("date_string"), "yyyy-MM/dd"))
df1.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select cast(date_string as date) from df

# COMMAND ----------

df.withColumn("date", current_date()).display()

# COMMAND ----------

df.withColumn("date", date_format(to_date(col("date_string"), "yyyy-MM-dd"), "yyyy/MM/dd")).display()

# COMMAND ----------

df.withColumn("date", add_months(to_date(col("date_string"), "yyyy-MM-dd"), 2)).display()
df.withColumn("date", date_add(to_date(col("date_string"), "yyyy-MM-dd"), 10)).display()
df.withColumn("date", date_sub(to_date(col("date_string"), "yyyy-MM-dd"), 2)).display()
df.withColumn("date", datediff(current_date(), to_date(col("date_string"), "yyyy-MM-dd"))).display()

# COMMAND ----------

df.withColumn("date", months_between(current_date(), to_date(col("date_string"), "yyyy-MM-dd"))).display()

# COMMAND ----------

df.withColumn("date", next_day(to_date(col("date_string")), "Monday")).display()

# COMMAND ----------

df_trunc = df.withColumn("truncated_to_month", trunc(to_date(col("date_string")), "MM"))
df_trunc.show(truncate=False)

# COMMAND ----------

df.withColumn("truncated_to_month", month(to_date(col("date_string")))).display()
df.withColumn("truncated_to_month", year(to_date(col("date_string")))).display()
df.withColumn("dayofmonth", dayofmonth(to_date(col("date_string")))).display()

# COMMAND ----------

df.withColumn("truncated_to_week", dayofweek(to_date(col("date_string")))).display()
df.withColumn("truncated_to_month", dayofmonth(to_date(col("date_string")))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC unix_timestamp

# COMMAND ----------

data = [
    ("2023-01-01 00:59:59",),
    ("2023-01-02 00:00:00",),
    ("2023-01-03 00:00:00",),
]

# Create DataFrame with date string column
df = spark.createDataFrame(data, ["date_string"])
df.display()
df1=df.withColumn("new", unix_timestamp(col("date_string"), "yyyy-MM-dd HH:mm:ss"))
df1.display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    hour,
    minute,
    second,
    to_timestamp
)

# Create a Spark session
# spark = SparkSession.builder.appName("TimestampFunctionsExample").getOrCreate()

# Sample data with timestamp strings
data = [
    ("2023-01-01 12:30:45",),
    ("2023-01-02 14:45:30",),
    ("2023-01-03 16:15:20",),
]

# Create DataFrame with timestamp string column
df = spark.createDataFrame(data, ["timestamp_string"])

#.display the original DataFrame
print("Original DataFrame:")
df.display(truncate=False)

# 1. Retrieve the current timestamp
df_current_timestamp = df.withColumn("current_timestamp", current_timestamp())
print("DataFrame with Current Timestamp:")
df_current_timestamp.display(truncate=False)

# 2. Extract hour from the timestamp
df_hour = df.withColumn("hour", hour(to_timestamp("timestamp_string", "yyyy-MM-dd HH:mm:ss")))
print("DataFrame with Extracted Hour:")
df_hour.display(truncate=False)

# 3. Extract minute from the timestamp
df_minute = df.withColumn("minute", minute(to_timestamp("timestamp_string", "yyyy-MM-dd HH:mm:ss")))
print("DataFrame with Extracted Minute:")
df_minute.display(truncate=False)

# 4. Extract second from the timestamp
df_second = df.withColumn("second", second(to_timestamp("timestamp_string", "yyyy-MM-dd HH:mm:ss")))
print("DataFrame with Extracted Second:")
df_second.display(truncate=False)

# 5. Convert string to timestamp
df_with_timestamp = df.withColumn("timestamp", to_timestamp("timestamp_string", "yyyy-MM-dd HH:mm:ss"))
print("DataFrame with Converted Timestamp:")
df_with_timestamp.display(truncate=False)

# Stop the Spark session

# COMMAND ----------

df.withColumn("n1", to_date(current_timestamp())).display()

# COMMAND ----------

df1= df.withColumn("n1", to_timestamp(col("date_string")))
df1.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (select cast(date_string as date) date_data from date_data)
# MAGIC
# MAGIC /*select * , dateadd(month, 1, date_data) as months_added, 
# MAGIC dateadd(day, 1, date_data) date_added ,
# MAGIC dateadd(day, -1, date_data) date_subbed, 
# MAGIC DATEDIFF(day , date_data, getdate()) date_diff,
# MAGIC DATEDIFF(month , date_data, getdate()) month_diff
# MAGIC from cte;
# MAGIC
# MAGIC SELECT *, 
# MAGIC        DATEADD(DAY, (7 + 3 - DATEPART(WEEKDAY, date_data)) % 7, date_data) AS next_wednesday
# MAGIC FROM cte
# MAGIC
# MAGIC SELECT DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AS FirstOfMonth
# MAGIC
# MAGIC select datepart(weekday, getdate())*/
