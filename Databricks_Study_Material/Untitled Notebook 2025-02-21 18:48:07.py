# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from datetime import datetime

# COMMAND ----------

data=[('jan', ), ('mar',)]

df=spark.createDataFrame(data, ["month"])


df.withColumn('month_number', month(to_date(col('month'), 'MMM'))).display()

# COMMAND ----------

windowSpec=Window.partitionBy('id').orderBy(col('Date')).rowsBetween(Window.unboundedPreceding, Window.currentRow-1)
df.withColumn('n', sum('Amt').over(windowSpec)).display()

# COMMAND ----------

df.na.drop(subset=['price', 'quantity']).display()

# COMMAND ----------

windowSpec1=Window.partitionBy('Store')
windowSpec2=Window.partitionBy('Store').orderBy(col('Sales').desc())
df.filter("Sales >= 1000 ").withColumn("total_sales", sum('Sales').over(windowSpec1)).withColumn("cum_sales", sum('Sales').over(windowSpec2)).display()

# COMMAND ----------

worker_schema = StructType([
    StructField("worker_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("joining_date", DateType(), True),
    StructField("department", StringType(), True)
])

def parse_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()
  

# Define data for worker table
worker_data = [
    (1, "John", "Doe", 5000, parse_date("2023-01-01"), "Engineering"),
    (2, "Jane", "Smith", 6000, parse_date("2023-01-15"), "Marketing"),
    (3, "Alice", "Johnson", 4500, parse_date("2023-02-05"), "Engineering")
]

# Create worker DataFrame
worker_df = spark.createDataFrame(worker_data, schema=worker_schema)

# Define schema for title table
title_schema = StructType([
    StructField("worker_ref_id", IntegerType(), True),
    StructField("worker_title", StringType(), True),
    StructField("affected_from", DateType(), True)
])

# Define data for title table
title_data = [
    (1, "Engineer", parse_date("2022-01-01")),
    (2, "Manager", parse_date("2022-01-01")),
    (3, "Engineer", parse_date("2022-01-01"))
]

# Create title DataFrame
title_df = spark.createDataFrame(title_data, schema=title_schema)

# Show the DataFrames
worker_df.display()
title_df.display()

# COMMAND ----------

window_asc = Window.orderBy("salary")
window_desc = Window.orderBy(col("salary").desc())

# Add row numbers and filter for lowest/highest salary
result_df = worker_df \
    .withColumn("Lowest", row_number().over(window_asc)) \
    .withColumn("Highest", row_number().over(window_desc)) \
    .filter((col("Lowest") == 1) | (col("Highest") == 1))

result_df.display()

# COMMAND ----------

print(worker_df.select(max('salary')).collect()[0][0])

# COMMAND ----------

worker_df.agg({'salary':'max' , 'salary':'min'}).display()

# COMMAND ----------

schema = StructType([
    StructField("StudentID", IntegerType(), True),
    StructField("StudentName", StringType(), True),
    StructField("Subject", StringType(), True),
    StructField("Score", IntegerType(), True)
])

# Create data
data = [
    (123, "A", "AScore", 30),
    (123, "A", "BScore", 31),
    (123, "A", "CScore", 32),
    (124, "B", "AScore", 40),
    (124, "B", "BScore", 41),
    (124, "B", "CScore", 42)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show()

# COMMAND ----------

df.groupBy('StudentID', 'StudentName').pivot('Subject').agg(sum('Score')).display()

# COMMAND ----------

schema = StructType([
    StructField("StudentID", IntegerType(), True),
    StructField("StudentName", StringType(), True),
    StructField("AScore", IntegerType(), True),
    StructField("BScore", IntegerType(), True),
    StructField("CScore", IntegerType(), True)
])

# Create data
data = [
    (123, "A", 30, 31, 32),
    (124, "B", 40, 41, 42)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show()

# COMMAND ----------

df.withColumn(
    "zipped",
    explode(arrays_zip(
        array(lit("AScore"), lit("BScore"), lit("CScore")).alias("Subject"),
        array("AScore", "BScore", "CScore").alias("Score")
    ))
).select(
    "StudentID",
    "StudentName",
    col("zipped.Subject").alias("Subject"),
    col("zipped.Score").alias("Score")
).show()

# COMMAND ----------

data = [ (1, "Laptop", 1000, 5), (2, "Mouse", None, None), (3, "Keyboard", 50, 2), (4, "Monitor", 200, None), (5, None, 500, None), ] 

# Define schema and create DataFrame 
columns = ["product_id", "product", "price", "quantity"]

df=spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------

avg_val=df.agg({'price':'avg'}).collect()[0][0]
df1=df.na.fill({'price':avg_val}).na.drop(subset=['product'])
df2=df1.na.fill({'quantity':1})
df2.display()

# COMMAND ----------

data = [ (1, "Laptop", "2024-08-01"), (1, "Mouse", "2024-08-05"), (2, "Keyboard", "2024-08-02"), (2, "Monitor", "2024-08-03") ] 
columns = ["customer_id", "product", "purchase_date"]
df=spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------

df.withColumn('rn', row_number().over(Window.partitionBy('customer_id').orderBy(col('purchase_date').desc()))).filter("rn=1").display()

# COMMAND ----------

data = [ (1, "HR", 50000, "2022-01-01"), (2, "IT", 70000, "2021-06-15"), (3, "HR", 60000, "2023-03-10"), (4, "IT", 80000, "2020-12-01"), ] 

# Define schema and create DataFrame 
columns = ["employee_id", "department", "salary", "join_date"] 
df = spark.createDataFrame(data, columns)

df.display()


# COMMAND ----------

df.withColumn('cum_sum', sum('salary').over(Window.partitionBy('department').orderBy(col('join_date')))).withColumn('rnk', dense_rank().over(Window.partitionBy('department').orderBy(col('salary').desc()))).display()

# COMMAND ----------

data = [ ("U1", "2024-12-30 10:00:00", "LOGIN"), ("U1", "2024-12-30 10:05:00", "BROWSE"), ("U1", "2024-12-30 10:20:00", "LOGOUT"), ("U2", "2024-12-30 11:00:00", "LOGIN"), ("U2", "2024-12-30 11:15:00", "BROWSE"), ("U2", "2024-12-30 11:30:00", "LOGOUT"), ("U1", "2024-12-30 10:20:00", "LOGOUT") ,(None, "2024-12-30 12:00:00", "LOGIN"), 
("U3", None, "LOGOUT") ]
 # Define schema 
schema = StructType([ StructField("user_id", StringType(), True), StructField("timestamp", StringType(), True), StructField("activity_type", StringType(), True) ])
df = spark.createDataFrame(data, schema)
df.display()

# COMMAND ----------

df.distinct().na.drop(subset=['user_id', 'timestamp']).withColumn('next', lead('timestamp').over(Window.partitionBy('user_id').orderBy('timestamp')))\
  .withColumn('n', (unix_timestamp(col('next'))-unix_timestamp(col('timestamp')))/60).display()

# COMMAND ----------

data = [
 ("HR", "2020", 2500), 
 ("HR", "2021", 3200), 
 ("HR", "2022", 2800), 
 ("Engineering", "2020", 5000), 
 ("Engineering", "2021", 6000), 
 ("Engineering", "2022", 5500), 
 ("Marketing", "2020", 4000), 
 ("Marketing", "2021", 3500), 
 ("Marketing", "2022", 3300)
]

# Create DataFrame
df = spark.createDataFrame(data, ["Department", "Year", "Salary"])
df.display()

# COMMAND ----------

df.withColumn("avg_m_sal", avg('salary').over(Window.partitionBy('department'))).withColumn("total_sal", sum('salary').over(Window.partitionBy('department'))).filter("salary>=avg_m_sal").display()

# COMMAND ----------

data = [ (1, 101, 500.0, "2024-01-01"), (2, 102, 200.0, "2024-01-02"), 
(3, 101, 300.0, "2024-01-03"), (4, 103, 100.0, "2024-01-04"), 
(5, 102, 400.0, "2024-01-05"), (6, 103, 600.0, "2024-01-04"), 
(7, 101, 200.0, "2024-01-07"), ] 

columns = ["transaction_id", "user_id", "transaction_amount", "transaction_date"]
df = spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------

df.groupBy('user_id').agg(sum('transaction_amount').alias('total_amount'), max('transaction_date').alias('recent_date')).display()
# .filter("rn>=3").display()

# COMMAND ----------

data = [
 (1, 101, 500.0, "2024-01-01"), 
 (2, 102, 200.0, "2024-01-02"), 
 (3, 101, 300.0, "2024-01-03"), 
 (4, 103, 100.0, "2024-01-04"), 
 (5, 102, 400.0, "2024-01-05"), 
 (6, 103, 600.0, "2024-01-06"), 
 (7, 101, 200.0, "2024-01-07"),
]
columns = ["transaction_id", "user_id", "transaction_amount", "transaction_date"]

df = spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------

df.withColumn("prev", coalesce(lag('transaction_date').over(Window.partitionBy('user_id').orderBy('transaction_date')), 'transaction_date')).withColumn('diff', datediff( 'transaction_date', 'prev'))\
  .groupBy('user_id').avg('diff').display()

# COMMAND ----------

data = [
 ("Alice", "Math", "Semester1", 85),
 ("Alice", "Math", "Semester2", 90),
 ("Alice", "English", "Semester1", 78),
 ("Alice", "English", "Semester2", 82),
 ("Bob", "Math", "Semester1", 65),
 ("Bob", "Math", "Semester2", 70),
 ("Bob", "English", "Semester1", 60),
 ("Bob", "English", "Semester2", 65),
 ("Charlie", "Math", "Semester1", 95),
 ("Charlie", "Math", "Semester2", 98),
 ("Charlie", "English", "Semester1", 88),
 ("Charlie", "English", "Semester2", 90),
 ("David", "Math", "Semester1", 78),
 ("David", "Math", "Semester2", 80),
 ("David", "English", "Semester1", 75),
 ("David", "English", "Semester2", 72),
 ("Eve", "Math", "Semester1", 88),
 ("Eve", "Math", "Semester2", 85),
 ("Eve", "English", "Semester1", 80),
 ("Eve", "English", "Semester2", 83)
]

# Create DataFrame
df = spark.createDataFrame(data, ["Student", "Subject", "Semester", "Grade"])

df.display()

# COMMAND ----------

df.groupBy('student').agg(avg('grade').alias('avg_grade')).filter("avg_grade >= 75").display()

# COMMAND ----------

df.withColumn("rn", row_number().over(Window.partitionBy('subject').orderBy(col('semester').desc() , col('Grade').desc()))).filter("rn<=3").display()

# COMMAND ----------

products_data = [ (1, "Laptop", 101), (2, "Smartphone", 102), (3, "Tablet", 101) ] 

products_schema = ["product_id", "product_name", "category_id"] 

products_df = spark.createDataFrame(products_data, schema=products_schema) 
products_df.display()
# Create Dataset 2 (Categories) 

categories_data = [ (101, "Electronics"), (102, "Mobile"), (103, "Home Appliance") ] 

categories_schema = ["cat_id", "category_name"]

categories_df = spark.createDataFrame(categories_data, schema=categories_schema) 
categories_df.display()

# COMMAND ----------

categories_df.join(products_df , categories_df.cat_id==products_df.category_id, 'inner').select('product_id', 'product_name','category_name' ).display()

# COMMAND ----------

data = [
 (1, "Laptop", 1000, 5),
 (2, "Mouse", None, None),
 (3, "Keyboard", 50, 2),
 (4, "Monitor", 200, None),
 (5, None, 500, None),
]
columns = ["product_id", "product", "price", "quantity"]

df = spark.createDataFrame(data, schema=columns) 
df.display()

# COMMAND ----------

listt=df.select('price').rdd.flatMap(lambda x : x).collect()
median =listt[len(listt)//2]

# COMMAND ----------

df.na.fill({'price': median}).display()

# COMMAND ----------

data = [ ("A", "North", 100), ("B", "East", 200), ("A", "East", 150), ("C", "North", 300), ("B", "South", 400), ("C", "East", 250) ] 
columns = ["Product", "Region", "Sales"]

df=spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------

df.groupBy('product').pivot('region').agg(sum('sales')).display()

# COMMAND ----------

@udf
def temp_f(x):
  return x*x
# temp1=udf(temp_f)
df.withColumn("new", temp_f(col('Sales'))).display()

# COMMAND ----------

x={'North':'Delhi', 'East':'Bengal', 'south': 'Bangalore'}

xy=sc.broadcast(x)

df.withColumn('n', xy,get(col(x))).display()

# COMMAND ----------

df.createOrReplaceTempView('df_t')
df.createOrReplaceGlobalTempView('df_g')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df_t

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.df_g

# COMMAND ----------

df.schema.fields

# COMMAND ----------

df.describe().show()

# COMMAND ----------

import requests

# Fetch the Wikipedia page
dd = requests.get('https://en.wikipedia.org/wiki/Data')

# View the HTML content
html = dd.text

# Print part of it
print(html[:500])


# COMMAND ----------

dd=requests.get('https://en.wikipedia.org/wiki/Data')
dd.text

# COMMAND ----------

print(html[:500])

# COMMAND ----------


