# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data=(1, 2, 3, 4, 5)
data1={2:1,3:2}
data2=[1, 2,3, 4, 5]
rdd=sc.parallelize(data2)

rdd1=rdd.map(lambda x: x+2)
rdd1.collect()

# COMMAND ----------

num_elements = rdd.take(2)
print(num_elements)

# COMMAND ----------


# Create a Spark session
# Create an RDD
rdd1 = spark.sparkContext.parallelize([1, 2, 3, 4])

# Convert RDD to DataFrame
df = rdd1.map(lambda x :(x,)).toDF('c')

# Show the DataFrame
df.show()

# COMMAND ----------

data=[(1, 2, 3, 4, 5), (4,6,7,8,3)]
data1={2:1,3:2}
rdd=sc.parallelize(data)

rdd.flatMap(lambda x: tuple( i+1 for i in x)).collect()

# COMMAND ----------

data = [[1, 2, 3], [4, 5], [6, 7, 8, 9]]
rdd=sc.parallelize(data)
rdd3=rdd.flatMap( lambda x: [ i+1 for i in x])

# COMMAND ----------

rdd.filter( lambda x : [ i== 2 for i in x]).collect()

# COMMAND ----------

data = [
    "Hello world",
    "Hello from PySpark",
    "Word count example in PySpark",
    "Hello world again"
]

rdd=sc.parallelize(data)
rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b).collect()

# COMMAND ----------

data=" ksdjfbkd sdfh iadfl idshf lAFD"
rdd=sc.parallelize(data.split(" "))
rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b).collect()

# COMMAND ----------

rdd= sc.textFile("/FileStore/tables/sample.txt")
rdd2=rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b).sortByKey(ascending=False)

# COMMAND ----------

rdd2.explain()

# COMMAND ----------

data=" ksdjfbkd sdfh iadfl idshf lAFD"
rdd=sc.parallelize(data)
rdd.collect()
# rdd.flatMap(lambda x: x.split(" ")).collect()

# COMMAND ----------

data=[1, 2, 3, 4, 5]
data1=[4,6,7,8,3]
rdd=sc.parallelize(data)
rdd1=sc.parallelize(data1)
rdd.union(rdd1).collect()

# COMMAND ----------

data=[1, 2, 3, 4, 5]
data1=[4,6,7,8,3]
rdd=sc.parallelize(data)
rdd1=sc.parallelize(data1)
rdd.intersection(rdd1).collect()

# COMMAND ----------

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("This is an info message")
logger.error("This is an error message")

# COMMAND ----------

df=rdd.map(lambda x : (x,)).toDF()

# COMMAND ----------

df.explain()

# COMMAND ----------

from pyspark import SparkContext

# Create a SparkContext

# Create an RDD with 6 elements and 3 partitions
data = [1, 2, 3, 4, 5, 6]
rdd = sc.parallelize(data, 3)  # 3 partitions

# Use glom() to group elements of each partition into lists
rdd_glommed = rdd.glom()

# Collect and print the results
result = rdd_glommed.collect()
print(result)  # Output will be a list of lists, e.g., [[1, 2], [3, 4], [5, 6]]

# COMMAND ----------

data=[{'x':1, 'y':2}]

spark.createDataFrame(data, ["k","y"]).display()

# COMMAND ----------

data='[{"x":"1", "y":"2"},{"x":"3", "y":"4"}]'

df=spark.createDataFrame([(data,)], ["json_string"])

schema=StructType([
  StructField("x", StringType(), True),
  StructField("y", StringType(), True)
])
df1=df.withColumn("mew",from_json(col("json_string"),ArrayType(schema)))
df2=df1.select("mew",explode(col('mew')).alias("mew1"))
display(df2.select("mew1.*"))

# COMMAND ----------

data = [(1, [Row(age=2, name='Alice'), Row(age=3, name='Bob')])]
df = spark.createDataFrame(data, ("key", "value"))
df.display()
json_df=df.select("value",to_json(col("value")).alias("new"))
json_df.display()


# COMMAND ----------

# Create a Spark session

# Sample data
data = [("Alice", "   Developer   "), ("Bob", "   Data Scientist   "), ("Charlie", "   Data Engineer   ")]
df = spark.createDataFrame(data, ["name", "job_title"])

# Show the original DataFrame
df.select("*", substring(trim(col("job_title")), 1, 3)).display()

df.withColumn("new_job_title", split(trim(col("job_title")), " ").getItem(0)).display()


# COMMAND ----------

df1=df.withColumn("new_job_title", split(trim(col("job_title")), " ")).select(col("new_job_title").getItem(1).alias("som"))
df1.display()

# COMMAND ----------

data = [
    (1, "Software Engineer"),
    (2, "Data Scientist"),
    (3, "Software Engineer"),
    (4, "Product Manager"),
    (5, "Data Analyst")
]

# Define the schema
df = spark.createDataFrame(data, ["id", "job_title"])
df.display()
listtt=df.select(collect_set(col("job_title")))
listtt.display()
print(listtt.collect()[0][0])

# COMMAND ----------

# Sample data
data = [
    ("2023-01-01", "Product A", 100),
    ("2023-01-01", "Product B", 150),
    ("2023-01-02", "Product A", 200),
    ("2023-01-02", "Product B", 250),
    ("2023-01-03", "Product A", 300),
    ("2023-01-03", "Product B", 350)
]

# Create DataFrame
df = spark.createDataFrame(data, ["date", "product", "sales"])

# Show the original DataFrame
df.show()

# COMMAND ----------

df.groupBy("date").pivot("product").agg(sum(col("sales")).alias("sales")).display()

# COMMAND ----------

df.groupBy('product').pivot('date').agg(sum('sales').alias('sum')).display()

# COMMAND ----------


