# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window

# COMMAND ----------

data=[{'x':1, 'y':2}, {'x':1, 'c':2}]

df=spark.createDataFrame(data)
df.display()

# COMMAND ----------

data={'x':1, 'y':2}

df=spark.createDataFrame( (key, value)for key, value in data.items())
df.display()

# COMMAND ----------

data= [('{"x":1, "y":2}', )]

json_schema= StructType([
  StructField("x", StringType(), True),
  StructField("y", IntegerType(), True)
  ])

df=spark.createDataFrame(data, ["json_string"])

df1=df.withColumn("s", from_json(col("json_string"), json_schema))
df.display()
df1.display()

# COMMAND ----------

data = [

('{"user":100, "ips" : ["191.168.192.101", "191.168.192.103", "191.168.192.96", "191.168.192.99"]}',),

('{"user":101, "ips" : ["191.168.192.102", "191.168.192.105", "191.168.192.103", "191.168.192.107"]}',),

('{"user":102, "ips" : ["191.168.192.105", "191.168.192.101", "191.168.192.105", "191.168.192.107"]}',),

('{"user":103, "ips" : ["191.168.192.96", "191.168.192.100", "191.168.192.107", "191.168.192.101"]}',),

('{"user":104, "ips": ["191.168.192.99", "191.168.192.99", "191.168.192.102", "191.168.192.99"]}',),

('{"user":105, "ips" : ["191.168.192.99", "191.168.192.99", "191.168.192.100", "191.168.192.96"]}',)]

json_schema= StructType([
   StructField("user", StringType(), True),
   StructField("ips", ArrayType(StringType()), True)

])

df=spark.createDataFrame(data, ['json_string'])
df1=df.withColumn("new", from_json(col("json_string"), json_schema)).select("new.*")
max_val=df1.selectExpr("max(size(ips))").collect()[0][0]

for i in range(max_val):
  df1=df1.withColumn( str(i+1) + "_col", col('ips').getItem(i))
# df1.withColumn("1st", col('ips').getItem(0)).display()
df1.display()

# COMMAND ----------


