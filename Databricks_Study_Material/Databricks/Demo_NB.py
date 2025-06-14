# Databricks notebook source
print("python")

# COMMAND ----------

df=spark.read.format("parquet").load("/FileStore/tables/userdata1.parquet")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *
df1=df.withColumn("new", lit("any"))

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.write.format("delta").save("/FileStore/tables/24thFeb")

# COMMAND ----------

df2=spark.read.load("/FileStore/tables/24thFeb")
df3=df2.withColumn("new2", lit("soma"))
df3.select("")
display(df2)

# COMMAND ----------


