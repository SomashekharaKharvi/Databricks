# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

rdd=sc.parallelize(data)

rdd.flatMap(lambda x: x.split(' ')).map(lambda x : (x, 1)).reduceByKey(lambda x, y : x+y).collect()

# COMMAND ----------

for i in range(size):
  df=df.withColumn(f"col_{i}", col('tags').getItem(i))

# COMMAND ----------

df.display()

# COMMAND ----------

df_uniform.withColumn("partition_id", spark_partition_id()).groupBy('partition_id').count()

# COMMAND ----------

df_skew1=df_skew.withColumn("salt", (rand()*3).cast('int'))

# COMMAND ----------

df_uniform1=df_uniform.withColumn("salt_val", array([ lit(i) for i in range(3)])).withColumn('salt', explode('salt_val'))

# COMMAND ----------

df_skew1.withColumn("partition_id", spark_partition_id()).groupBy('partition_id').count().display()

# COMMAND ----------

df_uniform1.withColumn("partition_id", spark_partition_id()).groupBy('partition_id').count().display()

# COMMAND ----------

df_joined = df_skew1.join(df_uniform1, ["value", "salt"], 'inner')

# COMMAND ----------

df_joined.withColumn('part_id', spark_partition_id())\
.groupBy('value', 'part_id').count().display()

# COMMAND ----------

df_skew1.groupBy('value', 'salt').agg(count('value').alias('count')).groupBy('value').sum('count').display()

# COMMAND ----------

(
    df_skew1
    .withColumn("salt", (F.rand() * 3).cast("int"))
    .groupBy("value", "salt")
    .agg(F.count("value").alias("count"))
    .groupBy("value")
    .agg(F.sum("count").alias("count"))
    .show()
)

# COMMAND ----------


