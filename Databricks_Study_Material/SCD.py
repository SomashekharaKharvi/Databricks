# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# Current dimension table (existing data)
current_data = [
    (1, "John", "john@example.com"),
    (2, "Jane", "jane@example.com"),
]

current_df = spark.createDataFrame(current_data, ["CustomerID", "Name", "Email"])

# Incoming changes (new data)
incoming_data = [
    (1, "John", "john.doe@example.com"),  # Email changed
    # (2, "Jane", "jane@example.com")      # No change
    (3, "Mike", "mike@example.com")      # New customer
]

incoming_df = spark.createDataFrame(incoming_data, ["CustomerID", "Name", "Email"])

current_df.display()
incoming_df.display()

# COMMAND ----------

updated_df_type1 = current_df.alias("current").join(
    incoming_df.alias("incoming"),
    "CustomerID",
    "full"
)
updated_df_type1.display()

# COMMAND ----------

# DBTITLE 1,SCD_1
# SCD Type 1: Overwrite existing records
updated_df_type1 = current_df.alias("current").join(
    incoming_df.alias("incoming"),
    "CustomerID",
    "outer"
).select(
 coalesce(incoming_df.CustomerID, current_df.CustomerID).alias("CustomerID"),
 coalesce(incoming_df.Name, current_df.Name).alias("Name"),
 coalesce(incoming_df.Email, current_df.Email).alias("Email")
)
# .na.fill({"Name": current_df["Name"], "Email": current_df["Email"]})

print("SCD Type 1 Result:")
updated_df_type1.show()

# COMMAND ----------

dbutils.fs.rm('dbfs:/user/hive/warehouse/scd2demo', True)

# COMMAND ----------

# DBTITLE 1,SCD_2
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE
# MAGIC scd2Demo (
# MAGIC pk1 INT,
# MAGIC pk2 STRING,
# MAGIC diml INT,
# MAGIC dim2 INT,
# MAGIC dim3 INT,
# MAGIC dim4 INT,
# MAGIC active_status STRING,
# MAGIC syart_date TIMESTAMP,
# MAGIC end_date TIMESTAMP)
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into scd2Demo values (111, 'Unit1', 200,500,800,400, 'Y', current_timestamp(),'9999-12-31');
# MAGIC
# MAGIC insert into scd2Demo values (222, 'Unit2', 900, Null, 709, 180, 'Y', current_timestamp(), '9999-12-31');
# MAGIC
# MAGIC insert into scd2Demo values (333, 'Unit3', 300,900,250,650, 'Y', current_timestamp(), '9999-12-31');

# COMMAND ----------

from delta import *
targetTable=DeltaTable.forPath(spark,'dbfs:/user/hive/warehouse/scd2demo')
targetDF=targetTable.toDF()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
schema = StructType([
  StructField("pk1", StringType(), True), \
StructField("pk2", StringType(), True), \
StructField("diml", IntegerType(), True), \
StructField("dim2", IntegerType(), True), \
StructField("dim3", IntegerType(), True), \
StructField("dim4", IntegerType(), True)])

data = [(111, 'Unit1', 200,500,800,400),
(222, "Unit2",800,1300,800,500),
(444, "Unit4", 100, None, 700,300)]
sourceDF=spark.createDataFrame(data=data, schema=schema)

# COMMAND ----------

print("Source DF--------------------------->")
display(sourceDF)
print("Target DF--------------------------->")
targetDF.display()

# COMMAND ----------

joinDF=sourceDF.join(targetDF, 'pk1', 'left').select(sourceDF["*"],
                                                      targetDF.pk1.alias("target_pk1"),
                                                      targetDF.pk2.alias("target_pk2"),
                                                      targetDF.diml.alias("target_diml"),
                                                      targetDF.dim2.alias("target_dim2"),
                                                      targetDF.dim3.alias("target_dim3"),
                                                      targetDF.dim4.alias("target_dim4"))
joinDF.display()

# COMMAND ----------

filterDF=joinDF.filter(xxhash64(joinDF.diml,joinDF.dim2,joinDF.dim3,joinDF.dim4) != xxhash64(joinDF.target_diml,joinDF.target_dim2,joinDF.target_dim3,joinDF.target_dim4))
filterDF.display()

# COMMAND ----------

merge_DF=filterDF.withColumn("merge_key", concat(col('pk1'),col('pk2')))
merge_DF.display()

# COMMAND ----------

dummyDF=filterDF.filter("target_pk1 is not null").withColumn("merge_key", lit(None))
dummyDF.display()

# COMMAND ----------

scdDF=merge_DF.union(dummyDF)
scdDF.display()

# COMMAND ----------

targetTable.alias("target").merge(
source = scdDF.alias("source"),
condition = "concat(target.pk1, target.pk2) = source.merge_key and target.active_status ='Y' "
).whenMatchedUpdate(set =
{
"active_status": "'N'",
"end_date": "current_date"
}
).whenNotMatchedInsert(values =
{
"pk1": "source.pk1",
"pk2": "source.pk2",
"diml": "source.diml",
"dim2": "source.dim2",
"dim3": "source.dim3",
"dim4": "source.dim4",
"active_status": "'Y'",
"syart_date": "current_date",
"end_date": """to_date('9999-12-31','yyyy-MM-dd')"""
}
).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from scd2demo

# COMMAND ----------


