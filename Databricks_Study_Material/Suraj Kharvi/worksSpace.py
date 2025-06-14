# Databricks notebook source
# MAGIC %scala
# MAGIC object sclaExample{
# MAGIC
# MAGIC     def main(args: Array[String]) { 
# MAGIC
# MAGIC         println("hello")
# MAGIC     }
# MAGIC
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC object sclaExample{
# MAGIC
# MAGIC     def main(args: Array[String]) { 
# MAGIC
# MAGIC         println("hello")
# MAGIC       
# MAGIC        val a=10
# MAGIC       
# MAGIC        a match{
# MAGIC          case 1=> println("yes")
# MAGIC          case _=> println("no")
# MAGIC        }
# MAGIC     }
# MAGIC
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC println("hello")

# COMMAND ----------

# MAGIC %scala
# MAGIC   val a=0
# MAGIC       
# MAGIC        a match{
# MAGIC          case 1=> println("yes")
# MAGIC          case _=> println("no")
# MAGIC        }

# COMMAND ----------

# MAGIC %scala
# MAGIC for(i<-1 to 10 by -1){
# MAGIC   print(i)
# MAGIC }

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/FileStore/tables/Data/Outputs

# COMMAND ----------

# MAGIC %fs mkdir dbfs:/input

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists outputs

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists validdata_13012022

# COMMAND ----------

# validdata.write\
#     .mode("overwrite")\
#     .option("delimiter", "|")\
#     .option("header", True)\
#     .csv(outputLocation + "Valid/ValidData"+previousDaySuffix)
# invaliddata.write\
#     .mode("overwrite")\
#     .option("delimiter", "|")\
#     .option("header", True)\
#     .csv(outputLocation + "Hold/HoldData"+previousDaySuffix)

# COMMAND ----------

# MAGIC %md
# MAGIC # this is header
# MAGIC * this is bullet

# COMMAND ----------

print("hello")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC cat dbfs:/databricks-datasets/COVID/CSSEGISandData/README.md
# MAGIC

# COMMAND ----------

dbutils.fs.head("dbfs:/databricks-datasets/COVID/CSSEGISandData/README.md")

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.notebook.run("./childnotebook",10)

# COMMAND ----------

dbutils.widgets.text("input","","enter the value")

# COMMAND ----------

v=dbutils.widgets.get("input")
print(v)

# COMMAND ----------


