# Databricks notebook source
print("hello")

# COMMAND ----------

# from pyspark.sql import SparkSession
# spark = SparkSession.builder.master("local[1]") \
#                     .appName('SparkByExamples.com') \
#                     .getOrCreate()
# print(spark.sparkContext)
# print("Spark App Name : "+

# COMMAND ----------

rdd = sc.parallelize (
   ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]
)
rddd= sc.parallelize (
   ["Hive", 
   "Hbase", 
   "AWS" ]
)

# COMMAND ----------

type(rdd)

# COMMAND ----------

# MAGIC %md Narrow Transformation

# COMMAND ----------

# map(), flatMap(), filter(), union() 

# COMMAND ----------

words_filter = rdd.filter(lambda x: 'spark' in x)
words_filter.collect()

# COMMAND ----------

rdd2=rdd.flatMap(lambda x: x.split(" "))
rdd2.collect()

# COMMAND ----------

rdd3=rdd2.map(lambda x: (x,1))
rdd3.collect()

# COMMAND ----------

union_rdd=rdd.union(rddd)
union_rdd.collect()

# COMMAND ----------

# MAGIC %md Wide transformation

# COMMAND ----------

# groupByKey(), aggregateByKey(), aggregate(), join(), repartition()

# COMMAND ----------

rdd4=rdd3.reduceByKey(lambda a,b: a+b)
rdd4.collect()

# COMMAND ----------


rdd5 = rdd4.map(lambda x: (x[1],x[0])).sortByKey()
rdd5.collect()

# COMMAND ----------

# MAGIC %md Actions

# COMMAND ----------

data=[("Z", 1),("A", 20),("B", 30),("C", 40),("B", 30),("B", 60)]
inputRDD = spark.sparkContext.parallelize(data)
listRdd = spark.sparkContext.parallelize([1,2,3,4,5,3,2])

# COMMAND ----------

redRes=listRdd.reduce(lambda x, y: x + y)
print(redRes)

# COMMAND ----------

listRdd.collect()

# COMMAND ----------

listRdd.count()

# COMMAND ----------

listRdd.first()

# COMMAND ----------

listRdd.top(2)

# COMMAND ----------

listRdd.max()

# COMMAND ----------

listRdd.take(2)

# COMMAND ----------

# MAGIC %md Different ways to create Spark RDD

# COMMAND ----------

rdd=spark.sparkContext.parallelize([("Java", 20000), 
  ("Python", 100000), ("Scala", 3000)])
rdd.collect()

# COMMAND ----------

rdd = spark.sparkContext.textFile("/FileStore/tables/new.txt")
rdd.collect()

# COMMAND ----------

# Creating from another RDD
rdd1=rdd.flatMap(lambda x: x.split(","))
rdd2= rdd1.filter(lambda x: 'USA' in x)
rdd2.collect()

# COMMAND ----------

# dbutils.notebook.exit("stop")

# COMMAND ----------

variable_a= "from the RDD notebook"

# COMMAND ----------

# dbutils.notebook.help()

# COMMAND ----------

# dbutils.notebook.run("Word Count", 5643)

# COMMAND ----------


