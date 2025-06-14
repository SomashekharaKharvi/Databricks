// Databricks notebook source
spark.sql("select * from myTable")

// COMMAND ----------

val df=spark.read.option("inferSchema","true").option("header","true").option("multiLine","true").csv("/FileStore/tables/Uspopulation1.csv")

// COMMAND ----------

df.show()

// COMMAND ----------

df.write.saveAsTable("uspop")

// COMMAND ----------

spark.sql("select * from uspop limit 5").show()

// COMMAND ----------

spark.sql("create database bdp")

// COMMAND ----------

spark.sql("create table  headertab1 (u_name STRING, idf BIGINT,Cn STRING,Ot STRING)row format delimited fields terminated by'|' stored as textfile location 'FileStore/tables/sample_2.csv' tblproperties("skip.header.line.count"="2'"")" )

// COMMAND ----------

spark.sql("select * from headertab1").show()

// COMMAND ----------

spark.sql("load data inpath 'dbfs:/FileStore/tables/sample_2.csv' into table headertab1")

// COMMAND ----------

// MAGIC %fs
// MAGIC ls

// COMMAND ----------

spark.sql("drop table headertab1")

// COMMAND ----------

df.write.format("parquet").saveAsTable("newTab")

// COMMAND ----------

spark.sql("INSERT OVERWRITE LOCAL DIRECTORY 'Filestore/new' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT * FROM uspop")

// COMMAND ----------

val df=spark.read.option("inferSchema","true").option("header","true").option("multiLine","true").csv("/FileStore/tables/Uspopulation1.csv")

// COMMAND ----------

df.write.saveAsTable("uspop1")

// COMMAND ----------

df.printSchema()

// COMMAND ----------

spark.sql("create table staticuspop (rank   int,City   string,estimate int,Census  int,change  string)PARTITIONED  by (State_Code  string)ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from staticuspop

// COMMAND ----------

spark.sql("load data inpath '/FileStore/tables/Uspopulation1.csv' into table staticuspop PARTITION ( State_Code='KSK')")


// COMMAND ----------

spark.sql("create table dynamicuspop (rank   int,City   string,estimate int,Census  int,change  string)PARTITIONED  by (State_Code  string)ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")

// COMMAND ----------

spark.sql("INSERT OVERWRITE TABLE dynamicuspop PARTITION(State_Code) SELECT rank,City,estimate,census,Change,State_Code FROM uspop3")

// COMMAND ----------

df.printSchema()

// COMMAND ----------

df.withColumnRenamed("rank","2019_rank").withColumnRenamed("estimate","2019_estimate").withColumnRenamed("2010_Census","Census").show()

// COMMAND ----------

df.printSchema()

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from uspop1

// COMMAND ----------

val df1=spark.sql("select 2019_rank as rank ,City,State_Code,2019_estimate as estimate,2010_Census census,Change from uspop1")

// COMMAND ----------

df1.show()

// COMMAND ----------

df1.write.saveAsTable("uspop3")

// COMMAND ----------

spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict")

// COMMAND ----------

spark.sql("create table bucketeduspop(rank int,City string,State_Code string,estimate int,Census  int,change  string)CLUSTERED  by (census) sorted by (census asc) into 4 buckets")

// COMMAND ----------

spark.conf.set("hive.enforce.bucketing","TRUE")

// COMMAND ----------

spark.sql("show partitions dynamicuspop").show()

// COMMAND ----------

spark.sql("describe formatted dynamicuspop").show()

// COMMAND ----------

spark.sql("ALTER table dynamicuspop drop if exists partition (State_Code='AZ')")

// COMMAND ----------

spark.sql("show partitions dynamicuspop ").show()

// COMMAND ----------


