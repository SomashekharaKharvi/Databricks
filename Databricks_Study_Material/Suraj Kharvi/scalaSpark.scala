// Databricks notebook source
import org.apache.spark.sql.SparkSession
val spark=SparkSession.builder().master("local[*]").appName("myapp").getOrCreate

// COMMAND ----------

val sc=spark.sparkContext

// COMMAND ----------

val data= Seq(("suraj",24),("praveen",24))
val rdd=sc.parallelize(data)
rdd.collect()

// COMMAND ----------

val rdd1=sc.parallelize("/FileStore/tables/Uspopulation1.csv")

// COMMAND ----------

// val r=rdd1.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceBy(_+_)
 val rdd5 = spark.sparkContext.parallelize(
      List("Germany India USA","USA India Russia","India Brazil Canada China")
    )
val counts = rdd5.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

counts.sortByKey().collect()
counts.collect()

// COMMAND ----------

val empRDD=sc.emptyRDD[(String,Int)]

// COMMAND ----------

counts.count()

// COMMAND ----------

val columns = Seq("language","users_count")
val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
val df=sc.parallelize(data).toDF(columns:_*)

// COMMAND ----------

df.show()

// COMMAND ----------

val df1=spark.createDataFrame(data,schema=columns)

// COMMAND ----------

df1.show

// COMMAND ----------

val data = Seq(("James"," ","Smith","1991-04-01","M",3000),
  ("Michael","Rose"," ","2000-05-19","M",4000),
  ("Robert"," ","Williams","1978-09-05","M",4000),
  ("Maria","Anne","Jones","1967-12-01","F",4000),
  ("Jen","Mary","Brown","1980-02-17","F",-1)
)

val columns = Seq("firstname","middlename","lastname","dob","gender","salary")
val df = spark.createDataFrame(data).toDF(columns:_*)

// COMMAND ----------

df.show()

// COMMAND ----------

val empdf=sc.emptyRDD[(String)].toDF()
empdf.show()

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
val data = Seq(Row(Row("James ","","Smith"),"36636","M",3000),
  Row(Row("Michael ","Rose",""),"40288","M",4000),
  Row(Row("Robert ","","Williams"),"42114","M",4000),
  Row(Row("Maria ","Anne","Jones"),"39192","F",4000),
  Row(Row("Jen","Mary","Brown"),"","F",-1)
)


val schema = new StructType()
  .add("name",new StructType()
    .add("firstname",StringType)
    .add("middlename",StringType)
    .add("lastname",StringType))
  .add("dob",StringType)
  .add("gender",StringType)
  .add("salary",IntegerType)

val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
df.printSchema()

// COMMAND ----------

df.withColumn("firstname",$"name.firstname").drop($"name").printSchema

// COMMAND ----------

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType} 
val data = Seq(Row(Row("James;","","Smith"),"36636","M","3000"),
      Row(Row("Michael","Rose",""),"40288","M","4000"),
      Row(Row("Robert","","Williams"),"42114","M","4000"),
      Row(Row("Maria","Anne","Jones"),"39192","F","4000"),
      Row(Row("Jen","Mary","Brown"),"","F","-1")
)

val schema = new StructType()
      .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
      .add("dob",StringType)
      .add("gender",StringType)
      .add("salary",StringType)

val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
df.show

// COMMAND ----------

import org.apache.spark.sql.functions.lit

val df1=df.withColumn("country",lit("USA"))
df1.show()

// COMMAND ----------

df1.printSchema

// COMMAND ----------

df1.withColumn("dob",col("dob").cast(IntegerType))

// COMMAND ----------

df1.withColumn("dob",when(col("dob")===36636, lit(2411))).show()

// COMMAND ----------

val simpleData = Seq(("James","Sales",3000),
      ("Michael","Sales",4600),
      ("Robert","Sales",4100),
      ("Maria","Finance",3000),
      ("Raman","Finance",3000),
      ("Scott","Finance",3300),
      ("Jen","Finance",3900),
      ("Jeff","Marketing",3000),
      ("Kumar","Marketing",2000)
    )
import spark.implicits._
val df = simpleData.toDF("Name","Department","Salary")
df.show()

// COMMAND ----------


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val windowspec=Window.partitionBy($"Department").orderBy($"salary")
val df1=df.withColumn("rank",row_number().over(windowspec)).where($"rank"===1)

// COMMAND ----------

df1.show()

// COMMAND ----------

df1.cache()

// COMMAND ----------

rdd1.mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}.collect


// COMMAND ----------

import org.apache.spark.sql.functions.spark_partition_id

val r=df.withColumn("partition_id", spark_partition_id).groupBy("partition_id")
r.count

// COMMAND ----------

var rdd= sc.parallelize(Seq("dsajyfg","error","error"))


// COMMAND ----------

var acc= sc.longAccumulator("error_Count")

rdd.foreach(x=> if (x.contains("error")) acc.add(1))

// COMMAND ----------

acc.value

// COMMAND ----------

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType} 
val data = Seq(Row(Row("James;","","Smith"),"36636","M","3000"),
      Row(Row("Michael","Rose",""),"40288","M","4000"),
      Row(Row("Robert","","Williams"),"42114","M","4000"),
      Row(Row("Maria","Anne","Jones"),"39192","F","4000"),
      Row(Row("Jen","Mary","Brown"),"","F","-1")
)

val schema = new StructType()
      .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
      .add("dob",StringType)
      .add("gender",StringType)
      .add("salary",StringType)

val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
df.show

// COMMAND ----------

val lookup=Map("M"->"MALE","F"->"FEMALE")
val broad=sc.broadcast(lookup)

// COMMAND ----------

val func=udf((x:String)=>{
   broad.value(x)
})

// COMMAND ----------


df.withColumn("new",func($"gender")).show()

// COMMAND ----------

df.createOrReplaceTempView("tab")

// COMMAND ----------

spark.sql("select getfullvalue(gender) from tab").show()

// COMMAND ----------

df.write.saveAsTable("suraj.myTable1")

// COMMAND ----------

// MAGIC %sql
// MAGIC create database suraj

// COMMAND ----------

// MAGIC %sql 
// MAGIC select * from suraj.mytable1

// COMMAND ----------

df.count

// COMMAND ----------

val rdd=sc.textFile("/FileStore/tables/Uspopulation1.csv")
rdd.collect

// COMMAND ----------

// Sacramento
// val s=! rdd.filter(line=>line.contains("Sacramento")).isEmpty()
val wordFound = !rdd.filter(line=>line.contains("Sacramento")).isEmpty()

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from mytable;
// MAGIC select * from suraj.mytable1;

// COMMAND ----------

df.write.saveAsTable("new1").path("/FileStore/")

// COMMAND ----------

df.write.parquet("/filestore/")

// COMMAND ----------


