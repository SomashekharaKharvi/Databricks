# Databricks notebook source
data = [
    (1, "2017-01-01", 10),
    (2, "2017-01-02", 109),
    (3, "2017-01-03", 150),
    (4, "2017-01-04", 99),
    (5, "2017-01-05", 145),
    (6, "2017-01-06", 1455),
    (7, "2017-01-07", 199),
    (8, "2017-01-08", 188)
]
schema =  ["id" , "date" , "people"]
# Create a PySpark DataFrame
stadium = spark.createDataFrame(data, schema)
stadium.createOrReplaceTempView('stadium')
display(stadium)
# --write a query to display the records which have 3 or more consecutive rows and the amount of people more than 100(inclusive).

# COMMAND ----------

data = [
    (1, 1),
    (2, 0),
    (3, 1),
    (4, 1),
    (5, 1)
]

schema = ["seat_id" , "free"]
cinema = spark.createDataFrame(data, schema)
cinema.createOrReplaceTempView('df')
display(cinema)

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC select *,(seat_id - row_number() over(order by seat_id)) as rnb from df where free=1)
# MAGIC
# MAGIC select * from cte where rnb in (
# MAGIC   select rnb from cte group by rnb having count(*)>1
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (
# MAGIC select *, lag(free , 1, 0) over (order by seat_id) * free as l1, lead(free, 1, 0) over (order by seat_id) * free as l2 from df ) where l1=1 or l2=1

# COMMAND ----------

# MAGIC %sql
# MAGIC select seat_id from (
# MAGIC select * , lag(free,1, 0) over( order by seat_id) * free as l1 , lead(free,1, 0) over( order by seat_id) * free as l2 from df)
# MAGIC where l1=1 or l2=1

# COMMAND ----------


