# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql.types import *

# COMMAND ----------

import time

time.sleep(50000)


# COMMAND ----------

candidates_schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("positions", StringType(), True),
    StructField("salary", IntegerType(), True)
])

# Define candidates data
candidates_data = [
    (1, 'Junior', 10000),
    (2, 'Junior', 15000),
    (3, 'Junior', 40000),
    (4, 'Senior', 16000),
    (5, 'Senior', 20000),
    (6, 'Senior', 50000)
]

# Create PySpark DataFrame
candidates_df = spark.createDataFrame(candidates_data, schema=candidates_schema)

# Create temporary SQL view
candidates_df.createOrReplaceTempView("candidates")

# Display the DataFrame
candidates_df.show()
# In this problem we have to write a SQL to build a team with a combination of seniors and juniors within a given salary budget. 

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(select * , sum(salary) over (partition by positions order by salary rows between unbounded preceding and current row) rn_sal from candidates order by positions desc),
# MAGIC
# MAGIC cte2 as(select * from cte where positions='Senior' and rn_sal<=60000)
# MAGIC
# MAGIC -- select * from cte2
# MAGIC -- union
# MAGIC select * from cte where positions='Junior' and rn_sal <= 60000 - (select sum(salary) from cte2)

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(select *, r - row_number() over (partition by n order by r) as rn from(
# MAGIC select left(seat, 1) as n,SUBSTRING(seat, 2, 2) r , occupancy from movie) where occupancy=0 )
# MAGIC select concat(n,r) as id from cte where rn in  (
# MAGIC select rn from cte group by rn having count(*)>3)

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (select *, r - row_number() over (partition by n order by r) as rn from(
# MAGIC select left(seat, 1) as n, SUBSTRING(seat, 2, 2) r , occupancy from movie) where occupancy=0 )
# MAGIC -- select n,r,rn from cte group by n, r, rn having count(*)>3
# MAGIC select * from cte

# COMMAND ----------


