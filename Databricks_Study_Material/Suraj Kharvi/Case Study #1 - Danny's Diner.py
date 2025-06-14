# Databricks notebook source
# %sql
# CREATE SCHEMA if not exists dannys_diner;
# SET search_path = dannys_diner;

# -- CREATE TABLE dannys_diner.sales (
# --   customer_id VARCHAR(1),
# --   order_date string,
# --   product_id string
# -- );

# -- INSERT INTO dannys_diner.sales
# --   (customer_id, order_date, product_id)
# -- VALUES
# --   ('A', '2021-01-01', '1'),
# --   ('A', '2021-01-01', '2'),
# --   ('A', '2021-01-07', '2'),
# --   ('A', '2021-01-10', '3'),
# --   ('A', '2021-01-11', '3'),
# --   ('A', '2021-01-11', '3'),
# --   ('B', '2021-01-01', '2'),
# --   ('B', '2021-01-02', '2'),
# --   ('B', '2021-01-04', '1'),
# --   ('B', '2021-01-11', '1'),
# --   ('B', '2021-01-16', '3'),
# --   ('B', '2021-02-01', '3'),
# --   ('C', '2021-01-01', '3'),
# --   ('C', '2021-01-01', '3'),
# --   ('C', '2021-01-07', '3');
  
# CREATE TABLE dannys_diner.menu (
#   product_id INTEGER,
#   product_name VARCHAR(5),
#   price INTEGER
# );

# INSERT INTO dannys_diner.menu
#   (product_id, product_name, price)
# VALUES
#   ('1', 'sushi', '10'),
#   ('2', 'curry', '15'),
#   ('3', 'ramen', '12');
  

# CREATE TABLE dannys_diner.members (
#   customer_id VARCHAR(1),
#   join_date DATE
# );

# INSERT INTO dannys_diner.members
#   (customer_id, join_date)
# VALUES
#   ('A', '2021-01-07'),
#   ('B', '2021-01-09');

# COMMAND ----------

sales=spark.read.format("delta").load("/user/hive/warehouse/dannys_diner.db/sales")
menu=spark.read.format("delta").load("/user/hive/warehouse/dannys_diner.db/menu")
members=spark.read.format("delta").load("/user/hive/warehouse/dannys_diner.db/members")

# COMMAND ----------



# COMMAND ----------

sales.show()
menu.show()
members.show()

# COMMAND ----------

#What is the total amount each customer spent at the restaurant?
sales_menu=sales.join(menu,sales.product_id==menu.product_id).drop(sales.product_id)
sales_menu.show()

# COMMAND ----------

from pyspark.sql.functions import col

result=sales_menu.groupBy(col("customer_id")).sum().drop(col("sum(product_id)"))
result.show()

# COMMAND ----------

#How many days has each customer visited the restaurant?
result=sales.groupBy(col("customer_id")).count()
result.show()

# COMMAND ----------

#What was the first item from the menu purchased by each customer?
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, dense_rank
w=Window.partitionBy(col("customer_id")).orderBy(col("order_date"))
result=sales_menu.withColumn("r", row_number().over(w)).filter(col("r")==1).drop(col("r"))
result.show()

# COMMAND ----------

#What is the most purchased item on the menu and how many times was it purchased by all customers?

result=sales_menu.groupBy(col("product_name")).count().sort(col("count").desc())
result.show(1)

# COMMAND ----------

#Which item was the most popular for each customer?
result1=sales_menu.groupBy(col("customer_id"),col("product_name")).count()
w=Window.partitionBy(col("customer_id")).orderBy(col("count").desc())
result2=result1.withColumn("most_popular",dense_rank().over(w)).filter(col("most_popular")==1).drop(col("most_popular"))
result2.show()

# COMMAND ----------

#Which item was purchased first by the customer after they became a member?
sales_menu_members=sales_menu.join(members,df.customer_id==members.customer_id).drop(members.customer_id).withColumn("valid_date",F.date_add(col("join_date"), 6))
sales_menu_members.show()


# COMMAND ----------

w=Window.partitionBy(col("customer_id")).orderBy(col("order_date"))
result=sales_menu_members.select(col("customer_id"),col("join_date"),col("order_date"),col("product_name")).filter(col("order_date")>=col("join_date")).withColumn("r",dense_rank().over(w)).filter(col("r")==1).drop(col("r"))
result.show()

# COMMAND ----------

#Which item was purchased just before the customer became a member?
w=Window.partitionBy(col("customer_id")).orderBy(col("order_date").desc())
result=sales_menu_members.select(col("customer_id"),col("join_date"),col("order_date"),col("product_name")).filter(col("order_date")<col("join_date")).withColumn("r",dense_rank().over(w)).filter(col("r")==1).drop(col("r"))
result.show()

# COMMAND ----------

# What is the total items and amount spent for each member before they became a member?
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import lit
result=sales_menu_members.filter(col("order_date")<col("join_date")).withColumn("count",lit(1)).groupBy("customer_id").sum().drop("sum(product_id)")
result.show()


# COMMAND ----------

# If each $1 spent equates to 10 points and sushi has a 2x points multiplier — how many points would each customer have?
from pyspark.sql.functions import when
result=sales_menu.withColumn("points", when(col("product_id")==1,col("price")*2*10).otherwise(col("price")*10))
result.groupBy("customer_id").sum("points").show()

# COMMAND ----------

# In the first week after a customer joins the program (including their join date) they earn 2x points on all items, not just sushi — how many points do customer A and B have at the end of January?
# import pyspark.sql.functions as F
# temp_df1=sales_menu_members.withColumn("valid_date",F.date_add(col("join_date"), 6))
# # result=temp_df1.withColumn("points", when(col("order_date").between("joined_date","valid_date"),col("price")*2*10).otherwise(col("price")*10))
# result=temp_df1.withColumn("points", when((col("order_date")>=col("join_date")) & (col("order_date") <=col("valid_date"))),col("price")*2*10).otherwise(col("price")*10)

# result.groupBy("customer_id").sum("points").show()


# COMMAND ----------

import pyspark.sql.functions as F
temp_df1=sales_menu_members.withColumn("valid_date",F.date_add(col("join_date"), 6))
result=temp_df1.withColumn("points", when(col("product_name")=="sushi",col("price")*2*10).when(col("order_date").between("joined_date","valid_date"),col("price")*2*10).otherwise(col("price")*10))
result.groupBy("customer_id").sum("points").show()

