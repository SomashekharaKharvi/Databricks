# Databricks notebook source
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql.functions import *

# COMMAND ----------

# Define the schema
schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("new_price", IntegerType(), True),
    StructField("change_date", DateType(), True)
])

# Data to insert
data = [
    (1, 10, datetime.strptime('2024-08-11', '%Y-%m-%d').date()),
    (1, 20, datetime.strptime('2024-08-12', '%Y-%m-%d').date()),
    (2, 20, datetime.strptime('2024-08-14', '%Y-%m-%d').date()),
    (1, 40, datetime.strptime('2024-08-16', '%Y-%m-%d').date()),
    (2, 50, datetime.strptime('2024-08-15', '%Y-%m-%d').date()),
    (3, 90, datetime.strptime('2024-08-18', '%Y-%m-%d').date())
]

# Create a DataFrame with the data
product_price_changes_df = spark.createDataFrame(data, schema)
product_price_changes_df.createOrReplaceTempView("df")

# Show the DataFrame
product_price_changes_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM 
# MAGIC (SELECT product_id, new_price AS price
# MAGIC  FROM df
# MAGIC  WHERE (product_id, change_date) IN (
# MAGIC                                      SELECT product_id, MAX(change_date)
# MAGIC                                      FROM df
# MAGIC                                      WHERE change_date <= '2024-08-16'
# MAGIC                                      GROUP BY product_id)
# MAGIC  UNION
# MAGIC  SELECT DISTINCT product_id, 10 AS price
# MAGIC  FROM df
# MAGIC  WHERE product_id NOT IN (SELECT product_id FROM df WHERE change_date <= '2024-08-16')
# MAGIC ) tmp
# MAGIC ORDER BY price DESC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Define the schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("action", StringType(), True),
    StructField("question_id", IntegerType(), True),
    StructField("answer_id", IntegerType(), True),
    StructField("q_num", IntegerType(), True),
    StructField("timestamp", IntegerType(), True)
])
data = [
    Row(id=5, action='show', question_id=285, answer_id=None, q_num=1, timestamp=123),
    Row(id=5, action='answer', question_id=285, answer_id=124124, q_num=1, timestamp=124),
    Row(id=5, action='show', question_id=369, answer_id=None, q_num=2, timestamp=125),
    Row(id=5, action='skip', question_id=369, answer_id=None, q_num=2, timestamp=126)
]

# Create a DataFrame with the defined schema
survey_log_df = spark.createDataFrame(data, schema)
survey_log_df.createOrReplaceTempView("survey_log")
# Show the DataFrame
survey_log_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select question_id,sum(if(answer_id is not null, 1, 0))/count(*) from survey_log group by 1

# COMMAND ----------

from pyspark.sql import Row

# Create a list of Rows for the data
data = [
    Row(name='A', id=1),
    Row(name='B', id=2),
    Row(name='C', id=3),
    Row(name='D', id=4),
    Row(name='E', id=5)
]
schema = StructType([
    StructField("name", StringType(), True),
    StructField("id", IntegerType(), True)
])
# Create a DataFrame
swap_id_df = spark.createDataFrame(data)
swap_id_df.createOrReplaceTempView("swap_id")

# Show the DataFrame
swap_id_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select name,case
# MAGIC when id%2!=0  and id not in  (select max(id) from swap_id) then id+1
# MAGIC when id%2=0  and id not in  (select max(id) from swap_id) then id-1 
# MAGIC else id end as id 
# MAGIC from swap_id

# COMMAND ----------

schema = StructType([
    StructField("topping_name", StringType(), True),
    StructField("cost", FloatType(), True)
])

# Create a list of toppings data
data = [
    ('Pepperoni', 0.50),
    ('Sausage', 0.70),
    ('Chicken', 0.55),
    ('Extra Cheese', 0.40)
]

# Create the DataFrame
toppings_df = spark.createDataFrame(data, schema)

# Display the DataFrame
toppings_df.show()

# Create a temporary view
toppings_df.createOrReplaceTempView("df")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT concat_ws(',',df1.topping_name, df2.topping_name, df3.topping_name) as pizza, (df1.cost + df2.cost + df3.cost) as cost
# MAGIC FROM df df1
# MAGIC JOIN df df2 ON df1.topping_name < df2.topping_name
# MAGIC JOIN df df3 ON df2.topping_name < df3.topping_name

# COMMAND ----------

# Define the schema for the DataFrame
schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("employee_name", StringType(), True),
    StructField("manager_id", IntegerType(), True),
    StructField("salary", IntegerType(), True)
])

# Create a list of employee data
data = [
    (1, 'Alice', None, 150000),
    (2, 'Bob', 1, 120000),
    (3, 'Charlie', 1, 110000),
    (4, 'David', 2, 105000),
    (5, 'Eve', 2, 100000),
    (6, 'Frank', 3, 95000),
    (7, 'Grace', 3, 98000),
    (8, 'Helen', 5, 90000)
]

# Create the DataFrame
employees_df = spark.createDataFrame(data, schema)

# Display the DataFrame
employees_df.show()

# Create a temporary view
employees_df.createOrReplaceTempView("df")

# COMMAND ----------

#  It needs a recursive query

# COMMAND ----------

schema = StructType([
    StructField("name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("email", StringType(), True),
    StructField("floor", IntegerType(), True),
    StructField("resources", StringType(), True)
])

# Create Data
data = [
    ("A", "Bangalore", "A@gmail.com", 1, "CPU"),
    ("A", "Bangalore", "A1@gmail.com", 1, "CPU"),
    ("A", "Bangalore", "A2@gmail.com", 2, "DESKTOP"),
    ("B", "Bangalore", "B@gmail.com", 2, "DESKTOP"),
    ("B", "Bangalore", "B1@gmail.com", 2, "DESKTOP"),
    ("B", "Bangalore", "B2@gmail.com", 1, "MONITOR")
]

# Create DataFramea
df = spark.createDataFrame(data, schema)

# Register DataFrame as Temporary View
df.createOrReplaceTempView("df")

# To verify, show the data
df.show()
# https://www.youtube.com/watch?v=P6kNMyqKD0A&list=PLBTZqjSKn0IeKBQDjLmzisazhqQy4iGkb&index=3

# COMMAND ----------

# MAGIC %sql
# MAGIC with fv as (select name, floor from (
# MAGIC select name, floor, count(*) , rank() over(partition by name order by count(*) desc) as  rn from df group by 1, 2) where rn=1),
# MAGIC
# MAGIC rs as (select name, count(*) as tv, collect_set(resources) as rsc from df group by 1)
# MAGIC
# MAGIC select fv.name ,rs.tv , fv.floor, rs.rsc  from fv join rs on fv.name= rs.name 
# MAGIC -- STRING_AGG(resources, ', ')

# COMMAND ----------

# DBTITLE 1,Pareto Principle
players_data = [
    Row(player_id=15, group_id=1),
    Row(player_id=25, group_id=1),
    Row(player_id=30, group_id=1),
    Row(player_id=45, group_id=1),
    Row(player_id=10, group_id=2),
    Row(player_id=35, group_id=2),
    Row(player_id=50, group_id=2),
    Row(player_id=20, group_id=3),
    Row(player_id=40, group_id=3)
]

players_df = spark.createDataFrame(players_data)

# Create a temporary view for players
players_df.createOrReplaceTempView("players")

# Create DataFrame for matches
matches_data = [
    Row(match_id=1, first_player=15, second_player=45, first_score=3, second_score=0),
    Row(match_id=2, first_player=30, second_player=25, first_score=1, second_score=2),
    Row(match_id=3, first_player=30, second_player=15, first_score=2, second_score=0),
    Row(match_id=4, first_player=40, second_player=20, first_score=5, second_score=2),
    Row(match_id=5, first_player=35, second_player=50, first_score=1, second_score=1)
]

matches_df = spark.createDataFrame(matches_data)

# Create a temporary view for matches
matches_df.createOrReplaceTempView("matches")

# Now you can run SQL queries on the temporary views
# Example query to select all players
result_players = spark.sql("SELECT * FROM players")
result_players.show()

# Example query to select all matches
result_matches = spark.sql("SELECT * FROM matches")
result_matches.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Player who score maximum in each group
# MAGIC with cte as(select player, sum(score) as total_score from (
# MAGIC select first_player as player, sum(first_score) as score from  matches group by 1
# MAGIC union
# MAGIC select second_player as player, sum(second_score) as score from matches group by 1) 
# MAGIC group by 1)
# MAGIC
# MAGIC select * from (
# MAGIC select player, total_score, group_id, row_number() over (partition by group_id order by total_score desc ) as rn from players p join cte on p.player_id=cte.player) where rn=1

# COMMAND ----------

users_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("join_date", DateType(), True),
    StructField("favorite_brand", StringType(), True)
])

# Create DataFrame for users
users_data = [
    Row(user_id=1, join_date=datetime.strptime('2019-01-01', '%Y-%m-%d').date(), favorite_brand='Lenovo'),
    Row(user_id=2, join_date=datetime.strptime('2019-02-09', '%Y-%m-%d').date(), favorite_brand='Samsung'),
    Row(user_id=3, join_date=datetime.strptime('2019-01-19', '%Y-%m-%d').date(), favorite_brand='LG'),
    Row(user_id=4, join_date=datetime.strptime('2019-05-21', '%Y-%m-%d').date(), favorite_brand='HP')
]

users_df = spark.createDataFrame(users_data, schema=users_schema)

# Create a temporary view for users
users_df.createOrReplaceTempView("users")

# Define schema for items
items_schema = StructType([
    StructField("item_id", IntegerType(), True),
    StructField("item_brand", StringType(), True)
])

# Create DataFrame for items
items_data = [
    Row(item_id=1, item_brand='Samsung'),
    Row(item_id=2, item_brand='Lenovo'),
    Row(item_id=3, item_brand='LG'),
    Row(item_id=4, item_brand='HP')
]

items_df = spark.createDataFrame(items_data, schema=items_schema)

# Create a temporary view for items
items_df.createOrReplaceTempView("items")

# Define schema for orders
orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("order_date", DateType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("buyer_id", IntegerType(), True),
    StructField("seller_id", IntegerType(), True)
])

# Create DataFrame for orders
orders_data = [
    Row(order_id=1, order_date=datetime.strptime('2019-08-01', '%Y-%m-%d').date(), item_id=4, buyer_id=1, seller_id=2),
    Row(order_id=2, order_date=datetime.strptime('2019-08-02', '%Y-%m-%d').date(), item_id=2, buyer_id=1, seller_id=3),
    Row(order_id=3, order_date=datetime.strptime('2019-08-03', '%Y-%m-%d').date(), item_id=3, buyer_id=2, seller_id=3),
    Row(order_id=4, order_date=datetime.strptime('2019-08-04', '%Y-%m-%d').date(), item_id=1, buyer_id=4, seller_id=2),
    Row(order_id=5, order_date=datetime.strptime('2019-08-04', '%Y-%m-%d').date(), item_id=1, buyer_id=3, seller_id=4),
    Row(order_id=6, order_date=datetime.strptime('2019-08-05', '%Y-%m-%d').date(), item_id=2, buyer_id=2, seller_id=4)
]

orders_df = spark.createDataFrame(orders_data, schema=orders_schema)

# Create a temporary view for orders
orders_df.createOrReplaceTempView("orders")

# Now you can run SQL queries on the temporary views
# Example query to select all users
result_users = spark.sql("SELECT * FROM users")
result_users.show()

# Example query to select all items
result_items = spark.sql("SELECT * FROM items")
result_items.show()

# Example query to select all orders
result_orders = spark.sql("SELECT * FROM orders")
result_orders.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- second order fav brand 
# MAGIC with cte as (select seller_id, if(item_brand=favorite_brand, 'Yes', 'No') as stat from (
# MAGIC select o.*, item_brand, favorite_brand, row_number() over (partition by seller_id order by order_date) as rn from orders o join items i on o.item_id=i.item_id 
# MAGIC right join users u on o.seller_id = u.user_id)
# MAGIC where rn=2)
# MAGIC
# MAGIC select user_id as seller_id , if(seller_id is null , 'No', stat) as stat from users u left join cte on u.user_id=cte.seller_id

# COMMAND ----------

schema = StructType([
    StructField("date_value", DateType(), True),
    StructField("state", StringType(), True)
])

# Create a list of data to insert
data = [
    (datetime.strptime('2019-01-01', '%Y-%m-%d').date(), 'success'),
    (datetime.strptime('2019-01-02', '%Y-%m-%d').date(), 'success'),
    (datetime.strptime('2019-01-03', '%Y-%m-%d').date(), 'success'),
    (datetime.strptime('2019-01-04', '%Y-%m-%d').date(), 'fail'),
    (datetime.strptime('2019-01-05', '%Y-%m-%d').date(), 'fail'),
    (datetime.strptime('2019-01-06', '%Y-%m-%d').date(), 'success')
]

# Create a DataFrame
df = spark.createDataFrame(data, schema)

# Create a temporary view
df.createOrReplaceTempView("tasks")

# Now you can run SQL queries against the temporary view
result = spark.sql("SELECT * FROM tasks")
result.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select state,rn, min(date_value) as start_date, max(date_value) as end_date from (
# MAGIC select *, date_sub(date_value, row_number() over(partition by state order by date_value) ) as rn from tasks) 
# MAGIC group by 1, 2 order by start_date

# COMMAND ----------

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("spend_date", DateType(), True),
    StructField("platform", StringType(), True),
    StructField("amount", IntegerType(), True)
])

# Create a list of data to insert
data = [
    (1, datetime.strptime('2019-07-01', '%Y-%m-%d').date(), 'mobile', 100),
    (1, datetime.strptime('2019-07-01', '%Y-%m-%d').date(), 'desktop', 100),
    (2, datetime.strptime('2019-07-01', '%Y-%m-%d').date(), 'mobile', 100),
    (2, datetime.strptime('2019-07-02', '%Y-%m-%d').date(), 'mobile', 100),
    (3, datetime.strptime('2019-07-01', '%Y-%m-%d').date(), 'desktop', 100),
    (3, datetime.strptime('2019-07-02', '%Y-%m-%d').date(), 'desktop', 100)
]

# Create a DataFrame
df = spark.createDataFrame(data, schema)

# Create a temporary view
df.createOrReplaceTempView("spending")

# Now you can run SQL queries against the temporary view
result = spark.sql("SELECT * FROM spending")
result.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(
# MAGIC select spend_date, user_id , max(platform) as platform, sum(amount) as amount
# MAGIC from spending group by spend_date , user_id having count(distinct platform)=1
# MAGIC union
# MAGIC select spend_date, user_id , 'both' as platform, sum(amount) as amount
# MAGIC from spending group by spend_date , user_id having count(distinct platform)=2
# MAGIC union
# MAGIC select distinct spend_date, Null , 'both' as platform , 0 as amount from spending)
# MAGIC
# MAGIC select spend_date,platform, sum(amount) as amount, count(distinct user_id) from cte
# MAGIC group by 1 , 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select spend_date, count(distinct(user_id)) as total_users, sum(if(platform='mobile', amount, 0)) as mobile_only ,sum(if(platform='desktop', amount, 0)) desktop_only, sum(amount) total_amount  from spending
# MAGIC group by 1

# COMMAND ----------

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True)
])

# Create a list of data to insert into orders
orders_data = [
    (1, 1, 1),
    (1, 1, 2),
    (1, 1, 3),
    (2, 2, 1),
    (2, 2, 2),
    (2, 2, 4),
    (3, 1, 5)
]

# Create a DataFrame for orders
orders_df = spark.createDataFrame(orders_data, orders_schema)

# Create a temporary view for orders
orders_df.createOrReplaceTempView("orders")

# Define the schema for the products DataFrame
products_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

# Create a list of data to insert into products
products_data = [
    (1, 'A'),
    (2, 'B'),
    (3, 'C'),
    (4, 'D'),
    (5, 'E')
]

# Create a DataFrame for products
products_df = spark.createDataFrame(products_data, products_schema)

# Create a temporary view for products
products_df.createOrReplaceTempView("products")
orders_df.display()
products_df.display()

# products which are most frequently bought together using simple SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (select o.*, p.name from orders o join products p on o.product_id=p.id)
# MAGIC
# MAGIC select concat(a.name,' ' ,b.name) , count(*) as cnt from cte a join cte b
# MAGIC on a.order_id=b.order_id
# MAGIC where a.product_id < b.product_id
# MAGIC group by 1

# COMMAND ----------

# Given the following two tables, return the fraction of users, rounded to two decimal places,
# who accessed Amazon music and upgraded to prime membership within the first 30 days of signing up. 
users_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("join_date", DateType(), True)
])

# Create a list of data to insert into users
users_data = [
    (1, 'Jon', datetime.strptime('2020-02-14', '%Y-%m-%d').date()),
    (2, 'Jane', datetime.strptime('2020-02-14', '%Y-%m-%d').date()),
    (3, 'Jill', datetime.strptime('2020-02-15', '%Y-%m-%d').date()),
    (4, 'Josh', datetime.strptime('2020-02-15', '%Y-%m-%d').date()),
    (5, 'Jean', datetime.strptime('2020-02-16', '%Y-%m-%d').date()),
    (6, 'Justin', datetime.strptime('2020-02-17', '%Y-%m-%d').date()),
    (7, 'Jeremy', datetime.strptime('2020-02-18', '%Y-%m-%d').date())
]

# Create a DataFrame for users
users_df = spark.createDataFrame(users_data, users_schema)

# Create a temporary view for users
users_df.createOrReplaceTempView("users")

# Define the schema for the events DataFrame
events_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("access_date", DateType(), True)
])

# Create a list of data to insert into events
events_data = [
    (1, 'Pay', datetime.strptime('2020-03-01', '%Y-%m-%d').date()),
    (2, 'Music', datetime.strptime('2020-03-02', '%Y-%m-%d').date()),
    (2, 'P', datetime.strptime('2020-03-12', '%Y-%m-%d').date()),
    (3, 'Music', datetime.strptime('2020-03-15', '%Y-%m-%d').date()),
    (4, 'Music', datetime.strptime('2020-03-15', '%Y-%m-%d').date()),
    (1, 'P', datetime.strptime('2020-03-16', '%Y-%m-%d').date()),
    (3, 'P', datetime.strptime('2020-03-22', '%Y-%m-%d').date())
]

# Create a DataFrame for events
events_df = spark.createDataFrame(events_data, events_schema)

# Create a temporary view for events
events_df.createOrReplaceTempView("events")
users_df.display()
events_df.display()

# Given the following two tables, return the fraction of users, rounded to two decimal places,
# who accessed Amazon music and upgraded to prime membership within the first 30 days of signing 

# COMMAND ----------

# MAGIC %sql
# MAGIC select round((count(case when access_date between join_date and date_add(join_date, 30) then u.user_id else null end)/ count(distinct u.user_id)),2) cnt from users u left join events e on u.user_id = e.user_id
# MAGIC and e.type='P'
# MAGIC where  u.user_id in (select distinct user_id from events where type='Music')

# COMMAND ----------

# DBTITLE 1,Customer retention refers to the ability of a company or product to retain its customers over some specified period. High customer retention means customers of the product or business tend to return to, continue to buy or in some other way not defect to another product or business, or to non-use entirely.  Company programs to retain customers: Zomato Pro , Cashbacks, Reward Programs etc. Once thes
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("cust_id", IntegerType(), True),
    StructField("order_date", DateType(), True),
    StructField("amount", IntegerType(), True)
])

# Create a list of transaction data with date conversion
data = [
    (1, 1, datetime.strptime('2020-01-15', '%Y-%m-%d').date(), 150),
    (2, 1, datetime.strptime('2020-02-10', '%Y-%m-%d').date(), 150),
    (3, 2, datetime.strptime('2020-01-16', '%Y-%m-%d').date(), 150),
    (4, 2, datetime.strptime('2020-02-25', '%Y-%m-%d').date(), 150),
    (5, 3, datetime.strptime('2020-01-10', '%Y-%m-%d').date(), 150),
    (6, 3, datetime.strptime('2020-02-20', '%Y-%m-%d').date(), 150),
    (7, 4, datetime.strptime('2020-01-20', '%Y-%m-%d').date(), 150),
    (8, 5, datetime.strptime('2020-02-20', '%Y-%m-%d').date(), 150)
]

# Create a DataFrame using the schema and data
transactions_df = spark.createDataFrame(data, schema)

# Create a temporary view from the DataFrame
transactions_df.createOrReplaceTempView("transactions")

# Display the DataFrame using Spark SQL
result_df = spark.sql("SELECT * FROM transactions")
result_df.display()
# Customer retention refers to the ability of a company or product to retain its customers over some specified period. High customer retention means customers of the product or business tend to return to, continue to buy or in some other way not defect to another product or business, or to non-use entirely. 
# Company programs to retain customers: Zomato Pro , Cashbacks, Reward Programs etc.
# Once these programs in place we need to build metrics to check if programs are working or not. That is where we will write SQL to drive customer retention count.  

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (select * , month(order_date) as od from transactions)
# MAGIC select od, sum(cnt) from (
# MAGIC select od, case when od = (select min(od) from cte) then 0
# MAGIC when cust_id not in (select distinct cust_id from cte where od < p.od) then 0
# MAGIC else 1 end as cnt from cte p ) group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select month(t1.order_date) as month, count(t2.cust_id)  from transactions t1 left join transactions t2
# MAGIC on t1.cust_id = t2.cust_id
# MAGIC and  round((months_between( t1.order_date,  t2.order_date))) =1
# MAGIC group by month(t1.order_date)

# COMMAND ----------

schema = StructType([
    StructField("username", StringType(), True),
    StructField("activity", StringType(), True),
    StructField("startDate", DateType(), True),
    StructField("endDate", DateType(), True)
])

# Convert string dates to Python datetime.date objects
data = [
    ("Alice", "Travel", datetime.strptime("2020-02-12", "%Y-%m-%d").date(), datetime.strptime("2020-02-20", "%Y-%m-%d").date()),
    ("Alice", "Dancing", datetime.strptime("2020-02-21", "%Y-%m-%d").date(), datetime.strptime("2020-02-23", "%Y-%m-%d").date()),
    ("Alice", "Travel", datetime.strptime("2020-02-24", "%Y-%m-%d").date(), datetime.strptime("2020-02-28", "%Y-%m-%d").date()),
    ("Bob", "Travel", datetime.strptime("2020-02-11", "%Y-%m-%d").date(), datetime.strptime("2020-02-18", "%Y-%m-%d").date())
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Register as a Temporary View
df.createOrReplaceTempView("UserActivity")

# Display the table using Spark SQL
spark.sql("SELECT * FROM UserActivity").show()
# Where we need to find second most recent activity and if user has only 1 activoty then return that as it is. 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (
# MAGIC select *, count(*) over(partition by username) as cnt,  row_number() over ( partition by username order by startDate) as rn from UserActivity)
# MAGIC where cnt=1 or rn=2

# COMMAND ----------

billings_schema = StructType([
    StructField("emp_name", StringType(), True),
    StructField("bill_date", DateType(), True),
    StructField("bill_rate", IntegerType(), True)
])

# Data for Billings Table (convert date strings to datetime.date)
billings_data = [
    ("Sachin", datetime.strptime("01-JAN-1990", "%d-%b-%Y").date(), 25),
    ("Sehwag", datetime.strptime("01-JAN-1989", "%d-%b-%Y").date(), 15),
    ("Dhoni", datetime.strptime("01-JAN-1989", "%d-%b-%Y").date(), 20),
    ("Sachin", datetime.strptime("05-FEB-1991", "%d-%b-%Y").date(), 30)
]

# Create Billings DataFrame
billings_df = spark.createDataFrame(billings_data, schema=billings_schema)

# Register as Temp View
billings_df.createOrReplaceTempView("Billings")

# Define Schema for HoursWorked Table
hours_schema = StructType([
    StructField("emp_name", StringType(), True),
    StructField("work_date", DateType(), True),
    StructField("bill_hrs", IntegerType(), True)
])

# Data for HoursWorked Table (convert date strings to datetime.date)
hours_data = [
    ("Sachin", datetime.strptime("01-JUL-1990", "%d-%b-%Y").date(), 3),
    ("Sachin", datetime.strptime("01-AUG-1990", "%d-%b-%Y").date(), 5),
    ("Sehwag", datetime.strptime("01-JUL-1990", "%d-%b-%Y").date(), 2),
    ("Sachin", datetime.strptime("01-JUL-1991", "%d-%b-%Y").date(), 4)
]

# Create HoursWorked DataFrame
hours_df = spark.createDataFrame(hours_data, schema=hours_schema)

# Register as Temp View
hours_df.createOrReplaceTempView("HoursWorked")

# Display Data from Both Tables
print("Billings Table:")
spark.sql("SELECT * FROM Billings").show()

print("HoursWorked Table:")
spark.sql("SELECT * FROM HoursWorked").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.emp_name, sum(bill_rate * bill_hrs) as bill from (
# MAGIC select *, date_sub(lead(bill_date, 1, '9999-01-01') over(partition by emp_name order by bill_date), 1) as next_date from Billings) a 
# MAGIC left join HoursWorked h on a.emp_name=h.emp_name and h.work_date between a.bill_date and a.next_date
# MAGIC group by a.emp_name

# COMMAND ----------

activity_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event_name", StringType(), True),
    StructField("event_date", DateType(), True),
    StructField("country", StringType(), True)
])

# Data for Activity Table (convert date strings to datetime.date)
activity_data = [
    ("1", "app-installed", datetime.strptime("2022-01-01", "%Y-%m-%d").date(), "India"),
    ("1", "app-purchase", datetime.strptime("2022-01-02", "%Y-%m-%d").date(), "India"),
    ("2", "app-installed", datetime.strptime("2022-01-01", "%Y-%m-%d").date(), "USA"),
    ("3", "app-installed", datetime.strptime("2022-01-01", "%Y-%m-%d").date(), "USA"),
    ("3", "app-purchase", datetime.strptime("2022-01-03", "%Y-%m-%d").date(), "USA"),
    ("4", "app-installed", datetime.strptime("2022-01-03", "%Y-%m-%d").date(), "India"),
    ("4", "app-purchase", datetime.strptime("2022-01-03", "%Y-%m-%d").date(), "India"),
    ("5", "app-installed", datetime.strptime("2022-01-03", "%Y-%m-%d").date(), "SL"),
    ("5", "app-purchase", datetime.strptime("2022-01-03", "%Y-%m-%d").date(), "SL"),
    ("6", "app-installed", datetime.strptime("2022-01-04", "%Y-%m-%d").date(), "Pakistan"),
    ("6", "app-purchase", datetime.strptime("2022-01-04", "%Y-%m-%d").date(), "Pakistan")
]

# Create DataFrame
activity_df = spark.createDataFrame(activity_data, schema=activity_schema)

# Register as a Temporary View
activity_df.createOrReplaceTempView("Activity")

# Display the Table Using Spark SQL
print("Activity Table:")
spark.sql("SELECT * FROM Activity").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select country1,CAST(COUNT(DISTINCT user_id) AS FLOAT) /(select count(*) from Activity where event_name='app-purchase' ) from (
# MAGIC select * , case when country in ('India', 'USA') then country else 'Others' end as country1 from Activity where event_name='app-purchase') a
# MAGIC group by country1

# COMMAND ----------

# DBTITLE 1,Consecutive Empty Seats
bms_schema = StructType([
    StructField("seat_no", IntegerType(), True),
    StructField("is_empty", StringType(), True)
])

# Data for BMS Table
bms_data = [
    (1, 'N'), (2, 'Y'), (3, 'N'), (4, 'Y'),
    (5, 'Y'), (6, 'Y'), (7, 'N'), (8, 'Y'),
    (9, 'Y'), (10, 'Y'), (11, 'Y'), (12, 'N'),
    (13, 'Y'), (14, 'Y')
]

# Create DataFrame
bms_df = spark.createDataFrame(bms_data, schema=bms_schema)

# Register as a Temporary View
bms_df.createOrReplaceTempView("BMS")

# Display the Table Using Spark SQL
print("BMS Table:")
spark.sql("SELECT * FROM BMS").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (
# MAGIC select * , is_empty * lag(is_empty,1 ,0) over (order by seat_no) as lg , is_empty * lead(is_empty,1 ,0) over (order by seat_no) as ld from (
# MAGIC select seat_no, if(is_empty='Y',1, 0) as is_empty from bms))
# MAGIC where lg=1 or ld=1

# COMMAND ----------

# DBTITLE 1,Missing Qrtr
stores_schema = StructType([
    StructField("Store", StringType(), True),
    StructField("Quarter", StringType(), True),
    StructField("Amount", IntegerType(), True)
])

# Data for STORES Table
stores_data = [
    ("S1", "Q1", 200), ("S1", "Q2", 300), ("S1", "Q4", 400),
    ("S2", "Q1", 500), ("S2", "Q3", 600), ("S2", "Q4", 700),
    ("S3", "Q1", 800), ("S3", "Q2", 750), ("S3", "Q3", 900)
]

# Create DataFrame
stores_df = spark.createDataFrame(stores_data, schema=stores_schema)

# Register as a Temporary View
stores_df.createOrReplaceTempView("STORES")

# Display the Table Using Spark SQL
print("STORES Table:")
spark.sql("SELECT * FROM STORES").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select store , concat( 'Q', cast( (10 - sum(int(right(Quarter, 1)))) as string)) as missing_qrt from STORES group by 1

# COMMAND ----------

exams_schema = StructType([
    StructField("student_id", IntegerType(), True),
    StructField("subject", StringType(), True),
    StructField("marks", IntegerType(), True)
])

# Data for EXAMS Table
exams_data = [
    (1, "Chemistry", 91), (1, "Physics", 91),
    (2, "Chemistry", 80), (2, "Physics", 90),
    (3, "Chemistry", 80),
    (4, "Chemistry", 71), (4, "Physics", 54)
]

# Create DataFrame
exams_df = spark.createDataFrame(exams_data, schema=exams_schema)

# Register as a Temporary View
exams_df.createOrReplaceTempView("EXAMS")

# Display the Table Using Spark SQL
print("EXAMS Table:")
spark.sql("SELECT * FROM EXAMS").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select student_id from EXAMS where subject in ('Chemistry', 'Physics') group by 1 having count(distinct subject)= 2 and count (distinct marks)=1

# COMMAND ----------

covid_schema = StructType([
    StructField("city", StringType(), True),
    StructField("days", DateType(), True),
    StructField("cases", IntegerType(), True)
])

# Data for COVID Table (convert date strings to datetime.date)
covid_data = [
    ("DELHI", datetime.strptime("2022-01-01", "%Y-%m-%d").date(), 100),
    ("DELHI", datetime.strptime("2022-01-02", "%Y-%m-%d").date(), 200),
    ("DELHI", datetime.strptime("2022-01-03", "%Y-%m-%d").date(), 300),

    ("MUMBAI", datetime.strptime("2022-01-01", "%Y-%m-%d").date(), 100),
    ("MUMBAI", datetime.strptime("2022-01-02", "%Y-%m-%d").date(), 100),
    ("MUMBAI", datetime.strptime("2022-01-03", "%Y-%m-%d").date(), 300),

    ("CHENNAI", datetime.strptime("2022-01-01", "%Y-%m-%d").date(), 100),
    ("CHENNAI", datetime.strptime("2022-01-02", "%Y-%m-%d").date(), 200),
    ("CHENNAI", datetime.strptime("2022-01-03", "%Y-%m-%d").date(), 150),

    ("BANGALORE", datetime.strptime("2022-01-01", "%Y-%m-%d").date(), 100),
    ("BANGALORE", datetime.strptime("2022-01-02", "%Y-%m-%d").date(), 300),
    ("BANGALORE", datetime.strptime("2022-01-03", "%Y-%m-%d").date(), 200),
    ("BANGALORE", datetime.strptime("2022-01-04", "%Y-%m-%d").date(), 400)
]

# Create DataFrame
covid_df = spark.createDataFrame(covid_data, schema=covid_schema)

# Register as a Temporary View
covid_df.createOrReplaceTempView("COVID")

# Display the Table Using Spark SQL
print("COVID Table:")
spark.sql("SELECT * FROM COVID").show()
# In this video we will discuss a problem where we need to identify cities where covid cases increasing everyday.ll

# COMMAND ----------

# MAGIC %sql
# MAGIC select city from (
# MAGIC select * , (rank() over(partition by city order by days) - rank() over(partition by city order by cases)) as diff from  covid)
# MAGIC group by 1 having count(distinct diff) =1 and max(diff)=0

# COMMAND ----------

# DBTITLE 1,englih & german both language
company_users_schema = StructType([
    StructField("company_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("language", StringType(), True)
])

# Data for COMPANY_USERS Table
company_users_data = [
    (1, 1, "English"), (1, 1, "German"),
    (1, 2, "English"),
    (1, 3, "German"), (1, 3, "English"),
    (1, 4, "English"),
    (2, 5, "English"), (2, 5, "German"), (2, 5, "Spanish"),
    (2, 6, "German"), (2, 6, "Spanish"),
    (2, 7, "English")
]

# Create DataFrame
company_users_df = spark.createDataFrame(company_users_data, schema=company_users_schema)

# Register as a Temporary View
company_users_df.createOrReplaceTempView("COMPANY_USERS")

# Display the Table Using Spark SQL
print("COMPANY_USERS Table:")
spark.sql("SELECT * FROM COMPANY_USERS").show()
# company Id who have more then 1 employees who speaks both eng and ger 

# COMMAND ----------

# MAGIC %sql
# MAGIC select  company_id from (
# MAGIC select company_id, user_id , count(*) as  from COMPANY_USERS where language in ("English", "German") group by 1 ,2
# MAGIC having count(distinct language)=2)
# MAGIC group by 1 having count(*) >= 2

# COMMAND ----------

products_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("cost", IntegerType(), True)
])

# Data for PRODUCTS Table
products_data = [
    ("P1", 200), ("P2", 300), ("P3", 500), ("P4", 800)
]

# Create PRODUCTS DataFrame
products_df = spark.createDataFrame(products_data, schema=products_schema)

# Register as a Temporary View
products_df.createOrReplaceTempView("PRODUCTS")

# Define Schema for CUSTOMER_BUDGET Table
customer_budget_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("budget", IntegerType(), True)
])

# Data for CUSTOMER_BUDGET Table
customer_budget_data = [
    (100, 400), (200, 800), (300, 1500)
]

# Create CUSTOMER_BUDGET DataFrame
customer_budget_df = spark.createDataFrame(customer_budget_data, schema=customer_budget_schema)

# Register as a Temporary View
customer_budget_df.createOrReplaceTempView("CUSTOMER_BUDGET")

# Display the Tables Using Spark SQL
print("PRODUCTS Table:")
spark.sql("SELECT * FROM PRODUCTS").show()

print("CUSTOMER_BUDGET Table:")
spark.sql("SELECT * FROM CUSTOMER_BUDGET").show()
# Products falls into customer budget

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.customer_id, a.budget, count(distinct b.product_id) , collect_set(b.product_id) from CUSTOMER_BUDGET a left join (
# MAGIC select * , sum(cost) over(order by product_id) as r_cost from PRODUCTS ) b 
# MAGIC on  b.r_cost <=  a.budget
# MAGIC group by 1, 2

# COMMAND ----------

subscriber_schema = StructType([
    StructField("sms_date", DateType(), True),
    StructField("sender", StringType(), True),
    StructField("receiver", StringType(), True),
    StructField("sms_no", IntegerType(), True)
])

# Data for SUBSCRIBER Table (convert date strings to datetime.date)
subscriber_data = [
    (datetime.strptime("2020-04-01", "%Y-%m-%d").date(), "Avinash", "Vibhor", 10),
    (datetime.strptime("2020-04-01", "%Y-%m-%d").date(), "Vibhor", "Avinash", 20),
    (datetime.strptime("2020-04-01", "%Y-%m-%d").date(), "Avinash", "Pawan", 30),
    (datetime.strptime("2020-04-01", "%Y-%m-%d").date(), "Pawan", "Avinash", 20),
    (datetime.strptime("2020-04-01", "%Y-%m-%d").date(), "Vibhor", "Pawan", 5),
    (datetime.strptime("2020-04-01", "%Y-%m-%d").date(), "Pawan", "Vibhor", 8),
    (datetime.strptime("2020-04-01", "%Y-%m-%d").date(), "Vibhor", "Deepak", 50)
]

# Create DataFrame
subscriber_df = spark.createDataFrame(subscriber_data, schema=subscriber_schema)

# Register as a Temporary View
subscriber_df.createOrReplaceTempView("SUBSCRIBER")

# Display the Table Using Spark SQL
print("SUBSCRIBER Table:")
spark.sql("SELECT * FROM SUBSCRIBER").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select greatest(sender, receiver ), least(sender, receiver ) , sum(sms_no) from SUBSCRIBER
# MAGIC group by greatest(sender, receiver ), least(sender, receiver )

# COMMAND ----------

students_schema = StructType([
    StructField("studentid", IntegerType(), True),
    StructField("studentname", StringType(), True),
    StructField("subject", StringType(), True),
    StructField("marks", IntegerType(), True),
    StructField("testid", IntegerType(), True),
    StructField("testdate", DateType(), True)
])

# Data for STUDENTS Table (convert date strings to datetime.date)
students_data = [
    (2, "Max Ruin", "Subject1", 63, 1, datetime.strptime("2022-01-02", "%Y-%m-%d").date()),
    (3, "Arnold", "Subject1", 95, 1, datetime.strptime("2022-01-02", "%Y-%m-%d").date()),
    (4, "Krish Star", "Subject1", 61, 1, datetime.strptime("2022-01-02", "%Y-%m-%d").date()),
    (5, "John Mike", "Subject1", 91, 1, datetime.strptime("2022-01-02", "%Y-%m-%d").date()),
    (4, "Krish Star", "Subject2", 71, 1, datetime.strptime("2022-01-02", "%Y-%m-%d").date()),
    (3, "Arnold", "Subject2", 32, 1, datetime.strptime("2022-01-02", "%Y-%m-%d").date()),
    (5, "John Mike", "Subject2", 61, 2, datetime.strptime("2022-11-02", "%Y-%m-%d").date()),
    (1, "John Deo", "Subject2", 60, 1, datetime.strptime("2022-01-02", "%Y-%m-%d").date()),
    (2, "Max Ruin", "Subject2", 84, 1, datetime.strptime("2022-01-02", "%Y-%m-%d").date()),
    (2, "Max Ruin", "Subject3", 29, 3, datetime.strptime("2022-01-03", "%Y-%m-%d").date()),
    (5, "John Mike", "Subject3", 98, 2, datetime.strptime("2022-11-02", "%Y-%m-%d").date())
]

# Create DataFrame
students_df = spark.createDataFrame(students_data, schema=students_schema)

# Register as a Temporary View
students_df.createOrReplaceTempView("STUDENTS")

# Display the Table Using Spark SQL
print("STUDENTS Table:")
spark.sql("SELECT * FROM STUDENTS").display()

# COMMAND ----------

# DBTITLE 1,above avg marks
# MAGIC %sql
# MAGIC select * from STUDENTS a  join (
# MAGIC select  subject , avg(marks) as avg from STUDENTS group by 1  ) b
# MAGIC on a.subject=b.subject
# MAGIC where a.marks > b.avg

# COMMAND ----------

# DBTITLE 1,percent of More the 90 marsk
# MAGIC %sql
# MAGIC select avg(distinct studentid) from students where marks>=90

# COMMAND ----------

# DBTITLE 1,2nd high and 2nd LOW
# MAGIC %sql
# MAGIC select subject, 
# MAGIC sum(case when high=2 then marks end) as high_val,
# MAGIC sum(case when low=2 then marks end) as low_val from (
# MAGIC select * , row_number() over(partition by subject order by marks desc) as high , row_number() over(partition by subject order by marks) as low from students) 
# MAGIC where high=2 or  low =2
# MAGIC group by 1

# COMMAND ----------

# DBTITLE 1,Marks increased or not
# MAGIC %sql
# MAGIC select *, case when nxt< marks then 'inc'
# MAGIC when nxt> marks then 'dec' else 'NA' end as stat from (
# MAGIC select studentid ,testid, marks, lag(marks) over (partition by studentid order by testdate) nxt from students )

# COMMAND ----------

event_status_schema = StructType([
    StructField("event_time", StringType(), False),
    StructField("status", StringType(), False)
])

# Data for EVENT_STATUS Table
event_status_data = [
    ("10:01", "on"), ("10:02", "on"), ("10:03", "on"),
    ("10:04", "off"), ("10:07", "on"), ("10:08", "on"),
    ("10:09", "off"), ("10:11", "on"), ("10:12", "off")
]

# Create DataFrame
event_status_df = spark.createDataFrame(event_status_data, schema=event_status_schema)

# Register as a Temporary View
event_status_df.createOrReplaceTempView("EVENT_STATUS")

# Display the Table Using Spark SQL
print("EVENT_STATUS Table:")
spark.sql("SELECT * FROM EVENT_STATUS").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select min(event_time), max(event_time), count(*) from (
# MAGIC select * , sum(case when prev_stat='off' then 1 else 0 end) over (order by event_time) as group_by_key from (
# MAGIC select * , lag(status, 1, status) over(order by event_time) as prev_stat from EVENT_STATUS)) group by group_by_key
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(
# MAGIC select *,  flag- row_number() over (order by flag) as rn from (
# MAGIC select *, right(event_time, 2) flag  from EVENT_STATUS))
# MAGIC
# MAGIC select rn , min(event_time), max(event_time), count(*) from cte group by 1

# COMMAND ----------

# Define Schema for PLAYERS_LOCATION Table
players_location_schema = StructType([
    StructField("name", StringType(), False),
    StructField("city", StringType(), False)
])

# Data for PLAYERS_LOCATION Table
players_location_data = [
    ("Sachin", "Mumbai"), ("Virat", "Delhi"),
    ("Rahul", "Bangalore"), ("Rohit", "Mumbai"),
    ("Mayank", "Bangalore")
]

# Create DataFrame
players_location_df = spark.createDataFrame(players_location_data, schema=players_location_schema)

# Register as a Temporary View
players_location_df.createOrReplaceTempView("PLAYERS_LOCATION")

# Display the Table Using Spark SQL
print("PLAYERS_LOCATION Table:")
spark.sql("SELECT * FROM PLAYERS_LOCATION").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(case when city='Bangalore' then name end) as Bangalore,
# MAGIC  max(case when city='Delhi' then name end) as Delhi,
# MAGIC   max(case when city='Mumbai' then name end) as Mumbai from (
# MAGIC select * , row_number() over (partition by city order by name) as rn from PLAYERS_LOCATION)
# MAGIC group by rn

# COMMAND ----------

# DBTITLE 1,median

# Create a DataFrame with the employee data
data = [
    Row(emp_id=1, company='A', salary=2341),
    Row(emp_id=2, company='A', salary=341),
    Row(emp_id=3, company='A', salary=15),
    Row(emp_id=4, company='A', salary=15314),
    Row(emp_id=5, company='A', salary=451),
    Row(emp_id=6, company='A', salary=513),
    Row(emp_id=7, company='B', salary=15),
    Row(emp_id=8, company='B', salary=13),
    Row(emp_id=9, company='B', salary=1154),
    Row(emp_id=10, company='B', salary=1345),
    Row(emp_id=11, company='B', salary=1221),
    Row(emp_id=12, company='B', salary=234),
    Row(emp_id=13, company='C', salary=2345),
    Row(emp_id=14, company='C', salary=2645),
    Row(emp_id=15, company='C', salary=2645),
    Row(emp_id=16, company='C', salary=2652),
    Row(emp_id=17, company='C', salary=65)
]

# Create a DataFrame
employee_df = spark.createDataFrame(data)

# Create a temporary view
employee_df.createOrReplaceTempView("employee")

# Display the data from the temporary view
result_df = spark.sql("SELECT * FROM employee")
result_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select company , avg(salary) as median from (
# MAGIC select *, ROW_NUMBER() over(partition by company order by salary) as rn, count(*) over(partition by company) as cnt from  employee ) where rn between  float(cnt)/2 and (float(cnt)/2)+1
# MAGIC group by 1

# COMMAND ----------

data = [
    Row(emp_id=1, emp_name='Ankit', salary=14300, manager_id=4, emp_age=39, dep_id=100, dep_name='Analytics', gender='Female'),
    Row(emp_id=2, emp_name='Mohit', salary=14000, manager_id=5, emp_age=48, dep_id=200, dep_name='IT', gender='Male'),
    Row(emp_id=3, emp_name='Vikas', salary=12100, manager_id=4, emp_age=37, dep_id=100, dep_name='Analytics', gender='Female'),
    Row(emp_id=4, emp_name='Rohit', salary=7260, manager_id=2, emp_age=16, dep_id=100, dep_name='Analytics', gender='Female'),
    Row(emp_id=5, emp_name='Mudit', salary=15000, manager_id=6, emp_age=55, dep_id=200, dep_name='IT', gender='Male'),
    Row(emp_id=6, emp_name='Agam', salary=15600, manager_id=2, emp_age=14, dep_id=200, dep_name='IT', gender='Male'),
    Row(emp_id=7, emp_name='Sanjay', salary=12000, manager_id=2, emp_age=13, dep_id=200, dep_name='IT', gender='Male'),
    Row(emp_id=8, emp_name='Ashish', salary=7200, manager_id=2, emp_age=12, dep_id=200, dep_name='IT', gender='Male'),
    Row(emp_id=9, emp_name='Mukesh', salary=7000, manager_id=6, emp_age=51, dep_id=300, dep_name='HR', gender='Male'),
    Row(emp_id=10, emp_name='Rakesh', salary=8000, manager_id=6, emp_age=50, dep_id=300, dep_name='HR', gender='Male'),
    Row(emp_id=11, emp_name='Akhil', salary=4000, manager_id=1, emp_age=31, dep_id=500, dep_name='Ops', gender='Male')
]

# Create a DataFrame
emp_df = spark.createDataFrame(data)

# Create a temporary view
emp_df.createOrReplaceTempView("emp")

# Display the data from the temporary view
result_df = spark.sql("SELECT * FROM emp")
result_df.show()
# write sql query to 3rd highest salary in each dept , incase less then 3 employee then employee with low salary

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (
# MAGIC select *, row_number() over (partition by dep_id order by salary desc) as rn , count(*) over (partition by dep_id) as cnt from emp)
# MAGIC where rn=3 or (cnt < 3 and rn=cnt)

# COMMAND ----------

# DBTITLE 1,year wise count of new cities
data = [
    Row(business_date='2020-01-02', city_id=3),
    Row(business_date='2020-07-01', city_id=7),
    Row(business_date='2021-01-01', city_id=3),
    Row(business_date='2021-02-03', city_id=19),
    Row(business_date='2022-12-01', city_id=3),
    Row(business_date='2022-12-15', city_id=3),
    Row(business_date='2022-02-28', city_id=12)
]

# Create a DataFrame
business_city_df = spark.createDataFrame(data)

# Convert the business_date column to DateType
business_city_df = business_city_df.withColumn("business_date", business_city_df["business_date"].cast(DateType()))

# Create a temporary view
business_city_df.createOrReplaceTempView("business_city")

# Display the data from the temporary view
result_df = spark.sql("SELECT * FROM business_city")
result_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * ,
# MAGIC  case 
# MAGIC  when business_date = (SELECT MIN(business_date) FROM business_city) THEN 1
# MAGIC when city_id not in (select city_id from business_city where business_date < p.business_date) then 1
# MAGIC else 0 end as new_city from business_city p

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year(business_date), sum(new_city) as new_city_cnt from (
# MAGIC SELECT *, 
# MAGIC     CASE 
# MAGIC         WHEN business_date = (SELECT MIN(business_date) FROM business_city) THEN 1
# MAGIC         WHEN city_id NOT IN (SELECT city_id FROM business_city WHERE business_date < p.business_date) THEN 1
# MAGIC         ELSE 0 
# MAGIC     END AS new_city 
# MAGIC FROM business_city p) GROUP BY 1

# COMMAND ----------

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

# MAGIC %sql
# MAGIC with grp_nbr as  (
# MAGIC select * , id- row_number() over (order by id) as rn  from stadium where people>=100)
# MAGIC select * from grp_nbr where rn in (
# MAGIC select rn from grp_nbr group by 1 having count(*)>=3)

# COMMAND ----------

# MAGIC %sql
# MAGIC with 
# MAGIC cte as(
# MAGIC select id,date,people,
# MAGIC lag(people) over(order by id) as previous_day,
# MAGIC lead(people) over (order by id) as next_day
# MAGIC from stadium 
# MAGIC ),
# MAGIC cte2 as
# MAGIC (select id from cte
# MAGIC where people>100 and previous_day>100 and next_day>100)
# MAGIC select  
# MAGIC id,date,people from stadium
# MAGIC where id in(
# MAGIC select id from cte2
# MAGIC union
# MAGIC select id-1 from cte2
# MAGIC union
# MAGIC select id+1 from cte2
# MAGIC );

# COMMAND ----------

data = [
    ('a1',1), ('a2',1), ('a3',0), ('a4',0), ('a5',0), ('a6',0), ('a7',1), ('a8',1), ('a9',0), ('a10',0),
    ('b1',0), ('b2',0), ('b3',0), ('b4',1), ('b5',1), ('b6',1), ('b7',1), ('b8',0), ('b9',0), ('b10',0),
    ('c1',0), ('c2',1), ('c3',0), ('c4',1), ('c5',1), ('c6',0), ('c7',1), ('c8',0), ('c9',0), ('c10',1)
]

# Define schema
schema = ["seat", "occupancy"]

# Create PySpark DataFrame
movie_df = spark.createDataFrame(data, schema=schema)

# Create temporary SQL view
movie_df.createOrReplaceTempView("movie")

# Display the DataFrame
movie_df.display()
# 4 consecutive empty seat in each row

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS (
# MAGIC     SELECT *, 
# MAGIC            LEFT(seat, 1) AS row_id, 
# MAGIC            SUBSTRING(seat, 2, 2) AS seat_id 
# MAGIC     FROM movie
# MAGIC ),
# MAGIC cte1 AS (
# MAGIC     SELECT *, 
# MAGIC            MAX(occupancy) OVER (PARTITION BY row_id ORDER BY seat_id ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING) AS is_empty,
# MAGIC            COUNT(occupancy) OVER (PARTITION BY row_id ORDER BY seat_id ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING) AS cnt 
# MAGIC     FROM cte
# MAGIC ),
# MAGIC cte2 AS (
# MAGIC     SELECT * 
# MAGIC     FROM cte1 
# MAGIC     WHERE is_empty = 0 AND cnt = 4
# MAGIC )
# MAGIC
# MAGIC SELECT cte1.* 
# MAGIC FROM cte1 
# MAGIC JOIN cte2 ON cte1.row_id = cte2.row_id 
# MAGIC            AND cte1.seat_id BETWEEN cte2.seat_id AND cte2.seat_id + 3;

# COMMAND ----------

data = [
    ('OUT', '181868', 13), ('OUT', '2159010', 8), ('OUT', '2159010', 178),
    ('SMS', '4153810', 1), ('OUT', '2159010', 152), ('OUT', '9140152', 18),
    ('SMS', '4162672', 1), ('SMS', '9168204', 1), ('OUT', '9168204', 576),
    ('INC', '2159010', 5), ('INC', '2159010', 4), ('SMS', '2159010', 1),
    ('SMS', '4535614', 1), ('OUT', '181868', 20), ('INC', '181868', 54),
    ('INC', '218748', 20), ('INC', '2159010', 9), ('INC', '197432', 66),
    ('SMS', '2159010', 1), ('SMS', '4535614', 1)
]

# Define schema
schema = ["call_type", "call_number", "call_duration"]

# Create PySpark DataFrame
call_details_df = spark.createDataFrame(data, schema=schema)

# Create temporary SQL view
call_details_df.createOrReplaceTempView("call_details")

# Display the DataFrame
call_details_df.display()

# 1. Numbers have both incoming and outgoing call
# 2. sum of duration of outgoing calls should be greater then sum of duration of incoming calls

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(
# MAGIC select call_number from call_details
# MAGIC where call_type in ('OUT', 'INC')
# MAGIC group by 1
# MAGIC having count(distinct call_type) = 2)
# MAGIC
# MAGIC select call_number , sum(if(call_type='OUT', call_duration, 0)) as out_dur,  sum(if(call_type='INC', call_duration, 0)) as inc_dur from call_details where call_number in (select call_number from cte) group by 1

# COMMAND ----------

# DBTITLE 1,leetcode problem 1412 : Find the Quiet Students in All Exams.
students_data = [
    (1, 'Daniel'), (2, 'Jade'), (3, 'Stella'), (4, 'Jonathan'), (5, 'Will')
]
students_schema = ["student_id", "student_name"]

# Create Students DataFrame
students_df = spark.createDataFrame(students_data, schema=students_schema)

# Define Exams Data
exams_data = [
    (10, 1, 70), (10, 2, 80), (10, 3, 90), (20, 1, 80), (30, 1, 70), 
    (30, 3, 80), (30, 4, 90), (40, 1, 60), (40, 2, 70), (40, 4, 80)
]
exams_schema = ["exam_id", "student_id", "score"]

# Create Exams DataFrame
exams_df = spark.createDataFrame(exams_data, schema=exams_schema)

# Register as Temporary Views
students_df.createOrReplaceTempView("students")
exams_df.createOrReplaceTempView("exams")

# Display DataFrames
print("Students Table:")
students_df.show()

print("Exams Table:")
exams_df.show()

# Student is quite when niether scored max marks or least marks in any of the exam 
# dont return the student who has never taken any exam order by student id 

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC select exam_id , max(score) as max_score, min(score) as min_score  from exams group by 1)
# MAGIC select student_id from (
# MAGIC select student_id , case 
# MAGIC when score!=max_score and score!=min_score then '0' else '1' end as stat from exams join cte on exams.exam_id=cte.exam_id) group by 1 having max(stat)=0
# MAGIC

# COMMAND ----------

phonelog_schema = StructType([
    StructField("Callerid", IntegerType(), True),
    StructField("Recipientid", IntegerType(), True),
    StructField("Datecalled", TimestampType(), True)
])

# Define phonelog data
phonelog_data = [
    (1, 2, datetime(2019, 1, 1, 9, 0, 0)),
    (1, 3, datetime(2019, 1, 1, 17, 0, 0)),
    (1, 4, datetime(2019, 1, 1, 23, 0, 0)),
    (2, 5, datetime(2019, 7, 5, 9, 0, 0)),
    (2, 3, datetime(2019, 7, 5, 17, 0, 0)),
    (2, 3, datetime(2019, 7, 5, 17, 20, 0)),
    (2, 5, datetime(2019, 7, 5, 23, 0, 0)),
    (2, 3, datetime(2019, 8, 1, 9, 0, 0)),
    (2, 3, datetime(2019, 8, 1, 17, 0, 0)),
    (2, 5, datetime(2019, 8, 1, 19, 30, 0)),
    (2, 4, datetime(2019, 8, 2, 9, 0, 0)),
    (2, 5, datetime(2019, 8, 2, 10, 0, 0)),
    (2, 5, datetime(2019, 8, 2, 10, 45, 0)),
    (2, 4, datetime(2019, 8, 2, 11, 0, 0))
]

# Create PySpark DataFrame
phonelog_df = spark.createDataFrame(phonelog_data, schema=phonelog_schema)

# Create temporary SQL view
phonelog_df.createOrReplaceTempView("phonelog")

# Display the DataFrame
phonelog_df.display()

# we need to find if the caller had done first and last call for the day to the same person. 

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct callerid,datte , first_rcp as Recipientid
# MAGIC  from (
# MAGIC select * ,date(datecalled) as datte,  first_value(Recipientid) over (partition by callerid , date(datecalled) order by datecalled) as first_rcp,
# MAGIC first_value(Recipientid) over (partition by callerid , date(datecalled) order by datecalled desc) as last_rcp  from phonelog)
# MAGIC where first_rcp=last_rcp

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
# MAGIC with cte as(select * , sum(salary) over (partition by experience order by salary rows between unbounded preceding and current row) rn_sal from candidates order by experience desc),
# MAGIC sen_cte as (select * from cte where experience='Senior' and rn_sal <= 70000)
# MAGIC select * from sen_cte
# MAGIC union
# MAGIC select * from cte where experience='Junior' and rn_sal <= 70000 - (select sum(salary) from sen_cte)

# COMMAND ----------

emp_schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("emp_name", StringType(), True),
    StructField("department_id", IntegerType(), True),
    StructField("salary", IntegerType(), True),
    StructField("manager_id", IntegerType(), True),
    StructField("emp_age", IntegerType(), True)
])

# Define employee data
emp_data = [
    (1, 'Ankit', 100, 10000, 4, 39),
    (2, 'Mohit', 100, 15000, 5, 48),
    (3, 'Vikas', 100, 12000, 4, 37),
    (4, 'Rohit', 100, 14000, 2, 16),
    (5, 'Mudit', 200, 20000, 6, 55),
    (6, 'Agam', 200, 12000, 2, 14),
    (7, 'Sanjay', 200, 9000, 2, 13),
    (8, 'Ashish', 200, 5000, 2, 12),
    (9, 'Mukesh', 300, 6000, 6, 51),
    (10, 'Rakesh', 500, 7000, 6, 50)
]

# Create PySpark DataFrame
emp_df = spark.createDataFrame(emp_data, schema=emp_schema)

# Create temporary SQL view
emp_df.createOrReplaceTempView("emp")

# Display the DataFrame
emp_df.display()
# list emp name along with manager and sen manager

# COMMAND ----------

# MAGIC %sql
# MAGIC select e1.emp_id, e1.emp_name, e2.emp_name, e3.emp_name from emp e1 left join emp e2 left join emp e3 
# MAGIC where e1.manager_id=e2.emp_id and e2.manager_id=e3.emp_id

# COMMAND ----------

# 43

# COMMAND ----------

schema = StructType([
    StructField("id", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("end_time", StringType(), True),
    StructField("start_loc", StringType(), True),
    StructField("end_loc", StringType(), True)
])

# Input data
data = [
    ('dri_1', '09:00', '09:30', 'a','b'),
    ('dri_1', '09:30', '10:30', 'b','c'),
    ('dri_1', '11:00', '11:30', 'd','e'),
    ('dri_1', '12:00', '12:30', 'f','g'),
    ('dri_1', '13:30', '14:30', 'c','h'),
    ('dri_2', '12:15', '12:30', 'f','g'),
    ('dri_2', '13:30', '14:30', 'c','h'),
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Create temporary view
df.createOrReplaceTempView("drivers")

# Display the data
df.show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC select id, max(cnt) as total_rides, sum(profit_ride) as profit_rides from(
# MAGIC select *, if(end_loc=ld , 1 ,0) profit_ride from (
# MAGIC select *,  lead(start_loc) over (partition by id order by start_time) as ld, count(*) over(partition by id) as cnt from drivers )) group by 1

# COMMAND ----------

purchase_schema = StructType([
    StructField("userid", IntegerType(), True),
    StructField("productid", IntegerType(), True),
    StructField("purchasedate", DateType(), True)
])

# Define purchase data
purchase_data = [
    (1, 1, datetime(2012, 1, 23).date()),
    (1, 2, datetime(2012, 1, 23).date()),
    (1, 3, datetime(2012, 1, 25).date()),
    (2, 1, datetime(2012, 1, 23).date()),
    (2, 2, datetime(2012, 1, 23).date()),
    (2, 2, datetime(2012, 1, 25).date()),
    (2, 4, datetime(2012, 1, 25).date()),
    (3, 4, datetime(2012, 1, 23).date()),
    (3, 1, datetime(2012, 1, 23).date()),
    (4, 1, datetime(2012, 1, 23).date()),
    (4, 2, datetime(2012, 1, 25).date())
]

# Create PySpark DataFrame
purchase_df = spark.createDataFrame(purchase_data, schema=purchase_schema)

# Create temporary SQL view
purchase_df.createOrReplaceTempView("purchase_history")

# Display the DataFrame
purchase_df.show()

# customers bought diff product on diff date

# COMMAND ----------

# MAGIC %sql
# MAGIC select userid from purchase_history group by 1 having count(distinct purchasedate) >=2 and count(productid) = count(distinct productid) 

# COMMAND ----------

schema = StructType([
    StructField("TRADE_ID", StringType(), False),
    StructField("Trade_Timestamp", TimestampType(), False),
    StructField("Trade_Stock", StringType(), False),
    StructField("Quantity", IntegerType(), False),
    StructField("Price", FloatType(), False)
])

# Define Data (Converted Timestamp)
data = [
    ('TRADE1', datetime.strptime('2024-03-28 10:01:05', '%Y-%m-%d %H:%M:%S'), 'ITJunction4All', 100, 20.0),
    ('TRADE2', datetime.strptime('2024-03-28 10:01:06', '%Y-%m-%d %H:%M:%S'), 'ITJunction4All', 20, 15.0),
    ('TRADE3', datetime.strptime('2024-03-28 10:01:08', '%Y-%m-%d %H:%M:%S'), 'ITJunction4All', 150, 30.0),
    ('TRADE4', datetime.strptime('2024-03-28 10:01:09', '%Y-%m-%d %H:%M:%S'), 'ITJunction4All', 300, 32.0),
    ('TRADE5', datetime.strptime('2024-03-28 10:10:00', '%Y-%m-%d %H:%M:%S'), 'ITJunction4All', -100, 19.0),
    ('TRADE6', datetime.strptime('2024-03-28 10:10:01', '%Y-%m-%d %H:%M:%S'), 'ITJunction4All', -300, 19.0)
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Register Temp View
df.createOrReplaceTempView("Trade_tbl")

# Display DataFrame
df.display()

# 49

# COMMAND ----------

# MAGIC %sql
# MAGIC select t1.* , t2.Trade_Timestamp, t2.Quantity, t2.price,
# MAGIC abs(((t1.price - t2.price) / t1.price)*100) as change from Trade_tbl t1 , Trade_tbl t2 where t1.TRADE_ID != t2.TRADE_ID and  t1.TRADE_ID < t2.TRADE_ID and (unix_timestamp(t2.Trade_Timestamp)-unix_timestamp(t1.Trade_Timestamp)) <=10 and abs(((t1.price - t2.price) / t1.price)*100)>=10

# COMMAND ----------

schema = StructType([
    StructField("section", StringType(), False),
    StructField("number", IntegerType(), False)
])

# Define Data
data = [
    ('A', 5), ('A', 7), ('A', 10),
    ('B', 7), ('B', 9), ('B', 10),
    ('C', 9), ('C', 7), ('C', 9),
    ('D', 10), ('D', 3), ('D', 8)
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Register Temp View
df.createOrReplaceTempView("section_data")

# Display DataFrame
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (select section from (
# MAGIC select * , dense_rank() over (order by number desc) as rnk from (
# MAGIC select * , row_number() over (partition by section order by number desc) as rn from section_data ) where rn=1) where rnk=1),
# MAGIC
# MAGIC cte2 as (select *, sum(number) over(partition by section) as ssm from (
# MAGIC select * , row_number() over(partition by section order by number desc) as rn from section_data where section in (select section from cte)) where rn<=2)
# MAGIC
# MAGIC select * from cte2 order by ssm desc limit 4

# COMMAND ----------


# Define Schema
schema = StructType([
    StructField("Booking_id", StringType(), False),
    StructField("Booking_date", StringType(), False),  # Initially as String
    StructField("User_id", StringType(), False),
    StructField("Line_of_business", StringType(), False)
])

# Define Data
data = [
    ('b1', '2022-03-23', 'u1', 'Flight'),
    ('b2', '2022-03-27', 'u2', 'Flight'),
    ('b3', '2022-03-28', 'u1', 'Hotel'),
    ('b4', '2022-03-31', 'u4', 'Flight'),
    ('b5', '2022-04-02', 'u1', 'Hotel'),
    ('b6', '2022-04-02', 'u2', 'Flight'),
    ('b7', '2022-04-06', 'u5', 'Flight'),
    ('b8', '2022-04-06', 'u6', 'Hotel'),
    ('b9', '2022-04-06', 'u2', 'Flight'),
    ('b10', '2022-04-10', 'u1', 'Flight'),
    ('b11', '2022-04-12', 'u4', 'Flight'),
    ('b12', '2022-04-16', 'u1', 'Flight'),
    ('b13', '2022-04-19', 'u2', 'Flight'),
    ('b14', '2022-04-20', 'u5', 'Hotel'),
    ('b15', '2022-04-22', 'u6', 'Flight'),
    ('b16', '2022-04-26', 'u4', 'Hotel'),
    ('b17', '2022-04-28', 'u2', 'Hotel'),
    ('b18', '2022-04-30', 'u1', 'Hotel'),
    ('b19', '2022-05-04', 'u4', 'Hotel'),
    ('b20', '2022-05-06', 'u1', 'Flight')
]

# 1Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Convert "Booking_date" from String to DateType
df1 = df.withColumn("Booking_date", to_date(df["Booking_date"], "yyyy-MM-dd"))

# Register Temp View
df1.createOrReplaceTempView("booking_table")

# Display DataFrame without truncation
df1.display()


# Define Schema
schema = StructType([
    StructField("User_id", StringType(), False),
    StructField("Segment", StringType(), False)
])

# Define Data
data = [
    ('u1', 's1'),
    ('u2', 's1'),
    ('u3', 's1'),
    ('u4', 's2'),
    ('u5', 's2'),
    ('u6', 's3'),
    ('u7', 's3'),
    ('u8', 's3'),
    ('u9', 's3'),
    ('u10', 's3')
]

# Create DataFrame
df2 = spark.createDataFrame(data, schema=schema)

# Register Temp View
df2.createOrReplaceTempView("user_table")

# Display DataFrame without truncation
df2.display()



# COMMAND ----------

# MAGIC %sql
# MAGIC select segment, count(distinct u.user_id) as user_id, count(distinct(case 
# MAGIC when Line_of_business='Flight' and Booking_date like '2022-04-%' then u.user_id end)) as april_2022_flight  from user_table u left join booking_table b on u.user_id=b.user_id group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (
# MAGIC select *, row_number() over (partition by user_id order by booking_date) as rn from booking_table) where rn=1 and Line_of_business='Hotel'

# COMMAND ----------

# MAGIC %sql
# MAGIC select user_id, date_diff( max(booking_date),min(booking_date) ) as rn from booking_table group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select segment, sum(if(Line_of_business='Hotel', 1, 0)) as hotel_booking ,sum(if(Line_of_business='Flight', 1, 0)) as flight_booking  from user_table u left join booking_table b on u.user_id=b.user_id  where year(booking_date)='2022' group by 1

# COMMAND ----------

schema = StructType([
    StructField("emp_id", IntegerType(), False),
    StructField("emp_name", StringType(), False),
    StructField("department_id", IntegerType(), False),
    StructField("salary", IntegerType(), False),
    StructField("manager_id", IntegerType(), False),
    StructField("emp_age", IntegerType(), False)
])

# Define Data
data = [
    (1, 'Ankit', 100, 10000, 4, 39),
    (2, 'Mohit', 100, 15000, 5, 48),
    (3, 'Vikas', 100, 10000, 4, 37),
    (4, 'Rohit', 100, 5000, 2, 16),
    (5, 'Mudit', 200, 12000, 6, 55),
    (6, 'Agam', 200, 12000, 2, 14),
    (7, 'Sanjay', 200, 9000, 2, 13),
    (8, 'Ashish', 200, 5000, 2, 12),
    (9, 'Mukesh', 300, 6000, 6, 51),
    (10, 'Rakesh', 300, 7000, 6, 50)
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Register Temp View
df.createOrReplaceTempView("emp")
df.display()
# 53 dept avg salary excluding dept avg salary

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (select department_id , avg(salary) avg_sal , count(emp_id) emp_cnt, sum(salary) total_sal  from emp group by 1)
# MAGIC -- select * from cte
# MAGIC select c1.department_id, max(avg_sal),max(emp_cnt),max(total_sal),  avg(c2.salary) from cte c1 left join emp c2 on c1.department_id != c2.department_id group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select department_id, avg(salary) from emp e1 group by 1 having avg(salary) < (select avg(salary) from emp e2 where e1.department_id!=e2.department_id )

# COMMAND ----------



# Schema for employee_checkin_details (timestamp as StringType initially)
checkin_schema = StructType([
    StructField("employeeid", IntegerType(), False),
    StructField("entry_details", StringType(), False),
    StructField("timestamp_details", StringType(), False)  # Initially as String
])

# Data for employee_checkin_details
checkin_data = [
    (1000, 'login', '2023-06-16 01:00:15.34'),
    (1000, 'login', '2023-06-16 02:00:15.34'),
    (1000, 'login', '2023-06-16 03:00:15.34'),
    (1000, 'logout', '2023-06-16 12:00:15.34'),
    (1001, 'login', '2023-06-16 01:00:15.34'),
    (1001, 'login', '2023-06-16 02:00:15.34'),
    (1001, 'login', '2023-06-16 03:00:15.34'),
    (1001, 'logout', '2023-06-16 12:00:15.34')
]

# Create DataFrame
df_checkin = spark.createDataFrame(checkin_data, schema=checkin_schema)

# Convert string timestamps to TimestampType
df_checkin = df_checkin.withColumn("timestamp_details", to_timestamp("timestamp_details", "yyyy-MM-dd HH:mm:ss.SS"))

# Register Temp View
df_checkin.createOrReplaceTempView("employee_checkin_details")

# Display DataFrame
df_checkin.display()


# Schema for employee_details
employee_schema = StructType([
    StructField("employeeid", IntegerType(), False),
    StructField("phone_number", StringType(), False),
    StructField("isdefault", BooleanType(), False)
])

# Data for employee_details
employee_data = [
    (1001, '9999', False),
    (1001, '1111', False),
    (1001, '2222', True),
    (1003, '3333', False)
]

# Create DataFrame
df_employee = spark.createDataFrame(employee_data, schema=employee_schema)

# Register Temp View
df_employee.createOrReplaceTempView("employee_details")

# Display DataFrame
df_employee.display()


# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(select *, case when entry_details='login' then timestamp_details end as lgn_t , 
# MAGIC case when entry_details='logout' then timestamp_details end as lgt_t from employee_checkin_details)
# MAGIC -- select * from cte
# MAGIC
# MAGIC select employeeid, count(*) as total_entry, sum(if(entry_details='login', 1, 0)) total_login, sum(if(entry_details='logout', 1, 0)) total_logout, 
# MAGIC max(lgn_t) as lgn_t, max(lgt_t) lgt_t  from cte group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select employeeid, count(*) as total_entry, sum(if(entry_details='login', 1, 0)), sum(if(entry_details='logout', 1, 0)), 
# MAGIC case when entry_details='login'and timestamp_details  from employee_checkin_details group by 1

# COMMAND ----------



# Schema for job_positions
job_positions_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("title", StringType(), False),
    StructField("groups", StringType(), False),
    StructField("levels", StringType(), False),
    StructField("payscale", IntegerType(), False),
    StructField("totalpost", IntegerType(), False)
])

# Data for job_positions
job_positions_data = [
    (1, 'General manager', 'A', 'l-15', 10000, 1),
    (2, 'Manager', 'B', 'l-14', 9000, 5),
    (3, 'Asst. Manager', 'C', 'l-13', 8000, 10)
]

# Create DataFrame
df_job_positions = spark.createDataFrame(job_positions_data, schema=job_positions_schema)

# Register Temp View
df_job_positions.createOrReplaceTempView("job_positions")

# Display DataFrame
df_job_positions.display()


# Schema for job_employees
job_employees_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("position_id", IntegerType(), False)
])

# Data for job_employees
job_employees_data = [
    (1, 'John Smith', 1),
    (2, 'Jane Doe', 2),
    (3, 'Michael Brown', 2),
    (4, 'Emily Johnson', 2),
    (5, 'William Lee', 3),
    (6, 'Jessica Clark', 3),
    (7, 'Christopher Harris', 3),
    (8, 'Olivia Wilson', 3),
    (9, 'Daniel Martinez', 3),
    (10, 'Sophia Miller', 3)
]

# Create DataFrame
df_job_employees = spark.createDataFrame(job_employees_data, schema=job_employees_schema)

# Register Temp View
df_job_employees.createOrReplaceTempView("job_employees")

# Display DataFrame
df_job_employees.display()

# 56

# COMMAND ----------

# %sql
# with cte as(select id, title, groups, levels, payscale, totalpost, 1 as rn from job_positions
# union all
# select id, title, groups, levels, payscale, totalpost, rn+1  from cte where rn+1 <= totalpost),

# temp as(select *, ROW_NUMBER() over(partition by position_id order by id) as rn from job_employees)

# select * from cte left join temp on cte.id=temp.position_id and cte.rn=temp.rn order by title

# COMMAND ----------

# ------------------------------
# namaste_orders DataFrame
# ------------------------------
orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("city", StringType(), False),
    StructField("sales", IntegerType(), False)
])

orders_data = [
    (1, 'Mysore', 100),
    (2, 'Mysore', 200),
    (3, 'Bangalore', 250),
    (4, 'Bangalore', 150),
    (5, 'Mumbai', 300),
    (6, 'Mumbai', 500),
    (7, 'Mumbai', 800)
]

df_orders = spark.createDataFrame(orders_data, schema=orders_schema)
df_orders.createOrReplaceTempView("namaste_orders")
df_orders.display()

# ------------------------------
# namaste_returns DataFrame
# ------------------------------
returns_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("return_reason", StringType(), False)
])

returns_data = [
    (3, 'wrong item'),
    (6, 'bad quality'),
    (7, 'wrong item')
]

df_returns = spark.createDataFrame(returns_data, schema=returns_schema)
df_returns.createOrReplaceTempView("namaste_returns")
df_returns.display()
# write sql query to return city where not even sengle order is returned

# COMMAND ----------

# MAGIC %sql
# MAGIC select city from namaste_orders where order_id not in (select order_id from namaste_returns)

# COMMAND ----------

# MAGIC %sql
# MAGIC select city from namaste_orders a left join namaste_returns b on a.order_id=b.order_id group by 1  having count(b.order_id) =0

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DateType
from datetime import datetime

# Start Spark session
spark = SparkSession.builder.appName("SKU Data").getOrCreate()

# Define schema
sku_schema = StructType([
    StructField("sku_id", IntegerType(), True),
    StructField("price_date", DateType(), True),
    StructField("price", IntegerType(), True)
])

# Define data with proper date objects
sku_data = [
    (1, datetime.strptime('2023-01-01', '%Y-%m-%d').date(), 10),
    (1, datetime.strptime('2023-02-15', '%Y-%m-%d').date(), 15),
    (1, datetime.strptime('2023-03-03', '%Y-%m-%d').date(), 18),
    (1, datetime.strptime('2023-03-27', '%Y-%m-%d').date(), 15),
    (1, datetime.strptime('2023-04-06', '%Y-%m-%d').date(), 20)
]

# Create DataFrame
df_sku = spark.createDataFrame(sku_data, schema=sku_schema)

# Register temp view
df_sku.createOrReplaceTempView("sku")

# Display DataFrame
df_sku.display()


# 59

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (select *, date_sub(lead(price_date, 1, '9999-01-01') over (order by price_date) , 1) as end_date from sku),
# MAGIC cte2 as (select distinct sku.sku_id,trunc(sku.price_date , 'MM') as tr_date, cte.price as tr_month from sku join cte on trunc(sku.price_date , 'MM') between cte.price_date and cte.end_date)
# MAGIC select * , (tr_month)-( lag(tr_month, 1, tr_month) over(order by tr_date)) as diff from cte2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from sku where trunc(price_date, 'MM')=price_date
# MAGIC union
# MAGIC select sku_id,next_date as price_date, price from (
# MAGIC select *, trunc(ifnull(date_sub(lead(price_date) over(order by price_date), 1), add_months(price_date, 1)) , 'MM') next_date from (
# MAGIC   select * from (
# MAGIC   select *, row_number()over(partition by month(price_date) order by price_date desc) rn from sku) where rn=1))

# COMMAND ----------


# Define schema
customers_schema = StructType([
    StructField("customer_name", StringType(), True)
])

# Define data
customers_data = [
    ("Ankit Bansal",),
    ("Vishal Pratap Singh",),
    ("Michael",)
]

# Create DataFrame
df_customers = spark.createDataFrame(customers_data, schema=customers_schema)

# Register temp view
df_customers.createOrReplaceTempView("customers")

# Display DataFrame
df_customers.display()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers;
# MAGIC
# MAGIC WITH cte AS (
# MAGIC     SELECT *, 
# MAGIC            (LEN(customer_name) - LEN(REPLACE(customer_name, ' ', ''))) AS spaces,
# MAGIC            CHARINDEX(' ', customer_name) AS first_space,
# MAGIC            CHARINDEX(' ', customer_name, CHARINDEX(' ', customer_name) + 1) AS second_space
# MAGIC     FROM customers
# MAGIC )
# MAGIC
# MAGIC
# MAGIC SELECT *, 
# MAGIC case when spaces=0 then customer_name
# MAGIC else  SUBSTRING(customer_name, 1, first_space - 1) end as first_name,
# MAGIC case when spaces<=1 then null
# MAGIC else SUBSTRING(customer_name , first_space+1 , second_space-first_space-1) end as mid_name,
# MAGIC case when spaces=0 then null
# MAGIC when spaces=1 then SUBSTRING(customer_name ,first_space+1 , len(customer_name)) 
# MAGIC when spaces=2 then SUBSTRING(customer_name ,second_space+1 , len(customer_name)) end as last_name
# MAGIC FROM cte;

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DateType
from datetime import date

# Start Spark session
spark = SparkSession.builder.appName("Stock Table").getOrCreate()

# Define schema
stock_schema = StructType([
    StructField("supplier_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("record_date", DateType(), True)
])

# Define data
stock_data = [
    (1, 1, 60, date(2022, 1, 1)),
    (1, 1, 40, date(2022, 1, 2)),
    (1, 1, 35, date(2022, 1, 3)),
    (1, 1, 45, date(2022, 1, 4)),
    (1, 1, 51, date(2022, 1, 6)),
    (1, 1, 55, date(2022, 1, 9)),
    (1, 1, 25, date(2022, 1, 10)),
    (1, 1, 48, date(2022, 1, 11)),
    (1, 1, 45, date(2022, 1, 15)),
    (1, 1, 38, date(2022, 1, 16)),
    (1, 2, 45, date(2022, 1, 8)),
    (1, 2, 40, date(2022, 1, 9)),
    (2, 1, 45, date(2022, 1, 6)),
    (2, 1, 55, date(2022, 1, 7)),
    (2, 2, 45, date(2022, 1, 8)),
    (2, 2, 48, date(2022, 1, 9)),
    (2, 2, 35, date(2022, 1, 10)),
    (2, 2, 52, date(2022, 1, 15)),
    (2, 2, 23, date(2022, 1, 16))
]

# Create DataFrame
df_stock = spark.createDataFrame(stock_data, schema=stock_schema)

# Create Temp View
df_stock.createOrReplaceTempView("stock")

# Display DataFrame
df_stock.display()
# 64

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(select * , date_sub(record_date, row_number() over(partition by supplier_id, product_id order by record_date)) as rn  from stock where stock_quantity <=50)
# MAGIC
# MAGIC select supplier_id, rn, product_id, count(*), min(record_date) from cte group by 1, 2, 3 having count(*) > 1

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Start Spark session
spark = SparkSession.builder.appName("City Distance Table").getOrCreate()

# Define schema
city_distance_schema = StructType([
    StructField("distance", IntegerType(), True),
    StructField("source", StringType(), True),
    StructField("destination", StringType(), True)
])

# Define data
city_distance_data = [
    (100, 'New Delhi', 'Panipat'),
    (200, 'Ambala', 'New Delhi'),
    (150, 'Bangalore', 'Mysore'),
    (150, 'Mysore', 'Bangalore'),
    (250, 'Mumbai', 'Pune'),
    (250, 'Pune', 'Mumbai'),
    (2500, 'Chennai', 'Bhopal'),
    (2500, 'Bhopal', 'Chennai'),
    (60, 'Tirupati', 'Tirumala'),
    (80, 'Tirumala', 'Tirupati')
]

# Create DataFrame
df_city_distance = spark.createDataFrame(city_distance_data, schema=city_distance_schema)

# Create Temp View
df_city_distance.createOrReplaceTempView("city_distance")

# Display DataFrame
df_city_distance.display()

# 65
# remove the duplicates incase of source , dest, dist are same and keep the first value only

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (select *, row_number() over (order by(select null)) as rn from city_distance)
# MAGIC
# MAGIC select c1.*  from cte c1 left join cte c2 on c1.source=c2.destination and c1.destination=c2.source 
# MAGIC where c2.distance is null or c2.distance!=c1.distance or c1.rn< c2.rn

# COMMAND ----------



# All your COVID data
raw_data = [
    ('2021-01-01',66),('2021-01-02',41),('2021-01-03',54),('2021-01-04',68),('2021-01-05',16),('2021-01-06',90),
    ('2021-01-07',34),('2021-01-08',84),('2021-01-09',71),('2021-01-10',14),('2021-01-11',48),('2021-01-12',72),
    ('2021-01-13',55),('2021-02-01',38),('2021-02-02',57),('2021-02-03',42),('2021-02-04',61),('2021-02-05',25),
    ('2021-02-06',78),('2021-02-07',33),('2021-02-08',93),('2021-02-09',62),('2021-02-10',15),('2021-02-11',52),
    ('2021-02-12',76),('2021-02-13',45),('2021-03-01',27),('2021-03-02',47),('2021-03-03',36),('2021-03-04',64),
    ('2021-03-05',29),('2021-03-06',81),('2021-03-07',32),('2021-03-08',89),('2021-03-09',63),('2021-03-10',19),
    ('2021-03-11',53),('2021-03-12',78),('2021-03-13',49),('2021-04-01',39),('2021-04-02',58),('2021-04-03',44),
    ('2021-04-04',65),('2021-04-05',30),('2021-04-06',87),('2021-04-07',37),('2021-04-08',95),('2021-04-09',60),
    ('2021-04-10',13),('2021-04-11',50),('2021-04-12',74),('2021-04-13',46),('2021-05-01',28),('2021-05-02',49),
    ('2021-05-03',35),('2021-05-04',67),('2021-05-05',26),('2021-05-06',82),('2021-05-07',31),('2021-05-08',92),
    ('2021-05-09',61),('2021-05-10',18),('2021-05-11',54),('2021-05-12',79),('2021-05-13',51),('2021-06-01',40),
    ('2021-06-02',59),('2021-06-03',43),('2021-06-04',66),('2021-06-05',27),('2021-06-06',85),('2021-06-07',38),
    ('2021-06-08',94),('2021-06-09',64),('2021-06-10',17),('2021-06-11',55),('2021-06-12',77),('2021-06-13',48),
    ('2021-07-01',34),('2021-07-02',50),('2021-07-03',37),('2021-07-04',69),('2021-07-05',32),('2021-07-06',80),
    ('2021-07-07',33),('2021-07-08',88),('2021-07-09',57),('2021-07-10',21),('2021-07-11',56),('2021-07-12',73),
    ('2021-07-13',42),('2021-08-01',41),('2021-08-02',53),('2021-08-03',39),('2021-08-04',62),('2021-08-05',23),
    ('2021-08-06',83),('2021-08-07',29),('2021-08-08',91),('2021-08-09',59),('2021-08-10',22),('2021-08-11',51),
    ('2021-08-12',75),('2021-08-13',44),('2021-09-01',36),('2021-09-02',45),('2021-09-03',40),('2021-09-04',68),
    ('2021-09-05',28),('2021-09-06',84),('2021-09-07',30),('2021-09-08',90),('2021-09-09',61),('2021-09-10',20),
    ('2021-09-11',52),('2021-09-12',71),('2021-09-13',43),('2021-10-01',46),('2021-10-02',58),('2021-10-03',41),
    ('2021-10-04',63),('2021-10-05',24),('2021-10-06',82),('2021-10-07',34),('2021-10-08',86),('2021-10-09',56),
    ('2021-10-10',14),('2021-10-11',57),('2021-10-12',70),('2021-10-13',47),('2021-11-01',31),('2021-11-02',44),
    ('2021-11-03',38),('2021-11-04',67),('2021-11-05',22),('2021-11-06',79),('2021-11-07',32),('2021-11-08',94),
    ('2021-11-09',60),('2021-11-10',15),('2021-11-11',54),('2021-11-12',73),('2021-11-13',46),('2021-12-01',29),
    ('2021-12-02',50),('2021-12-03',42),('2021-12-04',65),('2021-12-05',25),('2021-12-06',83),('2021-12-07',30),
    ('2021-12-08',93),('2021-12-09',58),('2021-12-10',19),('2021-12-11',52),('2021-12-12',75),('2021-12-13',48)
]

# Convert string dates to datetime objects
data = [(datetime.strptime(date, '%Y-%m-%d'), count) for date, count in raw_data]

# Define schema
schema = StructType([
    StructField("record_date", DateType(), False),
    StructField("cases_count", IntegerType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)
df.createOrReplaceTempView('covid_cases')
# Show first 20 rows
df.display()


# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(
# MAGIC select month(record_date) as  month, sum(cases_count) as cases from covid_cases group by month(record_date))
# MAGIC select *, sum(cases) over(order by month rows between unbounded preceding and 1 preceding ) as cases from cte

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create Spark session
spark = SparkSession.builder.appName("KingData").getOrCreate()

# King table data
kings_data = [
    (1, 'Robb Stark', 'House Stark'),
    (2, 'Joffrey Baratheon', 'House Lannister'),
    (3, 'Stannis Baratheon', 'House Baratheon'),
    (4, 'Balon Greyjoy', 'House Greyjoy'),
    (5, 'Mace Tyrell', 'House Tyrell'),
    (6, 'Doran Martell', 'House Martell'),
]

# Define schema
schema = StructType([
    StructField("k_no", IntegerType(), False),
    StructField("king", StringType(), False),
    StructField("house", StringType(), False)
])

# Create DataFrame
kings_df = spark.createDataFrame(kings_data, schema=schema)

# Register temp view
kings_df.createOrReplaceTempView("king")

# Directly display the DataFrame
kings_df.show(truncate=False)

# Battle table data
battle_data = [
    (1, 'Battle of Oxcross', 1, 2, 1, 'The North'),
    (2, 'Battle of Blackwater', 3, 4, 0, 'The North'),
    (3, 'Battle of the Fords', 1, 5, 1, 'The Reach'),
    (4, 'Battle of the Green Fork', 2, 6, 0, 'The Reach'),
    (5, 'Battle of the Ruby Ford', 1, 3, 1, 'The Riverlands'),
    (6, 'Battle of the Golden Tooth', 2, 1, 0, 'The North'),
    (7, 'Battle of Riverrun', 3, 4, 1, 'The Riverlands'),
    (8, 'Battle of Riverrun', 1, 3, 0, 'The Riverlands')
]

# Define schema
schema = StructType([
    StructField("battle_number", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("attacker_king", IntegerType(), False),
    StructField("defender_king", IntegerType(), False),
    StructField("attacker_outcome", IntegerType(), False),
    StructField("region", StringType(), False)
])

# Create DataFrame
battle_df = spark.createDataFrame(battle_data, schema=schema)

# Register temp view
battle_df.createOrReplaceTempView("battle")

# Directly display the DataFrame
battle_df.show(truncate=False)
# each region top battle won by king

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (select region, king, sum(won) as battle_won from(
# MAGIC select region, attacker_king as king , if(attacker_outcome=1, 1, 0) as won from battle
# MAGIC union all
# MAGIC select region, defender_king as king , if(attacker_outcome=0, 1, 0) as won from battle)
# MAGIC group by 1 , 2)
# MAGIC select house, region, battle_won from (
# MAGIC select cte.region, cte.king, cte.battle_won, king.king, king.house, dense_rank() over(partition by region order by battle_won desc) as rn from cte join king on cte.king=king.k_no) where rn=1
# MAGIC

# COMMAND ----------


# Cinema table data
cinema_data = [
    (1, 1), (2, 0), (3, 1), (4, 1), (5, 1), (6, 0), (7, 1), (8, 1), (9, 0), (10, 1),
    (11, 0), (12, 1), (13, 0), (14, 1), (15, 1), (16, 0), (17, 1), (18, 1), (19, 1), (20, 1)
]

# Define schema
schema = StructType([
    StructField("seat_id", IntegerType(), False),
    StructField("free", IntegerType(), False)
])

# Create DataFrame
cinema_df = spark.createDataFrame(cinema_data, schema=schema)

# Register temp view
cinema_df.createOrReplaceTempView("cinema")

# Display the DataFrame directly
cinema_df.show(truncate=False)
# consecutive

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (
# MAGIC select *, free * (lag(free, 1) over(order by seat_id)) as lg, 
# MAGIC free * (lead(free, 1) over(order by seat_id)) as ld from cinema)
# MAGIC where ld=1 or lg=1

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Start Spark session
spark = SparkSession.builder.appName("FriendsLikesData").getOrCreate()

# Data for friends table
friends_data = [
    (1, 2), (1, 3), (1, 4),
    (2, 1),
    (3, 1), (3, 4),
    (4, 1), (4, 3)
]

# Schema and DataFrame for friends
friends_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("friend_id", IntegerType(), False)
])
friends_df = spark.createDataFrame(friends_data, schema=friends_schema)
friends_df.createOrReplaceTempView("friends")

# Display friends table
print("=== Friends Table ===")
friends_df.show()

# Data for likes table
likes_data = [
    (1, 'A'), (1, 'B'), (1, 'C'),
    (2, 'A'),
    (3, 'B'), (3, 'C'),
    (4, 'B')
]

# Schema and DataFrame for likes
likes_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("page_id", StringType(), False)
])
likes_df = spark.createDataFrame(likes_data, schema=likes_schema)
likes_df.createOrReplaceTempView("likes")

# Display likes table
print("=== Likes Table ===")
likes_df.show()
#  70 - determine the user id and corresponding page id of the pages like by thier frnd but not user yet

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from friends f 
# MAGIC join likes fp on f.friend_id=fp.user_id
# MAGIC left join likes up on f.user_id=up.user_id and fp.page_id=up.page_id where up.page_id is null

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from datetime import datetime

# Create Spark session
spark = SparkSession.builder.appName("SwipeData").getOrCreate()

# Swipe table data
swipe_data = [
    (1, 'login',  datetime.strptime('2024-07-23 08:00:00', '%Y-%m-%d %H:%M:%S')),
    (1, 'logout', datetime.strptime('2024-07-23 12:00:00', '%Y-%m-%d %H:%M:%S')),
    (1, 'login',  datetime.strptime('2024-07-23 13:00:00', '%Y-%m-%d %H:%M:%S')),
    (1, 'logout', datetime.strptime('2024-07-23 17:00:00', '%Y-%m-%d %H:%M:%S')),
    (2, 'login',  datetime.strptime('2024-07-23 09:00:00', '%Y-%m-%d %H:%M:%S')),
    (2, 'logout', datetime.strptime('2024-07-23 11:00:00', '%Y-%m-%d %H:%M:%S')),
    (2, 'login',  datetime.strptime('2024-07-23 12:00:00', '%Y-%m-%d %H:%M:%S')),
    (2, 'logout', datetime.strptime('2024-07-23 15:00:00', '%Y-%m-%d %H:%M:%S')),
    (1, 'login',  datetime.strptime('2024-07-24 08:30:00', '%Y-%m-%d %H:%M:%S')),
    (1, 'logout', datetime.strptime('2024-07-24 12:30:00', '%Y-%m-%d %H:%M:%S')),
    (2, 'login',  datetime.strptime('2024-07-24 09:30:00', '%Y-%m-%d %H:%M:%S')),
    (2, 'logout', datetime.strptime('2024-07-24 10:30:00', '%Y-%m-%d %H:%M:%S')),
]

# Define schema
swipe_schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("activity_type", StringType(), False),
    StructField("activity_time", TimestampType(), False)
])

# Create DataFrame
swipe_df = spark.createDataFrame(swipe_data, schema=swipe_schema)

# Register temp view
swipe_df.createOrReplaceTempView("swipe")

# Display DataFrame
swipe_df.display(truncate=False)


# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(
# MAGIC select *,to_date(activity_time) as date,  lead(activity_time) over(partition by employee_id , to_date(activity_time) order by activity_time) as logout_time from swipe )
# MAGIC select employee_id, date, datediff(hour,min(activity_time) ,max(activity_time) ) as total_hours, 
# MAGIC  sum(datediff(hour, activity_time,logout_time )) as pr_hrs
# MAGIC  from cte
# MAGIC  where activity_type='login'
# MAGIC group by 1, 2

# COMMAND ----------

# active prime members at the end of 2020
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from datetime import datetime

# Create Spark session
spark = SparkSession.builder.appName("SubscriptionHistory").getOrCreate()

# Prepare the data
subscription_data = [
    (1, 'India',   datetime.strptime('2020-01-05', '%Y-%m-%d').date(), 'S', 6),
    (1, 'India',   datetime.strptime('2020-12-05', '%Y-%m-%d').date(), 'R', 1),
    (1, 'India',   datetime.strptime('2021-02-05', '%Y-%m-%d').date(), 'C', None),
    (2, 'India',   datetime.strptime('2020-02-15', '%Y-%m-%d').date(), 'S', 12),
    (2, 'India',   datetime.strptime('2020-11-20', '%Y-%m-%d').date(), 'C', None),
    (3, 'USA',     datetime.strptime('2019-12-01', '%Y-%m-%d').date(), 'S', 12),
    (3, 'USA',     datetime.strptime('2020-12-01', '%Y-%m-%d').date(), 'R', 12),
    (4, 'USA',     datetime.strptime('2020-01-10', '%Y-%m-%d').date(), 'S', 6),
    (4, 'USA',     datetime.strptime('2020-09-10', '%Y-%m-%d').date(), 'R', 3),
    (4, 'USA',     datetime.strptime('2020-12-25', '%Y-%m-%d').date(), 'C', None),
    (5, 'UK',      datetime.strptime('2020-06-20', '%Y-%m-%d').date(), 'S', 12),
    (5, 'UK',      datetime.strptime('2020-11-20', '%Y-%m-%d').date(), 'C', None),
    (6, 'UK',      datetime.strptime('2020-07-05', '%Y-%m-%d').date(), 'S', 6),
    (6, 'UK',      datetime.strptime('2021-03-05', '%Y-%m-%d').date(), 'R', 6),
    (7, 'Canada',  datetime.strptime('2020-08-15', '%Y-%m-%d').date(), 'S', 12),
    (8, 'Canada',  datetime.strptime('2020-09-10', '%Y-%m-%d').date(), 'S', 12),
    (8, 'Canada',  datetime.strptime('2020-12-10', '%Y-%m-%d').date(), 'C', None),
    (9, 'Canada',  datetime.strptime('2020-11-10', '%Y-%m-%d').date(), 'S', 1),
]

# Define schema
subscription_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("marketplace", StringType(), False),
    StructField("event_date", DateType(), False),
    StructField("event", StringType(), False),
    StructField("subscription_period", IntegerType(), True)
])

# Create DataFrame
subscription_df = spark.createDataFrame(subscription_data, schema=subscription_schema)

# Register as temp view
subscription_df.createOrReplaceTempView("subscription_history")

# Show the data
subscription_df.show(truncate=False)


# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(select *, row_number() over (partition by marketplace, customer_id order by event_date desc)as rn from subscription_history where event_date <='2020-12-31' )
# MAGIC select * , dateadd(month, subscription_period, event_date ) valid_till from cte where rn=1 and event!='C' and dateadd(month, subscription_period, event_date ) >='2020-12-31'

# COMMAND ----------

# Define schema
schema = StructType([
    StructField("n", IntegerType(), True)
])

# Create data
data = [(1,), (2,), (3,), (4,), (5,), (9,)]

# Create DataFrame
numbers_df = spark.createDataFrame(data, schema)

# Create temporary view
numbers_df.createOrReplaceTempView("numbers")

# Display DataFrame
numbers_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select n1.n from numbers n1 , numbers n2 where n1.n>=n2.n
# MAGIC -- with cte as (
# MAGIC -- select n ,1 as num_cnt from numbers
# MAGIC -- union all
# MAGIC -- select n, num_cnt+1 as num_cnt from cte where num_cnt+1 <=n)
# MAGIC
# MAGIC -- select * from cte order by n

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from datetime import date

# Initialize Spark session
spark = SparkSession.builder.appName("PollsExample").getOrCreate()

# --- Polls Table ---

polls_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("poll_id", StringType(), True),
    StructField("poll_option_id", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("created_date", DateType(), True),
])

polls_data = [
    ("id1", "p1", "A", 200, date(2021, 12, 1)),
    ("id2", "p1", "C", 250, date(2021, 12, 1)),
    ("id3", "p1", "A", 200, date(2021, 12, 1)),
    ("id4", "p1", "B", 500, date(2021, 12, 1)),
    ("id5", "p1", "C", 50, date(2021, 12, 1)),
    ("id6", "p1", "D", 500, date(2021, 12, 1)),
    ("id7", "p1", "C", 200, date(2021, 12, 1)),
    ("id8", "p1", "A", 100, date(2021, 12, 1)),
    ("id9", "p2", "A", 300, date(2023, 1, 10)),
    ("id10", "p2", "C", 400, date(2023, 1, 11)),
    ("id11", "p2", "B", 250, date(2023, 1, 12)),
    ("id12", "p2", "D", 600, date(2023, 1, 13)),
    ("id13", "p2", "C", 150, date(2023, 1, 14)),
    ("id14", "p2", "A", 100, date(2023, 1, 15)),
    ("id15", "p2", "C", 200, date(2023, 1, 16)),
]

polls_df = spark.createDataFrame(data=polls_data, schema=polls_schema)
polls_df.createOrReplaceTempView("polls")

# --- Poll Answers Table ---

answers_schema = StructType([
    StructField("poll_id", StringType(), True),
    StructField("correct_option_id", StringType(), True),
])

answers_data = [
    ("p1", "C"),
    ("p2", "A"),
]

answers_df = spark.createDataFrame(data=answers_data, schema=answers_schema)
answers_df.createOrReplaceTempView("poll_answers")

# --- Display both tables ---
print("Polls Table:")
polls_df.show()

print("Poll Answers Table:")
answers_df.show()
# 75

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(select p.poll_id, sum(amount) amnt from polls p join poll_answers pa on p.poll_id=pa.poll_id where poll_option_id!=correct_option_id group by 1),
# MAGIC
# MAGIC cte1 as(select p.*, (amount/ sum(amount) over(partition by p.poll_id)) as proportion from polls p join poll_answers pa on p.poll_id=pa.poll_id where poll_option_id=correct_option_id)
# MAGIC
# MAGIC select *,proportion*amnt as dist_amnt  from cte1 join cte on cte1.poll_id=cte.poll_id

# COMMAND ----------


# Define schema
phone_schema = StructType([
    StructField("num", StringType(), True)
])

# Insert data
phone_data = [
    ("1234567780",),
    ("2234578996",),
    ("+1-12244567780",),
    ("+32-2233567889",),
    ("+2-23456987312",),
    ("+91-9087654123",),
    ("+23-9085761324",),
    ("+11-8091013345",)
]

# Create DataFrame
phone_df = spark.createDataFrame(data=phone_data, schema=phone_schema)

# Create Temp View
phone_df.createOrReplaceTempView("phone_numbers")

# Show the data
phone_df.show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC select new_num, count(*) as total_digits , count(distinct digits) as dist_digits from (
# MAGIC select new_num, explode(split(new_num, ''))as digits from (
# MAGIC select * , case when split(num, '-')[1] is not null then split(num, '-')[1] else num end as new_num from phone_numbers)) group by 1

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from datetime import datetime

# Start Spark session
spark = SparkSession.builder.appName("EventsTable").getOrCreate()

# Define schema
events_schema = StructType([
    StructField("userid", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", TimestampType(), True)
])

# Insert data
events_data = [
    (1, 'click', datetime(2023, 9, 10, 9, 0, 0)),
    (1, 'click', datetime(2023, 9, 10, 10, 0, 0)),
    (1, 'scroll', datetime(2023, 9, 10, 10, 20, 0)),
    (1, 'click', datetime(2023, 9, 10, 10, 50, 0)),
    (1, 'scroll', datetime(2023, 9, 10, 11, 40, 0)),
    (1, 'click', datetime(2023, 9, 10, 12, 40, 0)),
    (1, 'scroll', datetime(2023, 9, 10, 12, 50, 0)),
    (2, 'click', datetime(2023, 9, 10, 9, 0, 0)),
    (2, 'scroll', datetime(2023, 9, 10, 9, 20, 0)),
    (2, 'click', datetime(2023, 9, 10, 10, 30, 0))
]

# Create DataFrame
events_df = spark.createDataFrame(data=events_data, schema=events_schema)

# Create Temp View
events_df.createOrReplaceTempView("events")

# Display data
events_df.show(truncate=False)


# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(select *, lag(event_time, 1, event_time) over(partition by userid order by event_time) as end_time from events)
# MAGIC
# MAGIC
# MAGIC select userid,session, min(event_time) as session_start, max(end_time) as session_end, count(*) as no_of_events, max(minutes) as min from(
# MAGIC select * , if(minutes<=30, 0, 1) as flag, sum( if(minutes<=30, 0, 1)) over (partition by userid order by event_time) as session from (
# MAGIC select * , datediff(minute, end_time, event_time) as minutes from cte))
# MAGIC group by 1, 2

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType

# Define schema for the assessments table
assessments_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("experience", IntegerType(), True),
    StructField("sql", IntegerType(), True),
    StructField("algo", IntegerType(), True),
    StructField("bug_fixing", IntegerType(), True)
])

# First batch of data
assessments_data_1 = [
    (1, 3, 100, None, 50),
    (2, 5, None, 100, 100),
    (3, 1, 100, 100, 100),
    (4, 5, 100, 50, None),
    (5, 5, 100, 100, 100)
]


# Create first DataFrame
df_assessments = spark.createDataFrame(assessments_data_1, schema=assessments_schema)
df_assessments.createOrReplaceTempView("assessments")
# Show final table
df_assessments.show()
# 70

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(SELECT *, 
# MAGIC        CASE 
# MAGIC            WHEN (CASE WHEN sql IS NULL OR sql = 100 THEN 1 ELSE 0 END +
# MAGIC                  CASE WHEN algo IS NULL OR algo = 100 THEN 1 ELSE 0 END +
# MAGIC                  CASE WHEN bug_fixing IS NULL OR bug_fixing = 100 THEN 1 ELSE 0 END) = 3 
# MAGIC            THEN '1' 
# MAGIC            ELSE '0' 
# MAGIC        END AS flag
# MAGIC FROM assessments)
# MAGIC
# MAGIC select experience, sum(flag), count(*) from cte group by 1

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType

# Initialize Spark session
spark = SparkSession.builder.appName("Transactions").getOrCreate()

# Define schema
schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("amount", IntegerType(), True),
    StructField("tran_Date", TimestampType(), True)
])

# Sample Data
data = [
    (1, 101, 500, datetime.strptime("2025-01-01 10:00:01", "%Y-%m-%d %H:%M:%S")),
    (2, 201, 500, datetime.strptime("2025-01-01 10:00:01", "%Y-%m-%d %H:%M:%S")),
    (3, 102, 300, datetime.strptime("2025-01-02 00:50:01", "%Y-%m-%d %H:%M:%S")),
    (4, 202, 300, datetime.strptime("2025-01-02 00:50:01", "%Y-%m-%d %H:%M:%S")),
    (5, 101, 700, datetime.strptime("2025-01-03 06:00:01", "%Y-%m-%d %H:%M:%S")),
    (6, 202, 700, datetime.strptime("2025-01-03 06:00:01", "%Y-%m-%d %H:%M:%S")),
    (7, 103, 200, datetime.strptime("2025-01-04 03:00:01", "%Y-%m-%d %H:%M:%S")),
    (8, 203, 200, datetime.strptime("2025-01-04 03:00:01", "%Y-%m-%d %H:%M:%S")),
    (9, 101, 400, datetime.strptime("2025-01-05 00:10:01", "%Y-%m-%d %H:%M:%S")),
    (10, 201, 400, datetime.strptime("2025-01-05 00:10:01", "%Y-%m-%d %H:%M:%S")),
    (11, 101, 500, datetime.strptime("2025-01-07 10:10:01", "%Y-%m-%d %H:%M:%S")),
    (12, 201, 500, datetime.strptime("2025-01-07 10:10:01", "%Y-%m-%d %H:%M:%S")),
    (13, 102, 200, datetime.strptime("2025-01-03 10:50:01", "%Y-%m-%d %H:%M:%S")),
    (14, 202, 200, datetime.strptime("2025-01-03 10:50:01", "%Y-%m-%d %H:%M:%S")),
    (15, 103, 500, datetime.strptime("2025-01-01 11:00:01", "%Y-%m-%d %H:%M:%S")),
    (16, 101, 500, datetime.strptime("2025-01-01 11:00:01", "%Y-%m-%d %H:%M:%S")),
    (17, 203, 200, datetime.strptime("2025-11-01 11:00:01", "%Y-%m-%d %H:%M:%S")),
    (18, 201, 200, datetime.strptime("2025-11-01 11:00:01", "%Y-%m-%d %H:%M:%S"))
]

# Create DataFrame
df_transactions = spark.createDataFrame(data, schema=schema)
df_transactions.createOrReplaceTempView("transactions")
# Show DataFrame
df_transactions.show()
# 80

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(select * from (
# MAGIC select transaction_id,customer_id as seller_id, amount , lead(customer_id) over(order by transaction_id) as buyer_id from transactions) where transaction_id%2!=0)
# MAGIC
# MAGIC select seller_id, buyer_id , count(*) as no_of_trans from cte group by 1, 2 order by no_of_trans desc limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(select * from (
# MAGIC select transaction_id,customer_id as seller_id, amount , lead(customer_id) over(order by transaction_id) as buyer_id from transactions) where transaction_id%2!=0),
# MAGIC  cte1 as(
# MAGIC select seller_id, buyer_id , count(*) as no_of_trans from cte group by 1, 2 order by no_of_trans desc)
# MAGIC
# MAGIC select * from cte where seller_id not in (
# MAGIC select seller_id from cte1
# MAGIC intersect 
# MAGIC select buyer_id from cte1)
# MAGIC and buyer_id not in (select seller_id from cte1
# MAGIC intersect 
# MAGIC select buyer_id from cte)
