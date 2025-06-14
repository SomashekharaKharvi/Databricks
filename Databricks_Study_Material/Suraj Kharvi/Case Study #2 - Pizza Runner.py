# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA  if not exists pizza_runner;
# MAGIC SET search_path = pizza_runner;
# MAGIC
# MAGIC DROP TABLE IF EXISTS pizza_runner.runners;
# MAGIC CREATE TABLE pizza_runner.runners (
# MAGIC   runner_id INTEGER,
# MAGIC   registration_date DATE
# MAGIC );
# MAGIC INSERT INTO pizza_runner.runners
# MAGIC   (runner_id, registration_date)
# MAGIC VALUES
# MAGIC   (1, '2021-01-01'),
# MAGIC   (2, '2021-01-03'),
# MAGIC   (3, '2021-01-08'),
# MAGIC   (4, '2021-01-15');
# MAGIC
# MAGIC
# MAGIC DROP TABLE IF EXISTS pizza_runner.customer_orders;
# MAGIC CREATE TABLE pizza_runner.customer_orders (
# MAGIC   order_id INTEGER,
# MAGIC   customer_id INTEGER,
# MAGIC   pizza_id INTEGER,
# MAGIC   exclusions VARCHAR(4),
# MAGIC   extras VARCHAR(4),
# MAGIC   order_time TIMESTAMP
# MAGIC );
# MAGIC INSERT INTO pizza_runner.customer_orders
# MAGIC   (order_id, customer_id, pizza_id, exclusions, extras, order_time)
# MAGIC VALUES
# MAGIC   ('1', '101', '1', '', '', '2020-01-01 18:05:02'),
# MAGIC   ('2', '101', '1', '', '', '2020-01-01 19:00:52'),
# MAGIC   ('3', '102', '1', '', '', '2020-01-02 23:51:23'),
# MAGIC   ('3', '102', '2', '', NULL, '2020-01-02 23:51:23'),
# MAGIC   ('4', '103', '1', '4', '', '2020-01-04 13:23:46'),
# MAGIC   ('4', '103', '1', '4', '', '2020-01-04 13:23:46'),
# MAGIC   ('4', '103', '2', '4', '', '2020-01-04 13:23:46'),
# MAGIC   ('5', '104', '1', 'null', '1', '2020-01-08 21:00:29'),
# MAGIC   ('6', '101', '2', 'null', 'null', '2020-01-08 21:03:13'),
# MAGIC   ('7', '105', '2', 'null', '1', '2020-01-08 21:20:29'),
# MAGIC   ('8', '102', '1', 'null', 'null', '2020-01-09 23:54:33'),
# MAGIC   ('9', '103', '1', '4', '1, 5', '2020-01-10 11:22:59'),
# MAGIC   ('10', '104', '1', 'null', 'null', '2020-01-11 18:34:49'),
# MAGIC   ('10', '104', '1', '2, 6', '1, 4', '2020-01-11 18:34:49');
# MAGIC
# MAGIC
# MAGIC DROP TABLE IF EXISTS pizza_runner.runner_orders;
# MAGIC CREATE TABLE pizza_runner.runner_orders (
# MAGIC   order_id INTEGER,
# MAGIC   runner_id INTEGER,
# MAGIC   pickup_time VARCHAR(19),
# MAGIC   distance VARCHAR(7),
# MAGIC   duration VARCHAR(10),
# MAGIC   cancellation VARCHAR(23)
# MAGIC );
# MAGIC INSERT INTO pizza_runner.runner_orders
# MAGIC   (order_id, runner_id, pickup_time, distance,duration, cancellation)
# MAGIC VALUES
# MAGIC   ('1', '1', '2020-01-01 18:15:34', '20km', '32 minutes', ''),
# MAGIC   ('2', '1', '2020-01-01 19:10:54', '20km', '27 minutes', ''),
# MAGIC   ('3', '1', '2020-01-03 00:12:37', '13.4km', '20 mins', NULL),
# MAGIC   ('4', '2', '2020-01-04 13:53:03', '23.4', '40', NULL),
# MAGIC   ('5', '3', '2020-01-08 21:10:57', '10', '15', NULL),
# MAGIC   ('6', '3', 'null', 'null', 'null', 'Restaurant Cancellation'),
# MAGIC   ('7', '2', '2020-01-08 21:30:45', '25km', '25mins', 'null'),
# MAGIC   ('8', '2', '2020-01-10 00:15:02', '23.4 km', '15 minute', 'null'),
# MAGIC   ('9', '2', 'null', 'null', 'null', 'Customer Cancellation'),
# MAGIC   ('10', '1', '2020-01-11 18:50:20', '10km', '10minutes', 'null');
# MAGIC
# MAGIC
# MAGIC DROP TABLE IF EXISTS pizza_runner.pizza_names;
# MAGIC CREATE TABLE pizza_runner.pizza_names (
# MAGIC   pizza_id INTEGER,
# MAGIC   pizza_name string
# MAGIC );
# MAGIC INSERT INTO pizza_runner.pizza_names
# MAGIC   (pizza_id, pizza_name)
# MAGIC VALUES
# MAGIC   (1, 'Meatlovers'),
# MAGIC   (2, 'Vegetarian');
# MAGIC
# MAGIC
# MAGIC DROP TABLE IF EXISTS pizza_runner.pizza_recipes;
# MAGIC CREATE TABLE pizza_runner.pizza_recipes (
# MAGIC   pizza_id INTEGER,
# MAGIC   toppings string
# MAGIC );
# MAGIC INSERT INTO pizza_runner.pizza_recipes
# MAGIC   (pizza_id, toppings)
# MAGIC VALUES
# MAGIC   (1, '1, 2, 3, 4, 5, 6, 8, 10'),
# MAGIC   (2, '4, 6, 7, 9, 11, 12');
# MAGIC
# MAGIC
# MAGIC DROP TABLE IF EXISTS pizza_runner.pizza_toppings;
# MAGIC CREATE TABLE pizza_runner.pizza_toppings (
# MAGIC   topping_id INTEGER,
# MAGIC   topping_name string
# MAGIC );
# MAGIC INSERT INTO pizza_runner. pizza_toppings
# MAGIC   (topping_id, topping_name)
# MAGIC VALUES
# MAGIC   (1, 'Bacon'),
# MAGIC   (2, 'BBQ Sauce'),
# MAGIC   (3, 'Beef'),
# MAGIC   (4, 'Cheese'),
# MAGIC   (5, 'Chicken'),
# MAGIC   (6, 'Mushrooms'),
# MAGIC   (7, 'Onions'),
# MAGIC   (8, 'Pepperoni'),
# MAGIC   (9, 'Peppers'),
# MAGIC   (10, 'Salami'),
# MAGIC   (11, 'Tomatoes'),
# MAGIC   (12, 'Tomato Sauce');

# COMMAND ----------

customer_orders=spark.read.format("delta").load("/user/hive/warehouse/pizza_runner.db/customer_orders")
pizza_names=spark.read.format("delta").load("/user/hive/warehouse/pizza_runner.db/pizza_names")
pizza_recipes=spark.read.format("delta").load("/user/hive/warehouse/pizza_runner.db/pizza_recipes")
pizza_toppings=spark.read.format("delta").load("/user/hive/warehouse/pizza_runner.db/pizza_toppings")
runner_orders=spark.read.format("delta").load("/user/hive/warehouse/pizza_runner.db/runner_orders")
runners=spark.read.format("delta").load("/user/hive/warehouse/pizza_runner.db/runners")

# COMMAND ----------

customer_orders.show()
pizza_names.show()
pizza_recipes.show()
pizza_toppings.show()
runner_orders.show()
runners.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###A. Pizza Metrics

# COMMAND ----------

from pyspark.sql.functions import when,col,lit
customer_orders=customer_orders.withColumn("exclusions", when(col("exclusions")=="null", lit('')).otherwise(col("exclusions"))).withColumn("extras", when(col("extras")=="null", lit('')).otherwise(col("extras")))
runner_orders=runner_orders.withColumn("pickup_time",when(col("pickup_time")=="null", lit('')).otherwise(col("extras"))).withColumn("distance ",when(col("distance ")=="null", lit(''))..otherwise(col("extras"))).withColumn("distance ",when("km" in col("distance ")", lit(''))..otherwise(col("extras"))).

# COMMAND ----------


