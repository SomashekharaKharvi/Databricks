# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------


data = [
    (1, 1, "2019-07-20", "open_session"),
    (1, 1, "2019-07-20", "scroll_down"),
    (1, 1, "2019-07-20", "end_session"),
    (2, 4, "2019-07-20", "open_session"),
    (2, 4, "2019-07-21", "send_message"),
    (2, 4, "2019-07-21", "end_session"),
    (3, 2, "2019-07-21", "open_session"),
    (3, 2, "2019-07-21", "send_message"),
    (3, 2, "2019-07-21", "end_session"),
    (4, 3, "2019-06-25", "open_session"),
    (4, 3, "2019-06-25", "end_session")
]

Activity = spark.createDataFrame(data, ["user_id", "session_id", "activity_date", "activity_type"])
Activity.createOrReplaceTempView('Activity')
display(Activity)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Write an SQL query to find the daily active user count for a period of 30 days ending 2019-07-27 inclusively. A user was active on some day if he/she made at least one activity on that day.
# MAGIC select activity_date as day, count(distinct(user_id)) as active_users from Activity
# MAGIC where activity_date between "2019-06-28" and "2019-07-27"
# MAGIC group by activity_date;

# COMMAND ----------

data = [[1, 3, 5, "2019-08-01"], [1, 3, 6, "2019-08-02"], [2, 7, 7, "2019-08-01"], 
        [2, 7, 6, "2019-08-02"], [4, 7, 1, "2019-07-22"], [3, 4, 4, "2019-07-21"], 
        [3, 4, 4, "2019-07-21"]]

# Define the schema
schema = ["article_id", "author_id", "viewer_id", "view_date"]

# Create a DataFrame
Views = spark.createDataFrame(data, schema)
Views.createOrReplaceTempView('Views')
display(Views)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Write an SQL query to find all the authors that viewed at least one of their own articles, sorted in ascending order by their id.
# MAGIC select distinct(author_id) as id from Views where author_id = viewer_id order by id asc;

# COMMAND ----------

data = [[1, 1, "2019-08-01", "2019-08-02"], [2, 5, "2019-08-02", "2019-08-02"], 
        [3, 1, "2019-08-11", "2019-08-11"], [4, 3, "2019-08-24", "2019-08-26"], 
        [5, 4, "2019-08-21", "2019-08-22"], [6, 2, "2019-08-11", "2019-08-13"]]

# Define the schema
schema = ["delivery_id", "customer_id", "order_date", "customer_pref_delivery_date"]

# Create a DataFrame
Delivery = spark.createDataFrame(data, schema)
Delivery.createOrReplaceTempView('Delivery')
display(Delivery)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Write an SQL query to find the percentage of immediate orders in the table, rounded to 2 decimal places.
# MAGIC select avg(if(order_date=customer_pref_delivery_date, 1, 0)) *100 from Delivery

# COMMAND ----------

data = [(1, 8000, "Jan"), (2, 9000, "Jan"), (3, 10000, "Feb"), (1, 7000, "Feb"), (1, 6000, "Mar")]

# create a DataFrame
Department = spark.createDataFrame(data, ["id", "revenue", "month"])
Department.createOrReplaceTempView('Department')
display(Department)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Write an SQL query to reformat the table such that there is a department id column and a revenue column for each month.
# MAGIC select 
# MAGIC     id,
# MAGIC     max(case when month = 'Jan' then revenue else null end) as Jan_Revenue,
# MAGIC     max(case when month = 'Feb' then revenue else null end) as Feb_Revenue,
# MAGIC     max(case when month = 'Mar' then revenue else null end) as Mar_Revenue,
# MAGIC     max(case when month = 'Apr' then revenue else null end) as Apr_Revenue,
# MAGIC     max(case when month = 'May' then revenue else null end) as May_Revenue,
# MAGIC     max(case when month = 'Jun' then revenue else null end) as Jun_Revenue,
# MAGIC     max(case when month = 'Jul' then revenue else null end) as Jul_Revenue,
# MAGIC     max(case when month = 'Aug' then revenue else null end) as Aug_Revenue,
# MAGIC     max(case when month = 'Sep' then revenue else null end) as Sep_Revenue,
# MAGIC     max(case when month = 'Oct' then revenue else null end) as Oct_Revenue,
# MAGIC     max(case when month = 'Nov' then revenue else null end) as Nov_Revenue,
# MAGIC     max(case when month = 'Dec' then revenue else null end) as Dec_Revenue
# MAGIC from Department
# MAGIC group by id

# COMMAND ----------

data = [(1, None), (2, None), (1, None), (12, None), (3, 1), (5, 2), (3, 1), (4, 1), (9, 1), (10, 2), (6, 7)]

# create a DataFrame
Submissions = spark.createDataFrame(data, ["sub_id", "parent_id"])
Submissions.createOrReplaceTempView('Submissions')
display(Submissions)

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC select sub_id as post_id from Submissions where parent_id is null)
# MAGIC select post_id, count(distinct(sub_id)) from cte left join Submissions on cte.post_id=Submissions.parent_id group by post_id

# COMMAND ----------

data = [
    (1, "2019-02-17", "2019-02-28", 5),
    (1, "2019-03-01", "2019-03-22", 20),
    (2, "2019-02-01", "2019-02-20", 15),
    (2, "2019-02-21", "2019-03-31", 30)
]

# Create a schema
schema = ["product_id", "start_date", "end_date", "price"]

# Create a PySpark DataFrame
Prices = spark.createDataFrame(data, schema)
Prices.createOrReplaceTempView('Prices')
display(Prices)
data = [
    (1, "2019-02-25", 100),
    (1, "2019-03-01", 15),
    (2, "2019-02-10", 200),
    (2, "2019-03-22", 30)
]

# Create a schema
schema = ["product_id", "purchase_date", "units"]

# Create a PySpark DataFrame
UnitsSold = spark.createDataFrame(data, schema)
UnitsSold.createOrReplaceTempView('UnitsSold')
display(UnitsSold)


# COMMAND ----------

# MAGIC %sql
# MAGIC --Write an SQL query to find the average selling price for each product.
# MAGIC select a.product_id, round(
# MAGIC     sum(a.price * b.units)/sum(b.units),
# MAGIC     2) as average_price
# MAGIC from Prices as a
# MAGIC join UnitsSold as b
# MAGIC on a.product_id = b.product_id and (b.purchase_date between a.start_date and a.end_date)
# MAGIC group by a.product_id;

# COMMAND ----------

data = [
    (1, "Boss", 1),
    (3, "Alice", 3),
    (2, "Bob", 1),
    (4, "Daniel", 2),
    (7, "Luis", 4),
    (8, "Jhon", 3),
    (9, "Angela", 8),
    (77, "Robert", 1)
]

# Create a schema
schema = ["employee_id", "employee_name", "manager_id"]

# Create a PySpark DataFrame
Employees = spark.createDataFrame(data, schema)
Employees.createOrReplaceTempView('Employees')
display(Employees)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Write an SQL query to find employee_id of all employees that directly or indirectly report their work to the head of the company.
# MAGIC select e3.employee_id from Employees e1, Employees e2, Employees e3
# MAGIC where e1.manager_id = 1 and e2.manager_id = e1.employee_id and e3.manager_id = e2.employee_id and e3.employee_id != 1

# COMMAND ----------

data = [(1, "Alice"), (2, "Bob"), (13, "John"), (6, "Alex")]

# create a DataFrame
Students = spark.createDataFrame(data, ["student_id", "student_name"])
Students.createOrReplaceTempView('Students')
data = [("Math",), ("Physics",), ("Programming",)]

# create a DataFrame
Subjects = spark.createDataFrame(data, ["subject_name"])
Subjects.createOrReplaceTempView('Subjects')
data = [(1, "Math"), (1, "Physics"), (1, "Programming"), (2, "Programming"), 
        (1, "Physics"), (1, "Math"), (13, "Math"), (13, "Programming"), 
        (13, "Physics"), (2, "Math"), (1, "Math")]

# create a DataFrame
Examinations = spark.createDataFrame(data, ["student_id", "subject_name"])
Examinations.createOrReplaceTempView('Examinations')
display(Students)
display(Subjects)
display(Examinations)


# COMMAND ----------

# MAGIC %sql
# MAGIC --Write an SQL query to find the number of times each student attended each exam.
# MAGIC select a.student_id, a.student_name, b.subject_name, count(c.subject_name) as attended_exams
# MAGIC from Students as a
# MAGIC join Subjects as b
# MAGIC left join Examinations as c
# MAGIC on a.student_id = c.student_id and b.subject_name = c.subject_name
# MAGIC group by a.student_id, a.student_name, b.subject_name;

# COMMAND ----------

data = [[1], [2], [3], [7], [8], [10]]

# Define the schema
schema = ["log_id"]

# Create a DataFrame
Logs = spark.createDataFrame(data, schema)
Logs.createOrReplaceTempView('Logs')
display(Logs)

# COMMAND ----------

# MAGIC %sql
# MAGIC select log_id from Logs where log_id - 1 not in (select log_id from Logs)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select log_id from Logs where log_id + 1 not in (select log_id from Logs)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from
# MAGIC (
# MAGIC select log_id from Logs where log_id - 1 not in (select log_id from Logs)
# MAGIC ) l1,
# MAGIC (
# MAGIC select log_id from Logs where log_id + 1 not in (select log_id from Logs)
# MAGIC ) l2
# MAGIC where l1.log_id <= l2.log_id
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Order the result table by start_id.
# MAGIC select l1.log_id as start_id, min(l2.log_id) as end_id
# MAGIC from
# MAGIC (
# MAGIC select log_id from Logs where log_id - 1 not in (select log_id from Logs)
# MAGIC ) l1,
# MAGIC (
# MAGIC select log_id from Logs where log_id + 1 not in (select log_id from Logs)
# MAGIC ) l2
# MAGIC where l1.log_id <= l2.log_id
# MAGIC group by l1.log_id;
# MAGIC

# COMMAND ----------

data = [(2, "USA"), (3, "Australia"), (7, "Peru"), (5, "China"), (8, "Morocco"), (9, "Spain")]

# create a DataFrame
Countries = spark.createDataFrame(data, ["country_id", "country_name"])
Countries.createOrReplaceTempView('Countries')
data = [(2, 15, "2019-11-01"), (2, 12, "2019-10-28"), (2, 12, "2019-10-27"), 
        (3, -2, "2019-11-10"), (3, 0, "2019-11-11"), (3, 3, "2019-11-12"), 
        (5, 16, "2019-11-07"), (5, 18, "2019-11-09"), (5, 21, "2019-11-23"), 
        (7, 25, "2019-11-28"), (7, 22, "2019-12-01"), (7, 20, "2019-12-02"), 
        (8, 25, "2019-11-05"), (8, 27, "2019-11-15"), (8, 31, "2019-11-25"), 
        (9, 7, "2019-10-23"), (9, 3, "2019-12-23")]

# create a DataFrame
Weather = spark.createDataFrame(data, ["country_id", "weather_state", "day"])
Weather.createOrReplaceTempView('Weather')
display(Countries)
display(Weather)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Write an SQL query to find the type of weather in each country for November 2019.
# MAGIC select c.country_name as country_name,
# MAGIC case
# MAGIC when avg(weather_state) <= 15 then "Cold"
# MAGIC when avg(weather_state) >= 25 then "Hot"
# MAGIC else "Warm"
# MAGIC end
# MAGIC as weather_type
# MAGIC from Countries c
# MAGIC join Weather w
# MAGIC on c.country_id = w.country_id
# MAGIC and w.day between "2019-11-01" and "2019-11-30"
# MAGIC group by c.country_name

# COMMAND ----------

data = [[1, 8], [2, 8], [3, 8], [4, 7], [5, 9], [6, 9]]

# Define the schema
schema = ["employee_id", "team_id"]

# Create a DataFrame
Employee = spark.createDataFrame(data, schema)
Employee.createOrReplaceTempView('Employee')
display(Employee)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Write an SQL query to find the team size of each of the employees.
# MAGIC select employee_id, team_size from Employee e
# MAGIC left join
# MAGIC (
# MAGIC select team_id, count(distinct(employee_id)) as team_size from Employee group by team_id
# MAGIC ) as t
# MAGIC on e.team_id = t.team_id;

# COMMAND ----------

data = [["Aron", "F", "2020-01-01", 17], ["Alice", "F", "2020-01-07", 23], 
        ["Bajrang", "M", "2020-01-07", 7], ["Khali", "M", "2019-12-25", 11], 
        ["Slaman", "M", "2019-12-30", 13], ["Joe", "M", "2019-12-30", 3], 
        ["Jose", "M", "2019-12-18", 2], ["Priya", "F", "2019-12-31", 23], 
        ["Priyanka", "F", "2019-12-30", 17]]

# Define the schema
schema = ["player_name", "gender", "day", "score_points"]

# Create a DataFrame
Scores = spark.createDataFrame(data, schema)
Scores.createOrReplaceTempView('Scores')

display(Scores)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, sum(score_points) over (partition by gender order by day) from Scores

# COMMAND ----------

data = [[1, 1, "Clicked"], [2, 2, "Clicked"], [3, 3, "Viewed"], 
        [5, 5, "Ignored"], [1, 7, "Ignored"], [2, 7, "Viewed"], 
        [3, 5, "Clicked"], [1, 4, "Viewed"], [2, 11, "Viewed"], 
        [1, 2, "Clicked"]]

# Define the schema
schema = ["ad_id", "user_id", "action"]

# Create a DataFrame
Ads = spark.createDataFrame(data, schema)
Ads.createOrReplaceTempView('Ads')
display(Ads)

# COMMAND ----------

# MAGIC %sql
# MAGIC select ad_id,ifnull(round(avg(
# MAGIC             case
# MAGIC                 when action = "Clicked" then 1
# MAGIC                 when action = "Viewed" then 0
# MAGIC                 else null
# MAGIC             end) *100,2),0)
# MAGIC as ctr
# MAGIC from Ads
# MAGIC group by ad_id
# MAGIC -- order by ctr desc, ad_id asc;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Write an SQL query to find the ctr of each Ad.
# MAGIC select ad_id,
# MAGIC ifnull(
# MAGIC     round(
# MAGIC         avg(
# MAGIC             case
# MAGIC                 when action = "Clicked" then 1
# MAGIC                 when action = "Viewed" then 0
# MAGIC                 else null
# MAGIC             end
# MAGIC         ) * 100,
# MAGIC     2),
# MAGIC 0)
# MAGIC as ctr
# MAGIC from Ads
# MAGIC group by ad_id
# MAGIC order by ctr desc, ad_id asc;

# COMMAND ----------

data = [(1, "Leetcode Solutions", "Book"), 
        (2, "Jewels of Stringology", "Book"), 
        (3, "HP", "Laptop"), 
        (4, "Lenovo", "Laptop"), 
        (5, "Leetcode Kit", "T-shirt")]

# create a DataFrame
Products = spark.createDataFrame(data, ["product_id", "product_name", "product_category"])
Products.createOrReplaceTempView('Products')
data = [(1, "2020-02-05", 60), (1, "2020-02-10", 70), 
        (2, "2020-01-18", 30), (2, "2020-02-11", 80), 
        (3, "2020-02-17", 2), (3, "2020-02-24", 3), 
        (4, "2020-03-01", 20), (4, "2020-03-04", 30), 
        (4, "2020-03-04", 60), (5, "2020-02-25", 50), 
        (5, "2020-02-27", 50), (5, "2020-03-01", 50)]

# create a DataFrame
Orders = spark.createDataFrame(data, ["product_id", "order_date", "unit"])
Orders.createOrReplaceTempView('Orders')
display(Products)
display(Orders)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Write an SQL query to get the names of products with greater than or equal to 100 units ordered in February 2020 and their amount.
# MAGIC select a.product_name, sum(unit) as unit
# MAGIC from Products a
# MAGIC left join Orders b
# MAGIC on a.product_id = b.product_id
# MAGIC where b.order_date between '2020-02-01' and '2020-02-29'
# MAGIC group by a.product_name
# MAGIC having sum(unit) >= 100

# COMMAND ----------

data = [(1, "Avengers"), (2, "Frozen 2"), (3, "Joker")]

# create a DataFrame
Movie = spark.createDataFrame(data, ["movie_id", "title"])
Movie.createOrReplaceTempView('Movie')

data = [(1, "Daniel"), (2, "Monica"), (3, "Maria"), (4, "James")]

# create a DataFrame
Users = spark.createDataFrame(data, ["user_id", "name"])
Users.createOrReplaceTempView('Users')
data = [
    (1, 1, 3, "2020-01-12"),
    (1, 2, 4, "2020-02-11"),
    (1, 3, 2, "2020-02-12"),
    (1, 4, 1, "2020-01-01"),
    (2, 1, 5, "2020-02-17"),
    (2, 2, 2, "2020-02-01"),
    (2, 3, 2, "2020-03-01"),
    (3, 1, 3, "2020-02-22"),
    (3, 2, 4, "2020-02-25")
]

# create a DataFrame
Movie_Rating = spark.createDataFrame(data, ["movie_id","user_id","rating","created_at"])
Movie_Rating.createOrReplaceTempView('Movie_Rating')
display(Movie)
display(Users)
display(Movie_Rating)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Write the following SQL query:
# MAGIC --Find the name of the user who has rated the greatest number of the movies.
# MAGIC --Find the movie name with the highest average rating as of Feb 2020.
# MAGIC SELECT user_name AS results FROM
# MAGIC (
# MAGIC SELECT a.name AS user_name, COUNT(*) AS counts FROM Movie_Rating AS b
# MAGIC     JOIN Users AS a
# MAGIC     on a.user_id = b.user_id
# MAGIC     GROUP BY a.name
# MAGIC     ORDER BY counts DESC, user_name ASC LIMIT 1
# MAGIC ) first_query
# MAGIC UNION
# MAGIC SELECT movie_name AS results FROM
# MAGIC (
# MAGIC SELECT c.title AS movie_name, AVG(d.rating) AS rate FROM Movie_Rating AS d
# MAGIC     JOIN Movie AS c
# MAGIC     on c.movie_id = d.movie_id
# MAGIC     WHERE substr(d.created_at, 1, 7) = '2020-02'
# MAGIC     GROUP BY c.title
# MAGIC     ORDER BY rate DESC, movie_name
# MAGIC ) second_query;
# MAGIC

# COMMAND ----------

data = [[1, "Electrical Engineering"], [7, "Computer Engineering"], [13, "Bussiness Administration"]]

# Define the schema
schema = ["id", "name"]

# Create a DataFrame
Departments = spark.createDataFrame(data, schema)
Departments.display()
Departments.createOrReplaceTempView('Departments')
data = [[23, "Alice", 1], [1, "Bob", 7], [5, "Jennifer", 13], [2, "John", 14], 
        [4, "Jasmine", 77], [3, "Steve", 74], [6, "Luis", 1], [8, "Jonathan", 7], 
        [7, "Daiana", 33], [11, "Madelynn", 1]]

# Define the schema
schema = ["id", "name", "department_id"]

# Create a DataFrame
Students = spark.createDataFrame(data, schema)
Students.createOrReplaceTempView('Students')
Students.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC --Write an SQL query to find the id and the name of all students who are enrolled in departments that no longer exists.
# MAGIC select id, name from Students where department_id not in (select id from Departments);

# COMMAND ----------

data = [[1, "Jonathan D.", "Eating"], [2, "Jade W.", "Singing"], 
        [3, "Victor J.", "Singing"], [4, "Elvis Q.", "Eating"], 
        [5, "Daniel A.", "Eating"], [6, "Bob B.", "Horse Riding"]]

# Define the schema
schema = ["id", "name", "activity"]

# Create a DataFrame
Friends = spark.createDataFrame(data, schema)
Friends.createOrReplaceTempView('Friends')
display(Friends)
data = [[1, "Eating"], [2, "Singing"], [3, "Horse Riding"]]

# Define the schema
schema = ["id", "name"]

# Create a DataFrame
Activities = spark.createDataFrame(data, schema)
Activities.createOrReplaceTempView('Activities')
display(Activities)


# COMMAND ----------

# MAGIC %sql
# MAGIC --Write an SQL query to find the names of all the activities with neither maximum, nor minimum number of participants.
# MAGIC --# Write your MySQL query statement below
# MAGIC select activity from Friends group by activity
# MAGIC having count(id) not in
# MAGIC (
# MAGIC select max(cnt) as cnt from
# MAGIC     (select count(*) as cnt from Friends group by activity) as t1
# MAGIC union
# MAGIC select min(cnt) as cnt from
# MAGIC     (select count(*) as cnt from Friends group by activity) as t2
# MAGIC )
# MAGIC

# COMMAND ----------

data = [
    (1, "Alice", "alice@leetcode.com"),
    (2, "Bob", "bob@leetcode.com"),
    (13, "John", "john@leetcode.com"),
    (6, "Alex", "alex@leetcode.com")
]

# create a DataFrame
Customers = spark.createDataFrame(data, ["customer_id", "customer_name", "email"])
Customers.createOrReplaceTempView('Customers')
display(Customers)

data = [
    (1, "Bob", "bob@leetcode.com"),
    (1, "John", "john@leetcode.com"),
    (1, "Jal", "jal@leetcode.com"),
    (2, "Omar", "omar@leetcode.com"),
    (2, "Meir", "meir@leetcode.com"),
    (6, "Alice", "alice@leetcode.com")
]

# create a DataFrame
Contacts = spark.createDataFrame(data, ["user_id", "contact_name", "contact_email"])
Contacts.createOrReplaceTempView('Contacts')
display(Contacts)
data = [
    (77, 100, 1),
    (88, 200, 1),
    (99, 300, 2),
    (66, 400, 2),
    (55, 500, 13),
    (44, 60, 6)
]

# create a DataFrame
Invoices = spark.createDataFrame(data, ["invoice_id", "price", "user_id"])
Invoices.createOrReplaceTempView('Invoices')
display(Invoices)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Write an SQL query to find the following for each invoice_id:
# MAGIC --customer_name: The name of the customer the invoice is related to.
# MAGIC --price: The price of the invoice.
# MAGIC --contacts_cnt: The number of contacts related to the customer.
# MAGIC --trusted_contacts_cnt: The number of contacts related to the customer and at the same time they are customers to the shop. (i.e His/Her email exists in the Customers table.)
# MAGIC --Order the result table by invoice_id.
# MAGIC select i.invoice_id,
# MAGIC c.customer_name,
# MAGIC i.price,
# MAGIC count(cont.contact_name) contacts_cnt,
# MAGIC sum(
# MAGIC     if(cont.contact_name in (select distinct customer_name from customers), 1, 0)
# MAGIC ) as trusted_contacts_cnt
# MAGIC from invoices i
# MAGIC join customers c on c.customer_id = i.user_id
# MAGIC left join Contacts cont on cont.user_id = c.customer_id
# MAGIC group by i.invoice_id,c.customer_name,
# MAGIC i.price
# MAGIC order by i.invoice_id;

# COMMAND ----------

data = [
    ("Alice", "Travel", "2020-02-12", "2020-02-20"),
    ("Alice", "Dancing", "2020-02-21", "2020-02-23"),
    ("Alice", "Travel", "2020-02-24", "2020-02-28"),
    ("Bob", "Travel", "2020-02-11", "2020-02-18")
]

# create a DataFrame
UserActivity = spark.createDataFrame(data, ["username", "activity", "startDate", "endDate"])
UserActivity.createOrReplaceTempView('UserActivity')
display(UserActivity)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (
# MAGIC select *, row_number() over (partition by username order by startDate) as rn,
# MAGIC count(*) over (partition by username ) as cnt from UserActivity)
# MAGIC where rn=2 or (cnt)=1

# COMMAND ----------

data = [
    (1, "Alice"),
    (7, "Bob"),
    (11, "Meir"),
    (90, "Winston"),
    (3, "Jonathan")
]

# create a DataFrame
Employees = spark.createDataFrame(data, ["id", "name"])
Employees.createOrReplaceTempView('Employees')
display(Employees)
data = [
    (3, 1),
    (11, 2),
    (90, 3)
]

# create a DataFrame
EmployeeUNI = spark.createDataFrame(data, ["id", "unique_id"])
EmployeeUNI.createOrReplaceTempView('EmployeeUNI')
display(EmployeeUNI)


# COMMAND ----------

# MAGIC %sql
# MAGIC --Write an SQL query to show the unique ID of each user, If a user doesn't have a unique ID replace just show null.
# MAGIC select uni.unique_id, emp.name from Employees emp
# MAGIC left join
# MAGIC EmployeeUNI uni
# MAGIC on emp.id = uni.id

# COMMAND ----------

data = [
    (1, "LC Phone"),
    (2, "LC T-Shirt"),
    (3, "LC Keychain")
]

# create a DataFrame
Product = spark.createDataFrame(data, ["product_id", "product_name"])
Product.createOrReplaceTempView('Product')
display(Product)
data = [
    (1, "2019-01-25", "2019-02-28", 100),
    (2, "2018-12-01", "2020-01-01", 10),
    (3, "2019-12-01", "2020-01-31", 1)
]

# create a DataFrame
Sales = spark.createDataFrame(data, ["product_id", "period_start", "period_end", "average_daily_sales"])
Sales.createOrReplaceTempView('Sales')
display(Sales)
# Write an SQL query to report the Total sales amount of each item for each year, with corresponding product name, product_id, product_name and report_year.


# COMMAND ----------

# MAGIC %sql
# MAGIC --Write an SQL query to report the Total sales amount of each item for each year, with corresponding product name, product_id, product_name and report_year.
# MAGIC with dates as
# MAGIC (select s_date =  min(period_start), e_date = max(period_end) from sales
# MAGIC 	union all
# MAGIC  select dateadd(day, 1 ,s_date) , e_date from dates
# MAGIC  where s_date<e_date
# MAGIC )
# MAGIC select
# MAGIC PRODUCT_ID  = cast(p.product_id  as varchar(200))
# MAGIC ,PRODUCT_NAME = p.product_name
# MAGIC ,REPORT_YEAR = cast(year(s_date) as varchar(10))
# MAGIC ,TOTAL_AMOUNT = sum(average_daily_sales)
# MAGIC from product p
# MAGIC left outer join sales s on p.product_id = s.product_id
# MAGIC left outer join dates d on d.s_date between s.period_start and s.period_end
# MAGIC group by p.product_id , p.product_name, year(s_date)
# MAGIC order by 1,3

# COMMAND ----------

data = [[1, "Alice"], [2, "Bob"], [3, "Alex"], [4, "Donald"], 
        [7, "Lee"], [13, "Jonathan"], [19, "Elvis"]]

# Define the schema
schema = ["id", "name"]

# Create a DataFrame
Users = spark.createDataFrame(data, schema)
Users.createOrReplaceTempView('Users')
display(Users)
data = [[1, 1, 120], [2, 2, 317], [3, 3, 222], [4, 7, 100], 
        [5, 13, 312], [6, 19, 50], [7, 7, 120], [8, 19, 400], 
        [9, 7, 230]]

# Define the schema
schema = ["id", "user_id", "distance"]

# Create a DataFrame
Rides = spark.createDataFrame(data, schema)
Rides.createOrReplaceTempView('Rides')
display(Rides)


# COMMAND ----------

# MAGIC %sql
# MAGIC --Write an SQL query to report the distance travelled by each user.
# MAGIC select name, sum(ifnull(distance, 0)) as travelled_distance
# MAGIC from rides r
# MAGIC right join users u
# MAGIC on r.user_id = u.id
# MAGIC group by name
# MAGIC order by 2 desc,1 asc;
# MAGIC

# COMMAND ----------

data = [
    (1, "John", "Doe"),
    (2, "Jane", "Doe"),
    (3, "Alice", "Smith")
]

# create a DataFrame
Person = spark.createDataFrame(data, ["PersonId", "FirstName", "LastName"])
Person.createOrReplaceTempView('Person')
Person.display()
data = [
    (1, 1, "New York", "NY"),
    (2, 1, "Los Angeles", "CA"),
    (3, 2, "Chicago", "IL"),
    (4, 3, "Houston", "TX")
]

# create a DataFrame
Address = spark.createDataFrame(data, ["AddressId", "PersonId", "City", "State"])
Address.createOrReplaceTempView('Address')
Address.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Write a SQL query for a report that provides the following information for each person in the Person table, regardless if there is an address for each of those people:
# MAGIC select Person.FirstName, Person.LastName, Address.City, Address.State from Person left join Address on Person.PersonId = Address.PersonId;

# COMMAND ----------

data = [[1, 100], [2, 200], [3, 300]]

# Define the schema
schema = ["Id", "Salary"]

# Create a DataFrame
Employee = spark.createDataFrame(data, schema)
Employee.createOrReplaceTempView('Employee')
display(Employee)


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Write a SQL query to get the second highest salary from the Employee table.
# MAGIC select max(Salary) as SecondHighestSalary from Employee where Salary < (select max(Salary) from Employee)

# COMMAND ----------

data = [[1, 3.50], [2, 3.65], [3, 4.00], [4, 3.85], 
        [5, 4.00], [6, 3.65]]

# Define the schema
schema = ["Id", "Score"]

# Create a DataFrame
Scores = spark.createDataFrame(data, schema)
Scores.createOrReplaceTempView('Scores')
display(Scores)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Write a SQL query to rank scores. If there is a tie between two scores, both should have the same ranking. Note that after a tie, the next ranking number should be the next consecutive integer value. In other words, there should be no "holes" between ranks.
# MAGIC select id, Score, dense_rank() over (order by Score desc)  from Scores

# COMMAND ----------

data = [
    (1, "Joe"),
    (2, "Henry"),
    (3, "Sam"),
    (4, "Max")
]

# create a DataFrame
Customers = spark.createDataFrame(data, ["Id", "Name"])
Customers.createOrReplaceTempView('Customers')
display(Customers)
data = [
    (1, 3),
    (2, 1)
]

# create a DataFrame
Orders = spark.createDataFrame(data, ["Id", "CustomerId"])
Orders.createOrReplaceTempView('Orders')
display(Orders)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Write a SQL query to find all customers who never order anything.
# MAGIC select Name as Customers from Customers left join Orders on Customers.Id = Orders.CustomerId where Orders.CustomerId is Null;

# COMMAND ----------

data = [
    (1, "Joe", 70000, 1),
    (2, "Henry", 80000, 2),
    (3, "Sam", 60000, 2),
    (4, "Max", 90000, 1),
    (5, "Janet", 69000, 1),
    (6, "Randy", 85000, 1)
]

# create a DataFrame
Employee = spark.createDataFrame(data, ["Id", "Name", "Salary", "DepartmentId"])
Employee.createOrReplaceTempView('Employee')
display(Employee)
data = [{"Id": 1, "Name": "IT"}, 
        {"Id": 2, "Name": "Sales"}]

Department = spark.createDataFrame(data)
Department.createOrReplaceTempView('Department')
display(Department)
# --Write a SQL query to find employees who earn the top three salaries in each of the department.


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (
# MAGIC select Name,salary,dept, dense_rank() over (partition by dept order by Salary desc ) as rank from (
# MAGIC select  e.Name as Name, Salary, d.Name as dept from Employee e join Department d on e.DepartmentId=d.id) ) where rank <=3

# COMMAND ----------

data = [
  (1, "john@example.com"), 
  (2, "bob@example.com"), 
  (3, "john@example.com")
  ]

#create a DataFrame
Person = spark.createDataFrame(data, ["Id", "Email"])
Person.createOrReplaceTempView('Person')
Person.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Write a SQL query to delete all duplicate email entries in a table named Person, keeping only unique emails based on its smallest Id.
# MAGIC select * from Person p1, Person p2 where p1.Email = p2.Email and p1.Id > p2.Id

# COMMAND ----------

data = [
    (1, "2015-01-01", 10),
    (2, "2015-01-02", 25),
    (3, "2015-01-03", 20),
    (4, "2015-01-04", 30)
]
schema = ["Id" , "Date" , "Temperature"]
# Create a PySpark DataFrame
Weather = spark.createDataFrame(data, schema)
Weather.createOrReplaceTempView('Weather')

display(Weather)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- write a SQL query to find all dates' Ids with higher temperature compared to its previous (yesterday's) dates.
# MAGIC
# MAGIC   SELECT Id, Date, Temperature,
# MAGIC          LAG(Temperature) OVER (ORDER BY Date) AS prev_temp
# MAGIC   FROM Weather

# COMMAND ----------

data = [
    (1, 1, 10, 1, "completed", "2013-10-01"),
    (2, 2, 11, 1, "cancelled_by_driver", "2013-10-01"),
    (3, 3, 12, 6, "completed", "2013-10-01"),
    (4, 4, 13, 6, "cancelled_by_client", "2013-10-01"),
    (5, 1, 10, 1, "completed", "2013-10-02"),
    (6, 2, 11, 6, "completed", "2013-10-02"),
    (7, 3, 12, 6, "completed", "2013-10-02"),
    (8, 2, 12, 12, "completed", "2013-10-03"),
    (9, 3, 10, 12, "completed", "2013-10-03"),
    (10, 4, 13, 12, "cancelled_by_driver", "2013-10-03")
]
schema =  ["Id" , "Client_Id" , "Driver_Id" , "City_Id" , "Status" , "Request_at"]
# Create a PySpark DataFrame
Trips = spark.createDataFrame(data, schema)
Trips.createOrReplaceTempView('Trips')
data = [
    (1, "No", "client"),
    (2, "Yes", "client"),
    (3, "No", "client"),
    (4, "No", "client"),
    (10, "No", "driver"),
    (11, "No", "driver"),
    (12, "No", "driver"),
    (13, "No", "driver")
]

schema = ["users_Id" , "Banned" , "Role"]
# Create a PySpark DataFrame
Users = spark.createDataFrame(data, schema)
Users.createOrReplaceTempView('Users')

display(Trips)
display(Users)

# COMMAND ----------

# MAGIC %sql
# MAGIC select t.Request_at, 
# MAGIC round(sum(case when t.Status  like 'cancelled_%' then 1 else 0 end)/count(*), 2)*100 as cnt from Trips t join Users u on t.client_id=u.users_id
# MAGIC where u.Banned='No' 
# MAGIC group by t.Request_at

# COMMAND ----------

# MAGIC %sql
# MAGIC --Write a SQL query to find the cancellation rate of requests made by unbanned clients between Oct 1, 2013 and Oct 3, 2013.
# MAGIC select t.Request_at as Day,
# MAGIC round(avg(case when t.Status like 'cancelled_%' then 1 else 0 end), 2) as Cancellation
# MAGIC from Trips t
# MAGIC inner join Users u
# MAGIC on t.Client_Id = u.Users_Id and u.Banned = 'No'
# MAGIC where t.Request_at between '2013-10-01' and '2013-10-03' group by t.Request_at

# COMMAND ----------

data = [(1, 2, "2016-03-01", 5), 
        (1, 2, "2016-05-02", 6), 
        (2, 3, "2017-06-25", 1), 
        (3, 1, "2016-03-02", 0), 
        (3, 4, "2018-07-03", 5)]

# Define the schema
schema = ["player_id", "device_id", "event_date", "games_played"]

# Create a DataFrame
Activity = spark.createDataFrame(data, schema)
Activity.createOrReplaceTempView('Activity')
display(Activity)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Write an SQL query that reports the first login date for each player.
# MAGIC select player_id, min(event_date) as first_login
# MAGIC from Activity
# MAGIC group by player_id

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ranked_devices AS (
# MAGIC   SELECT player_id, device_id, event_date,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY event_date) AS rank
# MAGIC   FROM Activity
# MAGIC )
# MAGIC SELECT player_id, event_date
# MAGIC FROM ranked_devices
# MAGIC WHERE rank = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Write a SQL query that reports the device that is first logged in for each player.
# MAGIC select player_id, device_id from Activity where
# MAGIC (player_id, event_date)
# MAGIC in
# MAGIC (select player_id, min(event_date) from Activity group by player_id);

# COMMAND ----------

data = [(1, 2, "2016-03-01", 5), 
        (1, 2, "2016-05-02", 6), 
        (2, 3, "2017-06-25", 1), 
        (3, 1, "2016-03-02", 0), 
        (3, 4, "2018-07-03", 5)]

# Define the schema
schema = ["player_id", "device_id", "event_date", "games_played"]

# Create a DataFrame
Activity = spark.createDataFrame(data, schema)
Activity.createOrReplaceTempView('Activity')
Activity.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT player_id, event_date, 
# MAGIC        SUM(games_played) OVER (PARTITION BY player_id ORDER BY event_date) AS total_games_played
# MAGIC FROM Activity
# MAGIC ORDER BY player_id, event_date;

# COMMAND ----------

data = [
    (1, "A", 2341),
    (2, "A", 341),
    (3, "A", 15),
    (4, "A", 15314),
    (5, "A", 451),
    (6, "A", 513),
    (7, "B", 15),
    (8, "B", 13),
    (9, "B", 1154),
    (10, "B", 1345),
    (11, "B", 1221),
    (12, "B", 234),
    (13, "C", 2345),
    (14, "C", 2645),
    (15, "C", 2645),
    (16, "C", 2652),
    (17, "C", 65)
]
schema = ["Id" , "Company" , "Salary"]
# Create a PySpark DataFrame
Employee = spark.createDataFrame(data, schema)
Employee.createOrReplaceTempView('Employee')
display(Employee)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ranked_salaries AS (
# MAGIC   SELECT Company, Salary,
# MAGIC          PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Salary) OVER (PARTITION BY Company) AS median_salary
# MAGIC   FROM Employee
# MAGIC )
# MAGIC SELECT DISTINCT Company, median_salary
# MAGIC FROM ranked_salaries;

# COMMAND ----------

data = [(101, "John", "A", None), 
        (102, "Dan", "A", 101), 
        (103, "James", "A", 101), 
        (104, "Amy", "A", 101), 
        (105, "Anne", "A", 101), 
        (106, "Ron", "B", 101)]

# Define the schema
schema = ["Id", "Name", "Department", "ManagerId"]

# Create a DataFrame
Employee = spark.createDataFrame(data, schema)
Employee.createOrReplaceTempView('Employee')
display(Employee)

# COMMAND ----------

# MAGIC %sql
# MAGIC --write a SQL query that finds out managers with at least 5 direct report.
# MAGIC select Name from Employee as Name where Id in
# MAGIC     (select ManagerId from Employee group by ManagerId having count(*) >= 5)
# MAGIC ;

# COMMAND ----------

data = [
    (0, 7),
    (1, 1),
    (2, 3),
    (3, 1)
]
schema = ["Number" , "Frequency"]
# Create a PySpark DataFrame
Numbers = spark.createDataFrame(data, schema)
Numbers.createOrReplaceTempView('Numbers')
Numbers.display()


# COMMAND ----------

# MAGIC %sql
# MAGIC --Write a query to find the median of all numbers and name the result as median.
# MAGIC with recursive cte as
# MAGIC (
# MAGIC   select number, frequency from Numbers
# MAGIC   union all
# MAGIC   select number, frequency -1 from cte where frequency > 1
# MAGIC )
# MAGIC select avg(number) as median
# MAGIC from
# MAGIC (
# MAGIC   select number, rank() over(order by number, frequency) as ranka,
# MAGIC   rank() over(order by number desc, frequency desc) as rankd
# MAGIC   from cte order by 1
# MAGIC ) x
# MAGIC where ranka = rankd or ranka = rankd + 1 or ranka = rankd - 1

# COMMAND ----------

data_candidate = [("1", "A"), ("2", "B"), ("3", "C"), ("4", "D"), ("5", "E")]
 
#Define the schema
columns_candidate = ["id", "Name"]

#Create a DataFrame
Candidate = spark.createDataFrame(data_candidate, columns_candidate)
Candidate.createOrReplaceTempView('Candidate')

#Define the schema

data_vote = [("1", "2"), ("2", "4"), ("3", "3"), ("4", "2"), ("5", "5")]

#Create a DataFrame
columns_vote = ["id", "CandidateId"]
Vote = spark.createDataFrame(data_vote, columns_vote)
Vote.createOrReplaceTempView('Vote')
display(Candidate)
display(Vote)


# COMMAND ----------

# MAGIC %sql
# MAGIC --Write a sql to find the name of the winning candidate, the above example will return the winner B.
# MAGIC select Name from Candidate as Name where id =
# MAGIC (select CandidateId from Vote group by CandidateId order by count(CandidateId) desc limit 1);

# COMMAND ----------

data = [
    (5, "show", 285, None, 1, 123),
    (5, "answer", 285, "124124", 1, 124),
    (5, "show", 369, None, 2, 125),
    (5, "skip", 369, None, 2, 126)
]
schema = ["uid" , "action" , "question_id" , "answer_id" , "q_num" , "timestamp" ]
# Create a PySpark DataFrame
survey_log = spark.createDataFrame(data, schema)
survey_log.createOrReplaceTempView('survey_log')

display(survey_log)

# COMMAND ----------

# MAGIC %sql
# MAGIC select question_id, avg( case when action ='answer' then 1 else 0 end) from survey_log group by question_id 

# COMMAND ----------

data = [
    ("Afghanistan", "Asia", 652230, 25500100, 20343000),
    ("Albania", "Europe", 28748, 2831741, 12960000),
    ("Algeria", "Africa", 2381741, 37100000, 188681000),
    ("Andorra", "Europe", 468, 78115, 3712000),
    ("Angola", "Africa", 1246700, 20609294, 100990000)
]
columns = ["name", "continent", "area", "population", "gdp"]
World = spark.createDataFrame(data, columns)
World.createOrReplaceTempView('World')
display(World)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Write a SQL solution to output big countries' name, population and area.
# MAGIC select name, population, area from World where area > 3000000 or population > 25000000;

# COMMAND ----------

data = [
    ("A", "Math"),
    ("B", "English"),
    ("C", "Math"),
    ("D", "Biology"),
    ("E", "Math"),
    ("F", "Computer"),
    ("G", "Math"),
    ("H", "Math"),
    ("I", "Math")
]
columns = ["student", "class"]
courses = spark.createDataFrame(data, columns)
courses.createOrReplaceTempView('courses')
display(courses)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Please list out all classes which have more than or equal to 5 students.
# MAGIC select class from courses
# MAGIC group by class
# MAGIC having count(distinct student) >= 5;

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

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC --write a query to display the records which have 3 or more consecutive rows and the amount of people more than 100(inclusive).
# MAGIC with cte as (
# MAGIC select *, id - row_number() over (order by date) as rn from stadium where people >=100)
# MAGIC select * from cte where rn in (
# MAGIC select rn from cte 
# MAGIC group by rn having count(*)>=3)

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
cinema.createOrReplaceTempView('cinema')
display(cinema)


# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC select *, seat_id - row_number() over (order by seat_id) as rn from cinema where free =1 )
# MAGIC select * from cte where rn in (
# MAGIC select rn from cte 
# MAGIC group by rn having count(*)>=3)

# COMMAND ----------

# MAGIC %sql
# MAGIC select seat_id from (
# MAGIC select 
# MAGIC seat_id,
# MAGIC free,
# MAGIC lag(free, 1, 0) over(order by seat_id) * free as previous_seat,
# MAGIC lead(free) over(order by seat_id) * free as next_seat
# MAGIC  from cinema )
# MAGIC  where previous_seat=1 or next_seat=1
# MAGIC

# COMMAND ----------

data = [
    (1, None),
    (2, 1),
    (3, 1),
    (4, 2),
    (5, 2)
]

schema = ["id" , "p_id"]
tree = spark.createDataFrame(data, schema)
tree.createOrReplaceTempView('tree')
display(tree)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Write a query to print the node id and the type of the node. Sort your output by the node id. The result for the above sample is:
# MAGIC select id , case when p_id is null then 'Root' 
# MAGIC when id in (select p_id from tree where p_id is not null) then 'Inner'
# MAGIC when  id not in (select p_id from tree where p_id is not null) then 'leaf' end as type from tree

# COMMAND ----------

data = [
    ("A", "B"),
    ("B", "C"),
    ("B", "D"),
    ("D", "E")
]

schema = ["followee" , "follower"]
follow = spark.createDataFrame(data, schema)
follow.createOrReplaceTempView('follow')
display(follow)

# COMMAND ----------

# MAGIC %sql
# MAGIC select follower, counts from follow left join (
# MAGIC select followee, count(*) as counts from follow group by 1 ) b on 
# MAGIC follow.follower =b.followee

# COMMAND ----------

data = [
    (1, 1, 9000, "2017-03-31"),
    (2, 2, 6000, "2017-03-31"),
    (3, 3, 10000, "2017-03-31"),
    (4, 1, 7000, "2017-02-28"),
    (5, 2, 6000, "2017-02-28"),
    (6, 3, 8000, "2017-02-28")
]
schema = ["id" , "employee_id" , "amount" , "pay_date"]
# Create a PySpark DataFrame
salary = spark.createDataFrame(data, schema)
salary.createOrReplaceTempView('salary')

data = [
    (1, 1),
    (2, 2),
    (3, 2)
]
schema = ["employee_id" , "department_id" ]
# Create a PySpark DataFrame
employee = spark.createDataFrame(data, schema)
employee.createOrReplaceTempView('employee')
display(salary)
display(employee)

# COMMAND ----------

# MAGIC %sql
# MAGIC   WITH company_avg AS (
# MAGIC     SELECT AVG(amount) AS company_avg_salary
# MAGIC     FROM salary
# MAGIC   ),
# MAGIC   department_avg AS (
# MAGIC     SELECT e.department_id, AVG(s.amount) AS department_avg_salary
# MAGIC     FROM salary s
# MAGIC     JOIN employee e ON s.employee_id = e.employee_id
# MAGIC     GROUP BY e.department_id
# MAGIC   )
# MAGIC   SELECT 
# MAGIC     d.department_id,
# MAGIC     d.department_avg_salary,
# MAGIC     c.company_avg_salary,
# MAGIC     CASE 
# MAGIC       WHEN d.department_avg_salary > c.company_avg_salary THEN 'higher'
# MAGIC       WHEN d.department_avg_salary < c.company_avg_salary THEN 'lower'
# MAGIC       ELSE 'ame'
# MAGIC     END AS comparison
# MAGIC   FROM department_avg d
# MAGIC   CROSS JOIN company_avg c

# COMMAND ----------

data = [
    (1, "War", "great 3D", "8.9"),
    (2, "Science", "fiction", "8.5"),
    (3, "irish", "boring", "6.2"),
    (4, "Ice song", "Fantacy", "8.6"),
    (5, "House card", "Interesting", "9.1")
]

schema = ["id" , "movie" , "description" , "rating"]
cinema = spark.createDataFrame(data, schema)
cinema.createOrReplaceTempView('cinema')
display(cinema)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Please write a SQL query to output movies with an odd numbered ID and a description that is not 'boring'. Order the result by rating.
# MAGIC select id, movie, description, rating
# MAGIC from cinema where mod (id, 2) = 1
# MAGIC and description not like 'boring'
# MAGIC order by rating desc;

# COMMAND ----------

 data = [
    (1, "Abbot"),
    (2, "Doris"),
    (3, "Emerson"),
    (4, "Green"),
    (5, "Jeames")
]

schema = ["id" , "student"]
seat = spark.createDataFrame(data, schema)
seat.createOrReplaceTempView('seat')
display(seat)

# COMMAND ----------

# MAGIC %sql
# MAGIC --write a SQL query to output the result for Mary?
# MAGIC select 
# MAGIC id +
# MAGIC (case when id%2=0 then -1 
# MAGIC when id=(select max(id) from seat) then 0 else 1  end) as id, student from Seat
# MAGIC order by id

# COMMAND ----------

data = [
    (1, "A", "m", 2500),
    (2, "B", "f", 1500),
    (3, "C", "m", 5500),
    (4, "D", "f", 500)
]
schema = ["id" , "name" , "sex" , "salary"]
# Create a PySpark DataFrame
salary = spark.createDataFrame(data, schema)
salary.createOrReplaceTempView('salary')
salary.display()


# COMMAND ----------

# MAGIC %sql
# MAGIC --write a single update statement, DO NOT write any select statement for this problem.
# MAGIC update salary
# MAGIC set sex = CHAR(ASCII('f') ^ ASCII('m') ^ ASCII(sex));
# MAGIC
# MAGIC update salary
# MAGIC set sex = case sex
# MAGIC     when 'm' then 'f'
# MAGIC     else 'm'
# MAGIC     end;
# MAGIC

# COMMAND ----------

marks_data = [
    ('A', 'X', 75),
    ('A', 'Y', 75),
    ('A', 'Z', 80),
    ('B', 'X', 90),
    ('B', 'Y', 91),
    ('B', 'Z', 75)
]

# Create a DataFrame for student IDs and marks
marks_df = spark.createDataFrame(marks_data, ["sname","sid", "marks"])
marks_df.createOrReplaceTempView('marks_df')
# Show the DataFrames
print("Student Name DataFrame:")
marks_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select sname, sum(marks) as total_marks from (
# MAGIC select * , row_number() over(partition by sname order by marks desc) as rn from marks_df) where rn <=2 group by sname 

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import *

windowSpec = Window.partitionBy("sname").orderBy(col("marks").desc())

marks_df.withColumn("rn" , row_number().over(windowSpec)).filter("rn<=2").groupBy("sname").agg(sum("marks").alias("sum")).display()

# COMMAND ----------

data = [
    ("jan", 15),  # Row 1: (month, ytd_sales)
    ("feb", 22),  # Row 2: (month, ytd_sales)
    ("mar", 35),  # Row 3: (month, ytd_sales)
    ("apr", 45),  # Row 4: (month, ytd_sales)
    ("may", 60)   # Row 5: (month, ytd_sales)
]
from pyspark.sql.functions import *
# Create a DataFrame from the sample data
sales_df = spark.createDataFrame(data, ["month", "ytd_sales"])
# .withColumn("rn" , lit(monotonically_increasing_id()))
# sales_df=sales_df.withColumn("sequence_number", sales_df.rn + 1)
sales_df.createOrReplaceTempView("sales_df")


# Show the DataFrame
sales_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select month, ytd_sales, (ytd_sales - prev_val) as diff from(
# MAGIC select * , lag(ytd_sales, 1 , 0) over (order by (select null) ) as prev_val from sales_df)

# COMMAND ----------

data = [
    (2, "Finland"),
    (3, "Denmark"),
    (4, "Iceland"),
    (5, "Israel"),
    (6, "Netherlands"),
    (7, "Sweden"),
    (126, "Norway"),
    (128, "India"),
    (129, "Srilanka")
]

# Create a DataFrame from the sample data
country_ranking_df = spark.createDataFrame(data, ["ranking", "country"])
country_ranking_df.createOrReplaceTempView("country_ranking_df")
# Show the DataFrame
country_ranking_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select  
# MAGIC *, 
# MAGIC case 
# MAGIC when country='India' then 1
# MAGIC when country='Srilanka' then 2
# MAGIC else 3
# MAGIC end as rn
# MAGIC from country_ranking_df 
# MAGIC order by rn 

# COMMAND ----------

data = [
    (1, 1),
    (2, 0),
    (3, 1),
    (4, 0),
    (5, 1),
    (6, 1),
    (7, 1),
    (8, 0),
    (9, 1),
    (10, 1)
]

schema = ["seat_id" , "free"]
cinema = spark.createDataFrame(data, schema)
cinema.createOrReplaceTempView('cinema')
display(cinema)


# COMMAND ----------

# MAGIC %sql
# MAGIC select seat_id from (
# MAGIC select 
# MAGIC seat_id,
# MAGIC free,
# MAGIC lag(free, 1, 0) over(order by seat_id) * free as previous_seat,
# MAGIC lead(free) over(order by seat_id) * free as next_seat
# MAGIC  from cinema )
# MAGIC  where previous_seat=1 or next_seat=1
# MAGIC

# COMMAND ----------

data = [
    ("chocolates", "5-star"), 
    (None, "dairymilk"), 
    (None, "perk"), 
    (None, "eclair"), 
    ("Biscuits", "Britania"), 
    (None, "goodday"), 
    (None, "boost")
]

# Create a DataFrame from the sample data
category_brand_df = spark.createDataFrame(data, ["category", "brand_name"])
category_brand_df.createOrReplaceTempView("category_brand_df")
# Show the DataFrame
category_brand_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select first_value(category) over(partition by cnt) as category, brand_name  from (
# MAGIC select *, count(category) over(order by rn ) as cnt from(
# MAGIC select * , row_number() over (order by (select null)) as rn from category_brand_df))

# COMMAND ----------

from pyspark.sql.functions import * 
data = [
    ("2024-01-13 09:25", 10),
    ("2024-01-13 19:35", 10),
    ("2024-01-16 09:10", 10),
    ("2024-01-16 18:10", 10),
    ("2024-02-11 09:07", 10),
    ("2024-02-11 19:20", 10),
    ("2024-02-17 08:40", 17),
    ("2024-02-17 18:04", 17),
    ("2024-03-23 09:20", 10),
    ("2024-03-23 18:30", 10)
]

# Create a DataFrame from the sample data
id_empid_df = spark.createDataFrame(data, ["id", "empid"]).withColumn("id", col("id").cast('timestamp'))
id_empid_df.createOrReplaceTempView("id_empid_df")
# Show the DataFrame
id_empid_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select empid, sum(hours) from(
# MAGIC select *, round(((unix_timestamp(end_hour)-unix_timestamp(id))/60)/60,2) as hours from (
# MAGIC select *, lead(id) over (partition by empid, to_date(id) order by (select null) ) as end_hour  from id_empid_df where dayofweek(to_date(id)) in (1,7)) where end_hour is not null)
# MAGIC group by empid

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (select cast(id as timestamp) as id, empid , to_date(cast(id as timestamp)) as date from id_empid_df 
# MAGIC where dayofweek(cast(id as timestamp))  in (1 , 7) ),
# MAGIC cte2 as (select empid, date, ((unix_timestamp(max(id)) - unix_timestamp(min(id))) /60) /60 as hours from cte
# MAGIC group by empid, date)
# MAGIC select empid, round(sum(hours), 2) as hours from cte2 group by empid

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("CreateSampleDataFrame").getOrCreate()

# Sample data for the DataFrame
data = [
    ("a", 10, 20),  # Row 1: (col1, col2, col3)
    ("b", 50, 30)   # Row 2: (col1, col2, col3)
]

# Create a DataFrame from the sample data
df = spark.createDataFrame(data, ["col1", "col2", "col3"])
df.createOrReplaceTempView('df')
# Show the DataFrame
df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC  col1,
# MAGIC  case when col2<=col3 then col3
# MAGIC  when col3<=col2 then col2
# MAGIC  end as maxVal
# MAGIC from df 

# COMMAND ----------

# MAGIC %sql
# MAGIC select col1, greatest(col2,col3 ) from df

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("CreateSalesDataFrame").getOrCreate()

# Sample data for the DataFrame
data = [
    ("2023-10-03", 10),  # Row 1: (dt, sales)
    ("2023-10-04", 20),  # Row 2: (dt, sales)
    ("2023-10-05", 60),  # Row 3: (dt, sales)
    ("2023-10-06", 50),  # Row 4: (dt, sales)
    ("2023-10-07", 10)   # Row 5: (dt, sales)
]

# Create a DataFrame from the sample data
sales_df = spark.createDataFrame(data, ["dt", "sales"])
sales_df.createOrReplaceTempView("df")
# Show the DataFrame
sales_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC select *, lag(sales, 1 , 0) over (order by (select null)) as prev_day_val from df)
# MAGIC select  dt, coalesce(((sales - prev_day_val) / prev_day_val ) *100 , 0)  as  var from cte
# MAGIC where coalesce(((sales - prev_day_val) / prev_day_val ) *100 , 0) >=0

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("CreateSampleDataFrame").getOrCreate()

# Sample data for the DataFrame
data = [
    (1, "a,b,c"),  # Row 1: (col1, col2)
    (2, "a,b")     # Row 2: (col1, col2)
]

# Create a DataFrame from the sample data
df = spark.createDataFrame(data, ["col1", "col2"])
df.createOrReplaceTempView("df")

# Show the DataFrame
df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select col1, size((split(col2,',' ))) as m from df

# COMMAND ----------

data = [
    (1, 'd1', 1.0),
    (2, 'd1', 5.28),
    (3, 'd1', 4.0),
    (4, 'd2', 8.0),
    (5, 'd1', 2.5),
    (6, 'd2', 7.0),
    (7, 'd3', 9.0),
    (8, 'd4', 10.2)
]

# Define the schema (column names)
columns = ["eid", "dept", "scores"]

# Create the DataFrame
empdept_tbl = spark.createDataFrame(data, schema=columns)

# Create a temporary view
empdept_tbl.createOrReplaceTempView("empdept_tbl")
empdept_tbl.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, max(scores) over (partition by dept order by (select null)) as s from empdept_tbl

# COMMAND ----------

data = [
    (1, 'abc@gmail.com'),
    (2, 'xyz@hotmail.com'),
    (3, 'pqr@outlook.com')
]

# Define the schema (column names)
columns = ["id", "email"]

# Create the DataFrame
customer_tbl = spark.createDataFrame(data, schema=columns)

# Create a temporary view
customer_tbl.createOrReplaceTempView("customer_tbl")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, regexp_replace(email, r"[a-z]+[@]" , "" ) from customer_tbl

# COMMAND ----------

data = [
    (1, 'John', 50000, None),
    (2, 'Alice', 40000, 1),
    (3, 'Bob', 70000, 1),
    (4, 'Emily', 55000, None),
    (5, 'Charlie', 65000, 4),
    (6, 'David', 50000, 4)
]

# Define the schema (column names)
columns = ["empid", "ename", "salary", "managerid"]

# Create the DataFrame
employees_tbl = spark.createDataFrame(data, schema=columns)

# Create a temporary view
employees_tbl.createOrReplaceTempView("employees_tbl")
employees_tbl.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.*from employees_tbl a join employees_tbl b
# MAGIC on a.managerid=b.empid
# MAGIC where a.salary > b.salary

# COMMAND ----------

data = [
    ('2023-12-01', 'A', 'A1', 1000),
    ('2023-12-01', 'A', 'A2', 1300),
    ('2023-12-01', 'B', 'B1', 800),
    ('2023-12-02', 'A', 'A1', 1800),
    ('2023-12-02', 'B', 'B1', 900),
    ('2023-12-10', 'A', 'A1', 1400),
    ('2023-12-10', 'A', 'A1', 1200),
    ('2023-12-10', 'C', 'C1', 2500)
]

# Define the schema (column names)
columns = ["dt", "brand", "model", "production_cost"]

# Create the DataFrame
prd_tbl = spark.createDataFrame(data, schema=columns)
prd_tbl.createOrReplaceTempView('prd_tbl')
prd_tbl.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * , sum(production_cost) over (partition by brand, dt order by dt rows between unbounded preceding and current row ) as n from prd_tbl

# COMMAND ----------

data = [
    ('USD', '2024-06-01', 1.20),
    ('USD', '2024-06-02', 1.21),
    ('USD', '2024-06-03', 1.22),
    ('USD', '2024-06-04', 1.23),
    ('USD', '2024-07-01', 1.25),
    ('USD', '2024-07-02', 1.26),
    ('USD', '2024-07-03', 1.27),
    ('EUR', '2024-06-01', 1.40),
    ('EUR', '2024-06-02', 1.41),
    ('EUR', '2024-06-03', 1.42),
    ('EUR', '2024-06-04', 1.43),
    ('EUR', '2024-07-01', 1.45),
    ('EUR', '2024-07-02', 1.46),
    ('EUR', '2024-07-03', 1.47)
]

# Define the schema (column names)
columns = ["currency_code", "date", "currency_exchange_rate"]

# Create the DataFrame
exchange_rates = spark.createDataFrame(data, schema=columns)

# Convert the 'date' column to date type
exchange_rates = exchange_rates.withColumn("date", to_date("date"))

# Create a temporary view
exchange_rates.createOrReplaceTempView("exchange_rates")
exchange_rates.display()

# COMMAND ----------

department_data = [
    (101, 'HR'),
    (102, 'Finance'),
    (103, 'Marketing')
]

# Define the schema for department
department_columns = ["deptid", "deptname"]

# Create the DataFrame for department
department_df = spark.createDataFrame(department_data, schema=department_columns)

# Create a temporary view for department
department_df.createOrReplaceTempView("department")

# Sample data for employee
employee_data = [
    (1, 70000, 101),
    (2, 50000, 101),
    (3, 60000, 101),
    (4, 65000, 102),
    (5, 65000, 102),
    (6, 55000, 102),
    (7, 60000, 103),
    (8, 70000, 103),
    (9, 80000, 103)
]

# Define the schema for employee
employee_columns = ["empid", "salary", "deptid"]

# Create the DataFrame for employee
employee_df = spark.createDataFrame(employee_data, schema=employee_columns)

# Create a temporary view for employee
employee_df.createOrReplaceTempView("employee")

# Show the DataFrames
print("Department Table:")
department_df.show()

print("Employee Table:")
employee_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (
# MAGIC select *, dense_rank() over (partition by deptid order by salary desc) rank from employee) where rank=2

# COMMAND ----------

data = [
    (1, 'John Doe', 'Male'),
    (2, 'Jane Smith', 'Female'),
    (3, 'Michael Johnson', 'Male'),
    (4, 'Emily Davis', 'Female'),
    (5, 'Robert Brown', 'Male'),
    (6, 'Sophia Wilson', 'Female'),
    (7, 'David Lee', 'Male'),
    (8, 'Emma White', 'Female'),
    (9, 'James Taylor', 'Male'),
    (10, 'William Clark', 'Male')
]

# Define the schema (column names)
columns = ["eid", "ename", "gender"]

# Create the DataFrame
employees_tbl = spark.createDataFrame(data, schema=columns)

# Create a temporary view
employees_tbl.createOrReplaceTempView("employees_tbl")

# Show the DataFrame
employees_tbl.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC sum(if(gender='Male',1,0))/count(*) *100 as g_pr,
# MAGIC sum(if(gender='Female',1,0))/count(*) *100 as m_pr
# MAGIC from employees_tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC avg(if(gender='Male',1,0)) *100 as g_pr,
# MAGIC avg(if(gender='Female',1,0)) *100 as m_pr
# MAGIC from employees_tbl

# COMMAND ----------

products_data = [
    (1, 'A', 1000),
    (2, 'B', 400),
    (3, 'C', 500)
]

# Define the schema for products
products_columns = ["pid", "pname", "price"]

# Create the DataFrame for products
products_df = spark.createDataFrame(products_data, schema=products_columns)

# Create a temporary view for products
products_df.createOrReplaceTempView("products")

# Sample data for transactions
transactions_data = [
    (1, '2024-02-01', 2, 2000),
    (1, '2024-03-01', 4, 4000),
    (1, '2024-03-15', 2, 2000),
    (3, '2024-04-24', 3, 1500),
    (3, '2024-05-16', 5, 2500)
]

# Define the schema for transactions
transactions_columns = ["pid", "sold_date", "qty", "amount"]

# Create the DataFrame for transactions
transactions_df = spark.createDataFrame(transactions_data, schema=transactions_columns)

# Convert the 'sold_date' column to date type
transactions_df = transactions_df.withColumn("sold_date", to_date("sold_date"))

# Create a temporary view for transactions
transactions_df.createOrReplaceTempView("transactions")

# Show the DataFrames
print("Products Table:")
products_df.show()

print("Transactions Table:")
transactions_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select pid, year(sold_date), month(sold_date), sum(amount) from transactions group by pid, year(sold_date), month(sold_date)

# COMMAND ----------

# %sql
# WITH  number_range AS (
#     SELECT 1 AS number
#     UNION ALL
#     SELECT number + 1
#     FROM number_range
#     WHERE number < 12
# )
# SELECT number FROM number_range;

# COMMAND ----------

# DBTITLE 1,Find 2nd Wednesday of current month.
# MAGIC %sql
# MAGIC WITH first_day AS (
# MAGIC   SELECT date_trunc('MM', current_date()) AS first_of_month
# MAGIC ),
# MAGIC first_wednesday AS (
# MAGIC   SELECT next_day(first_of_month, 'WEDNESDAY') AS first_wed
# MAGIC FROM first_day
# MAGIC )
# MAGIC SELECT date_add(first_wed, 7) AS second_wednesday
# MAGIC FROM first_wednesday;

# COMMAND ----------

data = [
    ('Bangalore', 'Chennai'),
    ('Chennai', 'Bangalore'),
    ('Pune', 'Chennai'),
    ('Delhi', 'Pune')
]

# Step 3: Define the schema
schema = StructType([
    StructField("Origin", StringType(), True),
    StructField("Destination", StringType(), True)
])

# Step 4: Create the DataFrame
routes_df = spark.createDataFrame(data, schema)

# Show the DataFrame
routes_df.show()
routes_df.createOrReplaceTempView('df')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     LEAST(Origin, Destination) AS Origin,
# MAGIC     GREATEST(Origin, Destination) AS Destination
# MAGIC FROM 
# MAGIC     df
# MAGIC GROUP BY 
# MAGIC     LEAST(Origin, Destination), 
# MAGIC     GREATEST(Origin, Destination);

# COMMAND ----------

data = [
    ('Siva', 1, 30000),
    ('Ravi', 2, 40000),
    ('Prasad', 1, 50000),
    ('Sai', 2, 20000),
    ('Anna', 2, 10000)
]

# Step 3: Define the schema
schema = StructType([
    StructField("emp_name", StringType(), True),
    StructField("dept_id", IntegerType(), True),
    StructField("salary", IntegerType(), True)
])

# Step 4: Create the DataFrame
emps_df = spark.createDataFrame(data, schema)

# Step 5: Create a temporary view
emps_df.createOrReplaceTempView("emps_tbl")
emps_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct dept_id, first_value(emp_name) over(partition by dept_id order by salary ) asval,
# MAGIC first_value(emp_name) over(partition by dept_id order by salary desc ) dsval from emps_tbl 

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (select *, row_number() over (partition by dept_id order by salary) as r_asc,
# MAGIC row_number() over (partition by dept_id order by salary desc ) r_desc from emps_tbl )
# MAGIC select dept_id, max(case when r_asc=1 then emp_name  end) as min_salary_emp_name,
# MAGIC max(case when r_desc=1 then emp_name end) as max_salary_emp_name
# MAGIC
# MAGIC from cte
# MAGIC group by dept_id

# COMMAND ----------

data = [
    (1234567812345678,),
    (2345678923456789,),
    (3456789034567890,)
]

# Step 3: Define the schema
schema = StructType([
    StructField("card_number", StringType(), True)
])

# Step 4: Create the DataFrame
cards_df = spark.createDataFrame(data, schema)

# Step 5: Create a temporary view
cards_df.createOrReplaceTempView("cards")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, concat(   repeat('*' ,length(card_number)-4),  substr((card_number), -4)) from cards

# COMMAND ----------

data = [
    (3, 'Bob', 60000),
    (4, 'Diana', 70000),
    (5, 'Eve', 60000),
    (6, 'Frank', 80000),
    (7, 'Grace', 70000),
    (8, 'Henry', 90000)
]

# Step 3: Define the schema
schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("ename", StringType(), True),
    StructField("salary", IntegerType(), True)
])

# Step 4: Create the DataFrame
employee_df = spark.createDataFrame(data, schema)

# Step 5: Create a temporary view
employee_df.createOrReplaceTempView("Employee")
employee_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Employee t1 , employee t2
# MAGIC where t1.salary=t2.salary and t1.ename != t2.ename

# COMMAND ----------

data = [
    (53151, 'deposit', 178, '2022-07-08'),
    (29776, 'withdrawal', 25, '2022-07-08'),
    (16461, 'withdrawal', 45, '2022-07-08'),
    (19153, 'deposit', 65, '2022-07-10'),
    (77134, 'deposit', 32, '2022-07-10')
]

# Step 3: Define the schema
schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("transaction_date", StringType(), True)
])

# Step 4: Create the DataFrame
transactions_df = spark.createDataFrame(data, schema).withColumn("transaction_date", to_date(col("transaction_date")))
transactions_df.createOrReplaceTempView('df')
transactions_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC %sql
# MAGIC select * , sum(n_amount) over (order by transaction_date rows between unbounded preceding and current row) as sum from(
# MAGIC select * , if(type='withdrawal',-amount, amount) as n_amount from df)

# COMMAND ----------

data = [
    (1, 'SG1234', 'Delhi', 'Hyderabad'),
    (1, 'SG3476', 'Kochi', 'Mangalore'),
    (1, '69876', 'Hyderabad', 'Kochi'),
    (2, '68749', 'Mumbai', 'Varanasi'),
    (2, 'SG5723', 'Varanasi', 'Delhi')
]

# Step 3: Define the schema
schema = StructType([
    StructField("cust_id", IntegerType(), True),
    StructField("flight_id", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("destination", StringType(), True)
])

# Step 4: Create the DataFrame
flights_df = spark.createDataFrame(data, schema)

# Step 5: Create a temporary view
flights_df.createOrReplaceTempView("df")

flights_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte1 as (select df1.cust_id,  df1.origin  from df df1 left join df df2 on df1.cust_id=df2.cust_id and df1.origin=df2.destination 
# MAGIC where df2.destination is null),
# MAGIC cte2 as (select df1.cust_id,  df1.destination  from df df1 left join df df2 on df1.cust_id=df2.cust_id and df1.destination=df2.origin
# MAGIC where df2.destination is null)
# MAGIC
# MAGIC select cte1.*, cte2.destination from cte1 join cte2 on cte1.cust_id=cte2.cust_id
# MAGIC

# COMMAND ----------

from datetime import datetime
data = [
    (201, datetime.strptime('2024-07-11', '%Y-%m-%d').date(), 140),
    (201, datetime.strptime('2024-07-18', '%Y-%m-%d').date(), 160),
    (201, datetime.strptime('2024-07-25', '%Y-%m-%d').date(), 150),
    (201, datetime.strptime('2024-08-01', '%Y-%m-%d').date(), 180),
    (201, datetime.strptime('2024-08-15', '%Y-%m-%d').date(), 170),
    (201, datetime.strptime('2024-08-29', '%Y-%m-%d').date(), 130)
]

# Step 3: Define the schema
schema = StructType([
    StructField("pid", IntegerType(), True),
    StructField("sls_dt", DateType(), True),
    StructField("sls_amt", IntegerType(), True)
])

# Step 4: Create the DataFrame
sls_df = spark.createDataFrame(data, schema)

# Step 5: Create a temporary view
sls_df.createOrReplaceTempView("sls_tbl")
sls_df.display()
# find missing week

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (
# MAGIC select *, case when date_add(sls_dt, 7) != next then date_add(sls_dt, 7)  end as missing_dt from(
# MAGIC select *, lead(sls_dt) over (order by sls_dt) as next from sls_tbl)) where missing_dt is not null

# COMMAND ----------

data = [
    ('1,2',),
    ('3',),
    ('4',)
]

# Step 3: Define the schema
schema = StructType([
    StructField("cola", StringType(), True)
])

# Step 4: Create the DataFrame
testtbl_df = spark.createDataFrame(data, schema)

# Step 5: Create a temporary view
testtbl_df.createOrReplaceTempView("testtbl")
testtbl_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte(select explode(split(cola,",")) as cola from testtbl)
# MAGIC select a.cola as col1, b.cola as col2 from cte a, cte b
# MAGIC where a.cola<b.cola
# MAGIC -- STRING_SPLIT

# COMMAND ----------

data = [
    (1, 2019),
    (1, 2020),
    (1, 2021),
    (2, 2022),
    (2, 2021),
    (3, 2019),
    (3, 2021),
    (3, 2022)
]

# Step 3: Define the schema
schema = StructType([
    StructField("pid", IntegerType(), True),
    StructField("year", IntegerType(), True)
])

# Step 4: Create the DataFrame
events_df = spark.createDataFrame(data, schema)

# Step 5: Create a temporary view
events_df.createOrReplaceTempView("events")

events_df.display()

#  3 Consecutive years

# COMMAND ----------

# MAGIC %sql
# MAGIC select pid, grp from (
# MAGIC select *, row_number() over(partition by pid order by year) as rnum,
# MAGIC year-(row_number() over(partition by pid order by year) ) as grp
# MAGIC  from events ) group by pid, grp having count(*)>=3

# COMMAND ----------

schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("new_price", IntegerType(), True),
    StructField("change_date", StringType(), True)  # Using StringType for date initially
])

# Step 3: Create the data for the DataFrame
data = [
    (1, 20, '2019-08-14'),
    (2, 50, '2019-08-14'),
    (1, 30, '2019-08-15'),
    (1, 35, '2019-08-16'),
    (2, 65, '2019-08-17'),
    (3, 20, '2019-08-18')
]

# Step 4: Create the DataFrame
products_df = spark.createDataFrame(data, schema)

# Step 5: Convert change_date to DateType
products_df = products_df.withColumn("change_date", to_date(products_df.change_date))
products_df.createOrReplaceTempView("Products")
# Step 6: Show the DataFrame
products_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM 
# MAGIC (SELECT product_id, new_price AS price
# MAGIC  FROM Products
# MAGIC  WHERE (product_id, change_date) IN (
# MAGIC                                      SELECT product_id, MAX(change_date)
# MAGIC                                      FROM Products
# MAGIC                                      WHERE change_date <= '2019-08-16'
# MAGIC                                      GROUP BY product_id)
# MAGIC  UNION
# MAGIC  SELECT DISTINCT product_id, 10 AS price
# MAGIC  FROM Products
# MAGIC  WHERE product_id NOT IN (SELECT product_id FROM Products WHERE change_date <= '2019-08-16')
# MAGIC ) tmp
# MAGIC ORDER BY price DESC

# COMMAND ----------

data = [
    (3, 108939),
    (2, 12747),
    (8, 87709),
    (6, 91796)
]

columns = ["account_id", "income"]
accounts_df = spark.createDataFrame(data, columns)
accounts_df.createOrReplaceTempView('Accounts')
accounts_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS (
# MAGIC     SELECT 'Low Salary' AS category
# MAGIC     UNION ALL
# MAGIC     SELECT 'Average Salary'
# MAGIC     UNION ALL 
# MAGIC     SELECT 'High Salary'
# MAGIC ),
# MAGIC cte1 AS (
# MAGIC     SELECT account_id, 
# MAGIC            CASE 
# MAGIC                WHEN income < 20000 THEN 'Low Salary'
# MAGIC                WHEN income >= 20000 AND income <= 50000 THEN 'Average Salary'
# MAGIC                ELSE 'High Salary' 
# MAGIC            END AS category
# MAGIC     FROM Accounts
# MAGIC )
# MAGIC
# MAGIC SELECT cte.category, 
# MAGIC        COUNT(cte1.account_id) AS accounts_count 
# MAGIC FROM cte 
# MAGIC LEFT JOIN cte1 ON cte.category = cte1.category
# MAGIC GROUP BY cte.category
# MAGIC ORDER BY cte.category;

# COMMAND ----------

# DBTITLE 1,Write a solution to report the sum of all total investment values in 2016 tiv_2016, for all policyholders who:  have the same tiv_2015 value as one or more other policyholders, and are not located in the same city as any other policyholder (i.e., the (lat, lon) attribute pairs must be unique). Round tiv_2016 to two decimal places.
schema = StructType([
    StructField("pid", IntegerType(), True),
    StructField("tiv_2015", IntegerType(), True),
    StructField("tiv_2016", IntegerType(), True),
    StructField("lat", IntegerType(), True),
    StructField("lon", IntegerType(), True)
])

# Step 3: Create the data for the DataFrame
data = [
    (1, 10, 5, 10, 10),
    (2, 20, 20, 20, 20),
    (3, 10, 30, 20, 20),
    (4, 10, 40, 40, 40)
]

# Step 4: Create the DataFrame
insurance_df = spark.createDataFrame(data, schema)
insurance_df.createOrReplaceTempView('insurance')
# Step 5: Show the DataFrame
insurance_df.display()
# Write a solution to report the sum of all total investment values in 2016 tiv_2016, for all policyholders who:  have the same tiv_2015 value as one or more other policyholders, and are not located in the same city as any other policyholder (i.e., the (lat, lon) attribute pairs must be unique). Round tiv_2016 to two decimal places.

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(TIV_2016) as TIV_2016
# MAGIC from insurance
# MAGIC where TIV_2015 in
# MAGIC (
# MAGIC   select TIV_2015 
# MAGIC   from insurance
# MAGIC   group by 1
# MAGIC   having count(*) > 1
# MAGIC ) and concat(LAT, LON) in
# MAGIC (
# MAGIC   select concat(LAT,LON) 
# MAGIC   from insurance
# MAGIC   group by LAT, LON
# MAGIC   having count(*) = 1)

# COMMAND ----------

data = [
    (1, 1),
    (2, 1),
    (3, 1),
    (4, 2),
    (5, 1),
    (6, 2),
    (7, 2),
    (6, 2),
    (7, 3),
     (7, 3),
      (7, 2),
]

# Step 3: Define the schema for the DataFrame
columns = ["id", "num"]

# Step 4: Create the DataFrame
num_df = spark.createDataFrame(data, columns)
num_df.createOrReplaceTempView('df')
# Step 5: Show the DataFrame
num_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, lag(num) over (order by (select null)) as prev_num,
# MAGIC lead(num) over (order by (select null)) as next_num from df

# COMMAND ----------

# MAGIC %sql
# MAGIC select id,num from (
# MAGIC select *, lag(num) over (order by (select null)) as prev_num,
# MAGIC lead(num) over (order by (select null)) as next_num from df)
# MAGIC where num=prev_num or num=next_num

# COMMAND ----------

data = [1, 2, 2, 2, 5, 8, 8, 8]
data = [(x,) for x in data] 
df=spark.createDataFrame(data, ["col"])
df.createOrReplaceTempView("df")
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC       SELECT 
# MAGIC         col,
# MAGIC         dense_rank(col) OVER (ORDER BY col) AS rn    FROM df
# MAGIC )
# MAGIC select distinct col from cte where rn in (
# MAGIC select rn from (
# MAGIC cte) group by rn having count (*)>=3)

# COMMAND ----------

data2 = [(123, 'impression', '07/18/2022 11:36:12'),(123, 'impression', '07/18/2022 11:37:12'),(123, 'click', '07/18/2022 11:37:42'),(234, 'impression', '07/18/2022 14:15:12'),(234, 'click', '07/18/2022 14:16:12')]
schema2 = ['app_id','event_type','timestamp']

df=spark.createDataFrame(data2, schema2)
df.createOrReplaceTempView('df')
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(if(event_type='click', 1, 0))/sum(if(event_type='impression', 1, 0)) *100.0 as rate from df group by app_id

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType
from datetime import datetime

# Define the schema
schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("log_date", DateType(), True),
    StructField("flag", StringType(), True)
])

# Sample data
data = [
    (101, datetime.strptime('02-01-2024', '%d-%m-%Y').date(), 'N'),
    (101, datetime.strptime('03-01-2024', '%d-%m-%Y').date(), 'Y'),
    (101, datetime.strptime('04-01-2024', '%d-%m-%Y').date(), 'N'),
    (101, datetime.strptime('07-01-2024', '%d-%m-%Y').date(), 'Y'),
    (102, datetime.strptime('01-01-2024', '%d-%m-%Y').date(), 'N'),
    (102, datetime.strptime('02-01-2024', '%d-%m-%Y').date(), 'Y'),
    (102, datetime.strptime('03-01-2024', '%d-%m-%Y').date(), 'Y'),
    (102, datetime.strptime('04-01-2024', '%d-%m-%Y').date(), 'N'),
    (102, datetime.strptime('05-01-2024', '%d-%m-%Y').date(), 'Y'),
    (102, datetime.strptime('06-01-2024', '%d-%m-%Y').date(), 'Y'),
    (102, datetime.strptime('07-01-2024', '%d-%m-%Y').date(), 'Y'),
    (103, datetime.strptime('01-01-2024', '%d-%m-%Y').date(), 'N'),
    (103, datetime.strptime('04-01-2024', '%d-%m-%Y').date(), 'N'),
    (103, datetime.strptime('05-01-2024', '%d-%m-%Y').date(), 'Y'),
    (103, datetime.strptime('06-01-2024', '%d-%m-%Y').date(), 'Y'),
    (103, datetime.strptime('07-01-2024', '%d-%m-%Y').date(), 'N')
]

# Create a DataFrame
attendance_df = spark.createDataFrame(data, schema)
attendance_df.createOrReplaceTempView('attendance_df')
# Show the DataFrame
attendance_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (
# MAGIC select emp_id, r , min(log_date) as start_date, max(log_date) as end_date , count(*) as cnt from (
# MAGIC select *, (dayofmonth(log_date) - rnum) r from (
# MAGIC select *, row_number() over (partition by emp_id order by log_date) as rnum from attendance_df where flag='Y') ) group by 1, 2)
# MAGIC where cnt>=2
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 1 AS number UNION ALL
# MAGIC SELECT 2 UNION ALL
# MAGIC SELECT 3 UNION ALL
# MAGIC SELECT 4 UNION ALL
# MAGIC SELECT 5 UNION ALL
# MAGIC SELECT 6 UNION ALL
# MAGIC SELECT 7 UNION ALL
# MAGIC SELECT 8 UNION ALL
# MAGIC SELECT 9 UNION ALL
# MAGIC SELECT 10;

# COMMAND ----------

# Create the data
data = [
    ('Moh98765i67t123',),
    ('Rah88765ul67123',),
    ('Roh99965a6n7123',),
    ('Rav9663567123i',),
    ('Pr987a00ta6712p3',),
    ('Sa9n86ja51071y23',),
    ('Rav907510i7123',),
    ('Go995pa10l07123',),
    ('Vik94r1a200m7650',),
    ('Ste99v7e6120088',)
]

# Create the DataFrame
candidate_info_df = spark.createDataFrame(data, ["NamePhoneDetails"])

candidate_info_df.withColumn("name", regexp_replace(col('NamePhoneDetails'), r"[^a-zA-z]", "")).display()

# COMMAND ----------

# Create the data
data = [
    ('India',),
    ('Pakistan',),
    ('Australia',),
    ('Afghanistan',)
]

# Create the DataFrame
teams_df = spark.createDataFrame(data, ["Team_Names"])
teams_df.createOrReplaceTempView('teams_df')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from teams_df a, teams_df b
# MAGIC where a.Team_Names != b.Team_Names and 
# MAGIC a.Team_Names < b.Team_Names

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType
from datetime import datetime

# Define the schema
schema = StructType([
    StructField("Emp_Id", IntegerType(), True),
    StructField("date_value", DateType(), True),
    StructField("Attendance", StringType(), True)
])

# Create the data
data = [
    (1, datetime.strptime('2022-01-01', '%Y-%m-%d').date(), 'present'),
    (1, datetime.strptime('2022-01-02', '%Y-%m-%d').date(), 'present'),
    (1, datetime.strptime('2022-01-03', '%Y-%m-%d').date(), 'present'),
    (1, datetime.strptime('2022-01-04', '%Y-%m-%d').date(), 'absent'),
    (1, datetime.strptime('2022-01-05', '%Y-%m-%d').date(), 'absent'),
    (1, datetime.strptime('2022-01-06', '%Y-%m-%d').date(), 'present'),
    (2, datetime.strptime('2022-01-01', '%Y-%m-%d').date(), 'present'),
    (2, datetime.strptime('2022-01-02', '%Y-%m-%d').date(), 'present'),
    (2, datetime.strptime('2022-01-03', '%Y-%m-%d').date(), 'absent'),
    (2, datetime.strptime('2022-01-04', '%Y-%m-%d').date(), 'absent'),
    (2, datetime.strptime('2022-01-05', '%Y-%m-%d').date(), 'absent'),
    (2, datetime.strptime('2022-01-06', '%Y-%m-%d').date(), 'present')
]

# Create the DataFrame
attendance_log_df = spark.createDataFrame(data, schema)
attendance_log_df.createOrReplaceTempView('df')
attendance_log_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select Emp_id, nm,Attendance,  min(date_value) as startDate, max(date_value) as endDate
# MAGIC  from(
# MAGIC select *, dayofmonth(date_value)-rn as nm from(
# MAGIC select * , row_number() over (partition by Emp_id, Attendance order by date_value) as rn from df)) group by 1, nm, Attendance

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType

# Define the schema
schema = StructType([
    StructField("CollegeId", IntegerType(), True),
    StructField("StudentId", IntegerType(), True),
    StructField("DeptId", IntegerType(), True)
])

# Create the data
data = [
    (1, 1, 101),
    (1, 2, None),  # NULL in SQL corresponds to None in Python
    (1, 3, None),
    (1, 4, None),
    (1, 11, 201),
    (1, 12, None),
    (1, 13, None),
    (1, 14, None)
]

# Create the DataFrame
student_detail_df = spark.createDataFrame(data, schema)
student_detail_df.display()
student_detail_df.createOrReplaceTempView("StudentDetail")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * , first_value(DeptId) over (partition by CollegeId,s order by StudentId) as k from(
# MAGIC select * , count(DeptId) over (partition by CollegeId order by StudentId) as s from StudentDetail)

# COMMAND ----------

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("CityName", StringType(), True)
])

# Create the data
data = [
    (1, 'Delhi'),
    (2, 'Mumbai'),
    (3, 'Kolkata'),
    (4, 'Chennai'),
    (5, 'Hyderabad')
]
city_df = spark.createDataFrame(data, schema)

city_df.show()
city_df.createOrReplaceTempView("City")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from City c1, City c2
# MAGIC where c1.id!=c2.id
# MAGIC and c1.id < c2.id

# COMMAND ----------

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("price", StringType(), True)
])

# Create the data
data = [
    (1, 'Product A', 10.99),
    (2, 'Product B', 15.49),
    (4, 'Product D', 8.79),
    (5, 'Product E', 12.99),
    (6, 'Product F', 18.99),
    (7, 'Product G', 22.49),
    (9, 'Product I', 14.29),
    (10, 'Product J', 9.99)
]

# Create the DataFrame
products_df = spark.createDataFrame(data, schema)
products_df.show()
products_df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC select case when(id+1 not in (select id from products) and id != (select max(id) from products)) then id+1 end as missing_id
# MAGIC from products p

# COMMAND ----------

from datetime import datetime

# Define the schema
schema = StructType([
    StructField("TransactionID", IntegerType(), True),
    StructField("TransactionDate", DateType(), True),
    StructField("Amount", StringType(), True),
    StructField("TransactionType", StringType(), True)
])

# Create the data
data = [
    (1, datetime.strptime('2024-03-01', '%Y-%m-%d').date(), 100.00, 'Credit'),
    (2, datetime.strptime('2024-03-02', '%Y-%m-%d').date(), 50.00, 'Debit'),
    (3, datetime.strptime('2024-03-02', '%Y-%m-%d').date(), 200.00, 'Credit'),
    (4, datetime.strptime('2024-03-02', '%Y-%m-%d').date(), 75.00, 'Debit'),
    (5, datetime.strptime('2024-03-05', '%Y-%m-%d').date(), 150.00, 'Credit'),
    (6, datetime.strptime('2024-03-05', '%Y-%m-%d').date(), 90.00, 'Debit'),
    (7, datetime.strptime('2024-03-09', '%Y-%m-%d').date(), 200.00, 'Credit'),
    (8, datetime.strptime('2024-03-10', '%Y-%m-%d').date(), 100.00, 'Debit'),
    (9, datetime.strptime('2024-03-12', '%Y-%m-%d').date(), 150.00, 'Credit'),
    (10, datetime.strptime('2024-03-12', '%Y-%m-%d').date(), 90.00, 'Debit'),
    (11, datetime.strptime('2024-03-12', '%Y-%m-%d').date(), 200.00, 'Credit'),
    (12, datetime.strptime('2024-03-12', '%Y-%m-%d').date(), 100.00, 'Debit')
]

# Create the DataFrame
transactions_df = spark.createDataFrame(data, schema)
transactions_df.show()
transactions_df.createOrReplaceTempView("Transactions")

# COMMAND ----------

# MAGIC %sql
# MAGIC select TransactionDate, sum(amt) over (order by TransactionDate) from (
# MAGIC select TransactionDate, sum(if(TransactionType='Credit', Amount, -Amount)) amt from Transactions
# MAGIC group by 1)

# COMMAND ----------

schema = StructType([
    StructField("point_id", IntegerType(), True),
    StructField("team", IntegerType(), True),
    StructField("opponent", IntegerType(), True),
    StructField("winner", IntegerType(), True)
])

# Data to insert (all integers)
data = [
    (1, 1, 2, 1),
    (2, 2, 3, 3),
    (3, 4, 5, 4),
    (4, 3, 1, 1)
]

# Create a new DataFrame with the data
new_points_df = spark.createDataFrame(data, schema)
new_points_df.createOrReplaceTempView('df')
# Show the new DataFrame
new_points_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select team , count(team) as match_played, sum(flag) as match_won,( count(team)-sum(flag)) match_lost from (
# MAGIC select team,  if(team=winner, 1, 0) flag from df
# MAGIC union all
# MAGIC select opponent as team,  if(opponent=winner, 1, 0) flag from df)
# MAGIC
# MAGIC group by 1

# COMMAND ----------

# Define the schema
schema = StructType([
    StructField("orderDate", DateType(), True),
    StructField("customer", StringType(), True),
    StructField("amount", IntegerType(), True)
])

# Data to insert
data = [
    (datetime.strptime('2024-01-01', '%Y-%m-%d').date(), 'Customer_1', 200),
    (datetime.strptime('2024-01-01', '%Y-%m-%d').date(), 'Customer_2', 300),
    (datetime.strptime('2024-02-01', '%Y-%m-%d').date(), 'Customer_1', 100),
    (datetime.strptime('2024-02-01', '%Y-%m-%d').date(), 'Customer_3', 105),
    (datetime.strptime('2024-03-01', '%Y-%m-%d').date(), 'Customer_5', 109),
    (datetime.strptime('2024-03-01', '%Y-%m-%d').date(), 'Customer_4', 100),
    (datetime.strptime('2024-04-01', '%Y-%m-%d').date(), 'Customer_3', 103),
    (datetime.strptime('2024-04-01', '%Y-%m-%d').date(), 'Customer_5', 105),
    (datetime.strptime('2024-04-01', '%Y-%m-%d').date(), 'Customer_6', 100)
]

# Create a DataFrame with the data
orders_df = spark.createDataFrame(data, schema)
orders_df.createOrReplaceTempView("orders")
# Show the DataFrame
orders_df.display()
# https://www.youtube.com/watch?v=rMVEiThgSvY&list=PLP1_hACWUYHyUapnM-HgBOm7ACgW15Cm6&index=11

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT orderDate, sum(flag) from (
# MAGIC SELECT *,
# MAGIC     CASE 
# MAGIC         WHEN orderDate = (SELECT MIN(orderDate) FROM orders) THEN 1
# MAGIC         WHEN customer IN (SELECT DISTINCT customer FROM orders WHERE orderDate < p.orderDate) THEN 0 
# MAGIC         ELSE 1
# MAGIC     END AS flag
# MAGIC FROM orders p) GROUP BY 1

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DateType
from datetime import datetime

# Define the schema
schema = StructType([
    StructField("OrderID", IntegerType(), True),
    StructField("OrderDate", DateType(), True),
    StructField("CustomerID", IntegerType(), True),
    StructField("OrderAmount", IntegerType(), True)
])

# Data to insert
data = [
    (100, datetime.strptime('2024-04-10', '%Y-%m-%d').date(), 1, 200),
    (101, datetime.strptime('2024-04-10', '%Y-%m-%d').date(), 2, 200),
    (102, datetime.strptime('2024-04-10', '%Y-%m-%d').date(), 3, 500),
    (103, datetime.strptime('2024-04-11', '%Y-%m-%d').date(), 3, 700),
    (104, datetime.strptime('2024-04-11', '%Y-%m-%d').date(), 4, 900),
    (105, datetime.strptime('2024-04-11', '%Y-%m-%d').date(), 5, 1000),
    (106, datetime.strptime('2024-04-11', '%Y-%m-%d').date(), 1, 2300),
    (107, datetime.strptime('2024-04-11', '%Y-%m-%d').date(), 2, 2400),
    (108, datetime.strptime('2024-04-12', '%Y-%m-%d').date(), 6, 1500),
    (109, datetime.strptime('2024-04-12', '%Y-%m-%d').date(), 7, 900),
    (110, datetime.strptime('2024-04-12', '%Y-%m-%d').date(), 1, 100),
    (111, datetime.strptime('2024-04-12', '%Y-%m-%d').date(), 2, 200)
]

# Create a DataFrame with the data
orders_df = spark.createDataFrame(data, schema)
orders_df.createOrReplaceTempView("Orders")

# Show the DataFrame
orders_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select OrderDate, sum(new_cutomers) as n, sum(old_cutomera) as o from (
# MAGIC select *,  CASE 
# MAGIC         WHEN orderDate = (SELECT MIN(orderDate) FROM orders) THEN 1
# MAGIC         when CustomerID IN (SELECT DISTINCT CustomerID FROM orders WHERE orderDate < p.orderDate) THEN 0 
# MAGIC         else 1 end as new_cutomers,
# MAGIC        CASE 
# MAGIC         WHEN orderDate = (SELECT MIN(orderDate) FROM orders) THEN 0
# MAGIC         when CustomerID IN (SELECT DISTINCT CustomerID FROM orders WHERE orderDate < p.orderDate) THEN 1
# MAGIC         else 0 end as old_cutomera
# MAGIC         from Orders p) group by 1

# COMMAND ----------

schema = StructType([
    StructField("category", StringType(), True),
    StructField("product_name", StringType(), True)
])

# Data to insert
data = [
    ('Chocolate', 'KitKat'),
    (None, 'Snickers'),
    (None, 'Cadbury'),
    (None, 'Ferrero Rocher'),
    ('Biscuits', 'Parle-G'),
    (None, "McVitie's"),
    (None, 'Oreo'),
    (None, 'Hide & Seek'),
    ('Soft Drinks', 'Coca-Cola'),
    (None, 'Pepsi'),
    (None, 'Sprite'),
    (None, 'Fanta'),
    ('Electronics', 'Samsung'),
    (None, 'Apple'),
    (None, 'Sony')
]

# Create a DataFrame with the data
product_df = spark.createDataFrame(data, schema)
product_df.createOrReplaceTempView("product")
# Show the DataFrame
product_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,  first_value(category) over(partition by cnt order by null) from ( 
# MAGIC select *, count(category) over (order by rn) as cnt from (
# MAGIC select * , row_number() over (order by (select null)) rn from product))
# MAGIC

# COMMAND ----------

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", DateType(), True)
])

# Data to insert into ORDERS
orders_data = [
    (101, 1, datetime.strptime('2024-01-01', '%Y-%m-%d').date()),
    (102, 1, datetime.strptime('2024-01-02', '%Y-%m-%d').date()),
    (103, 1, datetime.strptime('2024-01-03', '%Y-%m-%d').date()),
    (104, 1, datetime.strptime('2024-01-03', '%Y-%m-%d').date()),
    (105, 1, datetime.strptime('2024-01-04', '%Y-%m-%d').date()),
    (106, 1, datetime.strptime('2024-01-05', '%Y-%m-%d').date()),
    (107, 1, datetime.strptime('2024-01-07', '%Y-%m-%d').date()),
    (201, 2, datetime.strptime('2024-01-01', '%Y-%m-%d').date()),
    (202, 2, datetime.strptime('2024-01-02', '%Y-%m-%d').date()),
    (203, 2, datetime.strptime('2024-01-03', '%Y-%m-%d').date()),
    (204, 2, datetime.strptime('2024-01-05', '%Y-%m-%d').date()),
    (205, 2, datetime.strptime('2024-01-06', '%Y-%m-%d').date()),
    (301, 3, datetime.strptime('2024-01-01', '%Y-%m-%d').date()),
    (302, 3, datetime.strptime('2024-01-02', '%Y-%m-%d').date()),
    (303, 3, datetime.strptime('2024-01-04', '%Y-%m-%d').date()),
    (304, 3, datetime.strptime('2024-01-05', '%Y-%m-%d').date()),
    (401, 4, datetime.strptime('2024-01-01', '%Y-%m-%d').date()),
    (402, 4, datetime.strptime('2024-01-02', '%Y-%m-%d').date()),
    (403, 4, datetime.strptime('2024-01-03', '%Y-%m-%d').date()),
    (404, 4, datetime.strptime('2024-01-04', '%Y-%m-%d').date()),
    (405, 4, datetime.strptime('2024-01-05', '%Y-%m-%d').date())
]

# Create a DataFrame for ORDERS
orders_df = spark.createDataFrame(orders_data, orders_schema)
orders_df.display()
orders_df.createOrReplaceTempView('df')
# https://www.youtube.com/watch?v=KhqDf2bhXds&list=PLP1_hACWUYHyUapnM-HgBOm7ACgW15Cm6&index=18

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id, nct , count(*) from (
# MAGIC select distinct customer_id, order_date, date_sub(order_date, rnk)  as nct from (
# MAGIC select * , dense_rank(order_date) over (partition by customer_id order by order_date) as rnk from df)) group by 1, 2

# COMMAND ----------


