-- Databricks notebook source
select * from marks_csv

-- COMMAND ----------

select Education,count(*)
from marks_csv
group by Education

-- COMMAND ----------

select Year,Education,avg(age)
from marks_csv
group  by Year,Education

-- COMMAND ----------

select * from uspopulation1_csv

-- COMMAND ----------

select City,State_Code
from uspopulation1_csv
order by 2019_estimate desc

-- COMMAND ----------

select City,max(2019_estimate) as f
from uspopulation1_csv
group by City
order by f desc

-- COMMAND ----------

select * from marks_csv

-- COMMAND ----------

UPDATE marks_csv
SET Age=35
WHERE Name = 'Bill'


-- COMMAND ----------

select marks_csv.Name, marks_1_csv.Age 
from marks_csv
inner Join marks_1_csv
on marks_csv.Name==marks_1_csv.Name

-- COMMAND ----------

select  t2.Name, t1.Name
from marks_csv as t1,marks_csv as t2
where t1.Year==t2.Year


-- COMMAND ----------

select * from marks_csv
union all
select * from marks_csv

-- COMMAND ----------

select * into marks_1_csv
from marks_csv;

-- COMMAND ----------

insert into marks_1_csv (Name,Age,Education,Year)
select * from marks_csv

-- COMMAND ----------

select Name,Age
case
    when Age>30 'Higher Age',
    when Age<30 'Lower Age'
End AS QuantityText
from marks_csv

-- COMMAND ----------

create PROCEDURE n1
as
select * from marks_csv
go;

-- COMMAND ----------


