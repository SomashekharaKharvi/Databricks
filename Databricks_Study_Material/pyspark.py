# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window

# COMMAND ----------

data = [
    (1001, "Satılmış", "İdari", 4000),
    (1002, "Özge", "Personel", 3000),
    (1003, "Hüsnü", "Bilgi Sistemleri", 4000),
    (1004, "Menşure", "Muhasebe", 6500),
    (1005, "Doruk", "Personel", 3000),
    (1006, "Şilan", "Muhasebe", 5000),
    (1007, "Baran", "Personel", 7000),
    (1008, "Ülkü", "İdari", 4000),
    (1009, "Cüneyt", "Bilgi Sistemleri", 6500),
    (1010, "Gülşen", "Bilgi Sistemleri", 7000),
    (1011, "Melih", "Bilgi Sistemleri", 8000),
    (1012, "Gülbahar", "Bilgi Sistemleri", 10000),
    (1013, "Tuna", "İdari", 2000),
    (1014, "Raşel", "Personel", 3000),
    (1015, "Şahabettin", "Bilgi Sistemleri", 4500),
    (1016, "Elmas", "Muhasebe", 6500),
    (1017, "Ahmet Hamdi", "Personel", 3500),
    (1018, "Leyla", "Muhasebe", 5500),
    (1019, "Cuma", "Personel", 8000),
    (1020, "Yelda", "İdari", 5000),
    (1021, "Rojda", "Bilgi Sistemleri", 6000),
    (1022, "İbrahim", "Bilgi Sistemleri", 8000),
    (1023, "Davut", "Bilgi Sistemleri", 8000),
    (1024, "Arzu", "Bilgi Sistemleri", 11000)
]

# Convert the list of tuples to a DataFrame
df = spark.createDataFrame(data, ["id", "name", "dept", "salary"])

# Create or replace a temporary view
df.createOrReplaceTempView("employee")


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import Window

# COMMAND ----------

windowSpec=Window.partitionBy("dept").orderBy(col("salary").desc())

# COMMAND ----------

df.withColumn("rn", row_number().over(windowSpec)).display()

# COMMAND ----------

data = [("Alice Smith",), ("Bob Johnson",), ("Cathy Brown",)]
df = spark.createDataFrame(data, ["full_name"])

# Show the original DataFrame
df.display()

# COMMAND ----------

split_col = split(col('full_name'), ' ')
display(split_col)

# COMMAND ----------

# Sample DataFrame with more complex names
data_complex = [("Alice Marie Smith",), ("Bob Johnson",), ("Cathy Ann Brown",)]
df_complex = spark.createDataFrame(data_complex, ["full_name"])
df_complex.display()
# Split the 'full_name' column into an array
split_col_complex = split(df_complex['full_name'], ' ')
df_temp=df_complex.withColumn("new", split(col("full_name"), " "))
df_temp.select(col("new").getItem(0).alias("first_name"),col("new").getItem(1).alias("middle_name"),col("new").getItem(2).alias("middle_name") ).display()
# Create new columns for first name and last name (last name as all parts after the first)
df_complex_split = df_complex.withColumn('first_name', split_col_complex.getItem(0)) \
                              .withColumn('last_name', 
                                          concat_ws(' ', split_col_complex.getItem(1), 
                                                      *[split_col_complex.getItem(i) for i in range(2, 3)]))

# Show the resulting DataFrame
df_complex_split.show()

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

# Define the schema with an array type column
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("FavoriteNumbers", ArrayType(IntegerType()), True)
])

# Create sample data
data = [
    Row("Alice", [1, 2, 3]),
    Row("Bob", [4, 5]),
    Row("Cathy", [6, 7, 8, 9])
]

# Create DataFrame with the specified schema
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show(truncate=False)

# COMMAND ----------

df.withColumn("X", col("FavoriteNumbers").getItem(0)).display()

# COMMAND ----------

df.withColumn("X", explode("FavoriteNumbers")).display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Array Structure DataFrame Example") \
    .getOrCreate()

# Create sample data
array_structure_data = [
    Row("James,,Smith", ["Java", "Scala", "C++"], ["Spark", "Java"], "OH", "CA"),
    Row("Michael,Rose,", ["Spark", "Java", "C++"], ["Spark", "Java"], "NY", "NJ"),
    Row("Robert,,Williams", ["CSharp", "VB"], ["Spark", "Python"], "UT", "NV")
]

# Define the schema
array_structure_schema = StructType([
    StructField("name", StringType(), True),
    StructField("languagesAtSchool", ArrayType(StringType()), True),
    StructField("languagesAtWork", ArrayType(StringType()), True),
    StructField("currentState", StringType(), True),
    StructField("previousState", StringType(), True)
])

# Create DataFrame
df = spark.createDataFrame(array_structure_data, schema=array_structure_schema)

# Print schema and show DataFrame
df.printSchema()
df.display(truncate=False)

# COMMAND ----------

df.withColumn("x", split("name",",")).display()

# COMMAND ----------

df.withColumn("x", array("currentState","previousState")).display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Complex DataFrame Example") \
    .getOrCreate()

# Create sample data
array_structure_data = [
    Row("James", [Row("Newark", "NY"), Row("Brooklyn", "NY")],
        {"hair": "black", "eye": "brown"}, {"height": "5.9"}),
    Row("Michael", [Row("SanJose", "CA"), Row("Sandiago", "CA")],
        {"hair": "brown", "eye": "black"}, {"height": "6"}),
    Row("Robert", [Row("LasVegas", "NV")],
        {"hair": "red", "eye": "gray"}, {"height": "6.3"}),
    Row("Maria", None,
        {"hair": "blond", "eye": "red"}, {"height": "5.6"}),
    Row("Jen", [Row("LAX", "CA"), Row("Orange", "CA")],
        {"white": "black", "eye": "black"}, {"height": "5.2"})
]

# Define the schema
array_structure_schema = StructType([
    StructField("name", StringType(), True),
    StructField("addresses", ArrayType(StructType([
        StructField("city", StringType(), True),
        StructField("state", StringType(), True)
    ])), True),
    StructField("properties", MapType(StringType(), StringType()), True),
    StructField("secondProp", MapType(StringType(), StringType()), True)
])

# Create DataFrame
map_type_df = spark.createDataFrame(array_structure_data, schema=array_structure_schema)

# Print schema and show DataFrame
map_type_df.printSchema()
map_type_df.display()

# COMMAND ----------

map_type_df.withColumn("x", map_keys(col("properties"))).display()
map_type_df.withColumn("x", map_values(col("properties"))).display()

# COMMAND ----------

map_type_df.withColumn("x", col("properties").getItem('hair')).display()

# COMMAND ----------

map_type_df.withColumn("x", col("secondProp").getItem('height').cast("int")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC How to create an Array (ArrayType) column on DataFrame
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, StructType

# Define the schema for the struct
address_schema = StructType([
    StructField("city", StringType(), True),
    StructField("state", StringType(), True)
])

# Define the schema for the main DataFrame
schema = StructType([
    StructField("name", StringType(), True),
    StructField("addresses", ArrayType(address_schema), True)  # Array of structs
])

# COMMAND ----------

# Create sample data
data = [
    ("James", [{"city": "Newark", "state": "NY"}, {"city": "Brooklyn", "state": "NY"}]),
    ("Michael", [{"city": "SanJose", "state": "CA"}, {"city": "Sandiago", "state": "CA"}]),
    ("Robert", [{"city": "LasVegas", "state": "NV"}]),
    ("Maria", None),  # Example with null array
    ("Jen", [{"city": "LAX", "state": "CA"}, {"city": "Orange", "state": "CA"}])

]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.display(truncate=False)

# COMMAND ----------

df.select( col("addresses").getItem(0).getItem('city')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC How to create a Map (MapType) column on DataFrame

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, MapType

# Define the schema for the DataFrame
schema = StructType([
    StructField("name", StringType(), True),
    StructField("attributes", MapType(StringType(), StringType()), True)  # MapType column
])

# Create sample data
data = [
    ("James", {"age": "30", "city": "Newark"}),
    ("Michael", {"age": "25", "city": "San Jose"}),
    ("Robert", {"age": "28", "city": "Las Vegas"}),
    ("Maria", None),  # Example with null map
    ("Jen", {"age": "22", "city": "Los Angeles"})
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.display(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC How to create an Array of struct column

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Define the schema for the struct
address_schema = StructType([
    StructField("city", StringType(), True),
    StructField("state", StringType(), True)
])

# Define the schema for the main DataFrame
schema = StructType([
    StructField("name", StringType(), True),
    StructField("addresses", ArrayType(address_schema), True)  # Array of structs
])

# Create sample data
data = [
    ("James", [{"city": "Newark", "state": "NY"}, {"city": "Brooklyn", "state": "NY"}]),
    ("Michael", [{"city": "SanJose", "state": "CA"}, {"city": "Sandiago", "state": "CA"}]),
    ("Robert", [{"city": "LasVegas", "state": "NV"}]),
    ("Maria", None),  # Example with null array
    ("Jen", [{"city": "LAX", "state": "CA"}, {"city": "Orange", "state": "CA"}])
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.display(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC How to convert an Array to columns

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Define the schema for the DataFrame
schema = StructType([
    StructField("name", StringType(), True),
    StructField("tags", ArrayType(StringType()), True)  # Array of strings
])

# Create sample data
data = [
    ("James", ["A", "B", "C"]),
    ("Michael", ["D", "E"]),
    ("Robert", ["F", "G", "H", "I"]),
    ("Maria", None),  # Example with null array
    ("Jen", ["J"])
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the original DataFrame
df.display(truncate=False)

from pyspark.sql.functions import col

# Get the maximum length of the array to create enough columns
max_length = df.selectExpr("max(size(tags))").first()[0]

# Create new columns for each element in the array
for i in range(max_length):
    df = df.withColumn(f"tag_{i+1}", col("tags").getItem(i))

# Show the resulting DataFrame
df.display(truncate=False)



# COMMAND ----------

# MAGIC %md
# MAGIC How to explode an Array and map columns

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType

# Define schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("addresses", ArrayType(MapType(StringType(), StringType())), True),  # Array of maps
    StructField("properties", MapType(StringType(), StringType()), True)  # Map column
])

# Create sample data
data = [
    ("James", [{"city": "Newark", "state": "NY"}, {"city": "Brooklyn", "state": "NY"}],
     {"eye": "brown", "hair": "black"}),
    ("Michael", [{"city": "SanJose", "state": "CA"}, {"city": "Sandiago", "state": "CA"}],
     {"eye": "black", "hair": "brown"}),
    ("Robert", [{"city": "LasVegas", "state": "NV"}],
     {"eye": "gray", "hair": "red"}),
    ("Maria", None, {"eye": "red", "hair": "blond"}),
    ("Jen", [{"city": "LAX", "state": "CA"}, {"city": "Orange", "state": "CA"}],
     {"eye": "black", "hair": "blond"})
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the original DataFrame
df.display()

# COMMAND ----------

from pyspark.sql.functions import explode

# Explode the 'addresses' array column
df_exploded = df.withColumn("address", explode("addresses"))

# Show the exploded DataFrame
df_exploded.display()

# COMMAND ----------

df_exploded.select("properties.eye").display()

# COMMAND ----------

# Accessing map values from the exploded DataFrame
df_final = df_exploded.select(
    "name",
    "address.city",
    "address.state",
    "properties.eye",
    "properties.hair"
)

# Show the final DataFrame
df_final.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC How to explode an Array of structs

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Define the schema for the struct
address_schema = StructType([
    StructField("city", StringType(), True),
    StructField("state", StringType(), True)
])

# Define the schema for the main DataFrame
schema = StructType([
    StructField("name", StringType(), True),
    StructField("addresses", ArrayType(address_schema), True)  # Array of structs
])

# Create sample data
data = [
    ("James", [{"city": "Newark", "state": "NY"}, {"city": "Brooklyn", "state": "NY"}]),
    ("Michael", [{"city": "SanJose", "state": "CA"}, {"city": "Sandiago", "state": "CA"}]),
    ("Robert", [{"city": "LasVegas", "state": "NV"}]),
    ("Maria", None),  # Example with null array
    ("Jen", [{"city": "LAX", "state": "CA"}, {"city": "Orange", "state": "CA"}])
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the original DataFrame
df.display(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import explode

# Explode the 'addresses' array column
df_exploded = df.withColumn("address", explode("addresses"))

# Show the exploded DataFrame
df_exploded.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import explode

# Explode the 'addresses' array column
df_exploded1 = df_exploded.select( "address.*")

# Show the exploded DataFrame
df_exploded1.show(truncate=False)

# COMMAND ----------

# Accessing fields from the exploded DataFrame
df_final = df_exploded.select(
    "name",
    "address.city",
    "address.state"
)

# Show the final DataFrame
df_final.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC How to create a DataFrame with nested Array

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Define the schema for the inner array (e.g., a list of strings)
inner_array_schema = ArrayType(StringType())

# Define the schema for the main DataFrame
schema = StructType([
    StructField("name", StringType(), True),
    StructField("nested_array", ArrayType(inner_array_schema), True)  # Nested array
])
# Create sample data
data = [
    ("James", [["A", "B"], ["C", "D"]]),
    ("Michael", [["E", "F"], ["G"]]),
    ("Robert", [["H", "I", "J"]]),
    ("Maria", None),  # Example with null nested array
    ("Jen", [["K"], ["L", "M"]])
]
# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show(truncate=False)

# COMMAND ----------

df.withColumn("X", explode("nested_array")).select('name','X').groupBy('name').agg(collect_list('X')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC How to flatten nested Array to single Array

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Define the schema for the nested array
schema = StructType([
    StructField("name", StringType(), True),
    StructField("nested_array", ArrayType(ArrayType(StringType())), True)  # Nested array
])

# Create sample data
data = [
    ("James", [["A", "B"], ["C", "D"]]),
    ("Michael", [["E", "F"], ["G"]]),
    ("Robert", [["H", "I", "J"]]),
    ("Maria", None),  # Example with null nested array
    ("Jen", [["K"], ["L", "M"]])
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the original DataFrame
df.show(truncate=False)

from pyspark.sql.functions import explode, col, collect_list

# Explode the nested array
df_exploded = df.withColumn("inner_array", explode("nested_array"))

# Flatten the inner arrays into a single array
df_flattened = df_exploded.select("name", explode("inner_array").alias("element")) \
                           .groupBy("name") \
                           .agg(collect_list("element").alias("flattened_array"))

# Show the flattened DataFrame
df_flattened.show(truncate=False)



# COMMAND ----------

df1=df.withColumn("new", explode(col('nested_array')))
df1.select( '*', explode(col('new'))).groupBy('name').agg(collect_list('col').alias('n')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Convert array of String to a String column

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Define the schema for the DataFrame
schema = StructType([
    StructField("name", StringType(), True),
    StructField("tags", ArrayType(StringType()), True)  # Array of strings
])

# Create sample data
data = [
    ("James", ["A", "B", "C"]),
    ("Michael", ["D", "E"]),
    ("Robert", ["F", "G", "H", "I"]),
    ("Maria", None),  # Example with null array
    ("Jen", ["J"])
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the original DataFrame
df.display(truncate=False)

from pyspark.sql.functions import concat_ws

# Convert the array of strings to a single string column
df_converted = df.withColumn("tags_string", concat_ws(", ", "tags"))

# Show the resulting DataFrame
df_converted.display()

# COMMAND ----------

# MAGIC %md
# MAGIC How to flatten nested column

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder \
    .appName("Flatten Nested Columns Example") \
    .getOrCreate()

# Sample data with nested structure
data = [
    (1, "Alice", {"age": 30, "city": "New York"}, ["Math", "Science"]),
    (2, "Bob", {"age": 25, "city": "Los Angeles"}, ["History", "Art"]),
]

# Define the schema
schema = "id INT, name STRING, info STRUCT<age: INT, city: STRING>, subjects ARRAY<STRING>"

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the original DataFrame
print("Original DataFrame:")
df.display()

# COMMAND ----------

df.select('id','name','info.age', 'info.city', explode('subjects')).display()

# COMMAND ----------

df.select('id','name','info.age', 'info.city', explode('subjects')).groupBy('id','name','age','city').agg(collect_list('col')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC CSV column with JSON Value

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a Spark session
spark = SparkSession.builder \
    .appName("Read JSON from DataFrame") \
    .getOrCreate()

# Sample data as a list of tuples
data = [
    (1, '{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}'),
    (2, '{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PASEO COSTA DEL SUR","State":"PR"}'),
    (3, '{"Zipcode":709,"ZipCodeType":"STANDARD","City":"BDA SAN LUIS","State":"PR"}'),
    (4, '{"Zipcode":76166,"ZipCodeType":"UNIQUE","City":"CINGULAR WIRELESS","State":"TX"}'),
    (5, '{"Zipcode":76177,"ZipCodeType":"STANDARD","City":"FORT WORTH","State":"TX"}'),
    (6, '{"Zipcode":76177,"ZipCodeType":"STANDARD","City":"FT WORTH","State":"TX"}'),
    (7, '{"Zipcode":704,"ZipCodeType":"STANDARD","City":"URB EUGENE RICE","State":"PR"}'),
    (8, '{"Zipcode":85209,"ZipCodeType":"STANDARD","City":"MESA","State":"AZ"}'),
    (9, '{"Zipcode":85210,"ZipCodeType":"STANDARD","City":"MESA","State":"AZ"}'),
    (10, '{"Zipcode":32046,"ZipCodeType":"STANDARD","City":"HILLIARD","State":"FL"}')
]

# Define the schema for the DataFrame
schema = StructType([
    StructField("Id", IntegerType(), True),
    StructField("JsonValue", StringType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the original DataFrame
print("Original DataFrame:")
df.display()

# COMMAND ----------

json_schema = StructType([
    StructField("Zipcode", IntegerType(), True),
    StructField("ZipCodeType", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True)
])

df1=df.withColumn('ParsedJson', from_json(col('JsonValue'), json_schema)).select('id', 'ParsedJson.*')
df1.display()

# COMMAND ----------

from pyspark.sql.types import *

data=[("l1", [1,2,3] , {1:'w', 2:'k'} ),
      ("l2", [4,5,6], None)]

json_schema=StructType([
    StructField("Zipcode", StringType(), True),
    StructField("ZipCodeType", ArrayType(IntegerType()), True),
    StructField("ZipCodeType", MapType(IntegerType(),StringType() ), True)]
)

df=spark.createDataFrame(data, ["l1","l2","l3"])
df.display()

# COMMAND ----------

sampleJson = [
    ('{"user": 100, "ip": [1, 2]}',),
    ('{"user": 200, "ip": [3, 4]}',)
]

# Create a DataFrame from the sample JSON data
df = spark.createDataFrame(sampleJson, ["json_string"])
df.display()

from pyspark.sql.functions import *
schema = StructType([
    StructField("user", IntegerType(), True),
    StructField("ip", ArrayType(IntegerType()), True)
])
df1=df.withColumn("new", from_json(col("json_string"), schema)).select("new.*")
df1.display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType

# Create a Spark session
spark = SparkSession.builder.appName("JsonExample").getOrCreate()

# Sample JSON data
sampleJson = [
    ('{"user": 100, "ip": [1, 2]}',),
    ('{"user": 200, "ip": [3, 4]}',)
]

# Create a DataFrame from the sample JSON data
df = spark.createDataFrame(sampleJson, ["json_string"])

# Define the schema for the JSON data
schema = StructType([
    StructField("user", IntegerType(), True),
    StructField("ip", ArrayType(IntegerType()), True)
])

# Parse the JSON string into a structured DataFrame
df_parsed = df.select(from_json(col("json_string"), schema).alias("data")).select("data.*")

# Show the DataFrame with new columns
df_parsed.show()

# COMMAND ----------


# Sample data for the DataFrame
data = [
    (49, "Airport"),
    (51, "Office"),
    (52, "Hospital"),
    (53, "Airport"),
    (54, "Hospital"),
    (55, "ShoppingMall"),
    (56, "Office"),
    (57, "Hospital"),
    (58, "Hospital")
]

# Create a DataFrame from the sample data
df = spark.createDataFrame(data, ["reqid", "pickup_location"])
df.createOrReplaceTempView('df')
# Show the DataFrame
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select pickup_location, count(*) from df group by pickup_location order by count(*) desc limit 3

# COMMAND ----------

df.select("pickup_location").groupBy("pickup_location").count().orderBy(col("count").desc()).limit(3).display()

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("CreateEmployeeDataFrame").getOrCreate()

# Sample data for the DataFrame
data = [
    (100, "IT", 100, "2024-05-12"),
    (200, "IT", 100, "2024-06-12"),
    (100, "FIN", 400, "2024-07-12"),
    (300, "FIN", 500, "2024-07-12"),
    (300, "FIN", 1543, "2024-07-12"),
    (300, "FIN", 1500, "2024-07-12")
]

# Create a DataFrame from the sample data
employee_df = spark.createDataFrame(data, ["empid", "dept", "salary", "date"])
employee_df.createOrReplaceTempView('employee_df')
# Show the DataFrame
employee_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_df where empid not in (select empid from employee_df group by empid having count(*)>1)

# COMMAND ----------

df1_emp = employee_df.groupBy("empid").count().filter("count = 1").select("empid")
employee_df.join(df1_emp, "empid").select(employee_df["*"]).show()

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("CreateEmployeeTable").getOrCreate()

# Sample data for the DataFrame
data = [
    (72657,),
    (1234,),
    ("Tom",),
    (8792,),
    ("Sam",),
    (19998,),
    ("Philip",)
]

# Create a DataFrame from the sample data
emp_tbl = spark.createDataFrame(data, ["emp_id"])
emp_tbl.createOrReplaceTempView('emp_tbl')

# Show the DataFrame
emp_tbl.display()

# COMMAND ----------

emp_tbl.withColumn("emp_id", col("emp_id").cast(IntegerType())).filter(col("emp_id").isNotNull()).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select try_cast(emp_id as INT)  from emp_tbl where try_cast(emp_id as INT) is not null

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("CreateDataFrames").getOrCreate()

# Sample data for the first DataFrame (df1)
data1 = [
    (1, "Bob"),
    (2, "Alice"),
    (3, "Tom")
]

# Create the first DataFrame
df1 = spark.createDataFrame(data1, ["id", "name"])

# Show the first DataFrame
print("DataFrame df1:")
df1.show()

# Sample data for the second DataFrame (df2)
data = [
    (1, "Bob"),
    (3, "Tom")
]

# Create the second DataFrame
df2 = spark.createDataFrame(data, ["id","name"])

# Show the second DataFrame
print("DataFrame df2:")
df2.show()

# COMMAND ----------

df1.join(df2, "id", "leftanti").display()

# COMMAND ----------

data = [
    (1, None, "ab"),   # Row 1
    (2, 10, None),     # Row 2
    (None, None, "cd")  # Row 3
]

# Create the DataFrame with specified column names
df = spark.createDataFrame(data, ["col1", "col2", "col3"])
df.createOrReplaceTempView('df')
# Show the DataFrame
df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(if(col1 is null, 1 , 0)) as col1,
# MAGIC sum(if(col2 is null, 1 , 0)) as col2,
# MAGIC sum(if(col3 is null, 1 , 0)) as col3  from df 

# COMMAND ----------

df.select( [sum(col(c).isNull().cast('int')).alias(c)    for c in df.columns ]).display()

# COMMAND ----------

data = [
    ("HR", 101, 1000),  # Row 1: (dept, empid, salary)
    ("Finance", 102, 900)  # Row 2: (dept, empid, salary)
]

# Create a DataFrame from the sample data
employee_df = spark.createDataFrame(data, ["dept", "empid", "salary"])

# Show the DataFrame
employee_df.display()

# COMMAND ----------


for c in employee_df.columns:
  employee_df=employee_df.withColumnRenamed(c , "pref_"+c)
  display(c)

# COMMAND ----------

employee_df.display()

# COMMAND ----------

data = [
    ("John Smith", "Canada"),
    ("Mike David", "USA")
]

# Define the schema (column names)
columns = ["Customers", "Country"]

# Create the DataFrame
df = spark.createDataFrame(data, schema=columns)
df.display()
df1=df.withColumn("new", split(col("Customers")," "))
df1.select(explode(col("new")).alias("Customers"), "country").display()

# COMMAND ----------


# Define the data
data = [
    ("virat kohli",),
    ("p v sindhu",)
]

# Define the schema (column names)
columns = ["name"]

# Create the DataFrame
df = spark.createDataFrame(data, schema=columns)

# Show the DataFrame
df.show()
df.withColumn("name1", initcap(col("name"))).display()

# COMMAND ----------

# DBTITLE 1,get file name
sales=spark.read.format("delta").load("/user/hive/warehouse/dannys_diner.db/sales")
sales1=sales.withColumn("file_name", split(input_file_name(), "/").getItem(6))

# COMMAND ----------

sales1.display()

# COMMAND ----------

sales1.withColumn("file_name", split(col('file_name'), "\.").getItem(0)).display()

# COMMAND ----------

product_data = [
 (1, 'Laptops', 'Electronics'),
 (2, 'Jeans', 'Clothing'),
 (3, 'Chairs', 'Home Appliances')
 ]

product_schema = ['product_id', 'product_name', 'category']

product_df = spark.createDataFrame(data = product_data , schema = product_schema)
product_df.show()
product_df.createOrReplaceTempView('product')
sales_data = [
 (1, 2019, 1000.00),
 (1, 2020, 1200.00),
 (1, 2021, 1100.00),
 (2, 2019, 500.00),
 (2, 2020, 600.00),
 (2, 2021, 900.00),
 (3, 2019, 300.00),
 (3, 2020, 450.00),
 (3, 2021, 400.00)
 ]

sales_schema = ['product_id', 'year', 'total_sales_revenue']

sales_df = spark.createDataFrame(data = sales_data , schema = sales_schema)
sales_df.show()
sales_df.createOrReplaceTempView('sales')

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, min(diff) from (
# MAGIC select *, ( ld -total_sales_revenue) as diff from (
# MAGIC select *, lead(total_sales_revenue, 1) over(partition by product_id order by year) ld from sales))
# MAGIC group by 1 
# MAGIC having min(diff)>=0

# COMMAND ----------

from pyspark.sql.types import *
data=[
  (1, 2, None),
  (None, 3, None),
  (5, None, None)
]
schema= StructType([
  StructField('col1', IntegerType(), True),
  StructField('col2', IntegerType(), True),
  StructField('col3', IntegerType(), True)
]
)
df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

df1 = df.agg(*[count(c).alias(c) for c in df.columns])

# Get columns with zero non-null values
nullColumns = [c for c in df1.columns if df1.first()[c] == 0]
nullColumns
df.drop(*nullColumns).display()

# COMMAND ----------

data = [ (1, 101, '2024-12-15 09:30:00'), (2, 102, '2024-12-15 11:45:00'), (3, 103, '2024-12-15 12:10:00'), (4, 104, '2024-12-15 13:15:00'), (5, 105, '2024-12-15 14:20:00'), (6, 106, '2024-12-15 15:30:00'), (7, 107, '2024-12-15 16:40:00'), (8, 108, '2024-12-16 09:50:00'), (9, 109, '2024-12-16 10:30:00'), (10, 110, '2024-12-16 12:05:00'), (11, 111, '2024-12-16 13:50:00'), (12, 112, '2024-12-16 14:15:00'), (13, 113, '2024-12-16 15:30:00'), (14, 114, '2024-12-17 09:45:00'), (15, 115, '2024-12-17 11:20:00'), (16, 116, '2024-12-17 12:25:00'), (17, 117, '2024-12-17 13:30:00'), (18, 118, '2024-12-17 14:55:00'), (19, 119, '2024-12-17 15:10:00'), (20, 120, '2024-12-18 10:40:00') ]

columns = ["order_id", "product_id", "timestamp"]

df=spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------

df1=df.withColumn("hour", hour(to_timestamp(col('timestamp'))))
df2=df1.withColumn("time", when((col('hour')>=6) & (col('hour')<=11) , lit("Morning")).when((col('hour')>=12) & (col('hour')<=15) , lit("AfterNoon"))\
              .when((col('hour')>=16) & (col('hour')<=18) , lit("Eveining")).otherwise('Night')).withColumn("dayOfWeek", dayofweek(to_timestamp(col('timestamp'))))

# COMMAND ----------

df2.groupBy('dayOfWeek', 'time').agg(count('order_id')).display()

# COMMAND ----------

data = [ (1, "Laptop", 1000, 5), (2, "Mouse", None, None), (3, "Keyboard", 50, 2), (4, "Monitor", 200, None), (5, None, 500, None), ] 

# Define schema and create DataFrame 
columns = ["product_id", "product", "price", "quantity"]

df=spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------

mean_val=df.select('price').agg(avg('price')).collect()[0][0]
df.withColumn("price", when(col('price').isNull(), lit(mean_val)).otherwise(col('price'))).display()

# COMMAND ----------

df.na.drop(subset=['product']).display()

# COMMAND ----------

df.fillna({'quantity':1}).display()

# COMMAND ----------

df.na.fill({'quantity':1, 'price':1}).display()

# COMMAND ----------

df.fillna({'price':mean_val}).display()

# COMMAND ----------

schema = StructType([
    StructField("TeamA", StringType(), True),
    StructField("TeamB", StringType(), True),
    StructField("Winner", StringType(), True)
])

# Create data
data = [
    ("CSK", "MI", "CSK"),
    ("KKR", "DC", "KKR"),
    ("LSG", "SRH", "SRH"),
    ("RCB", "PBKS", "RCB")
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show DataFrame
df.show()

# COMMAND ----------

df1=df.withColumnRenamed('TeamA', 'Team').withColumn('Points', when(col('Team')==col('Winner'), 1 ).otherwise(0))
df1.display()
df2=df.withColumnRenamed('TeamB', 'Team').withColumn('Points', when(col('Team')==col('Winner'), 1 ).otherwise(0))
df2.display()
df3=df1.select('Team', 'Points').unionByName(df2.select('Team', 'Points'))
df3.display()
df4=df3.groupBy('Team').agg(sum('points').alias('win'),  (count('*')-(sum('points'))).alias('Lost')).withColumn('Points', lit(col('win')*2)).display()

# COMMAND ----------

# tab A	tab B   	 
# id	id     
# 1	1      
# 1	2 	
# 2	2       
# 3	2       
# null	2       
#  	4                            
#  	5                              
#  	null

# COMMAND ----------

data1 = [
    (1,),
    (1,),
    (2,),
    (None,),
    (None,)
]

data2 = [
    (1,),
    (1,),
    (2,),
    (None,)
]

# Define schema
schema1 = StructType([
    StructField("col1111", IntegerType(), True),
])

schema2 = StructType([
    StructField("col1", IntegerType(), True),
])

# Create DataFrame
df1 = spark.createDataFrame(data1, schema1)
df2 = spark.createDataFrame(data2, schema2)
df1.createOrReplaceTempView('df1')
df2.createOrReplaceTempView('df2')

# Show DataFrame
df1.union(df2).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df1
# MAGIC union
# MAGIC select * from df2

# COMMAND ----------

df2.toDF(*df1.columns).display()

# COMMAND ----------

dataA = [(21,), (54,), (34,), (45,), (47,), (90,), (100,)]
dataB = [('A',), ('B',), ('C',), ('K',), ('E',), ('F',), ('G',)]

# Define schemas
schemaA = StructType([StructField("number", IntegerType(), True)])
schemaB = StructType([StructField("letter", StringType(), True)])

# Create DataFrames
dfA = spark.createDataFrame(dataA, schema=schemaA)
dfB = spark.createDataFrame(dataB, schema=schemaB)

# Show the DataFrames
dfA.show()
dfB.show()

# COMMAND ----------

windowSpec=Window.orderBy(lit(None))
df1=dfA.withColumn("rn", row_number().over(windowSpec))
df2=dfB.withColumn("rn", row_number().over(windowSpec))

df1.join(df2, 'rn').select(concat_ws(',',df1.number,df2.letter)).display()

# COMMAND ----------

data = [
    (1, 1, 100),
    (1, 2, 50),
    (1,3, 30),
    (2, 4, 100),
    (2,5, 40)
]

# Define schema
schema = StructType([
    StructField("Id", IntegerType(), True),
    StructField("Date", StringType(), True),
    StructField("Amt", IntegerType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show DataFrame
df.display()

# COMMAND ----------

windowSpec=Window.partitionBy('id').orderBy('Date').rowsBetween(Window.unboundedPreceding,Window.currentRow)

df.withColumn("avg", avg('amt').over(windowSpec)).display()

# COMMAND ----------

data = [
    ("2024-01-01", "Laptop", 2000),
    ("2024-01-01", "Phone", 1000),
    ("2024-01-02", "Laptop", 1500),
    ("2024-01-02", "Tablet", 800),
    ("2024-01-03", "Phone", 1200),
    ("2024-01-03", "Laptop", 1800)
]

columns = ["Date", "Product", "Sales"]

df = spark.createDataFrame(data, schema=columns)
df.show()

# COMMAND ----------

df1=df.groupBy('Date').agg(sum('Sales').alias('Daily_Sales'))

windowSpec=Window.orderBy('Date')

df1.withColumn('Running_Total', sum('Daily_Sales').over(windowSpec)).display()

# COMMAND ----------

df_source = spark.createDataFrame(
[ (1, 'A'),(2, 'B'),(3, 'C'),(4, 'D')], ['id', 'name'])
df_target = spark.createDataFrame([
 (1, 'A'),(2, 'B'),(5, 'F'),(4, 'X')], ['id', 'name'])

df=df_source.join(df_target, 'id', 'full').select(df_source['*'], df_target.name.alias('tgt_name'))
df.display()
df.na.fill({'name':'Not in target'}).display()

# COMMAND ----------

data = [ (1, "Alice", None), (2, "Bob", 1), 
(3, "Charlie", 1), (4, "David", 2), 
(5, "Eva", 2), (6, "Frank", 3), (7, "Grace", 3) ] 

columns = ["employee_id", "employee_name", "manager_id"]

df=spark.createDataFrame(data, columns)

df.alias('df1').join(df.alias('df2'), col('df1.manager_id')==col('df2.employee_id')).filter(col('df2.manager_id').isNull()).display()

# COMMAND ----------

data=[('jan', ), ('mar',)]

df=spark.createDataFrame(data, ["month"])\
        .withColumn('month1', month(to_date(col('month'), 'MMM'))).display()

# COMMAND ----------

# Data
dept = [
    'IT', 'IT', 'IT',
    'IT', 'HR', 'HR', 'HR', 'HR', 'HR',
    'Sales', 'Sales', 'Sales', 'Sales', 'Sales', 'Sales', 'Sales'
]

gender = [
    'M', 'F', 'M',
    'F', 'M', 'F', 'F', 'F', 'F',
    'F', 'M', 'M', 'M', 'M', 'F'
]

# Combine into list of tuples
data = list(zip(dept, gender))

# Create DataFrame
df = spark.createDataFrame(data, ["DeptName", "Gender"])

# Show DataFrame
df.show()

# COMMAND ----------

df1=df.withColumn('MaleEmp', when(col('Gender')=='M', 1).otherwise(lit(0)))\
   .withColumn('FemaleEmp', when(col('Gender')=='F', 1).otherwise(lit(0)))

Df_final=df1.groupBy('DeptName').agg( count('*').alias('total_emp'), sum(col('MaleEmp')).alias('MaleEmp'), sum(col('FemaleEmp')).alias('FemaleEmp'))
Df_final.display()


# COMMAND ----------


