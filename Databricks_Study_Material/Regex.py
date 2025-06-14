# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Step 2: Create a sample DataFrame
data = [
    ("Hello World",),  # Question 1
    ("This is a test",),  # Question 2
    ("User  123 has 45 apples",),  # Question 3
    ("This    is    a    test",),  # Question 4
    ("user@example.com",),  # Question 5
    ("Hello@World! #2023",),  # Question 6
    ("Today's date is 12/31/2023",),  # Question 7
    ("Visit us at https://www.example.com for more info.",),  # Question 8
    ("hello world",),  # Question 9
    ("The color is #FF5733 and the background is #C0C0C0.",)  # Question 10
]

# Create DataFrame
df = spark.createDataFrame(data, ["text"])
df.display()

# COMMAND ----------

# DBTITLE 1,Remove Ovels
df.withColumn("new", regexp_replace(col("text"), r"[aeiouAEIOU]", " " )).display()

# COMMAND ----------

# DBTITLE 1,remove space
df.withColumn("new", regexp_replace(col("text"), r"[\s]", "" )).display()

# COMMAND ----------

# DBTITLE 1,remove numbers
df.withColumn("new", regexp_replace(col("text"), r"[\d]", "" )).display()

# COMMAND ----------

# DBTITLE 1,remove double space
df.withColumn("new", regexp_replace(col("text"), r"\s+", " " )).display()

# COMMAND ----------

df.withColumn("new", regexp_replace(col("text"), r"[^a-zA-z0-9]", "" )).display()

# COMMAND ----------

# DBTITLE 1,Mask Email Addresses:
df.withColumn("new", regexp_replace(col("text"), r"[A-za-z0-9]+[@][a-z]+[\.][a-z]{2,3}" , "********@.com")).display()

# COMMAND ----------

# DBTITLE 1,# 6. Remove Special Characters
df.withColumn("new", regexp_replace(col("text"), r"[^A-za-z0-9\s]" , "")).display()

# COMMAND ----------

# DBTITLE 1,# 8. Extract and Replace URLs
df.withColumn("text", regexp_replace("text", "https://www\\.example\\.com", "[link]")).display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace

# Step 1: Create a Spark session
spark = SparkSession.builder.appName("RegexReplaceExample").getOrCreate()

# Step 2: Create a sample DataFrame
data = [
    ("Hello World",),  # Question 1
    ("This is a test",),  # Question 2
    ("User  123 has 45 apples",),  # Question 3
    ("This    is    a    test",),  # Question 4
    ("user@example.com",),  # Question 5
    ("Hello@World! #2023",),  # Question 6
    ("Today's date is 12/31/2023",),  # Question 7
    ("Visit us at https://www.example.com for more info.",),  # Question 8
    ("hello world",),  # Question 9
    ("The color is #FF5733 and the background is #C0C0C0.",)  # Question 10
]

# Create DataFrame
df = spark.createDataFrame(data, ["text"])

# Show the original DataFrame
print("Original DataFrame:")
df.show(truncate=False)

# Step 3: Run the regex replace operations

# 1. Remove All Vowels
df_no_vowels = df.withColumn("no_vowels", regexp_replace("text", r"[aeiouAEIOU]", ""))
df_no_vowels.show(truncate=False)

# 2. Replace Spaces with Underscores
df_spaces_to_underscores = df.withColumn("spaces_to_underscores", regexp_replace("text", r"\s+", "_"))
df_spaces_to_underscores.show(truncate=False)

# 3. Remove All Digits
df_no_digits = df.withColumn("no_digits", regexp_replace("text", r"\d+", ""))
df_no_digits.show(truncate=False)

# 4. Replace Multiple Spaces with a Single Space
df_single_space = df.withColumn("single_space", regexp_replace("text", r"\s+", " "))
df_single_space.show(truncate=False)

# 5. Mask Email Addresses
df_masked_email = df.withColumn("masked_email", regexp_replace("text", r"([^@]+)@([a-zA-Z0-9.]+)", r"****@\2"))
df_masked_email.show(truncate=False)

# 6. Remove Special Characters
df_no_special_chars = df.withColumn("no_special_chars", regexp_replace("text", r"[^a-zA-Z0-9\s]", ""))
df_no_special_chars.show(truncate=False)

# 7. Replace Dates in MM/DD/YYYY Format
df_replaced_dates = df.withColumn("replaced_dates", regexp_replace("text", r"(\d{1,2})/(\d{1,2})/(\d{4})", r"\3-\1-\2"))
df_replaced_dates.show(truncate=False)

# 8. Extract and Replace URLs
df_replaced_urls = df.withColumn("replaced_urls", regexp_replace("text", r"https?://[^\s]+", "[LINK]"))
df_replaced_urls.show(truncate=False)

# 9. Capitalize First Letter of Each Word
df_capitalize_words = df.withColumn("capitalize_words", regexp_replace("text", r"(\b[a-z])", lambda m: m.group(1).upper()))
df_capitalize_words.show(truncate=False)

# 10. Replace Hexadecimal Color Codes
df_replaced_hex_colors = df.withColumn("replaced_hex_colors", regexp_replace("text", r"#([0-9A-Fa-f]{6}|[0-9A-Fa-f]{3})", "[COLOR]"))
df_replaced_hex_colors.show(truncate=False)

# Stop the Spark session
spark.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC regex_extract

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("RegexExtractExample").getOrCreate()

# Sample data
data = [
    ("John Doe, 30 years old, john.doe@example.com",),
    ("Jane Smith, 25 years old, jane.smith@domain.org",),
    ("Alice, 40 years, alice123@example.com",),
    ("Bob, 50, bob@example.com",),
    ("Charlie, 35 years old, charlie@company.net",)
]

# Create DataFrame
df = spark.createDataFrame(data, ["info"])

# Show the original DataFrame
print("Original DataFrame:")
df.display()

# COMMAND ----------

# DBTITLE 1,# 1. Extract Name
df.withColumn("new", regexp_extract(col("info"), r"[A-Za-z\s]+", 0)).display()

# COMMAND ----------

# DBTITLE 1,# 2. Extract Age
df.withColumn("new", regexp_extract(col("info"), r"[0-9]+[\s][a-z]+", 0)).display()

# COMMAND ----------

df.withColumn("year", regexp_extract("info", r"[\d]+", 0)).display()

# COMMAND ----------

from pyspark.sql.functions import regexp_extract

# 1. Extract Name
df_name = df.withColumn("name", regexp_extract("info", r"([A-Za-z\s]+),", 1))
df_name.show(truncate=False)

# 2. Extract Age
df_age = df.withColumn("age", regexp_extract("info", r"(\d{1,3})\s+years", 1))
df_age.show(truncate=False)

# 3. Extract Email Address
df_email = df.withColumn("email", regexp_extract("info", r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})", 1))
df_email.show(truncate=False)

# 4. Extract Year (if present)
df_year = df.withColumn("year", regexp_extract("info", r"(\d{4})", 1))
df_year.show(truncate=False)

# 5. Extract All Words
df_all_words = df.withColumn("all_words", regexp_extract("info", r"(\b\w+\b)", 0))
df_all_words.show(truncate=False)


# COMMAND ----------


