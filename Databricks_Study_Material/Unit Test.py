# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def add_column(df):
    return df.withColumn("new_column", col("existing_column") * 2)

# COMMAND ----------

import unittest
from pyspark.sql import SparkSession

class TestAddColumn(unittest.TestCase):
    # spark=None

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("UnitTest") \
            .master("local[*]") \
            .getOrCreate()

    # @classmethod
    # def tearDownClass(cls):
    #     cls.spark.stop()

    def test_add_column(self):
        # Create a sample DataFrame
        data = [(1,), (2,), (3,)]
        df = self.spark.createDataFrame(data, ["existing_column"])

        # Apply the transformation
        result_df = add_column(df)

        # Collect results
        result = result_df.collect()

        # Expected results
        expected_data = [(1, 2), (2, 4), (3, 6)]
        expected_df = self.spark.createDataFrame(expected_data, ["existing_column", "new_column"])

        # Collect expected results
        expected_result = expected_df.collect()

        # Assert that the results match
        self.assertEqual(result, expected_result)

if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)

# COMMAND ----------


