# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType,TimestampType, FloatType
from datetime import datetime, date, time, timedelta
from pyspark.sql.functions import col
landingFileSchema=  StructType([ \
    StructField("Sale_ID",StringType(),True), \
    StructField("Product_ID",StringType(),True), \
    StructField("Quantity_Sold",IntegerType(),True), \
    StructField("Vendor_ID", StringType(), True), \
    StructField("Sale_Date", TimestampType(), True), \
    StructField("Sale_Amount", DoubleType(), True), \
    StructField("Sale_Currency", StringType(), True) \
  ])
productFileSchema=  StructType([ \
    StructField("Product_ID",StringType(),True), \
    StructField("Product_Name",StringType(),True), \
    StructField("Product_Price",IntegerType(),True), \
    StructField("Product_Price_Currency", StringType(), True), \
    StructField("Product_updated_date", TimestampType(), True) \
  ])

# COMMAND ----------

currDayZoneSuffix="_14012022"
previousDaySuffix="_13012022"

# COMMAND ----------

inputLocation="/FileStore/tables/Data/Inputs/"
outputLocation="/FileStore/tables/Data/Outputs/"

# COMMAND ----------

validDataDF=spark.read.format("csv").option("delimiter", "|").schema(landingFileSchema).option("header",True).load(outputLocation + "Valid/ValidData"+currDayZoneSuffix)
productPriceReferenceDF=spark.read.format("csv").option("delimiter", "|").schema(productFileSchema).option("header",True).load(inputLocation +"Products")

# COMMAND ----------

validDataDF.createOrReplaceTempView("validDataDF")
productPriceReferenceDF.createOrReplaceTempView("productPriceReferenceDF")
validDataDF.show()
productPriceReferenceDF.show()

# COMMAND ----------

productEnrichedDF=spark.sql("select a.Sale_ID, a.Product_ID, b.Product_Name, a.Quantity_Sold,a.Vendor_ID, a.Sale_Date, "
                            "b.Product_Price*a.Quantity_Sold as Sale_Amount, a.Sale_Currency "
                           "from validDataDF a inner join productPriceReferenceDF b "
                           "on a.Product_ID=b.Product_ID")

# COMMAND ----------

productEnrichedDF.show()

# COMMAND ----------

productEnrichedDF.write.mode("overwrite").option("delimiter","|").option("header","true").csv(outputLocation+"Enriched/SaleAmountEnrichment/SaleAmountEnriched"+currDayZoneSuffix)

# COMMAND ----------


