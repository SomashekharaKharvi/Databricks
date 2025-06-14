# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType,TimestampType, FloatType
from datetime import datetime, date, time, timedelta
from pyspark.sql.functions import col
landingFileSchema=  StructType([ \
    StructField("Sale_ID",StringType(),True), \
    StructField("Product_ID",StringType(),True), \
    StructField("Product_Name",StringType(),True), \
    StructField("Quantity_Sold",IntegerType(),True), \
    StructField("Vendor_ID", StringType(), True), \
    StructField("Sale_Date", TimestampType(), True), \
    StructField("Sale_Amount", DoubleType(), True), \
    StructField("Sale_Currency", StringType(), True) \
  ])
vendorFileSchema=  StructType([ \
    StructField("Vendor_ID",StringType(),True), \
    StructField("Vendor_Name",StringType(),True), \
    StructField("Vendor_Add_Street",StringType(),True), \
    StructField("Vendor_Add_City",StringType(),True), \
    StructField("Vendor_Add_State", StringType(), True), \
    StructField("Vendor_Add_Country", StringType(), True), \
    StructField("Vendor_Add_Zip", StringType(), True), \
    StructField("Vendor_Updated_Date", TimestampType(), True) \
  ])

usdReferenceSchema=  StructType([ \
    StructField("currency",StringType(),True), \
    StructField("currency_code",StringType(),True), \
    StructField("Exchange_Rate",StringType(),True), \
    StructField("Currency_Updated_Date",StringType(),True) \
  ])


# COMMAND ----------

currDayZoneSuffix="_14012022"
previousDaySuffix="_13012022"

# COMMAND ----------

inputLocation="/FileStore/tables/Data/Inputs/"
outputLocation="/FileStore/tables/Data/Outputs/"

# COMMAND ----------

productEnrichedDF=spark.read.format("csv").option("delimiter", "|").option("header",True).schema(landingFileSchema).load(outputLocation+"Enriched/SaleAmountEnrichment/SaleAmountEnriched"+currDayZoneSuffix)

vendorReferenceDF=spark.read.format("csv").option("delimiter", "|").schema(vendorFileSchema).load(inputLocation +"Vendors")

usdReferenceDF=spark.read.format("csv").option("delimiter", "|").schema(usdReferenceSchema).load(inputLocation +"USD_Rates")

# COMMAND ----------

productEnrichedDF.createOrReplaceTempView("productEnrichedDF")
vendorReferenceDF.createOrReplaceTempView("vendorReferenceDF")
usdReferenceDF.createOrReplaceTempView("usdReferenceDF")

# COMMAND ----------

vendorEnrichedDF=spark.sql("select a.*, b.Vendor_Name "
                          "from productEnrichedDF a inner join vendorReferenceDF b "
                          "on a.Vendor_ID=b.Vendor_ID")
vendorEnrichedDF.createOrReplaceTempView("vendorEnrichedDF")

# COMMAND ----------

vendor_USD_Enriched=spark.sql("select a.*, ROUND(a.Sale_Amount/b.Exchange_Rate,2) as Sale_Amount_USD "
                             "from vendorEnrichedDF a inner join usdReferenceDF b "
                             "on a.Sale_Currency=b.currency_code")

# COMMAND ----------

vendor_USD_Enriched.show()

# COMMAND ----------

vendor_USD_Enriched.write.mode("overwrite").option("delimiter","|").option("header",True).csv(outputLocation+"Enriched/Vendor_USD_Enriched/Vendor_USD_Enriched"+currDayZoneSuffix)

# COMMAND ----------


