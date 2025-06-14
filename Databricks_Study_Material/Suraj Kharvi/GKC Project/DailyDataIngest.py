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

# COMMAND ----------

currDayZoneSuffix="_14012022"
previousDaySuffix="_13012022"

# COMMAND ----------

inputLocation="/FileStore/tables/Data/Inputs/"
outputLocation="/FileStore/tables/Data/Outputs/"

# COMMAND ----------

landingFileDF=spark.read.format("csv").option("delimiter", "|").schema(landingFileSchema).load(inputLocation + "Sales_Landing/SalesDump"+currDayZoneSuffix)

# COMMAND ----------

previousHoldDF = spark.read \
    .schema(landingFileSchema) \
    .option("delimiter", "|") \
    .option("header", True) \
    .csv(outputLocation + "Hold/HoldData"+previousDaySuffix)

# COMMAND ----------


previousHoldDF.show()
landingFileDF.show()

# COMMAND ----------

landingFileDF.createOrReplaceTempView("landingFileDF")
previousHoldDF.createOrReplaceTempView("previousHoldDF")
refreshedLandingZoneDF=spark.sql("select a.Sale_ID, a.Product_ID, "
          "CASE "
          "WHEN (a.Quantity_Sold IS NULL) THEN b.Quantity_Sold "
          "ELSE a.Quantity_Sold "
          "END AS Quantity_Sold, "
          "CASE "
          "WHEN (a.Vendor_ID IS NULL) THEN b.Vendor_ID "
          "ELSE a.Vendor_ID "
          "END AS Vendor_ID, "
          "a.Sale_Date, a.Sale_Amount, a.Sale_Currency "
          "from landingFileDF a left outer join previousHoldDF b on a.Sale_ID = b.Sale_ID ")

# COMMAND ----------

refreshedLandingZoneDF.show()

# COMMAND ----------

refreshedLandingZoneDF.createOrReplaceTempView("refreshedLandingData")
validLandingData = refreshedLandingZoneDF.filter(col("Quantity_Sold").isNotNull() & col("Vendor_ID").isNotNull())
validLandingData.createOrReplaceTempView("validLandingData")

# COMMAND ----------

realseFromHold=spark.sql("select a.Sale_ID from validLandingData a inner join previousHoldDF b on a.Sale_ID=b.Sale_ID")
realseFromHold.createOrReplaceTempView("releasedFromHold")

# COMMAND ----------

notReleasedFromHold =spark.sql("select * from previousHoldDF where Sale_ID not in (select Sale_ID from releasedFromHold )")
notReleasedFromHold.createOrReplaceTempView("notReleasedFromHold")

# COMMAND ----------

from pyspark.sql import functions as psf
from pyspark.sql.functions import lit
notReleasedFromHold=notReleasedFromHold.withColumn("Hold_Reason", psf
                .when(psf.col("Quantity_Sold").isNull(), "Qty Sold Missing")
                .otherwise(psf.when(psf.col("Vendor_ID").isNull(), "Vendor ID Missing")))
inValidLandingData = refreshedLandingZoneDF.filter(psf.col("Quantity_Sold").isNull() | psf.col("Vendor_ID").isNull() |
                                                 psf.col("Sale_Currency").isNull())\
    .withColumn("Hold_Reason", psf
                .when(psf.col("Quantity_Sold").isNull(), "Qty Sold Missing")
                .otherwise(psf.when(psf.col("Vendor_ID").isNull(), "Vendor ID Missing")))\
    .union(notReleasedFromHold)


# COMMAND ----------

inValidLandingData.show()

# COMMAND ----------

validLandingData.write\
    .mode("overwrite")\
    .option("delimiter", "|")\
    .option("header", True)\
    .csv(outputLocation + "Valid/ValidData"+currDayZoneSuffix)

# COMMAND ----------


inValidLandingData.write\
    .mode("overwrite")\
    .option("delimiter", "|")\
    .option("header", True)\
    .csv(outputLocation + "Hold/HoldData"+currDayZoneSuffix)
