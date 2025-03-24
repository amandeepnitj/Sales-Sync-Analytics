# Databricks notebook source
# MAGIC %md
# MAGIC # SILVER LAYER SCRIPT
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # DATA LOADING

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run "/Workspace/Users/mann_singh03@outlook.com/connect adls g2 using service principal"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Calender data
# MAGIC

# COMMAND ----------

df = spark.read.format()

# COMMAND ----------

df_calender= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load('/mnt/bronze/AdventureWorks_Calendar/AdventureWorks_Calendar.csv')
df_customers= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load('/mnt/bronze/AdventureWorks_Customers/AdventureWorks_Customers.csv')
df_Prodcat= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load('/mnt/bronze/AdventureWorks_Product_Categories/AdventureWorks_Product_Categories.csv')
df_products= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load('/mnt/bronze/AdventureWorks_Products/AdventureWorks_Products.csv')
df_returns= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load('/mnt/bronze/AdventureWorks_Returns/AdventureWorks_Returns.csv')
df_sales_2015= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load('/mnt/bronze/AdventureWorks_Sales_2015/AdventureWorks_Sales_2015.csv')
df_sales_2016= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load('/mnt/bronze/AdventureWorks_Sales_2016/AdventureWorks_Sales_2016.csv')
df_sales_2017= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load('/mnt/bronze/AdventureWorks_Sales_2017/AdventureWorks_Sales_2017.csv')
df_subcategories= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load('/mnt/bronze/AdventureWorks_Product_Subcategories/AdventureWorks_Product_Subcategories.csv')
df_territories= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load('/mnt/bronze/AdventureWorks_Territories/AdventureWorks_Territories.csv')


# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": dbutils.secrets.get("scope-to-key-vault", "app-id"),
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get("scope-to-key-vault", "app-value"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/0195259f-7472-459e-8c4c-f13514d7de3a/oauth2/token"}
dbutils.fs.mount(source="abfss://silver@azuredl2.dfs.core.windows.net/", mount_point="/mnt/silver", extra_configs=configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calender

# COMMAND ----------

df_calender_updated = df_calender.withColumn("Month", month(col("Date"))).withColumn("Year", year(col("Date")))
df_calender_updated.display()

# COMMAND ----------

df_calender_updated.write.format('parquet')\
    .mode('append')\
    .option('path', '/mnt/silver/AdventureWorks_Calendar')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers

# COMMAND ----------

df_customers_updated = df_customers.withColumn("fullName",concat_ws(' ',col("Prefix"),col("FirstName"),col("LastName")))

# COMMAND ----------

df_customers_updated.display()

# COMMAND ----------

df_customers_updated.write.format('parquet')\
    .mode('append')\
    .option('path', '/mnt/silver/AdventureWorks_Customers')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sub Categories
# MAGIC

# COMMAND ----------

df_subcategories.write.format('parquet')\
    .mode('append')\
    .option('path', '/mnt/silver/AdventureWorks_Product_Subcategories')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products
# MAGIC

# COMMAND ----------

df_products_updated = df_products.withColumn('ProductSKU',split(col('ProductSKU'),'-')[0]).withColumn('ProductName',split(col('ProductName'),' ')[0])

# COMMAND ----------

df_products_updated.write.format('parquet')\
    .mode('append')\
    .option('path', '/mnt/silver/AdventureWorks_Products')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Returns

# COMMAND ----------

df_returns.write.format('parquet')\
    .mode('append')\
    .option('path', '/mnt/silver/AdventureWorks_Returns')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Territories

# COMMAND ----------

df_subcategories.write.format('parquet')\
    .mode('append')\
    .option('path', '/mnt/silver/AdventureWorks_Product_Subcategories')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales

# COMMAND ----------

df_sales = df_sales_2015.union(df_sales_2016).union(df_sales_2017)


# COMMAND ----------

df_sales = df_sales.withColumn('StockDate',to_timestamp('StockDate'))

# COMMAND ----------

df_sales =df_sales.withColumn('OrderNumber',regexp_replace(col('OrderNumber'),'S','T'))

# COMMAND ----------

df_sales =df_sales.withColumn('multiply',col('OrderLineItem')*col('OrderQuantity'))

# COMMAND ----------

df_sales.write.format('parquet')\
    .mode('append')\
    .option('path', '/mnt/silver/AdventureWorks_Sales')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # Sales Analysis

# COMMAND ----------

df_sales.groupBy('OrderDate').agg(count('OrderNumber').alias('total_orders')).display()

# COMMAND ----------

df_calender_updated.display()