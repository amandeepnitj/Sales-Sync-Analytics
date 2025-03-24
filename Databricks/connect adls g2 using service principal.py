# Databricks notebook source
dbutils.secrets.list('scope-to-key-vault')
# dbutils.secrets.listScopes()


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": dbutils.secrets.get("scope-to-key-vault", "app-id"),
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get("scope-to-key-vault", "app-value"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/0195259f-7472-459e-8c4c-f13514d7de3a/oauth2/token"}

dbutils.fs.mount(source="abfss://bronze@azuredl2.dfs.core.windows.net/", mount_point="/mnt/bronze", extra_configs=configs)



# COMMAND ----------

dbutils.fs.unmount('/mnt/curated')

# COMMAND ----------

df= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/curated/classes/classes.csv")

# COMMAND ----------

dbutils.fs.ls("/mnt/curated/classes")

# COMMAND ----------

df= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load('/mnt/bronze/AdventureWorks_Calendar/AdventureWorks_Calendar.csv')


# COMMAND ----------

display(df)