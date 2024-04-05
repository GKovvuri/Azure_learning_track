# Databricks notebook source
# DBTITLE 1,Setting the access using SAS (Shared Access Signature) tokens 
# MAGIC %md 
# MAGIC Steps to access the data container using the SAS tokens :
# MAGIC - 1) Set Authentication type to SAS
# MAGIC - 2) Set the Provider type to Fixed SAS Token Provider
# MAGIC - 3) Set the Security Key from the ADLS service 
# MAGIC
# MAGIC Syntax:
# MAGIC
# MAGIC Step 1:
# MAGIC spark.conf.set("fs.azure.account.auth.type.\<storage-account>.dfs.core.windows.net","SAS")
# MAGIC </br>Step 2:
# MAGIC spark.conf.set("fs.azure.sas.token.provider.type./<storage-account>.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
# MAGIC </br>Step3:
# MAGIC spark.conf.set("fs.azure.sas.fixed.token.\<storage-account>.dfs.core.windows.net","\<token>")

# COMMAND ----------

demo_sas_token=dbutils.secrets.get(scope = 'formula1-scope',key = 'f1azda-sastk')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1azda.dfs.core.windows.net","SAS")
spark.conf.set("fs.azure.sas.token.provider.type.f1azda.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.f1azda.dfs.core.windows.net",demo_sas_token)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@f1azda.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1azda.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("abfss://demo@f1azda.dfs.core.windows.net/circuits.csv",header=True)

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1azda.dfs.core.windows.net/circuits.csv",header=True))

# COMMAND ----------

