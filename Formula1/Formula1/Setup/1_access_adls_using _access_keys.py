# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Access Azure Data Lake using access keys
# MAGIC </br>
# MAGIC 1. Set the spark config using fs.azure.account.key</br>
# MAGIC 2. List files from demo cluster. </br>
# MAGIC 3. Read data from circuits.csv file.</br>

# COMMAND ----------

access_key=dbutils.secrets.get('formula1-scope','f1azda-access')

# COMMAND ----------

spark.conf.set("fs.azure.account.key.f1azda.dfs.core.windows.net",access_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1azda.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1azda.dfs.core.windows.net/circuits.csv",header=True))

# COMMAND ----------

