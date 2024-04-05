# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Access Azure Data Lake using Cluster scoped keys
# MAGIC </br>
# MAGIC 1. Set the spark config using fs.azure.account.key in the cluster</br>
# MAGIC 2. List files from demo cluster. </br>
# MAGIC 3. Read data from circuits.csv file.</br>
# MAGIC
# MAGIC ## Access Azure Datalake using the Azure Active Directory passthrough
# MAGIC 1. DataLake Storage --> Access Control --> Add role to user under Grant access to this resource --> select the job function role --> Asign the job function to either user or service policy --> review and assign.
# MAGIC 2. Make sure that cluster is in single user mode and enable user level passthrough is checked in advanced settings.

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1azda.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1azda.dfs.core.windows.net/circuits.csv",header=True))

# COMMAND ----------

