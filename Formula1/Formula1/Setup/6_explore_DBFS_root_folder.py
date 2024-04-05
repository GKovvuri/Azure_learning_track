# Databricks notebook source
# MAGIC %md 
# MAGIC 1. List all folders in dbfs root 
# MAGIC 2. interact with dbfs File browser
# MAGIC 3. Upload file to dbfs root

# COMMAND ----------

display(dbutils.fs.ls("/"))