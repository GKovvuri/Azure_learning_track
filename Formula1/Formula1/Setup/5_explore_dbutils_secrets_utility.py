# Databricks notebook source
# MAGIC %md
# MAGIC ### Explore the capabilities of dbutils.secrets.utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('formula1-scope')

# COMMAND ----------

dbutils.secrets.get('formula1-scope','f1azda-access')

# COMMAND ----------

