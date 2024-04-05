# Databricks notebook source
# MAGIC %md
# MAGIC ### Local Temp View
# MAGIC
# MAGIC - sample_df.sql.createOrReplaceTempView(table_name)
# MAGIC - Can be accessed by SELECT * FROM table_name.
# MAGIC - can be accessed within the context of the same notebook 
# MAGIC
# MAGIC ### Global Temp View
# MAGIC
# MAGIC - sample_df.sql.createOrGlobalTempView(table_name).
# MAGIC - The table is stored in the global_temp database so to access the table SELECT * FROM global_temp.table_name.
# MAGIC - View can be accessed across different notebooks.
# MAGIC

# COMMAND ----------

# MAGIC %run ../Includes/configuration

# COMMAND ----------

race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_race_results LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM v_race_results 
# MAGIC WHERE race_year = 2020;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1) 
# MAGIC FROM v_race_results 
# MAGIC WHERE race_year = 2020;

# COMMAND ----------

race_results_2019_df = spark.sql("SELECT * FROM v_race_results WHERE race_year =2019")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

race_results.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM global_temp.gv_race_results
# MAGIC LIMIT 5;

# COMMAND ----------

