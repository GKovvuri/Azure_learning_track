# Databricks notebook source
v_result = dbutils.notebook.run("1_ingest_csv_data_circuits.csv",0,{"p_data_source":"circuits.csv","p_file_date":"2021-04-18"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("2_ingest_csv_data_races.csv",0,{"p_data_source":"races.csv","p_file_date":"2021-04-18"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("3_ingest_json_data_constructors.json",0,{"p_data_source":"constructors.json","p_file_date":"2021-04-18"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("4_ingest_json_data_nested_drivers.json",0,{"p_data_source":"drivers.json","p_file_date":"2021-04-18"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("5_ingest_json_data_nested_results.json",0,{"p_data_source":"results.json","p_file_date":"2021-04-18"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("6_ingest_multiline_json_data_pitstops.json",0,{"p_data_source":"pit_stops.json","p_file_date":"2021-04-18"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("7_ingest_multiple_csv_data_lap_times.csv",0,{"p_data_source":"lap_times_split*.csv","p_file_date":"2021-04-18"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("8_ingest_multiple_json_data_qualifying.json",0,{"p_data_source":"qualifying*.json","p_file_date":"2021-04-18"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT race_id, Count(1) as cnt
# MAGIC FROM f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

