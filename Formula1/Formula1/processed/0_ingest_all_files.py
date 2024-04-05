# Databricks notebook source
v_result = dbutils.notebook.run("2_process_csv_data_circuits.csv",0,{"p_data_source":"circuits.csv"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("3_process_csv_data_races.csv",0,{"p_data_source":"races.csv"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("4_process_json_data_constructors.json",0,{"p_data_source":"constructors.json"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("5_process_json_data_nested_drivers.json",0,{"p_data_source":"drivers.json"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("6_process_json_data_nested_results.json",0,{"p_data_source":"results.json"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("7_process_multiline_json_data_pitstops.json",0,{"p_data_source":"pit_stops.json"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("8_process_multiple_csv_data_lap_times.csv",0,{"p_data_source":"lap_times_split*.csv"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

v_result = dbutils.notebook.run("9_process_multiple_json_data_qualifying.json",0,{"p_data_source":"qualifying*.json"})

# COMMAND ----------

print(v_result)

# COMMAND ----------

