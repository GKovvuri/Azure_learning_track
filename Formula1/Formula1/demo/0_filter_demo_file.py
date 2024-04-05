# Databricks notebook source
# MAGIC %run ../Includes/configuration

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

races_df_filtered = races_df.filter("race_year = 2019 and round =5")

# COMMAND ----------

races_df_filtered_w2 = races_df.filter((races_df.race_year == 2019) & (races_df.round == 5))
races_df_filtered_w3 = races_df.filter((races_df["race_year"] == 2019) & (races_df["round"] == 5))

# COMMAND ----------

display(races_df_filtered_w3)