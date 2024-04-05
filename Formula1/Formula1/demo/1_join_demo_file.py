# Databricks notebook source
# MAGIC %run ../Includes/configuration

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")\
    .filter("race_year = 2019")\
        .withColumnRenamed("name","race_name")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
    .filter("circuit_id < 70")\
        .withColumnRenamed("name","circuit_name")\
            .withColumnRenamed("RaceCountry","country")

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"inner")\
    .select(circuits_df.circuit_name,circuits_df.circuit_id,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

races_df_left = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id, "left")\
    .select(circuits_df.circuit_name,circuits_df.circuit_id,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(races_df_left)

# COMMAND ----------

races_df_right = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id, "right")\
    .select(circuits_df.circuit_name,circuits_df.circuit_id,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(races_df_right)

# COMMAND ----------

races_df_full = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id, "full")\
    .select(circuits_df.circuit_name,circuits_df.circuit_id,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(races_df_full)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Semi Join
# MAGIC ### Semi Left Join 
# MAGIC - Similar to Inner join and Semi Left join allows to select columns from the dataframe from the left of the join condition 
# MAGIC ### Semi right Join 
# MAGIC - Similar to Inner join and Semi right join allows to select fields from dataframe on the right of the join condition

# COMMAND ----------

races_df_semi = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id, "semi")\
    .select(circuits_df.circuit_name,circuits_df.circuit_id,circuits_df.location,circuits_df.country)#,races_df.race_name,races_df.round)

# COMMAND ----------

display(races_df_semi)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Anti Join
# MAGIC - Gives the records from the dataframe on the left which dont have matching records in the right data frame 

# COMMAND ----------

races_df_anti_1 = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id, "anti")
display(races_df_anti_1)

# COMMAND ----------

races_df_anti_2 = races_df.join(circuits_df,circuits_df.circuit_id == races_df.circuit_id, "anti")
display(races_df_anti_2)

# COMMAND ----------

print(races_df.count())
print(circuits_df.count())

# COMMAND ----------

21*69

# COMMAND ----------

races_df_cross = races_df.crossJoin(circuits_df)
display(races_df_cross)

# COMMAND ----------

