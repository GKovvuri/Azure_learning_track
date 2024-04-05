# Databricks notebook source
from pyspark.sql import functions as f
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ../Includes/configuration

# COMMAND ----------

# MAGIC %run ../Includes/common_functions

# COMMAND ----------

dbutils.widgets.text("p_file_date",'2021-03-21')
v_file_date = dbutils.widgets.get("p_file_date")
print(v_file_date)

# COMMAND ----------

race_res_df=spark.read.format("delta").load(f"{presentation_folder_path}/race_results").filter(f.col("file_date")==v_file_date)

# COMMAND ----------

year_list = race_res_df.select(f.collect_set("race_year")).head()[0]
# print(year_list)
race_cons_df= race_res_df.filter(f.col("race_year").isin(year_list))

# COMMAND ----------

constructors_standings=race_cons_df\
    .groupBy("race_year","team")\
        .agg(f.sum("points").alias("points"),\
             f.sum('wins').alias("wins"))

# COMMAND ----------

windowConsRnk = Window.partitionBy("race_year").orderBy(f.desc('points'),f.desc("wins"))
constructors_standings_final = constructors_standings.withColumn("team_ranking",f.rank().over(windowConsRnk))

# COMMAND ----------

# incremental_load(constructors_standings_final,"race_year","f1_presentation","constructor_standings")
merge_condition = "src.team = tgt.team AND src.race_year = tgt.race_year"
mergeDeltaTable(constructors_standings_final,"constructors_standings",merge_condition,"race_year","f1_presentation",presentation_folder_path)

# COMMAND ----------

