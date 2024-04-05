# Databricks notebook source
from pyspark.sql.window import Window
from  pyspark.sql import functions as f

# COMMAND ----------

# MAGIC %run ../Includes/configuration

# COMMAND ----------

# MAGIC %run ../Includes/common_functions

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

races_processed_df=spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
    .filter(f"file_date = '{v_file_date}'") 

# COMMAND ----------

from pyspark.sql.functions import collect_set,col

race_year_set = races_processed_df.select(collect_set("race_year")).head()[0]
races_df = races_processed_df.filter(col("race_year").isin(race_year_set))

# COMMAND ----------

# races_processed_df = races_processed_df.withColumn("wins",f.when(f.col("position") == 1,f.lit(1)).otherwise(f.lit(0)))

# COMMAND ----------

drivers_standings = races_df\
    .groupBy("race_year","driver_id","driver_name","driver_nationality")\
        .agg(f.sum("points").alias("points"),\
             f.sum("wins").alias("wins"))

# COMMAND ----------

windowRnkSpec= Window.partitionBy("race_year").orderBy(f.desc("points"),f.desc("wins"))
drivers_standings_final = drivers_standings.withColumn("Driver_ranking",f.rank().over(windowRnkSpec))

# COMMAND ----------

drivers_standings_final.printSchema()

# COMMAND ----------

# incremental_load(drivers_standings_final,"race_year","f1_presentation","driver_standings")

# COMMAND ----------

merge_condition="src.driver_id = tgt.driver_id AND src.race_year = tgt.race_year "
mergeDeltaTable(drivers_standings_final,"driver_standings",merge_condition,"race_year","f1_presentation",presentation_folder_path) 

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `hive_metastore`; select * from `f1_presentation`.`driver_standings`;

# COMMAND ----------

