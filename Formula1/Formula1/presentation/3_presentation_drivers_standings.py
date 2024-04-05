# Databricks notebook source
from pyspark.sql.window import Window
from  pyspark.sql import functions as f

# COMMAND ----------

# MAGIC %run ../Includes/configuration

# COMMAND ----------

races_processed_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

races_processed_df = races_processed_df.withColumn("wins",f.when(f.col("position") == 1,f.lit(1)).otherwise(f.lit(0)))

# COMMAND ----------

# races_processed_df.show(5,False)

# COMMAND ----------

drivers_standings = races_processed_df\
    .groupBy("race_year","driver_id","driver_name","driver_nationality","team")\
        .agg(f.sum("points").alias("points"),\
             f.sum("wins").alias("wins"))

# COMMAND ----------

# display(drivers_standings.filter("race_year = 2020").orderBy(f.desc("race_year"),f.desc("points")))

# COMMAND ----------

windowRnkSpec= Window.partitionBy("race_year").orderBy(f.desc("points"),f.desc("wins"))
drivers_standings_final = drivers_standings.withColumn("Driver_ranking",f.rank().over(windowRnkSpec))

# COMMAND ----------

# display(drivers_standings_final)

# COMMAND ----------

# drivers_standings_final.write.parquet(f"{presentation_folder_path}/driver_standings",mode="overwrite")

drivers_standings_final.write.format("parquet").mode("overwrite").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------

