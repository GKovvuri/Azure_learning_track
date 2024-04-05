# Databricks notebook source
from pyspark.sql import functions as f
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ../Includes/configuration

# COMMAND ----------

race_res_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# race_res_df.show(5)

# COMMAND ----------

constructors_standings=race_res_df\
    .groupBy("race_year","team")\
        .agg(f.sum("points").alias("points"),\
             f.sum('wins').alias("wins"))

# COMMAND ----------

windowConsRnk = Window.partitionBy("race_year").orderBy(f.desc('points'),f.desc("wins"))
constructors_standings_final = constructors_standings.withColumn("team_ranking",f.rank().over(windowConsRnk))

# COMMAND ----------

display(constructors_standings_final.filter("race_year =2020"))

# COMMAND ----------

constructors_standings_final.write.format('parquet').mode('overwrite').saveAsTable("f1_presentation.constructor_standings")

# COMMAND ----------

