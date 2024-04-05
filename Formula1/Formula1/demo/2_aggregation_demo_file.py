# Databricks notebook source
# MAGIC %run ../Includes/configuration

# COMMAND ----------

race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

demo_results = race_results.filter("race_year = 2020")

# COMMAND ----------

demo_results.show(5,False)

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

demo_results.select(f.count("*")).show()

# COMMAND ----------

demo_results.select(f.countDistinct("race_name")).show()

# COMMAND ----------

demo_results.select(f.sum("points")).show()

# COMMAND ----------

demo_results.filter("driver_name = 'Lewis Hamilton'").select(f.sum("points").alias('Total Points'),f.countDistinct('race_name').alias("No. of Races")).show()

# COMMAND ----------

demo_results\
    .groupBy('driver_name')\
        .agg(f.sum("points")\
            .alias('Total Points'),f.countDistinct('race_name').alias("No. of Races"))\
                .sort(f.desc("Total Points"))\
                    .show()

# COMMAND ----------

demo_results_2 = race_results.filter("race_year in (2019,2020)")

# COMMAND ----------

demo_results_2_final=demo_results_2\
    .groupBy('driver_name','race_year')\
        .agg(f.sum("points")\
            .alias('Total Points'),f.countDistinct('race_name').alias("No. of Races"))\
                .sort('race_year',f.desc("Total Points"))
            

# COMMAND ----------

from pyspark.sql.window import Window

driver_rank_spec=Window.partitionBy("race_year").orderBy(f.desc("Total Points"))
demo_grouped_df = demo_results_2_final.withColumn("rank", f.rank().over(driver_rank_spec))

# COMMAND ----------

display(demo_grouped_df)

# COMMAND ----------

