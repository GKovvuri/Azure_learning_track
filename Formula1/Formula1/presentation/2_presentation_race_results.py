# Databricks notebook source
from pyspark.sql import functions as f

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %run ../Includes/configuration

# COMMAND ----------

# MAGIC %run ../Includes/common_functions

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races')\
    .select(["race_id","circuit_id","name","race_timestamp","race_year"])\
        .withColumnRenamed("race_id","id")\
            .withColumnRenamed("name", "race_name")\
                .withColumn("race_date",f.split("race_timestamp"," ")[0])\
                    .drop("race_timestamp")
# races_df.show(1,False)

# COMMAND ----------

drivers_df = spark.read.parquet(f'{processed_folder_path}/drivers')\
    .select(["driver_id","name", "nationality", "number"])\
        .withColumnRenamed("driver_id","id")\
            .withColumnRenamed("name", "driver_name")\
                .withColumnRenamed("nationality","driver_nationality")\
                    .withColumnRenamed("number","driver_number")
# drivers_df.show(1,False)

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits')\
    .withColumnRenamed("RaceCountry","country")\
        .select(["circuit_id","name","location","country"])\
            .withColumnRenamed("circuit_id","id")\
                .withColumnRenamed("name","circuit_name")\
                    .withColumnRenamed("location","circuit_location")\
                        .withColumnRenamed("country","circuit_country")
# circuits_df.show(1,False)

# COMMAND ----------

constructors_df = spark.read.parquet(f'{processed_folder_path}/constructors')\
    .select(["constructor_id","name"])\
        .withColumnRenamed("constructor_id","id")\
                        .withColumnRenamed("name","team")
# constructors_df.show(1)

# COMMAND ----------

results_df = spark.read.parquet(f'{processed_folder_path}/results')\
    .select(["constructor_id","driver_id","race_id","fastest_lap","grid","time","points","position"])\
        .withColumnRenamed("time","race_time")
# results_df.show(1)

# COMMAND ----------

races_final_df = results_df.join(constructors_df,results_df.constructor_id == constructors_df.id,"inner").drop("id")\
    .join(drivers_df,results_df.driver_id == drivers_df.id,"inner").drop("id")\
        .join(races_df,results_df.race_id == races_df.id,"inner").drop("id")\
            .join(circuits_df,races_df.circuit_id == circuits_df.id,"inner").drop("id")\
                .withColumn("circuit_location",f.concat(f.col("circuit_location"),f.lit(", "),f.col("circuit_country")))

races_final_df=add_ingestion_date(races_final_df).withColumnRenamed("ingestion_date","created_date")

# COMMAND ----------

display(races_final_df.select(["race_year","race_name","race_date","circuit_name","circuit_location",\
                               "driver_name","driver_nationality","team","grid",\
                               "fastest_lap","race_time","position","points","created_date"]))

# COMMAND ----------

races_final_df = races_final_df.withColumn("wins",f.when(f.col("position") == 1,f.lit(1)).otherwise(f.lit(0)))

# COMMAND ----------

# races_final_df.write.parquet(f"{presentation_folder_path}/race_results",mode="overwrite")

races_final_df.write.format("parquet").mode("overwrite").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

# races_final_df.show(5,False)