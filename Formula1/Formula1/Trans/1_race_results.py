# Databricks notebook source
from pyspark.sql import functions as f

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date =dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run ../Includes/configuration

# COMMAND ----------

# MAGIC %run ../Includes/common_functions

# COMMAND ----------

races_df = spark.read.format("delta").load(f'{processed_folder_path}/races')\
    .select(["race_id","circuit_id","name","race_timestamp","race_year"])\
        .withColumnRenamed("race_id","id")\
            .withColumnRenamed("name", "race_name")\
                .withColumn("race_date",f.split("race_timestamp"," ")[0])\
                    .drop("race_timestamp")
# races_df.show(1,False)

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f'{processed_folder_path}/drivers')\
    .select(["driver_id","name", "nationality", "number"])\
        .withColumnRenamed("driver_id","id")\
            .withColumnRenamed("name", "driver_name")\
                .withColumnRenamed("nationality","driver_nationality")\
                    .withColumnRenamed("number","driver_number")
# drivers_df.show(1,False)

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f'{processed_folder_path}/circuits')\
    .withColumnRenamed("RaceCountry","country")\
        .select(["circuitID","name","location","country"])\
            .withColumnRenamed("circuitID","id")\
                .withColumnRenamed("name","circuit_name")\
                    .withColumnRenamed("location","circuit_location")\
                        .withColumnRenamed("country","circuit_country")
# circuits_df.show(1,False)

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f'{processed_folder_path}/constructors')\
    .select(["constructor_id","name"])\
        .withColumnRenamed("constructor_id","id")\
                        .withColumnRenamed("name","team")
# constructors_df.show(1)

# COMMAND ----------

results_df = spark.read.format("delta").load(f'{processed_folder_path}/results')\
    .filter(f"file_date = '{v_file_date}'")\
        .select(["constructor_id","driver_id","race_id","fastest_lap","grid","time","points","position","file_date"])\
            .withColumnRenamed("time","race_time")\
# results_df.show(1)

# COMMAND ----------

races_final_df = results_df.join(constructors_df,results_df.constructor_id == constructors_df.id,"inner").drop("id")\
    .join(drivers_df,results_df.driver_id == drivers_df.id,"inner").drop("id")\
        .join(races_df,results_df.race_id == races_df.id,"inner").drop("id")\
            .join(circuits_df,races_df.circuit_id == circuits_df.id,"inner").drop("id")\
                .withColumn("circuit_location",f.concat(f.col("circuit_location"),f.lit(", "),f.col("circuit_country")))

races_final_df=add_ingestion_date(races_final_df).withColumnRenamed("ingestion_date","created_date")

# COMMAND ----------

races_final_df = races_final_df.withColumn("wins",f.when(f.col("position") == 1,f.lit(1)).otherwise(f.lit(0)))


# COMMAND ----------

races_final_df = races_final_df.select(["race_id","race_year","race_name","race_date","circuit_name","circuit_location",\
                               "driver_id","driver_name","driver_nationality","team","grid",\
                               "fastest_lap","race_time","position","points","wins",'file_date',"created_date"])



# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# incremental_load(races_final_df,"race_id","f1_presentation","race_results")

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
mergeDeltaTable(races_final_df,"race_results",merge_condition,"race_id","f1_presentation",presentation_folder_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `hive_metastore`; select race_id, count(1)from `f1_presentation`.`race_results` group by race_id order by race_id desc;
# MAGIC --limit 100;

# COMMAND ----------

