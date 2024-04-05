# Databricks notebook source
# DBTITLE 1,Ingest a nested json file 
# MAGIC %md 
# MAGIC ###### Ingest a simple JSON file using the spark reader API. 
# MAGIC ###### Another way of defining the schema for a dataframe --> DDL method
# MAGIC ###### Writing the file as a parquet file into the processed zone

# COMMAND ----------

# DBTITLE 1,Importing necessary Libraries
from pyspark.sql import functions as f
from pyspark.sql.types import StructType,StructField,IntegerType, StringType,DateType,FloatType,TimestampType

# COMMAND ----------

# DBTITLE 1,Creating an inner schema for 
name_schema = StructType([StructField("forename",StringType(),False),
                          StructField("surname",StringType(),False)])

# COMMAND ----------

# DBTITLE 1,Exporting configuration notebook to bring in variables
# MAGIC %run ../Includes/configuration

# COMMAND ----------

# DBTITLE 1,Exporting common_functions notebook to bring in functions
# MAGIC %run ../Includes/common_functions

# COMMAND ----------

# DBTITLE 1,Creating a run time parameter to specify the source data 
dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")
print(v_data_source)

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_dt = dbutils.widgets.get('p_file_date')
print(v_file_dt)

# COMMAND ----------

# DBTITLE 0,t
# MAGIC %%time
# MAGIC results_schema =StructType([StructField("constructorId", IntegerType(),True),
# MAGIC                             StructField("driverId",IntegerType(), True),
# MAGIC                             StructField("fastestLap",IntegerType(),True),
# MAGIC                             StructField("fastestLapSpeed",StringType(),True),
# MAGIC                             StructField("fastestLapTime",StringType(),True),
# MAGIC                             StructField("grid",IntegerType(),True),
# MAGIC                             StructField("laps",IntegerType(),True),
# MAGIC                             StructField("milliseconds",IntegerType(),True),
# MAGIC                             StructField("number",IntegerType(),True),
# MAGIC                             StructField("points",FloatType(),True),
# MAGIC                             StructField("position",IntegerType(),True),
# MAGIC                             StructField("positionOrder",IntegerType(),True),
# MAGIC                             StructField("positionText",StringType(),True),
# MAGIC                             StructField("raceId",IntegerType(),True),
# MAGIC                             StructField("rank",IntegerType(),True),
# MAGIC                             StructField("resultId",IntegerType(),True),
# MAGIC                             StructField("statusId",IntegerType(),True),
# MAGIC                             StructField("time",StringType(),True)])

# COMMAND ----------

# MAGIC %%time
# MAGIC results_df=spark.read.json(f"{raw_folder_path}/{v_file_dt}/{v_data_source}",schema = results_schema)

# COMMAND ----------

# MAGIC %%time
# MAGIC results_df_int=results_df.withColumnRenamed("constructorId","constructor_id")\
# MAGIC     .withColumnRenamed("driverId","driver_id")\
# MAGIC         .withColumnRenamed("raceId","race_id")\
# MAGIC             .withColumnRenamed("resultId","result_id")\
# MAGIC                 .withColumnRenamed("positionText","position_text")\
# MAGIC                     .withColumnRenamed("positionOrder","position_order")\
# MAGIC                         .withColumnRenamed("fastestLap","fastest_lap")\
# MAGIC                             .withColumnRenamed("fastestLapTime","fastest_lap_time")\
# MAGIC                                 .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
# MAGIC                                     .withColumn("data_source", f.lit(v_data_source))\
# MAGIC                                         .drop("statusId")
# MAGIC
# MAGIC results_df_final=add_ingestion_date(results_df_int)\
# MAGIC                         .withColumn("file_date",f.lit(v_file_dt))
# MAGIC

# COMMAND ----------

results_df_final_de_dup = results_df_final.dropDuplicates(['race_id',"driver_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 1 (Only using append mode to write data)

# COMMAND ----------

# results_df_final.write.parquet(f"{processed_folder_path}/results",mode='overwrite',partitionBy="race_id")
# results_df_final.write.format('parquet').mode("append").partitionBy("race_id").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2 (Using dynamic overwrite mode mode to insert and update data using insertinto and saprk dynamic overwrite method)

# COMMAND ----------

# MAGIC %%time
# MAGIC # incremental_load(results_df_final,"race_id","f1_processed","results")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Method 3 (Using the merge API for delta table)

# COMMAND ----------

mergeDeltaTable(results_df_final_de_dup,
                'results',
                "tgt.result_id = src.result_id AND tgt.race_id = src.race_id",
                "race_id",
                "f1_processed",
                "/mnt/f1azda/processed/")

# COMMAND ----------

# # %sql
# -- %%time
# -- drop table f1_processed.results;

# COMMAND ----------

# MAGIC %sql
# MAGIC select r.race_id, r.driver_id,count(1) from  f1_processed.results r
# MAGIC group by r.race_id, r.driver_id 
# MAGIC having count(1)>1;

# COMMAND ----------

dbutils.notebook.exit("Success")