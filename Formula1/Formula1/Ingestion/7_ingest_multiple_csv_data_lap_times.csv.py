# Databricks notebook source
# DBTITLE 1,Importing the important library functions for use later on
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql.functions import col,current_timestamp, to_timestamp,concat,lit

# COMMAND ----------

# DBTITLE 1,Creating the Schema 
lap_time_schema=StructType(fields = [StructField(("raceID"),IntegerType(),True),
                                    StructField(("driverID"),IntegerType(),True),
                                    StructField(("lap"),IntegerType(),True),
                                    StructField(("position"),IntegerType(),True),
                                    StructField(("time"),StringType(),True),
                                    StructField(("milliseconds"),IntegerType(),True)
                                    ])

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

# DBTITLE 1,Reading the data for circuits.csv from raw 
laptime_df = spark.read.csv(f"{raw_folder_path}/{v_file_dt}/lap_times/{v_data_source}",header=True,schema=lap_time_schema)

# COMMAND ----------

laptime_df_int = laptime_df.withColumnRenamed("raceID","race_id")\
    .withColumnRenamed("driverID","driver_id")\
        .withColumn("data_source",lit(v_data_source))

laptime_final_df = add_ingestion_date(laptime_df_int)\
                        .withColumn("file_date",lit(v_file_dt))

# COMMAND ----------

# %sql
# -- drop table f1_processed.laptimes

laptime_final_df.printSchema()

# COMMAND ----------

# incremental_load(laptime_final_df,"race_id","f1_processed","laptimes")

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap"

mergeDeltaTable(laptime_final_df,
                "laptimes",
                 merge_condition, 
                "race_id",
                "f1_processed",
                "/mnt/f1azda/processed/")

# COMMAND ----------

# laptime_final_df.write.parquet(f"{processed_folder_path}/laptimes/",mode ="overwrite")
# laptime_final_df.write.format('parquet').mode("overwrite").saveAsTable("f1_processed.laptimes")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_processed.laptimes where file_date='2021-04-18'

# COMMAND ----------

dbutils.notebook.exit("Success")