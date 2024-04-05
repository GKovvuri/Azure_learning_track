# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as f

# COMMAND ----------

pitstop_schema = StructType([StructField("raceId",IntegerType(),False),
                             StructField("driverId",IntegerType(),True),
                             StructField("stop",StringType(),True),
                             StructField("lap",IntegerType(),True),
                             StructField("time",StringType(),True),
                             StructField("duration",StringType(),True),
                             StructField("milliseconds",IntegerType(),True)])

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

pitstops_df = spark.read.json(f"{raw_folder_path}/{v_file_dt}/{v_data_source}",schema=pitstop_schema,multiLine=True)

# COMMAND ----------

pitstops_int_df = pitstops_df.withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
        .withColumn("data_source",f.lit(v_data_source))

pitstops_final_df = add_ingestion_date(pitstops_int_df)\
                        .withColumn("file_date",f.lit(v_file_dt))

# COMMAND ----------

pitstops_final_df.printSchema()

# COMMAND ----------

# incremental_load(pitstops_final_df,"race_id","f1_processed","pitstops")

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop"

mergeDeltaTable(pitstops_final_df,
                "pit_stops",
                 merge_condition,
                "race_id",
                "f1_processed",
                "/mnt/f1azda/processed/")

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT * FROM f1_processed.pit_stops where file_date = "2021-04-18";

# COMMAND ----------

# pitstops_final_df.write.parquet(f"{processed_folder_path}/pitstops",mode = "overwrite")
# pitstops_final_df.write.format('parquet').mode("overwrite").saveAsTable("f1_processed.pitstops")

# COMMAND ----------

dbutils.notebook.exit("Success")