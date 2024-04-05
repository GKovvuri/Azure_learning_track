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

pitstops_df = spark.read.json(f"{raw_folder_path}/{v_data_source}",schema=pitstop_schema,multiLine=True)

# COMMAND ----------

display(pitstops_df)

# COMMAND ----------

pitstops_int_df = pitstops_df.withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
        .withColumn("data_source",f.lit(v_data_source))

pitstops_final_df = add_ingestion_date(pitstops_int_df)

# COMMAND ----------

display(pitstops_final_df)

# COMMAND ----------

# pitstops_final_df.write.parquet(f"{processed_folder_path}/pitstops",mode = "overwrite")

pitstops_final_df.write.format("parquet").mode("overwrite").saveAsTable("f1_processed.pitstops")

# COMMAND ----------

display(spark.read.parquet('/mnt/f1azda/processed/pitstops/'))

# COMMAND ----------

dbutils.notebook.exit("Success")