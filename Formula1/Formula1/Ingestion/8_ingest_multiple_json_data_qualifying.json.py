# Databricks notebook source
# DBTITLE 1,Importing the important library functions for use later on 
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
import pyspark.sql.functions as f 

# COMMAND ----------

# DBTITLE 1,Exporting configuration notebook to bring in variables
# MAGIC %run ../Includes/configuration

# COMMAND ----------

# DBTITLE 1,Exporting common_functions notebook to bring in functions
# MAGIC %run ../Includes/common_functions

# COMMAND ----------

print(raw_folder_path)

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

# DBTITLE 1,Creating the Schema 
qualifying_schema=StructType(fields = [StructField(("qualifyId"),IntegerType(),True),
                                    StructField(("raceId"),IntegerType(),True),
                                    StructField(("driverId"),IntegerType(),True),
                                    StructField(("constructorId"),IntegerType(),True),
                                    StructField(("number"),IntegerType(),True),
                                    StructField(("position"),IntegerType(),True),
                                    StructField(("q1"),StringType(),True),
                                    StructField(("q2"),StringType(),True),
                                    StructField(("q3"),StringType(),True),
                                    ])

# COMMAND ----------

# DBTITLE 1,Reading the data for .json from raw 
qualifying_df = spark.read.json(f"{raw_folder_path}/{v_file_dt}/qualifying/{v_data_source}",multiLine=True,schema=qualifying_schema)

# COMMAND ----------

qualifying_int_df = qualifying_df.withColumnRenamed("qualifyId","qualify_id")\
    .withColumnRenamed("driverID","driver_id")\
        .withColumnRenamed("raceId","race_id")\
            .withColumnRenamed("constructorId","constructor_id")\
                .withColumn("data_source",f.lit(v_data_source))

qualifying_final_df = add_ingestion_date(qualifying_int_df)\
                        .withColumn("file_date",f.lit(v_file_dt))

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Drop table f1_processed.qualifying
# MAGIC
# MAGIC -- # qualifying_final_df.printSchema()

# COMMAND ----------

# incremental_load(qualifying_final_df,"race_id","f1_processed","qualifying")


merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"

mergeDeltaTable(qualifying_final_df,
                "qualifying",
                 merge_condition,
                "race_id",
                "f1_processed",
                "/mnt/f1azda/processed/")

# COMMAND ----------

# qualifying_final_df.write.parquet(f"{processed_folder_path}/qualifying/",mode ="overwrite")

# qualifying_final_df.write.format("parquet").mode("overwrite").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM f1_processed.qualifying where file_date = '2021-04-18';

# COMMAND ----------

dbutils.notebook.exit("Success")