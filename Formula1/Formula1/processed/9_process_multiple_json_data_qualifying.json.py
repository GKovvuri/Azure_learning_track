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
qualifying_df = spark.read.json(f"{raw_folder_path}/qualifying/{v_data_source}",multiLine=True,schema=qualifying_schema)

# COMMAND ----------

qualifying_int_df = qualifying_df.withColumnRenamed("qualifyId","qualify_id")\
    .withColumnRenamed("driverID","driver_id")\
        .withColumnRenamed("raceId","race_id")\
            .withColumnRenamed("constructorId","constructor_id")\
                .withColumn("data_source",f.lit(v_data_source))

qualifying_final_df = add_ingestion_date(qualifying_int_df)

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

# qualifying_final_df.write.parquet(f"{processed_folder_path}/qualifying/",mode ="overwrite")
qualifying_final_df.write.format("parquet").mode("overwrite").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")