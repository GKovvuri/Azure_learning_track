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

# DBTITLE 0,t
results_schema =StructType([StructField("constructorId", IntegerType(),True),
                            StructField("driverId",IntegerType(), True),
                            StructField("fastestLap",IntegerType(),True),
                            StructField("fastestLapSpeed",StringType(),True),
                            StructField("fastestLapTime",StringType(),True),
                            StructField("grid",IntegerType(),True),
                            StructField("laps",IntegerType(),True),
                            StructField("milliseconds",IntegerType(),True),
                            StructField("number",IntegerType(),True),
                            StructField("points",FloatType(),True),
                            StructField("position",IntegerType(),True),
                            StructField("positionOrder",IntegerType(),True),
                            StructField("positionText",StringType(),True),
                            StructField("raceId",IntegerType(),True),
                            StructField("rank",IntegerType(),True),
                            StructField("resultId",IntegerType(),True),
                            StructField("statusId",IntegerType(),True),
                            StructField("time",StringType(),True)])

# COMMAND ----------

results_df=spark.read.json(f"{raw_folder_path}/{v_data_source}",schema = results_schema)

# COMMAND ----------

display(results_df)

# COMMAND ----------

results_df_int=results_df.withColumnRenamed("constructorId","constructor_id")\
    .withColumnRenamed("driverId","driver_id")\
        .withColumnRenamed("raceId","race_id")\
            .withColumnRenamed("resultId","result_id")\
                .withColumnRenamed("positionText","position_text")\
                    .withColumnRenamed("positionOrder","position_order")\
                        .withColumnRenamed("fastestLap","fastest_lap")\
                            .withColumnRenamed("fastestLapTime","fastest_lap_time")\
                                .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                                    .withColumn("data_source", f.lit(v_data_source))\
                                        .drop("statusId")

results_df_final=add_ingestion_date(results_df_int)


# COMMAND ----------

display(results_df_final)

# COMMAND ----------

# results_df_final.write.parquet(f"{processed_folder_path}/results",mode='overwrite',partitionBy="race_id")

results_df_final.write.format("parquet").mode("overwrite").saveAsTable("f1_processed.results")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/results").filter(f.col("race_id")==800))

# COMMAND ----------

dbutils.notebook.exit("Success")