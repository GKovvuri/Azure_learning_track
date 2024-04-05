# Databricks notebook source
# DBTITLE 1,Importing the important library functions for use later on 
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,StringType,DoubleType
from pyspark.sql.functions import col, to_timestamp,concat,lit

# COMMAND ----------

# MAGIC %run ../Includes/configuration

# COMMAND ----------

# MAGIC %run ../Includes/common_functions

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")
print(v_data_source)

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_dt = dbutils.widgets.get('p_file_date')
print(v_file_dt)

# COMMAND ----------

# DBTITLE 1,Races file read
if v_data_source == "races.csv":
    races_df=spark.read.csv(f"{raw_folder_path}/{v_file_dt}/{v_data_source}",inferSchema=True, header = True)
else:
    print("Check the datasource file name")

# COMMAND ----------

# DBTITLE 1,Transforming the dataframe and dropping the unwanted records from the dataframe
races_df_final = races_df.withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '),col('time')),"yyyy-MM-dd HH:mm:ss"))\
    .withColumnRenamed('raceId', 'race_id')\
        .withColumnRenamed('circuitId','circuit_id')\
            .withColumnRenamed('year','race_year')\
                .drop('date','time','url')\
                    .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

races_df_final = add_ingestion_date(races_df_final)\
                        .withColumn("file_date",lit(v_file_dt))

# COMMAND ----------

# DBTITLE 1,Write the file into processed layer
# races_df_final.write.parquet(f"{processed_folder_path}/races",mode='overwrite',partitionBy='race_year')
races_df_final.write.format("delta").mode("overwrite").partitionBy('race_year').saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("Success")