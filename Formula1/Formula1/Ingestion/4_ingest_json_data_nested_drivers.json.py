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

driver_schema = StructType([StructField("driverId", IntegerType(),False),
                            StructField("driverRef",StringType(), False),
                            StructField("number",IntegerType(),False),
                            StructField("code",StringType(),False),
                            StructField("name",name_schema),
                            StructField("dob",DateType(),False),
                            StructField("nationality",StringType(),False),
                            StructField("url",StringType(),False)])

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

# DBTITLE 0,Untitledt
drivers_df = spark.read.json(f'{raw_folder_path}/{v_file_dt}/{v_data_source}',schema=driver_schema)

# COMMAND ----------

drivers_df = drivers_df.drop("url")

# COMMAND ----------

drivers_int_df = drivers_df.withColumnRenamed("driverID","driver_id")\
    .withColumnRenamed("driverRef","driver_ref")\
        .withColumn('name',f.concat(f.col("name.forename"),f.lit(" "),f.col("name.surname")))\
            .withColumn("data_source",f.lit(v_data_source))

drivers_final_df = add_ingestion_date(drivers_int_df)\
                        .withColumn("file_date",f.lit(v_file_dt))

# COMMAND ----------

# drivers_final_df.write.parquet(f"{processed_folder_path}/drivers",mode="overwrite")
# drivers_final_df.write.format('parquet').mode("overwrite").saveAsTable("f1_processed.drivers")
drivers_final_df.write.format("delta").mode("overwrite").saveAsTable('f1_processed.drivers')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * from f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

