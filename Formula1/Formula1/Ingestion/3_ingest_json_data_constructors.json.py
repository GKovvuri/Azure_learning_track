# Databricks notebook source
# DBTITLE 1,Ingest a json file 
# MAGIC %md 
# MAGIC ###### Ingest a simple JSON file using the spark reader API. 
# MAGIC ###### Another way of defining the schema for a dataframe --> DDL method
# MAGIC ###### Writing the file as a parquet file into the processed zone 

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING , name STRING, nationality STRING, url STRING"
# this is new form of declaring the schema of the input data 

# COMMAND ----------

# DBTITLE 1,Importing Variables from another notebook
# MAGIC %run ../Includes/configuration

# COMMAND ----------

# MAGIC %run ../Includes/common_functions

# COMMAND ----------

# DBTITLE 1,Adding a runtime parameter using widgets
dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")
print(v_data_source)

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_dt = dbutils.widgets.get('p_file_date')
print(v_file_dt)

# COMMAND ----------

# DBTITLE 0,Untitledt
# 
constructor_df = spark.read.json(f'{raw_folder_path}/{v_file_dt}/{v_data_source}',schema=constructors_schema)

# COMMAND ----------

constructor_final_df = constructor_df.drop("url")\
    .withColumnRenamed("constructorId","constructor_id")\
        .withColumnRenamed("constructorRef","constructor_ref")\
            .withColumn("data_source", f.lit(v_data_source))

# addition of the code block below to call another notebook from the above mentioned common_functions.ipynb file 

constructor_final_df= add_ingestion_date(constructor_final_df)\
                        .withColumn("file_date",f.lit(v_file_dt))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# constructor_final_df.write.parquet(f"{processed_folder_path}/constructors",mode="overwrite")
constructor_final_df.write.format("delta").mode("overwrite").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit("Success")