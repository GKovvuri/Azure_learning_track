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

# DBTITLE 1,Creating the Schema 
circuits_schema=StructType(fields = [StructField(("circuitID"),IntegerType(),True),
                                    StructField(("circuitRef"),StringType(),True),
                                    StructField(("name"),StringType(),True),
                                    StructField(("location"),StringType(),True),
                                    StructField(("country"),StringType(),True),
                                    StructField(("lat"),DoubleType(),True),
                                    StructField(("lng"),DoubleType(),True),
                                    StructField(("alt"),IntegerType(),True),
                                    StructField(("url"),StringType(),True)
                                    ])

# COMMAND ----------

# DBTITLE 1,Reading the data for circuits.csv from raw 
circuits_df = spark.read.csv(f"{raw_folder_path}/{v_file_dt}/{v_data_source}/",header=True,schema=circuits_schema)

# COMMAND ----------

# DBTITLE 1, Making use of col function to select the columns from the dataset/dataframe
circuits_df_selected= circuits_df.select(col("circuitID"),col("circuitRef"),col("name"),col("location"),col("country").alias("RaceCountry"),col("lat"),col("lng"),col("alt"))


# COMMAND ----------

# DBTITLE 1,Renaming columns in a dataframe 
circuits_df_renamed = circuits_df_selected.withColumnRenamed('circuitID','circuit_id')\
    .withColumnRenamed('circuitRef','circuit_ref')\
        .withColumnRenamed('lat','latitude')\
            .withColumnRenamed('lng','longitude')\
                .withColumnRenamed('alt','altitude')\
                    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Adding a column to the existing dataframe using .withColumn operation/function
# MAGIC - We can add a new column and assign a value that in two different ways.
# MAGIC   - withColumn('new_column_name',column value)--> if the column value is a constant value we can convert it using f.lit()

# COMMAND ----------

# DBTITLE 1,Creating a new column with the timestamp of ingestion of the records 
circuits_final_df=add_ingestion_date(circuits_df_renamed)\
                        .withColumn("file_date",lit(v_file_dt))

# COMMAND ----------

# incremental_load(circuits_final_df,"race_id")

# COMMAND ----------

# DBTITLE 1,Writing a file out to ADLS
# circuits_final_df.write.parquet(f"{processed_folder_path}/circuits",mode='overwrite')
# circuits_final_df.write.format("parquet").mode("overwrite").saveAsTable("f1_processed.circuits")

circuits_df.write.format("delta").mode('overwrite').saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

