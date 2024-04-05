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

circuits_df = spark.read.csv(f"{raw_folder_path}/{v_data_source}",header=True,schema=circuits_schema)

# COMMAND ----------

# DBTITLE 1,Selecting the data using the normal select function
circuits_df_selected_1 = circuits_df.select("circuitID","circuitRef","name","location","country","lat","lng","alt")
circuits_df_selected_1.show(5,truncate = False)

# COMMAND ----------

# DBTITLE 1,Making use of the array slice method for selecting columns from the dataset/dataframe
circuits_df_selected_2 = circuits_df.select(circuits_df["circuitID"],circuits_df["circuitRef"],circuits_df["name"],circuits_df["location"],\
                                            circuits_df["country"],circuits_df["lat"],circuits_df["lng"],circuits_df["alt"])
circuits_df_selected_2.show(5,truncate = False)

# COMMAND ----------

# DBTITLE 1, Making use of .(dot) notation to select the columns from the dataset/dataframe
circuits_df_selected_3 = circuits_df.select(circuits_df.circuitID,circuits_df.circuitRef,circuits_df.name,circuits_df.location,\
                                            circuits_df.country,circuits_df.lat,circuits_df.lng,circuits_df.alt)
circuits_df_selected_3.show(5,truncate = False)

# COMMAND ----------

# DBTITLE 1, Making use of col function to select the columns from the dataset/dataframe
circuits_df_selected_4 = circuits_df.select(col("circuitID"),col("circuitRef"),col("name"),col("location"),\
                                            col("country").alias("RaceCountry"),col("lat"),col("lng"),col("alt"))
circuits_df_selected_4.show(5,False)

# COMMAND ----------

# DBTITLE 1,Renaming columns in a dataframe 
circuits_df_renamed = circuits_df_selected_4.withColumnRenamed('circuitID','circuit_id')\
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
circuits_final_df=add_ingestion_date(circuits_df_renamed)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

print(processed_folder_path)

# COMMAND ----------

# DBTITLE 1,Writing a file out to ADLS
# circuits_final_df.write.parquet(f"{processed_folder_path}/circuits",mode='overwrite')
circuits_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.circuits')

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

