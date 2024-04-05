# Databricks notebook source
# MAGIC %md 
# MAGIC 1. Write data on to a delta lake (managed table)
# MAGIC 2. Write data on to a delta lake (external table)
# MAGIC 3. Read data from a delta lake (Table)
# MAGIC 4. Read data from a delta lake (file)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS F1_DEMO_DELTA
# MAGIC LOCATION '/mnt/f1azda/demo'

# COMMAND ----------

results_df = spark.read
                    .option("inferSchema",True)\
                    .json('/mnt/f1azda/raw/2021-03-28/results.json')

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo_delta.results_managed")

# COMMAND ----------

# %sql 
# DROP TABLE f1_demo_delta.results;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_demo_delta.results_managed;

# COMMAND ----------

# DBTITLE 1,Save table directly to a location rather than as a table
results_df.write.format("delta").mode("overwrite").save("/mnt/f1azda/demo/results_external")

# COMMAND ----------

# DBTITLE 1,after converting the parquet file into a delta format, we need to create a table from that file
# MAGIC %sql
# MAGIC CREATE TABLE f1_demo_delta.results_external
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/f1azda/demo/results_external"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo_delta.results_external order by ;

# COMMAND ----------

df_results_ext = spark.read.format('delta').load("/mnt/f1azda/demo/results_external/")

# COMMAND ----------

display(df_results_ext)

# COMMAND ----------

# DBTITLE 1,Creating a partitioned delta table from anohter parquet file
results_df.write.partitionBy("constructorId").format("delta").mode('overwrite').saveAsTable("f1_demo_delta.results_partitioned")

# COMMAND ----------

# MAGIC %sql 
# MAGIC show partitions f1_demo_delta.results_partitioned

# COMMAND ----------

# DBTITLE 1,Update and delete from a delta table
# MAGIC %sql 
# MAGIC -- in a datalake we could not delete or update data in any table which is has been overcomed in a delta table
# MAGIC
# MAGIC SELECT * FROM f1_demo_delta.results_managed; -- created by the saveAsTable command 
# MAGIC
# MAGIC --  updating a delta table by using SQL 
# MAGIC UPDATE f1_demo_delta.results_managed 
# MAGIC SET points = 11-POSITION
# MAGIC WHERE POSITION <11;
# MAGIC
# MAGIC SELECT * FROM f1_demo_delta.results_managed;

# COMMAND ----------

# DBTITLE 1,UPDATE TABLE USING PYTHON COMMAND

from delta.tables import * 

delta_table = DeltaTable.forPath(spark,"/mnt/f1azda/demo/results_managed")

delta_table.update("position<11",{"points":"21-position"})

# COMMAND ----------

# DBTITLE 1,Time travel command
# MAGIC %sql
# MAGIC -- RESTORE TABLE f1_demo_delta.results_managed TO TIMESTAMP AS OF "2024-03-20 14:10:50.0";
# MAGIC -- SELECT * FROM f1_demo_delta.results_managed;
# MAGIC -- To find out the history of a table (manamged or external use the below commands)
# MAGIC -- describe history f1_demo_delta.results_managed;
# MAGIC -- DESCRIBE HISTORY '/mnt/f1azda/demo/results_managed'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DELETE FROM f1_demo_delta.results_managed WHERE POSITION >10;
# MAGIC
# MAGIC --  "2024-03-20 14:10:50.0" and '2024-03-20 15:59:35.0' 

# COMMAND ----------

# DBTITLE 1,Delete delta table using python
from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark,"/mnt/f1azda/demo/results_managed")
delta_table.delete("position >=10")

# COMMAND ----------

# DBTITLE 1,Merge or Upsert statements
# Upsert using Merge 

# reading in the first 10 records of the drivers dataset from 03/28

drivers_day1=spark.read\
            .option('inferSchema',True)\
            .format('json')\
            .load('/mnt/f1azda/raw/2021-03-28/drivers.json')\
            .filter("driverId <= 10")\
            .select("driverId","dob","name.forename","name.surname")

# COMMAND ----------

drivers_day1.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

import pyspark.sql.functions as f

drivers_day2=spark.read\
            .option('inferSchema',True)\
            .format('json')\
            .load('/mnt/f1azda/raw/2021-03-28/drivers.json')\
            .filter("driverId between 6 AND 15")\
            .select("driverId","dob",f.upper("name.forename").alias("forename"),f.upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day2.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

drivers_day3=spark.read\
            .option('inferSchema',True)\
            .format('json')\
            .load('/mnt/f1azda/raw/2021-03-28/drivers.json')\
            .filter("driverId BETWEEN 1 and 5 OR driverId BETWEEN 16 and 20")\
            .select("driverId","dob",f.upper("name.forename").alias("forename"),f.upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day3.createOrReplaceTempView("drivers_day3")

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE f1_demo_delta.drivers_day_merge;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo_delta.drivers_day_merge(
# MAGIC   driverId INT, 
# MAGIC   dob TIMESTAMP,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate TIMESTAMP, 
# MAGIC   updatedDate TIMESTAMP 
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql 
# MAGIC MERGE INTO f1_demo_delta.drivers_day_merge D
# MAGIC USING drivers_day2 D_upd
# MAGIC ON D_upd.driverId =D.driverId
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE  SET D.dob = D_upd.dob,
# MAGIC             D.forename =D_upd.forename,
# MAGIC             D.surname = D_upd.surname,
# MAGIC             D.updatedDate = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (driverId,dob,forename,surname,createdDate) values (driverId,dob,forename,surname,current_timestamp)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo_delta.drivers_day_merge order by driverId;

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

delta_table = DeltaTable.forPath(spark,'/mnt/f1azda/demo/drivers_day_merge')

delta_table.alias('D').merge(drivers_day3.alias('D_upd'),"D.driverId = D_upd.driverId")\
                      .whenMatchedUpdate(set= { "D.dob" : "D_upd.dob",\
                                                "D.forename" : "D_upd.forename",\
                                                "D.surname" : "D_upd.surname",\
                                                "D.updatedDate" : "current_timestamp()"})\
                      .whenNotMatchedInsert(values= { "D.driverId" : "D_upd.driverId",
                                                "D.dob" : "D_upd.dob",
                                                "D.forename" : "D_upd.forename",
                                                "D.surname" : "D_upd.surname",
                                                "D.createdDate" : "current_timestamp()"})\
                      .execute()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo_delta.drivers_day_merge order by driverId

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY f1_demo_delta.drivers_day_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_delta.drivers_day_merge VERSION AS OF 2 ORDER BY driverId ;

# COMMAND ----------

df_old = spark.read.format('delta').option("timeStampAsOf","2024-03-20T21:49:09.000+00:00").load("/mnt/f1azda/demo/drivers_day_merge")
display(df_old)

# COMMAND ----------

df_old = spark.read.format('delta').option("VersionAsOf",2).load("/mnt/f1azda/demo/drivers_day_merge")
display(df_old)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo_delta.drivers_day_merge RETAIN 0 HOURS 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_delta.drivers_day_merge 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_delta.drivers_day_merge TIMESTAMP AS OF "2024-03-20T21:49:09.000+00:00"

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo_delta.drivers_txn_log_chk(
# MAGIC   driverId INT, 
# MAGIC   dob TIMESTAMP,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate TIMESTAMP, 
# MAGIC   updatedDate TIMESTAMP 
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into f1_demo_delta.drivers_txn_log_chk 
# MAGIC select * from f1_demo_delta.drivers_day_merge
# MAGIC where driverId = 2 ;

# COMMAND ----------

# MAGIC %sql 
# MAGIC Select * from f1_demo_delta.drivers_txn_log_chk

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from  f1_demo_delta.drivers_txn_log_chk where driverId = 2;

# COMMAND ----------

for driver in range(2,20):
    spark.sql(f"Insert into f1_demo_delta.drivers_txn_log_chk select * from f1_demo_delta.drivers_day_merge where driverId = {driver}")

# COMMAND ----------

