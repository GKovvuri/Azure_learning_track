-- Databricks notebook source
-- MAGIC %md 
-- MAGIC
-- MAGIC - Spark SQL documentation
-- MAGIC - Create a Database
-- MAGIC - Data Tab in the UI
-- MAGIC - SHOW command in SQL 
-- MAGIC - DESC / DESCRIBE command
-- MAGIC - Find the current Database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE DEMO

-- COMMAND ----------

DESC DATABASE EXTENDED DEMO;  -- can be used on a DATABASE 

-- COMMAND ----------

USE DEMO;
SELECT CURRENT_DATABASE();

-- COMMAND ----------

SHOW TABLES IN DEMO; -- SHOWING ALL THE TABLES BORN AND WITHIN THE SPECIFIED DATABASE

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## MANAGED AND EXTERNAL TABLE
-- MAGIC
-- MAGIC - There are to two types of tables : 1) Managed Tables 2) External Table
-- MAGIC   - Managed table metadata is stored in a hive metastore and the data is stored in ADLS
-- MAGIC   - External table metadata is stored in a hive metastore and the data is stored in an external location for example in s3 bucket 
-- MAGIC - Difference between those both are when a managed table is deleted both the metadata and the actual data is also deleted where as 
-- MAGIC in external table during the data deletion the metadata is deleted from the hive metastore where as the data is available.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ../Includes/configuration

-- COMMAND ----------

-- DBTITLE 1,OBJ1:  CREATING A MANAGED TABLE USING PYTHON
-- MAGIC %python 
-- MAGIC sql_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
-- MAGIC display(sql_df)
-- MAGIC sql_df.write.format("parquet").mode("overwrite").saveAsTable("demo.race_results_python")

-- COMMAND ----------

use demo;
show tables

-- COMMAND ----------

DESCRIBE EXTENDED race_results_python

-- COMMAND ----------

SELECT * 
  FROM DEMO.RACE_RESULTS_PYTHON 
  LIMIT 10

-- COMMAND ----------

CREATE TABLE DEMO.RACE_RESULTS_SQL 
AS 
  SELECT * FROM DEMO.RACE_RESULTS_PYTHON  
    WHERE race_year =2020;

-- COMMAND ----------

USE DEMO;
SHOW TABLES;

-- COMMAND ----------

drop table demo.race_results_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # External Table
-- MAGIC - Create External table using Python
-- MAGIC - Create External table using SQL.
-- MAGIC - Effect of dropping an External Table
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ../Includes/configuration

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql_df =  spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql_df.write.format("parquet").option("path",f"{demo_folder}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_SQL (
  constructor_id INT,
  driver_id INT,
  race_id INT,
  fastest_lap INT,
  grid INT,
  race_time STRING,
  points INT,
  position INT,
  team STRING,
  driver_name STRING,
  driver_nationality STRING,
  driver_number INT,
  circuit_id INT,
  race_name STRING,
  race_year INT,
  race_date string,
  circuit_name STRING,
  circuit_location STRING,
  circuit_country STRING,
  created_date timestamp,
  wins INT
)
USING PARQUET
LOCATION "/mnt/f1azda/presentation/race_results_ext_sql"

-- COMMAND ----------

INSERT 
  INTO demo.race_results_ext_sql
    SELECT * 
      FROM demo.race_results_ext_py
        WHERE race_year = 2020;

-- COMMAND ----------

select count(1) from demo.race_results_ext_sql;

-- COMMAND ----------

DESC EXTENDED DEMO.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES IN DEMO;

-- COMMAND ----------

drop table demo.race_results_ext_sql;

-- COMMAND ----------

show tables in demo