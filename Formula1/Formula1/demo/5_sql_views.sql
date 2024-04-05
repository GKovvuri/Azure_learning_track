-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Views
-- MAGIC
-- MAGIC - Creating views on tables to created curated datasets for various requirements set by the downstream teams.
-- MAGIC
-- MAGIC #### Types of views
-- MAGIC
-- MAGIC - Local Temp View -- v_race_results
-- MAGIC   - Can be accessed only in the current workspace in which the view is created.
-- MAGIC - Global Temp View -- gv_race_results
-- MAGIC   - Can be accessed across multiple workbooks once created 
-- MAGIC   - Code is stored in the global_temp database 
-- MAGIC - Permanent View --pv_race_results

-- COMMAND ----------

-- local Temp view syntax

CREATE OR REPLACE TEMP VIEW v_race_results
AS 
SELECT * FROM DEMO.RACE_RESULTS_PYTHON

-- COMMAND ----------

SELECT * FROM v_race_results;

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS 
SELECT * FROM DEMO.race_results_python
WHERE race_year =2020;

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS 
SELECT * FROM demo.race_results_python
WHERE race_year = 2020


-- COMMAND ----------

