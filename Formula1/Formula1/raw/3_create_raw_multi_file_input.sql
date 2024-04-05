-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ## Ingesting multiple raw files of the same format 
-- MAGIC
-- MAGIC #### Laptimes (CSV)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.LAPTIME;

CREATE TABLE IF NOT EXISTS f1_raw.LAPTIME(
  raceID INT,
  driverID INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
using csv
OPTIONS (PATH "/mnt/f1azda/raw/lap_times/",multiLine True)

-- COMMAND ----------

select * from f1_raw.LAPTIME LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Qualifying (JSON)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  qualifyId integer,
  raceId integer,
  driverId integer,
  constructorId integer,
  number integer,
  position integer,
  q1 STRING,
  q2 STRING,
  q3 STRING)
USING json
OPTIONS (path "/mnt/f1azda/raw/qualifying/", multiLine True)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying;

-- COMMAND ----------

