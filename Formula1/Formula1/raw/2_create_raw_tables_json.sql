-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###### CREATING TABLES FOR JSON FILES
-- MAGIC

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ### Constructors file
-- MAGIC - /mnt/f1azda/raw/constructors.json
-- MAGIC   - Single Line JSON structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;

CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
using JSON
OPTIONS (path "/mnt/f1azda/raw/constructors.json")

-- COMMAND ----------

SELECT * FROM f1_raw.constructors LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## JSON Drivers file
-- MAGIC - /mnt/f1azda/raw/drivers.json
-- MAGIC   - Complex structure(Nested columns)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers_test;

CREATE TABLE IF NOT EXISTS f1_raw.drivers_test(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename:STRING,surname: STRING>,
  dob date,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS (path "/mnt/f1azda/raw/drivers.json")

-- COMMAND ----------

SELECT * FROM f1_raw.drivers_test

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Results table
-- MAGIC - /mnt/f1azda/raw/results.json
-- MAGIC   - simple structure
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;

CREATE TABLE IF NOT EXISTS f1_raw.RESULTS(
  constructorId INTEGER,
  driverId INT,
  fastestLap INT,
  fastestLapSpeed STRING,
  fastestLapTime STRING,
  grid INT,
  laps INT,
  milliseconds INT,
  number INT,
  points FLOAT,
  position INT,
  positionOrder INT,
  positionText STRING,
  raceId INT,
  rank INT,
  resultId INT,
  statusId INT,
  time STRING
)
using JSON
OPTIONS (path "/mnt/f1azda/raw/results.json")

-- COMMAND ----------

SELECT * FROM f1_raw.RESULTS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## PitStops table 
-- MAGIC - Multi-line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.PITSTOPS;

CREATE TABLE IF NOT EXISTS f1_raw.PITSTOPS(
  raceId INT,
  driverId INT,
  stop String,
  lap INT,
  time STRING,
  duration STRING,
  milliseconds INT
)
using JSON
OPTIONS(path "/mnt/f1azda/raw/pit_stops.json", multiLine True)

-- COMMAND ----------

select * from f1_raw.PITSTOPS

-- COMMAND ----------

