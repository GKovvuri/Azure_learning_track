-- Databricks notebook source
-- DBTITLE 1,DROPPING THE TABLES IN F1_PROCESSED DB
DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/f1azda/processed"

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/f1azda/presentation"

-- COMMAND ----------

