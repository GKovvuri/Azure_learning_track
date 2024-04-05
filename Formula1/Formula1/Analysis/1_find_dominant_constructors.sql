-- Databricks notebook source
USE f1_presentation

-- COMMAND ----------

USE f1_presentation;

SELECT team_name,
       COUNT(1) Total_races,
       sum(calc_points) as total_points,
       round(avg(calc_points),2) as avg_points
FROM f1_presentation.calculated_race_results
GROUP BY TEAM_NAME
HAVING COUNT(1) >100
ORDER BY avg_points DESC;

-- COMMAND ----------

SELECT team_name,
       COUNT(1) Total_races,
       sum(calc_points) as total_points,
       round(avg(calc_points),2) as avg_points
FROM f1_presentation.calculated_race_results
where race_year between 2001 and 2010
GROUP BY TEAM_NAME
HAVING COUNT(1) >100
ORDER BY avg_points DESC;

-- COMMAND ----------

