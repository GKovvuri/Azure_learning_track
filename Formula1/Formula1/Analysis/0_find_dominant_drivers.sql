-- Databricks notebook source
SELECT  driver_name,
        COUNT(1) AS TOTAL_RACES,
        SUM(calc_points) AS TOTAL_POINTS,
        ROUND(AVG(calc_points),2) AS AVG_POINTS 
FROM f1_presentation.calculated_race_results
WHERE RACE_YEAR BETWEEN 1981 AND 1990
GROUP BY driver_name
HAVING COUNT(1) >=50
ORDER BY AVG_POINTS DESC;


-- COMMAND ----------

