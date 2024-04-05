# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS f1.presentation_layer.calculated_race_results
    (
        race_year INT,
        team_name STRING,
        driver_id INT,
        driver_name STRING,
        race_id INT, 
        position INT,
        points INT,
        calculated_points INT,
        created_datetime TIMESTAMP,
        updated_datetime TIMESTAMP
    )
    USING DELTA
    LOCATION '/mnt/f1azda/presentation/calc_race_results'    
    """
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE TABLE f1_presentation.calculated_race_results
# MAGIC -- USING PARQUET
# MAGIC -- AS
# MAGIC -- SELECT  ra.race_year,
# MAGIC --         c.name as team_name,
# MAGIC --         d.name as driver_name,
# MAGIC --         r.position,
# MAGIC --         r.points,
# MAGIC --         11 - r.position as calc_points
# MAGIC -- FROM f1_processed.results r
# MAGIC --   INNER JOIN f1_processed.drivers d 
# MAGIC --     ON r.driver_id = d.driver_id
# MAGIC --   INNER JOIN constructors c
# MAGIC --     ON r.constructor_id = c.constructor_id
# MAGIC --   INNER JOIN races ra
# MAGIC --     on r.race_id = ra.race_id
# MAGIC -- WHERE r.position <=10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.calculated_race_results limit 10;

# COMMAND ----------

