# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def add_partition_col(df,partition_col):
    sel_lst=[]
    for nms in df.schema.names:
        if nms != partition_col:
            sel_lst.append(nms)
    sel_lst.append(partition_col)
    return sel_lst

# COMMAND ----------

def incremental_load(source_df,partition_col,dest_schema,dest_table):
    # creating the select list so that the partition by column is at the end of the dataframe 
    sel_lst = add_partition_col(source_df,partition_col)
    df_final = source_df.select(sel_lst)

    # setting the overwrite data mode in spark to dynamic so that the particular partitions only get overwritten rather than the whole dataset
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

    #insert update block which checks if the table already exists just overwrite the existing partitions or else if the table is not present create and insert the data into the table.
    if(spark.catalog.tableExists(f"{dest_schema}.{dest_table}")):
        print(f"Updating the existing data wrt to {partition_col}")
        df_final.write.mode("overwrite").insertInto(f"{dest_schema}.{dest_table}")
    else:
        print("Creating a new table")
        df_final.write.mode("overwrite").format('parquet').partitionBy(partition_col).saveAsTable(f"{dest_schema}.{dest_table}")
    

# COMMAND ----------

def mergeDeltaTable(source_df,tgt_df,joinCond,partBy,db_name,folder_path):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning",True)
    from delta.tables import DeltaTable
    if(spark.catalog.tableExists(f"{db_name}.{tgt_df}")):
        print(f"Updating {tgt_df} table in {db_name} wrt to {partBy}")
        delta_table=DeltaTable.forPath(spark,f"{folder_path}/{tgt_df}")
        delta_table.alias('tgt').merge(source_df.alias("src"),joinCond)\
                                .whenMatchedUpdateAll()\
                                .whenNotMatchedInsertAll()\
                                .execute()
    else:
        print(f"Creating {tgt_df} table in {db_name} wrt to {partBy}")
        source_df.write.format("delta").mode("overwrite").partitionBy(partBy).saveAsTable(f"{db_name}.{tgt_df}")

# COMMAND ----------

