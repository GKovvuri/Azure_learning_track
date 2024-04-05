# Databricks notebook source
# DBTITLE 1,Azure Data Lake storage Access
# MAGIC %md
# MAGIC  
# MAGIC - Each storage container comes with a set of Access keys that one can use to read and write data which are called as Storage Access Keys.
# MAGIC - They also come with a Shared Access Signature token or SAS Token that can be used to maintain the the access to the user at a more granular level than the Storage Access Keys.
# MAGIC - Service Principal can be assigned with particular permission to access the datalake and then credentials of this service princiapl can be used to access the datalake from databricks
# MAGIC
# MAGIC The above mentioned methods to access the data lake container can be achecived from databricks using 
# MAGIC   - Session Scoped Authentication 
# MAGIC     - Usage of the above credentials is within the notebook.
# MAGIC     - This access would tied to that session i.e., until the notebook is detached from the cluster 
# MAGIC   
# MAGIC   - Cluster Scoped Authentication
# MAGIC     - Usage of the credentials at the cluster level and would be valid until the cluster is terminated
# MAGIC
# MAGIC - Through Azure Active Directory (AAD)
# MAGIC   - AAD Passthrough Authentication: where the databricks cluster looks at the credentials of user and based on his roles assigned through the IAM( Identity Access Management) will be able to parse through the data the user has access to

# COMMAND ----------

# DBTITLE 1,Steps involved in creating a Azure Data lake Storage 
# MAGIC %md 
# MAGIC 1) Click on the hamburger icon on the top left corner of the console. </br>
# MAGIC 2) Create a Resource and search for storage account. </br>
# MAGIC 3) Select the subscription from teh dropdown for billing purposes. </br>
# MAGIC 4) Select the resource group under which the storage account should be created for. </br>
# MAGIC 5) Enter the nema of the storage container nad the region closest to you and also the redundacy method for maintaining a HA storage account.</br>
# MAGIC     - there are four types of redundancies to choose from:
# MAGIC       - LRS (Local Redundant Storage)
# MAGIC       - ZRS (Zone Redundant Storage)
# MAGIC       - GRS (Geo Redundant Storage)
# MAGIC       - GZRS (Geo Zone Redundant Storage)
# MAGIC 6) Click next on to Advanced settings and to enable the Datalake storage (ADLS v2) capability of the storage container select the tick mark the option to enable Hierarchical Namespace.
# MAGIC 7) Leaving everything as it is click on review and create the storage account 

# COMMAND ----------

# DBTITLE 1,Information about a storage account 
# MAGIC %md
# MAGIC - Each storage account comes two Access Keys
# MAGIC   - Has full access to the storage account (Super User).
# MAGIC   - Recommended secure using Azure Key Vault.
# MAGIC   - If Key is compromissed/lost they can rotated or regenerated.

# COMMAND ----------

# DBTITLE 1,Access Keys 
# MAGIC %md 
# MAGIC  setting up a spark configuration on Azure Databricks using the below command would help databeicks access the data stored in the data container
# MAGIC  
# MAGIC  spark.conf.set("fs.azure.account.key.\<storage-account\>.dfs.core.windows.net","\<access-key\>") </br>
# MAGIC  spark.conf.set(config.endpoint,access_key)
# MAGIC
# MAGIC  - Azure Recommends to use **Azure Blob File System (ABFS)** driver to access data in the containers rather than the https protocol 
# MAGIC
# MAGIC  URI Syntax 
# MAGIC  - abfss://container@storage_account_name.dfs.core.windows.net/folder_path/file_name 
# MAGIC   - Ex. abfss://demo@f1azda.dfs.core.windows.net/circuits.csv
# MAGIC   - Ex. abfss://demo@f1azda.dfs.core.windows.net/test/circuits.csv
# MAGIC
# MAGIC Once we have the URI we can use the dbutils.fs.ls command along with the URI to access the data within the storage container

# COMMAND ----------

# DBTITLE 1,Shared Access Signature Tokens
Provides more fine grained access to the data in the storage accounts by:
-   Restricting access to only specific type of files or only access specific containers in the storage account.
-   Allowing specific permissions to the user like the user is only allowed to read data within the files but not update/write.
-   Restricting the users to access the storage accounts only at a given time period of the day.
-   Limiting the access to the data to a range or specific IP addresses.
-   Best Practice to share SAS tokes with External clients who dont have direct access to the Environment.


# COMMAND ----------

# DBTITLE 1,Service Principle
# MAGIC %md
# MAGIC Service Priniciples are similar to users which can be defined in the Azure Active Directory and then be assigned RBAC roles to assign permissions/ level of access to various Azure Services 
# MAGIC - Recommended approach to give access to Services during automation processes such as in CI/CD pipelines or automated job runs 

# COMMAND ----------

# DBTITLE 1,Cluster Based Access
# MAGIC %md 
# MAGIC Above Discussed access methods are valid only for a given session that is valid only till the notebook is detached from from teh cluster or until the cluster automatically terminates.
# MAGIC So we can assign variables during cluster startup so that the authentication is done during cluster initialization and all the notebooks attahced to the cluster or users accessing the cluster can access the resources (for ex. directly access the data in the data lake) without any setup in the notebook.
# MAGIC
# MAGIC But this approach is not a good approach in the case of a large organisation as individual team should have thier cluster launched as the application for each team will be different and this architecture would difficult to manage as the team/ organisation grows.

# COMMAND ----------

# DBTITLE 1,Securing keys using Azure and Databricks 
# MAGIC %md 
# MAGIC Databricks --> Secret Scopes and in Azure --> Azure Key Vault
# MAGIC
# MAGIC Secret Scopes are a way through which credentials can be stored in databricks securily by referencing them in clusters, notebooks and jobs
# MAGIC - Two types of Scopes 
# MAGIC     - Databricks backed Secret Scope --> This is a solution which is backed and owned by Databricks and can be accessed by Databricks CLI 
# MAGIC     - Azure Key Vault backed Secret Scope (Recommended approach while using Databricks on Azure)
# MAGIC
# MAGIC ## Linking Azure Key Vault to Databricks 
# MAGIC
# MAGIC - Create a Azure Key vault Resource --> Link it to the Databricks backed Scope --> reference the secrets using the databricks secrets utility **dbutils.secrets.get**
# MAGIC
# MAGIC ## Process 
# MAGIC
# MAGIC - Step 1: Copy the token values from the azure container (Access key token, SAS Token)
# MAGIC - Step 2: Go to Azure key Vault and create a new Vault
# MAGIC - Step 3: Within the newly created Vault --> go to Secrets option in the side menu bar --> import/generate a new secret (top menu bar) --> Give the secret a name and secret value (Like a Key Value pair).
# MAGIC - Step 4: In the key Vault --> Go to Properties under settings in the side menu --> make a note of the **Vault URI** and **Resource ID**.
# MAGIC - Step 5: In the databricks homepage modify the URL to include **#secrets/createScope** at the end and also make sure that you are not there on onboarding page i.e., in the URI after **azuredatabtricks.net/onboarding?o=** --> **incorrect** and it should be **azuredatabtricks.net/?o=**.
# MAGIC - Step 6: In the create secret Scope menu give a name to the secret scope and then also provide the details such as **Azure Vault URI** and **Resource ID** to link the Azure Key Vault to the Databricks Secret Scope feature.
# MAGIC - Step 7a: To get the list of Scopes available in the Secret Scope database --> dbutils.secrets.listScopes()
# MAGIC - Step 7b: To get the list of Secrets within the scope --> dbutils.secrets.list(scope = "\<Scope Name>")
# MAGIC - Step 7c: To get the information from a particular scope and an element in the list --> dbutils.secrets.get(scope = "\<Scope Name>", key = "\<Key Value>")

# COMMAND ----------

# DBTITLE 1,Spark Architecture 
# MAGIC %md 
# MAGIC - Comprised of **driver and Worker nodes** and a **cluster manager node**
# MAGIC   -  The Driver is repsonsible for setting up the sparkContext which acts as an Entry point into the spark cluster 
# MAGIC   -  Once the program is submitted the logical plan for execution is created at the driver node.
# MAGIC   -  the logical plan is made until by the **DAG Scheduler** until an action keyword is called and based on the differnet transformations till then the plan is then split into stages which consist of different tasks.
# MAGIC   -  Based on the dataset it is operating the driver node does negotiates with the cluster manager on allocation of resources(worker nodes) to complete the tasks.
# MAGIC   -  The Cluster manager spins up the worker nodes with the help of the **Task Scheduler** and communicates the job to be performed by them.
# MAGIC   -  The status of the jobs are reported periodically to the cluster manager and incase of the a worker node failure the cluster manager will spin up a new cluster and reassign the task the lost node as assigned.
# MAGIC
# MAGIC References:
# MAGIC - https://www.interviewbit.com/blog/apache-spark-architecture/
# MAGIC - https://www.databricks.com/glossary/catalyst-optimizer
# MAGIC - https://www.analyticsvidhya.com/blog/2021/08/understand-the-internal-working-of-apache-spark/
# MAGIC - https://stackoverflow.com/questions/24909958/spark-on-yarn-concept-understanding/38598830#38598830
# MAGIC

# COMMAND ----------

# DBTITLE 1,Running another notebook from within a notebook 
# MAGIC %md 
# MAGIC - Can use the %run command to run another notebook from within another one and use the variables or functions from the 
# MAGIC - For Example Syntax can be like: </br>
# MAGIC   **%run ../path/to/file**

# COMMAND ----------

# DBTITLE 1,Passing parameters using widgets option of dbutils
dbutils.widgets.<option>

dbutils.widgets.help --> gets us the manual on how to use the various other methods which can be used to configure widgets. 
dbutlis.widgets.text("param_name","default_val") --> creates a text box on top of the page on what parameter value can be passed through.
dbutils.widgets.get("param_name") --> get the value entered in the parameter above created to bring it into the context of the workbook.

and more...

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC - Hive Metastore is a registry which stores information about the metadata of the files in a tablular manner which includes details such as location of the file, file extension etc.
# MAGIC
# MAGIC - Apache Hive provides a metastore for Spark to use while query.
# MAGIC
# MAGIC #### Spark Implementation
# MAGIC
# MAGIC - When SQL query is querying a table from a spark context it first checks for the metadata in the hive catalog and if it is present the data 
# MAGIC
# MAGIC - Types of tables that can be created using the spark sql are Managed and external where
# MAGIC   - Managed table is a kind of table where the metadata and table data is handled by Spark SQL itself 
# MAGIC   - External table is a kind of table where the only the metadata is handled by spark and data is handled by the user
# MAGIC
# MAGIC - **NOTE: When a managed table is deleted by spark the metadata and the data is deleted and when an external table is deleted, metadata only gets deleted where as the data still remains as is**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake
# MAGIC   - This is a datalake with ACID transaction controls which enables delta table to ingest streaming data along with the batch load processing.
# MAGIC
# MAGIC   - There are three stages of transformation which are:
# MAGIC     - raw data storage (Bronze Zone)
# MAGIC     - Pre processed data storage (Silver Zone)
# MAGIC     - Aggregated data storage for BI reporting (Gold Zone)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC #### UPDATE
# MAGIC
# MAGIC 1) How to update an existing delta table using SQL 
# MAGIC     - pretty straight forward:
# MAGIC     - UPDATE TBL_NM SET COL_NAME = OPERATION WHERE CONDITION 
# MAGIC     - Eg:- UPDATE RESULTS_MANAGED SET POINTS = 11 - POSITION WHERE POSITION < 11
# MAGIC 2) How to update an existing delta table using Python 
# MAGIC     - import statement --> from delta.tables import DeltaTable
# MAGIC     - delta_table = DeltaTable.forPath("/mnt/f1azda/demo/results_managed")
# MAGIC     - delta_table.update("position<11",{"points":"21-position"})
# MAGIC
# MAGIC #### DELETE 
# MAGIC
# MAGIC 4) Delete data from delta table using SQL
# MAGIC     - DELETE FROM tbl_nm where condition
# MAGIC
# MAGIC 5) Delete data from delta table using python \
# MAGIC     from delta.tables import DeltaTable \
# MAGIC     deltatable = DeltaTable.forPath(spark,"/mnt/f1azda/demo/result_managed") \
# MAGIC     deltatable.delete("position > 10")
# MAGIC
# MAGIC #### MERGE/ UPSERT(Update or Insert)
# MAGIC
# MAGIC 6) SQL Syntax \
# MAGIC     -  MERGE INTO TGT_TBL \
# MAGIC        USING SRC_TBL \
# MAGIC        ON JOIN CONDITION \
# MAGIC        WHEN MATCHED THEN \
# MAGIC        UPDATE SET TGT_TBL.COL = SRC_TBL.COL \
# MAGIC        WHEN NOT MATCHED THEN \
# MAGIC        INSERT (TGT_TBL COLS) VALUES (SRC_TBL COLS) \
# MAGIC 7) PySpark Syntax: \
# MAGIC     - from delta.tables import DeltaTable\
# MAGIC       from pyspark.sql.functions import * as f \
# MAGIC       _loading the target table_ \
# MAGIC       delta_table = DeltaTable.forPath(spark,"path_to_storage folder") \
# MAGIC       _defining the merge function_ \
# MAGIC       delta_table.alias("TGT").merge(source_tbl.alias("SRC"),"TGT.col_name = SRC.col_name")\
# MAGIC       .whenMatchedUpdate(set = {"TGT.col2" : "SRC.col2",\
# MAGIC                                 "TGT.col3" : "SRC.col3"})\
# MAGIC       .whenNotMatchedInsert(values = {"TGT.col1" : "SRC.col1", \
# MAGIC                                       "TGT.col1" : "SRC.col1"}) \
# MAGIC       .execute()
# MAGIC     
# MAGIC #### TIME TRAVEL
# MAGIC
# MAGIC 3) Time travel
# MAGIC - SQL Syntax
# MAGIC     - RESTORE TABLE tbl_nm TO TIMESTAMP AS OF of "yyyy-mm-dd hh:mm:ss.ms"
# MAGIC     - To know the history of changes made to a delta table we can use:
# MAGIC     -  DESCRIBE HISTORY tbl_name
# MAGIC - To get the table data from a particular point in time or a specific version.
# MAGIC
# MAGIC     - SELECT * FROM tbl_nm VERSION AS OF 1;
# MAGIC     - SELECT * FROM tbl_nm TIMESTAMP AS OF "2024-03-20T21:49:09.000+00:00"
# MAGIC
# MAGIC - Python Syntax
# MAGIC     - Format to get the older version of the dataframe from the file using the version.
# MAGIC     df_old = spark.read.format("delta").option("versionAsOf",1).load("/mnt/f1azda/demo/drivers_day_merge")
# MAGIC     - Format to get the older version of the dataframe from the file using the timestamp.
# MAGIC     df_old = spark.read.format("delta").option("timeStampAsOf",1).load("/mnt/f1azda/demo/drivers_day_merge")
# MAGIC
# MAGIC #### VACUUM METHOD TO SATISY GDPR CONSTRAINTS 
# MAGIC     - Vacuum is the method to delete the history of all the data and the latest data in the delta lake
# MAGIC     - SQL Syntax
# MAGIC         - VACUUM tbl_nm RETAIN n HOURS --> default is 168 HOURS that is 7 days
# MAGIC     - To change the default to lesser than 7 days we have to set the spark parameter
# MAGIC         - SET spark.databricks.delta.rententionDurationCheck.enabled = false
# MAGIC
# MAGIC #### Sccenario --> Data has been corrupted and it needs to be brought back to its original state
# MAGIC - SQL syntax (using MERGE INTO )
# MAGIC     - MERGE INTO TGT_TBL \
# MAGIC       USING SRC \
# MAGIC       ON TGT.COL = SRC.COL\
# MAGIC       WHEN NOT MATCHED \
# MAGIC       INSERT * 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Transaction log for a delta table 
# MAGIC 1) For each operation on the table (Insert, Update, Delete) a new parquet file is created in the storage location and a corresponding 
# MAGIC    log transactionfile in the form of a json file.
# MAGIC 2) a. when a table is created it first creates a parquet file in teh given location of the database. \
# MAGIC    b. Also a file in the transaction log is created with vital information against the processs. \
# MAGIC    c. When more and more data is being generated there will be a transaction checkpoint to which the process can refer to ignoring 
# MAGIC       all the individaul files that represent sinlgle transactions s that happened on the table before the checkpoint. \s
# MAGIC
# MAGIC 3) Transaction table content when a table is created is pretty straight forward it contains the following:
# MAGIC   - Transaction commit information:
# MAGIC     - timestamp: when the create table statment was commited.
# MAGIC     - Username: Email of the account responsible for the action.
# MAGIC     - Operation: Type of CRUD operation performed (WRITE). 
# MAGIC     - a few other details such as notebook information, cluster information, transaction id
# MAGIC   - etadata
# MAGIC     - createdTime: Created time stamp
# MAGIC     - paritionColumns (list): if the table is to be partitoned by any columnm it would be mentioned here 
# MAGIC     - schemaString: type of the table which is \
# MAGIC         - data type of table :"struct"
# MAGIC         - field (list of dictionaries):contains information about the fields in the table such as name, type, nullable, metadata of a given column in the table 
# MAGIC
# MAGIC 4) Transaction table content when a record in a table is added :
# MAGIC    -Transaction commit info
# MAGIC     - timestamp: when the create table statment was commited.
# MAGIC     - Username: Email of the account responsible for the action.
# MAGIC     - Operation: Type of CRUD operation performed (CREATE TABLE). 
# MAGIC     - a few other details such as notebook information, cluster information, transaction id
# MAGIC   - add (as a new parquet file is created when ever an operation is done on a delta table)
# MAGIC     - path: "name of the file being referred to"
# MAGIC     - dataChange: Boolean object stating if there is any change in the underlying data of the file.
# MAGIC     - numstats (numerical stats): suggesting min and max values in each columns, numOfRecords, and also number of null values in each column is mentioned.
# MAGIC
# MAGIC 5) Transaction table content when a record in a table is deleted :
# MAGIC    -Transaction commit info
# MAGIC     - timestamp: when the create table statment was commited.
# MAGIC     - Username: Email of the account responsible for the action.
# MAGIC     - Operation: Type of CRUD operation performed (DELETE). 
# MAGIC     - a few other details such as notebook information, cluster information, transaction id
# MAGIC     - OperationMetrics are mentioned such as number of removed files, number of removed records, time taken to scan the table for finding the records and also time taken to rewrite the information.  
# MAGIC   - remove 
# MAGIC     - path: "name of the file being referred to"
# MAGIC     - dataChange: Boolean object stating if there is any change in the underlying data of the file.
# MAGIC     - Deletion_timestamp --> epoch tome of when a file is deleted.  
# MAGIC
# MAGIC 6) There is also a .crc --> Checksum Redundancy Check file which can be used to check if the data is a match. 
# MAGIC       
# MAGIC

# COMMAND ----------

import hashlib

def mask_sensitive_data(input_data, field_to_mask):
    """
    Mask the specified field in the input data using a hash function.

    Parameters:
    - input_data: Dictionary containing sensitive information.
    - field_to_mask: Name of the field to be masked.

    Returns:
    - Masked data.
    """
    masked_data = input_data.copy()  # Create a copy to avoid modifying the original data

    if field_to_mask in masked_data:
        original_value = masked_data[field_to_mask]

        # Using SHA-256 hash for masking (you can choose a different method based on your requirements)
        print(original_value)
        hash_object = hashlib.md5(original_value.encode())
        print(hash_object)
        masked_value = hash_object.hexdigest()
        print(masked_value)

        masked_data[field_to_mask] = masked_value

    return masked_data

# Example Usage:
sensitive_data = {
    "patient_id": "12345",
    "patient_name": "John Doe",
    "ssn": "123-45-6789",
    "medical_history": "Sensitive information here",
}

field_to_mask = "patient_name"

masked_data = mask_sensitive_data(sensitive_data, field_to_mask)
# print("Original Data:", sensitive_data)
# print("Masked Data:", masked_data)