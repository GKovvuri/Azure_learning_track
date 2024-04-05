# Databricks notebook source
# DBTITLE 1,Mount Azure Data Lake using service principle
# MAGIC %md
# MAGIC
# MAGIC ### Steps to create a service principle
# MAGIC
# MAGIC Service principles are simiilar to user and we need to follow the below steps to register an application with Azure Entra ID to use service principles:
# MAGIC -   Step 1: Register the application/Service Principle in Azure entra: </br>
# MAGIC     -   Give it a suitable name.</br>
# MAGIC     -   Select the kind of tenancy of the application.</br>
# MAGIC     -   Take a note of the client and tenant id.</br>
# MAGIC -   Step 2: From the application registry: </br>
# MAGIC     -   Create a new Client Secret by going into certificates and secrets side bar menu option.
# MAGIC     -   make a note of the client secret Value 
# MAGIC -   Step 3: Set the Spark Configuration by using the type of authentication and type of authentication provider as well as passing in the variables taken note in Step 1 and Step 2.
# MAGIC -   Step 4: Assign the role "Storage Blob Data Contributor" to the Azure Data Lake Storage.
# MAGIC     - ADLS --> Storage Account --> Access Control (IAM) --> Add --> Add Role Assignment --> Sleect the job function role --> assign access to User/Service Principle --> review and assign
# MAGIC
# MAGIC ### Steps to mount an azure datalake storage using service principle
# MAGIC
# MAGIC - Step1 : Get client id, tenant id and client_secret_value from key vault
# MAGIC - Step2 : Set Spark conf with client id, tenant id and client_secret_value
# MAGIC - step3 : Call the file system utility's mount to mount the storage
# MAGIC - step4 : Explore other file system utility commands.

# COMMAND ----------

client_id = dbutils.secrets.get(scope = "formula1-scope",key = "f1azda-cid")
tenant_id = dbutils.secrets.get(scope = "formula1-scope",key = "f1azda-tid")
client_secret_val = dbutils.secrets.get(scope = "formula1-scope",key = "f1azda-csv")

# COMMAND ----------

# DBTITLE 1,Setting the spark configuration
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret_val,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# spark.conf.set("fs.azure.account.auth.type.f1azda.dfs.core.windows.net", "OAuth")
# spark.conf.set("fs.azure.account.oauth.provider.type.f1azda.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set("fs.azure.account.oauth2.client.id.f1azda.dfs.core.windows.net", client_id)
# spark.conf.set("fs.azure.account.oauth2.client.secret.f1azda.dfs.core.windows.net", client_secret_val)
# spark.conf.set("fs.azure.account.oauth2.client.endpoint.f1azda.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# DBTITLE 1,Mounting
dbutils.fs.mount(
    source = "abfss://demo@f1azda.dfs.core.windows.net",
    mount_point = "/mnt/formula1/demo", # convention --> mount/storage_account/container
    extra_configs = configs
    )

# COMMAND ----------

# dbutils.fs.unmount("/mnt/formula1/demo")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# display(dbutils.fs.mounts())

# COMMAND ----------

# dbutils.fs.ls("abfss://demo@f1azda.dfs.core.windows.net")
dbutils.fs.ls("/mnt/formula1/demo")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1/demo"))

# COMMAND ----------

spark.read.csv("/mnt/formula1/demo/circuits.csv",header=True)

# COMMAND ----------

display(spark.read.csv("/mnt/formula1/demo/circuits.csv",header=True))

# COMMAND ----------

