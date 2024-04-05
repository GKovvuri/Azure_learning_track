# Databricks notebook source
# DBTITLE 1,Setting the access using Service Principle 
# MAGIC %md
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

# COMMAND ----------

client_id = dbutils.secrets.get(scope = "formula1-scope",key = "f1azda-cid")
tenant_id = dbutils.secrets.get(scope = "formula1-scope",key = "f1azda-tid")
client_secret_val = dbutils.secrets.get(scope = "formula1-scope",key = "f1azda-csv")

# COMMAND ----------

# DBTITLE 1,Step 1
# client_id = "ad7fa220-6908-429f-8b88-e60293a61f54"
# tenant_id = "f6ba26bc-1c0a-4bbf-a32e-c31d08528c61"
# client_secret_val = "t-v8Q~N0OXPee5c9owGQCaHZDSgF7cjDy4_FjaMn"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1azda.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.f1azda.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.f1azda.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.f1azda.dfs.core.windows.net", client_secret_val)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.f1azda.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@f1azda.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1azda.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("abfss://demo@f1azda.dfs.core.windows.net/circuits.csv",header=True)

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1azda.dfs.core.windows.net/circuits.csv",header=True))