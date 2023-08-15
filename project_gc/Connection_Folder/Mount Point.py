# Databricks notebook source
secre-scope - mahim

# COMMAND ----------

# MAGIC %md ##Creating Mount for Blob storage source - /mnt/source_blob

# COMMAND ----------

# MAGIC %scala
# MAGIC val containerName = "source" //dbutils.secrets.get(scope="mahim",key="")
# MAGIC val storageAccountName = "mahimstorageblobsource" //dbutils.secrets.get(scope="geekcoders-secret",key="storageaccountname")
# MAGIC val sas = dbutils.secrets.get(scope="mahim",key="blob-sas-token")
# MAGIC val config = "fs.azure.sas." + containerName+ "." + storageAccountName + ".blob.core.windows.net"
# MAGIC
# MAGIC dbutils.fs.mount(
# MAGIC source = "wasbs://source@mahimstorageblobsource.blob.core.windows.net",//dbutils.secrets.get(scope="geekcoders-secret",key="blob-mnt-path"),
# MAGIC mountPoint = "/mnt/source_blob/",
# MAGIC extraConfigs = Map(config -> sas))
# MAGIC

# COMMAND ----------

dbutils.fs.ls("/mnt/source_blob/")

# COMMAND ----------

# MAGIC %md ###creating mount point for gen2 raw container using svn - /mnt/raw_datalake/

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope = "mahim", key = "data-app-id"),
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="mahim",key="data-app-secret"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/372bb24a-e2a3-4e26-8672-057a0a2661ef/oauth2/token"} #dbutils.secrets.get(scope = "geekcoders-secret", key = "data-client-refresh-url")}

 #Optionally, you can add <directory-name> to the source URI of your mount point.
mountPoint="/mnt/raw_datalake/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
    source = "abfss://raw@mahimstorageaccount.dfs.core.windows.net/",
    mount_point = mountPoint,
    extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/raw_datalake/")

# COMMAND ----------

# MAGIC %md ###creating mount point for gen2 cleansed container using svn - /mnt/cleansed_datalake/

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope = "mahim", key = "data-app-id"),
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="mahim",key="data-app-secret"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/372bb24a-e2a3-4e26-8672-057a0a2661ef/oauth2/token"} #dbutils.secrets.get(scope = "geekcoders-secret", key = "data-client-refresh-url")}

 #Optionally, you can add <directory-name> to the source URI of your mount point.
mountPoint="/mnt/cleansed_datalake/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
    source = "abfss://cleansed@mahimstorageaccount.dfs.core.windows.net/",
    mount_point = mountPoint,
    extra_configs = configs)

# COMMAND ----------


