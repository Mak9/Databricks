# Databricks notebook source
# MAGIC %run /project_gc/Utilities

# COMMAND ----------

dbutils.fs.rm("/dbfs/FileStore/tables/schema/Cancellation",True)

# COMMAND ----------

df=spark.readStream.format("cloudFiles").option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","/dbfs/FileStore/tables/schema/UNIQUE_CARRIERS")\
    .load("/mnt/raw_datalake/UNIQUE_CARRIERS/")


# COMMAND ----------

df_base=df.selectExpr("replace(Code,'\"','') as code",
    "replace(Description,'\"','') as description",
    "to_date(Date_Part,'yyyy-MM-dd') as Date_Part")
df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/UNIQUE_CARRIERS")\
    .start("/mnt/cleansed_datalake/unique_carriers")

# COMMAND ----------

df=spark.read.format("delta").load("/mnt/cleansed_datalake/unique_carriers")
schema=pre_schema(df)
f_delta_cleansed_load("unique_carriers","/mnt/cleansed_datalake/unique_carriers",schema,"cleansed_geekcoders")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_geekcoders.unique_carriers

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended cleansed_geekcoders.plane

# COMMAND ----------


