# Databricks notebook source
# MAGIC %run /project_gc/Utilities

# COMMAND ----------

dbutils.fs.rm("/dbfs/FileStore/tables/schema/Cancellation",True)

# COMMAND ----------

df=spark.readStream.format("cloudFiles").option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","/dbfs/FileStore/tables/schema/Cancellation")\
    .load("/mnt/raw_datalake/Cancellation/")


# COMMAND ----------

df_base=df.selectExpr("replace(Code,'\"','') as code",
    "replace(Description,'\"','') as description",
    "to_date(Date_Part,'yyyy-MM-dd') as Date_Part")
df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/Cancellation")\
    .start("/mnt/cleansed_datalake/cancellation")

# COMMAND ----------

df=spark.read.format("delta").load("/mnt/cleansed_datalake/cancellation")
schema=pre_schema(df)
f_delta_cleansed_load("cancellation","/mnt/cleansed_datalake/cancellation",schema,"cleansed_geekcoders")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_geekcoders.cancellation

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended cleansed_geekcoders.plane

# COMMAND ----------


