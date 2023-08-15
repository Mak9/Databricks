# Databricks notebook source
# MAGIC %run /Shared/project_gc/Utilities

# COMMAND ----------

df=spark.readStream.format("cloudFiles").option("cloudFiles.format","csv")\
    .option("cloudFiles.schemaLocation","/dbfs/FileStore/tables/schema/PLANE")\
    .load("/mnt/raw_datalake/PLANE/")


# COMMAND ----------

display(df)

# COMMAND ----------

df_base=df.selectExpr( "tailnum as tailid",
    "type",
    "manufacturer",
    "to_date(issue_date) as issue_date",
    "model",
    "status",
    "aircraft_type",
    "engine_type",
    "cast(year as int) as year",
    "to_date(Date_Part,'yyyy-MM-dd') as Date_Part")
df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/PLANE")\
    .start("/mnt/cleansed_datalake/plane")

# COMMAND ----------

df=spark.read.format("delta").load("/mnt/cleansed_datalake/plane")
schema=pre_schema(df)
f_delta_cleansed_load("plane","/mnt/cleansed_datalake/plane",schema,"cleansed_geekcoders")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_geekcoders.plane

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history  cleansed_geekcoders.plane

# COMMAND ----------


