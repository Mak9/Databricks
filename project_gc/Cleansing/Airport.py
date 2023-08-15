# Databricks notebook source
# MAGIC %run /Repos/anas.khan_me22@gla.ac.in/Databricks/project_gc/Utilities

# COMMAND ----------

df=spark.readStream.format("cloudFiles").option("cloudFiles.format","csv")\
    .option("cloudFiles.schemaLocation","/dbfs/FileStore/tables/schema/Airport")\
    .load("/mnt/raw_datalake/Airport/")


# COMMAND ----------

df_base=df.selectExpr("Code as code",
    "split(Description,',')[0] as city",
    "split(split(Description,',')[1],':')[0] as country",
    "split(split(Description,',')[1],':')[1] as airport",
    "to_date(Date_Part,'yyyy-MM-dd') as Date_Part")
df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/Airport")\
    .start("/mnt/cleansed_datalake/airport")

# COMMAND ----------

df=spark.read.format("delta").load("/mnt/cleansed_datalake/airport")
schema=pre_schema(df)
f_delta_cleansed_load("airport","/mnt/cleansed_datalake/airport",schema,"cleansed_geekcoders")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_geekcoders.airport

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended cleansed_geekcoders.plane

# COMMAND ----------


