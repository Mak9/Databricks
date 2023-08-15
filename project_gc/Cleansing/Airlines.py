# Databricks notebook source
# MAGIC %run /Repos/anas.khan_me22@gla.ac.in/Databricks/project_gc/Utilities

# COMMAND ----------

# df=spark.readStream.format("cloudFiles").option("cloudFiles.format","parquet")\
#     .option("cloudFiles.schemaLocation","/dbfs/FileStore/tables/schema/UNIQUE_CARRIERS")\
#     .load("/mnt/raw_datalake/UNIQUE_CARRIERS/")


# COMMAND ----------

# df_base=df.selectExpr("replace(Code,'\"','') as code",
#     "replace(Description,'\"','') as description",
#     "to_date(Date_Part,'yyyy-MM-dd') as Date_Part")
# df_base.writeStream.trigger(once=True)\
#     .format("delta")\
#     .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/UNIQUE_CARRIERS")\
#     .start("/mnt/cleansed_datalake/unique_carriers")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df=spark.read.json("/mnt/raw_datalake/airline/")
df1=df.select(explode("response"),"Date_format")
df_final=df1.select("col.*","Date_format")
display(df_final)

# COMMAND ----------

df_final.write.format("delta").mode("overwrite").save("/mnt/cleansed_datalake/airline")

# COMMAND ----------

df=spark.read.format("delta").load("/mnt/cleansed_datalake/airline")
schema=pre_schema(df)
f_delta_cleansed_load("airline","/mnt/cleansed_datalake/airline",schema,"cleansed_geekcoders")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_geekcoders.airline

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended cleansed_geekcoders.plane

# COMMAND ----------


