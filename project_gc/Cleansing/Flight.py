# Databricks notebook source
# MAGIC %run /project_gc/Utilities

# COMMAND ----------

dbutils.fs.rm("/dbfs/FileStore/tables/schema/Cancellation",True)

# COMMAND ----------

df=spark.readStream.format("cloudFiles").option("cloudFiles.format","csv")\
    .option("cloudFiles.schemaLocation","/dbfs/FileStore/tables/schema/flight")\
    .load("/mnt/raw_datalake/flight/")


# COMMAND ----------

display(df)

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# COMMAND ----------

from pyspark.sql.functions import concat_ws
df_base=df.selectExpr("to_date(concat_ws('-',year,month,dayofmonth),'yyyy-MM-dd') as date",
"from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm')  as deptime",
"from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm')  as CRSDepTime",
"from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm')  as ArrTime",
"from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm')  as CRSArrTime",
"UniqueCarrier",
"cast(FlightNum as int) as FlightNum",
"cast(TailNum as int) as TailNum" ,
"cast(ActualElapsedTime as int) as ActualElapsedTime",
"cast(CRSElapsedTime as int) as CRSElapsedTime",
"cast(AirTime as int) as AirTime",
"cast(ArrDelay as int) as ArrDelay",
"cast(DepDelay as int) as DepDelay",
 "Origin",
 "Dest",
 "cast(Distance as int) as  Distance",
 "cast(TaxiIn as int) as TaxiIn",
 "cast(TaxiOut as int) as TaxiOut",
 "Cancelled",
 "CancellationCode",
 "cast(Diverted as int) as castDiverted",
 "cast(CarrierDelay as int) as CarrierDelay",
 "cast(WeatherDelay as int) as WeatherDelay" ,
 "cast(NASDelay as int) as NASDelay",
 "cast(SecurityDelay as int) as SecurityDelay",
 "cast(LateAircraftDelay as int) as LateAircraftDelay" ,
 "to_date(Date_Part,'yyyy-MM-dd') as Date_Part ")
df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/flight")\
    .start("/mnt/cleansed_datalake/flight")

# COMMAND ----------

df=spark.read.format("delta").load("/mnt/cleansed_datalake/flight")
schema=pre_schema(df)
f_delta_cleansed_load("flight","/mnt/cleansed_datalake/flight",schema,"cleansed_geekcoders")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_geekcoders.flight

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended cleansed_geekcoders.flight

# COMMAND ----------


