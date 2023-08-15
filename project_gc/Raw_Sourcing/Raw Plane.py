# Databricks notebook source
pip install tabula-py

# COMMAND ----------

import tabula
from datetime import date
tabula.convert_into("/dbfs/mnt/source_blob/PLANE.pdf",f"/dbfs/mnt/raw_datalake/PLANE.csv",output_format="csv",pages="all")

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/raw_datalake/PLANE/Date_Part=2023-08-14/")

# COMMAND ----------

import tabula
from datetime import date
tabula.convert_into('/dbfs/mnt/source_blob/PLANE.pdf',f'/dbfs/mnt/raw_datalake/PLANE/Date_Part={date.today()}/PLANE.csv',output_format='csv',pages='all')

# COMMAND ----------

dbutils.fs.ls("/mnt/raw_datalake")

# COMMAND ----------

dbutils.fs.ls("/mnt/raw_datalake/")

# COMMAND ----------

# MAGIC %md ###final solution with parameters

# COMMAND ----------

# Databricks notebook source
pip install tabula-py

# COMMAND ----------

import tabula
from datetime import date
def f_source_pdf_datalake(source_path,sink_path,output_format,page,file_name):
    try:
        dbutils.fs.mkdirs(f"/{sink_path}{file_name.split('.')[0]}/Date_Part={date.today()}/")
        tabula.convert_into(f'{source_path}{file_name}',f"/dbfs/{sink_path}{file_name.split('.')[0]}/Date_Part={date.today()}/{file_name.split('.')[0]}.{output_format}",output_format=output_format,pages=page)
    except Exception as err:
        print("Error Occured ",str(err))

# COMMAND ----------

list_files=[(i.name,i.name.split('.')[1]) for i in dbutils.fs.ls('/mnt/source_blob/') if(i.name.split('.')[1]=='pdf')]
for i in list_files:
    f_source_pdf_datalake('/dbfs/mnt/source_blob/','mnt/raw_datalake/','csv','all',i[0])

