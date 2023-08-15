# Databricks notebook source
# MAGIC %run /project_gc/Utilities

# COMMAND ----------

list_table_info = [
    ("STREAMING UPDATE", "plane", 100),
    ("STREAMING UPDATE", "flight", 200),
    ("STREAMING UPDATE", "Airport", 100),
    ("STREAMING UPDATE", "cancellation", 100),
    ("STREAMING UPDATE", "unique_carriers", 500),
    ("Write", "airline", 10),
]
for i in list_table_info:
    f_count_check("cleansed_geekcoders", i[0], i[1], i[2])

# COMMAND ----------


