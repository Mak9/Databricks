# Databricks notebook source
def pre_schema(df):
    try:
        schema=""
        for i in df.dtypes:
            schema=schema+i[0]+" "+i[1]+","
        return schema[0:-1]
    except Exception as err:
        print("Error Occured ",str(err))

# COMMAND ----------

def f_delta_cleansed_load(table_name,location,schema,database):
    try:
        spark.sql(f"""DROP TABLE IF EXISTS {database}.{table_name}""");
        spark.sql("""
                CREATE TABLE IF NOT EXISTS {3}.{2}
                (
                    {0}
                )
                using delta
                location '{1}'
                """.format(schema,location,table_name,database))
    except Exception as err:
        print("Error Occured", str(err))

# COMMAND ----------


def f_count_check(database,operation_type,table_name,number_diff):    
         spark.sql(f"""DESC HISTORY {database}.{table_name}""").createOrReplaceTempView("Table_count")
         count_current=spark.sql(f"""select operationMetrics.numOutputRows from Table_count where version=(select max(version) from Table_count where trim(lower(operation))=lower('{operation_type}'))""")
         if(count_current.first() is None):
            final_count_current=0
         else:
            final_count_current=int(count_current.first().numOutputRows)
         count_previous=spark.sql(f"""select operationMetrics.numOutputRows from Table_count where version<(select version from Table_count where lower(trim(operation))=lower('{operation_type}') order by version desc limit 1) order by version desc""")
         if(count_previous.first() is None):
            final_count_previous=0
         else:
             final_count_previous=int(count_previous.first().numOutputRows)
         if((final_count_current-final_count_previous)>number_diff):
             #print("Differnce is huge in ",table_name)
             raise Exception("Differnce is huge in ",table_name)
         else:
             pass

# COMMAND ----------

f_count_check("cleansed_geekcoders","WRITE","airline",1000000)

# COMMAND ----------


