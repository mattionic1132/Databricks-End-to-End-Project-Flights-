# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.silver_flights

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Parameters**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Fetching Parameters & Creating Variables**

# COMMAND ----------

# DBTITLE 1,Widget Declarations
# Create widgets for parameterization
dbutils.widgets.text("key_cols", "['passenger_id']", "Key Columns (as list string)")
dbutils.widgets.text("source_object", "silver_passengers", "Source Table Name")
dbutils.widgets.text("target_object", "DimPassengers", "Target Dimension Table")
dbutils.widgets.text("surrogate_key", "DimPassengersKey", "Surrogate Key Column Name")
dbutils.widgets.text("backdated_refresh", "", "Backdated Refresh Date (optional)")

# COMMAND ----------

# # Catalog Name
# catalog = "workspace"

# # Key Cols List
# key_cols = "['flight_id']"
# key_cols_list = eval(key_cols)

# # CDC Column
# cdc_col = "modifiedDate"

# # Backdated Refresh
# backdated_refresh = ""

# # Source Object
# source_object = "silver_flights"

# # Source Schema
# source_schema = "silver"

# # Target Schema 
# target_schema = "gold"

# # Target Object
# target_object = "DimFlights"

# #Surrogate Key
# surrogate_key = "DimFlightsKey"


# COMMAND ----------

# # Catalog Name
# catalog = "workspace"

# # Key Cols List
# key_cols = "['airport_id']"
# key_cols_list = eval(key_cols)

# # CDC Column
# cdc_col = "modifiedDate"

# # Backdated Refresh
# backdated_refresh = ""

# # Source Object
# source_object = "silver_airports"

# # Source Schema
# source_schema = "silver"

# # Target Schema 
# target_schema = "gold"

# # Target Object
# target_object = "DimAirports"

# #Surrogate Key
# surrogate_key = "DimAirportsKey"


# COMMAND ----------

# DBTITLE 1,Get Parameters from Widgets
# Catalog Name
catalog = "workspace"

# Key Cols List (from widget)
key_cols = dbutils.widgets.get("key_cols")
key_cols_list = eval(key_cols)

# CDC Column
cdc_col = "modifiedDate"

# Backdated Refresh (from widget)
backdated_refresh = dbutils.widgets.get("backdated_refresh")

# Source Object (from widget)
source_object = dbutils.widgets.get("source_object")

# Source Schema
source_schema = "silver"

# Target Schema 
target_schema = "gold"

# Target Object (from widget)
target_object = dbutils.widgets.get("target_object")

# Surrogate Key (from widget)
surrogate_key = dbutils.widgets.get("surrogate_key")


# COMMAND ----------

# MAGIC %md
# MAGIC ### **INCREMENTAL DATA INGESTION**

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Last Load Date**

# COMMAND ----------

#No backdated refresh

if len(backdated_refresh) == 0:

    #if table exists in the destination
    if spark.catalog.tableExists(target_schema + "." + target_object):
        last_load = spark.sql(f"SELECT max({cdc_col}) FROM workspace.{target_schema}.{target_object}").collect()[0][0]

    else:
        last_load = "1900-01-01 00:00:00"

#back dated refresh
else:
    last_load = backdated_refresh

# COMMAND ----------

# Test the last load
last_load

# COMMAND ----------

# DBTITLE 1,Cell 10
df_src = spark.sql(f"SELECT * FROM {source_schema}.{source_object} WHERE {cdc_col} > '{last_load}'")

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### OLD vs NEW RECORDS 

# COMMAND ----------

if spark.catalog.tableExists(target_schema + "." + target_object):

    # Key Columns String For Incremental
    key_cols_string_incremental = ', '.join(key_cols_list)
    
    df_trg = spark.sql(f"SELECT {key_cols_string_incremental}, {surrogate_key}, create_date, update_date FROM {catalog}.{target_schema}.{target_object}")

else:

    #Key Columns String For Initial
    key_cols_string_init = [f"'' AS {i}" for i in key_cols_list]
    key_cols_string_init = ", ".join(key_cols_string_init)

    df_trg = spark.sql(f"SELECT {key_cols_string_init}, CAST('0' AS INT) AS {surrogate_key},CAST('1900-01-01 00:00:00' AS timestamp) AS create_date, CAST('1900-01-01 00:00:00' AS timestamp) AS update_date WHERE 1=0")


# COMMAND ----------

df_trg.display()

# COMMAND ----------

key_cols_list = ['flight_id', 'flight_name']

# COMMAND ----------

# MAGIC %md
# MAGIC #### **JOIN CONDITION**

# COMMAND ----------

join_condition = ' AND '.join([f"src.{i} = trg.{i}" for i in key_cols_list])

# COMMAND ----------

df_src.createOrReplaceTempView("src")
df_trg.createOrReplaceTempView("trg")

# Use configured key columns from Cell 6
key_cols_list = eval(key_cols)
join_condition = ' AND '.join([f"src.{i} = trg.{i}" for i in key_cols_list])

df_join = spark.sql(f"""
          SELECT src.*,
                 trg.{surrogate_key},
                 trg.create_date,
                 trg.update_date
          FROM src
          LEFT JOIN trg
          ON {join_condition}  
          """)

# COMMAND ----------

df_join.display()

# COMMAND ----------

# OLD RECORDS
df_old = df_join.filter(col(f'{surrogate_key}').isNotNull())

# NEW RECORDS
df_new = df_join.filter(col(f'{surrogate_key}').isNull())

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ENRICHING DFS

# COMMAND ----------

# MAGIC %md
# MAGIC **PREPARING DF_OLD**

# COMMAND ----------

df_old_enr = df_old.withColumn('update_date',current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC **PREPARING DF_NEW**

# COMMAND ----------

df_new.display()

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    max_surrogate_key = spark.sql(f"""
                            SELECT max({surrogate_key}) FROM {catalog}.{target_schema}.{target_object}
                        """).collect()[0][0]
    df_new_enr = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key) + lit(1) + monotonically_increasing_id())\
        .withColumn('create_date', current_timestamp())\
        .withColumn('update_date', current_timestamp())
else:
    max_surrogate_key = 0
    df_new_enr = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key) + lit(1) + monotonically_increasing_id())\
    .withColumn('create_date', current_timestamp())\
    .withColumn('update_date', current_timestamp())



# COMMAND ----------

df_new_enr.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Unioning OLD AND NEW RECORDS**

# COMMAND ----------

df_union = df_old_enr.unionByName(df_new_enr)

# COMMAND ----------

df_union.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **UPSERT**

# COMMAND ----------

from delta.tables import DeltaTable

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
    dlt_obj.alias("trg").merge(df_union.alias("src"), f"trg.{surrogate_key} = src.{surrogate_key}")\
        .whenMatchedUpdateAll(condition = f"src.{cdc_col} >= trg.{cdc_col}")\
        .whenNotMatchedInsertAll()\
        .execute()

else:
    df_union.write.format('delta')\
        .mode("append")\
        .saveAsTable(f"{catalog}.{target_schema}.{target_object}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.gold.dimpassengers WHERE passenger_id = 'P0049'