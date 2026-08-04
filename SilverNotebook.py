# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.silver.silver_airports

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("delta").load("/Volumes/workspace/bronze/bronzevolume/customers/data/")

df = df.drop("_rescued_data")\
         .withColumn("modifiedDate", current_timestamp())

display(df)


# COMMAND ----------

df = spark.read.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
display(df)

# COMMAND ----------

df = df.withColumn("amount",col("amount").cast(DoubleType()))\
    .withColumn("modifiedDate", current_timestamp())\
    .withColumn("booking_date", to_date("booking_date"))\
    .drop("rescued_data")
display(df)

# COMMAND ----------

@dlt.table(
    name = "stage_bookings"
)
def stage_bookings():
    df = spark.readStream.format("delta")\
        .load("dbfs:/FileStore/shared_uploads/booking_data")
    return df

# COMMAND ----------

@dlt.view(
    name = "trans_bookings"
)
def trans_bookings():
    df = spark.readStream.table("stage_bookings")
    df = df.withColumn("amount",col("amount").cast(DoubleType()))\
        .withColumn("modifiedDate", current_timestamp())\
        .withColumn("booking_date", to_date("booking_date"))\
        .drop("rescued_data")
    return df

# COMMAND ----------

rules = {
    "rule1": "booking_id IS NOT NULL",
    "rule2": "passenger_id IS NOT NULL"
}

# COMMAND ----------

@dlt.table(
    name = "silver_bookings"
)
@dlt.expect_all(rules)
def silver_bookings():
    df = spark.readStream.table("trans_bookings")
    return df