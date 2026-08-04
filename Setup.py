# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME workspace.raw.rawvolume

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/airports")

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC SELECT * FROM delta.`/Volumes/workspace/bronze/bronzevolume/flights/data/`

# COMMAND ----------

