# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.gold.dim_geo (
# MAGIC     geo_key BIGINT,
# MAGIC     address STRING,
# MAGIC     city STRING,
# MAGIC     state STRING,
# MAGIC     country STRING,
# MAGIC     created_at TIMESTAMP,
# MAGIC     updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA ;
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

# Load new geo data
geo_raw_df = spark.read.table("dev.bronze.geo_raw")

# Define target table
target_table = DeltaTable.forName(spark, "dev.gold.dim_geo")

# Perform Merge
target_table.alias("tgt").merge(
    geo_raw_df.alias("src"),
    "tgt.geo_key = src.geo_key"
).whenMatchedUpdate(set={
    "tgt.address": "src.address",
    "tgt.city": "src.city",
    "tgt.state": "src.state",
    "tgt.country": "src.country",
    "tgt.updated_at": "current_timestamp()"
}).whenNotMatchedInsert(values={
    "geo_key": "src.geo_key",
    "address": "src.address",
    "city": "src.city",
    "state": "src.state",
    "country": "src.country",
    "created_at": "current_timestamp()",
    "updated_at": "current_timestamp()"
}).execute()

