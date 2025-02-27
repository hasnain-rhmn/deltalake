# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dev.bronze.geo_raw (
# MAGIC     geo_key BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     address STRING,
# MAGIC     city STRING,
# MAGIC     state STRING,
# MAGIC     country STRING
# MAGIC ) ;

# COMMAND ----------

transactions_raw = spark.read.table("dev.bronze.transactions_raw")

distinct_geo = transactions_raw.select(
    "address", "city", "state", "country"
).distinct()

# COMMAND ----------

distinct_geo.write \
    .format("parquet") \
    .mode("overwrite") \
    .saveAsTable("dev.bronze.geo_raw")
