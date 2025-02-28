# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dev.bronze.product_raw (
# MAGIC     product_key BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     product_code STRING,
# MAGIC     name STRING,
# MAGIC     brand STRING,
# MAGIC     category STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

transactions_raw = spark.read.table("dev.bronze.transactions_raw")

distinct_products = transactions_raw.select(
    "product_code", "name", "brand", "category"
).distinct()

# COMMAND ----------

distinct_products.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dev.bronze.product_raw")
