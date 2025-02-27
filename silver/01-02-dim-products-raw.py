# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.silver.dim_products_raw (
# MAGIC     product_key BIGINT,
# MAGIC     product_code STRING,
# MAGIC     name STRING,
# MAGIC     brand STRING,
# MAGIC     category STRING,
# MAGIC     created_at TIMESTAMP,
# MAGIC     updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'true'
# MAGIC );

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

# Load new product data
product_raw_df = spark.read.table("dev.bronze.product_raw")

# Define target table
target_table = DeltaTable.forName(spark, "dev.silver.dim_products_raw")

# Perform Merge
target_table.alias("tgt").merge(
    product_raw_df.alias("src"),
    "tgt.product_key = src.product_key"
).whenMatchedUpdate(set={
    "tgt.product_code": "src.product_code",
    "tgt.name": "src.name",
    "tgt.brand": "src.brand",
    "tgt.category": "src.category",
    "tgt.updated_at": "current_timestamp()"
}).whenNotMatchedInsert(values={
    "product_key": "src.product_key",
    "product_code": "src.product_code",
    "name": "src.name",
    "brand": "src.brand",
    "category": "src.category",
    "created_at": "current_timestamp()",
    "updated_at": "current_timestamp()"
}).execute()

