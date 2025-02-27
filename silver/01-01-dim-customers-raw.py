# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.silver.dim_customers_raw (
# MAGIC     customer_key BIGINT,
# MAGIC     full_name STRING,
# MAGIC     marital_status STRING,
# MAGIC     gender STRING,
# MAGIC     birthdate DATE,
# MAGIC     created_at TIMESTAMP,
# MAGIC     updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'true'
# MAGIC );
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

# Load new customer data
customer_raw_df = spark.read.table("dev.bronze.customer_raw")

# Define target table
target_table = DeltaTable.forName(spark, "dev.silver.dim_customers_raw")

# Perform Merge
target_table.alias("tgt").merge(
    customer_raw_df.alias("src"),
    "tgt.customer_key = src.customer_key"
).whenMatchedUpdate(set={
    "tgt.full_name": "src.full_name",
    "tgt.marital_status": "src.marital_status",
    "tgt.gender": "src.gender",
    "tgt.birthdate": "src.birthdate",
    "tgt.updated_at": "current_timestamp()"
}).whenNotMatchedInsert(values={
    "customer_key": "src.customer_key",
    "full_name": "src.full_name",
    "marital_status": "src.marital_status",
    "gender": "src.gender",
    "birthdate": "src.birthdate",
    "created_at": "current_timestamp()",
    "updated_at": "current_timestamp()"
}).execute()

