# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dev.bronze.customer_raw (
# MAGIC     customer_key BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     full_name STRING,
# MAGIC     marital_status STRING,
# MAGIC     gender STRING,
# MAGIC     birthdate DATE
# MAGIC );
# MAGIC

# COMMAND ----------

transactions_raw = spark.read.table("dev.bronze.transactions_raw")

distinct_customers = transactions_raw.select(
    "full_name", "marital_status", "gender", "birthdate"
).distinct()

# COMMAND ----------

# Overwrite customer_raw with distinct data
distinct_customers.write \
    .format("parquet") \
    .mode("overwrite") \
    .saveAsTable("dev.bronze.customer_raw")

