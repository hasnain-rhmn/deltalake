# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.gold.fact_order (
# MAGIC     order_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     order_datetime TIMESTAMP,
# MAGIC     order_date DATE,
# MAGIC     product_key BIGINT,
# MAGIC     customer_key BIGINT,
# MAGIC     geo_key BIGINT,
# MAGIC     total_quantity INT,
# MAGIC     total_amount FLOAT
# MAGIC ) 
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (order_date);
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

# Read raw tables
transactions_df = spark.read.table("dev.bronze.transactions_raw")
product_df = spark.read.table("dev.bronze.product_raw")
customer_df = spark.read.table("dev.bronze.customer_raw")
geo_df = spark.read.table("dev.bronze.geo_raw")

# Join raw data to get surrogate keys
fact_order_df = transactions_df.alias("t") \
    .join(product_df.alias("p"), ["product_code", "name", "brand", "category"], "left") \
    .join(customer_df.alias("c"), ["full_name", "marital_status", "gender", "birthdate"], "left") \
    .join(geo_df.alias("g"), ["address", "city", "state", "country"], "left") \
    .withColumn('order_date', F.to_date("order_datetime")) \
    .select(
        "t.order_datetime",
        "p.product_key",
        "c.customer_key",
        "g.geo_key",
        "t.total_quantity",
        "t.total_amount",
        "order_date"
    )

# COMMAND ----------

fact_order_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("dev.gold.fact_order")
