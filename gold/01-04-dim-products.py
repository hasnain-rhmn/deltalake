# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.gold.dim_products (
# MAGIC     product_id BIGINT,         
# MAGIC     product_code STRING,
# MAGIC     name STRING,
# MAGIC     brand STRING,
# MAGIC     category STRING,
# MAGIC     product_cost FLOAT,
# MAGIC     updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, upper, sum, round, max

# Load target table reference
target_table = DeltaTable.forName(spark, "dev.gold.dim_products")

# Get max updated timestamp from target table
max_updated_at = spark.read.table("dev.gold.dim_products").select(max("updated_at")).collect()[0][0]

# Handle empty target table
if max_updated_at is None:
    max_updated_at = "1900-01-01 00:00:00"

# Read changes from the CDF-enabled source table since the last update
product_changes_df = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingTimestamp", max_updated_at) \
    .table("dev.silver.dim_products_raw") \
    .filter(col("_change_type").isin("insert", "update_postimage")) 

# Load fact order data for cost calculation
fact_order_df = spark.read.table("dev.gold.fact_order")

# Aggregate total cost per product
product_cost_df = fact_order_df.groupBy("product_key").agg(
    round((sum(col("total_amount")) / sum(col("total_quantity"))), 2).alias("product_cost")
)

# Apply transformations before merging
transformed_df = product_changes_df \
    .withColumnRenamed("product_key", "product_id") \
    .withColumn("product_code", upper(col("product_code"))) \
    .join(product_cost_df, col("product_id") == col("product_key"), "left") \
    .select("product_id", "product_code", "name", "brand", "category", "product_cost", "updated_at")

# Perform Merge
target_table.alias("tgt").merge(
    transformed_df.alias("src"),
    "tgt.product_id = src.product_id"
).whenMatchedUpdate(set={
    "tgt.product_code": "src.product_code",
    "tgt.name": "src.name",
    "tgt.brand": "src.brand",
    "tgt.category": "src.category",
    "tgt.product_cost": "src.product_cost",
    "tgt.updated_at": "src.updated_at"
}).whenNotMatchedInsert(values={
    "product_id": "src.product_id",
    "product_code": "src.product_code",
    "name": "src.name",
    "brand": "src.brand",
    "category": "src.category",
    "product_cost": "src.product_cost",
    "updated_at": "src.updated_at"
}).execute()

