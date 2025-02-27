# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.gold.dim_products (
# MAGIC     product_id BIGINT,         
# MAGIC     product_code STRING,
# MAGIC     name STRING,
# MAGIC     brand STRING,
# MAGIC     category STRING,
# MAGIC     product_cost FLOAT    
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import upper, col, sum, round

# Load product data from Silver
product_df = spark.read.table("dev.silver.dim_products_raw")

# Load fact order data for cost calculation
fact_order_df = spark.read.table("dev.gold.fact_order")

# Aggregate total cost per product
product_cost_df = fact_order_df.groupBy("product_key").agg(
    round((sum(col("total_amount")) / sum(col("total_quantity"))),2).alias("product_cost")
)

# Apply transformations
transformed_df = product_df \
    .withColumnRenamed("product_key", "product_id") \
    .withColumn("product_code", upper(col("product_code"))) \
    .join(product_cost_df, col("product_id") == col("product_key"), "left")

# Define target table
target_table = DeltaTable.forName(spark, "dev.gold.dim_products")

# Perform Merge
target_table.alias("tgt").merge(
    transformed_df.alias("src"),
    "tgt.product_id = src.product_id"
).whenMatchedUpdate(set={
    "tgt.product_code": "src.product_code",
    "tgt.name": "src.name",
    "tgt.brand": "src.brand",
    "tgt.category": "src.category",
    "tgt.product_cost": "src.product_cost"
}).whenNotMatchedInsert(values={
    "product_id": "src.product_id",
    "product_code": "src.product_code",
    "name": "src.name",
    "brand": "src.brand",
    "category": "src.category",
    "product_cost": "src.product_cost"
}).execute()

# COMMAND ----------


