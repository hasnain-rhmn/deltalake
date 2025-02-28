# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.gold.dim_customers (
# MAGIC     customer_id BIGINT,  
# MAGIC     first_name STRING,
# MAGIC     last_name STRING,
# MAGIC     marital_status STRING,
# MAGIC     gender STRING,
# MAGIC     birthdate DATE,
# MAGIC     age INT,
# MAGIC     age_group STRING,
# MAGIC     updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, max, split, year, current_date, when

# Load target table to get the last updated timestamp
target_table = DeltaTable.forName(spark, "dev.gold.dim_customers")

# Get max updated timestamp from target table
max_updated_at = spark.read.table("dev.gold.dim_customers").select(max("updated_at")).collect()[0][0]

# When target table is empty
if max_updated_at is None:
    max_updated_at = "1900-01-01 00:00:00"

# Read changes from the CDF-enabled source table since the last update
customer_changes_df = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingTimestamp", max_updated_at) \
    .table("dev.silver.dim_customer_raw") \
    .filter(col("_change_type").isin("insert", "update_postimage")) 

# Apply transformations
transformed_df = customer_changes_df \
    .withColumn("first_name", split(col("full_name"), " ")[0]) \
    .withColumn("last_name", split(col("full_name"), " ")[1]) \
    .withColumn("age", year(current_date()) - year(col("birthdate"))) \
    .withColumn("age_group", when(col("age") < 18, "Teens")
                            .when((col("age") >= 18) & (col("age") < 35), "Young Adults")
                            .when((col("age") >= 35) & (col("age") < 50), "Middle-Aged Adults")
                            .otherwise("Senior Adults"))

# Perform Merge
target_table.alias("tgt").merge(
    transformed_df.alias("src"),
    "tgt.customer_id = src.customer_key"
).whenMatchedUpdate(set={
    "tgt.first_name": "src.first_name",
    "tgt.last_name": "src.last_name",
    "tgt.marital_status": "src.marital_status",
    "tgt.gender": "src.gender",
    "tgt.birthdate": "src.birthdate",
    "tgt.age": "src.age",
    "tgt.age_group": "src.age_group",
    "tgt.updated_at": "src.updated_at"
}).whenNotMatchedInsert(values={
    "customer_id": "src.customer_key",
    "first_name": "src.first_name",
    "last_name": "src.last_name",
    "marital_status": "src.marital_status",
    "gender": "src.gender",
    "birthdate": "src.birthdate",
    "age": "src.age",
    "age_group": "src.age_group",
    "updated_at": "src.updated_at"
}).execute()
