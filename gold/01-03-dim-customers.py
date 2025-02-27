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
# MAGIC     age_group STRING
# MAGIC )
# MAGIC USING DELTA ;

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import split, year, current_date, when, col, current_timestamp

# Load raw customer data
customer_raw_df = spark.read.table("dev.silver.dim_customers_raw")

# Apply transformations
transformed_df = customer_raw_df \
    .withColumn("first_name", split(col("full_name"), " ")[0]) \
    .withColumn("last_name", split(col("full_name"), " ")[1]) \
    .withColumn("age", year(current_date()) - year(col("birthdate"))) \
    .withColumn("age_group", when(col("age") < 18, "Teens")
                            .when((col("age") >= 18) & (col("age") < 35), "Young Adults")
                            .when((col("age") >= 35) & (col("age") < 50), "Middle-Aged Adults")
                            .otherwise("Senior Adults"))

# Define target table
target_table = DeltaTable.forName(spark, "dev.gold.dim_customers")

# Perform Merge into transformed table
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
    "tgt.age_group": "src.age_group"
}).whenNotMatchedInsert(values={
    "customer_id": "src.customer_key",
    "first_name": "src.first_name",
    "last_name": "src.last_name",
    "marital_status": "src.marital_status",
    "gender": "src.gender",
    "birthdate": "src.birthdate",
    "age": "src.age",
    "age_group": "src.age_group"
}).execute()
