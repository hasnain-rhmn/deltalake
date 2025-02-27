# Databricks notebook source
landing_dir = 'abfss://delta-lake@storageaccdeltalake.dfs.core.windows.net/landing/'
checkpoint = 'abfss://delta-lake@storageaccdeltalake.dfs.core.windows.net/dev-catalog-data/checkpoints/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dev.bronze.transactions_raw (
# MAGIC     order_id INT,
# MAGIC     order_datetime TIMESTAMP,
# MAGIC     product_code STRING,
# MAGIC     name STRING,
# MAGIC     brand STRING,
# MAGIC     category STRING,
# MAGIC     total_quantity INT,
# MAGIC     total_amount FLOAT,
# MAGIC     address STRING,
# MAGIC     city STRING,
# MAGIC     state STRING,
# MAGIC     country STRING,
# MAGIC     full_name STRING,
# MAGIC     marital_status STRING,
# MAGIC     gender STRING,
# MAGIC     birthdate DATE
# MAGIC );

# COMMAND ----------

from pyspark.sql.types import StructType, IntegerType, StringType, FloatType, TimestampType, DateType

schema = StructType() \
    .add("order_id", IntegerType(), True) \
    .add("order_datetime", TimestampType(), True) \
    .add("product_code", StringType(), True) \
    .add("name", StringType(), True) \
    .add("brand", StringType(), True) \
    .add("category", StringType(), True) \
    .add("total_quantity", IntegerType(), True) \
    .add("total_amount", FloatType(), True) \
    .add("address", StringType(), True) \
    .add("city", StringType(), True) \
    .add("state", StringType(), True) \
    .add("country", StringType(), True) \
    .add("full_name", StringType(), True) \
    .add("marital_status", StringType(), True) \
    .add("gender", StringType(), True) \
    .add("birthdate", DateType(), True)


# COMMAND ----------


transactions_raw = spark.readStream \
    .format("parquet") \
    .schema(schema) \
    .load(landing_dir)

# COMMAND ----------

transactions_raw.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint) \
    .trigger(once=True) \
    .table("dev.bronze.transactions_raw")
