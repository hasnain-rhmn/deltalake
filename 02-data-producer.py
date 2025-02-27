# Databricks notebook source
pip install faker

# COMMAND ----------

from datetime import datetime, timedelta

# Number of rows to generate
num_rows_customer = 50000
num_rows_order = 1000000

start_date = '2024-04-01'

# Range for customer, product, and location IDs to be randomly assigned
customer_id_range = (1, num_rows_customer)
product_id_range = (1, num_rows_customer)
location_id_range = (1, num_rows_customer)

# COMMAND ----------

from pyspark.sql.functions import udf, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
import random
from faker import Faker

# Initialize Faker instance
fake = Faker()

# UDF for generating customer data
def generate_customer():
    return (fake.name(), random.choice(["Single", "Married", "Divorced"]), random.choice(["Male", "Female", "Other"]),
            fake.date_of_birth(minimum_age=18, maximum_age=80).strftime('%Y-%m-%d'))

# UDF for generating product data
def generate_product():
    return (fake.bothify(text='???-#####'), fake.word(), fake.company(), random.choice(["Electronics", "Clothing", "Furniture", "Books", "Home Appliances", "Toys", 
                "Health & Beauty", "Sports Equipment", "Automotive", "Kitchenware"]))

# UDF for generating location data
def generate_location():
    return (fake.address().replace("\n", ", "), fake.city(), fake.state(), fake.country())


# Define the schema for the DataFrames
customer_schema = StructType([
    #StructField("customer_id", IntegerType(), False),
    StructField("full_name", StringType(), True),
    StructField("marital_status", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birthdate", StringType(), True)
])

product_schema = StructType([
    #StructField("product_id", IntegerType(), False),
    StructField("product_code", StringType(), True),
    StructField("name", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("category", StringType(), True)
])

location_schema = StructType([
    #StructField("location_id", IntegerType(), False),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
])

# Register the UDFs
generate_customer_udf = udf(generate_customer, customer_schema)
generate_product_udf = udf(generate_product, product_schema)
generate_location_udf = udf(generate_location, location_schema)


# Generate customer dataframe
customers_df = spark.range(1, num_rows_customer + 1).selectExpr("id as customer_id")
customers_df = customers_df.withColumn("customer_data", generate_customer_udf()).select(
    "customer_id", "customer_data.*"
)

# Generate product dataframe
products_df = spark.range(1, num_rows_customer + 1).selectExpr("id as product_id")
products_df = products_df.withColumn("product_data", generate_product_udf()).select(
    "product_id", "product_data.*"
)

# Generate location dataframe
locations_df = spark.range(1, num_rows_customer + 1).selectExpr("id as location_id")
locations_df = locations_df.withColumn("location_data", generate_location_udf()).select(
    "location_id", "location_data.*"
)




# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from datetime import datetime, timedelta
import random

# Create a DataFrame with 'num_rows_order' rows
orders_df = spark.range(1, num_rows_order + 1).selectExpr("id as order_id")

# Generate random product_id, location_id, customer_id, total_quantity, total_amount, and order_datetime
orders_df = orders_df.withColumn(
    "product_id", (F.floor(F.rand() * num_rows_customer) + 1).cast(IntegerType())  # Random product_id
).withColumn(
    "location_id", (F.floor(F.rand() * num_rows_customer) + 1).cast(IntegerType())  # Random location_id
).withColumn(
    "customer_id", (F.floor(F.rand() * num_rows_customer) + 1).cast(IntegerType())  # Random customer_id
).withColumn(
    "total_quantity", (F.floor(F.rand() * 10) + 1).cast(IntegerType())  # Random quantity between 1 and 10
).withColumn(
    "total_amount", F.round(F.rand() * 500 + 20, 2).cast(FloatType())  # Random amount between 20 and 500
).withColumn(
    "order_datetime",  # Random date between start_date and 30 days later with random time
    F.date_add(F.lit(start_date), F.floor(F.rand() * 30).cast(IntegerType()))  # Random date
)

# Add random time (hours, minutes, seconds) to the order_datetime
orders_df = orders_df.withColumn(
    "order_datetime", 
    F.concat(
        F.col("order_datetime"),
        F.lit(" "),
        (F.floor(F.rand() * 24)).cast(IntegerType()).cast(StringType()),  # Random hour (0-23)
        F.lit(":"),
        (F.floor(F.rand() * 60)).cast(IntegerType()).cast(StringType()),  # Random minute (0-59)
        F.lit(":"),
        (F.floor(F.rand() * 60)).cast(IntegerType()).cast(StringType())   # Random second (0-59)
    )
)


# COMMAND ----------

# Join orders_df with products_df on product_id
orders_products_df = orders_df.alias("orders").join(
    products_df.alias("products"), 
    F.col("orders.product_id") == F.col("products.product_id"), 
    how="inner"
)

# Join the result with locations_df on location_id
orders_products_locations_df = orders_products_df.join(
    locations_df.alias("locations"), 
    F.col("orders.location_id") == F.col("locations.location_id"), 
    how="inner"
)

# Join the result with customers_df on customer_id
final_df = orders_products_locations_df.join(
    customers_df.alias("customers"), 
    F.col("orders.customer_id") == F.col("customers.customer_id"), 
    how="inner"
)

final_df = final_df.select(
    'order_id', 'order_datetime', 'product_code',
    'name', 'brand', 'category',
    'total_quantity', 'total_amount',
    'address', 'city', 'state', 'country',
    'full_name', 'marital_status', 'gender', 'birthdate'
)


# COMMAND ----------

final_df = final_df.select(
    'order_id', 'order_datetime', 'product_code',
    'name', 'brand', 'category',
    'total_quantity', 'total_amount',
    'address', 'city', 'state', 'country',
    'full_name', 'marital_status', 'gender', 'birthdate'
)

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.coalesce(1).write.csv(
    f'abfss://raw-data@storageaccrawdata.dfs.core.windows.net/{start_date}_transactions.csv', 
    header=True, 
    mode='overwrite'
)
