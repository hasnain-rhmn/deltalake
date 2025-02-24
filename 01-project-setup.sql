-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS dev
MANAGED LOCATION 'abfss://delta-lake@storageaccdeltalake.dfs.core.windows.net/dev-catalog-data';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS dev.bronze
MANAGED LOCATION 'abfss://delta-lake@storageaccdeltalake.dfs.core.windows.net/dev-catalog-data/bronze';

CREATE SCHEMA IF NOT EXISTS dev.silver
MANAGED LOCATION 'abfss://delta-lake@storageaccdeltalake.dfs.core.windows.net/dev-catalog-data/silver';

CREATE SCHEMA IF NOT EXISTS dev.gold
MANAGED LOCATION 'abfss://delta-lake@storageaccdeltalake.dfs.core.windows.net/dev-catalog-data/gold';

