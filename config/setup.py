# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Setup database credentials

# COMMAND ----------


"""
Setup the name of the database
"""

catalog_name = "ml_demos"
database_name = "redox_himss"
bundle_directory = "dbfs:/tmp/ml_demo"

# COMMAND ----------


"""
Create catalogs and databases
"""

# create catalog and database if needed
try:
  spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
except Exception as err:
  raise ValueError(f"Failed to create catalog : {catalog_name}, error : {err}")

try:
  spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{database_name}")
except Exception as err:
  raise ValueError(f"Failed to create database : {catalog_name}, error : {err}")
