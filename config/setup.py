# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Setup database credentials

# COMMAND ----------


"""
Setup the name of the database
"""

import os

catalog_name = "ml_demos"
database_name = "lean_workshop"
data_directory = os.getcwd().replace("/Workspace/", "file:/Workspace/").replace("src", "data")
# bundle_directory = data_directory
bundle_directory = f"/Volumes/{catalog_name}/{database_name}/data"

# COMMAND ----------


"""
Create catalogs, databases, and volumes
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

try:
  spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{database_name}.data")
except Exception as err:
  raise ValueError(f"Failed to create volume : {{bundle_directory}}, error : {err}")
