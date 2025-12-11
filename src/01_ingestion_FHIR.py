# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Ingest HL7 FHIR encoded messages with DBIgnite

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Install DBIgnite

# COMMAND ----------

# MAGIC %pip install git+https://github.com/databrickslabs/dbignite.git

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Setup

# COMMAND ----------

# MAGIC %run ../config/setup

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Ingest ADT messages to delta

# COMMAND ----------


"""
Parse ADT messages from FHIR bundle
"""

from dbignite.hosp_feeds.adt import ADTActions
from pyspark.sql.functions import size,col, sum
from dbignite.readers import read_from_directory

# load for ADT actions
ADTActions()

#Read sample FHIR Bundle data from this repo
try:
  bundle = read_from_directory(bundle_directory)
  bundle.entry()
except Exception as err:
  raise ValueError(f"Failed to read FHIR bundles from {bundle_directory}, error : {err}")

#Show the total number of patient resources in all bundles
bundle.count_resource_type("Patient").show()

# COMMAND ----------


"""
Write out the data to a delta table
"""

# set the location for the data
location = '.'.join((catalog_name, database_name))

try:
  bundle.bulk_table_write(location=location,
                          write_mode="overwrite",
                          columns=["Patient", "MessageHeader", "Encounter",
                                  "Account", "Organization",
                                  "CareTeam", "Location", "Coverage"]
  )
except Exception as err:
  raise ValueError(f"Failed to write out tables at {location}, error : {err}")
