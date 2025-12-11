# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # We can get useful admin/ops level information from the ADT messages

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %run ../config/setup

# COMMAND ----------


"""
Configure destination for files
"""

for _query in (f"USE CATALOG {catalog_name}", f"USE DATABASE {database_name}"):
  try:
    spark.sql(_query)
  except Exception as error:
    print(f"Error : {error}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Process Data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --
# MAGIC -- Process the amount of rooms per lcoation
# MAGIC --
# MAGIC
# MAGIC CREATE OR REPLACE VIEW V_BEDS_CAPACITY AS
# MAGIC   WITH _DATA AS (
# MAGIC       SELECT DATE(TIMESTAMP) AS DATE,
# MAGIC              LOCATION_BUILDING,
# MAGIC              LOCATION_POINT_OF_CARE,
# MAGIC              LOCATION_ROOM,
# MAGIC              PATIENT_ID,
# MAGIC              ADT_A02,
# MAGIC              ADT_A31,
# MAGIC              ADT_A03,
# MAGIC              ADT_A01
# MAGIC       FROM patient_journey
# MAGIC       PIVOT (1 FOR ADT_CODE IN ("ADT_A02", "ADT_A31", "ADT_A03", "ADT_A01"))
# MAGIC     ),
# MAGIC     _DATA_AGG AS (
# MAGIC       SELECT DATE,
# MAGIC              LOCATION_BUILDING,
# MAGIC              LOCATION_POINT_OF_CARE,
# MAGIC              COUNT(LOCATION_ROOM) as ROOM_COUNTS,
# MAGIC              COUNT(PATIENT_ID) AS PATIENT_COUNTS,
# MAGIC              SUM(ADT_A02) AS ADT_A02,
# MAGIC              SUM(ADT_A03) AS ADT_A03,
# MAGIC              SUM(ADT_A31) AS ADT_A31,
# MAGIC              SUM(ADT_A01) AS ADT_A01
# MAGIC       FROM _DATA
# MAGIC       GROUP BY DATE,
# MAGIC               LOCATION_BUILDING,
# MAGIC               LOCATION_POINT_OF_CARE
# MAGIC     )
# MAGIC     SELECT *,
# MAGIC            LEAD(ROOM_COUNTS) OVER (PARTITION BY LOCATION_POINT_OF_CARE ORDER BY DATE ASC) AS NEXT_ROOM_COUNTS
# MAGIC     FROM _DATA_AGG;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --
# MAGIC -- Write data to delta
# MAGIC --
# MAGIC
# MAGIC CREATE OR REPLACE TABLE BEDS_CAPACITY AS
# MAGIC   SELECT *
# MAGIC   FROM V_BEDS_CAPACITY;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --
# MAGIC --
# MAGIC -- Get bed capacity
# MAGIC
# MAGIC select *
# MAGIC from beds_capacity;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Train Model with AutoML

# COMMAND ----------


"""
Forecast Bed Count codes using AutoML
"""

from databricks import automl

# Columsn to train against
columns = ["DATE", "LOCATION_BUILDING", "LOCATION_POINT_OF_CARE", "ROOM_COUNTS", "NEXT_ROOM_COUNTS", "PATIENT_COUNTS", "ADT_A02", "ADT_A31", "ADT_A01", "ADT_A03"]

# Load dataset
train_df = spark.read.format("delta").table(f"{catalog_name}.{database_name}.beds_capacity").select(*columns)

try:
  # Run AutoML
  summary = automl.regress(train_df, target_col="NEXT_ROOM_COUNTS", timeout_minutes=30)
except Exception as error:
  raise ValueError(f"Failed to train regressor, error : {err}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Predict with AutoML

# COMMAND ----------


"""
Predict next room capacity with best model
"""

import mlflow
from pyspark.sql.functions import round

# get the model URI for the best model
model_uri = summary.best_trial.model_path

# get some ADT messages
df = spark.read.format("delta").table(f"{catalog_name}.{database_name}.beds_capacity")

try:
  # setup the pyfunc udf for prediction
  predict_udf = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri, result_type="string")
except Exception as err:
  raise ValueError(f"Failed to load model for inferencing, model_uri : {model_uri}, error : {err}")

# predict the results
predict_df = df.withColumn("predicted_next_room_counts", round(predict_udf()))

try:
  # display the results
  display(predict_df.select("location_point_of_care", "room_counts", "next_room_counts", "predicted_next_room_counts"))
except Exception as err:
  raise ValueError(f"Failed to inference with model : {model_uri}, error : {err}")

# COMMAND ----------


"""
Write results to delta
"""

predict_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{database_name}.beds_capacity_forecasted")
