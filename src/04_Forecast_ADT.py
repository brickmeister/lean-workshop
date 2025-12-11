# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # AutoML For Training Data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Setup

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
# MAGIC ## Forecasting ADT codes for Patients

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Train Model with AutoML

# COMMAND ----------


"""
Forecast ADT codes using AutoML
"""

from databricks import automl

# Columsn to train against
columns = ["ADT_CODE", "encounter_status", "encounter_diagnosis", "account_name", 
           "account_procedure", "organization_name", "careteam_name", "careteam_period_start",
           "careteam_period_end", "careteam_period_diff", "location_point_of_care", "location_room",
           "insurer", "insurance_plan", "DIFF_ADT_TIME", "NEXT_ADT_CODE"]

# Load dataset
train_df = spark.read.format("delta").table(f"{catalog_name}.{database_name}.patient_journey_train").select(*columns)

try:
  # Run AutoML
  summary = automl.classify(train_df, target_col="NEXT_ADT_CODE", timeout_minutes=30)
except Exception as error:
  raise ValueError(f"Failed to train classifier, error : {err}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Predict with AutoML

# COMMAND ----------


"""
Predict next ADT code with best model
"""

import mlflow

# get the model URI for the best model
model_uri = summary.best_trial.model_path

# get some ADT messages
df = spark.read.format("delta").table(f"{catalog_name}.{database_name}.patient_journey")

try:
  # setup the pyfunc udf for prediction
  predict_udf = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri, result_type="string")
except Exception as err:
  raise ValueError(f"Failed to load model for inferencing, model_uri : {model_uri}, error : {err}")

# predict the results
predict_df = df.withColumn("predicted_next_adt_code", predict_udf())

try:
  # display the results
  display(predict_df.select("patient_id", "adt_code", "next_adt_code", "predicted_next_adt_code"))
except Exception as err:
  raise ValueError(f"Failed to inference with model : {model_uri}, error : {err}")

# COMMAND ----------


"""
Write predicted results to Delta Lake
"""

predict_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{database_name}.patient_journey_forecasted")
