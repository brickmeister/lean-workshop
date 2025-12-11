# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Link patient data for patient journey
# MAGIC
# MAGIC To generate an overview of the patient journey from ADT messages, we'll be pulling from the following FHIR resources:
# MAGIC   1. MessageHeader
# MAGIC   2. Encounter
# MAGIC   3. Patient
# MAGIC   4. Account
# MAGIC   5. Organization
# MAGIC   6. CareTeam
# MAGIC   7. Location
# MAGIC   8. Coverage
# MAGIC

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
# MAGIC # Link Patient Journey

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --
# MAGIC -- Combine patient level data to get patient journey
# MAGIC --
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW V_PATIENT_JOURNEY AS (
# MAGIC   WITH _PATIENT AS (
# MAGIC         SELECT bundleUUID,
# MAGIC                timestamp,
# MAGIC                patient[0].identifier[0].value as PATIENT_ID
# MAGIC         FROM patient
# MAGIC     ),
# MAGIC     _MESSAGE_HEADER AS (
# MAGIC         SELECT bundleUUID,
# MAGIC                timestamp,
# MAGIC                MessageHeader[0].eventCoding.code as ADT_CODE
# MAGIC         FROM messageheader
# MAGIC     ),
# MAGIC     _ENCOUNTER AS (
# MAGIC         SELECT bundleUUID,
# MAGIC                timestamp,
# MAGIC                Encounter[0].status as encounter_status,
# MAGIC                Encounter[0].diagnosis.condition[0].concept.coding.code[0][0] as encounter_diagnosis
# MAGIC         FROM encounter
# MAGIC     ),
# MAGIC     _ACCOUNT AS (
# MAGIC         SELECT bundleUUID,
# MAGIC                timestamp,
# MAGIC                account[0].name as account_name,
# MAGIC                account[0].procedure.code.concept.coding[0].code[0] as account_procedure
# MAGIC         FROM account
# MAGIC     ),
# MAGIC     _ORGANIZATION AS (
# MAGIC         SELECT bundleUUID,
# MAGIC                timestamp,
# MAGIC                organization[0].name as organization_name
# MAGIC         FROM organization
# MAGIC     ),
# MAGIC     _CARE_TEAM AS (
# MAGIC         SELECT bundleUUID,
# MAGIC                timestamp,
# MAGIC                careteam[0].name as careteam_name,
# MAGIC                careteam[0].period.start as careteam_period_start,
# MAGIC                careteam[0].period.end as careteam_period_end,
# MAGIC                careteam[0].period.start - careteam[0].period.end as careteam_period_diff
# MAGIC         FROM careteam
# MAGIC     ),
# MAGIC     _LOCATION AS (
# MAGIC         select bundleUUID,
# MAGIC                timestamp,
# MAGIC                case 
# MAGIC                   when length(location[0].name) = 2
# MAGIC                     then location[0].name
# MAGIC                   when length(location[1].name) = 2
# MAGIC                     then location[1].name
# MAGIC                   when length(location[2].name) = 2
# MAGIC                     then location[2].name
# MAGIC                   when length(location[3].name) = 2
# MAGIC                     then location[3].name
# MAGIC                 end as location_point_of_care,
# MAGIC               case 
# MAGIC                   when length(location[0].name) = 3
# MAGIC                     then location[0].name
# MAGIC                   when length(location[1].name) = 3
# MAGIC                     then location[1].name
# MAGIC                   when length(location[2].name) = 3
# MAGIC                     then location[2].name
# MAGIC                   when length(location[3].name) = 3
# MAGIC                     then location[3].name
# MAGIC                 end as location_room,
# MAGIC               case 
# MAGIC                   when location[0].name LIKE '%-%'
# MAGIC                     then location[0].name
# MAGIC                   when location[1].name LIKE '%-%'
# MAGIC                     then location[1].name
# MAGIC                   when location[2].name LIKE '%-%'
# MAGIC                     then location[2].name
# MAGIC                   when location[3].name LIKE '%-%'
# MAGIC                     then location[3].name
# MAGIC                 end as location_item,
# MAGIC               case 
# MAGIC                   when location[0].name NOT LIKE '%-%'
# MAGIC                       AND length(location[0].name) > 3
# MAGIC                     then location[0].name
# MAGIC                   when location[1].name NOT LIKE '%-%'
# MAGIC                       AND length(location[1].name) > 3
# MAGIC                     then location[1].name
# MAGIC                   when location[2].name NOT LIKE '%-%'
# MAGIC                       AND length(location[2].name) > 3
# MAGIC                     then location[2].name
# MAGIC                   when location[3].name NOT LIKE '%-%'
# MAGIC                       AND length(location[3].name) > 3
# MAGIC                     then location[3].name
# MAGIC                 end as location_building
# MAGIC         from location
# MAGIC     ),
# MAGIC     _COVERAGE AS (
# MAGIC         SELECT bundleUUID,
# MAGIC                timestamp,
# MAGIC                coverage[0].insurer as insurer,
# MAGIC                coverage[0].insurancePlan as insurance_plan
# MAGIC         FROM coverage
# MAGIC     )
# MAGIC     SELECT A.bundleUUID,
# MAGIC            A.timestamp,
# MAGIC            A.patient_id,
# MAGIC            B.ADT_CODE,
# MAGIC            C.encounter_status,
# MAGIC            c.encounter_diagnosis,
# MAGIC            d.account_name,
# MAGIC            d.account_procedure,
# MAGIC            e.organization_name,
# MAGIC            f.careteam_name,
# MAGIC            f.careteam_period_start,
# MAGIC            f.careteam_period_end,
# MAGIC            f.careteam_period_diff,
# MAGIC            g.location_point_of_care,
# MAGIC            g.location_room,
# MAGIC            g.location_building,
# MAGIC            h.insurer,
# MAGIC            h.insurance_plan
# MAGIC     FROM _PATIENT AS A
# MAGIC     LEFT JOIN _MESSAGE_HEADER AS B
# MAGIC       ON A.bundleUUID = B.bundleUUID
# MAGIC     LEFT JOIN _ENCOUNTER AS C
# MAGIC       ON A.bundleUUID = C.bundleUUID
# MAGIC     LEFT JOIN _ACCOUNT AS D
# MAGIC       ON A.bundleUUID = D.bundleUUID
# MAGIC     LEFT JOIN _ORGANIZATION AS E
# MAGIC       ON A.bundleUUID = E.bundleUUID
# MAGIC     LEFT JOIN _CARE_TEAM AS F
# MAGIC       ON A.bundleUUID = F.bundleUUID
# MAGIC     LEFT JOIN _LOCATION AS G
# MAGIC       ON A.bundleUUID = G.bundleUUID
# MAGIC     LEFT JOIN _COVERAGE AS H
# MAGIC       ON A.bundleUUID = H.bundleUUID
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --
# MAGIC -- Showcase the patient journey
# MAGIC --
# MAGIC
# MAGIC select *
# MAGIC from V_PATIENT_JOURNEY;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --
# MAGIC -- Write out to delta
# MAGIC -- Add in the next adt_code for classification training
# MAGIC -- 
# MAGIC --
# MAGIC
# MAGIC CREATE OR REPLACE TABLE PATIENT_JOURNEY AS
# MAGIC   SELECT *,
# MAGIC          LEAD(ADT_CODE) OVER (PARTITION BY PATIENT_ID ORDER BY TIMESTAMP ASC) as NEXT_ADT_CODE,
# MAGIC          TIMESTAMP - LAG(TIMESTAMP) OVER (PARTITION BY PATIENT_ID ORDER BY TIMESTAMP ASC) AS DIFF_ADT_TIME
# MAGIC   FROM V_PATIENT_JOURNEY;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --
# MAGIC -- Display the results of patient journey
# MAGIC --
# MAGIC
# MAGIC SELECT *
# MAGIC FROM PATIENT_JOURNEY;

# COMMAND ----------


