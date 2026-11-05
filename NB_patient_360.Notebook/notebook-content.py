# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5f460371-eb13-4b92-bab1-f0cb9a4d5945",
# META       "default_lakehouse_name": "lh_caresync",
# META       "default_lakehouse_workspace_id": "a4385c18-728e-40ab-8981-6717e6f19984",
# META       "known_lakehouses": [
# META         {
# META           "id": "5f460371-eb13-4b92-bab1-f0cb9a4d5945"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # NB_patient_360 – Native Fabric PySpark implementation
# 
# This notebook builds the `lh_caresync.gold.patient_360` table directly in Fabric using PySpark/SQL.
# 
# The logic is migrated from the original Databricks notebook `NB_patient_360` and no longer depends on reading the exported `.py` file from `Files/databricks_notebooks`.
# 
# Run the code cell below to (re)create the `patient_360` gold table in the `lh_caresync` Lakehouse.

# CELL ********************

# NB_patient_360 – native Fabric PySpark logic
# This cell creates or replaces the gold table lh_caresync.gold.patient_360
# using source data from the lh_caresync.silver layer.

from pyspark.sql import SparkSession

# Create or replace the patient_360 gold table
spark.sql(
    """
    CREATE OR REPLACE TABLE lh_caresync.gold.patient_360 AS
    WITH lab_results_agg AS (
      SELECT
        patient_id,
        CAST(COUNT(*) AS INT) AS total_tests,
        CAST(SUM(CASE WHEN result_status = 'Abnormal' THEN 1 ELSE 0 END) AS INT) AS abnormal_test_count,
        CAST(SUM(CASE WHEN result_status = 'Critical' THEN 1 ELSE 0 END) AS INT) AS critical_test_count,
        MAX(test_date) AS last_test_date
      FROM lh_caresync.silver.labresults
      GROUP BY patient_id
    )
    SELECT
      p.patient_id,
      p.first_name,
      p.last_name,
      CAST(p.date_of_birth AS DATE) AS date_of_birth,
      CAST(FLOOR(datediff(current_date(), CAST(p.date_of_birth AS DATE)) / 365.25) AS INT) AS age,
      p.gender,
      p.city,
      p.state,
      p.registered_hospital_id AS hospital_id,
      h.hospital_name,
      p.insurance_provider_id,
      ip.provider_name AS insurance_provider_name,
      COALESCE(lra.total_tests, 0) AS total_tests,
      COALESCE(lra.abnormal_test_count, 0) AS abnormal_test_count,
      COALESCE(lra.critical_test_count, 0) AS critical_test_count,
      lra.last_test_date
    FROM lh_caresync.silver.patients AS p
    LEFT JOIN lh_caresync.silver.hospitals AS h
      ON p.registered_hospital_id = h.hospital_id
    LEFT JOIN lh_caresync.silver.insuranceproviders AS ip
      ON p.insurance_provider_id = ip.insurance_provider_id
    LEFT JOIN lab_results_agg AS lra
      ON p.patient_id = lra.patient_id
    """
)

# Simple verification: count rows and show a small sample
row_count = spark.table("lh_caresync.gold.patient_360").count()
print(f"lh_caresync.gold.patient_360 row count: {row_count}")

display(spark.table("lh_caresync.gold.patient_360").limit(20))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
