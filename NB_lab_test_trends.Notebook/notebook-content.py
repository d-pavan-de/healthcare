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

# # NB_lab_test_trends (Fabric PySpark version)
# 
# This notebook is a Fabric PySpark conversion of the Databricks notebook stored at `Files/databricks_notebooks/NB_lab_test_trends.py`.
# 
# It builds a `lab_test_trends` gold table from monthly lab-result aggregates with optional calendar enrichment, then computes trend metrics.

# CELL ********************

# Create lab test trends gold table (Fabric PySpark translation of Databricks %sql cell)

# NOTE:
# - This assumes the source tables `lh_caresync.silver.labresults` and
#   `lh_caresync.silver.calendar` are available in this Spark catalog.
# - If your tables live in a Fabric Lakehouse schema (e.g., `lh_caresync.silver.labresults`),
#   update the references below accordingly.

# Create schema if it does not exist
spark.sql("""
CREATE SCHEMA IF NOT EXISTS lh_caresync.gold
""")

# Create or replace the lab_test_trends gold table
spark.sql("""
CREATE OR REPLACE TABLE lh_caresync.gold.lab_test_trends AS
WITH monthly_metrics AS (
  SELECT
    test_name,
    year(test_date) AS year,
    month(test_date) AS month,
    date_trunc('month', test_date) AS month_start_date,
    CAST(COUNT(*) AS INT) AS total_tests,
    CAST(SUM(CASE WHEN result_status = 'Abnormal' THEN 1 ELSE 0 END) AS INT) AS abnormal_test_count,
    CAST(SUM(CASE WHEN result_status = 'Critical' THEN 1 ELSE 0 END) AS INT) AS critical_test_count
  FROM lh_caresync.silver.labresults
  GROUP BY test_name, year(test_date), month(test_date), date_trunc('month', test_date)
),

enriched AS (
  SELECT
    mm.test_name,
    mm.year,
    mm.month,
    COALESCE(c.month_name, date_format(mm.month_start_date, 'MMMM')) AS month_name,
    CONCAT('Q', quarter(mm.month_start_date)) AS quarter_name,
    mm.total_tests,
    mm.abnormal_test_count,
    mm.critical_test_count,
    CAST(CASE WHEN mm.total_tests = 0 THEN NULL ELSE ROUND(mm.abnormal_test_count * 100.0 / mm.total_tests, 2) END AS DECIMAL(5,2)) AS abnormal_rate_pct,
    CAST(CASE WHEN mm.total_tests = 0 THEN NULL ELSE ROUND(mm.critical_test_count * 100.0 / mm.total_tests, 2) END AS DECIMAL(5,2)) AS critical_rate_pct
  FROM monthly_metrics AS mm
  LEFT JOIN lh_caresync.silver.calendar AS c
    ON CAST(date_format(mm.month_start_date, 'yyyyMMdd') AS INT) = c.date_key
)

SELECT
  test_name,
  year,
  month,
  month_name,
  quarter_name,
  total_tests,
  abnormal_test_count,
  critical_test_count,
  abnormal_rate_pct,
  critical_rate_pct,
  CAST(LAG(abnormal_rate_pct) OVER (PARTITION BY test_name ORDER BY year, month) AS DECIMAL(5,2)) AS prev_month_abnormal_rate_pct,
  CAST(
    CASE
      WHEN LAG(abnormal_rate_pct) OVER (PARTITION BY test_name ORDER BY year, month) IS NULL THEN NULL
      ELSE ROUND(
        abnormal_rate_pct - LAG(abnormal_rate_pct) OVER (PARTITION BY test_name ORDER BY year, month),
        2
      )
    END AS DECIMAL(6,2)
  ) AS abnormal_rate_change_pct
FROM enriched
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
