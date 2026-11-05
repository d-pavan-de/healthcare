-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "5f460371-eb13-4b92-bab1-f0cb9a4d5945",
-- META       "default_lakehouse_name": "lh_caresync",
-- META       "default_lakehouse_workspace_id": "a4385c18-728e-40ab-8981-6717e6f19984",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "5f460371-eb13-4b92-bab1-f0cb9a4d5945"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

CREATE SCHEMA IF NOT EXISTS lh_caresync.silver;

CREATE OR REPLACE TABLE lh_caresync.silver.calendar AS
WITH dates AS (
  SELECT explode(
    sequence(
      TO_DATE('1990-01-01'),
      TO_DATE('2030-12-31'),
      INTERVAL 1 DAY
    )
  ) AS calendar_date
)
SELECT
  CAST(date_format(calendar_date, 'yyyyMMdd') AS INT) AS date_key,
  calendar_date AS date,
  year(calendar_date) AS year,
  quarter(calendar_date) AS quarter,
  ((month(calendar_date) - 1) % 3) + 1 AS month_of_quarter,
  month(calendar_date) AS month,
  date_format(calendar_date, 'MMMM') AS month_name,
  date_format(calendar_date, 'MMM') AS month_name_short,
  concat(year(calendar_date), '-', lpad(month(calendar_date), 2, '0')) AS year_month,
  concat(year(calendar_date), lpad(month(calendar_date), 2, '0')) AS year_month_key,
  weekofyear(calendar_date) AS week_of_year,
  concat(year(calendar_date), '-W', lpad(weekofyear(calendar_date), 2, '0')) AS year_week,
  day(calendar_date) AS day_of_month,
  dayofyear(calendar_date) AS day_of_year,
  weekday(calendar_date) + 1 AS iso_day_of_week,
  dayofweek(calendar_date) AS us_day_of_week,
  date_format(calendar_date, 'EEEE') AS day_name,
  date_format(calendar_date, 'E') AS day_name_short,
  CASE WHEN dayofweek(calendar_date) IN (1, 7) THEN true ELSE false END AS is_weekend,
  CASE WHEN calendar_date = trunc(calendar_date, 'month') THEN true ELSE false END AS is_month_start,
  CASE WHEN calendar_date = last_day(calendar_date) THEN true ELSE false END AS is_month_end,
  CASE WHEN calendar_date = trunc(calendar_date, 'quarter') THEN true ELSE false END AS is_quarter_start,
  CASE WHEN calendar_date = date_sub(add_months(trunc(calendar_date, 'quarter'), 3), 1) THEN true ELSE false END AS is_quarter_end,
  CASE WHEN calendar_date = trunc(calendar_date, 'year') THEN true ELSE false END AS is_year_start,
  CASE WHEN calendar_date = date_sub(add_months(trunc(calendar_date, 'year'), 12), 1) THEN true ELSE false END AS is_year_end,
  trunc(calendar_date, 'week') AS week_start_date,
  date_add(trunc(calendar_date, 'week'), 6) AS week_end_date,
  trunc(calendar_date, 'month') AS month_start_date,
  last_day(calendar_date) AS month_end_date,
  trunc(calendar_date, 'quarter') AS quarter_start_date,
  date_sub(add_months(trunc(calendar_date, 'quarter'), 3), 1) AS quarter_end_date,
  trunc(calendar_date, 'year') AS year_start_date,
  date_sub(add_months(trunc(calendar_date, 'year'), 12), 1) AS year_end_date,
  CASE WHEN (year(calendar_date) % 400 = 0) OR (year(calendar_date) % 4 = 0 AND year(calendar_date) % 100 <> 0) THEN true ELSE false END AS is_leap_year
FROM dates;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT
  COUNT(*) AS total_rows,
  MIN(date) AS min_date,
  MAX(date) AS max_date,
  MIN(date_key) AS min_date_key,
  MAX(date_key) AS max_date_key,
  COUNT(DISTINCT year) AS distinct_years,
  COUNT(DISTINCT year_month_key) AS distinct_months
FROM lh_caresync.silver.calendar;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT *
FROM lh_caresync.silver.calendar
ORDER BY date
LIMIT 5;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
