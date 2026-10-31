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

# PARAMETERS CELL ********************

source_system="Azure SQL"
table_name="doctors"
landing_timestamp="2026-10-31_11-11-27"
source_file_format="parquet"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

base_landing_path = "Files/landing"
source_path = f"{base_landing_path}/{source_system}"
table_path = f"{source_path}/{table_name}"
landing_path = f"{table_path}/{landing_timestamp}/"

# display(landing_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read landing data based on the configured source_file_format and landing_path
# Note: landing_path is already constructed in the previous cell as a Lakehouse Files-relative path

if source_file_format == "csv":
    # Read CSV files from the landing folder (header + schema inference)
    landing_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(landing_path)
    )
else:
    # Read Parquet files from the same landing folder
    # Use the computed landing_path instead of a hard-coded, invalid path
    landing_df = spark.read.format("parquet").load(landing_path)

# Preview the loaded data
# display(landing_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

updated_df = landing_df.withColumn("landing_timestamp", F.lit(landing_timestamp)) \
    .withColumn("insert_timestamp", F.current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

updated_df.write.mode("append").saveAsTable(f"lh_caresync.bronze.{table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
