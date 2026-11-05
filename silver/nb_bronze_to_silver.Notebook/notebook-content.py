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

# Welcome to your new notebook
# Type here in the cell editor to add code!
landing_timestamp="2026-10-31_11-11-27"
table_name="doctors"
merge_keys=""
load_type="FULL"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("[STEP 1] Reading source bronze table")
# source_query = f"SELECT * FROM lh_caresync.bronze.{table_name} WHERE landing_timestamp = '{landing_timestamp}'"
# print(f"[SOURCE QUERY]\n{source_query}")

bronze_df = spark.read.table(f"lh_caresync.bronze.{table_name}").filter(
    f"landing_timestamp='{landing_timestamp}'"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

print("[STEP 2] Preparing load metadata")
target_table = f"lh_caresync.silver.{table_name}"
normalized_load_type = (load_type or "FULL").strip().upper()
merge_key_list = [key.strip() for key in merge_keys.split(",") if key.strip()]

print(f"[TARGET TABLE] {target_table}")
print(f"[RAW LOAD TYPE] '{load_type}'")
print(f"[NORMALIZED LOAD TYPE] {normalized_load_type}")
print(f"[MERGE KEYS] {merge_key_list}")


updated_df = (
    bronze_df.withColumn("insert_timestamp", F.current_timestamp())
    .withColumn("update_timestamp", F.current_timestamp())
)

print(f"[UPDATED COLUMNS] {updated_df.columns}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"[STEP 3] Starting write to target table: {target_table}")

if normalized_load_type == "FULL":
    print("[WRITE MODE] FULL")
    updated_df.write.mode("overwrite").saveAsTable(target_table)
    print("[STEP 3 COMPLETE] FULL overwrite finished successfully")
elif normalized_load_type == "APPEND":
    print("[WRITE MODE] APPEND")
    updated_df.write.mode("append").saveAsTable(target_table)
    print("[STEP 3 COMPLETE] APPEND finished successfully")
elif normalized_load_type == "MERGE":
    print("[WRITE MODE] MERGE")
    target_exists = spark.catalog.tableExists(target_table)
    print(f"[TARGET EXISTS] {target_exists}")

    if not target_exists:
        print("[MERGE FALLBACK] Target table does not exist. Creating target table with current dataset.")
        updated_df.write.mode("overwrite").saveAsTable(target_table)
        print("[STEP 3 COMPLETE] Target table created for initial MERGE load")
    else:
        source_view = "silver_merge_source"
        updated_df.createOrReplaceTempView(source_view)
        print(f"[TEMP VIEW] Created temp view '{source_view}' for merge source")

        merge_condition = " AND ".join(
            [f"target.`{key}` = source.`{key}`" for key in merge_key_list]
        )
        update_assignments = ", ".join(
            [
                f"target.`{column}` = source.`{column}`"
                for column in updated_df.columns
                if column != "insert_timestamp"
            ]
        )
        insert_columns = ", ".join([f"`{column}`" for column in updated_df.columns])
        insert_values = ", ".join([f"source.`{column}`" for column in updated_df.columns])

        merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING {source_view} AS source
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET {update_assignments}
            WHEN NOT MATCHED THEN INSERT ({insert_columns})
            VALUES ({insert_values})
        """.strip()

        print("[MERGE SQL]")
        print(merge_sql)

        spark.sql(merge_sql)
        print("[STEP 3 COMPLETE] MERGE finished successfully")
else:
    raise ValueError("load_type must be one of: FULL, APPEND, MERGE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("[STEP 4] Computing processed record count")
record_count = updated_df.count()
print(f"[STEP 4 COMPLETE] Processed record count: {record_count}")
notebookutils.notebook.exit(str(record_count))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
