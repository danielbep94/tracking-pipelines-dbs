# Databricks notebook source
"""One-time initialization for CEO Asistencia DBFS folders and Hive schema."""

SCHEMA_NAME = "RH_DANONE"
BASE_PATH = "dbfs:/FileStore/tables/rh_danone"
INCOMING_PATH = f"{BASE_PATH}/incoming"
CURRENT_PATH = f"{BASE_PATH}/current"

for folder in (INCOMING_PATH, CURRENT_PATH):
    dbutils.fs.mkdirs(folder)

spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}
    COMMENT 'RH Danone data domain and CEO Asistencia monthly staging'
    WITH DBPROPERTIES (
        ID='002',
        NAME='VICTOR_HERNANDEZ'
    )
    """
)

print("DBFS folders")
for folder in (INCOMING_PATH, CURRENT_PATH):
    files = dbutils.fs.ls(folder)
    print(f"OK | {folder} | files={len(files)}")

print("Hive schema")
spark.sql(f"SHOW DATABASES LIKE '{SCHEMA_NAME.lower()}'").show(truncate=False)
print("CEO Asistencia initialization completed")
