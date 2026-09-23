# Databricks notebook source
# Databricks notebook source

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------

SCHEMA_NAME = "RH_DANONE"

BASE_PATH = f"dbfs:/FileStore/tables/{SCHEMA_NAME.lower()}"

INCOMING_PATH = f"{BASE_PATH}/incoming"
CURRENT_PATH  = f"{BASE_PATH}/current"

# ------------------------------------------------------------------------------
# Create DBFS folder structure
# ------------------------------------------------------------------------------

for folder in [INCOMING_PATH, CURRENT_PATH]:
    dbutils.fs.mkdirs(folder)

print("✓ DBFS folder structure created")

# ------------------------------------------------------------------------------
# Create schema if it does not exist
# ------------------------------------------------------------------------------

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}
COMMENT 'RH Danone data domain'
WITH DBPROPERTIES (
    ID='002',
    NAME='VICTOR_HERNANDEZ'
)
""")

print(f"✓ Schema {SCHEMA_NAME} created or already exists")

# ------------------------------------------------------------------------------
# Validate folders
# ------------------------------------------------------------------------------

print("\n=== FOLDERS ===")

for folder in [INCOMING_PATH, CURRENT_PATH]:
    try:
        files = dbutils.fs.ls(folder)
        print(f"✓ {folder} ({len(files)} files)")
    except Exception as e:
        print(f"✗ {folder}: {e}")

# ------------------------------------------------------------------------------
# Validate schema
# ------------------------------------------------------------------------------

print("\n=== SCHEMA ===")

spark.sql(
    f"SHOW DATABASES LIKE '{SCHEMA_NAME.lower()}'"
).show(truncate=False)


print("\nArchitecture initialization")

# COMMAND ----------

