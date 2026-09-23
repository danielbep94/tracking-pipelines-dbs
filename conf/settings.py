"""Production configuration for the CEO Asistencia monthly ETL."""

# Databricks monthly staging source
SRC_CATALOG = "hive_metastore"
SRC_SCHEMA = "RH_DANONE"
SRC_TABLE_PREFIX = "ceo_asistencia"

# Curated Delta target
HIVE_CATALOG = "hive_metastore"
HIVE_SCHEMA = "default"
HIVE_TABLE = "fact_ceo_asistencia"
HIVE_FULL_TABLE = f"{HIVE_CATALOG}.{HIVE_SCHEMA}.{HIVE_TABLE}"

# Snowflake connector and target
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
SNOWFLAKE_URL = "danonenam.east-us-2.azure.snowflakecomputing.com"
SNOWFLAKE_DATABASE = "PRD_MDP"
SNOWFLAKE_SCHEMA = "MDP_STG"
SNOWFLAKE_TABLE = "FACT_CEO_ASISTENCIA"
SNOWFLAKE_FULL_TABLE = (
    f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}"
)
SNOWFLAKE_WAREHOUSE = "PRD_MDP_ANL_WH"
SNOWFLAKE_ROLE = "PRD_MDP"

# Databricks-backed Azure Key Vault secrets
SNOWFLAKE_SECRET_SCOPE = "DAN-AM-P-KVT800-R-MDP-DB"
SNOWFLAKE_SECRET_KEYS = {
    "sfUser": "snowflake-user",
    "pem_private_key": "Snowflake-Private-Key",
}

# Backward-compatible aliases for existing repository imports.
KEYVAULT_NAME = SNOWFLAKE_SECRET_SCOPE
KEY_NAME_USR = SNOWFLAKE_SECRET_KEYS["sfUser"]
SF_URL = SNOWFLAKE_URL
TARGET_DB = SNOWFLAKE_DATABASE
TARGET_SCHEMA = SNOWFLAKE_SCHEMA
SOURCE_CATALOG_SCHEMA = f"{SRC_CATALOG}.{SRC_SCHEMA}"
TARGET_SNOWFLAKE_TABLE = SNOWFLAKE_FULL_TABLE
TARGET_HIVE_TABLE = HIVE_FULL_TABLE
REFERENCE_AREA_TABLE = SNOWFLAKE_FULL_TABLE

# Shared operational telemetry
PIPELINE_NAME = "CEO_ASISTENCIA"
PIPELINE_ENVIRONMENT = "PROD"
PIPELINE_SOURCE_NAME = "DBFS_MONTHLY_CSV"
PIPELINE_RUN_TRACKING_ENABLED = True
PIPELINE_RUN_HISTORY_TABLE = "PIPELINE_RUNS"

# Code-derived target contract. Live target metadata remains authoritative for
# physical Snowflake types and nullability.
CEO_ASISTENCIA_TARGET_SCHEMA = {
    "ID": "STRING",
    "EMPRESA": "SOURCE_INFERRED",
    "NUMERO_EMPLEADO": "STRING",
    "PATERNO": "SOURCE_INFERRED",
    "MATERNO": "SOURCE_INFERRED",
    "NOMBRE": "SOURCE_INFERRED",
    "AREA": "STRING",
    "FECHA": "DATE",
    "REGISTRO_1": "SOURCE_INFERRED",
    "REGISTRO_2": "SOURCE_INFERRED",
    "ANIO": "INTEGER",
    "NUM_MES": "INTEGER",
    "WEEK_NUMBER": "INTEGER",
    "DAY": "STRING",
    "RESULTADO": "INTEGER",
    "DIFERENCIA_HORAS": "DOUBLE",
}

FINAL_COLUMNS = list(CEO_ASISTENCIA_TARGET_SCHEMA.keys())
