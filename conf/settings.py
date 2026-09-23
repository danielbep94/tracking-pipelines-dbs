# conf/settings.py

# Azure Key Vault / Secret Scope configs
KEYVAULT_NAME = "DAN-AM-P-KVT800-R-MDP-DB" # Update if your secret scope has a different name
KEY_NAME_USR = "snowflake-user"
KEY_NAME_PWD = "snowflake-password"

# Snowflake connection configs
SF_URL = "danonenam.east-us-2.azure.snowflakecomputing.com"
TARGET_DB = "PRD_MDP"
TARGET_SCHEMA = "MDP_STG"

# Source definitions based on Databricks UI file upload
SOURCE_CATALOG_SCHEMA = "hive_metastore.rh_danone"

# Target definitions
TARGET_SNOWFLAKE_TABLE = f"{TARGET_DB}.{TARGET_SCHEMA}.FACT_CEO_ASISTENCIA"
TARGET_HIVE_TABLE = "hive_metastore.default.fact_ceo_asistencia" 
REFERENCE_AREA_TABLE = f"{TARGET_DB}.{TARGET_SCHEMA}.FACT_CEO_ASISTENCIA"

# Expected schema order
FINAL_COLUMNS = [
    "ID", "EMPRESA", "NUMERO_EMPLEADO", "PATERNO", "MATERNO", 
    "NOMBRE", "AREA", "FECHA", "REGISTRO_1", "REGISTRO_2", 
    "ANIO", "NUM_MES", "WEEK_NUMBER", "DAY", "RESULTADO", "DIFERENCIA_HORAS"
]