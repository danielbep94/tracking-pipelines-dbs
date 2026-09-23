# conf/credentials.py
from pyspark.sql import SparkSession
from conf import settings

def get_dbutils(spark: SparkSession):
    """Safely retrieves dbutils inside modular Python scripts on Databricks."""
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except ImportError:
        import IPython
        return IPython.get_ipython().user_ns.get("dbutils")

def get_snowflake_options(spark: SparkSession) -> dict:
    """Retrieves Snowflake credentials from Azure Key Vault via dbutils."""
    dbutils = get_dbutils(spark)
    
    user = dbutils.secrets.get(scope=settings.KEYVAULT_NAME, key=settings.KEY_NAME_USR)
    password = dbutils.secrets.get(scope=settings.KEYVAULT_NAME, key=settings.KEY_NAME_PWD)
    
    return {
        "sfURL": settings.SF_URL,
        "sfUser": user,
        "sfPassword": password,
        "sfDatabase": settings.TARGET_DB,
        "sfSchema": settings.TARGET_SCHEMA,
        "sfWarehouse": "PRD_MDP_ANL_WH"
    }