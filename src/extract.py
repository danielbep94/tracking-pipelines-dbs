# src/extract.py
from pyspark.sql import SparkSession
from logs.logger import get_logger
from conf.settings import REFERENCE_AREA_TABLE

logger = get_logger(__name__)

def extract_source_data(spark: SparkSession, table_name: str):
    logger.info(f"Extracting source data from Databricks table: {table_name}")
    return spark.read.table(table_name)

def extract_reference_areas(spark: SparkSession, sf_options: dict):
    logger.info("Extracting AREA references from Snowflake")
    query = f"SELECT NUMERO_EMPLEADO, ID, AREA FROM {REFERENCE_AREA_TABLE} WHERE AREA IS NOT NULL"
    
    return spark.read \
        .format("snowflake") \
        .options(**sf_options) \
        .option("query", query) \
        .load()