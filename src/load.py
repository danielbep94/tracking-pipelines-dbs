# src/load.py
from pyspark.sql import DataFrame, SparkSession
from logs.logger import get_logger
from conf.settings import TARGET_HIVE_TABLE, TARGET_SNOWFLAKE_TABLE

logger = get_logger(__name__)

def load_to_delta(spark: SparkSession, df: DataFrame, year: int, month: int, insert_count: int):
    logger.info(f"Loading to Delta Lake: {TARGET_HIVE_TABLE} for {year}-{month:02d}")
    replace_condition = f"ANIO = {year} AND NUM_MES = {month}"
    
    # Write using replaceWhere
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .option("replaceWhere", replace_condition) \
      .saveAsTable(TARGET_HIVE_TABLE)
      
    logger.info(f"✅ Inserted/Replaced {insert_count} rows in Delta table.")
    
    # Fetch final table count
    try:
        total_count = spark.read.table(TARGET_HIVE_TABLE).count()
        logger.info(f"📊 Total rows in Delta {TARGET_HIVE_TABLE} after load: {total_count}")
    except Exception as e:
        logger.warning(f"Could not retrieve total count for Delta table: {e}")

def load_to_snowflake(spark: SparkSession, df: DataFrame, sf_options: dict, year: int, month: int, insert_count: int):
    logger.info(f"Loading to Snowflake: {TARGET_SNOWFLAKE_TABLE} for {year}-{month:02d}")
    
    # 1. Fetch count of rows to be deleted
    try:
        query_before = f"SELECT COUNT(*) as CNT FROM {TARGET_SNOWFLAKE_TABLE} WHERE ANIO = {year} AND NUM_MES = {month}"
        rows_to_delete = spark.read.format("snowflake").options(**sf_options).option("query", query_before).load().collect()[0]["CNT"]
        logger.info(f"🧹 Rows to be deleted in Snowflake (target period): {rows_to_delete}")
    except Exception as e:
        logger.warning(f"Could not fetch rows to delete: {e}")

    # 2. Perform write with preactions
    delete_statement = f"DELETE FROM {TARGET_SNOWFLAKE_TABLE} WHERE ANIO = {year} AND NUM_MES = {month}"
    df.write \
      .format("snowflake") \
      .options(**sf_options) \
      .option("dbtable", TARGET_SNOWFLAKE_TABLE) \
      .option("preactions", delete_statement) \
      .mode("append") \
      .save()
      
    logger.info(f"✅ Inserted {insert_count} rows into Snowflake.")

    # 3. Fetch final total count
    try:
        query_after = f"SELECT COUNT(*) as CNT FROM {TARGET_SNOWFLAKE_TABLE}"
        total_rows = spark.read.format("snowflake").options(**sf_options).option("query", query_after).load().collect()[0]["CNT"]
        logger.info(f"📊 Total rows in Snowflake {TARGET_SNOWFLAKE_TABLE} after load: {total_rows}")
    except Exception as e:
        logger.warning(f"Could not fetch final Snowflake total count: {e}")