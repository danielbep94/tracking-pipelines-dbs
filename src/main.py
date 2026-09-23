# src/main.py
from pyspark.sql import SparkSession
from logs.logger import get_logger
from conf.credentials import get_snowflake_options
from conf.settings import SOURCE_CATALOG_SCHEMA
from src.extract import extract_source_data, extract_reference_areas
from src.transform import clean_and_transform
from src.load import load_to_delta, load_to_snowflake

logger = get_logger(__name__)

def run_pipeline(spark: SparkSession, year: int, month: int, dry_run: bool):
    logger.info(f"Starting pipeline for {year}-{month:02d} | Dry Run: {dry_run}")
    
    # 0. Fetch Options
    sf_options = get_snowflake_options(spark)
    
    # Dynamically build the table name
    source_table_name = f"{SOURCE_CATALOG_SCHEMA}.ceo_asistencia_{month:02d}_{year}"
    
    # 1. Extract
    df_raw = extract_source_data(spark, source_table_name)
    df_ref = extract_reference_areas(spark, sf_options)
    
    # 2. Transform
    df_transformed = clean_and_transform(df_raw, df_ref, year, month)
    
    # 3. Cache and Count (Optimizes the DAG for multiple downstream actions)
    df_transformed.cache()
    insert_count = df_transformed.count()
    logger.info(f"⚙️ Rows transformed and ready for target tables: {insert_count}")
    
    # 4. Load or Dry Run
    if dry_run:
        logger.info("DRY RUN ENABLED. Displaying transformed data and exiting.")
        df_transformed.display()
        df_transformed.unpersist()
        return
        
    logger.info("Writing to targets...")
    
    # Pass spark session and insert_count to loaders for rich logging
    load_to_delta(spark, df_transformed, year, month, insert_count)
    load_to_snowflake(spark, df_transformed, sf_options, year, month, insert_count)
    
    df_transformed.unpersist()
    logger.info("Pipeline completed successfully.")