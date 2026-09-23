"""Monthly Delta extraction and AREA reference retrieval."""

from conf import settings
from logs.logger import get_logger


logger = get_logger("Extract")


def resolve_source_table(target_year: int, target_month: int) -> str:
    """Return the fully qualified monthly staging table."""
    if not isinstance(target_year, int) or not 1900 <= target_year <= 9999:
        raise ValueError("target_year must be a four-digit integer.")
    if not isinstance(target_month, int) or not 1 <= target_month <= 12:
        raise ValueError("target_month must be an integer from 1 to 12.")
    return (
        f"{settings.SRC_CATALOG}.{settings.SRC_SCHEMA}."
        f"{settings.SRC_TABLE_PREFIX}_{target_month:02d}_{target_year}"
    )


def extract_source_data(spark, target_year: int, target_month: int):
    """Read one requested monthly staging table."""
    table_name = resolve_source_table(target_year, target_month)
    logger.info("Reading Databricks staging table: %s", table_name)
    return spark.read.table(table_name)


def extract_reference_areas(spark, snowflake_options):
    """Read historical non-null AREA mappings used by the existing process."""
    logger.info("Reading historical AREA references from Snowflake")
    query = f"""
        SELECT NUMERO_EMPLEADO, ID, AREA
        FROM {settings.SNOWFLAKE_FULL_TABLE}
        WHERE AREA IS NOT NULL
    """
    return (
        spark.read.format(settings.SNOWFLAKE_SOURCE_NAME)
        .options(**snowflake_options)
        .option("query", query)
        .load()
    )
