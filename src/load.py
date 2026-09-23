"""Period-idempotent Delta and Snowflake loads for CEO Asistencia."""

from typing import Dict

from conf import settings
from logs.logger import get_logger


logger = get_logger("Load")


def _validate_period(target_year: int, target_month: int) -> None:
    if not isinstance(target_year, int) or not 1900 <= target_year <= 9999:
        raise ValueError("target_year must be a four-digit integer.")
    if not isinstance(target_month, int) or not 1 <= target_month <= 12:
        raise ValueError("target_month must be an integer from 1 to 12.")


def count_delta_period(spark, target_year: int, target_month: int) -> int:
    """Count one period in the curated Delta target."""
    _validate_period(target_year, target_month)
    return int(
        spark.table(settings.HIVE_FULL_TABLE)
        .where(
            f"ANIO = {target_year} AND NUM_MES = {target_month}"
        )
        .count()
    )


def count_snowflake_period(
    spark,
    snowflake_options: Dict[str, str],
    target_year: int,
    target_month: int,
) -> int:
    """Count one period in the permanent Snowflake target."""
    _validate_period(target_year, target_month)
    query = f"""
        SELECT COUNT(*) AS TARGET_ROWS
        FROM {settings.SNOWFLAKE_FULL_TABLE}
        WHERE TRY_TO_NUMBER(ANIO) = {target_year}
          AND TRY_TO_NUMBER(NUM_MES) = {target_month}
    """
    row = (
        spark.read.format(settings.SNOWFLAKE_SOURCE_NAME)
        .options(**snowflake_options)
        .option("query", query)
        .load()
        .first()
    )
    if row is None:
        raise RuntimeError("Snowflake period row-count query returned no row.")
    return int(row[0])


def load_to_delta(
    spark,
    dataframe,
    target_year: int,
    target_month: int,
    expected_rows: int,
) -> int:
    """Replace one curated Delta period and validate its final count."""
    _validate_period(target_year, target_month)
    if int(expected_rows) <= 0:
        raise ValueError("Cannot load an empty CEO Asistencia DataFrame.")

    replace_condition = (
        f"ANIO = {target_year} AND NUM_MES = {target_month}"
    )
    logger.info(
        "Replacing Delta period | table=%s | period=%s/%02d | rows=%s",
        settings.HIVE_FULL_TABLE,
        target_year,
        target_month,
        f"{expected_rows:,}",
    )
    (
        dataframe.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_condition)
        .saveAsTable(settings.HIVE_FULL_TABLE)
    )
    actual_rows = count_delta_period(spark, target_year, target_month)
    if actual_rows != expected_rows:
        raise RuntimeError(
            "Delta post-load validation failed. "
            f"Expected {expected_rows:,} rows for "
            f"{target_year}/{target_month:02d}, but found {actual_rows:,}."
        )
    logger.info("Delta load validated | rows=%s", f"{actual_rows:,}")
    return actual_rows


def load_to_snowflake(
    spark,
    dataframe,
    snowflake_options: Dict[str, str],
    target_year: int,
    target_month: int,
    expected_rows: int,
) -> int:
    """Replace one Snowflake period and validate its final count."""
    _validate_period(target_year, target_month)
    if int(expected_rows) <= 0:
        raise ValueError("Cannot load an empty CEO Asistencia DataFrame.")

    delete_statement = (
        f"DELETE FROM {settings.SNOWFLAKE_FULL_TABLE} "
        f"WHERE TRY_TO_NUMBER(ANIO) = {target_year} "
        f"AND TRY_TO_NUMBER(NUM_MES) = {target_month}"
    )
    connector_options = dict(snowflake_options)
    connector_options.update(
        {
            "column_mapping": "name",
            "column_mismatch_behavior": "error",
            "continue_on_error": "off",
            "truncate_columns": "off",
        }
    )
    logger.info(
        "Replacing Snowflake period | table=%s | period=%s/%02d | rows=%s",
        settings.SNOWFLAKE_FULL_TABLE,
        target_year,
        target_month,
        f"{expected_rows:,}",
    )
    (
        dataframe.write.format(settings.SNOWFLAKE_SOURCE_NAME)
        .options(**connector_options)
        .option("dbtable", settings.SNOWFLAKE_TABLE)
        .option("preactions", delete_statement)
        .mode("append")
        .save()
    )
    actual_rows = count_snowflake_period(
        spark=spark,
        snowflake_options=snowflake_options,
        target_year=target_year,
        target_month=target_month,
    )
    if actual_rows != expected_rows:
        raise RuntimeError(
            "Snowflake post-load validation failed. "
            f"Expected {expected_rows:,} rows for "
            f"{target_year}/{target_month:02d}, but found {actual_rows:,}."
        )
    logger.info("Snowflake load validated | rows=%s", f"{actual_rows:,}")
    return actual_rows
