"""CEO Asistencia extract, transform, period validation, and load orchestration."""

from pyspark.sql import functions as F

from conf import settings
from logs.logger import get_logger
from src.extract import (
    extract_reference_areas,
    extract_source_data,
    resolve_source_table,
)
from src.load import load_to_delta, load_to_snowflake
from src.transform import clean_and_transform


logger = get_logger("Main")


def run_pipeline(
    spark,
    year: int,
    month: int,
    snowflake_options,
    dry_run: bool = False,
):
    """Run one validated monthly attendance snapshot and return audit counts."""
    source_table = resolve_source_table(year, month)
    logger.info(
        "Starting CEO Asistencia pipeline | period=%s/%02d | dry_run=%s",
        year,
        month,
        dry_run,
    )

    raw_df = extract_source_data(spark, year, month)
    source_rows = int(raw_df.count())
    if source_rows == 0:
        raise ValueError("The source returned zero rows; replacement was cancelled.")

    reference_df = extract_reference_areas(spark, snowflake_options)
    transformed_df = clean_and_transform(raw_df, reference_df).cache()
    transformed_rows = int(transformed_df.count())
    if transformed_rows == 0:
        raise ValueError(
            "The transformation returned zero rows; replacement was cancelled."
        )

    invalid_period_rows = int(
        transformed_df.where(
            F.col("ANIO").isNull()
            | F.col("NUM_MES").isNull()
            | (F.col("ANIO") != F.lit(year))
            | (F.col("NUM_MES") != F.lit(month))
        ).count()
    )
    if invalid_period_rows:
        raise ValueError(
            f"Period validation failed: {invalid_period_rows:,} row(s) have an "
            f"invalid date or do not match filename period {year}-{month:02d}. "
            "Target writes were cancelled."
        )

    if transformed_rows != source_rows:
        raise ValueError(
            "Transformation row-count mismatch: "
            f"source={source_rows:,}, transformed={transformed_rows:,}."
        )

    delta_rows = None
    target_rows = None
    if dry_run:
        logger.info(
            "Dry run: would replace %s/%02d with %s rows in %s and %s",
            year,
            month,
            f"{transformed_rows:,}",
            settings.HIVE_FULL_TABLE,
            settings.SNOWFLAKE_FULL_TABLE,
        )
    else:
        delta_rows = load_to_delta(
            spark=spark,
            dataframe=transformed_df,
            target_year=year,
            target_month=month,
            expected_rows=transformed_rows,
        )
        target_rows = load_to_snowflake(
            spark=spark,
            dataframe=transformed_df,
            snowflake_options=snowflake_options,
            target_year=year,
            target_month=month,
            expected_rows=transformed_rows,
        )

    transformed_df.unpersist()
    logger.info("CEO Asistencia pipeline completed")
    return {
        "source_table": source_table,
        "source_rows": source_rows,
        "transformed_rows": transformed_rows,
        "delta_rows": delta_rows,
        "target_rows": target_rows,
        "target_table": settings.SNOWFLAKE_FULL_TABLE,
    }
