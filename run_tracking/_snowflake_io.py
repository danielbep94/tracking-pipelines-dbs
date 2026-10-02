"""Snowflake metrics reads and append-only tracking record writes."""

import logging
from typing import Any, Dict, Optional

from ._constants import logger
from ._utils import emit_tracking_log, normalize_optional_long, sanitize_error_message
from ._snowflake_session import build_query_tag_statement


# ---------------------------------------------------------------------------
# Snowflake read, write, and verification
# ---------------------------------------------------------------------------

def read_snowflake_metrics(
    spark,
    snowflake_options: Dict[str, str],
    query: str,
) -> Dict[str, Any]:
    """Execute a SELECT query against Snowflake and return the first row."""
    row = (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**snowflake_options)
        .option("query", query)
        .load()
        .first()
    )

    return row.asDict(recursive=True) if row is not None else {}


def append_run_record(
    spark,
    snowflake_options: Dict[str, str],
    table_name: str,
    record: Dict[str, Any],
) -> None:
    """
    Append exactly one tracking record to Snowflake.

    The write is tagged with QUERY_TAG (RUN_ID, PIPELINE_NAME,
    PIPELINE_VERSION) via the "preactions" option, so this write is visible
    and searchable in Snowflake QUERY_HISTORY.

    Exceptions are intentionally not suppressed; the caller decides whether
    a write failure should be raised or handled.
    """
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("RUN_ID", StringType(), False),
            StructField("PIPELINE_NAME", StringType(), False),
            StructField("PIPELINE_VERSION", StringType(), True),
            StructField("ENVIRONMENT", StringType(), False),
            StructField("STATUS", StringType(), False),
            StructField("DATABRICKS_JOB_ID", StringType(), True),
            StructField("DATABRICKS_JOB_RUN_ID", StringType(), True),
            StructField("DATABRICKS_TASK_RUN_ID", StringType(), True),
            StructField("DATABRICKS_TASK_NAME", StringType(), True),
            StructField("ATTEMPT_NUMBER", LongType(), True),
            StructField("TRIGGER_TYPE", StringType(), True),
            StructField("SOURCE_NAME", StringType(), True),
            StructField("SOURCE_FILE", StringType(), True),
            StructField("TARGET_NAME", StringType(), True),
            StructField("PERIOD_START_DATE", DateType(), True),
            StructField("PERIOD_END_DATE", DateType(), True),
            StructField("STARTED_AT_UTC", TimestampType(), False),
            StructField("COMPLETED_AT_UTC", TimestampType(), False),
            StructField("DURATION_SECONDS", DoubleType(), False),
            StructField("SOURCE_ROWS", LongType(), True),
            StructField("STAGING_ROWS", LongType(), True),
            StructField("TRANSFORMED_ROWS", LongType(), True),
            StructField("TARGET_ROWS_BEFORE", LongType(), True),
            StructField("TARGET_ROWS_AFTER", LongType(), True),
            StructField("TARGET_ROW_DELTA", LongType(), True),
            StructField("BUSINESS_METRICS_JSON", StringType(), True),
            StructField("ERROR_TYPE", StringType(), True),
            StructField("ERROR_MESSAGE", StringType(), True),
            StructField("CREATED_AT_UTC", TimestampType(), False),
        ]
    )

    dataframe = spark.createDataFrame([record], schema=schema)

    query_tag_statement = build_query_tag_statement(
        run_id=record.get("RUN_ID"),
        pipeline_name=record.get("PIPELINE_NAME"),
        pipeline_version=record.get("PIPELINE_VERSION"),
    )

    (
        dataframe.write.format("net.snowflake.spark.snowflake")
        .options(**snowflake_options)
        .option("dbtable", table_name)
        .option("preactions", query_tag_statement)
        .mode("append")
        .save()
    )


def verify_run_record(
    spark,
    snowflake_options: Dict[str, str],
    table_name: str,
    run_id: str,
) -> int:
    """
    Return the number of Snowflake rows found for RUN_ID.

    Expected result:
        1   -> record confirmed.
        0   -> record not found.
        >1  -> duplicate records found for the same RUN_ID.
    """
    if not run_id:
        raise ValueError(
            "RUN_ID is required to verify the tracking record."
        )

    if not table_name or not str(table_name).strip():
        raise ValueError(
            "table_name is required to verify the tracking record."
        )

    safe_run_id = str(run_id).replace("'", "''")

    verification_query = f"""
        SELECT
            COUNT(*) AS RECORD_COUNT
        FROM {table_name}
        WHERE RUN_ID = '{safe_run_id}'
    """

    verification_result = read_snowflake_metrics(
        spark=spark,
        snowflake_options=snowflake_options,
        query=verification_query,
    )

    raw_record_count = None

    for key, value in verification_result.items():
        if str(key).upper() == "RECORD_COUNT":
            raw_record_count = value
            break

    if raw_record_count is None:
        raise RuntimeError(
            "Snowflake verification did not return RECORD_COUNT."
        )

    record_count = normalize_optional_long(raw_record_count)

    if record_count is None:
        raise RuntimeError(
            "Snowflake verification returned an empty RECORD_COUNT."
        )

    return record_count


def append_run_record_safely(
    spark,
    snowflake_options: Dict[str, str],
    table_name: str,
    record: Dict[str, Any],
    log_obj: Optional[logging.Logger] = None,
    verify_write: bool = True,
) -> bool:
    """
    Convenience helper: append and optionally verify one record in a single
    call, catching and logging all errors instead of raising.

    This is a simple, all-in-one alternative to RunTracker for callers who
    want to append a single record directly without the retry-aware
    lifecycle logic used internally by RunTracker.
    """
    active_logger = log_obj or logger

    run_id = record.get("RUN_ID")
    status = record.get("STATUS")

    emit_tracking_log(
        level="INFO",
        event="TRACKING_APPEND_STARTED",
        log_obj=active_logger,
        run_id=run_id,
        status=status,
        table=table_name,
    )

    try:
        append_run_record(
            spark=spark,
            snowflake_options=snowflake_options,
            table_name=table_name,
            record=record,
        )

        emit_tracking_log(
            level="INFO",
            event="TRACKING_APPEND_SUCCEEDED",
            log_obj=active_logger,
            run_id=run_id,
            status=status,
            table=table_name,
        )

    except Exception as write_error:
        emit_tracking_log(
            level="ERROR",
            event="TRACKING_APPEND_FAILED",
            log_obj=active_logger,
            run_id=run_id,
            table=table_name,
            error_type=type(write_error).__name__,
            error_message=sanitize_error_message(write_error),
        )

        return False

    if not verify_write:
        emit_tracking_log(
            level="WARNING",
            event="TRACKING_WRITE_NOT_VERIFIED",
            log_obj=active_logger,
            run_id=run_id,
            table=table_name,
            reason="verification_disabled",
        )

        return True

    try:
        emit_tracking_log(
            level="INFO",
            event="TRACKING_VERIFICATION_STARTED",
            log_obj=active_logger,
            run_id=run_id,
            table=table_name,
        )

        record_count = verify_run_record(
            spark=spark,
            snowflake_options=snowflake_options,
            table_name=table_name,
            run_id=run_id,
        )

    except Exception as verification_error:
        emit_tracking_log(
            level="ERROR",
            event="TRACKING_VERIFICATION_FAILED",
            log_obj=active_logger,
            run_id=run_id,
            table=table_name,
            error_type=type(verification_error).__name__,
            error_message=sanitize_error_message(verification_error),
        )

        return False

    if record_count == 0:
        emit_tracking_log(
            level="ERROR",
            event="TRACKING_RECORD_NOT_FOUND",
            log_obj=active_logger,
            run_id=run_id,
            table=table_name,
            record_count=record_count,
        )

        return False

    if record_count > 1:
        emit_tracking_log(
            level="ERROR",
            event="TRACKING_DUPLICATE_RECORDS_FOUND",
            log_obj=active_logger,
            run_id=run_id,
            table=table_name,
            record_count=record_count,
        )

        return False

    emit_tracking_log(
        level="INFO",
        event="TRACKING_RECORD_CONFIRMED",
        log_obj=active_logger,
        run_id=run_id,
        status=status,
        table=table_name,
        record_count=record_count,
    )

    return True
