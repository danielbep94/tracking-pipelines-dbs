# Databricks notebook source
# MAGIC %md
# MAGIC # CEO Asistencia ETL — PRODUCTION
# MAGIC
# MAGIC Processes one complete monthly `CEO_ASISTENCIA_<MM>_<YYYY>.csv`
# MAGIC snapshot. The filename supplies the business period; Databricks task
# MAGIC parameters supply operational run metadata.

# COMMAND ----------

import calendar
from datetime import date
import logging
import os
import re
import sys
import unicodedata

repo_root = os.path.abspath(".")
if repo_root not in sys.path:
    sys.path.append(repo_root)

from conf import settings
from conf.credentials import get_snowflake_options
from src.main import run_pipeline
from src.run_tracking import (
    RunContext,
    append_run_record,
    append_run_record_safely,
    get_widget_value,
    parse_optional_int,
    parse_utc_datetime,
    read_snowflake_metrics,
    sanitize_error_message,
    utc_now,
)

try:
    from logs.logger import get_logger

    logger = get_logger("Notebook_Main")
except Exception:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger("Notebook_Main")

logging.getLogger("py4j").setLevel(logging.WARNING)
logging.getLogger("py4j.clientserver").setLevel(logging.WARNING)
logging.getLogger("py4j.java_gateway").setLevel(logging.WARNING)


def step(number, message):
    logger.info("[%s/5] %s", number, message)


def _period_dates(year, month):
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last_day)


def _read_period_metrics(spark, snowflake_options, year, month):
    """Return safe reconciliation metrics for one attendance period."""
    query = f"""
        SELECT
            COUNT(*) AS TARGET_ROWS,
            COUNT(NUMERO_EMPLEADO) AS NON_NULL_EMPLOYEE_COUNT,
            COUNT(DISTINCT NUMERO_EMPLEADO) AS DISTINCT_EMPLOYEE_COUNT,
            COUNT_IF(NUMERO_EMPLEADO IS NULL) AS NULL_EMPLOYEE_COUNT,
            COUNT(DISTINCT ID) AS DISTINCT_ID_COUNT,
            COUNT(DISTINCT AREA) AS DISTINCT_AREA_COUNT,
            COUNT_IF(TRY_TO_NUMBER(RESULTADO) = 1) AS VALID_ATTENDANCE_COUNT
        FROM {settings.SNOWFLAKE_FULL_TABLE}
        WHERE TRY_TO_NUMBER(ANIO) = {int(year)}
          AND TRY_TO_NUMBER(NUM_MES) = {int(month)}
    """
    return read_snowflake_metrics(
        spark=spark,
        snowflake_options=snowflake_options,
        query=query,
    )


def _read_period_metrics_safely(spark, snowflake_options, year, month):
    """Collect metrics without changing the business-load result."""
    try:
        return _read_period_metrics(
            spark,
            snowflake_options,
            year,
            month,
        )
    except Exception as metrics_error:
        logger.warning(
            "TRACKING_METRICS_READ_FAILED | %s: %s",
            type(metrics_error).__name__,
            sanitize_error_message(metrics_error),
        )
        return {}


# COMMAND ----------
# BUSINESS INPUT AND TRACKING CONFIGURATION

BASE_PATH = "dbfs:/FileStore/tables/rh_danone"
INCOMING_PATH = f"{BASE_PATH}/incoming/"
CURRENT_PATH = f"{BASE_PATH}/current/"
HIVE_CATALOG = settings.SRC_CATALOG
HIVE_DATABASE = settings.SRC_SCHEMA
TABLE_PREFIX = settings.SRC_TABLE_PREFIX

FILENAME_REGEX = re.compile(
    r"^CEO_ASISTENCIA_(0[1-9]|1[0-2])_(\d{4})\.csv$"
)


class PipelineError(Exception):
    """Controlled production-pipeline failure."""


TRACKING_ENABLED = getattr(settings, "PIPELINE_RUN_TRACKING_ENABLED", False)
TRACKING_TABLE = getattr(settings, "PIPELINE_RUN_HISTORY_TABLE", "PIPELINE_RUNS")

tracking_started_at = (
    parse_utc_datetime(get_widget_value(dbutils, "tracking_job_started_at_utc"))
    or utc_now()
)
run_context = RunContext(
    pipeline_name=getattr(settings, "PIPELINE_NAME", "CEO_ASISTENCIA"),
    environment=getattr(settings, "PIPELINE_ENVIRONMENT", "PROD"),
    pipeline_version=get_widget_value(
        dbutils,
        "tracking_pipeline_version",
        "workspace-unversioned",
    ),
    databricks_job_id=get_widget_value(dbutils, "tracking_job_id"),
    databricks_job_run_id=get_widget_value(dbutils, "tracking_job_run_id"),
    databricks_task_run_id=get_widget_value(dbutils, "tracking_task_run_id"),
    databricks_task_name=get_widget_value(
        dbutils,
        "tracking_task_name",
        "CEO_ASISTENCIA",
    ),
    attempt_number=parse_optional_int(
        get_widget_value(dbutils, "tracking_attempt_number")
    ),
    trigger_type=get_widget_value(
        dbutils,
        "tracking_trigger_type",
        "one_time",
    ),
    started_at_utc=tracking_started_at,
)


# COMMAND ----------
# FILE DISCOVERY, STAGING, PROMOTION AND EXISTING FAILURE CLEANUP


def normalize_column_name(name):
    decomposed = unicodedata.normalize("NFKD", str(name))
    ascii_only = decomposed.encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", ascii_only)
    return re.sub(r"_+", "_", cleaned).strip("_").upper()


def get_incoming_file():
    """Return the only CSV in incoming/, or None when no CSV is present."""
    for path in (BASE_PATH, INCOMING_PATH, CURRENT_PATH):
        dbutils.fs.mkdirs(path)

    incoming_items = [
        item
        for item in dbutils.fs.ls(INCOMING_PATH)
        if not item.name.endswith("/")
    ]
    csv_files = [
        item for item in incoming_items if item.name.lower().endswith(".csv")
    ]
    other_files = [
        item.name
        for item in incoming_items
        if not item.name.lower().endswith(".csv")
    ]
    if other_files:
        logger.warning(
            "        Non-CSV file(s) present in incoming/: %s",
            ", ".join(sorted(other_files)),
        )
    if len(csv_files) == 0:
        return None
    if len(csv_files) > 1:
        names = ", ".join(sorted(item.name for item in csv_files))
        raise PipelineError(
            f"Expected exactly 1 CSV in incoming, found {len(csv_files)}: {names}"
        )
    return csv_files[0].name, csv_files[0].path


def get_period_from_filename(file_name):
    """Use the canonical filename as the period source of truth."""
    match = FILENAME_REGEX.match(file_name)
    if not match:
        raise PipelineError(
            f"Invalid filename '{file_name}'. Required: "
            "CEO_ASISTENCIA_<MM>_<YYYY>.csv "
            "(uppercase prefix, two-digit month, four-digit year, "
            "no copy suffixes)."
        )
    month_str, year_str = match.group(1), match.group(2)
    return int(year_str), int(month_str), month_str, year_str


def stage_csv_to_hive(file_path, month_str, year_str):
    """Overwrite the complete monthly Delta staging table and reconcile rows."""
    staging_table = (
        f"{HIVE_CATALOG}.{HIVE_DATABASE}."
        f"{TABLE_PREFIX}_{month_str}_{year_str}"
    )
    dataframe = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("encoding", "UTF-8")
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(file_path)
    )

    original_columns = dataframe.columns
    normalized_columns = [
        normalize_column_name(name) for name in original_columns
    ]
    duplicates = sorted(
        {
            name
            for name in normalized_columns
            if normalized_columns.count(name) > 1
        }
    )
    if duplicates:
        raise PipelineError(
            f"Column normalization produced duplicates: {duplicates}."
        )
    dataframe = dataframe.toDF(*normalized_columns)

    source_rows = int(dataframe.count())
    if source_rows == 0:
        raise PipelineError("The CSV has zero data rows.")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_CATALOG}.{HIVE_DATABASE}")
    (
        dataframe.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(staging_table)
    )
    staged_rows = int(spark.table(staging_table).count())
    if staged_rows != source_rows:
        raise PipelineError(
            f"Staging mismatch: CSV={source_rows}, table={staged_rows}"
        )

    renamed = [
        f"{old} -> {new}"
        for old, new in zip(original_columns, normalized_columns)
        if old != new
    ]
    logger.info("        Table: %s", staging_table)
    logger.info(
        "        Rows: %s CSV = %s staged  [OK]",
        source_rows,
        staged_rows,
    )
    logger.info(
        "        Columns: %s | renamed: %s",
        len(original_columns),
        len(renamed),
    )
    if renamed:
        logger.info("        %s", " | ".join(renamed))
    return staging_table, source_rows, staged_rows, len(original_columns)


def promote_to_current(file_name, source_path):
    """Move the accepted file to current/, replacing the same canonical file."""
    target_path = f"{CURRENT_PATH}{file_name}"
    if any(item.name == file_name for item in dbutils.fs.ls(CURRENT_PATH)):
        dbutils.fs.rm(target_path, recurse=False)
    dbutils.fs.mv(source_path, target_path)
    logger.info("        %s -> current/  [promoted]", file_name)


def clear_incoming_after_failure():
    """Preserve the existing deletion policy after a selected CSV fails."""
    removed_count = 0
    failed_items = []
    try:
        dbutils.fs.mkdirs(INCOMING_PATH)
        incoming_items = dbutils.fs.ls(INCOMING_PATH)
    except Exception as cleanup_error:
        logger.error(
            " Cleanup could not access %s: %s: %s",
            INCOMING_PATH,
            type(cleanup_error).__name__,
            sanitize_error_message(cleanup_error),
        )
        logger.error(
            " MANUAL ACTION REQUIRED: clear files from %s before the next upload.",
            INCOMING_PATH,
        )
        return

    files = [item for item in incoming_items if not item.name.endswith("/")]
    if not files:
        logger.error(" Cleanup: incoming/ is already empty; nothing to remove.")
        logger.error(" Cleanup total: 0 file(s) removed.")
        return

    for item in files:
        try:
            deleted = dbutils.fs.rm(item.path, recurse=False)
            if deleted is False:
                failed_items.append(item.name)
                logger.error(" Cleanup could not remove: %s", item.name)
                continue
            removed_count += 1
            logger.error(" Cleanup removed: %s", item.name)
        except Exception as cleanup_error:
            failed_items.append(item.name)
            logger.error(
                " Cleanup could not remove %s: %s: %s",
                item.name,
                type(cleanup_error).__name__,
                sanitize_error_message(cleanup_error),
            )

    logger.error(" Cleanup total: %s file(s) removed.", removed_count)
    if failed_items:
        logger.error(
            " MANUAL ACTION REQUIRED: clear files from %s before the next upload.",
            INCOMING_PATH,
        )


# COMMAND ----------
# ETL EXECUTION

FILE_NAME = None
TARGET_YEAR = None
TARGET_MONTH = None
source_rows = None
staged_rows = None
source_column_count = None
result = {}
snowflake_options = None
target_before_metrics = {}
target_after_metrics = {}
no_files_to_process = False
exit_message = "NO FILES TO PROCESS"

try:
    logger.info("=" * 64)
    logger.info(" CEO ASISTENCIA — PRODUCTION RUN")
    logger.info("=" * 64)

    step(1, "Discovery — looking for one CSV in incoming/")
    incoming = get_incoming_file()

    if incoming is None:
        logger.info("        NO FILES TO PROCESS — incoming/ holds no CSV.")
        exit_message = "NO FILES TO PROCESS | tracking=DISABLED"

        if TRACKING_ENABLED:
            snowflake_options = get_snowflake_options(dbutils)
            skipped_record = run_context.build_record(
                status="SKIPPED",
                source_name=settings.PIPELINE_SOURCE_NAME,
                target_name=settings.SNOWFLAKE_FULL_TABLE,
                business_metrics={"reason": "incoming_folder_has_no_csv"},
            )
            append_run_record(
                spark=spark,
                snowflake_options=snowflake_options,
                table_name=TRACKING_TABLE,
                record=skipped_record,
            )
            exit_message = (
                "NO FILES TO PROCESS | SKIPPED RUN RECORDED"
                f" | run_id={skipped_record['RUN_ID']}"
                f" | table={TRACKING_TABLE}"
            )
            logger.info(exit_message)

        logger.info("=" * 64)
        no_files_to_process = True

    else:
        FILE_NAME, FILE_PATH = incoming
        logger.info("        File: %s", FILE_NAME)

        step(2, "Validating filename and period")
        TARGET_YEAR, TARGET_MONTH, MONTH_STR, YEAR_STR = get_period_from_filename(
            FILE_NAME
        )
        logger.info("        Period: %s/%s  [OK]", MONTH_STR, YEAR_STR)
        logger.info("        Target: %s", settings.SNOWFLAKE_FULL_TABLE)

        snowflake_options = get_snowflake_options(dbutils)
        if TRACKING_ENABLED:
            target_before_metrics = _read_period_metrics_safely(
                spark,
                snowflake_options,
                TARGET_YEAR,
                TARGET_MONTH,
            )

        step(3, "Staging CSV into Hive")
        (
            staging_table,
            source_rows,
            staged_rows,
            source_column_count,
        ) = stage_csv_to_hive(FILE_PATH, MONTH_STR, YEAR_STR)

        step(4, "Transforming, validating and replacing the target period")
        result = run_pipeline(
            spark=spark,
            year=TARGET_YEAR,
            month=TARGET_MONTH,
            snowflake_options=snowflake_options,
            dry_run=False,
        )
        logger.info("        Pipeline completed  [OK]")
        for key, value in result.items():
            logger.info("        %s: %s", key, value)

        if TRACKING_ENABLED:
            target_after_metrics = _read_period_metrics_safely(
                spark,
                snowflake_options,
                TARGET_YEAR,
                TARGET_MONTH,
            )
            target_after_metrics.setdefault(
                "TARGET_ROWS",
                result.get("target_rows"),
            )

        step(5, "Promoting file to current/")
        promote_to_current(FILE_NAME, FILE_PATH)

        if TRACKING_ENABLED:
            period_start, period_end = _period_dates(TARGET_YEAR, TARGET_MONTH)
            success_record = run_context.build_record(
                status="SUCCEEDED",
                source_name=settings.PIPELINE_SOURCE_NAME,
                source_file=FILE_NAME,
                target_name=settings.SNOWFLAKE_FULL_TABLE,
                period_start_date=period_start,
                period_end_date=period_end,
                source_rows=source_rows,
                staging_rows=staged_rows,
                transformed_rows=result.get("transformed_rows"),
                target_rows_before=target_before_metrics.get("TARGET_ROWS"),
                target_rows_after=target_after_metrics.get("TARGET_ROWS"),
                business_metrics={
                    "before": target_before_metrics,
                    "after": target_after_metrics,
                    "source_columns": source_column_count,
                    "delta_rows_after": result.get("delta_rows"),
                },
            )
            append_run_record_safely(
                spark=spark,
                snowflake_options=snowflake_options,
                table_name=TRACKING_TABLE,
                record=success_record,
                logger=logger,
            )

        logger.info("-" * 64)
        logger.info(
            " SUCCESS | %s | period %s/%s | %s transformed rows",
            FILE_NAME,
            MONTH_STR,
            YEAR_STR,
            f"{result.get('transformed_rows', 0):,}",
        )
        logger.info("=" * 64)

except Exception as pipeline_error:
    logger.error("-" * 64)
    logger.error(
        " FAILED | %s: %s",
        type(pipeline_error).__name__,
        sanitize_error_message(pipeline_error),
    )

    # A no-input telemetry failure must never delete source files. The existing
    # business-failure cleanup applies only after a CSV has been selected.
    if FILE_NAME is not None:
        clear_incoming_after_failure()
        logger.error(
            " NEXT STEP: correct the source file and re-upload it "
            "with its original canonical name."
        )

    if TRACKING_ENABLED:
        try:
            if snowflake_options is None:
                snowflake_options = get_snowflake_options(dbutils)

            period_start = None
            period_end = None
            if TARGET_YEAR is not None and TARGET_MONTH is not None:
                period_start, period_end = _period_dates(
                    TARGET_YEAR,
                    TARGET_MONTH,
                )
                target_after_metrics = _read_period_metrics_safely(
                    spark,
                    snowflake_options,
                    TARGET_YEAR,
                    TARGET_MONTH,
                )

            failure_record = run_context.build_record(
                status="FAILED",
                source_name=settings.PIPELINE_SOURCE_NAME,
                source_file=FILE_NAME,
                target_name=settings.SNOWFLAKE_FULL_TABLE,
                period_start_date=period_start,
                period_end_date=period_end,
                source_rows=source_rows,
                staging_rows=staged_rows,
                transformed_rows=result.get("transformed_rows"),
                target_rows_before=target_before_metrics.get("TARGET_ROWS"),
                target_rows_after=target_after_metrics.get("TARGET_ROWS"),
                business_metrics={
                    "before": target_before_metrics,
                    "after": target_after_metrics,
                    "source_columns": source_column_count,
                    "delta_rows_after": result.get("delta_rows"),
                },
                error_type=type(pipeline_error).__name__,
                error_message=str(pipeline_error),
            )
            append_run_record_safely(
                spark=spark,
                snowflake_options=snowflake_options,
                table_name=TRACKING_TABLE,
                record=failure_record,
                logger=logger,
            )
        except Exception as tracking_error:
            logger.error(
                "TRACKING_FAILURE_HANDLER_ERROR | %s: %s",
                type(tracking_error).__name__,
                sanitize_error_message(tracking_error),
            )

    logger.error("=" * 64)
    raise

if no_files_to_process:
    dbutils.notebook.exit(exit_message)
