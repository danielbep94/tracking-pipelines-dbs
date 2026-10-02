"""
Reusable append-only pipeline run tracking for Databricks and Snowflake.

This standalone module provides run context management, metrics tracking,
lightweight DataFrame profiling, lightweight DQ checks, runtime metadata
collection, and telemetry logging to Snowflake tables
(e.g. PRD_MDP.MDP_STG.PIPELINE_RUNS).

Design principles
-----------------
* One tracker execution produces (at most) one row in PIPELINE_RUNS.
* BUSINESS_METRICS_JSON is the single extensibility point for additional
  profiling metrics, reconciliation values, DQ summaries, and business KPIs.
* No additional Snowflake tables are introduced.
* Public function and class names remain backward-compatible.
* A failed Snowflake write or verification does not permanently disable the
  tracker: calling finish() again retries only the step that previously
  failed (append is not retried once it has succeeded; verification is not
  retried once it has succeeded).
* If a previous append attempt raised an exception but may have actually
  committed (e.g. the write succeeded but the client lost the
  acknowledgment), the tracker reconciles by querying Snowflake for RUN_ID
  BEFORE attempting another append, instead of blindly retrying. This
  mitigates -- but does not fully eliminate -- duplicate-row risk: it
  protects against a single writer retrying after an uncertain outcome, but
  it does not provide atomic exactly-once guarantees against concurrent
  writers appending for the same RUN_ID at the same time (that would
  require a uniqueness constraint or MERGE-based upsert enforced at the
  Snowflake layer). Do not describe this module as fully idempotent under
  concurrent writers.
* finish() must only be called once, after all business logic has
  completed. When using the ``with run_tracking(...) as tracker:`` pattern,
  do NOT call tracker.finish() manually -- if you do, the call is DEFERRED
  (returns None) and finalization still happens correctly in __exit__ once
  the `with` block truly completes, so a later exception is never masked by
  an earlier, premature success. This module does not create automatic
  "corrective" second rows for the same RUN_ID -- once a row is confirmed
  committed, any later status/error mismatch is only reported loudly
  (TRACKING_STATUS_CHANGED_AFTER_APPEND), never silently rewritten or
  silently duplicated.
"""


import base64
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
import json
import logging
from typing import Any, Dict, List, Optional
from uuid import uuid4


# ---------------------------------------------------------------------------
# Sub-module imports (Phase 1: constants + utilities extracted)
# ---------------------------------------------------------------------------

from ._constants import logger
from ._utils import (
    utc_now,
    parse_optional_int,
    normalize_optional_long,
    parse_utc_datetime,
    sanitize_error_message,
    emit_tracking_log,
)

# ---------------------------------------------------------------------------
# Sub-module imports (Phase 2: JSON helpers extracted)
# ---------------------------------------------------------------------------

from ._json import (
    _normalize_metrics_value,
    _json_default,
    metrics_to_json,
)


# ---------------------------------------------------------------------------
# Databricks helpers
# ---------------------------------------------------------------------------

def get_widget_value(
    dbutils,
    name: str,
    default: Optional[str] = None,
) -> Optional[str]:
    """Safely read a Databricks notebook widget."""
    if dbutils is None:
        return default

    try:
        value = dbutils.widgets.get(name)
    except Exception:
        return default

    normalized = str(value).strip()

    return normalized if normalized else default


def get_databricks_context_tag(
    dbutils,
    tag_name: str,
) -> Optional[str]:
    """Safely extract a Databricks runtime context tag."""
    if dbutils is None:
        return None

    try:
        context_json = (
            dbutils.notebook.entry_point
            .getDbutils()
            .notebook()
            .getContext()
            .toJson()
        )

        context_data = json.loads(context_json)
        tags = context_data.get("tags", {})

        value = tags.get(tag_name)

        if value is None:
            return None

        normalized = str(value).strip()

        return normalized if normalized else None

    except Exception:
        return None


def _collect_runtime_metadata(
    spark=None,
    dbutils=None,
) -> Dict[str, Any]:
    """Collect optional Databricks and Spark runtime metadata."""
    metadata: Dict[str, Any] = {}

    if spark is not None:
        try:
            metadata["runtime_spark_version"] = spark.version
        except Exception:
            pass

        try:
            metadata["runtime_cluster_id"] = spark.conf.get(
                "spark.databricks.clusterUsageTags.clusterId",
                None,
            )
        except Exception:
            pass

    if dbutils is not None:
        try:
            context_json = (
                dbutils.notebook.entry_point
                .getDbutils()
                .notebook()
                .getContext()
                .toJson()
            )

            context_data = json.loads(context_json)
            tags = context_data.get("tags", {})
            extra_context = context_data.get("extraContext", {})

            metadata_mapping = (
                ("notebookPath", "runtime_notebook_path"),
                ("gitCommit", "runtime_git_sha"),
                ("browserHostName", "runtime_host"),
            )

            for source_key, target_key in metadata_mapping:
                value = tags.get(source_key) or extra_context.get(source_key)

                if value:
                    metadata[target_key] = str(value).strip()

        except Exception:
            pass

    return {
        key: value
        for key, value in metadata.items()
        if value is not None
    }


# ---------------------------------------------------------------------------
# Snowflake authentication
# ---------------------------------------------------------------------------

def _encode_private_key_for_spark(private_key_pem: str) -> str:
    """
    Convert an unencrypted PEM RSA private key into a Base64 PKCS#8 DER key
    for the Snowflake Spark connector.
    """
    try:
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
    except ImportError as exc:
        raise ImportError(
            "The 'cryptography' package is required to encode "
            "Snowflake private keys."
        ) from exc

    normalized_pem = private_key_pem.strip()

    if "\\n" in normalized_pem and "\n" not in normalized_pem:
        normalized_pem = normalized_pem.replace("\\n", "\n")

    try:
        private_key = serialization.load_pem_private_key(
            normalized_pem.encode("utf-8"),
            password=None,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "Snowflake private key must contain a valid, unencrypted "
            "PEM private key."
        ) from exc

    if not isinstance(private_key, rsa.RSAPrivateKey):
        raise TypeError(
            "Snowflake key-pair authentication requires an RSA key."
        )

    if private_key.key_size < 2048:
        raise ValueError(
            "Snowflake RSA private key must be at least 2048 bits."
        )

    private_key_der = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    return base64.b64encode(private_key_der).decode("ascii")


def get_snowflake_options(
    dbutils,
    secret_scope: str = "DAN-AM-P-KVT800-R-MDP-DB",
    user_key: str = "snowflake-user",
    private_key_secret: str = "Snowflake-Private-Key",
    url: str = "danonenam.east-us-2.azure.snowflakecomputing.com",
    database: str = "PRD_MDP",
    schema: str = "MDP_STG",
    warehouse: str = "PRD_MDP_ANL_WH",
    role: str = "PRD_MDP",
) -> Dict[str, str]:
    """Obtain Snowflake credentials from Databricks secrets."""
    if dbutils is None:
        raise ValueError(
            "dbutils is required to obtain Snowflake credentials."
        )

    snowflake_user = dbutils.secrets.get(scope=secret_scope, key=user_key)
    private_key_pem = dbutils.secrets.get(
        scope=secret_scope,
        key=private_key_secret,
    )

    if not str(snowflake_user).strip():
        raise ValueError("The Snowflake username secret is empty.")

    if not str(private_key_pem).strip():
        raise ValueError("The Snowflake private-key secret is empty.")

    return {
        "sfURL": url,
        "sfUser": snowflake_user,
        "pem_private_key": _encode_private_key_for_spark(private_key_pem),
        "sfDatabase": database,
        "sfSchema": schema,
        "sfWarehouse": warehouse,
        "sfRole": role,
    }


# ---------------------------------------------------------------------------
# Snowflake query tagging
# ---------------------------------------------------------------------------

def build_query_tag_statement(
    run_id: str,
    pipeline_name: str,
    pipeline_version: Optional[str] = None,
) -> str:
    """
    Build an "ALTER SESSION SET QUERY_TAG = ..." statement for auditing in
    Snowflake QUERY_HISTORY.

    This statement must be attached to an actual read or write operation
    using the "preactions" (or "postactions") option of the Snowflake Spark
    connector. The connector's DataFrameReader "query" option only supports
    SELECT statements, so ALTER SESSION cannot be executed as a standalone
    read.

    Example
    -------
        tag_sql = build_query_tag_statement(
            run_id=tracker.context.run_id,
            pipeline_name="FACT_SALES_OTC",
            pipeline_version="1.2.0",
        )

        (
            df.write.format("net.snowflake.spark.snowflake")
            .options(**sf_options)
            .option("dbtable", "MDP_STG.MY_TABLE")
            .option("preactions", tag_sql)
            .mode("append")
            .save()
        )
    """
    tag_payload = json.dumps(
        {
            "run_id": run_id,
            "pipeline_name": pipeline_name,
            "pipeline_version": pipeline_version or "unversioned",
        },
        separators=(",", ":"),
    )

    escaped_tag_payload = tag_payload.replace("'", "''")

    return f"ALTER SESSION SET QUERY_TAG = '{escaped_tag_payload}'"


def tag_snowflake_session(
    spark=None,
    snowflake_options: Optional[Dict[str, str]] = None,
    run_id: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    pipeline_version: Optional[str] = None,
) -> str:
    """
    Deprecated.

    Previous versions of this function executed a standalone read using
    ``spark.read...option("query", "ALTER SESSION ...")``. Snowflake's Spark
    connector only supports SELECT statements through that path, so the
    previous implementation silently failed or raised at runtime.

    This function no longer executes anything. It returns the QUERY_TAG SQL
    statement so callers can attach it to their own read/write operation via
    the "preactions" option. See build_query_tag_statement() for the
    non-deprecated equivalent.
    """
    emit_tracking_log(
        level="WARNING",
        event="SNOWFLAKE_TAG_DEPRECATED",
        run_id=run_id,
        pipeline_name=pipeline_name,
        reason=(
            "tag_snowflake_session() no longer executes a standalone ALTER "
            "SESSION statement, because Snowflake's Spark connector only "
            "supports SELECT through the read 'query' option. Use "
            "build_query_tag_statement() with the 'preactions' write "
            "option instead."
        ),
    )

    return build_query_tag_statement(
        run_id=run_id,
        pipeline_name=pipeline_name,
        pipeline_version=pipeline_version,
    )


# ---------------------------------------------------------------------------
# Run context
# ---------------------------------------------------------------------------

@dataclass
class RunContext:
    """Execution metadata for one pipeline run."""

    pipeline_name: str
    environment: str
    pipeline_version: Optional[str] = None
    databricks_job_id: Optional[str] = None
    databricks_job_run_id: Optional[str] = None
    databricks_task_run_id: Optional[str] = None
    databricks_task_name: Optional[str] = None
    attempt_number: Optional[int] = None
    trigger_type: Optional[str] = None
    started_at_utc: datetime = field(default_factory=utc_now)
    run_id: str = field(default_factory=lambda: str(uuid4()))

    def build_record(
        self,
        *,
        status: str,
        source_name: Optional[str] = None,
        source_file: Optional[str] = None,
        target_name: Optional[str] = None,
        period_start_date: Optional[date] = None,
        period_end_date: Optional[date] = None,
        source_rows: Optional[int] = None,
        staging_rows: Optional[int] = None,
        transformed_rows: Optional[int] = None,
        target_rows_before: Optional[int] = None,
        target_rows_after: Optional[int] = None,
        business_metrics: Optional[Dict[str, Any]] = None,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
        completed_at_utc: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Build one record matching the PIPELINE_RUNS schema."""
        completed_at = completed_at_utc or utc_now()

        duration_seconds = max(
            0.0,
            (completed_at - self.started_at_utc).total_seconds(),
        )

        source_rows = normalize_optional_long(source_rows)
        staging_rows = normalize_optional_long(staging_rows)
        transformed_rows = normalize_optional_long(transformed_rows)
        target_rows_before = normalize_optional_long(target_rows_before)
        target_rows_after = normalize_optional_long(target_rows_after)

        target_delta = None

        if target_rows_before is not None and target_rows_after is not None:
            target_delta = target_rows_after - target_rows_before

        return {
            "RUN_ID": self.run_id,
            "PIPELINE_NAME": self.pipeline_name,
            "PIPELINE_VERSION": self.pipeline_version,
            "ENVIRONMENT": self.environment,
            "STATUS": status,
            "DATABRICKS_JOB_ID": self.databricks_job_id,
            "DATABRICKS_JOB_RUN_ID": self.databricks_job_run_id,
            "DATABRICKS_TASK_RUN_ID": self.databricks_task_run_id,
            "DATABRICKS_TASK_NAME": self.databricks_task_name,
            "ATTEMPT_NUMBER": normalize_optional_long(self.attempt_number),
            "TRIGGER_TYPE": self.trigger_type,
            "SOURCE_NAME": source_name,
            "SOURCE_FILE": source_file,
            "TARGET_NAME": target_name,
            "PERIOD_START_DATE": period_start_date,
            "PERIOD_END_DATE": period_end_date,
            "STARTED_AT_UTC": self.started_at_utc,
            "COMPLETED_AT_UTC": completed_at,
            "DURATION_SECONDS": duration_seconds,
            "SOURCE_ROWS": source_rows,
            "STAGING_ROWS": staging_rows,
            "TRANSFORMED_ROWS": transformed_rows,
            "TARGET_ROWS_BEFORE": target_rows_before,
            "TARGET_ROWS_AFTER": target_rows_after,
            "TARGET_ROW_DELTA": target_delta,
            "BUSINESS_METRICS_JSON": metrics_to_json(business_metrics),
            "ERROR_TYPE": sanitize_error_message(error_type),
            "ERROR_MESSAGE": sanitize_error_message(error_message),
            "CREATED_AT_UTC": completed_at,
        }


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


# ---------------------------------------------------------------------------
# RunTracker
# ---------------------------------------------------------------------------

class RunTracker:
    """
    Pipeline run tracker for Databricks and Snowflake.

    Usage (explicit finish, recommended for plain scripts/notebooks)::

        tracker = run_tracking(
            spark=spark,
            snowflake_options=sf_opts,
            pipeline_name="FACT_SALES_OTC",
            dbutils=dbutils,
        )

        # ... all business logic here ...

        tracker.finish()

    Usage (context manager)::

        with run_tracking(spark=spark, ...) as tracker:
            # ... all business logic here ...
            pass
        # Do NOT call tracker.finish() manually inside this block.
        # __exit__ finalizes the run automatically, including marking the
        # record FAILED if an exception is raised anywhere in the block.

    Calling finish() manually while still inside a ``with`` block no longer
    risks recording a false success: the call is DEFERRED (it returns None
    and logs TRACKING_FINISH_DEFERRED_TO_CONTEXT_EXIT) and the record is
    only actually built and written later, in __exit__, once the whole
    `with` block has genuinely completed -- so an exception raised
    afterward is still correctly recorded as FAILED.

    This tracker does not create automatic "corrective" second rows for the
    same RUN_ID. Once a row is confirmed committed (self._append_succeeded
    is True), it is treated as immutable: any later status/error mismatch
    is only ever reported loudly via TRACKING_STATUS_CHANGED_AFTER_APPEND
    and exposed through self.tracking_error -- never silently rewritten and
    never silently duplicated.

    Retrying finish() after an append raised an exception does not blindly
    resend the record either: because the exception could mean "the write
    failed" or "the write succeeded but the acknowledgment was lost", the
    retry first reconciles against Snowflake by RUN_ID (see
    _reconcile_uncertain_append). This reduces -- but, under concurrent
    writers, does not fully eliminate -- the risk of duplicate rows for the
    same RUN_ID; see the module-level docstring for the precise guarantee.
    """

    def __init__(
        self,
        spark=None,
        snowflake_options: Optional[Dict[str, str]] = None,
        pipeline_name: str = "UNNAMED_PIPELINE",
        environment: str = "PROD",
        table_name: str = "PRD_MDP.MDP_STG.PIPELINE_RUNS",
        dbutils=None,
        pipeline_version: Optional[str] = None,
        source_name: Optional[str] = None,
        source_file: Optional[str] = None,
        target_name: Optional[str] = None,
        period_start_date: Optional[date] = None,
        period_end_date: Optional[date] = None,
        enabled: bool = True,
        log_obj: Optional[logging.Logger] = None,
        collect_runtime_metadata: bool = True,
        raise_on_failure: bool = False,
        verify_write: bool = True,
    ):
        self.spark = spark
        self.snowflake_options = snowflake_options
        self.pipeline_name = pipeline_name
        self.environment = environment
        self.table_name = table_name
        self.dbutils = dbutils
        self.enabled = enabled
        self.logger = log_obj or logger
        self.raise_on_failure = raise_on_failure
        self.verify_write = verify_write

        self.source_name = source_name
        self.source_file = source_file
        self.target_name = target_name
        self.period_start_date = period_start_date
        self.period_end_date = period_end_date

        # Row-count metrics remain NULL unless explicitly assigned.
        self.source_rows: Optional[int] = None
        self.staging_rows: Optional[int] = None
        self.transformed_rows: Optional[int] = None
        self.target_rows_before: Optional[int] = None
        self.target_rows_after: Optional[int] = None

        # Extensible business-metrics payload.
        self.business_metrics: Dict[str, Any] = {}

        # DQ results accumulated during execution.
        self._dq_results: List[Dict[str, Any]] = []

        # Pipeline status. Overridden to "FAILED" by an exception or by a
        # failed required DQ check.
        self.status: str = "SUCCEEDED"

        # Deferred error info set by a failed *required* DQ check, used only
        # if no real exception occurs (a real exception always takes
        # precedence).
        self._pending_error_type: Optional[str] = None
        self._pending_error_message: Optional[str] = None

        # Lifecycle state. Each stage tracks its own tri-state result
        # (None = not attempted, True = succeeded, False = failed) so that a
        # retry only re-attempts the stage that actually failed.
        self._context_manager_active: bool = False
        self._manual_finish_called: bool = False
        self._record: Optional[Dict[str, Any]] = None
        self._append_succeeded: Optional[bool] = None
        self._verification_succeeded: Optional[bool] = None
        self._verification_record_count: Optional[int] = None
        self._write_succeeded: Optional[bool] = None
        self._tracking_error: Optional[str] = None

        try:
            started_at = (
                parse_utc_datetime(
                    get_widget_value(
                        dbutils,
                        "tracking_job_started_at_utc",
                    )
                )
                or utc_now()
            )

            version = pipeline_version or get_widget_value(
                dbutils,
                "tracking_pipeline_version",
                "workspace-unversioned",
            )

            job_id = get_widget_value(
                dbutils,
                "tracking_job_id",
            ) or get_databricks_context_tag(dbutils, "jobId")

            job_run_id = (
                get_widget_value(dbutils, "tracking_job_run_id")
                or get_databricks_context_tag(
                    dbutils, "multitaskParentRunId"
                )
                or get_databricks_context_tag(dbutils, "jobRunId")
                or get_databricks_context_tag(dbutils, "idInJob")
            )

            task_run_id = get_widget_value(
                dbutils,
                "tracking_task_run_id",
            ) or get_databricks_context_tag(dbutils, "taskRunId")

            task_name = (
                get_widget_value(dbutils, "tracking_task_name")
                or get_databricks_context_tag(dbutils, "taskKey")
                or pipeline_name
            )

            attempt_number = parse_optional_int(
                get_widget_value(dbutils, "tracking_attempt_number")
            )

            trigger_type = get_widget_value(
                dbutils,
                "tracking_trigger_type",
                "one_time",
            )

            self.context = RunContext(
                pipeline_name=pipeline_name,
                environment=environment,
                pipeline_version=version,
                databricks_job_id=job_id,
                databricks_job_run_id=job_run_id,
                databricks_task_run_id=task_run_id,
                databricks_task_name=task_name,
                attempt_number=attempt_number,
                trigger_type=trigger_type,
                started_at_utc=started_at,
            )

            if collect_runtime_metadata:
                runtime_metadata = _collect_runtime_metadata(
                    spark=spark,
                    dbutils=dbutils,
                )

                if runtime_metadata:
                    self.business_metrics.update(runtime_metadata)

            emit_tracking_log(
                level="INFO",
                event="TRACKING_INITIALIZED",
                log_obj=self.logger,
                run_id=self.context.run_id,
                pipeline_name=self.context.pipeline_name,
                pipeline_version=self.context.pipeline_version,
                environment=self.context.environment,
                job_id=self.context.databricks_job_id,
                job_run_id=self.context.databricks_job_run_id,
                task_run_id=self.context.databricks_task_run_id,
                task_name=self.context.databricks_task_name,
                attempt_number=self.context.attempt_number,
                trigger_type=self.context.trigger_type,
                verification_enabled=self.verify_write,
                raise_on_failure=self.raise_on_failure,
                table=self.table_name,
            )

        except Exception as initialization_error:
            self.enabled = False
            self._write_succeeded = False
            self._tracking_error = sanitize_error_message(
                initialization_error
            )

            emit_tracking_log(
                level="ERROR",
                event="TRACKING_INIT_FAILED",
                log_obj=self.logger,
                pipeline_name=pipeline_name,
                table=table_name,
                error_type=type(initialization_error).__name__,
                error_message=self._tracking_error,
                action="tracking_disabled",
            )

    # ------------------------------------------------------------------
    # Public result properties
    # ------------------------------------------------------------------

    @property
    def write_succeeded(self) -> Optional[bool]:
        """Return the result of the Snowflake write and verification."""
        return self._write_succeeded

    @property
    def verification_record_count(self) -> Optional[int]:
        """Return the number of Snowflake records found during verification."""
        return self._verification_record_count

    @property
    def tracking_error(self) -> Optional[str]:
        """Return the latest sanitized tracking error message, if any."""
        return self._tracking_error

    @property
    def record(self) -> Optional[Dict[str, Any]]:
        """Return the final tracking record, once built."""
        return self._record

    # ------------------------------------------------------------------
    # Metric helpers
    # ------------------------------------------------------------------

    def set_metrics(self, **metrics: Any) -> None:
        """Add or overwrite individual BUSINESS_METRICS_JSON values."""
        try:
            self.business_metrics.update(metrics)
        except Exception as metrics_error:
            emit_tracking_log(
                level="WARNING",
                event="TRACKING_SET_METRICS_FAILED",
                log_obj=self.logger,
                error_type=type(metrics_error).__name__,
                error_message=sanitize_error_message(metrics_error),
            )

    def add_metrics(self, metrics: Dict[str, Any]) -> None:
        """Merge a metrics dictionary into BUSINESS_METRICS_JSON."""
        try:
            if metrics:
                self.business_metrics.update(metrics)
        except Exception as metrics_error:
            emit_tracking_log(
                level="WARNING",
                event="TRACKING_ADD_METRICS_FAILED",
                log_obj=self.logger,
                error_type=type(metrics_error).__name__,
                error_message=sanitize_error_message(metrics_error),
            )

    # ------------------------------------------------------------------
    # DataFrame profiling
    # ------------------------------------------------------------------

    def profile(
        self,
        df,
        *,
        prefix: str = "",
        count: bool = True,
        distinct_columns: Optional[List[str]] = None,
        key_columns: Optional[List[str]] = None,
        null_columns: Optional[List[str]] = None,
        sum_columns: Optional[List[str]] = None,
        min_columns: Optional[List[str]] = None,
        max_columns: Optional[List[str]] = None,
        avg_columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Compute lightweight profiling metrics on a Spark DataFrame and return
        them as a flat dictionary. Call add_metrics() to merge the result
        into BUSINESS_METRICS_JSON.

        Duplicate-related metrics (only computed when key_columns is given):

        * {prefix}duplicate_key_group_count -- number of DISTINCT key
          combinations that appear more than once (i.e. duplicated groups).
        * {prefix}duplicate_row_count -- number of EXCESS rows beyond the
          first occurrence of each duplicated key (e.g. 5 rows sharing one
          key produce duplicate_key_group_count=1, duplicate_row_count=4).

        Null-rate metrics are computed correctly even when count=False: an
        internal row count is still calculated whenever null_columns is
        supplied, so {col}_null_rate is never silently forced to None just
        because the row_count metric itself was not requested.
        """
        result: Dict[str, Any] = {}

        try:
            from pyspark.sql import functions as F

            need_internal_row_count = count or bool(null_columns)

            aggregate_expressions = []

            if need_internal_row_count:
                aggregate_expressions.append(
                    F.count("*").alias("__row_count")
                )

            for column_name in distinct_columns or []:
                aggregate_expressions.append(
                    F.countDistinct(column_name).alias(
                        f"__distinct__{column_name}"
                    )
                )

            for column_name in null_columns or []:
                aggregate_expressions.append(
                    F.sum(F.col(column_name).isNull().cast("int")).alias(
                        f"__null__{column_name}"
                    )
                )

            for column_name in sum_columns or []:
                aggregate_expressions.append(
                    F.sum(column_name).alias(f"__sum__{column_name}")
                )

            for column_name in min_columns or []:
                aggregate_expressions.append(
                    F.min(column_name).alias(f"__min__{column_name}")
                )

            for column_name in max_columns or []:
                aggregate_expressions.append(
                    F.max(column_name).alias(f"__max__{column_name}")
                )

            for column_name in avg_columns or []:
                aggregate_expressions.append(
                    F.avg(column_name).alias(f"__avg__{column_name}")
                )

            internal_row_count = 0

            if aggregate_expressions:
                row = df.agg(*aggregate_expressions).first()

                if row:
                    values = row.asDict()

                    internal_row_count = values.get("__row_count") or 0

                    if count:
                        result[f"{prefix}row_count"] = values.get(
                            "__row_count"
                        )

                    for column_name in distinct_columns or []:
                        result[
                            f"{prefix}{column_name.lower()}_distinct_count"
                        ] = values.get(f"__distinct__{column_name}")

                    for column_name in null_columns or []:
                        null_count = values.get(f"__null__{column_name}")

                        result[
                            f"{prefix}{column_name.lower()}_null_count"
                        ] = null_count

                        null_rate_key = (
                            f"{prefix}{column_name.lower()}_null_rate"
                        )

                        if internal_row_count and null_count is not None:
                            result[null_rate_key] = round(
                                null_count / internal_row_count,
                                6,
                            )
                        else:
                            result[null_rate_key] = None

                    for column_name in sum_columns or []:
                        result[
                            f"{prefix}{column_name.lower()}_sum"
                        ] = values.get(f"__sum__{column_name}")

                    for column_name in min_columns or []:
                        result[
                            f"{prefix}{column_name.lower()}_min"
                        ] = values.get(f"__min__{column_name}")

                    for column_name in max_columns or []:
                        result[
                            f"{prefix}{column_name.lower()}_max"
                        ] = values.get(f"__max__{column_name}")

                    for column_name in avg_columns or []:
                        average_value = values.get(f"__avg__{column_name}")

                        result[
                            f"{prefix}{column_name.lower()}_avg"
                        ] = (
                            round(float(average_value), 6)
                            if average_value is not None
                            else None
                        )

            if key_columns:
                duplicate_groups_df = (
                    df.groupBy(*key_columns)
                    .count()
                    .filter(F.col("count") > 1)
                )

                duplicate_key_group_count = duplicate_groups_df.count()

                excess_row_count_row = duplicate_groups_df.agg(
                    F.sum(F.col("count") - F.lit(1)).alias("__excess_rows")
                ).first()

                duplicate_row_count = 0

                if (
                    excess_row_count_row is not None
                    and excess_row_count_row["__excess_rows"] is not None
                ):
                    duplicate_row_count = int(
                        excess_row_count_row["__excess_rows"]
                    )

                result[
                    f"{prefix}duplicate_key_group_count"
                ] = duplicate_key_group_count

                result[
                    f"{prefix}duplicate_row_count"
                ] = duplicate_row_count

        except Exception as profile_error:
            emit_tracking_log(
                level="WARNING",
                event="PROFILE_FAILED",
                log_obj=self.logger,
                error_type=type(profile_error).__name__,
                error_message=sanitize_error_message(profile_error),
            )

        return result

    # ------------------------------------------------------------------
    # Data-quality checks
    # ------------------------------------------------------------------

    def check(
        self,
        condition: bool,
        name: str,
        required: bool = False,
    ) -> bool:
        """
        Register one data-quality check result.

        Parameters
        ----------
        condition : Boolean result of the check. True = passed.
        name      : Human-readable check name (used for logging only).
        required  : When True, a failed check forces the pipeline run
                    status to FAILED at finish() time (unless a real
                    business exception already set it). When False
                    (default), the check is purely informational: results
                    are still counted in BUSINESS_METRICS_JSON
                    (dq_checks_total, dq_checks_passed, dq_checks_failed)
                    but do not change the run status by themselves.

        Returns
        -------
        bool -- the value of condition, so callers can branch on it.

        Example::

            tracker.check(
                df.filter("KILOS < 0").count() == 0,
                "no_negative_kilos",
                required=True,
            )
        """
        passed = bool(condition)

        self._dq_results.append(
            {"name": name, "passed": passed, "required": required}
        )

        if passed:
            emit_tracking_log(
                level="INFO",
                event="DQ_CHECK_PASSED",
                log_obj=self.logger,
                check=name,
                required=required,
            )
        else:
            emit_tracking_log(
                level="ERROR" if required else "WARNING",
                event="DQ_CHECK_FAILED",
                log_obj=self.logger,
                check=name,
                required=required,
            )

        return passed

    def _flush_dq_summary(self) -> None:
        """
        Merge aggregate DQ results into BUSINESS_METRICS_JSON. If any
        *required* check failed, force the run status to FAILED (unless a
        real business exception already set it), so silently-ignored
        required checks can no longer leave STATUS=SUCCEEDED.
        """
        if not self._dq_results:
            return

        total_checks = len(self._dq_results)

        passed_checks = sum(
            1 for result in self._dq_results if result["passed"]
        )

        failed_required_checks = [
            result["name"]
            for result in self._dq_results
            if not result["passed"] and result["required"]
        ]

        self.business_metrics.update(
            {
                "dq_checks_total": total_checks,
                "dq_checks_passed": passed_checks,
                "dq_checks_failed": total_checks - passed_checks,
                "dq_required_checks_failed": len(failed_required_checks),
            }
        )

        if failed_required_checks and self.status != "FAILED":
            self.status = "FAILED"
            self._pending_error_type = "RequiredDataQualityCheckFailed"
            self._pending_error_message = (
                "Required data-quality checks failed: "
                + ", ".join(failed_required_checks)
            )

    # ------------------------------------------------------------------
    # Tracking lifecycle
    # ------------------------------------------------------------------

    def _query_run_id_state(
        self,
        run_id: str,
    ) -> "tuple[int, Optional[str]]":
        """
        Query Snowflake for the number of rows and the stored STATUS for a
        given RUN_ID.

        Returns
        -------
        tuple[int, Optional[str]]
            (record_count, stored_status). stored_status is None when
            record_count is 0, or when record_count > 1 (ambiguous).

        Used both for post-append verification-with-context and for
        reconciling an *uncertain* previous append attempt (see
        _reconcile_uncertain_append). Exceptions are not suppressed; the
        caller decides how to handle a failed reconciliation query.
        """
        if not run_id:
            raise ValueError(
                "RUN_ID is required to query Snowflake tracking state."
            )

        safe_run_id = str(run_id).replace("'", "''")

        query = f"""
            SELECT
                COUNT(*) AS RECORD_COUNT,
                MAX(STATUS) AS STORED_STATUS
            FROM {self.table_name}
            WHERE RUN_ID = '{safe_run_id}'
        """

        result = read_snowflake_metrics(
            spark=self.spark,
            snowflake_options=self.snowflake_options,
            query=query,
        )

        raw_record_count = None
        stored_status = None

        for key, value in result.items():
            upper_key = str(key).upper()
            if upper_key == "RECORD_COUNT":
                raw_record_count = value
            elif upper_key == "STORED_STATUS":
                stored_status = value

        if raw_record_count is None:
            raise RuntimeError(
                "Snowflake reconciliation query did not return "
                "RECORD_COUNT."
            )

        record_count = normalize_optional_long(raw_record_count)

        if record_count is None:
            raise RuntimeError(
                "Snowflake reconciliation query returned an empty "
                "RECORD_COUNT."
            )

        return record_count, (
            str(stored_status) if stored_status is not None else None
        )

    def _reconcile_uncertain_append(self, record: Dict[str, Any]) -> str:
        """
        Reconcile an *uncertain* previous append attempt before retrying.

        A previous _attempt_append() call may have raised an exception even
        though Snowflake actually committed the row (e.g. the write
        succeeded but the client lost the acknowledgment due to a network
        timeout). Blindly retrying the append in that situation creates a
        second row with the same RUN_ID. This method queries Snowflake by
        RUN_ID first, so a genuinely-uncertain write is resolved without
        duplicating the record.

        Returns one of:
            "present"  -- exactly one row already exists for RUN_ID. The
                          append is considered recovered; do not append
                          again. If the stored STATUS differs from the
                          status we are about to write, the mismatch is
                          logged loudly (TRACKING_STATUS_CHANGED_AFTER_APPEND)
                          instead of being silently accepted or silently
                          overwritten.
            "absent"   -- no row exists for RUN_ID. It is safe to proceed
                          with a real append attempt.
            "duplicate" -- more than one row already exists for RUN_ID.
                          Appending again would make this worse; the caller
                          must not retry.
            "unknown"  -- the reconciliation query itself failed, so we
                          cannot determine whether the previous write landed.
                          The caller must not guess; refusing to append
                          again is the safer default (a missed row is
                          recoverable by re-running finish(); a duplicate
                          row is not).

        Limitation
        ----------
        This mitigates duplicate rows caused by a single writer retrying
        after an uncertain outcome (lost acknowledgment, timeout, etc.). It
        does not provide atomic exactly-once guarantees against *concurrent*
        writers appending for the same RUN_ID at the same time -- a real
        race between this SELECT and another process's INSERT is still
        possible. True multi-writer exactly-once semantics would require a
        uniqueness constraint or a MERGE-based upsert enforced at the
        Snowflake layer, which is outside what an append-only Spark-connector
        write can guarantee. In practice RUN_ID is generated once per
        RunTracker instance (a single Databricks task attempt), so
        concurrent writers for the same RUN_ID are not expected in normal
        operation.
        """
        run_id = record.get("RUN_ID")
        intended_status = record.get("STATUS")

        emit_tracking_log(
            level="WARNING",
            event="TRACKING_RECONCILIATION_STARTED",
            log_obj=self.logger,
            run_id=run_id,
            table=self.table_name,
            reason=(
                "Retrying an append after a previous uncertain failure. "
                "Checking Snowflake by RUN_ID before writing again, to "
                "avoid creating a duplicate row if the previous write "
                "actually committed."
            ),
        )

        try:
            record_count, stored_status = self._query_run_id_state(run_id)

        except Exception as reconciliation_error:
            self._tracking_error = sanitize_error_message(
                reconciliation_error
            )

            emit_tracking_log(
                level="ERROR",
                event="TRACKING_RECONCILIATION_FAILED",
                log_obj=self.logger,
                run_id=run_id,
                table=self.table_name,
                error_type=type(reconciliation_error).__name__,
                error_message=self._tracking_error,
                reason=(
                    "Cannot determine whether the previous append actually "
                    "committed. Refusing to append again to avoid a "
                    "possible duplicate row."
                ),
            )

            if self.raise_on_failure:
                raise

            return "unknown"

        if record_count == 0:
            emit_tracking_log(
                level="INFO",
                event="TRACKING_RECONCILIATION_CONFIRMED_ABSENT",
                log_obj=self.logger,
                run_id=run_id,
                table=self.table_name,
            )
            return "absent"

        if record_count > 1:
            self._tracking_error = (
                "Duplicate Snowflake records already exist for run_id="
                f"{run_id}: record_count={record_count}."
            )

            emit_tracking_log(
                level="ERROR",
                event="TRACKING_DUPLICATE_RECORDS_FOUND",
                log_obj=self.logger,
                run_id=run_id,
                table=self.table_name,
                record_count=record_count,
                reason="Refusing to append again; this must be reviewed manually.",
            )

            if self.raise_on_failure:
                raise RuntimeError(self._tracking_error)

            return "duplicate"

        # record_count == 1: the previous write actually committed.
        emit_tracking_log(
            level="WARNING",
            event="TRACKING_RECONCILED_AS_ALREADY_COMMITTED",
            log_obj=self.logger,
            run_id=run_id,
            table=self.table_name,
            stored_status=stored_status,
        )

        if stored_status != intended_status:
            self._tracking_error = (
                f"Snowflake already stores STATUS={stored_status!r} for "
                f"run_id={run_id}, but the current in-memory pipeline "
                f"status is {intended_status!r}. The already-committed row "
                "was NOT modified, and no automatic corrective row was "
                "appended (a second row for the same RUN_ID would violate "
                "the one-row-per-run design). Review this run_id manually."
            )

            emit_tracking_log(
                level="ERROR",
                event="TRACKING_STATUS_CHANGED_AFTER_APPEND",
                log_obj=self.logger,
                run_id=run_id,
                table=self.table_name,
                stored_status=stored_status,
                current_status=intended_status,
                reason=self._tracking_error,
            )

            if self.raise_on_failure:
                raise RuntimeError(self._tracking_error)

        else:
            # Recovery confirmed with matching status: clear any stale error
            # left over from the original uncertain failure.
            self._tracking_error = None

        return "present"

    def _attempt_append(self, record: Dict[str, Any]) -> None:
        """
        Attempt the Snowflake append. Updates self._append_succeeded.

        If a previous attempt for this same record already failed (i.e.
        self._append_succeeded is False, meaning the outcome of that prior
        attempt is uncertain -- it may have actually committed despite
        raising), this reconciles against Snowflake by RUN_ID first, instead
        of blindly appending again. See _reconcile_uncertain_append() for
        details and limitations.
        """
        run_id = record.get("RUN_ID")

        if self._append_succeeded is False:
            reconciliation = self._reconcile_uncertain_append(record)

            if reconciliation == "present":
                self._append_succeeded = True
                return

            if reconciliation in ("duplicate", "unknown"):
                # Do not attempt another append; the caller (_finalize) will
                # see self._append_succeeded remain False and stop there.
                self._append_succeeded = False
                return

            # reconciliation == "absent": fall through and attempt a real
            # append below, since we now know for certain nothing was
            # committed previously.

        emit_tracking_log(
            level="INFO",
            event="TRACKING_APPEND_STARTED",
            log_obj=self.logger,
            run_id=run_id,
            status=record.get("STATUS"),
            table=self.table_name,
        )

        try:
            append_run_record(
                spark=self.spark,
                snowflake_options=self.snowflake_options,
                table_name=self.table_name,
                record=record,
            )

            self._append_succeeded = True
            self._tracking_error = None

            emit_tracking_log(
                level="INFO",
                event="TRACKING_APPEND_SUCCEEDED",
                log_obj=self.logger,
                run_id=run_id,
                table=self.table_name,
            )

        except Exception as append_error:
            self._append_succeeded = False
            self._tracking_error = sanitize_error_message(append_error)

            emit_tracking_log(
                level="ERROR",
                event="TRACKING_APPEND_FAILED",
                log_obj=self.logger,
                run_id=run_id,
                table=self.table_name,
                error_type=type(append_error).__name__,
                error_message=self._tracking_error,
                reason=(
                    "Outcome is uncertain: the row may or may not have been "
                    "committed. A later retry will reconcile by RUN_ID "
                    "before appending again."
                ),
            )

            if self.raise_on_failure:
                raise

    def _attempt_verify(self, run_id: str) -> None:
        """Attempt Snowflake verification. Updates self._verification_succeeded."""
        emit_tracking_log(
            level="INFO",
            event="TRACKING_VERIFICATION_STARTED",
            log_obj=self.logger,
            run_id=run_id,
            table=self.table_name,
        )

        try:
            record_count = verify_run_record(
                spark=self.spark,
                snowflake_options=self.snowflake_options,
                table_name=self.table_name,
                run_id=run_id,
            )

        except Exception as verify_error:
            self._verification_succeeded = False
            self._tracking_error = sanitize_error_message(verify_error)

            emit_tracking_log(
                level="ERROR",
                event="TRACKING_VERIFICATION_FAILED",
                log_obj=self.logger,
                run_id=run_id,
                table=self.table_name,
                error_type=type(verify_error).__name__,
                error_message=self._tracking_error,
            )

            if self.raise_on_failure:
                raise

            return

        self._verification_record_count = record_count

        if record_count == 1:
            self._verification_succeeded = True
            self._tracking_error = None

            emit_tracking_log(
                level="INFO",
                event="TRACKING_RECORD_CONFIRMED",
                log_obj=self.logger,
                run_id=run_id,
                table=self.table_name,
                record_count=record_count,
            )

        elif record_count == 0:
            self._verification_succeeded = False
            self._tracking_error = (
                f"No Snowflake record found for run_id={run_id}."
            )

            emit_tracking_log(
                level="ERROR",
                event="TRACKING_RECORD_NOT_FOUND",
                log_obj=self.logger,
                run_id=run_id,
                table=self.table_name,
                record_count=record_count,
            )

            if self.raise_on_failure:
                raise RuntimeError(self._tracking_error)

        else:
            self._verification_succeeded = False
            self._tracking_error = (
                "Duplicate Snowflake records found for run_id="
                f"{run_id}: record_count={record_count}."
            )

            emit_tracking_log(
                level="ERROR",
                event="TRACKING_DUPLICATE_RECORDS_FOUND",
                log_obj=self.logger,
                run_id=run_id,
                table=self.table_name,
                record_count=record_count,
            )

            if self.raise_on_failure:
                raise RuntimeError(self._tracking_error)

    def _finalize(self, exc_type, exc_val, exc_tb) -> None:
        """
        Build (once, or rebuild while nothing has been committed yet) and
        write/verify the tracking record.

        Retry-safe: if a previous call already fully succeeded, this is a
        no-op. If append succeeded but verification failed, a later call
        retries only verification (not append again). If a previous append
        attempt failed with an uncertain outcome, a later call reconciles
        against Snowflake by RUN_ID before appending again (see
        _attempt_append / _reconcile_uncertain_append), rather than blindly
        creating a duplicate row.

        Once self._append_succeeded is True (the row is known to be
        physically committed, either from a clean write or from reconciling
        an uncertain one), the in-memory record is treated as immutable: a
        later status/error change is never used to silently rewrite the
        committed row's meaning, and it is never used to trigger an
        automatic second ("corrective") row -- both would conflict with the
        one-row-per-run design. Instead, any such mismatch is logged loudly
        via TRACKING_STATUS_CHANGED_AFTER_APPEND so it is visible to
        operators, and the discrepancy is exposed through
        self.tracking_error.
        """
        if self._write_succeeded is True:
            emit_tracking_log(
                level="INFO",
                event="TRACKING_ALREADY_FINISHED",
                log_obj=self.logger,
                run_id=getattr(
                    getattr(self, "context", None), "run_id", None
                ),
            )
            return

        if not self.enabled:
            self._write_succeeded = False

            if not self._tracking_error:
                self._tracking_error = (
                    "Run tracker is disabled. Initialization likely failed."
                )

            emit_tracking_log(
                level="ERROR",
                event="TRACKING_SKIPPED",
                log_obj=self.logger,
                reason=self._tracking_error,
            )

            if self.raise_on_failure and exc_type is None:
                raise RuntimeError(self._tracking_error)

            return

        if exc_type is not None:
            self.status = "FAILED"
            error_type = exc_type.__name__
            error_message = str(exc_val)
        else:
            error_type = self._pending_error_type
            error_message = self._pending_error_message

        emit_tracking_log(
            level="INFO",
            event="TRACKING_FINALIZATION_STARTED",
            log_obj=self.logger,
            run_id=getattr(getattr(self, "context", None), "run_id", None),
            status=self.status,
        )

        self._flush_dq_summary()

        if exc_type is None and self._pending_error_type and not error_type:
            error_type = self._pending_error_type
            error_message = self._pending_error_message

        if self._append_succeeded is True:
            # The row is already known to be physically committed. Do not
            # rebuild/resend it. Only detect and loudly report a mismatch
            # between what was committed and the current in-memory status.
            stored_status = (
                self._record["STATUS"] if self._record else None
            )

            if stored_status is not None and stored_status != self.status:
                mismatch_message = (
                    f"The tracking record for run_id="
                    f"{self._record['RUN_ID']} was already committed to "
                    f"Snowflake with STATUS={stored_status!r}, but the "
                    f"in-memory pipeline status is now {self.status!r}. "
                    "The committed row was NOT modified, and no automatic "
                    "corrective row was appended (a second row for the "
                    "same RUN_ID would violate the one-row-per-run "
                    "design). Review this run_id manually."
                )

                self._tracking_error = mismatch_message

                emit_tracking_log(
                    level="ERROR",
                    event="TRACKING_STATUS_CHANGED_AFTER_APPEND",
                    log_obj=self.logger,
                    run_id=self._record["RUN_ID"],
                    table=self.table_name,
                    stored_status=stored_status,
                    current_status=self.status,
                    error_type=error_type,
                    error_message=error_message,
                )

                if self.raise_on_failure:
                    raise RuntimeError(mismatch_message)
        else:
            # Nothing has been committed yet (or the previous attempt's
            # outcome is uncertain and will be reconciled inside
            # _attempt_append). It is safe to (re)build the record so that
            # any status/error change since the last attempt is reflected.
            self._record = self.context.build_record(
                status=self.status,
                source_name=self.source_name,
                source_file=self.source_file,
                target_name=self.target_name,
                period_start_date=self.period_start_date,
                period_end_date=self.period_end_date,
                source_rows=self.source_rows,
                staging_rows=self.staging_rows,
                transformed_rows=self.transformed_rows,
                target_rows_before=self.target_rows_before,
                target_rows_after=self.target_rows_after,
                business_metrics=(
                    self.business_metrics if self.business_metrics else None
                ),
                error_type=error_type,
                error_message=error_message,
            )

            emit_tracking_log(
                level="INFO",
                event="TRACKING_RECORD_BUILT",
                log_obj=self.logger,
                run_id=self._record["RUN_ID"],
                pipeline_name=self._record["PIPELINE_NAME"],
                status=self._record["STATUS"],
                job_id=self._record["DATABRICKS_JOB_ID"],
                job_run_id=self._record["DATABRICKS_JOB_RUN_ID"],
                task_run_id=self._record["DATABRICKS_TASK_RUN_ID"],
                table=self.table_name,
            )

        if self.spark is None:
            self._write_succeeded = False
            self._tracking_error = "Spark session was not provided to RunTracker."

            emit_tracking_log(
                level="ERROR",
                event="TRACKING_WRITE_SKIPPED",
                log_obj=self.logger,
                run_id=self._record["RUN_ID"],
                reason="spark_not_provided",
            )

            if self.raise_on_failure:
                raise ValueError(self._tracking_error)

            return

        if self.snowflake_options is None:
            self._write_succeeded = False
            self._tracking_error = (
                "Snowflake options were not provided to RunTracker."
            )

            emit_tracking_log(
                level="ERROR",
                event="TRACKING_WRITE_SKIPPED",
                log_obj=self.logger,
                run_id=self._record["RUN_ID"],
                reason="snowflake_options_not_provided",
            )

            if self.raise_on_failure:
                raise ValueError(self._tracking_error)

            return

        run_id = self._record["RUN_ID"]

        if self._append_succeeded is not True:
            self._attempt_append(self._record)

            if self._append_succeeded is not True:
                self._write_succeeded = False
                return

        if self.verify_write:
            if self._verification_succeeded is not True:
                self._attempt_verify(run_id)

            self._write_succeeded = self._verification_succeeded is True
        else:
            self._write_succeeded = True

        if self._write_succeeded:
            self._tracking_error = None

            emit_tracking_log(
                level="INFO",
                event="TRACKING_FINISHED_SUCCESSFULLY",
                log_obj=self.logger,
                run_id=run_id,
                pipeline_name=self._record["PIPELINE_NAME"],
                status=self._record["STATUS"],
                job_run_id=self._record["DATABRICKS_JOB_RUN_ID"],
                table=self.table_name,
            )
        else:
            emit_tracking_log(
                level="ERROR",
                event="TRACKING_FINISHED_WITH_ERROR",
                log_obj=self.logger,
                run_id=run_id,
                pipeline_name=self._record["PIPELINE_NAME"],
                status=self._record["STATUS"],
                job_run_id=self._record["DATABRICKS_JOB_RUN_ID"],
                table=self.table_name,
                reason=self._tracking_error,
            )

    def finish(self, status: Optional[str] = None) -> Optional[bool]:
        """
        Write and verify the final tracking record.

        Returns
        -------
        Optional[bool]
            True  -- the record was written and (when verify_write is
                      enabled) confirmed with exactly one Snowflake row for
                      RUN_ID.
            False -- tracking is disabled, or the write/verification failed
                      (see tracking_error for details).
            None  -- finalization was DEFERRED, because finish() was called
                      manually while this tracker is still active as a
                      context manager (``with run_tracking(...) as
                      tracker:``). Finalization happens later, in
                      __exit__, once the whole `with` block has actually
                      completed -- so that an exception raised AFTER this
                      call is still correctly recorded as FAILED, instead of
                      being masked by an earlier, premature SUCCEEDED
                      record. Do not treat None as an error; it simply means
                      "not finalized yet".

        Notes
        -----
        * Calling finish() more than once (after it actually finalizes) is
          safe: if it already fully succeeded, subsequent calls are a
          no-op returning True. If a previous call partially failed (e.g.
          append succeeded but verification failed), the next call retries
          only the step that failed. If a previous append attempt's outcome
          was uncertain, the next call reconciles against Snowflake by
          RUN_ID before appending again, to avoid creating a duplicate row.
        * Once the record is fully confirmed (this method has returned
          True), a later call with a different `status` no longer changes
          self.status -- the committed record is treated as final, per the
          one-row-per-run design.
        * Prefer the context-manager form
          (``with run_tracking(...) as tracker:``) over calling finish()
          manually, so this deferral logic is applied automatically.
        """
        if self._context_manager_active:
            if status:
                self.status = status

            self._manual_finish_called = True

            emit_tracking_log(
                level="WARNING",
                event="TRACKING_FINISH_DEFERRED_TO_CONTEXT_EXIT",
                log_obj=self.logger,
                run_id=getattr(
                    getattr(self, "context", None), "run_id", None
                ),
                reason=(
                    "finish() was called while the tracker is still being "
                    "used as a context manager. Finalization is deferred "
                    "until the `with` block exits, so a later exception is "
                    "still recorded correctly instead of being masked by "
                    "an earlier, premature success."
                ),
            )

            return None

        if self._write_succeeded is True:
            if status and status != self.status:
                emit_tracking_log(
                    level="WARNING",
                    event="TRACKING_STATUS_CHANGE_IGNORED_AFTER_FINALIZATION",
                    log_obj=self.logger,
                    run_id=getattr(
                        getattr(self, "context", None), "run_id", None
                    ),
                    requested_status=status,
                    stored_status=self.status,
                    reason=(
                        "The tracking record is already confirmed and "
                        "committed. Its status can no longer be changed "
                        "from here."
                    ),
                )

            emit_tracking_log(
                level="INFO",
                event="TRACKING_ALREADY_FINISHED",
                log_obj=self.logger,
                run_id=getattr(
                    getattr(self, "context", None), "run_id", None
                ),
            )

            return True

        self._manual_finish_called = True

        if status:
            self.status = status

        try:
            self._finalize(None, None, None)

        except Exception as finish_error:
            self._tracking_error = sanitize_error_message(finish_error)

            emit_tracking_log(
                level="ERROR",
                event="TRACKING_FINISH_FAILED",
                log_obj=self.logger,
                run_id=getattr(
                    getattr(self, "context", None), "run_id", None
                ),
                error_type=type(finish_error).__name__,
                error_message=self._tracking_error,
            )

            if self.raise_on_failure:
                raise

            return False

        return self._write_succeeded is True

    def complete(self, status: Optional[str] = None) -> Optional[bool]:
        """Alias for finish()."""
        return self.finish(status)

    def __enter__(self):
        """Mark the context manager as active and return the tracker."""
        self._context_manager_active = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Finalize the tracking record on context-manager exit.

        Business exceptions are never suppressed (always returns False).
        Because finish() defers finalization while
        self._context_manager_active is True (see finish()), this is
        normally the ONLY place where the record is actually built and
        written when the tracker is used as a context manager -- so an
        exception raised anywhere inside the `with` block, even after a
        premature manual finish() call, is still reflected correctly here.
        """
        self._context_manager_active = False

        try:
            self._finalize(exc_type, exc_val, exc_tb)

        except Exception as tracker_error:
            self._tracking_error = sanitize_error_message(tracker_error)

            emit_tracking_log(
                level="ERROR",
                event="TRACKING_EXIT_FAILED",
                log_obj=self.logger,
                run_id=getattr(
                    getattr(self, "context", None), "run_id", None
                ),
                table=self.table_name,
                error_type=type(tracker_error).__name__,
                error_message=self._tracking_error,
            )

            if self.raise_on_failure and exc_type is None:
                raise

        return False


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def run_tracking(
    spark=None,
    snowflake_options: Optional[Dict[str, str]] = None,
    pipeline_name: str = "UNNAMED_PIPELINE",
    environment: str = "PROD",
    table_name: str = "PRD_MDP.MDP_STG.PIPELINE_RUNS",
    dbutils=None,
    **kwargs,
) -> RunTracker:
    """Create and return a RunTracker instance."""
    return RunTracker(
        spark=spark,
        snowflake_options=snowflake_options,
        pipeline_name=pipeline_name,
        environment=environment,
        table_name=table_name,
        dbutils=dbutils,
        **kwargs,
    )
