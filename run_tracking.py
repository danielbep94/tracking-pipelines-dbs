"""
Reusable append-only pipeline run tracking for Databricks and Snowflake.

This standalone module provides run context management, metrics tracking,
lightweight DataFrame profiling, lightweight DQ checks, runtime metadata
collection, and telemetry logging to Snowflake tables
(e.g. PRD_MDP.MDP_STG.PIPELINE_RUNS).

Design principles
-----------------
* One Databricks job execution  ->  one row in PIPELINE_RUNS.
* BUSINESS_METRICS_JSON is the single extensibility point for all additional
  profiling metrics, reconciliation values, DQ summaries, and business KPIs.
* No additional Snowflake tables are introduced.
* All public names and call signatures remain backward-compatible.
"""

import base64
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
import json
import logging
import re
from typing import Any, Dict, List, Optional
from uuid import uuid4

_MAX_ERROR_LENGTH = 4000
_SENSITIVE_ASSIGNMENT = re.compile(
    r"(?i)(password|passwd|pwd|token|secret|private[_-]?key|access[_-]?key)"
    r"(\s*[:=]\s*)([^,;\s]+)"
)

logger = logging.getLogger("run_tracking")


# ---------------------------------------------------------------------------
# Core utilities
# ---------------------------------------------------------------------------

def utc_now() -> datetime:
    """Return current UTC datetime with timezone info."""
    return datetime.now(timezone.utc)


def parse_optional_int(value: object) -> Optional[int]:
    """Parse an optional integer, returning None for empty/unexpanded template strings."""
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized or normalized.startswith("{{"):
        return None
    try:
        return int(normalized)
    except (TypeError, ValueError):
        return None


def normalize_optional_long(value: object) -> Optional[int]:
    """Convert Snowflake NUMBER/Decimal counts into Spark LongType values."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError("Boolean values are not valid row counts.")
    try:
        numeric_value = Decimal(str(value))
    except (ArithmeticError, TypeError, ValueError) as exc:
        raise TypeError(f"Row count is not numeric: {value!r}") from exc
    if not numeric_value.is_finite():
        raise ValueError(f"Row count must be finite: {value!r}")
    if numeric_value != numeric_value.to_integral_value():
        raise ValueError(f"Row count must be an integer: {value!r}")
    normalized = int(numeric_value)
    if not -(2**63) <= normalized <= (2**63 - 1):
        raise OverflowError(f"Row count exceeds Spark LongType: {value!r}")
    return normalized


def parse_utc_datetime(value: object) -> Optional[datetime]:
    """Parse ISO formatted datetime string into a UTC timezone-aware datetime."""
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized or normalized.startswith("{{"):
        return None
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def sanitize_error_message(value: object) -> Optional[str]:
    """Sanitize error messages by masking credentials and truncating to max length."""
    if value is None:
        return None
    text = str(value).replace("\n", " ").replace("\r", " ")
    text = _SENSITIVE_ASSIGNMENT.sub(r"\1\2[REDACTED]", text)
    return text[:_MAX_ERROR_LENGTH]


def _json_default(value: object) -> object:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def metrics_to_json(metrics: Optional[Dict[str, Any]]) -> Optional[str]:
    """Serialize business metrics dictionary to a compact JSON string."""
    if not metrics:
        return None
    return json.dumps(
        metrics,
        default=_json_default,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


# ---------------------------------------------------------------------------
# Databricks / Snowflake helpers
# ---------------------------------------------------------------------------

def get_widget_value(dbutils, name: str, default: Optional[str] = None) -> Optional[str]:
    """Safely fetch a Databricks widget parameter if available."""
    if dbutils is None:
        return default
    try:
        value = dbutils.widgets.get(name)
    except Exception:
        return default
    normalized = str(value).strip()
    return normalized if normalized else default


def get_databricks_context_tag(dbutils, tag_name: str) -> Optional[str]:
    """Safely extract Databricks runtime context tags when widgets are not configured."""
    if dbutils is None:
        return None
    try:
        context_json = (
            dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
        )
        context_data = json.loads(context_json)
        tags = context_data.get("tags", {})
        val = tags.get(tag_name)
        if val is not None:
            normalized = str(val).strip()
            return normalized if normalized else None
    except Exception:
        pass
    return None


def _collect_runtime_metadata(spark=None, dbutils=None) -> Dict[str, Any]:
    """
    Collect Databricks/Spark runtime metadata for inclusion in BUSINESS_METRICS_JSON.

    All keys are prefixed with ``runtime_`` so they never clash with user-defined
    metrics.  Returns an empty dict if metadata cannot be read.
    """
    meta: Dict[str, Any] = {}
    try:
        if spark is not None:
            try:
                meta["runtime_spark_version"] = spark.version
            except Exception:
                pass
            try:
                meta["runtime_cluster_id"] = spark.conf.get(
                    "spark.databricks.clusterUsageTags.clusterId", None
                )
            except Exception:
                pass

        if dbutils is not None:
            try:
                context_json = (
                    dbutils.notebook.entry_point
                    .getDbutils().notebook().getContext().toJson()
                )
                ctx = json.loads(context_json)
                tags = ctx.get("tags", {})
                extra = ctx.get("extraContext", {})
                for src_key, dst_key in (
                    ("notebookPath",    "runtime_notebook_path"),
                    ("gitCommit",       "runtime_git_sha"),
                    ("browserHostName", "runtime_host"),
                ):
                    val = tags.get(src_key) or extra.get(src_key)
                    if val:
                        meta[dst_key] = str(val).strip()
            except Exception:
                pass
    except Exception:
        pass

    return {k: v for k, v in meta.items() if v is not None}


def _encode_private_key_for_spark(private_key_pem: str) -> str:
    """Return an unencrypted PKCS#8 DER key encoded for the Spark Snowflake connector."""
    try:
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
    except ImportError as exc:
        raise ImportError(
            "The 'cryptography' package is required to encode Snowflake private keys."
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
            "Snowflake private key must contain a valid, unencrypted PEM private key."
        ) from exc

    if not isinstance(private_key, rsa.RSAPrivateKey):
        raise TypeError("Snowflake key-pair authentication requires an RSA key.")
    if private_key.key_size < 2048:
        raise ValueError("Snowflake RSA private key must be at least 2048 bits.")

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
    """
    Fetch Snowflake authentication secrets via dbutils and return connector options.

    All scope names, secret keys, and database parameters can be customized per call.
    """
    if dbutils is None:
        raise ValueError("dbutils is required to obtain Snowflake credentials.")

    snowflake_user = dbutils.secrets.get(scope=secret_scope, key=user_key)
    private_key_pem = dbutils.secrets.get(scope=secret_scope, key=private_key_secret)

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


def tag_snowflake_session(
    spark,
    snowflake_options: Dict[str, str],
    run_id: str,
    pipeline_name: str,
    pipeline_version: Optional[str] = None,
) -> None:
    """
    Tag the active Snowflake session with RUN_ID, PIPELINE_NAME, and PIPELINE_VERSION
    for Snowflake QUERY_HISTORY auditing.

    Best-effort: any failure is logged and silently suppressed so it never
    interrupts business execution.

    Example::

        tag_snowflake_session(
            spark=spark,
            snowflake_options=sf_options,
            run_id=tracker.context.run_id,
            pipeline_name="FACT_SALES_OTC",
            pipeline_version="1.2.0",
        )
    """
    try:
        tag_payload = json.dumps(
            {
                "run_id": run_id,
                "pipeline_name": pipeline_name,
                "pipeline_version": pipeline_version or "unversioned",
            },
            separators=(",", ":"),
        )
        query = f"ALTER SESSION SET QUERY_TAG = '{tag_payload}'"
        (
            spark.read.format("net.snowflake.spark.snowflake")
            .options(**snowflake_options)
            .option("query", query)
            .load()
        )
    except Exception as tag_err:
        logger.warning(
            "SNOWFLAKE_TAG_FAILED | %s: %s",
            type(tag_err).__name__,
            sanitize_error_message(tag_err),
        )


# ---------------------------------------------------------------------------
# RunContext
# ---------------------------------------------------------------------------

@dataclass
class RunContext:
    """Encapsulates execution metadata for a single pipeline run."""

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
        """Construct a complete pipeline run record dictionary matching PIPELINE_RUNS schema."""
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
# Snowflake write helpers
# ---------------------------------------------------------------------------

def read_snowflake_metrics(
    spark,
    snowflake_options: Dict[str, str],
    query: str,
) -> Dict[str, Any]:
    """Execute a SELECT query against Snowflake through Spark connector and return row dict."""
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
    """Append one run history record to Snowflake target table."""
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
    (
        dataframe.write.format("net.snowflake.spark.snowflake")
        .options(**snowflake_options)
        .option("dbtable", table_name)
        .mode("append")
        .save()
    )


def append_run_record_safely(
    spark,
    snowflake_options: Dict[str, str],
    table_name: str,
    record: Dict[str, Any],
    log_obj: Optional[logging.Logger] = None,
) -> bool:
    """Write telemetry safely without letting tracking failures disrupt business execution."""
    active_logger = log_obj or logger
    try:
        append_run_record(
            spark=spark,
            snowflake_options=snowflake_options,
            table_name=table_name,
            record=record,
        )
        active_logger.info(
            "Pipeline telemetry recorded: run_id=%s status=%s",
            record["RUN_ID"],
            record["STATUS"],
        )
        return True
    except Exception as tracking_error:
        active_logger.error(
            "TRACKING_WRITE_FAILED | %s: %s",
            type(tracking_error).__name__,
            sanitize_error_message(tracking_error),
        )
        return False


# ---------------------------------------------------------------------------
# RunTracker
# ---------------------------------------------------------------------------

class RunTracker:
    """
    Context manager for effortless run tracking in Databricks notebooks.

    Design
    ------
    * One Databricks job execution produces exactly one row in PIPELINE_RUNS.
    * Core execution details (RUN_ID, timestamps, duration, status, errors) are
      captured automatically.
    * Row count fields default to None (NULL in Snowflake) to avoid unnecessary
      PySpark .count() overhead.  Assign them only when business requires it.
    * All additional metrics, profiling results, DQ summaries, and runtime
      metadata are stored inside BUSINESS_METRICS_JSON.

    Usage (explicit finish)::

        tracker = run_tracking(
            spark=spark,
            snowflake_options=sf_opts,
            pipeline_name="FACT_SALES_OTC",
            dbutils=dbutils,
        )

        # pipeline logic ...

        # Optional: lightweight profiling
        profile = tracker.profile(df_source, sum_columns=["KILOS"], key_columns=["ID"])
        tracker.add_metrics(profile)

        # Optional: DQ checks
        tracker.check(df_target.filter("KILOS < 0").count() == 0, "no_negative_kilos")
        tracker.check(df_target.filter("KILOS IS NULL").count() == 0, "no_null_kilos")

        tracker.finish()

    Usage (context manager)::

        with run_tracking(spark=spark, ...) as tracker:
            # pipeline logic ...
            pass
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
    ):
        self.spark = spark
        self.snowflake_options = snowflake_options
        self.pipeline_name = pipeline_name
        self.environment = environment
        self.table_name = table_name
        self.dbutils = dbutils
        self.enabled = enabled
        self.logger = log_obj or logger

        self.source_name = source_name
        self.source_file = source_file
        self.target_name = target_name
        self.period_start_date = period_start_date
        self.period_end_date = period_end_date

        # Row count metrics -- default NULL; assign only when business requires it
        self.source_rows: Optional[int] = None
        self.staging_rows: Optional[int] = None
        self.transformed_rows: Optional[int] = None
        self.target_rows_before: Optional[int] = None
        self.target_rows_after: Optional[int] = None

        # BUSINESS_METRICS_JSON payload (extensible key/value store)
        self.business_metrics: Dict[str, Any] = {}

        # DQ check accumulator
        self._dq_results: List[Dict[str, Any]] = []

        self.status: str = "SUCCEEDED"

        try:
            started_at = (
                parse_utc_datetime(get_widget_value(dbutils, "tracking_job_started_at_utc"))
                or utc_now()
            )
            version = pipeline_version or get_widget_value(
                dbutils, "tracking_pipeline_version", "workspace-unversioned"
            )
            job_id = get_widget_value(dbutils, "tracking_job_id") or get_databricks_context_tag(dbutils, "jobId")
            job_run_id = (
                get_widget_value(dbutils, "tracking_job_run_id")
                or get_databricks_context_tag(dbutils, "multitaskParentRunId")
                or get_databricks_context_tag(dbutils, "jobRunId")
                or get_databricks_context_tag(dbutils, "idInJob")
            )
            task_run_id = (
                get_widget_value(dbutils, "tracking_task_run_id")
                or get_databricks_context_tag(dbutils, "taskRunId")
            )
            task_name = (
                get_widget_value(dbutils, "tracking_task_name")
                or get_databricks_context_tag(dbutils, "taskKey")
                or pipeline_name
            )
            attempt_number = parse_optional_int(
                get_widget_value(dbutils, "tracking_attempt_number")
            )
            trigger_type = get_widget_value(
                dbutils, "tracking_trigger_type", "one_time"
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

            # Auto-collect runtime metadata into business_metrics
            if collect_runtime_metadata:
                runtime_meta = _collect_runtime_metadata(spark=spark, dbutils=dbutils)
                if runtime_meta:
                    self.business_metrics.update(runtime_meta)

        except Exception as init_err:
            self.logger.error(
                "TRACKING_INIT_FAILED | %s: %s. Operational telemetry disabled.",
                type(init_err).__name__,
                sanitize_error_message(init_err),
            )
            self.enabled = False

    # ------------------------------------------------------------------
    # Metric helpers
    # ------------------------------------------------------------------

    def set_metrics(self, **metrics: Any) -> None:
        """
        Add or overwrite individual keys in the BUSINESS_METRICS_JSON payload.

        Example::

            tracker.set_metrics(reconciliation_status="PASS", duplicate_rows=0)
        """
        try:
            self.business_metrics.update(metrics)
        except Exception as err:
            self.logger.warning("Failed to set metrics: %s", err)

    def add_metrics(self, metrics: Dict[str, Any]) -> None:
        """
        Merge a dictionary of metrics into the BUSINESS_METRICS_JSON payload.

        Use this to merge profiling results returned by profile().

        Example::

            profile_result = tracker.profile(df, sum_columns=["KILOS"])
            tracker.add_metrics(profile_result)
        """
        try:
            if metrics:
                self.business_metrics.update(metrics)
        except Exception as err:
            self.logger.warning("Failed to add metrics: %s", err)

    # ------------------------------------------------------------------
    # Lightweight DataFrame profiling
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
        them as a flat dictionary ready to merge into BUSINESS_METRICS_JSON.

        Metrics are RETURNED, not automatically stored. Call add_metrics() to merge.

        Parameters
        ----------
        df          : PySpark DataFrame to profile.
        prefix      : Optional prefix applied to every key (e.g. "source_").
        count       : Include row_count (default True).
        distinct_columns : Columns for {col}_distinct_count.
        key_columns : Columns used to detect duplicates -> duplicate_count.
        null_columns: Columns for {col}_null_count and {col}_null_rate.
        sum_columns : Columns for {col}_sum.
        min_columns : Columns for {col}_min.
        max_columns : Columns for {col}_max.
        avg_columns : Columns for {col}_avg.

        Returns
        -------
        Dict[str, Any] of profiling metrics.

        Example::

            profile = tracker.profile(
                df_source,
                prefix="source_",
                sum_columns=["KILOS"],
                key_columns=["ORDER_ID"],
                null_columns=["KILOS"],
            )
            tracker.add_metrics(profile)
        """
        result: Dict[str, Any] = {}
        p = prefix

        try:
            from pyspark.sql import functions as F

            agg_exprs = []

            if count:
                agg_exprs.append(F.count("*").alias("__row_count"))

            for col in (distinct_columns or []):
                agg_exprs.append(F.countDistinct(col).alias(f"__distinct__{col}"))

            for col in (null_columns or []):
                agg_exprs.append(
                    F.sum(F.col(col).isNull().cast("int")).alias(f"__null__{col}")
                )

            for col in (sum_columns or []):
                agg_exprs.append(F.sum(col).alias(f"__sum__{col}"))

            for col in (min_columns or []):
                agg_exprs.append(F.min(col).alias(f"__min__{col}"))

            for col in (max_columns or []):
                agg_exprs.append(F.max(col).alias(f"__max__{col}"))

            for col in (avg_columns or []):
                agg_exprs.append(F.avg(col).alias(f"__avg__{col}"))

            if agg_exprs:
                row = df.agg(*agg_exprs).first()
                if row:
                    row_dict = row.asDict()

                    if count:
                        result[f"{p}row_count"] = row_dict.get("__row_count")

                    for col in (distinct_columns or []):
                        result[f"{p}{col.lower()}_distinct_count"] = row_dict.get(f"__distinct__{col}")

                    total_rows = result.get(f"{p}row_count") or 0
                    for col in (null_columns or []):
                        null_ct = row_dict.get(f"__null__{col}")
                        result[f"{p}{col.lower()}_null_count"] = null_ct
                        if total_rows and null_ct is not None:
                            result[f"{p}{col.lower()}_null_rate"] = round(null_ct / total_rows, 6)
                        else:
                            result[f"{p}{col.lower()}_null_rate"] = None

                    for col in (sum_columns or []):
                        result[f"{p}{col.lower()}_sum"] = row_dict.get(f"__sum__{col}")

                    for col in (min_columns or []):
                        result[f"{p}{col.lower()}_min"] = row_dict.get(f"__min__{col}")

                    for col in (max_columns or []):
                        result[f"{p}{col.lower()}_max"] = row_dict.get(f"__max__{col}")

                    for col in (avg_columns or []):
                        val = row_dict.get(f"__avg__{col}")
                        result[f"{p}{col.lower()}_avg"] = (
                            round(float(val), 6) if val is not None else None
                        )

            # Duplicate count requires a separate groupBy action
            if key_columns:
                dup_count = (
                    df.groupBy(*key_columns)
                    .count()
                    .filter(F.col("count") > 1)
                    .count()
                )
                result[f"{p}duplicate_count"] = dup_count

        except Exception as profile_err:
            self.logger.warning(
                "PROFILE_FAILED | %s: %s",
                type(profile_err).__name__,
                sanitize_error_message(profile_err),
            )

        return result

    # ------------------------------------------------------------------
    # Lightweight DQ framework
    # ------------------------------------------------------------------

    def check(self, condition: bool, name: str) -> bool:
        """
        Register a single DQ check result.

        Results are accumulated and written as aggregate counts into
        BUSINESS_METRICS_JSON (dq_checks_total, dq_checks_passed,
        dq_checks_failed) automatically at finish() time.

        Parameters
        ----------
        condition : Boolean result of the check. True = passed.
        name      : Human-readable check name (used for logging only).

        Returns
        -------
        bool -- the value of condition, so callers can branch on it.

        Example::

            tracker.check(df.filter("KILOS < 0").count() == 0, "no_negative_kilos")
            tracker.check(df.filter("ID IS NULL").count() == 0, "no_null_ids")
        """
        passed = bool(condition)
        self._dq_results.append({"name": name, "passed": passed})
        if not passed:
            self.logger.warning("DQ_CHECK_FAILED | check=%s", name)
        return passed

    def _flush_dq_summary(self) -> None:
        """Merge DQ aggregate counts into business_metrics if any checks were registered."""
        if not self._dq_results:
            return
        total = len(self._dq_results)
        passed = sum(1 for r in self._dq_results if r["passed"])
        self.business_metrics.update(
            {
                "dq_checks_total": total,
                "dq_checks_passed": passed,
                "dq_checks_failed": total - passed,
            }
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def finish(self, status: Optional[str] = None) -> bool:
        """
        Safely complete and log the pipeline telemetry record.
        Use this at the end of a notebook when not using a 'with' context block.
        """
        try:
            if status:
                self.status = status
            self.__exit__(None, None, None)
            return True
        except Exception as finish_err:
            self.logger.error(
                "TRACKING_FINISH_FAILED | %s: %s",
                type(finish_err).__name__,
                sanitize_error_message(finish_err),
            )
            return False

    def complete(self, status: Optional[str] = None) -> bool:
        """Alias for finish()."""
        return self.finish(status)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.enabled:
            return False

        try:
            if exc_type is not None:
                self.status = "FAILED"
                error_type = exc_type.__name__
                error_message = str(exc_val)
            else:
                error_type = None
                error_message = None

            # Flush DQ summary into business_metrics before building the record
            self._flush_dq_summary()

            record = self.context.build_record(
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
                business_metrics=self.business_metrics if self.business_metrics else None,
                error_type=error_type,
                error_message=error_message,
            )

            if self.spark is not None and self.snowflake_options is not None:
                append_run_record_safely(
                    spark=self.spark,
                    snowflake_options=self.snowflake_options,
                    table_name=self.table_name,
                    record=record,
                    log_obj=self.logger,
                )
        except Exception as tracker_error:
            self.logger.error(
                "TRACKING_EXIT_FAILED | %s: %s",
                type(tracker_error).__name__,
                sanitize_error_message(tracker_error),
            )

        # Return False to let Python propagate business exceptions normally
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
    """Helper factory function to create a RunTracker context manager."""
    return RunTracker(
        spark=spark,
        snowflake_options=snowflake_options,
        pipeline_name=pipeline_name,
        environment=environment,
        table_name=table_name,
        dbutils=dbutils,
        **kwargs,
    )
