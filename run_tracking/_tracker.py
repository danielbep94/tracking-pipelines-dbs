"""Run tracking lifecycle state machine."""

import logging
import sys
from datetime import date
from typing import Any, Dict, Optional

from ._constants import logger
from ._context import RunContext
from ._databricks import (
    _collect_runtime_metadata, get_databricks_context_tag, get_widget_value,
)
from ._dq import DQAccumulator
from ._profiler import DataFrameProfiler
from ._utils import (
    emit_tracking_log, normalize_optional_long, parse_optional_int,
    parse_utc_datetime, sanitize_error_message, utc_now,
)


def append_run_record(*args, **kwargs):
    return sys.modules["run_tracking"].append_run_record(*args, **kwargs)


def read_snowflake_metrics(*args, **kwargs):
    return sys.modules["run_tracking"].read_snowflake_metrics(*args, **kwargs)


def verify_run_record(*args, **kwargs):
    return sys.modules["run_tracking"].verify_run_record(*args, **kwargs)


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
        self._profiler = DataFrameProfiler(log_obj=log_obj or logger)
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
        self._dq = DQAccumulator(log_obj=self.logger)
        self._dq_results = self._dq.results

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
        self, df, *, prefix="", count=True, distinct_columns=None,
        key_columns=None, null_columns=None, sum_columns=None,
        min_columns=None, max_columns=None, avg_columns=None,
    ) -> Dict[str, Any]:
        """Calculate DataFrame profile metrics through the profiler helper."""
        return self._profiler.profile(
            df, prefix=prefix, count=count, distinct_columns=distinct_columns,
            key_columns=key_columns, null_columns=null_columns,
            sum_columns=sum_columns, min_columns=min_columns,
            max_columns=max_columns, avg_columns=avg_columns,
        )

    # ------------------------------------------------------------------
    # Data-quality checks
    # ------------------------------------------------------------------

    def check(self, condition: bool, name: str, required: bool = False) -> bool:
        """Register a DQ check and return its boolean result."""
        return self._dq.check(condition, name, required)

    def _flush_dq_summary(self) -> None:
        """Merge accumulated DQ metrics and apply required-check failures."""
        summary = self._dq.summary()
        if summary is None:
            return
        failed_required = summary.pop("failed_required_checks")
        self.business_metrics.update(summary)
        if failed_required and self.status != "FAILED":
            self.status = "FAILED"
            self._pending_error_type = "RequiredDataQualityCheckFailed"
            self._pending_error_message = (
                "Required data-quality checks failed: "
                + ", ".join(failed_required)
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
