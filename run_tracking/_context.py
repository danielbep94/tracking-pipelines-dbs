"""Run metadata model and PIPELINE_RUNS record assembly."""

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, Optional
from uuid import uuid4

from ._json import metrics_to_json
from ._utils import normalize_optional_long, sanitize_error_message, utc_now


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
