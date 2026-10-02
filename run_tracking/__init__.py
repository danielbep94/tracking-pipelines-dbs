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


from typing import Dict, Optional


# ---------------------------------------------------------------------------
# Sub-module imports (Phase 1: constants + utilities extracted)
# ---------------------------------------------------------------------------

from ._context import RunContext
from ._profiler import DataFrameProfiler
from ._dq import DQAccumulator
from ._tracker import RunTracker

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
# Sub-module imports (Phase 3: platform integrations extracted)
# ---------------------------------------------------------------------------

from ._databricks import (
    get_widget_value,
    get_databricks_context_tag,
    _collect_runtime_metadata,
)
from ._snowflake_auth import (
    _encode_private_key_for_spark,
    get_snowflake_options,
)
from ._snowflake_session import (
    build_query_tag_statement,
    tag_snowflake_session,
)
from ._snowflake_io import (
    read_snowflake_metrics,
    append_run_record,
    verify_run_record,
    append_run_record_safely,
)


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
