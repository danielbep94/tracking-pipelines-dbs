"""Append-only pipeline run tracking for Databricks and Snowflake.

The package root re-exports the supported tracker, context, helper classes,
and utility functions. Implementations live in focused private submodules;
existing imports such as ``from run_tracking import run_tracking`` remain
available.
"""

from ._constants import logger
from ._context import RunContext
from ._databricks import (
    _collect_runtime_metadata,
    get_databricks_context_tag,
    get_widget_value,
)
from ._dq import DQAccumulator
from ._factory import run_tracking
from ._json import _json_default, _normalize_metrics_value, metrics_to_json
from ._profiler import DataFrameProfiler
from ._snowflake_auth import (
    _encode_private_key_for_spark,
    get_snowflake_options,
)
from ._snowflake_io import (
    append_run_record,
    append_run_record_safely,
    read_snowflake_metrics,
    verify_run_record,
)
from ._snowflake_session import (
    build_query_tag_statement,
    tag_snowflake_session,
)
from ._tracker import RunTracker
from ._utils import (
    emit_tracking_log,
    normalize_optional_long,
    parse_optional_int,
    parse_utc_datetime,
    sanitize_error_message,
    utc_now,
)
