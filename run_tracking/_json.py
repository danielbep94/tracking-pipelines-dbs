"""
JSON serialization helpers for business metrics.

Provides nan/inf-safe normalization and the metrics_to_json() public API.
No Spark, Snowflake, or Databricks dependencies.
"""

import json
import math
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Optional

from ._constants import logger
from ._utils import sanitize_error_message


# ---------------------------------------------------------------------------
# JSON metrics serialization
# ---------------------------------------------------------------------------

def _normalize_metrics_value(value: object) -> object:
    """
    Recursively replace non-finite floats and Decimals (NaN, Infinity,
    -Infinity) with None so the resulting JSON is always standard-compliant.
    """
    if isinstance(value, dict):
        return {
            key: _normalize_metrics_value(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple)):
        return [_normalize_metrics_value(item) for item in value]

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value

    if isinstance(value, Decimal):
        if not value.is_finite():
            return None
        return value

    return value


def _json_default(value: object) -> object:
    """
    Convert non-JSON-native values into serializable values.

    Decimal values are converted to their exact string representation
    (rather than float) to avoid losing precision on financial metrics.
    """
    if isinstance(value, Decimal):
        return str(value)

    if isinstance(value, (date, datetime)):
        return value.isoformat()

    return str(value)


def metrics_to_json(metrics: Optional[Dict[str, Any]]) -> Optional[str]:
    """
    Serialize the business metrics dictionary into a compact, standard JSON
    string. Non-finite numeric values are normalized to null instead of
    being emitted as invalid JSON tokens (NaN, Infinity, -Infinity).
    """
    if not metrics:
        return None

    normalized_metrics = _normalize_metrics_value(metrics)

    try:
        return json.dumps(
            normalized_metrics,
            default=_json_default,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as serialization_error:
        logger.error(
            "METRICS_SERIALIZATION_FAILED | error_type=%s | error_message=%s",
            type(serialization_error).__name__,
            sanitize_error_message(serialization_error),
        )

        return json.dumps(
            {
                "metrics_serialization_error": sanitize_error_message(
                    serialization_error
                )
            }
        )
