"""
Pure utility functions — no Spark, Snowflake, or Databricks dependencies.

All functions here are safe to import and unit-test without a live cluster.
"""

import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from ._constants import (
    _MAX_ERROR_LENGTH,
    _PEM_BLOCK_PATTERN,
    _AUTHORIZATION_QUOTED_VALUE,
    _AUTHORIZATION_BARE_VALUE,
    _BEARER_TOKEN_PATTERN,
    _SENSITIVE_QUOTED_ASSIGNMENT,
    _SENSITIVE_BARE_ASSIGNMENT,
    logger,
)


# ---------------------------------------------------------------------------
# Core utilities
# ---------------------------------------------------------------------------

def utc_now() -> datetime:
    """Return the current UTC datetime with timezone information."""
    return datetime.now(timezone.utc)


def parse_optional_int(value: object) -> Optional[int]:
    """
    Parse an optional integer.

    Returns None for missing values, empty strings, invalid integers, or
    unexpanded Databricks dynamic value references (e.g. "{{task.name}}").
    """
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
    """
    Convert a numeric value into a Spark LongType-compatible Python integer.
    """
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
    """
    Parse an ISO-formatted datetime string into a UTC-aware datetime.
    """
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
    """
    Mask credential-like values and truncate error messages.

    Handles, in order:
    1. PEM private-key / certificate blocks -- redacted as a whole BEFORE
       newlines are flattened, so multi-line key material can never survive
       simply because it did not match a single-line pattern later.
    2. Newline flattening (for readable, single-line log output).
    3. Authorization headers with quoted values, key optionally quoted,
       e.g. {"Authorization": "Basic Zm9v"} -- quotes are preserved, only
       the inner value is redacted.
    4. Authorization headers with unquoted values, e.g.
       "Authorization: Bearer XYZ".
    5. Standalone "Bearer <token>" occurrences not preceded by
       "Authorization".
    6. Quoted assignments for sensitive keys, key optionally quoted, e.g.
       password="two word secret", password="abc\\"def" (escaped quote),
       or {"password": "EXAMPLE_SECRET"}. Escape-aware, so an escaped quote
       inside the value does not truncate the match and leak the remainder.
    7. Unquoted single-token assignments, e.g. token=abc123.
    """
    if value is None:
        return None

    text = str(value)

    # Step 1: redact PEM blocks first, while newlines are still intact.
    text = _PEM_BLOCK_PATTERN.sub("[REDACTED PRIVATE KEY]", text)

    # Step 2: flatten newlines for single-line log output.
    text = text.replace("\n", " ").replace("\r", " ")

    # Step 3: Authorization with a quoted value (preserve quotes).
    text = _AUTHORIZATION_QUOTED_VALUE.sub(
        lambda m: (
            f"{m.group('prefix')}{m.group('quote')}"
            f"[REDACTED]{m.group('quote')}"
        ),
        text,
    )

    # Step 4: Authorization with an unquoted value.
    text = _AUTHORIZATION_BARE_VALUE.sub(
        lambda m: f"{m.group('prefix')}[REDACTED]",
        text,
    )

    # Step 5: standalone Bearer tokens not caught above.
    text = _BEARER_TOKEN_PATTERN.sub(
        lambda m: f"{m.group('prefix')}[REDACTED]",
        text,
    )

    # Step 6: quoted sensitive-key assignments (escape-aware).
    text = _SENSITIVE_QUOTED_ASSIGNMENT.sub(
        lambda m: (
            f"{m.group('prefix')}{m.group('quote')}"
            f"[REDACTED]{m.group('quote')}"
        ),
        text,
    )

    # Step 7: unquoted sensitive-key assignments.
    text = _SENSITIVE_BARE_ASSIGNMENT.sub(
        lambda m: f"{m.group('prefix')}[REDACTED]",
        text,
    )

    return text[:_MAX_ERROR_LENGTH]


def emit_tracking_log(
    level: str,
    event: str,
    log_obj: Optional[logging.Logger] = None,
    **details: Any,
) -> None:
    """
    Emit a concise tracking event to both Python logging and print().

    print() is included because Databricks task output does not always
    display Python logger messages, depending on the logging configuration.
    Values passed through details are sanitized before being emitted.
    """
    active_logger = log_obj or logger

    detail_parts = []

    for key, detail_value in details.items():
        if detail_value is None:
            continue

        sanitized_value = sanitize_error_message(detail_value)

        if sanitized_value is not None:
            detail_parts.append(f"{key}={sanitized_value}")

    message = event

    if detail_parts:
        message = f"{event} | {' | '.join(detail_parts)}"

    normalized_level = str(level).strip().upper()

    if normalized_level == "ERROR":
        active_logger.error(message)
    elif normalized_level == "WARNING":
        active_logger.warning(message)
    else:
        active_logger.info(message)

    print(message)
