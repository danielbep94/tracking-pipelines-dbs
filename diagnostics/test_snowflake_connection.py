# Databricks notebook source
"""Read-only Snowflake connectivity diagnostic using production configuration."""

from time import perf_counter

from conf.credentials import get_snowflake_options
from conf.settings import (
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_ROLE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_SECRET_KEYS,
    SNOWFLAKE_SECRET_SCOPE,
    SNOWFLAKE_URL,
    SNOWFLAKE_WAREHOUSE,
)
from src.run_tracking import sanitize_error_message


def classify_connection_error(message: str) -> str:
    normalized = message.lower()
    if (
        "jwt token is invalid" in normalized
        or "public key fingerprint" in normalized
        or "key pair authentication" in normalized
    ):
        return "KEY_PAIR_AUTHENTICATION_FAILED"
    if "incorrect username or password" in normalized:
        return "AUTHENTICATION_FAILED"
    if "locked" in normalized or "disabled" in normalized:
        return "USER_LOCKED_OR_DISABLED"
    if (
        "insufficient privileges" in normalized
        or "not authorized" in normalized
        or "access control error" in normalized
    ):
        return "AUTHORIZATION_FAILED"
    if (
        "timed out" in normalized
        or "timeout" in normalized
        or "connection refused" in normalized
        or "unknown host" in normalized
        or "name or service not known" in normalized
    ):
        return "NETWORK_OR_ENDPOINT_FAILED"
    return "CONNECTION_FAILED"


print("=" * 72)
print("SNOWFLAKE CONNECTION TEST — READ ONLY")
print("=" * 72)
print(f"Secret scope: {SNOWFLAKE_SECRET_SCOPE}")
print(f"Secret keys configured: {sorted(SNOWFLAKE_SECRET_KEYS.values())}")
print(f"Snowflake URL: {SNOWFLAKE_URL}")
print(f"Database: {SNOWFLAKE_DATABASE}")
print(f"Schema: {SNOWFLAKE_SCHEMA}")
print(f"Warehouse: {SNOWFLAKE_WAREHOUSE}")
print(f"Role: {SNOWFLAKE_ROLE}")
print("Credential values: [NOT DISPLAYED]")

started = perf_counter()

try:
    options = get_snowflake_options(dbutils)
    missing_options = [
        name
        for name in ("sfUser", "pem_private_key")
        if not str(options.get(name, "")).strip()
    ]
    if missing_options:
        raise ValueError(
            "Required credential values are empty: " + ", ".join(missing_options)
        )

    result = (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**options)
        .option("query", "SELECT 1 AS CONNECTION_OK")
        .load()
        .first()
    )
    if result is None or int(result["CONNECTION_OK"]) != 1:
        raise RuntimeError("Snowflake returned an unexpected result for SELECT 1.")

    elapsed_seconds = perf_counter() - started
    print("-" * 72)
    print("RESULT: CONNECTION_SUCCESS")
    print(f"Elapsed seconds: {elapsed_seconds:.3f}")
    print("Authentication, endpoint, role, warehouse, database, and schema are usable.")
    print("No business data was read or modified.")
    print("=" * 72)

except Exception as connection_error:
    elapsed_seconds = perf_counter() - started
    sanitized_message = sanitize_error_message(connection_error)
    category = classify_connection_error(sanitized_message or "")
    print("-" * 72)
    print(f"RESULT: {category}")
    print(f"Elapsed seconds: {elapsed_seconds:.3f}")
    print(f"Sanitized error: {sanitized_message}")
    print("Credential values were not displayed.")
    print("=" * 72)
    raise RuntimeError(
        f"{category}: Snowflake connection test failed. "
        "Correct the credential or account state before running production."
    ) from None
