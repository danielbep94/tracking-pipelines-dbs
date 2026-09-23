"""Snowflake key-pair credentials loaded from Databricks secrets."""

import base64
from typing import Dict

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from conf.settings import (
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_ROLE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_SECRET_KEYS,
    SNOWFLAKE_SECRET_SCOPE,
    SNOWFLAKE_URL,
    SNOWFLAKE_WAREHOUSE,
)


def _encode_private_key_for_spark(private_key_pem: str) -> str:
    """Return an unencrypted PKCS#8 DER key encoded for the Spark connector."""
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
            "Snowflake-Private-Key must contain a valid, unencrypted PEM "
            "private key."
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


def get_snowflake_options(dbutils) -> Dict[str, str]:
    """Return Spark connector key-pair options without exposing secrets."""
    if dbutils is None:
        raise ValueError("dbutils is required to obtain Snowflake credentials.")

    snowflake_user = dbutils.secrets.get(
        scope=SNOWFLAKE_SECRET_SCOPE,
        key=SNOWFLAKE_SECRET_KEYS["sfUser"],
    )
    private_key_pem = dbutils.secrets.get(
        scope=SNOWFLAKE_SECRET_SCOPE,
        key=SNOWFLAKE_SECRET_KEYS["pem_private_key"],
    )

    if not str(snowflake_user).strip():
        raise ValueError("The Snowflake username secret is empty.")
    if not str(private_key_pem).strip():
        raise ValueError("The Snowflake private-key secret is empty.")

    return {
        "sfURL": SNOWFLAKE_URL,
        "sfUser": snowflake_user,
        "pem_private_key": _encode_private_key_for_spark(private_key_pem),
        "sfDatabase": SNOWFLAKE_DATABASE,
        "sfSchema": SNOWFLAKE_SCHEMA,
        "sfWarehouse": SNOWFLAKE_WAREHOUSE,
        "sfRole": SNOWFLAKE_ROLE,
    }
