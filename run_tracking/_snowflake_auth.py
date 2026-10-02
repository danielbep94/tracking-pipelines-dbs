"""Snowflake secret retrieval and Spark connector key-pair authentication."""

import base64
from typing import Dict


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
