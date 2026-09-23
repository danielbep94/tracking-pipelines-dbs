import base64
import unittest

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from conf.credentials import get_snowflake_options
from conf.settings import (
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_ROLE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_SECRET_SCOPE,
    SNOWFLAKE_URL,
    SNOWFLAKE_WAREHOUSE,
)


class FakeSecrets:
    def __init__(self, values):
        self.values = values
        self.requests = []

    def get(self, scope, key):
        self.requests.append((scope, key))
        return self.values[key]


class FakeDbutils:
    def __init__(self, values):
        self.secrets = FakeSecrets(values)


def generate_private_key_pem(key_size=2048):
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=key_size,
    )
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")


class SnowflakeCredentialsTests(unittest.TestCase):
    def test_builds_key_pair_connector_options(self):
        dbutils = FakeDbutils(
            {
                "snowflake-user": "SERVICE_USER",
                "Snowflake-Private-Key": generate_private_key_pem(),
            }
        )
        options = get_snowflake_options(dbutils)

        self.assertEqual(options["sfUser"], "SERVICE_USER")
        self.assertNotIn("sfPassword", options)
        self.assertEqual(options["sfURL"], SNOWFLAKE_URL)
        self.assertEqual(options["sfDatabase"], SNOWFLAKE_DATABASE)
        self.assertEqual(options["sfSchema"], SNOWFLAKE_SCHEMA)
        self.assertEqual(options["sfWarehouse"], SNOWFLAKE_WAREHOUSE)
        self.assertEqual(options["sfRole"], SNOWFLAKE_ROLE)

        parsed_key = serialization.load_der_private_key(
            base64.b64decode(options["pem_private_key"]),
            password=None,
        )
        self.assertIsInstance(parsed_key, rsa.RSAPrivateKey)
        self.assertGreaterEqual(parsed_key.key_size, 2048)
        self.assertEqual(
            dbutils.secrets.requests,
            [
                (SNOWFLAKE_SECRET_SCOPE, "snowflake-user"),
                (SNOWFLAKE_SECRET_SCOPE, "Snowflake-Private-Key"),
            ],
        )

    def test_rejects_invalid_private_key(self):
        dbutils = FakeDbutils(
            {
                "snowflake-user": "SERVICE_USER",
                "Snowflake-Private-Key": "not-a-private-key",
            }
        )
        with self.assertRaisesRegex(ValueError, "valid, unencrypted PEM"):
            get_snowflake_options(dbutils)

    def test_rejects_rsa_keys_smaller_than_2048_bits(self):
        dbutils = FakeDbutils(
            {
                "snowflake-user": "SERVICE_USER",
                "Snowflake-Private-Key": generate_private_key_pem(1024),
            }
        )
        with self.assertRaisesRegex(ValueError, "at least 2048 bits"):
            get_snowflake_options(dbutils)


if __name__ == "__main__":
    unittest.main()
