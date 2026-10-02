"""
Shared constants, logging instance, and credential-sanitization patterns.

Nothing in this module has external side-effects; it is safe to import
from any other module in the package without risk of circular imports.
"""

import logging
import re

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MAX_ERROR_LENGTH = 4000

logger = logging.getLogger("run_tracking")

# ---------------------------------------------------------------------------
# Credential sanitization patterns
# ---------------------------------------------------------------------------

_SENSITIVE_KEY_NAMES = (
    r"password",
    r"passwd",
    r"pwd",
    r"token",
    r"secret",
    r"private[_-]?key",
    r"access[_-]?key",
    r"api[_-]?key",
    r"client[_-]?secret",
)

_SENSITIVE_KEY_GROUP = "(?:" + "|".join(_SENSITIVE_KEY_NAMES) + ")"

# PEM key/certificate blocks (private keys, RSA keys, certificates, etc.).
# Matched with DOTALL so the block is redacted as a whole BEFORE newlines
# are flattened elsewhere in sanitize_error_message -- flattening first would
# still allow this pattern to match across the now-single-line text, but
# redacting first guarantees no key material can leak even if a future
# change reorders these steps.
_PEM_BLOCK_PATTERN = re.compile(
    r"-----BEGIN [A-Z0-9 ]*(?:PRIVATE KEY|CERTIFICATE)-----"
    r".*?"
    r"-----END [A-Z0-9 ]*(?:PRIVATE KEY|CERTIFICATE)-----",
    re.DOTALL,
)

# Authorization header/field with a QUOTED value, key optionally quoted:
#   "Authorization": "Basic Zm9v"
#   Authorization='Bearer abc123'
# The surrounding quotes are preserved; only the inner value is redacted, so
# the output remains readable/valid-looking JSON where applicable. The value
# body is escape-aware so an escaped quote inside it does not end the match
# prematurely.
_AUTHORIZATION_QUOTED_VALUE = re.compile(
    r'(?i)(?P<prefix>"?\bauthorization\b"?\s*[:=]\s*)'
    r'(?P<quote>["\'])(?P<value>(?:\\.|(?!(?P=quote)).)*)(?P=quote)'
)

# Authorization header/field with an UNQUOTED value, e.g.:
#   Authorization: Bearer XYZ
#   authorization=abc.def-ghi
# Captures up to two space-separated tokens so a "<scheme> <token>" pair
# (e.g. "Bearer XYZ") is redacted as a whole.
_AUTHORIZATION_BARE_VALUE = re.compile(
    r'(?i)(?P<prefix>"?\bauthorization\b"?\s*[:=]\s*)'
    r'(?P<value>[^\s,;"\']+(?:\s+[^\s,;"\']+)?)'
)

# Standalone "Bearer <token>" occurrences not preceded by "Authorization".
_BEARER_TOKEN_PATTERN = re.compile(
    r"(?i)(?P<prefix>\bBearer\s+)(?P<value>[A-Za-z0-9\-_.~+/]+=*)"
)

# Quoted values following a sensitive key, key optionally quoted, e.g.:
#   password="two word secret"
#   password="abc\"def"          (escaped quote inside the value)
#   "password": "EXAMPLE_SECRET"
#   secret: 'value with spaces'
# The escape-aware value body ensures an escaped quote inside the secret does
# not terminate the match prematurely and leak the remainder of the value.
_SENSITIVE_QUOTED_ASSIGNMENT = re.compile(
    r'(?i)(?P<prefix>"?' + _SENSITIVE_KEY_GROUP + r'"?\s*[:=]\s*)'
    r'(?P<quote>["\'])(?P<value>(?:\\.|(?!(?P=quote)).)*)(?P=quote)'
)

# Unquoted single-token values following a sensitive key, key optionally
# quoted, e.g.:
#   token=abc123
#   pwd: hunter2
#   "pwd"=hunter2
_SENSITIVE_BARE_ASSIGNMENT = re.compile(
    r'(?i)(?P<prefix>"?\b' + _SENSITIVE_KEY_GROUP + r'\b"?\s*[:=]\s*)'
    r"(?P<value>[^,;\s\"']+)"
)
