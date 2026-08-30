from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

REDACTED = "<redacted>"

_SECRET_KEY = re.compile(
    r"(?:password|passwd|token|secret|private[_-]?key|passphrase|credential)",
    re.IGNORECASE,
)
_CONNECTION_USERINFO = re.compile(r"(?P<scheme>[a-z][a-z0-9+.-]*://)[^/@\s]+@", re.I)


def is_secret_key(key: str) -> bool:
    return bool(_SECRET_KEY.search(key))


def redact(value: Any, *, key: str | None = None) -> Any:
    if key is not None and is_secret_key(key):
        return REDACTED
    if isinstance(value, Mapping):
        return {str(k): redact(v, key=str(k)) for k, v in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [redact(item) for item in value]
    if isinstance(value, str):
        return _CONNECTION_USERINFO.sub(r"\g<scheme><redacted>@", value)
    return value

