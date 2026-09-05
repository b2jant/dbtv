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
_INLINE_SECRET = re.compile(
    r"(?P<key>password|passwd|token|secret|private[_-]?key|passphrase|credential)"
    r"(?P<separator>[\"']?\s*[:=]\s*[\"']?)"
    r"(?P<value>[^\s,}\"']+)",
    re.IGNORECASE,
)


def is_secret_key(key: str) -> bool:
    return bool(_SECRET_KEY.search(key))


def redact(value: Any, *, key: str | None = None) -> Any:
    if key is not None and is_secret_key(key):
        return REDACTED
    if isinstance(value, Mapping):
        # These maps use dbt unique_ids as keys. A source called "tokens" is an
        # identity, not a credential field; retain its public digest.
        if key in {"snapshots", "source_scopes"} and all(
            isinstance(child, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", child)
            for child in value.values()
        ):
            return dict(value)
        if key == "outputs":
            return {
                str(k): redact(v) if isinstance(v, Mapping) else redact(v, key=str(k))
                for k, v in value.items()
            }
        return {str(k): redact(v, key=str(k)) for k, v in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [redact(item) for item in value]
    if isinstance(value, str):
        return redact_text(value)
    return value


def redact_text(value: str) -> str:
    value = _CONNECTION_USERINFO.sub(r"\g<scheme><redacted>@", value)
    return _INLINE_SECRET.sub(
        lambda match: f"{match.group('key')}{match.group('separator')}{REDACTED}",
        value,
    )
