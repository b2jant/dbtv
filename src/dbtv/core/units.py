from __future__ import annotations

import re
from datetime import timedelta

from dbtv.core.errors import ConfigError

_DURATION = re.compile(r"^(?P<value>\d+)(?P<unit>s|m|h|d|w)$", re.IGNORECASE)
_SIZE = re.compile(r"^(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>B|KB|MB|GB|TB)$", re.IGNORECASE)


def parse_duration(value: str) -> timedelta:
    match = _DURATION.fullmatch(value.strip())
    if not match:
        raise ConfigError(f"Invalid duration {value!r}; use values such as 30m, 24h, or 7d.")
    amount = int(match.group("value"))
    unit = match.group("unit").lower()
    seconds = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}[unit]
    return timedelta(seconds=amount * seconds)


def parse_size(value: str) -> int:
    match = _SIZE.fullmatch(value.strip())
    if not match:
        raise ConfigError(f"Invalid size {value!r}; use values such as 512MB, 8GB, or 1TB.")
    amount = float(match.group("value"))
    unit = match.group("unit").upper()
    multiplier = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}[unit]
    return int(amount * multiplier)
