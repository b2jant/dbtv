from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any, cast

from dbtv.core.redaction import redact


@dataclass(frozen=True)
class RunEvent:
    invocation_id: str
    name: str
    phase: str
    severity: str = "info"
    timestamp: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    attributes: dict[str, Any] = field(default_factory=dict)
    version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return cast(dict[str, Any], redact(asdict(self)))
