from __future__ import annotations

import json
import threading
from collections.abc import Callable
from contextlib import suppress
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
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


class EventReporter:
    """Writes the canonical redacted event stream and optionally mirrors events."""

    def __init__(
        self,
        path: Path,
        *,
        consumer: Callable[[RunEvent], None] | None = None,
    ) -> None:
        self.path = path
        self.consumer = consumer
        self._lock = threading.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=True)
        with suppress(OSError):
            self.path.chmod(0o600)

    def emit(self, event: RunEvent) -> None:
        line = json.dumps(event.to_dict(), sort_keys=True) + "\n"
        with self._lock, self.path.open("a", encoding="utf-8") as handle:
            handle.write(line)
        if self.consumer:
            self.consumer(event)
