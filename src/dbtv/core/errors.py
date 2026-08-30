from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(eq=False)
class DbtvError(Exception):
    message: str
    error_id: str = "DBTV-INTERNAL-001"
    exit_code: int = 10
    hint: str | None = None
    context: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return self.message


class ConfigError(DbtvError):
    def __init__(self, message: str, *, hint: str | None = None) -> None:
        super().__init__(message, "DBTV-CONFIG-001", 2, hint)


class ProjectError(DbtvError):
    def __init__(self, message: str, *, hint: str | None = None) -> None:
        super().__init__(message, "DBTV-PROJECT-001", 3, hint)


class DbtInvocationError(DbtvError):
    def __init__(
        self,
        message: str,
        *,
        hint: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, "DBTV-DBT-001", 3, hint, context or {})


class ManifestError(DbtvError):
    def __init__(self, message: str, *, hint: str | None = None) -> None:
        super().__init__(message, "DBTV-MANIFEST-001", 3, hint)


class OfflineViolation(DbtvError):
    def __init__(self, message: str) -> None:
        super().__init__(message, "DBTV-OFFLINE-001", 4)


class NotImplementedMilestone(DbtvError):
    def __init__(self, capability: str) -> None:
        super().__init__(
            f"{capability} is not implemented in the current milestone.",
            "DBTV-MILESTONE-001",
            2,
            "Use `dbtv plan` to validate the project while the first execution slice is built.",
        )

