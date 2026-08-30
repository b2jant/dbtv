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
        super().__init__(message, "DBTV-OFFLINE-001", 7)


class CredentialError(DbtvError):
    def __init__(self, message: str, *, hint: str | None = None) -> None:
        super().__init__(message, "DBTV-CREDENTIAL-001", 4, hint)


class SourceConnectionError(DbtvError):
    def __init__(self, message: str, *, hint: str | None = None) -> None:
        super().__init__(message, "DBTV-SOURCE-CONNECTION-001", 4, hint)


class ExtractionError(DbtvError):
    def __init__(
        self,
        message: str,
        *,
        hint: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, "DBTV-EXTRACTION-001", 5, hint, context or {})


class PolicyError(DbtvError):
    def __init__(self, message: str, *, hint: str | None = None) -> None:
        super().__init__(message, "DBTV-POLICY-001", 6, hint)


class CacheError(DbtvError):
    def __init__(
        self,
        message: str,
        *,
        hint: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, "DBTV-CACHE-001", 7, hint, context or {})


class LockError(DbtvError):
    def __init__(self, message: str, *, hint: str | None = None) -> None:
        super().__init__(message, "DBTV-LOCK-001", 7, hint)


class BindingError(DbtvError):
    def __init__(
        self,
        message: str,
        *,
        hint: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, "DBTV-BINDING-001", 8, hint, context or {})


class CompatibilityError(DbtvError):
    def __init__(self, message: str, *, hint: str | None = None) -> None:
        super().__init__(message, "DBTV-COMPAT-001", 9, hint)


class CancellationError(DbtvError):
    def __init__(self, message: str = "Operation cancelled by the user.") -> None:
        super().__init__(message, "DBTV-CANCELLED-001", 130)
