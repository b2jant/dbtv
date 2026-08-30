from __future__ import annotations

import threading
from collections.abc import Callable

from dbtv.core.errors import CancellationError


class CancellationToken:
    """Thread-safe cooperative cancellation shared by extraction and dbt execution."""

    def __init__(self) -> None:
        self._event = threading.Event()
        self._callbacks: list[Callable[[], None]] = []
        self._lock = threading.Lock()

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()

    def raise_if_cancelled(self) -> None:
        if self.cancelled:
            raise CancellationError()

    def wait(self, seconds: float) -> None:
        if self._event.wait(seconds):
            raise CancellationError()

    def register(self, callback: Callable[[], None]) -> Callable[[], None]:
        with self._lock:
            if self.cancelled:
                callback()
            else:
                self._callbacks.append(callback)

        def unregister() -> None:
            with self._lock:
                if callback in self._callbacks:
                    self._callbacks.remove(callback)

        return unregister

    def cancel(self) -> None:
        if self._event.is_set():
            return
        self._event.set()
        with self._lock:
            callbacks = tuple(self._callbacks)
            self._callbacks.clear()
        for callback in callbacks:
            try:
                callback()
            except Exception:
                # Cancellation is best effort and must continue notifying peers.
                continue
