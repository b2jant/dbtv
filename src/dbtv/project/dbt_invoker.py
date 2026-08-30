from __future__ import annotations

import os
import shlex
import shutil
import signal
import subprocess
import threading
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

from dbtv.core.cancellation import CancellationToken
from dbtv.core.errors import DbtInvocationError


@dataclass(frozen=True)
class DbtInvocationResult:
    command: tuple[str, ...]
    return_code: int
    stdout: str
    stderr: str


class DbtInvoker:
    def __init__(self, executable: str = "dbt") -> None:
        self.executable = executable

    def resolved_executable(self) -> str:
        if os.path.sep in self.executable:
            path = Path(self.executable).expanduser().resolve()
            if path.is_file() and os.access(path, os.X_OK):
                return str(path)
        if resolved := shutil.which(self.executable):
            return resolved
        raise DbtInvocationError(
            f"dbt executable {self.executable!r} was not found.",
            hint=(
                "Install dbt and the Snowflake/DuckDB adapters in this environment, "
                "or set project.dbt_executable."
            ),
        )

    def version(self) -> DbtInvocationResult:
        return self.run(["--version"], check=False)

    def run(
        self,
        args: Sequence[str],
        *,
        cwd: Path | None = None,
        env: Mapping[str, str] | None = None,
        check: bool = True,
        cancellation: CancellationToken | None = None,
    ) -> DbtInvocationResult:
        executable = self.resolved_executable()
        command = (executable, *args)
        process_env = os.environ.copy()
        if env:
            process_env.update(env)
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=process_env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=os.name != "nt",
        )
        unregister = (
            cancellation.register(lambda: _terminate_process(process))
            if cancellation is not None
            else lambda: None
        )
        try:
            stdout, stderr = process.communicate()
        finally:
            unregister()
        if cancellation is not None:
            cancellation.raise_if_cancelled()
        result = DbtInvocationResult(
            command=command,
            return_code=process.returncode,
            stdout=stdout,
            stderr=stderr,
        )
        if check and result.return_code != 0:
            raise DbtInvocationError(
                f"dbt command failed with exit code {result.return_code}.",
                context={
                    "command": shlex.join(command),
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                },
            )
        return result


def _terminate_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":  # pragma: no cover - exercised on Windows CI
        process.terminate()
    else:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGTERM)

    def force() -> None:
        if process.poll() is not None:
            return
        if os.name == "nt":  # pragma: no cover - exercised on Windows CI
            process.kill()
        else:
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)

    timer = threading.Timer(2, force)
    timer.daemon = True
    timer.start()
