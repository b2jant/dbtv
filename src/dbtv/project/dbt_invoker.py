from __future__ import annotations

import os
import shlex
import shutil
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

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
    ) -> DbtInvocationResult:
        executable = self.resolved_executable()
        command = (executable, *args)
        process_env = os.environ.copy()
        if env:
            process_env.update(env)
        completed = subprocess.run(
            command,
            cwd=cwd,
            env=process_env,
            text=True,
            capture_output=True,
            check=False,
        )
        result = DbtInvocationResult(
            command=command,
            return_code=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
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
