from __future__ import annotations

import zipfile
from pathlib import Path

from dbtv.config.schema import DbtvConfig
from dbtv.diagnostics import create_diagnostics_bundle
from dbtv.workspace import Workspace


def test_diagnostics_bundle_excludes_data_and_redacts_canaries(tmp_path: Path) -> None:
    workspace = Workspace(tmp_path)
    run = workspace.create_run()
    run.events_path.write_text(
        'password=canary-secret\n{"token":"another-canary"}\n',
        encoding="utf-8",
    )
    data = workspace.cache / "snapshots" / "source" / "snapshot" / "data"
    data.mkdir(parents=True)
    (data / "part-00000.parquet").write_bytes(b"sensitive-row-data")
    output = create_diagnostics_bundle(
        config=DbtvConfig().resolve_paths(tmp_path),
        workspace=workspace,
    )
    with zipfile.ZipFile(output) as archive:
        names = archive.namelist()
        content = b"\n".join(archive.read(name) for name in names)
    assert not any(name.endswith(".parquet") for name in names)
    assert b"sensitive-row-data" not in content
    assert b"canary-secret" not in content
    assert b"another-canary" not in content
    assert b"<redacted>" in content
