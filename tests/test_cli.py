from pathlib import Path

from click.testing import CliRunner

from dbtv.cli.app import main
from dbtv.workspace import Workspace


def create_project(root: Path) -> None:
    (root / "dbt_project.yml").write_text(
        "name: analytics\nprofile: analytics_profile\n", encoding="utf-8"
    )


def test_version_command() -> None:
    result = CliRunner().invoke(main, ["version"])
    assert result.exit_code == 0
    assert "dbtv:" in result.output


def test_init_creates_config_and_workspace(tmp_path: Path) -> None:
    create_project(tmp_path)
    result = CliRunner().invoke(main, ["--project-dir", str(tmp_path), "init"])
    assert result.exit_code == 0
    assert (tmp_path / "dbtv.yml").is_file()
    assert (tmp_path / ".dbtv").is_dir()


def test_init_updates_gitignore_only_when_requested(tmp_path: Path) -> None:
    create_project(tmp_path)
    result = CliRunner().invoke(
        main,
        ["--project-dir", str(tmp_path), "init", "--update-gitignore"],
    )
    assert result.exit_code == 0
    assert (tmp_path / ".gitignore").read_text(encoding="utf-8") == ".dbtv/\n"


def test_doctor_reports_missing_dbt(tmp_path: Path) -> None:
    create_project(tmp_path)
    result = CliRunner().invoke(
        main,
        ["--project-dir", str(tmp_path), "--output", "json", "doctor"],
        env={"PATH": ""},
    )
    assert result.exit_code == 2
    assert "dbt executable" in result.output


def test_execution_command_requires_a_dbt_project(tmp_path: Path) -> None:
    result = CliRunner().invoke(main, ["--project-dir", str(tmp_path), "run"])
    assert result.exit_code == 3
    assert "No dbt_project.yml" in result.output


def test_help_lists_complete_stable_command_surface() -> None:
    result = CliRunner().invoke(main, ["--help"])
    assert result.exit_code == 0
    for command in (
        "init",
        "doctor",
        "plan",
        "sync",
        "run",
        "test",
        "build",
        "status",
        "inspect",
        "clean",
        "diagnostics",
        "version",
    ):
        assert command in result.output


def test_clean_preview_then_exact_run_cleanup(tmp_path: Path) -> None:
    create_project(tmp_path)
    workspace = Workspace(tmp_path)
    run = workspace.create_run()
    (run.root / "sentinel.txt").write_text("local", encoding="utf-8")
    preview = CliRunner().invoke(
        main,
        ["--project-dir", str(tmp_path), "clean", "--runs", "--preview"],
    )
    assert preview.exit_code == 0
    assert str(run.root) in preview.output
    assert run.root.exists()
    cleaned = CliRunner().invoke(
        main,
        ["--project-dir", str(tmp_path), "clean", "--runs", "--yes"],
    )
    assert cleaned.exit_code == 0
    assert not run.root.exists()
