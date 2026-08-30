from pathlib import Path

from click.testing import CliRunner

from dbtv.cli.app import main


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


def test_doctor_reports_missing_dbt(tmp_path: Path) -> None:
    create_project(tmp_path)
    result = CliRunner().invoke(
        main,
        ["--project-dir", str(tmp_path), "--output", "json", "doctor"],
        env={"PATH": ""},
    )
    assert result.exit_code == 2
    assert "dbt executable" in result.output


def test_unimplemented_execution_command_is_explicit(tmp_path: Path) -> None:
    result = CliRunner().invoke(main, ["--project-dir", str(tmp_path), "run"])
    assert result.exit_code == 2
    assert "not implemented" in result.output

