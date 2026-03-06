"""Tests for human-readable scenario DSL compilation."""

from __future__ import annotations

import json

import pytest
import yaml
from typer.testing import CliRunner

from fp_wraptr.cli import app
from fp_wraptr.scenarios.dsl import (
    DSLCompileError,
    compile_scenario_dsl_file,
    compile_scenario_dsl_text,
)

runner = CliRunner()


def test_compile_scenario_dsl_text_basic() -> None:
    text = """
    scenario dsl_case
    description "Higher growth scenario"
    fp_home FM
    forecast 2025.4 to 2028.4
    track PCY,UR,GDPR
    policy monetary_rule rate=4.0 method=SAMEVALUE
    set RS CHGSAMEABS 0.5
    alert UR max 6.0
    patch cmd:SETUPSOLVE.MAXCHECK 80
    extra owner=macro-team active=true
    """

    config = compile_scenario_dsl_text(text)

    assert config.name == "dsl_case"
    assert config.description == "Higher growth scenario"
    assert config.forecast_start == "2025.4"
    assert config.forecast_end == "2028.4"
    assert config.track_variables == ["PCY", "UR", "GDPR"]
    assert config.overrides["RS"].method == "CHGSAMEABS"
    assert config.overrides["RS"].value == 0.5
    assert config.alerts["UR"]["max"] == 6.0
    assert config.input_patches["cmd:SETUPSOLVE.MAXCHECK"] == "80"
    assert config.extra["owner"] == "macro-team"
    assert config.extra["active"] is True
    assert config.extra["compiled_policies"]


def test_compile_scenario_dsl_file_uses_stem_default_name(tmp_path) -> None:
    path = tmp_path / "auto_name.dsl"
    path.write_text("set YS CHGSAMEPCT 0.008\n", encoding="utf-8")

    config = compile_scenario_dsl_file(path)

    assert config.name == "auto_name"
    assert config.overrides["YS"].value == 0.008


def test_compile_scenario_dsl_unknown_command_raises() -> None:
    with pytest.raises(DSLCompileError, match="Unknown DSL command"):
        compile_scenario_dsl_text("nope something")


def test_compile_scenario_dsl_requires_name_without_default() -> None:
    with pytest.raises(DSLCompileError, match="Scenario name is required"):
        compile_scenario_dsl_text("set YS CHGSAMEPCT 0.008", default_name=None)


def test_cli_dsl_compile_yaml(tmp_path) -> None:
    dsl_path = tmp_path / "example.dsl"
    dsl_path.write_text(
        """
        scenario cli_dsl
        set YS CHGSAMEPCT 0.008
        alert UR max 6.0
        """,
        encoding="utf-8",
    )

    result = runner.invoke(app, ["dsl", "compile", str(dsl_path)])

    assert result.exit_code == 0
    payload = yaml.safe_load(result.stdout)
    assert payload["name"] == "cli_dsl"
    assert payload["overrides"]["YS"]["value"] == 0.008
    assert payload["alerts"]["UR"]["max"] == 6.0


def test_cli_dsl_compile_json_to_file(tmp_path) -> None:
    dsl_path = tmp_path / "json.dsl"
    out_path = tmp_path / "compiled.json"
    dsl_path.write_text("scenario json_case\nset YS CHGSAMEPCT 0.008\n", encoding="utf-8")

    result = runner.invoke(
        app,
        ["dsl", "compile", str(dsl_path), "--format", "json", "--output", str(out_path)],
    )

    assert result.exit_code == 0
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["name"] == "json_case"
    assert payload["overrides"]["YS"]["method"] == "CHGSAMEPCT"


def test_cli_dsl_compile_failure_has_line_info(tmp_path) -> None:
    dsl_path = tmp_path / "bad.dsl"
    dsl_path.write_text("set ONLY_TWO_ARGS 1\n", encoding="utf-8")

    result = runner.invoke(app, ["dsl", "compile", str(dsl_path)])

    assert result.exit_code == 1
    output = (result.stdout + result.stderr).lower()
    assert "line 1" in output
    assert "dsl compile failed" in output
