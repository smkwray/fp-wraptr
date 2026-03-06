from __future__ import annotations

from pathlib import Path

from fp_wraptr.scenarios.config import ScenarioConfig


def test_from_yaml_tolerates_null_overrides(tmp_path: Path) -> None:
    # Some operator-authored YAMLs may include `overrides:` with only comments,
    # which YAML parses as null/None. from_yaml() should treat that as {}.
    yaml_path = tmp_path / "scenario.yaml"
    yaml_path.write_text(
        "\n".join([
            "name: null_overrides",
            'description: "x"',
            "fp_home: FM",
            "overrides:",
            "  # comment-only overrides block => YAML null",
            "track_variables:",
            "  - PCY",
            "",
        ]),
        encoding="utf-8",
    )
    cfg = ScenarioConfig.from_yaml(yaml_path)
    assert cfg.overrides == {}


def test_from_yaml_coerces_numeric_forecast_periods_to_strings(tmp_path: Path) -> None:
    yaml_path = tmp_path / "scenario.yaml"
    yaml_path.write_text(
        "\n".join([
            "name: numeric_periods",
            "fp_home: FM",
            "forecast_start: 2030.1",
            "forecast_end: 2031.4",
            "",
        ]),
        encoding="utf-8",
    )
    cfg = ScenarioConfig.from_yaml(yaml_path)
    assert cfg.forecast_start == "2030.1"
    assert cfg.forecast_end == "2031.4"
