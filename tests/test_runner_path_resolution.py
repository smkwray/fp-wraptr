from __future__ import annotations

from pathlib import Path

from fp_wraptr.scenarios.runner import load_scenario_config


def test_load_scenario_config_resolves_fp_home_relative_to_yaml(tmp_path: Path) -> None:
    # Scenario YAMLs are often run from arbitrary working directories. Ensure a
    # relative fp_home is interpreted relative to the YAML file location.
    fm_dir = tmp_path / "FM"
    fm_dir.mkdir()

    scenario_dir = tmp_path / "scenarios"
    scenario_dir.mkdir()
    scenario_path = scenario_dir / "baseline.yaml"
    scenario_path.write_text(
        "\n".join([
            "name: baseline",
            "description: test",
            "fp_home: ../FM",
            "",
        ]),
        encoding="utf-8",
    )

    cfg = load_scenario_config(scenario_path)
    assert cfg.fp_home == fm_dir.resolve()
