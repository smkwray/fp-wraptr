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


def test_load_scenario_config_resolves_fppy_fmout_override_relative_to_yaml(
    tmp_path: Path,
) -> None:
    fm_dir = tmp_path / "FM"
    fm_dir.mkdir()
    overrides_dir = tmp_path / "research"
    overrides_dir.mkdir()
    override_path = overrides_dir / "gmcoef.txt"
    override_path.write_text("stub\n", encoding="utf-8")

    scenario_dir = tmp_path / "scenarios"
    scenario_dir.mkdir()
    scenario_path = scenario_dir / "baseline.yaml"
    scenario_path.write_text(
        "\n".join([
            "name: baseline",
            "description: test",
            "fp_home: ../FM",
            "backend: fppy",
            "fppy:",
            "  fmout_coefs_override: ../research/gmcoef.txt",
            "",
        ]),
        encoding="utf-8",
    )

    cfg = load_scenario_config(scenario_path)
    assert cfg.fp_home == fm_dir.resolve()
    assert cfg.fppy.fmout_coefs_override == str(override_path.resolve())


def test_load_scenario_config_resolves_fpr_paths_relative_to_yaml(tmp_path: Path) -> None:
    fm_dir = tmp_path / "FM"
    fm_dir.mkdir()
    fp_r_dir = tmp_path / "fp-r"
    fixtures_dir = fp_r_dir / "fixtures"
    fixtures_dir.mkdir(parents=True)
    bundle_path = fixtures_dir / "real_stock_eq_bundle.R"
    bundle_path.write_text(
        "bundle <- list(state = list(periods = c('2025.1'), series = list()))\n", encoding="utf-8"
    )
    tools_dir = tmp_path / "tools"
    tools_dir.mkdir()
    rscript_path = tools_dir / "Rscript.exe"
    rscript_path.write_text("", encoding="utf-8")
    expected_path = fixtures_dir / "real_stock_eq_expected.csv"
    expected_path.write_text("period,A\n2025.1,1.0\n", encoding="utf-8")

    scenario_dir = tmp_path / "scenarios"
    scenario_dir.mkdir()
    scenario_path = scenario_dir / "fpr.yaml"
    scenario_path.write_text(
        "\n".join([
            "name: fpr",
            "description: test",
            "fp_home: ../FM",
            "backend: fp-r",
            "fpr:",
            "  bundle_path: ../fp-r/fixtures/real_stock_eq_bundle.R",
            "  rscript_path: ../tools/Rscript.exe",
            "  expected_csv: ../fp-r/fixtures/real_stock_eq_expected.csv",
            "",
        ]),
        encoding="utf-8",
    )

    cfg = load_scenario_config(scenario_path)
    assert cfg.fp_home == fm_dir.resolve()
    assert cfg.fpr["bundle_path"] == str(bundle_path.resolve())
    assert cfg.fpr["rscript_path"] == str(rscript_path.resolve())
    assert cfg.fpr["expected_csv"] == str(expected_path.resolve())
