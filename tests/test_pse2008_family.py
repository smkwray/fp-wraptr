from __future__ import annotations

from pathlib import Path

from fp_wraptr.scenarios.bundle import BundleConfig
from fp_wraptr.scenarios.config import ScenarioConfig


def test_pse2008_bundle_resolves_base_low_high_variants() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    bundle = BundleConfig.from_yaml(repo_root / "bundles" / "pse2008.yaml")
    assert bundle.base["name"] == "pse2008"
    assert bundle.base["forecast_start"] == "2008.4"
    assert bundle.base["forecast_end"] == "2012.4"
    assert bundle.base["fp_home"] == (repo_root / "FM").resolve()
    assert bundle.base["input_overlay_dir"] == (repo_root / "projects_local" / "pse2008").resolve()
    assert [variant.name for variant in bundle.variants] == ["base", "low", "high"]

    resolved = bundle.resolve_variants()
    assert [config.name for config in resolved] == ["pse2008_base", "pse2008_low", "pse2008_high"]
    assert [config.input_file for config in resolved] == [
        "psebase.txt",
        "pselow.txt",
        "psehigh.txt",
    ]


def test_pse2008_examples_load_with_historical_forecast_window() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    base = ScenarioConfig.from_yaml(repo_root / "examples" / "pse2008_base.yaml")
    low = ScenarioConfig.from_yaml(repo_root / "examples" / "pse2008_low.yaml")
    high = ScenarioConfig.from_yaml(repo_root / "examples" / "pse2008_high.yaml")

    assert [cfg.name for cfg in (base, low, high)] == [
        "pse2008_base",
        "pse2008_low",
        "pse2008_high",
    ]
    assert {cfg.forecast_start for cfg in (base, low, high)} == {"2008.4"}
    assert {cfg.forecast_end for cfg in (base, low, high)} == {"2012.4"}
    assert {cfg.input_overlay_dir for cfg in (base, low, high)} == {
        Path("../projects_local/pse2008")
    }
