"""Tests for bundle runner (scenario bundles with variants + grid)."""

from __future__ import annotations

from pathlib import Path

from fp_wraptr.io.parser import ForecastVariable, FPOutputData
from fp_wraptr.runtime.backend import RunResult
from fp_wraptr.scenarios.bundle import BundleConfig, BundleResult, BundleResultEntry
from fp_wraptr.scenarios.config import ScenarioConfig
from fp_wraptr.scenarios.runner import ScenarioResult


def _mock_run_scenario(config, output_dir=None, backend=None):
    """Return a synthetic ScenarioResult without calling fp.exe."""
    output = FPOutputData(
        periods=["2025.4", "2026.1"],
        variables={
            "GDPR": ForecastVariable(var_id=1, name="GDPR", levels=[100.0, 101.0]),
            "UR": ForecastVariable(var_id=2, name="UR", levels=[4.5, 4.3]),
        },
    )
    run_result = RunResult(
        return_code=0,
        stdout="OK",
        stderr="",
        working_dir=output_dir or Path("mock"),
        input_file=Path("mock/fminput.txt"),
        output_file=Path("mock/fmout.txt"),
    )
    return ScenarioResult(
        config=config,
        output_dir=output_dir or Path("mock"),
        run_result=run_result,
        parsed_output=output,
    )


def test_bundle_config_from_dict() -> None:
    """BundleConfig can be created from a plain dict."""
    data = {
        "base": {
            "name": "test_base",
        },
        "variants": [
            {"name": "v1", "patch": {"overrides.TRGHQ.value": 10.0}},
        ],
        "variant_grid": {},
    }
    config = BundleConfig(**data)
    assert config.base["name"] == "test_base"
    assert len(config.variants) == 1
    assert config.variants[0].name == "v1"


def test_bundle_resolve_variants() -> None:
    """resolve_variants() produces the expected list of ScenarioConfigs."""
    data = {
        "base": {
            "name": "base_scenario",
        },
        "variants": [
            {"name": "low", "patch": {}},
            {"name": "high", "patch": {}},
        ],
    }
    config = BundleConfig(**data)
    variants = config.resolve_variants()

    assert len(variants) == 2
    assert all(isinstance(v, ScenarioConfig) for v in variants)
    assert variants[0].name == "base_scenario_low"
    assert variants[1].name == "base_scenario_high"


def test_bundle_resolve_grid() -> None:
    """resolve_variants() expands a variant_grid into cartesian product."""
    data = {
        "base": {
            "name": "grid_test",
            "overrides": {
                "TRGHQ": {"method": "SAMEVALUE", "value": 0.0},
            },
        },
        "variant_grid": {
            "overrides.TRGHQ.value": [1.0, 2.0, 3.0],
        },
    }
    config = BundleConfig(**data)
    variants = config.resolve_variants()

    assert len(variants) == 3
    # Names should include the grid parameter
    assert "value=1.0" in variants[0].name
    assert "value=2.0" in variants[1].name
    assert "value=3.0" in variants[2].name


def test_run_bundle(monkeypatch, tmp_path) -> None:
    """run_bundle() executes all variants and collects results."""
    from fp_wraptr.scenarios import bundle as bundle_mod

    monkeypatch.setattr(
        "fp_wraptr.scenarios.runner.run_scenario",
        _mock_run_scenario,
    )

    data = {
        "base": {"name": "bundle_test"},
        "variants": [
            {"name": "v1", "patch": {}},
            {"name": "v2", "patch": {}},
        ],
    }
    config = BundleConfig(**data)
    result = bundle_mod.run_bundle(config, output_dir=tmp_path)

    assert isinstance(result, BundleResult)
    assert result.bundle_name == "bundle_test"
    assert result.n_variants == 2
    assert result.n_succeeded == 2
    assert result.n_failed == 0


def test_bundle_result_to_dict() -> None:
    """BundleResult.to_dict() produces valid serialized output."""
    result = BundleResult(
        bundle_name="test_bundle",
        entries=[
            BundleResultEntry(
                variant_name="v1",
                config=ScenarioConfig(name="v1"),
                success=True,
                output_dir=Path("out/v1"),
            ),
            BundleResultEntry(
                variant_name="v2",
                config=ScenarioConfig(name="v2"),
                success=False,
                error="simulated failure",
            ),
        ],
    )
    d = result.to_dict()

    assert d["bundle_name"] == "test_bundle"
    assert d["n_variants"] == 2
    assert d["n_succeeded"] == 1
    assert d["n_failed"] == 1
    assert d["entries"][0]["success"] is True
    assert d["entries"][1]["error"] == "simulated failure"


def test_bundle_empty_variants() -> None:
    """Bundle with no variants returns just the base scenario."""
    data = {
        "base": {"name": "solo_base"},
    }
    config = BundleConfig(**data)
    variants = config.resolve_variants()

    assert len(variants) == 1
    assert variants[0].name == "solo_base"
