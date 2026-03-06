"""Batch scenario execution and golden-output comparison tools."""

from __future__ import annotations

import shutil
from pathlib import Path

from fp_wraptr.io.parser import FPOutputData, parse_fp_output
from fp_wraptr.scenarios.config import ScenarioConfig
from fp_wraptr.scenarios.runner import ScenarioResult


def run_batch(
    configs: list[ScenarioConfig],
    output_dir: Path,
    baseline_dir: Path | None = None,
) -> list[ScenarioResult]:
    """Run each scenario configuration in sequence and return results."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[ScenarioResult] = []

    for config in configs:
        # Import lazily so tests can monkeypatch fp_wraptr.scenarios.runner.run_scenario
        # without depending on module import order.
        from fp_wraptr.scenarios.runner import run_scenario

        result = run_scenario(config, output_dir=output_dir)
        if baseline_dir is None:
            result.golden_comparison = None
        else:
            golden_subdir = Path(baseline_dir) / config.name
            if golden_subdir.exists():
                result.golden_comparison = compare_to_golden(result, golden_subdir)
            else:
                result.golden_comparison = None
        results.append(result)

    return results


def _load_or_parse_output(
    result: ScenarioResult,
) -> tuple[FPOutputData | None, str]:
    if result.parsed_output:
        return result.parsed_output, ""

    output_path = result.output_dir / "fmout.txt"
    if not output_path.exists():
        return None, f"Missing fmout.txt in {result.output_dir}"
    return parse_fp_output(output_path), ""


def compare_to_golden(
    result: ScenarioResult,
    golden_dir: Path,
    tolerance: float = 1e-4,
) -> dict:
    """Compare a scenario result against a stored golden output."""
    golden_dir = Path(golden_dir)
    golden_path = golden_dir / "fmout.txt"
    if not golden_path.exists():
        return {"matches": False, "variable_diffs": {}, "max_delta": float("inf")}

    scenario_output, scenario_error = _load_or_parse_output(result)
    if scenario_output is None:
        return {
            "matches": False,
            "variable_diffs": {},
            "max_delta": float("inf"),
            "error": scenario_error,
        }

    baseline_output = parse_fp_output(golden_path)
    deltas = {}
    max_delta = 0.0

    for var_name in sorted(scenario_output.variables):
        if var_name not in baseline_output.variables:
            deltas[var_name] = {
                "status": "missing_in_golden",
                "abs_delta": None,
                "pct_delta": None,
                "baseline": None,
                "scenario": scenario_output.variables[var_name].levels[-1]
                if scenario_output.variables[var_name].levels
                else None,
            }
            max_delta = float("inf")
            continue

        scenario_values = scenario_output.variables[var_name].levels
        baseline_values = baseline_output.variables[var_name].levels
        if not scenario_values or not baseline_values:
            deltas[var_name] = {
                "status": "empty_series",
                "abs_delta": None,
                "pct_delta": None,
                "baseline": baseline_values[-1] if baseline_values else None,
                "scenario": scenario_values[-1] if scenario_values else None,
            }
            continue

        abs_delta = scenario_values[-1] - baseline_values[-1]
        pct_delta = (abs_delta / baseline_values[-1] * 100) if baseline_values[-1] != 0 else None
        deltas[var_name] = {
            "baseline": baseline_values[-1],
            "scenario": scenario_values[-1],
            "abs_delta": abs_delta,
            "pct_delta": pct_delta,
        }
        max_delta = max(max_delta, abs(abs_delta))

    matches = max_delta <= tolerance and all(
        values.get("status") is None
        and (values["abs_delta"] is None or abs(values["abs_delta"]) <= tolerance)
        for values in deltas.values()
    )

    # Include extra vars in golden that are missing from the scenario output.
    for missing in sorted(set(baseline_output.variables) - set(scenario_output.variables)):
        baseline_values = baseline_output.variables[missing].levels
        deltas[missing] = {
            "status": "missing_in_scenario",
            "abs_delta": None,
            "pct_delta": None,
            "baseline": baseline_values[-1] if baseline_values else None,
            "scenario": None,
        }
        max_delta = float("inf")
        matches = False

    return {
        "matches": matches,
        "variable_diffs": deltas,
        "max_delta": max_delta,
    }


def save_golden(result: ScenarioResult, golden_dir: Path) -> Path:
    """Copy `fmout.txt` into a golden directory."""
    golden_dir = Path(golden_dir)
    golden_dir.mkdir(parents=True, exist_ok=True)

    source = result.output_dir / "fmout.txt"
    if not source.exists():
        raise FileNotFoundError(f"No fmout.txt to save at {result.output_dir}")

    destination = golden_dir / "fmout.txt"
    shutil.copy2(source, destination)
    return destination
