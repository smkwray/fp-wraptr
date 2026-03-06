"""Compare FP runs and produce summary deltas.

Compares two parsed FP outputs (or two run directories) and reports
the top variable differences, both absolute and percentage.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from fp_wraptr.io.parser import FPOutputData, parse_fp_output

__all__ = [
    "diff_outputs",
    "diff_run_dirs",
    "diff_runs",
    "export_diff_csv",
    "export_diff_excel",
]

if TYPE_CHECKING:
    from fp_wraptr.scenarios.runner import ScenarioResult


def diff_runs(
    baseline: ScenarioResult,
    scenario: ScenarioResult,
    top_n: int = 20,
) -> dict:
    """Compare two scenario results.

    Args:
        baseline: The baseline ScenarioResult.
        scenario: The scenario ScenarioResult to compare.
        top_n: Number of top deltas to include.

    Returns:
        Dict with comparison summary.
    """
    if not baseline.parsed_output or not scenario.parsed_output:
        return {"error": "One or both runs have no parsed output", "deltas": {}}

    return diff_outputs(baseline.parsed_output, scenario.parsed_output, top_n=top_n)


def diff_run_dirs(
    dir_a: Path | str,
    dir_b: Path | str,
    top_n: int = 20,
) -> dict:
    """Compare two run directories by parsing their fmout.txt files.

    Args:
        dir_a: First run directory (baseline).
        dir_b: Second run directory (scenario).
        top_n: Number of top deltas to include.

    Returns:
        Dict with comparison summary.
    """
    dir_a = Path(dir_a)
    dir_b = Path(dir_b)
    output_a = dir_a / "fmout.txt"
    output_b = dir_b / "fmout.txt"

    if not output_a.exists() or not output_b.exists():
        missing = []
        if not output_a.exists():
            missing.append(str(output_a))
        if not output_b.exists():
            missing.append(str(output_b))
        return {"error": f"Missing output files: {', '.join(missing)}", "deltas": {}}

    parsed_a = parse_fp_output(output_a)
    parsed_b = parse_fp_output(output_b)

    return diff_outputs(parsed_a, parsed_b, top_n=top_n)


def diff_outputs(
    baseline: FPOutputData,
    scenario: FPOutputData,
    top_n: int = 20,
) -> dict:
    """Compare two parsed FP outputs.

    Computes the difference in the final forecast period level for each
    common variable and ranks by absolute difference.

    Args:
        baseline: Baseline parsed output.
        scenario: Scenario parsed output.
        top_n: Number of top deltas to include.

    Returns:
        Dict with:
        - deltas: {var_name: {baseline, scenario, abs_delta, pct_delta}}
        - common_variables: list of variables in both outputs
        - baseline_only: list of variables only in baseline
        - scenario_only: list of variables only in scenario
    """
    base_vars = set(baseline.variables.keys())
    scen_vars = set(scenario.variables.keys())
    common = sorted(base_vars & scen_vars)

    deltas = {}
    for var_name in common:
        base_var = baseline.variables[var_name]
        scen_var = scenario.variables[var_name]

        # Use the last forecast period level value
        if not base_var.levels or not scen_var.levels:
            continue

        base_val = base_var.levels[-1]
        scen_val = scen_var.levels[-1]
        abs_delta = scen_val - base_val
        pct_delta = (abs_delta / base_val * 100) if base_val != 0 else None

        deltas[var_name] = {
            "baseline": base_val,
            "scenario": scen_val,
            "abs_delta": abs_delta,
            "pct_delta": pct_delta,
        }

    # Sort by absolute delta, descending
    sorted_deltas = dict(
        sorted(deltas.items(), key=lambda x: abs(x[1]["abs_delta"]), reverse=True)[:top_n]
    )

    return {
        "deltas": sorted_deltas,
        "common_variables": common,
        "baseline_only": sorted(base_vars - scen_vars),
        "scenario_only": sorted(scen_vars - base_vars),
        "total_compared": len(common),
    }


def _records_from_diff(diff_result: dict) -> list[dict]:
    rows: list[dict] = []
    for var_name, values in diff_result.get("deltas", {}).items():
        row = {
            "variable": var_name,
            "baseline": values.get("baseline"),
            "scenario": values.get("scenario"),
            "abs_delta": values.get("abs_delta"),
            "pct_delta": values.get("pct_delta"),
        }
        rows.append(row)
    return rows


def export_diff_csv(diff_result: dict, output_path: Path | str) -> Path:
    """Export diff deltas as a CSV file."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows = _records_from_diff(diff_result)
    if not rows:
        output_path.write_text(
            "variable,baseline,scenario,abs_delta,pct_delta\n", encoding="utf-8"
        )
        return output_path

    with output_path.open("w", encoding="utf-8", newline="") as stream:
        import csv

        writer = csv.DictWriter(
            stream,
            fieldnames=["variable", "baseline", "scenario", "abs_delta", "pct_delta"],
        )
        writer.writeheader()

        formatted_rows = []
        for row in rows:
            formatted_row = dict(row)
            if formatted_row.get("pct_delta") is None:
                formatted_row["pct_delta"] = ""
            formatted_rows.append(formatted_row)
        writer.writerows(formatted_rows)

    return output_path


def export_diff_excel(diff_result: dict, output_path: Path | str) -> Path:
    """Export diff deltas as an Excel file."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise RuntimeError("pandas is required for Excel export") from exc

    rows = _records_from_diff(diff_result)
    if not rows:
        pd.DataFrame(
            columns=["variable", "baseline", "scenario", "abs_delta", "pct_delta"]
        ).to_excel(output_path, index=False)
        return output_path

    frame = pd.DataFrame(rows)
    if "pct_delta" in frame.columns:
        frame["pct_delta"] = frame["pct_delta"].where(
            frame["pct_delta"].notna(),
            None,
        )
    frame.to_excel(output_path, index=False)
    return output_path
