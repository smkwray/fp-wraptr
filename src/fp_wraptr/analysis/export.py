"""Export run data to Excel workbooks.

Produces structured Excel files with multiple sheets covering
configuration, forecast data, and optionally comparison deltas.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from fp_wraptr.io.parser import FPOutputData

if TYPE_CHECKING:
    from fp_wraptr.scenarios.runner import ScenarioResult

__all__ = ["export_comparison_workbook", "export_run_workbook"]


def export_run_workbook(
    result: ScenarioResult,
    output_path: Path | str,
) -> Path:
    """Export a single run to an Excel workbook.

    Sheets:
        - Config: scenario configuration summary
        - Forecast: level values for all variables, periods as rows
        - Changes: absolute change values
        - PctChanges: percent change values

    Args:
        result: Completed ScenarioResult with parsed output.
        output_path: Path for the output .xlsx file.

    Returns:
        Path to the written workbook.
    """
    import pandas as pd

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # Config sheet
        config_data = _config_to_records(result)
        pd.DataFrame(config_data).to_excel(writer, sheet_name="Config", index=False)

        # Forecast sheets
        if result.parsed_output and result.parsed_output.variables:
            data = result.parsed_output
            periods = data.periods

            levels = _variables_to_df(data, periods, "levels")
            levels.to_excel(writer, sheet_name="Forecast")

            changes = _variables_to_df(data, periods, "changes")
            changes.to_excel(writer, sheet_name="Changes")

            pct_changes = _variables_to_df(data, periods, "pct_changes")
            pct_changes.to_excel(writer, sheet_name="PctChanges")

    return output_path


def export_comparison_workbook(
    baseline: ScenarioResult,
    scenario: ScenarioResult,
    output_path: Path | str,
    diff_result: dict | None = None,
) -> Path:
    """Export a run comparison to an Excel workbook.

    Sheets:
        - Config: side-by-side configuration comparison
        - Baseline: baseline forecast levels
        - Scenario: scenario forecast levels
        - Comparison: delta table from diff_result (if provided)

    Args:
        baseline: Baseline ScenarioResult.
        scenario: Scenario ScenarioResult.
        output_path: Path for the output .xlsx file.
        diff_result: Optional pre-computed diff result dict.

    Returns:
        Path to the written workbook.
    """
    import pandas as pd

    from fp_wraptr.analysis.diff import diff_runs

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if diff_result is None:
        diff_result = diff_runs(baseline, scenario)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # Config comparison
        config_records = _comparison_config_records(baseline, scenario)
        pd.DataFrame(config_records).to_excel(writer, sheet_name="Config", index=False)

        # Baseline forecast
        if baseline.parsed_output and baseline.parsed_output.variables:
            bl_data = baseline.parsed_output
            levels = _variables_to_df(bl_data, bl_data.periods, "levels")
            levels.to_excel(writer, sheet_name="Baseline")

        # Scenario forecast
        if scenario.parsed_output and scenario.parsed_output.variables:
            sc_data = scenario.parsed_output
            levels = _variables_to_df(sc_data, sc_data.periods, "levels")
            levels.to_excel(writer, sheet_name="Scenario")

        # Comparison deltas
        deltas = diff_result.get("deltas", {})
        if deltas:
            rows = []
            for var_name, vals in deltas.items():
                rows.append({
                    "Variable": var_name,
                    "Baseline": vals.get("baseline"),
                    "Scenario": vals.get("scenario"),
                    "Abs Delta": vals.get("abs_delta"),
                    "Pct Delta": vals.get("pct_delta"),
                })
            pd.DataFrame(rows).to_excel(writer, sheet_name="Comparison", index=False)

    return output_path


def _config_to_records(result: ScenarioResult) -> list[dict[str, str]]:
    """Convert a ScenarioResult's config to key-value records."""
    config = result.config
    records = [
        {"Key": "Name", "Value": config.name},
        {"Key": "FP Home", "Value": str(config.fp_home)},
        {"Key": "Forecast Start", "Value": str(config.forecast_start)},
        {"Key": "Forecast End", "Value": str(config.forecast_end)},
        {"Key": "Backend", "Value": str(getattr(config, "backend", "fpexe"))},
    ]
    if config.overrides:
        for name, ov in config.overrides.items():
            records.append({"Key": f"Override: {name}", "Value": f"{ov.method} = {ov.value}"})
    if config.track_variables:
        records.append({"Key": "Track Variables", "Value": ", ".join(config.track_variables)})
    return records


def _comparison_config_records(
    baseline: ScenarioResult,
    scenario: ScenarioResult,
) -> list[dict[str, str]]:
    """Build side-by-side config comparison records."""
    records = [
        {
            "Key": "Baseline Name",
            "Value": baseline.config.name,
        },
        {
            "Key": "Scenario Name",
            "Value": scenario.config.name,
        },
    ]
    # List overrides that differ
    base_ov = baseline.config.overrides or {}
    scen_ov = scenario.config.overrides or {}
    all_keys = sorted(set(base_ov.keys()) | set(scen_ov.keys()))
    for key in all_keys:
        b = base_ov.get(key)
        s = scen_ov.get(key)
        b_str = f"{b.method}={b.value}" if b else "(none)"
        s_str = f"{s.method}={s.value}" if s else "(none)"
        records.append({
            "Key": f"Override: {key}",
            "Value": f"Baseline: {b_str} | Scenario: {s_str}",
        })
    return records


def _variables_to_df(
    data: FPOutputData,
    periods: list[str],
    attr: str,
) -> pd.DataFrame:
    """Build a DataFrame from parsed output variables.

    Args:
        data: Parsed FP output.
        periods: Period labels.
        attr: Attribute to extract from ForecastVariable (levels, changes, pct_changes).
    """

    columns: dict[str, list[float]] = {}
    for name, var in data.variables.items():
        values = getattr(var, attr, [])
        columns[name] = values

    max_len = max(len(v) for v in columns.values()) if columns else 0
    index = periods[:max_len]
    return pd.DataFrame(columns, index=index)
