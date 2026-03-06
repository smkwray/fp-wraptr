"""Utilities for generating markdown run reports."""

from __future__ import annotations

from pathlib import Path

from fp_wraptr.analysis.diff import diff_outputs
from fp_wraptr.io.parser import FPOutputData, parse_fp_output
from fp_wraptr.scenarios.config import ScenarioConfig


def _normalize_path(path: str | Path) -> Path:
    return Path(path)


def _read_scenario(run_dir: Path) -> tuple[ScenarioConfig | None, str]:
    scenario_path = run_dir / "scenario.yaml"
    if not scenario_path.exists():
        return None, f"Run configuration not found: {scenario_path}"
    try:
        return ScenarioConfig.from_yaml(scenario_path), ""
    except Exception as exc:
        return None, f"Failed to parse scenario config: {exc}"


def _read_output(run_dir: Path) -> FPOutputData | None:
    output_path = run_dir / "fmout.txt"
    if not output_path.exists():
        return None
    return parse_fp_output(output_path)


def _top_levels_table_rows(parsed_output: FPOutputData) -> list[tuple[str, float]]:
    entries: list[tuple[str, float]] = []
    for name, variable in parsed_output.variables.items():
        if variable.levels:
            entries.append((name, float(variable.levels[-1])))
    return sorted(entries, key=lambda item: abs(item[1]), reverse=True)[:10]


def build_run_report(
    run_dir: str | Path,
    baseline_dir: str | Path | None = None,
) -> str:
    """Build a markdown summary for a completed fp-wraptr run."""
    run_dir = _normalize_path(run_dir)

    lines: list[str] = [
        "# fp-wraptr Run Report",
        "",
        f"Run directory: `{run_dir}`",
        "",
    ]

    config, config_error = _read_scenario(run_dir)
    if config_error:
        lines.append(config_error)
        lines.append("")
    else:
        timestamp = "N/A"
        if "_" in run_dir.name:
            timestamp = run_dir.name.split("_", 1)[1]
        lines.extend([
            f"- Name: `{config.name}`",
            f"- Description: {config.description or '(none)'}",
            f"- Timestamp: `{timestamp}`",
        ])
        lines.append("")
        if config.track_variables:
            lines.append(f"- Track variables: {', '.join(config.track_variables)}")
        else:
            lines.append("- Track variables: <none>")
        lines.append("")

        if config.overrides:
            lines.append("## Overrides")
            for name, override in config.overrides.items():
                lines.append(f"- `{name}`: `{override.method}` -> `{override.value}`")
            lines.append("")

    parsed_output = _read_output(run_dir)
    if not parsed_output:
        lines.append("No forecast output available for this run.")
        return "\n".join(lines)

    lines.extend([
        "## Forecast",
        f"- Forecast start: `{parsed_output.forecast_start or 'N/A'}`",
        f"- Forecast end: `{parsed_output.forecast_end or 'N/A'}`",
        "",
    ])

    top_levels = _top_levels_table_rows(parsed_output)
    if top_levels:
        lines.extend([
            "### Top 10 variable levels (final period)",
            "",
            "|Variable|Final level|",
            "|---|---:|",
        ])
        for variable, value in top_levels:
            lines.append(f"|{variable}|{value:.4f}|")
        lines.append("")
    else:
        lines.append("No level series found in output.")
        lines.append("")

    if baseline_dir is None:
        return "\n".join(lines)

    baseline_output_path = _normalize_path(baseline_dir) / "fmout.txt"
    if not baseline_output_path.exists():
        lines.append("## Baseline comparison")
        lines.append(f"Baseline output missing: `{baseline_output_path}`")
        return "\n".join(lines)

    baseline_output = parse_fp_output(baseline_output_path)
    comparison = diff_outputs(baseline_output, parsed_output, top_n=10)
    lines.extend([
        "## Baseline comparison",
        f"- Compared variables: `{comparison.get('total_compared', 0)}`",
        "",
        "|Variable|Baseline|Scenario|Abs delta|% change|",
        "|---|---:|---:|---:|---:|",
    ])

    for name, values in comparison.get("deltas", {}).items():
        baseline_value = values.get("baseline")
        scenario_value = values.get("scenario")
        abs_delta = values.get("abs_delta")
        pct_delta = values.get("pct_delta")
        lines.append(
            "|{var}|{baseline}|{scenario}|{abs_delta}|{pct_delta}|".format(
                var=name,
                baseline="N/A" if baseline_value is None else f"{baseline_value:.4f}",
                scenario="N/A" if scenario_value is None else f"{scenario_value:.4f}",
                abs_delta="N/A" if abs_delta is None else f"{abs_delta:.4f}",
                pct_delta="N/A" if pct_delta is None else f"{pct_delta:.4f}",
            )
        )

    return "\n".join(lines)
