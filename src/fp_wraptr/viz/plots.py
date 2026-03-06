"""Forecast visualization using matplotlib.

Provides two canonical plot types:
1. Forecast levels: time series of variable levels over the forecast horizon.
2. Forecast comparison: overlay baseline vs scenario levels with delta shading.
"""

from __future__ import annotations

from pathlib import Path

from fp_wraptr.io.parser import FPOutputData


def plot_forecast(
    data: FPOutputData,
    variables: list[str] | None = None,
    output_path: Path | str = Path("artifacts/forecast.png"),
    title: str | None = None,
) -> Path:
    """Plot forecast levels for selected variables.

    Args:
        data: Parsed FP output data.
        variables: List of variable names to plot. If None, plots all.
        output_path: Path to save the chart.
        title: Chart title. If None, auto-generated.

    Returns:
        Path to the saved chart file.
    """
    import matplotlib.pyplot as plt

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if variables is None:
        variables = list(data.variables.keys())

    # Filter to variables that exist
    plot_vars = [v for v in variables if v in data.variables]
    if not plot_vars:
        raise ValueError(f"No matching variables found. Available: {list(data.variables.keys())}")

    n_vars = len(plot_vars)
    fig, axes = plt.subplots(n_vars, 1, figsize=(12, 3 * n_vars), squeeze=False)

    for i, var_name in enumerate(plot_vars):
        ax = axes[i, 0]
        var = data.variables[var_name]
        periods = data.periods[: len(var.levels)]

        ax.plot(periods, var.levels, marker="o", linewidth=2, markersize=4)
        ax.set_ylabel(var_name)
        ax.set_title(f"{var_name} (id={var.var_id})")
        ax.grid(True, alpha=0.3)

        # Rotate x labels for readability
        ax.tick_params(axis="x", rotation=45)

        # Only show x labels on bottom subplot
        if i < n_vars - 1:
            ax.set_xticklabels([])

    fig.suptitle(
        title or f"FP Forecast: {data.forecast_start} to {data.forecast_end}",
        fontsize=14,
        fontweight="bold",
    )
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    return output_path


def plot_comparison(
    baseline: FPOutputData,
    scenario: FPOutputData,
    variables: list[str] | None = None,
    output_path: Path | str = Path("artifacts/comparison.png"),
    title: str | None = None,
) -> Path:
    """Plot baseline vs scenario comparison for selected variables.

    Args:
        baseline: Baseline parsed output.
        scenario: Scenario parsed output.
        variables: Variables to compare. If None, uses common variables.
        output_path: Path to save the chart.
        title: Chart title.

    Returns:
        Path to the saved chart file.
    """
    import matplotlib.pyplot as plt

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if variables is None:
        common = set(baseline.variables.keys()) & set(scenario.variables.keys())
        variables = sorted(common)

    plot_vars = [v for v in variables if v in baseline.variables and v in scenario.variables]
    if not plot_vars:
        raise ValueError("No common variables found to compare.")

    n_vars = len(plot_vars)
    fig, axes = plt.subplots(n_vars, 1, figsize=(12, 3.5 * n_vars), squeeze=False)

    for i, var_name in enumerate(plot_vars):
        ax = axes[i, 0]
        base_var = baseline.variables[var_name]
        scen_var = scenario.variables[var_name]

        n_points = min(len(base_var.levels), len(scen_var.levels))
        periods = baseline.periods[:n_points]
        base_vals = base_var.levels[:n_points]
        scen_vals = scen_var.levels[:n_points]

        ax.plot(periods, base_vals, marker="s", linewidth=2, markersize=4, label="Baseline")
        ax.plot(periods, scen_vals, marker="o", linewidth=2, markersize=4, label="Scenario")
        ax.fill_between(
            range(n_points), base_vals, scen_vals, alpha=0.15, color="orange", label="Delta"
        )
        ax.set_ylabel(var_name)
        ax.set_title(var_name)
        ax.legend(loc="upper left", fontsize=8)
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis="x", rotation=45)

        if i < n_vars - 1:
            ax.set_xticklabels([])

    fig.suptitle(title or "FP Run Comparison", fontsize=14, fontweight="bold")
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    return output_path
