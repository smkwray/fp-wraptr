"""Sensitivity analysis — sweep an override across a range and measure response.

Given a base scenario config and a variable to sweep, runs the scenario at each
value in the sweep range and collects how tracked output variables respond.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from fp_wraptr.scenarios.config import ScenarioConfig
from fp_wraptr.scenarios.runner import ScenarioResult, run_scenario

if TYPE_CHECKING:
    from fp_wraptr.runtime.backend import ModelBackend

__all__ = ["SensitivityResult", "run_sensitivity"]


@dataclass
class SensitivityResult:
    """Result of a sensitivity sweep."""

    sweep_variable: str
    sweep_values: list[float]
    results: list[ScenarioResult]
    response_table: dict[str, list[float | None]] = field(default_factory=dict)

    @property
    def n_runs(self) -> int:
        return len(self.results)

    def to_dict(self) -> dict:
        """Convert to a JSON-serializable dictionary."""
        return {
            "sweep_variable": self.sweep_variable,
            "sweep_values": self.sweep_values,
            "n_runs": self.n_runs,
            "response_table": self.response_table,
        }


def run_sensitivity(
    config: ScenarioConfig,
    sweep_variable: str,
    sweep_values: list[float],
    method: str = "CHGSAMEPCT",
    output_dir: Path | None = None,
    backend: ModelBackend | None = None,
    track_variables: list[str] | None = None,
) -> SensitivityResult:
    """Run a scenario across a range of override values for one variable.

    Args:
        config: Base scenario configuration.
        sweep_variable: The exogenous variable to sweep.
        sweep_values: List of override values to test.
        method: Override method (CHGSAMEPCT, SAMEVALUE, CHGSAMEABS).
        output_dir: Base directory for run artifacts.
        backend: Model execution backend.
        track_variables: Variables to track in the response table.
            Defaults to config.track_variables.

    Returns:
        SensitivityResult with all runs and a response table.
    """
    if output_dir is None:
        output_dir = Path("artifacts") / "sensitivity"

    if track_variables is None:
        track_variables = list(config.track_variables)

    results: list[ScenarioResult] = []

    for value in sweep_values:
        # Deep copy config and apply the sweep override
        sweep_config = _make_sweep_config(config, sweep_variable, value, method)
        sweep_config.name = f"{config.name}_sweep_{sweep_variable}_{value}"

        result = run_scenario(
            sweep_config,
            output_dir=output_dir,
            backend=backend,
        )
        results.append(result)

    # Build response table: for each tracked variable, collect final-period level
    response_table = _build_response_table(results, track_variables)

    return SensitivityResult(
        sweep_variable=sweep_variable,
        sweep_values=sweep_values,
        results=results,
        response_table=response_table,
    )


def _make_sweep_config(
    base: ScenarioConfig,
    variable: str,
    value: float,
    method: str,
) -> ScenarioConfig:
    """Create a config copy with the sweep variable override applied."""
    data = base.model_dump(mode="json")
    overrides = dict(data.get("overrides", {}))
    overrides[variable] = {"method": method, "value": value}
    data["overrides"] = overrides
    return ScenarioConfig(**data)


def _build_response_table(
    results: list[ScenarioResult],
    track_variables: list[str],
) -> dict[str, list[float | None]]:
    """Extract final-period levels for tracked variables across all runs."""
    table: dict[str, list[float | None]] = {}

    for var_name in track_variables:
        values: list[float | None] = []
        for result in results:
            if result.parsed_output and var_name in result.parsed_output.variables:
                levels = result.parsed_output.variables[var_name].levels
                values.append(levels[-1] if levels else None)
            else:
                values.append(None)
        table[var_name] = values

    return table
