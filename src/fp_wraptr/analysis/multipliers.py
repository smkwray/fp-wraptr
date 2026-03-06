"""Fiscal and monetary multiplier computation.

Given a baseline run and a shocked run (where a policy variable was changed),
compute standard multipliers: how much GDP, employment, and other variables
respond per unit of the policy shock over the forecast horizon.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from fp_wraptr.io.parser import FPOutputData

if TYPE_CHECKING:
    from fp_wraptr.scenarios.runner import ScenarioResult

__all__ = ["MultiplierResult", "MultiplierRow", "compute_multipliers"]


@dataclass
class MultiplierRow:
    """Multiplier values for a single forecast period."""

    period: str
    response_var: str
    baseline_level: float
    shocked_level: float
    delta: float
    multiplier: float | None


@dataclass
class MultiplierResult:
    """Result of a multiplier computation."""

    shock_variable: str
    shock_size: float
    response_variables: list[str]
    rows: list[MultiplierRow] = field(default_factory=list)

    @property
    def n_periods(self) -> int:
        if not self.rows:
            return 0
        periods = {r.period for r in self.rows}
        return len(periods)

    def table(self) -> dict[str, dict[str, float | None]]:
        """Build a {response_var: {period: multiplier}} lookup table."""
        result: dict[str, dict[str, float | None]] = {}
        for row in self.rows:
            result.setdefault(row.response_var, {})[row.period] = row.multiplier
        return result

    def to_dict(self) -> dict:
        """Convert to a JSON-serializable dictionary."""
        return {
            "shock_variable": self.shock_variable,
            "shock_size": self.shock_size,
            "response_variables": self.response_variables,
            "n_periods": self.n_periods,
            "table": self.table(),
        }


def compute_multipliers(
    baseline: FPOutputData | ScenarioResult,
    shocked: FPOutputData | ScenarioResult,
    shock_variable: str,
    shock_size: float,
    response_variables: list[str] | None = None,
) -> MultiplierResult:
    """Compute multipliers from a baseline and shocked run.

    The multiplier for each response variable in each period is:
        multiplier = (shocked_level - baseline_level) / shock_size

    Args:
        baseline: Baseline run output (FPOutputData or ScenarioResult).
        shocked: Shocked run output (FPOutputData or ScenarioResult).
        shock_variable: The policy variable that was shocked.
        shock_size: Size of the shock applied (e.g. 0.01 for 1% of GDP).
        response_variables: Variables to compute multipliers for.
            Defaults to all common variables between the two runs.

    Returns:
        MultiplierResult with per-period multiplier rows.
    """
    base_data = _extract_output(baseline)
    shock_data = _extract_output(shocked)

    if base_data is None or shock_data is None:
        return MultiplierResult(
            shock_variable=shock_variable,
            shock_size=shock_size,
            response_variables=response_variables or [],
        )

    common = set(base_data.variables.keys()) & set(shock_data.variables.keys())

    if response_variables is None:
        response_variables = sorted(common)
    else:
        response_variables = [v for v in response_variables if v in common]

    rows: list[MultiplierRow] = []
    periods = base_data.periods

    for var_name in response_variables:
        base_var = base_data.variables[var_name]
        shock_var = shock_data.variables[var_name]
        n_points = min(len(base_var.levels), len(shock_var.levels), len(periods))

        for i in range(n_points):
            base_val = base_var.levels[i]
            shock_val = shock_var.levels[i]
            delta = shock_val - base_val
            multiplier = delta / shock_size if shock_size != 0 else None

            rows.append(
                MultiplierRow(
                    period=periods[i],
                    response_var=var_name,
                    baseline_level=base_val,
                    shocked_level=shock_val,
                    delta=delta,
                    multiplier=multiplier,
                )
            )

    return MultiplierResult(
        shock_variable=shock_variable,
        shock_size=shock_size,
        response_variables=response_variables,
        rows=rows,
    )


def _extract_output(
    source: FPOutputData | ScenarioResult,
) -> FPOutputData | None:
    """Extract FPOutputData from either an FPOutputData or ScenarioResult."""
    if isinstance(source, FPOutputData):
        return source
    # ScenarioResult
    return getattr(source, "parsed_output", None)
