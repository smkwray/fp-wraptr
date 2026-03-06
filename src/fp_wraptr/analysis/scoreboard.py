"""Macro scoreboard — standard derived metrics for every run.

Computes a small set of "brief-ready" metrics that economists expect:
GDP, unemployment, inflation, interest rates, deficit, and decompositions.
All metrics are computed as deviations from baseline by default.

Also includes a JG metrics pack for job guarantee policy analysis.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from fp_wraptr.io.parser import FPOutputData

__all__ = [
    "MetricValue",
    "Scoreboard",
    "compute_scoreboard",
]

# Standard variable sets used in metric packs
MACRO_CORE_VARS = ["GDPR", "UR", "PCY", "RS", "PIEF"]
GDP_COMPONENT_VARS = ["CS", "CN", "CD", "IHH", "IF", "EX", "IM"]
FISCAL_VARS = ["TRGHQ", "TRGSQ", "TBGQ", "SGP"]


@dataclass
class MetricValue:
    """A single metric value for one variable in one period."""

    variable: str
    period: str
    label: str
    baseline_level: float | None
    scenario_level: float
    deviation: float | None
    pct_deviation: float | None
    units: str = ""


@dataclass
class Scoreboard:
    """Collection of computed metrics for a scenario run."""

    scenario_name: str
    baseline_name: str = ""
    metrics: list[MetricValue] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def has_baseline(self) -> bool:
        return self.baseline_name != ""

    def get_variable(self, var_name: str) -> list[MetricValue]:
        """Get all metrics for a specific variable."""
        return [m for m in self.metrics if m.variable == var_name]

    def get_period(self, period: str) -> list[MetricValue]:
        """Get all metrics for a specific period."""
        return [m for m in self.metrics if m.period == period]

    def final_period_summary(self) -> dict[str, MetricValue]:
        """Get the last-period metric for each variable."""
        last: dict[str, MetricValue] = {}
        for m in self.metrics:
            if m.variable not in last or m.period > last[m.variable].period:
                last[m.variable] = m
        return last

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-compatible dict."""
        return {
            "scenario_name": self.scenario_name,
            "baseline_name": self.baseline_name,
            "has_baseline": self.has_baseline,
            "summary": self.summary,
            "metrics": [
                {
                    "variable": m.variable,
                    "period": m.period,
                    "label": m.label,
                    "baseline_level": m.baseline_level,
                    "scenario_level": m.scenario_level,
                    "deviation": m.deviation,
                    "pct_deviation": m.pct_deviation,
                    "units": m.units,
                }
                for m in self.metrics
            ],
        }


# Variable metadata for display
_VAR_LABELS: dict[str, tuple[str, str]] = {
    "GDPR": ("Real GDP", "B2017$"),
    "UR": ("Unemployment Rate", "%"),
    "PCY": ("Inflation (CPI)", "%"),
    "RS": ("3-Month T-Bill Rate", "%"),
    "RL": ("10-Year Bond Rate", "%"),
    "PIEF": ("GDP Deflator Inflation", "%"),
    "CS": ("Consumer Spending: Services", "B2017$"),
    "CN": ("Consumer Spending: Nondurables", "B2017$"),
    "CD": ("Consumer Spending: Durables", "B2017$"),
    "IHH": ("Residential Investment", "B2017$"),
    "IF": ("Nonresidential Fixed Investment", "B2017$"),
    "EX": ("Exports", "B2017$"),
    "IM": ("Imports", "B2017$"),
    "TRGHQ": ("Federal Transfers to Persons", "B$"),
    "TRGSQ": ("State Transfers to Persons", "B$"),
    "TBGQ": ("Federal Tax Receipts", "B$"),
    "SGP": ("Federal Surplus (+) or Deficit (-)", "B$"),
    "YS": ("Total Output (Production)", "B2017$"),
    "SR": ("Saving Rate", "%"),
}


def compute_scoreboard(
    scenario: FPOutputData,
    baseline: FPOutputData | None = None,
    scenario_name: str = "",
    baseline_name: str = "",
    variables: list[str] | None = None,
) -> Scoreboard:
    """Compute a macro scoreboard for a scenario run.

    Args:
        scenario: Parsed output from the scenario run.
        baseline: Optional parsed output from the baseline run.
            If provided, metrics are computed as deviations from baseline.
        scenario_name: Name of the scenario.
        baseline_name: Name of the baseline.
        variables: Variables to include. Defaults to MACRO_CORE_VARS.

    Returns:
        Scoreboard with computed metrics.
    """
    if variables is None:
        variables = MACRO_CORE_VARS

    scoreboard = Scoreboard(
        scenario_name=scenario_name,
        baseline_name=baseline_name,
    )

    periods = scenario.periods

    for var_name in variables:
        if var_name not in scenario.variables:
            continue

        scen_var = scenario.variables[var_name]
        label, units = _VAR_LABELS.get(var_name, (var_name, ""))

        base_var = None
        if baseline is not None and var_name in baseline.variables:
            base_var = baseline.variables[var_name]

        n_points = len(scen_var.levels)
        if base_var is not None:
            n_points = min(n_points, len(base_var.levels))
        n_points = min(n_points, len(periods))

        for i in range(n_points):
            scen_level = scen_var.levels[i]
            base_level = base_var.levels[i] if base_var is not None else None

            deviation = None
            pct_deviation = None
            if base_level is not None:
                deviation = round(scen_level - base_level, 6)
                if base_level != 0:
                    pct_deviation = round(100.0 * (scen_level - base_level) / abs(base_level), 4)

            scoreboard.metrics.append(
                MetricValue(
                    variable=var_name,
                    period=periods[i],
                    label=label,
                    baseline_level=base_level,
                    scenario_level=scen_level,
                    deviation=deviation,
                    pct_deviation=pct_deviation,
                    units=units,
                )
            )

    # Compute summary stats
    final = scoreboard.final_period_summary()
    scoreboard.summary = {
        var: {
            "final_level": m.scenario_level,
            "final_deviation": m.deviation,
            "final_pct_deviation": m.pct_deviation,
        }
        for var, m in final.items()
    }

    return scoreboard


def compute_jg_metrics(
    scenario: FPOutputData,
    baseline: FPOutputData,
    gross_cost_bn: float,
    scenario_name: str = "",
    baseline_name: str = "",
) -> dict[str, Any]:
    """Compute JG-specific derived metrics.

    Args:
        scenario: Parsed scenario output.
        baseline: Parsed baseline output.
        gross_cost_bn: Gross annual JG program cost in billions of dollars.
        scenario_name: Scenario name for labeling.
        baseline_name: Baseline name for labeling.

    Returns:
        Dict of JG-specific metrics.
    """
    result: dict[str, Any] = {
        "scenario_name": scenario_name,
        "baseline_name": baseline_name,
        "gross_cost_bn": gross_cost_bn,
    }

    # Cost as % of GDP (using final period)
    if "GDPR" in scenario.variables and scenario.variables["GDPR"].levels:
        final_gdp = scenario.variables["GDPR"].levels[-1]
        if final_gdp > 0:
            result["cost_pct_gdp"] = round(100.0 * gross_cost_bn / final_gdp, 2)

    # Net fiscal impact (change in deficit)
    if (
        "SGP" in scenario.variables
        and "SGP" in baseline.variables
        and scenario.variables["SGP"].levels
        and baseline.variables["SGP"].levels
    ):
        scen_deficit = scenario.variables["SGP"].levels[-1]
        base_deficit = baseline.variables["SGP"].levels[-1]
        # SGP is surplus; negative deviation means deficit widened
        result["net_fiscal_impact_bn"] = round(scen_deficit - base_deficit, 2)

    # Employment effect (UR deviation)
    if (
        "UR" in scenario.variables
        and "UR" in baseline.variables
        and scenario.variables["UR"].levels
        and baseline.variables["UR"].levels
    ):
        scen_ur = scenario.variables["UR"].levels[-1]
        base_ur = baseline.variables["UR"].levels[-1]
        result["ur_deviation_pp"] = round(scen_ur - base_ur, 3)

    # GDP multiplier (delta GDP / gross cost)
    if (
        "GDPR" in scenario.variables
        and "GDPR" in baseline.variables
        and scenario.variables["GDPR"].levels
        and baseline.variables["GDPR"].levels
    ):
        scen_gdp = scenario.variables["GDPR"].levels[-1]
        base_gdp = baseline.variables["GDPR"].levels[-1]
        gdp_delta = scen_gdp - base_gdp
        if gross_cost_bn > 0:
            result["gdp_multiplier"] = round(gdp_delta / gross_cost_bn, 3)

    # Inflation impact
    if (
        "PCY" in scenario.variables
        and "PCY" in baseline.variables
        and scenario.variables["PCY"].levels
        and baseline.variables["PCY"].levels
    ):
        scen_pcy = scenario.variables["PCY"].levels[-1]
        base_pcy = baseline.variables["PCY"].levels[-1]
        result["inflation_deviation_pp"] = round(scen_pcy - base_pcy, 3)

    return result
