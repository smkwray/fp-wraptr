"""Historical fit analysis — compare model estimation fit statistics.

Uses estimation results already parsed from fmout.txt (EstimationResult)
to summarize goodness-of-fit across all behavioral equations: R-squared,
standard error, Durbin-Watson, and coefficient significance.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fp_wraptr.io.parser import FPOutputData

__all__ = ["EquationFitSummary", "FitReport", "build_fit_report"]


@dataclass
class EquationFitSummary:
    """Fit summary for a single estimated equation."""

    equation_number: int
    dependent_var: str
    sample_start: str
    sample_end: str
    n_obs: int
    r_squared: float
    se_equation: float
    durbin_watson: float
    n_coefficients: int
    significant_coefficients: int
    rho_iterations: int
    mean_dep_var: float

    @property
    def pct_significant(self) -> float:
        """Percentage of coefficients with |t-stat| > 2."""
        if self.n_coefficients == 0:
            return 0.0
        return 100.0 * self.significant_coefficients / self.n_coefficients


@dataclass
class FitReport:
    """Summary fit report across all estimated equations."""

    equations: list[EquationFitSummary] = field(default_factory=list)

    @property
    def n_equations(self) -> int:
        return len(self.equations)

    @property
    def avg_r_squared(self) -> float:
        if not self.equations:
            return 0.0
        return sum(e.r_squared for e in self.equations) / len(self.equations)

    @property
    def avg_durbin_watson(self) -> float:
        if not self.equations:
            return 0.0
        return sum(e.durbin_watson for e in self.equations) / len(self.equations)

    @property
    def weakest_equations(self) -> list[EquationFitSummary]:
        """Equations with R-squared below 0.8, sorted ascending."""
        return sorted(
            [e for e in self.equations if e.r_squared < 0.8],
            key=lambda e: e.r_squared,
        )

    @property
    def dw_flagged(self) -> list[EquationFitSummary]:
        """Equations with DW statistic outside [1.5, 2.5] range."""
        return [e for e in self.equations if e.durbin_watson < 1.5 or e.durbin_watson > 2.5]

    def to_dict(self) -> dict:
        """Convert to a JSON-serializable dictionary."""
        return {
            "n_equations": self.n_equations,
            "avg_r_squared": round(self.avg_r_squared, 4),
            "avg_durbin_watson": round(self.avg_durbin_watson, 4),
            "equations": [
                {
                    "equation_number": e.equation_number,
                    "dependent_var": e.dependent_var,
                    "r_squared": e.r_squared,
                    "se_equation": e.se_equation,
                    "durbin_watson": e.durbin_watson,
                    "n_obs": e.n_obs,
                    "n_coefficients": e.n_coefficients,
                    "significant_coefficients": e.significant_coefficients,
                    "pct_significant": round(e.pct_significant, 1),
                    "sample": f"{e.sample_start} to {e.sample_end}",
                }
                for e in self.equations
            ],
            "weakest_equations": [
                {
                    "equation_number": e.equation_number,
                    "dependent_var": e.dependent_var,
                    "r_squared": e.r_squared,
                }
                for e in self.weakest_equations
            ],
            "dw_flagged": [
                {
                    "equation_number": e.equation_number,
                    "dependent_var": e.dependent_var,
                    "durbin_watson": e.durbin_watson,
                }
                for e in self.dw_flagged
            ],
        }


def build_fit_report(data: FPOutputData) -> FitReport:
    """Build a fit report from parsed FP output.

    Args:
        data: Parsed FP output containing estimation results.

    Returns:
        FitReport summarizing goodness-of-fit across all equations.
    """
    summaries: list[EquationFitSummary] = []

    for est in data.estimations:
        significant = sum(1 for c in est.coefficients if abs(c.t_statistic) > 2.0)
        summaries.append(
            EquationFitSummary(
                equation_number=est.equation_number,
                dependent_var=est.dependent_var,
                sample_start=est.sample_start,
                sample_end=est.sample_end,
                n_obs=est.n_obs,
                r_squared=est.r_squared,
                se_equation=est.se_equation,
                durbin_watson=est.durbin_watson,
                n_coefficients=len(est.coefficients),
                significant_coefficients=significant,
                rho_iterations=est.rho_iterations,
                mean_dep_var=est.mean_dep_var,
            )
        )

    return FitReport(equations=summaries)
