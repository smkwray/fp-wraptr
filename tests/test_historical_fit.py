"""Tests for historical fit analysis module."""

from __future__ import annotations

from fp_wraptr.analysis.historical_fit import (
    EquationFitSummary,
    FitReport,
    build_fit_report,
)
from fp_wraptr.io.parser import (
    EstimationCoefficient,
    EstimationResult,
    FPOutputData,
)


def _make_estimation(
    eq_num: int,
    dep_var: str,
    r_squared: float = 0.95,
    dw: float = 2.0,
    n_coefs: int = 5,
    significant: int = 4,
) -> EstimationResult:
    coefficients = []
    for i in range(n_coefs):
        t_stat = 3.0 if i < significant else 1.0
        coefficients.append(
            EstimationCoefficient(
                var_id=i + 1,
                var_name=f"X{i}",
                lag=0,
                estimate=0.5,
                std_error=0.1,
                t_statistic=t_stat,
                mean=1.0,
            )
        )
    return EstimationResult(
        equation_number=eq_num,
        dependent_var=dep_var,
        sample_start="1954.1",
        sample_end="2025.3",
        n_obs=286,
        coefficients=coefficients,
        r_squared=r_squared,
        se_equation=0.01,
        durbin_watson=dw,
        mean_dep_var=5.0,
    )


def test_equation_fit_summary_pct_significant():
    summary = EquationFitSummary(
        equation_number=1,
        dependent_var="CS",
        sample_start="1954.1",
        sample_end="2025.3",
        n_obs=286,
        r_squared=0.95,
        se_equation=0.01,
        durbin_watson=2.0,
        n_coefficients=10,
        significant_coefficients=8,
        rho_iterations=1,
        mean_dep_var=5.0,
    )
    assert summary.pct_significant == 80.0


def test_equation_fit_summary_zero_coefficients():
    summary = EquationFitSummary(
        equation_number=1,
        dependent_var="CS",
        sample_start="",
        sample_end="",
        n_obs=0,
        r_squared=0.0,
        se_equation=0.0,
        durbin_watson=0.0,
        n_coefficients=0,
        significant_coefficients=0,
        rho_iterations=0,
        mean_dep_var=0.0,
    )
    assert summary.pct_significant == 0.0


def test_fit_report_properties():
    report = FitReport(
        equations=[
            EquationFitSummary(
                equation_number=1,
                dependent_var="CS",
                sample_start="",
                sample_end="",
                n_obs=100,
                r_squared=0.95,
                se_equation=0.01,
                durbin_watson=2.0,
                n_coefficients=5,
                significant_coefficients=5,
                rho_iterations=1,
                mean_dep_var=5.0,
            ),
            EquationFitSummary(
                equation_number=2,
                dependent_var="CN",
                sample_start="",
                sample_end="",
                n_obs=100,
                r_squared=0.70,
                se_equation=0.02,
                durbin_watson=1.2,
                n_coefficients=4,
                significant_coefficients=2,
                rho_iterations=1,
                mean_dep_var=3.0,
            ),
        ]
    )
    assert report.n_equations == 2
    assert report.avg_r_squared == (0.95 + 0.70) / 2
    assert len(report.weakest_equations) == 1
    assert report.weakest_equations[0].dependent_var == "CN"
    assert len(report.dw_flagged) == 1
    assert report.dw_flagged[0].dependent_var == "CN"


def test_fit_report_empty():
    report = FitReport()
    assert report.n_equations == 0
    assert report.avg_r_squared == 0.0
    assert report.avg_durbin_watson == 0.0
    assert report.weakest_equations == []
    assert report.dw_flagged == []


def test_build_fit_report():
    data = FPOutputData(
        estimations=[
            _make_estimation(1, "CS", r_squared=0.98, dw=1.9),
            _make_estimation(2, "CN", r_squared=0.92, dw=2.1),
            _make_estimation(3, "CD", r_squared=0.65, dw=1.3),
        ]
    )
    report = build_fit_report(data)
    assert report.n_equations == 3
    assert len(report.weakest_equations) == 1
    assert report.weakest_equations[0].dependent_var == "CD"
    assert len(report.dw_flagged) == 1


def test_fit_report_to_dict():
    data = FPOutputData(
        estimations=[
            _make_estimation(1, "CS", r_squared=0.98, dw=1.9, n_coefs=5, significant=4),
        ]
    )
    report = build_fit_report(data)
    d = report.to_dict()
    assert d["n_equations"] == 1
    assert "equations" in d
    assert d["equations"][0]["dependent_var"] == "CS"
    assert d["equations"][0]["pct_significant"] == 80.0
    assert "weakest_equations" in d
    assert "dw_flagged" in d


def test_build_fit_report_no_estimations():
    data = FPOutputData()
    report = build_fit_report(data)
    assert report.n_equations == 0
