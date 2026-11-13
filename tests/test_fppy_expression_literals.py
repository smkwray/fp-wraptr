"""Valid FP expressions must evaluate the same in the series and per-period paths.

The solver evaluates equations one period at a time; GENR runs over whole
series. If the two paths parse a statement differently, the same deck gives
different numbers (or a crash) depending on where the statement is evaluated.
"""

from __future__ import annotations

import math

import pandas as pd
import pytest

from fppy.eq_solver import apply_eq_backfill
from fppy.expressions import evaluate_expression, evaluate_expression_at_period
from fppy.parser import FPCommand, FPCommandRecord

_DATA = pd.DataFrame({"Y": [1.0, 2.0]}, index=["2025.4", "2026.1"])


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        # Intrinsics with an integer literal argument are calls, not lags of a
        # variable named LOG/EXP.
        ("Y*LOG(10)", 2.0 * math.log(10.0)),
        ("EXP(1)+Y", math.e + 2.0),
        ("ABS(-3)+Y", 5.0),
        # FORTRAN-style literal with a bare mantissa dot: "E" is an exponent marker.
        ("Y(-1)+1.E-3", 1.001),
    ],
)
def test_series_and_period_paths_agree_on_literals(expression: str, expected: float) -> None:
    series_value = float(evaluate_expression(expression, data=_DATA).loc["2026.1"])
    period_value = evaluate_expression_at_period(expression, data=_DATA, period="2026.1")
    assert series_value == pytest.approx(expected)
    assert period_value == pytest.approx(expected)


def test_lead_of_variable_is_still_a_lag_reference() -> None:
    # Guard against the intrinsic exclusion swallowing ordinary leads/lags.
    data = pd.DataFrame({"LOGX": [1.0, 5.0]}, index=["2025.4", "2026.1"])
    assert evaluate_expression_at_period("LOGX(-1)", data=data, period="2026.1") == 1.0
    assert evaluate_expression_at_period("LOGX(1)", data=data, period="2025.4") == 5.0


def _record(line_number: int, command: FPCommand, statement: str) -> FPCommandRecord:
    return FPCommandRecord(
        line_number=line_number,
        line=statement,
        command=command,
        statement=statement,
        raw_lines=(statement,),
        terminated=True,
    )


def test_lagged_arithmetic_failure_is_reported_by_solver() -> None:
    # fp.exe (TURBO2) treats a zero divisor as a solution error. The per-period
    # solver must report it, not silently keep the previous value.
    frame = pd.DataFrame(
        {"X": [0.0, 0.0], "Y": [1.0, 1.0], "Z": [7.0, 7.0]}, index=["2025.4", "2026.1"]
    )
    records = [
        _record(1, FPCommand.SMPL, "SMPL 2026.1 2026.1 ;"),
        _record(2, FPCommand.GENR, "GENR Z = Y(-1)/X(-1) ;"),
    ]
    result = apply_eq_backfill(
        records, frame, {}, period_sequential=True, period_sequential_all_assignments=True
    )
    assert result.frame.loc["2026.1", "Z"] == 7.0
    assert result.issues
    assert any("division by zero" in issue.error for issue in result.issues)
