"""EXTRAPOLATE and CREATE must follow fp.exe semantics, or scenario edits vanish.

Both behaviours below were checked against fp.exe on the stock model:

- EXTRAPOLATE (fp.for CRX) overwrites every period of the SMPL window with the
  value one period before it. It does not just fill missing cells. If fppy only
  fills gaps, values set earlier in the window survive in fppy but not in fp.exe.
- CREATE runs once, when the deck reaches it. It is not part of the solved model
  (GENR is: fp.exe re-evaluates GENR over the solve window). Replaying a CREATE
  during SOLVE over its command-time SMPL resets the variable. That drops later
  CHANGEVAR shocks and the values solved for earlier periods.
"""

from __future__ import annotations

import pandas as pd
import pytest

from fppy.cli import _records_for_eq_iteration
from fppy.eq_solver import apply_eq_backfill
from fppy.executor import SampleWindow
from fppy.mini_run import run_mini_run
from fppy.parser import FPCommand, parse_fminput

_INDEX = pd.Index([f"{year}.{q}" for year in (2020, 2021) for q in (1, 2, 3, 4)])


def _run(deck: str) -> pd.DataFrame:
    result = run_mini_run(parse_fminput(deck), pd.DataFrame(index=_INDEX))
    assert result.issues == ()
    return result.frame


def test_extrapolate_overwrites_window_with_prior_period_value() -> None:
    frame = _run(
        "SMPL 2020.1 2021.4;\n"
        "CREATE X=0;\n"
        "SMPL 2021.1 2021.4;\n"
        "GENR X=7;\n"
        "SMPL 2020.4 2020.4;\n"
        "GENR X=3;\n"
        "SMPL 2021.1 2021.4;\n"
        "EXTRAPOLATE;\n"
    )
    assert frame.loc["2020.4":"2021.4", "X"].tolist() == [3.0] * 5


def test_extrapolate_variable_option_touches_only_that_variable() -> None:
    frame = _run(
        "SMPL 2020.1 2021.4;\n"
        "CREATE X=1;\n"
        "CREATE W=1;\n"
        "SMPL 2021.1 2021.4;\n"
        "GENR X=5;\n"
        "GENR W=5;\n"
        "EXTRAPOLATE VARIABLE=W;\n"
    )
    assert frame.loc["2021.1":"2021.4", "W"].tolist() == [1.0] * 4
    assert frame.loc["2021.1":"2021.4", "X"].tolist() == [5.0] * 4


_SOLVE_DECK = """SMPL 2020.1 2021.4;
CREATE XCR=5;
CREATE XG=0;
SMPL 2020.4 2020.4;
GENR XG=4;
SMPL 2020.4 2021.4;
CHANGEVAR;
XCR SAMEVALUE
8
XG SAMEVALUE
6
;
SMPL 2021.1 2021.4;
SOLVE;
"""


def _solve_period_by_period(records: list, frame: pd.DataFrame, create_lines: frozenset[int]):
    # Mirrors the mini-run solve loop: one scope per period, CREATE kept only in
    # the records handed to the first iteration of each scope.
    for period in ("2021.1", "2021.2", "2021.3", "2021.4"):
        for iteration in (1, 2):
            iteration_records = _records_for_eq_iteration(
                records,
                iteration=iteration,
                context_assignments_first_iter_only=False,
                pre_executed_create_lines=create_lines,
            )
            frame = apply_eq_backfill(
                iteration_records,
                frame,
                {},
                period_sequential=True,
                windows_override=(SampleWindow(period, period),),
            ).frame
    return frame


def test_solve_does_not_replay_executed_create() -> None:
    records = parse_fminput(_SOLVE_DECK)
    pre_solve = run_mini_run(records, pd.DataFrame(index=_INDEX))
    assert pre_solve.issues == ()
    create_lines = frozenset(r.line_number for r in records if r.command == FPCommand.CREATE)
    assert create_lines <= set(pre_solve.executed_line_numbers)

    solved = _solve_period_by_period(records, pre_solve.frame, create_lines)

    # fp.exe: the CHANGEVAR value survives the solve...
    assert solved.loc["2020.4":"2021.4", "XCR"].tolist() == [8.0] * 5
    # ...and GENR XG=4 is re-evaluated in every solve period, not just the last.
    assert solved.loc["2020.4", "XG"] == 6.0
    assert solved.loc["2021.1":"2021.4", "XG"].tolist() == [4.0] * 4


@pytest.mark.parametrize("iteration", [1, 2])
def test_create_not_executed_before_solve_is_still_replayed_once(iteration: int) -> None:
    # A CREATE the pre-solve pass never ran (e.g. filtered runs) still needs its
    # first-iteration replay; later iterations never replay CREATE.
    records = parse_fminput(_SOLVE_DECK)
    kept = _records_for_eq_iteration(
        records,
        iteration=iteration,
        context_assignments_first_iter_only=False,
        pre_executed_create_lines=frozenset(),
    )
    has_create = any(r.command == FPCommand.CREATE for r in kept)
    assert has_create is (iteration == 1)


def test_unparseable_smpl_is_reported_and_keeps_previous_window() -> None:
    # An SMPL fppy cannot parse used to drop the window, so the next GENR ran
    # over every period with no issue reported.
    records = parse_fminput(
        "SMPL 2020.1 2021.4;\nCREATE X=0;\nSMPL 2021.1 2021.4;\nSMPL 2021.1,2021.2;\nGENR X=1;\n"
    )
    result = run_mini_run(records, pd.DataFrame(index=_INDEX))
    assert [issue.line_number for issue in result.issues] == [4]
    assert result.frame["X"].tolist() == [0.0] * 4 + [1.0] * 4


def test_setupsolve_after_solve_does_not_configure_that_solve() -> None:
    # fp.exe applies SETUPSOLVE options to later SOLVEs only; the mini-run passes
    # the active SOLVE line as before_line.
    from fppy.solve_setup import extract_setupsolve_config

    records = parse_fminput(
        "SETUPSOLVE MINITERS=40 MAXCHECK=30;\n"
        "SMPL 2021.1 2021.4;\n"
        "SOLVE DYNAMIC;\n"
        "SETUPSOLVE MINITERS=3 TOLALL=0.5;\n"
    )
    solve_line = next(r.line_number for r in records if r.command == FPCommand.SOLVE)
    config = extract_setupsolve_config(records, before_line=solve_line)
    assert (config.miniters, config.maxcheck, config.tolall) == (40, 30, None)
