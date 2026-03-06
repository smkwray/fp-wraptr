"""Execution planning and windowed assignment application for FP commands."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

import numpy as np
import pandas as pd

from fppy.dependency import extract_definition_steps, order_definition_steps
from fppy.expressions import EvalContext, evaluate_expression, parse_assignment
from fppy.parser import FPCommand, FPCommandRecord

_SMPL_RE = re.compile(
    r"^\s*SMPL\s+(?P<start>[^\s;]+)\s+(?P<end>[^\s;]+)\s*;\s*$",
    re.IGNORECASE,
)
_PERIOD_RE = re.compile(r"^\s*(?P<year>\d+)(?:\.(?P<subperiod>\d+))?\s*$")


@dataclass(frozen=True)
class SampleWindow:
    """Inclusive sample period window."""

    start: str
    end: str


@dataclass(frozen=True)
class PlannedCommand:
    """A parsed assignment command tied to the active sample window."""

    line_number: int
    command: FPCommand
    statement: str
    window: SampleWindow | None
    dependency_reordered: bool = False


def parse_smpl_statement(statement: str) -> SampleWindow:
    """Parse an `SMPL start end;` statement."""

    match = _SMPL_RE.match(statement)
    if not match:
        raise ValueError(f"invalid SMPL statement: {statement!r}")

    start = match.group("start")
    end = match.group("end")
    return SampleWindow(start=start, end=end)


def _period_key(value: object) -> tuple[int, int]:
    text = str(value).strip()
    match = _PERIOD_RE.match(text)
    if not match:
        raise ValueError(f"invalid period value: {value!r}")
    year = int(match.group("year"))
    subperiod = int(match.group("subperiod") or 0)
    return year, subperiod


def _window_mask(index: pd.Index, window: SampleWindow | None) -> pd.Series:
    if window is None:
        return pd.Series(True, index=index)

    start_key = _period_key(window.start)
    end_key = _period_key(window.end)
    if start_key > end_key:
        raise ValueError(
            f"invalid SMPL window: start {window.start!r} is after end {window.end!r}"
        )

    keys = [_period_key(item) for item in index]
    return pd.Series([start_key <= key <= end_key for key in keys], index=index)


def build_execution_plan(
    records: Iterable[FPCommandRecord], *, initial_window: SampleWindow | None = None
) -> list[PlannedCommand]:
    """Build a sequential plan for assignment commands under active SMPL windows."""

    plan: list[PlannedCommand] = []
    active_window = initial_window
    window_records: list[FPCommandRecord] = []

    def flush_window_records() -> None:
        nonlocal window_records, plan
        if not window_records:
            return

        if active_window is None:
            for record in window_records:
                plan.append(
                    PlannedCommand(
                        line_number=record.line_number,
                        command=record.command,
                        statement=record.statement,
                        window=None,
                    )
                )
            window_records = []
            return

        try:
            steps = extract_definition_steps(window_records, include_eq=False)
            ordered = order_definition_steps(steps)
            original_order = [step.line_number for step in steps]
            for idx, step in enumerate(ordered):
                plan.append(
                    PlannedCommand(
                        line_number=step.line_number,
                        command=step.command,
                        statement=step.statement,
                        window=active_window,
                        dependency_reordered=step.line_number != original_order[idx],
                    )
                )
        except ValueError:
            for record in window_records:
                plan.append(
                    PlannedCommand(
                        line_number=record.line_number,
                        command=record.command,
                        statement=record.statement,
                        window=active_window,
                    )
                )

        window_records = []

    for record in records:
        if record.command == FPCommand.SMPL:
            flush_window_records()
            active_window = parse_smpl_statement(record.statement)
            continue

        if record.command in {FPCommand.GENR, FPCommand.IDENT, FPCommand.LHS, FPCommand.CREATE}:
            window_records.append(record)

    flush_window_records()

    return plan


def execute_plan(
    plan: Iterable[PlannedCommand],
    *,
    data: pd.DataFrame,
    inplace: bool = False,
    eval_context: EvalContext | None = None,
) -> pd.DataFrame:
    """Execute a pre-built plan over a DataFrame."""

    target = data if inplace else data.copy()

    for step in plan:
        assignment = parse_assignment(step.statement)
        evaluated = evaluate_expression(
            assignment.rhs,
            data=target,
            eval_context=eval_context,
        )
        mask = _window_mask(target.index, step.window)

        if assignment.lhs not in target.columns:
            target[assignment.lhs] = pd.Series(np.nan, index=target.index, dtype="float64")

        scoped = evaluated.loc[mask]
        existing = pd.to_numeric(target.loc[mask, assignment.lhs], errors="coerce")
        # Preserve existing values when the expression cannot be evaluated for a period
        # (for example lag-boundary rows in legacy FP scripts).
        target.loc[mask, assignment.lhs] = existing.where(scoped.isna(), scoped)

    return target


def execute_records(
    records: Iterable[FPCommandRecord],
    *,
    data: pd.DataFrame,
    inplace: bool = False,
    eval_context: EvalContext | None = None,
) -> pd.DataFrame:
    """Plan and execute parsed records in one step."""

    plan = build_execution_plan(records)
    return execute_plan(plan, data=data, inplace=inplace, eval_context=eval_context)
