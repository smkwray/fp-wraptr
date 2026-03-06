"""Deterministic EQ backfill using fmout equation coefficient tables."""

from __future__ import annotations

import math
import re
import struct
import warnings
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from pandas.errors import PerformanceWarning

from fppy.executor import SampleWindow, parse_smpl_statement
from fppy.expressions import (
    Assignment,
    EvalContext,
    evaluate_expression,
    evaluate_expression_at_period,
    parse_assignment,
)
from fppy.parser import FPCommand, FPCommandRecord
from fppy.runtime_commands import ExogenousCommand, parse_runtime_command


@dataclass(frozen=True)
class EqTerm:
    """A single coefficient term from an fmout equation specification."""

    variable: str
    coefficient: float
    lag: int = 0
    index: int | None = None


@dataclass(frozen=True)
class EqSpec:
    """Parsed coefficient specification for one equation."""

    lhs: str
    terms: tuple[EqTerm, ...]
    equation_number: int | None = None


@dataclass(frozen=True)
class EqIssue:
    """A deterministic issue captured during backfill application."""

    line_number: int
    statement: str
    error: str


@dataclass(frozen=True)
class EqBackfillResult:
    """Result payload from EQ/LHS backfill execution."""

    frame: pd.DataFrame
    applied: int
    failed: int
    issues: tuple[EqIssue, ...]
    modeq_fsr_updates: int = 0
    modeq_fsr_active_equation_count: int = 0
    modeq_fsr_active_term_count: int = 0
    eq_fsr_skipped: int = 0
    modeq_fsr_side_channel_mode: str = "off"
    modeq_fsr_side_channel_status: str = "disabled"
    modeq_fsr_side_channel_events: tuple[dict[str, object], ...] = ()
    modeq_fsr_side_channel_events_truncated: bool = False
    modeq_fsr_side_channel_breach_count: int = 0
    modeq_fsr_side_channel_mutation_count: int = 0
    modeq_fsr_side_channel_first_breach: dict[str, object] | None = None
    modeq_fsr_side_channel_effective_limit: int = 0
    rho_resid_iteration_state: tuple[tuple[int, float], ...] = ()
    rho_resid_iteration_state_keyed: tuple[tuple[int, int, float], ...] = ()
    rho_resid_iteration_state_positioned: tuple[tuple[int, float, int], ...] = ()
    rho_resid_iteration_state_positioned_keyed: tuple[tuple[int, int, float, int], ...] = ()
    rho_resid_ar1_trace_events: tuple[dict[str, object], ...] = ()
    rho_resid_ar1_trace_events_truncated: bool = False
    rho_resid_ar1_focus_trace_spec: dict[str, object] | None = None
    rho_resid_ar1_focus_trace_events: tuple[dict[str, object], ...] = ()
    rho_resid_ar1_focus_trace_events_truncated: bool = False
    rho_resid_ar1_focus_trace_no_match: bool = False
    rho_resid_ar1_focus_trace_event_counts_by_stage: tuple[tuple[str, int], ...] = ()
    context_replay_trace_events: tuple[dict[str, object], ...] = ()
    context_replay_trace_events_truncated: bool = False


_RHO_RESID_TRACE_FOCUS_STAGE_ORDER: dict[str, int] = {
    "resid_eval": 0,
    "ar1_state_write": 1,
    "la257_u_phase_update": 2,
    "convergence_check": 3,
}
_RHO_RESID_TRACE_FOCUS_DEFAULT_STAGES = frozenset(_RHO_RESID_TRACE_FOCUS_STAGE_ORDER.keys())


@dataclass(frozen=True)
class Ar1TraceFocusSpec:
    """Focused residual-carry trace filter for compact lifecycle diagnostics."""

    targets: frozenset[str]
    periods: frozenset[str]
    iter_min: int = 1
    iter_max: int = 5
    max_events: int = 200
    stages: frozenset[str] = _RHO_RESID_TRACE_FOCUS_DEFAULT_STAGES


@dataclass(frozen=True)
class _DeferredStep:
    """One period-sequential step to execute in record order."""

    kind: str
    record: FPCommandRecord
    window: SampleWindow | None
    target: str
    eq_spec: EqSpec | None = None
    assignment: Assignment | None = None


@dataclass
class _RhoResidualState:
    """Mutable residual-carry state for residual-style AR(1) replay."""

    rho_lag1: float
    residual: float | None = None
    residual_position: int | None = None
    transient_u: float = 0.0
    transient_u_position: int | None = None
    u_phase_gain_anchor_abs: float | None = None


_RhoResidSeedKey = int | tuple[int, int]
_LineOccurrenceSelector = int | tuple[int, int]

_HEADER_RE = re.compile(
    r"^\s*(?:"
    r"(?P<lhs_a>[A-Za-z_][A-Za-z0-9_]*)\s+EQUATION\s+(?P<number_a>\d+)"
    r"|"
    r"EQUATION\s+(?P<number_b>\d+)\s+(?P<lhs_b>[A-Za-z_][A-Za-z0-9_]*)"
    r")",
    re.IGNORECASE,
)
_TERM_RE = re.compile(
    r"^\s*(?P<coefficient>[+-]?\d*\.?\d+(?:[Ee][+-]?\d+)?)\s+(?P<index>\d+)\s+(?P<variable>[A-Za-z_][A-Za-z0-9_]*)\s*(?:\(\s*(?P<lag>[+-]?\d+)\s*\))?",
    re.IGNORECASE,
)
_EQ_RE = re.compile(
    r"^\s*EQ\s+(?P<number>\d+)(?:\s+(?P<token>[A-Za-z_][A-Za-z0-9_]*))?",
    re.IGNORECASE,
)
_MODEQ_RE = re.compile(
    r"^\s*MODEQ\s+(?P<number>\d+)(?P<body>.*)$",
    re.IGNORECASE,
)
_MODEQ_TERM_RE = re.compile(
    r"^\s*(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?:\(\s*(?P<lag>[+-]?\d+)\s*\))?\s*$",
    re.IGNORECASE,
)
_PERIOD_RE = re.compile(r"^\s*(?P<year>\d+)(?:\.(?P<subperiod>\d+))?\s*$")
_LA257_U_PHASE_RHO_MIN = 1e-6
_FLOAT64_LOG_MAX = float(np.log(np.finfo(np.float64).max))
_HF_LINE182_PRE_OVERFLOW_MARGIN = 2.0
_HF_LINE182_CANONICAL_RHS = "EXP(LHF1)*HF(-1)"
_BOUNDARY_MISSING_SENTINELS = frozenset({-99.0})


def _reset_rho_resid_state(state: _RhoResidualState) -> None:
    state.residual = None
    state.residual_position = None
    state.transient_u = 0.0
    state.transient_u_position = None
    state.u_phase_gain_anchor_abs = None


def _clear_rho_resid_transient_state(state: _RhoResidualState) -> None:
    state.transient_u = 0.0
    state.transient_u_position = None


def _clone_rho_resid_state(state: _RhoResidualState | None) -> _RhoResidualState | None:
    if state is None:
        return None
    return _RhoResidualState(
        rho_lag1=float(state.rho_lag1),
        residual=(float(state.residual) if state.residual is not None else None),
        residual_position=(
            int(state.residual_position) if state.residual_position is not None else None
        ),
        transient_u=float(state.transient_u),
        transient_u_position=(
            int(state.transient_u_position) if state.transient_u_position is not None else None
        ),
        u_phase_gain_anchor_abs=(
            float(state.u_phase_gain_anchor_abs)
            if state.u_phase_gain_anchor_abs is not None
            else None
        ),
    )


def _compute_rho_residual_update(
    *,
    structural_result: float,
    result_value: float,
    lhs_value: float | None,
    carry_damp_mode: str,
    update_source: str,
) -> float | None:
    if carry_damp_mode == "la257":
        if lhs_value is not None and not pd.isna(lhs_value):
            return float(lhs_value - structural_result)
        return None
    if update_source == "solved":
        return float(result_value - structural_result)
    if lhs_value is None or pd.isna(lhs_value):
        return None
    if update_source == "result":
        return float(lhs_value - result_value)
    return float(lhs_value - structural_result)


def _is_line182_hf_exp_lag_assignment(
    record: FPCommandRecord,
    assignment: Assignment | None,
) -> bool:
    if assignment is None:
        return False
    if record.command != FPCommand.LHS:
        return False
    if int(record.line_number) != 182:
        return False
    if str(assignment.lhs).strip().upper() != "HF":
        return False
    rhs_canonical = re.sub(r"\s+", "", str(assignment.rhs).strip()).upper()
    return rhs_canonical == _HF_LINE182_CANONICAL_RHS


def _probe_line182_hf_pre_overflow(
    data: pd.DataFrame,
    *,
    period_position: int,
) -> dict[str, object] | None:
    hf_column = _resolve_existing_column(data, "HF")
    lhf1_column = _resolve_existing_column(data, "LHF1")
    if hf_column is None or lhf1_column is None:
        return None
    if period_position <= 0 or period_position >= len(data.index):
        return None

    try:
        hf_lag = float(data[hf_column].iat[period_position - 1])
        lhf1 = float(data[lhf1_column].iat[period_position])
    except (TypeError, ValueError):
        return None

    log_abs_hf_lag: float | None = None
    log_product: float | None = None
    margin_to_overflow: float | None = None
    would_overflow = False
    if math.isfinite(hf_lag) and math.isfinite(lhf1) and hf_lag != 0.0:
        log_abs_hf_lag = float(math.log(abs(hf_lag)))
        log_product = float(log_abs_hf_lag + lhf1)
        margin_to_overflow = float(_FLOAT64_LOG_MAX - log_product)
        would_overflow = bool(log_product > (_FLOAT64_LOG_MAX - _HF_LINE182_PRE_OVERFLOW_MARGIN))

    return {
        "hf_lag": float(hf_lag),
        "lhf1": float(lhf1),
        "log_abs_hf_lag": log_abs_hf_lag,
        "log_product": log_product,
        "overflow_log_limit": float(_FLOAT64_LOG_MAX),
        "margin_to_overflow": margin_to_overflow,
        "would_overflow": bool(would_overflow),
        "hf_lag_source": "state_frame_t_minus_1",
        "hf_lag_period_position": int(period_position - 1),
    }


def parse_eq_specs_from_fmout_text(text: str) -> dict[str, EqSpec]:
    """Parse equation coefficient tables from fmout text into equation specs."""

    specs: dict[str, EqSpec] = {}
    current_lhs: str | None = None

    for line in text.splitlines():
        raw = line.strip()
        if not raw:
            continue

        header_match = _HEADER_RE.match(line)
        if header_match:
            lhs = header_match.group("lhs_a") or header_match.group("lhs_b")
            number = header_match.group("number_a") or header_match.group("number_b")
            if not lhs or not number:
                current_lhs = None
                continue
            current_lhs = str(lhs).upper()
            equation_number = int(number)
            specs[current_lhs] = EqSpec(lhs=current_lhs, terms=(), equation_number=equation_number)
            continue

        term_match = _TERM_RE.match(line)
        if term_match and current_lhs is not None:
            lhs = current_lhs
            if lhs not in specs:
                current_lhs = None
                continue

            index = int(term_match.group("index"))
            variable = term_match.group("variable").upper()
            coefficient = float(term_match.group("coefficient"))
            lag = int(term_match.group("lag") or 0)
            term = EqTerm(variable=variable, coefficient=coefficient, lag=lag, index=index)
            spec = specs[lhs]
            specs[lhs] = EqSpec(
                lhs=spec.lhs,
                terms=(*spec.terms, term),
                equation_number=spec.equation_number,
            )
            continue

        # Stop scanning the active block on unrelated lines.
        if current_lhs is not None:
            current_lhs = None

    return specs


def load_eq_specs_from_fmout(path: Path | str) -> dict[str, EqSpec]:
    """Load equation specs from an fmout text file."""

    fmout_path = Path(path)
    return parse_eq_specs_from_fmout_text(fmout_path.read_text(encoding="utf-8", errors="replace"))


def build_coef_table(specs: dict[str, EqSpec]) -> dict[tuple[int, int], float]:
    """Build COEF(i,j)-style lookup table from parsed equation specs."""

    coef_values: dict[tuple[int, int], float] = {}
    for spec in specs.values():
        if spec.equation_number is None:
            continue
        for term in spec.terms:
            if term.index is None:
                continue
            coef_values.setdefault((term.index, spec.equation_number), term.coefficient)
    return coef_values


def apply_eq_backfill(
    records: Sequence[FPCommandRecord],
    data: pd.DataFrame,
    specs: dict[str, EqSpec],
    on_error: str = "continue",
    *,
    strict_missing_inputs: bool = True,
    strict_missing_assignments: bool = True,
    period_sequential: bool = False,
    period_sequential_fp_pass_order: bool = True,
    period_sequential_all_assignments: bool = False,
    period_sequential_defer_create: bool = False,
    period_sequential_context_replay_command_time_smpl: bool = False,
    period_sequential_context_replay_command_time_smpl_clip_to_solve_window: bool = False,
    period_sequential_context_retain_overlap_only: bool = False,
    period_sequential_context_replay_trace: bool = False,
    period_sequential_context_replay_trace_max_events: int = 200,
    period_sequential_context_replay_trace_targets: frozenset[str] = frozenset(),
    period_sequential_context_replay_trace_periods: frozenset[str] = frozenset(),
    period_sequential_assignment_targets: frozenset[str] = frozenset(),
    period_sequential_eq_eval_precision: str = "float64",
    period_sequential_eq_term_order: str = "as_parsed",
    period_sequential_eq_read_mode: str = "live",
    period_sequential_assignment_math_backend: str = "numpy",
    period_sequential_eq_commit_quantize: str = "off",
    windows_override: tuple[SampleWindow, ...] | None = None,
    modeq_fsr_side_channel_mode: str = "off",
    modeq_fsr_side_channel_max_events: int = 200,
    rho_aware: bool = False,
    rho_resid_ar1: bool = False,
    rho_resid_iteration_seed: dict[object, object] | None = None,
    rho_resid_iteration_seed_lag: int = 1,
    rho_resid_iteration_seed_mode: str = "legacy",
    rho_resid_boundary_reset: str = "off",
    rho_resid_carry_lag: int = 0,
    rho_resid_carry_damp: float = 1.0,
    rho_resid_carry_damp_mode: str = "term",
    rho_resid_carry_multipass: bool = False,
    rho_resid_lag_gating: str = "off",
    rho_resid_update_source: str = "structural",
    rho_resid_la257_update_rule: str = "legacy",
    rho_resid_la257_u_phase_max_gain: float | None = None,
    rho_resid_la257_u_phase_max_gain_mode: str = "relative",
    rho_resid_la257_fortran_cycle_carry_style: str = "legacy",
    rho_resid_la257_staged_lifecycle: bool = False,
    rho_resid_la257_u_phase_lines: Sequence[object] | None = None,
    rho_resid_commit_after_check: bool = False,
    rho_resid_trace_lines: Sequence[object] | None = None,
    rho_resid_trace_event_limit: int = 200,
    rho_resid_trace_iteration: int | None = None,
    rho_resid_trace_focus_spec: Ar1TraceFocusSpec | None = None,
    rho_resid_fpe_trap: bool = False,
    eval_context: EvalContext | None = None,
) -> EqBackfillResult:
    """Apply fmout-derived EQ/LHS backfill to records over an input DataFrame."""

    if on_error not in {"continue", "stop"}:
        raise ValueError("on_error must be 'continue' or 'stop'")
    if modeq_fsr_side_channel_mode not in {"off", "capture", "enforce"}:
        raise ValueError("modeq_fsr_side_channel_mode must be 'off', 'capture', or 'enforce'")
    if modeq_fsr_side_channel_max_events < 0:
        raise ValueError("modeq_fsr_side_channel_max_events must be nonnegative")
    if rho_aware and rho_resid_ar1:
        raise ValueError("rho_aware and rho_resid_ar1 cannot both be enabled")
    if period_sequential_eq_eval_precision not in {"float64", "longdouble"}:
        raise ValueError("period_sequential_eq_eval_precision must be 'float64' or 'longdouble'")
    resolved_term_order = str(period_sequential_eq_term_order).strip().lower()
    if resolved_term_order not in {"as_parsed", "by_index"}:
        raise ValueError("period_sequential_eq_term_order must be 'as_parsed' or 'by_index'")
    resolved_eq_read_mode = str(period_sequential_eq_read_mode).strip().lower()
    if resolved_eq_read_mode not in {"live", "frozen"}:
        raise ValueError("period_sequential_eq_read_mode must be 'live' or 'frozen'")
    resolved_assignment_math_backend = (
        str(period_sequential_assignment_math_backend).strip().lower()
    )
    if resolved_assignment_math_backend not in {"numpy", "math"}:
        raise ValueError("period_sequential_assignment_math_backend must be 'numpy' or 'math'")
    if str(period_sequential_eq_commit_quantize).strip().lower() not in {"off", "float32"}:
        raise ValueError("period_sequential_eq_commit_quantize must be 'off' or 'float32'")
    if period_sequential_defer_create and not period_sequential:
        raise ValueError("period_sequential_defer_create requires period_sequential")
    # fp_pass_order defaults to True; silently ignore when period_sequential
    # is False rather than raising an error.
    if period_sequential_context_replay_command_time_smpl and not period_sequential:
        raise ValueError(
            "period_sequential_context_replay_command_time_smpl requires period_sequential"
        )
    if (
        period_sequential_context_replay_command_time_smpl_clip_to_solve_window
        and not period_sequential
    ):
        raise ValueError(
            "period_sequential_context_replay_command_time_smpl_clip_to_solve_window "
            "requires period_sequential"
        )
    if period_sequential_context_retain_overlap_only and not period_sequential:
        raise ValueError(
            "period_sequential_context_retain_overlap_only requires period_sequential"
        )
    if period_sequential_context_replay_trace and not period_sequential:
        raise ValueError("period_sequential_context_replay_trace requires period_sequential")
    if int(period_sequential_context_replay_trace_max_events) < 0:
        raise ValueError("period_sequential_context_replay_trace_max_events must be nonnegative")
    normalized_context_trace_targets = frozenset(
        str(target).strip().upper()
        for target in period_sequential_context_replay_trace_targets
        if str(target).strip()
    )
    normalized_context_trace_periods = frozenset(
        str(period).strip()
        for period in period_sequential_context_replay_trace_periods
        if str(period).strip()
    )
    if (
        normalized_context_trace_targets or normalized_context_trace_periods
    ) and not period_sequential_context_replay_trace:
        raise ValueError(
            "period_sequential_context_replay_trace_targets/periods require "
            "period_sequential_context_replay_trace"
        )
    if rho_resid_iteration_seed_lag < 0:
        raise ValueError("rho_resid_iteration_seed_lag must be nonnegative")
    if rho_resid_carry_lag < 0:
        raise ValueError("rho_resid_carry_lag must be nonnegative")
    if (
        not math.isfinite(float(rho_resid_carry_damp))
        or rho_resid_carry_damp < 0.0
        or rho_resid_carry_damp > 1.0
    ):
        raise ValueError("rho_resid_carry_damp must be finite and between 0.0 and 1.0")
    if rho_resid_carry_damp_mode not in {"term", "state", "sol4", "la257"}:
        raise ValueError("rho_resid_carry_damp_mode must be 'term', 'state', 'sol4', or 'la257'")
    if rho_resid_lag_gating not in {"off", "lhs", "all"}:
        raise ValueError("rho_resid_lag_gating must be 'off', 'lhs', or 'all'")
    if rho_resid_iteration_seed_mode not in {"legacy", "positioned"}:
        raise ValueError("rho_resid_iteration_seed_mode must be 'legacy' or 'positioned'")
    if rho_resid_boundary_reset not in {"off", "window"}:
        raise ValueError("rho_resid_boundary_reset must be 'off' or 'window'")
    if rho_resid_update_source not in {"structural", "result", "solved", "resid_pass"}:
        raise ValueError(
            "rho_resid_update_source must be 'structural', 'result', 'solved', or 'resid_pass'"
        )
    if rho_resid_la257_update_rule not in {"legacy", "fortran_u_phase", "fortran_cycle"}:
        raise ValueError(
            "rho_resid_la257_update_rule must be 'legacy', 'fortran_u_phase', or 'fortran_cycle'"
        )
    if rho_resid_la257_fortran_cycle_carry_style not in {"legacy", "ema"}:
        raise ValueError("rho_resid_la257_fortran_cycle_carry_style must be 'legacy' or 'ema'")
    if rho_resid_la257_u_phase_max_gain is not None and (
        not math.isfinite(float(rho_resid_la257_u_phase_max_gain))
        or float(rho_resid_la257_u_phase_max_gain) < 0.0
    ):
        raise ValueError("rho_resid_la257_u_phase_max_gain must be finite and nonnegative")
    if rho_resid_la257_u_phase_max_gain_mode not in {"relative", "anchored"}:
        raise ValueError("rho_resid_la257_u_phase_max_gain_mode must be 'relative' or 'anchored'")
    if rho_resid_la257_staged_lifecycle:
        if not rho_resid_ar1:
            raise ValueError("rho_resid_la257_staged_lifecycle requires rho_resid_ar1")
        if not period_sequential:
            raise ValueError("rho_resid_la257_staged_lifecycle requires period_sequential")
        if rho_resid_carry_damp_mode != "la257":
            raise ValueError(
                "rho_resid_la257_staged_lifecycle requires rho_resid_carry_damp_mode='la257'"
            )
        if rho_resid_update_source != "resid_pass":
            raise ValueError(
                "rho_resid_la257_staged_lifecycle requires rho_resid_update_source='resid_pass'"
            )
    if rho_resid_commit_after_check:
        if not rho_resid_ar1:
            raise ValueError("rho_resid_commit_after_check requires rho_resid_ar1")
        if not period_sequential:
            raise ValueError("rho_resid_commit_after_check requires period_sequential")
    if rho_resid_trace_event_limit < 0:
        raise ValueError("rho_resid_trace_event_limit must be nonnegative")
    if rho_resid_trace_iteration is not None and int(rho_resid_trace_iteration) < 1:
        raise ValueError("rho_resid_trace_iteration must be >= 1 when provided")
    normalized_focus_spec = _normalize_rho_resid_trace_focus_spec(rho_resid_trace_focus_spec)
    (
        rho_resid_la257_u_phase_line_set,
        rho_resid_la257_u_phase_occurrence_set,
    ) = _normalize_la257_u_phase_selectors(rho_resid_la257_u_phase_lines)
    (
        rho_resid_trace_line_set,
        rho_resid_trace_occurrence_set,
    ) = _normalize_line_occurrence_selectors(
        rho_resid_trace_lines,
        field_name="rho_resid_trace_lines",
    )

    output = data.copy(deep=True)
    active_window: SampleWindow | None = None
    exogenous_windows_by_target: dict[str, list[SampleWindow | None]] = {}
    active_specs = dict(specs)
    # Ensure every EQ LHS exists as a column before evaluation.
    #
    # Legacy FP can reference lagged LHS terms (and RHO terms) immediately. When
    # the input frame lacks an explicit pre-seeded series for the LHS, create it
    # as NaN so evaluation can proceed and the solver can populate values.
    for spec in active_specs.values():
        lhs = str(getattr(spec, "lhs", "") or "").strip()
        if not lhs:
            continue
        target = _resolve_existing_column(output, lhs) or lhs
        if target not in output.columns:
            output[target] = pd.Series(np.nan, index=output.index, dtype="float64")
    spec_by_number = _index_specs_by_number(active_specs)
    applied = 0
    failed = 0
    issues: list[EqIssue] = []
    deferred_steps: list[_DeferredStep] = []
    rho_resid_states: dict[int, _RhoResidualState | None] = {}
    rho_resid_result_states: dict[int, _RhoResidualState | None] = rho_resid_states
    modeq_fsr_terms_by_equation: dict[int, tuple[tuple[str, int], ...]] = {}
    modeq_fsr_updates = 0
    eq_fsr_skipped = 0
    side_channel_mode = str(modeq_fsr_side_channel_mode)
    side_channel_enabled = side_channel_mode != "off"
    side_channel_effective_limit = int(modeq_fsr_side_channel_max_events)
    side_channel_events: list[dict[str, object]] = []
    side_channel_events_truncated = False
    side_channel_breach_count = 0
    side_channel_mutation_count = 0
    side_channel_first_breach: dict[str, object] | None = None
    pending_modeq_fsr_equation: int | None = None
    rho_resid_ar1_trace_events: list[dict[str, object]] = []
    rho_resid_ar1_trace_events_truncated = False
    rho_resid_ar1_focus_trace_events: list[dict[str, object]] = []
    rho_resid_ar1_focus_trace_events_truncated = False
    context_replay_mutation_positions: dict[int, set[int]] = {}
    skipped_historical_create_targets: set[str] = set()
    rho_resid_ar1_focus_seq = 0
    rho_resid_ar1_focus_stage_keys: set[tuple[int, str, str, str]] = set()
    trace_iteration = (
        int(rho_resid_trace_iteration) if rho_resid_trace_iteration is not None else 0
    )
    focus_existing_targets: set[str] = set()
    if normalized_focus_spec is not None:
        for focus_target in normalized_focus_spec.targets:
            if _resolve_existing_column(output, focus_target) is not None:
                focus_existing_targets.add(str(focus_target))
    eq_fsr_line_numbers = collect_eq_fsr_command_lines(records)
    windows_override_bounds: tuple[tuple[tuple[int, int], tuple[int, int]], ...] | None = None
    assignment_window_override: SampleWindow | None = None
    if windows_override:
        bounds: list[tuple[tuple[int, int], tuple[int, int]]] = []
        invalid_windows: list[tuple[object, object]] = []
        for item in windows_override:
            start_key = _period_key(item.start)
            end_key = _period_key(item.end)
            if start_key <= end_key:
                bounds.append((start_key, end_key))
            else:
                invalid_windows.append((item.start, item.end))
        if invalid_windows:
            raise ValueError("windows_override contains invalid windows where start is after end")
        windows_override_bounds = tuple(bounds)
        assignment_window_override = windows_override[-1]

    def _append_issue(line_number: int, statement: str, error: str) -> bool:
        nonlocal failed
        issues.append(EqIssue(line_number=line_number, statement=statement, error=error))
        failed += 1
        return on_error == "stop"

    def _append_trace_event(event: dict[str, object]) -> None:
        nonlocal rho_resid_ar1_trace_events_truncated
        if rho_resid_trace_event_limit <= 0:
            return
        if len(rho_resid_ar1_trace_events) >= rho_resid_trace_event_limit:
            rho_resid_ar1_trace_events_truncated = True
            return
        rho_resid_ar1_trace_events.append(event)

    def _append_side_channel_event(
        event: dict[str, object],
        *,
        breach: bool = False,
        mutation: bool = False,
    ) -> None:
        nonlocal side_channel_events_truncated
        nonlocal side_channel_breach_count
        nonlocal side_channel_mutation_count
        nonlocal side_channel_first_breach
        if not side_channel_enabled:
            return

        payload = dict(event)
        payload["breach"] = bool(breach)
        payload["mutation"] = bool(mutation)
        if breach:
            side_channel_breach_count += 1
            if side_channel_first_breach is None:
                side_channel_first_breach = dict(payload)
        if mutation:
            side_channel_mutation_count += 1
        if side_channel_effective_limit <= 0:
            side_channel_events_truncated = True
            return
        if len(side_channel_events) >= side_channel_effective_limit:
            side_channel_events_truncated = True
            return
        side_channel_events.append(payload)

    def _resolve_side_channel_status() -> str:
        if not side_channel_enabled:
            return "disabled"
        if side_channel_breach_count > 0:
            return "aborted" if side_channel_mode == "enforce" else "violated"
        if side_channel_events_truncated:
            return "warn"
        return "ok"

    def _focus_targets_for_event(*, target: object, period: object) -> tuple[str, ...]:
        if normalized_focus_spec is None:
            return tuple()
        if (
            trace_iteration < normalized_focus_spec.iter_min
            or trace_iteration > normalized_focus_spec.iter_max
        ):
            return tuple()
        target_key = str(target).upper()
        period_key = str(period)
        if normalized_focus_spec.periods and period_key not in normalized_focus_spec.periods:
            return tuple()
        if target_key in normalized_focus_spec.targets:
            return (target_key,)
        if focus_existing_targets:
            return tuple(sorted(focus_existing_targets))
        return tuple()

    def _focus_matches_target_period(*, target: object, period: object) -> bool:
        return bool(_focus_targets_for_event(target=target, period=period))

    def _append_focus_event(event: dict[str, object]) -> None:
        nonlocal rho_resid_ar1_focus_seq
        nonlocal rho_resid_ar1_focus_trace_events_truncated
        if normalized_focus_spec is None:
            return
        stage = str(event.get("stage") or "")
        if stage not in normalized_focus_spec.stages:
            return
        base_target = str(event.get("target") or "").upper()
        period_key = str(event.get("period") or "")
        focus_targets = _focus_targets_for_event(target=base_target, period=period_key)
        if not focus_targets:
            return
        for focus_target in focus_targets:
            focus_key = (
                int(trace_iteration),
                str(period_key),
                str(focus_target),
                str(stage),
            )
            if focus_key in rho_resid_ar1_focus_stage_keys:
                continue
            if len(rho_resid_ar1_focus_trace_events) >= normalized_focus_spec.max_events:
                rho_resid_ar1_focus_trace_events_truncated = True
                return
            rho_resid_ar1_focus_seq += 1
            payload = dict(event)
            payload["target"] = str(focus_target)
            if base_target and base_target != focus_target:
                payload["lhs_target"] = str(base_target)
            payload["seq"] = int(rho_resid_ar1_focus_seq)
            rho_resid_ar1_focus_trace_events.append(payload)
            rho_resid_ar1_focus_stage_keys.add(focus_key)

    def _build_focus_trace_result_fields() -> tuple[
        dict[str, object] | None,
        tuple[dict[str, object], ...],
        bool,
        bool,
        tuple[tuple[str, int], ...],
    ]:
        if normalized_focus_spec is None:
            return None, tuple(), False, False, tuple()
        sorted_events = sorted(
            rho_resid_ar1_focus_trace_events,
            key=lambda event: (
                int(event.get("iter", 0)),
                str(event.get("period", "")),
                str(event.get("target", "")),
                _RHO_RESID_TRACE_FOCUS_STAGE_ORDER.get(
                    str(event.get("stage", "")),
                    len(_RHO_RESID_TRACE_FOCUS_STAGE_ORDER),
                ),
                int(event.get("seq", 0)),
            ),
        )
        stage_counts = Counter(
            str(event.get("stage", "")) for event in sorted_events if event.get("stage")
        )
        stage_count_rows = tuple(
            (stage, int(stage_counts[stage]))
            for stage in sorted(
                stage_counts.keys(),
                key=lambda key: _RHO_RESID_TRACE_FOCUS_STAGE_ORDER.get(key, 99),
            )
        )
        spec_payload = {
            "targets": sorted(normalized_focus_spec.targets),
            "periods": sorted(normalized_focus_spec.periods),
            "iter_min": int(normalized_focus_spec.iter_min),
            "iter_max": int(normalized_focus_spec.iter_max),
            "max_events": int(normalized_focus_spec.max_events),
            "stages": sorted(
                normalized_focus_spec.stages,
                key=lambda key: _RHO_RESID_TRACE_FOCUS_STAGE_ORDER.get(key, 99),
            ),
        }
        return (
            spec_payload,
            tuple(sorted_events),
            bool(rho_resid_ar1_focus_trace_events_truncated),
            bool(len(sorted_events) == 0),
            stage_count_rows,
        )

    def _build_context_replay_trace_result_fields() -> tuple[
        tuple[dict[str, object], ...],
        bool,
    ]:
        if not period_sequential_context_replay_trace:
            return tuple(), False
        if not period_sequential or not deferred_steps:
            return tuple(), False

        max_events = int(period_sequential_context_replay_trace_max_events)
        if max_events <= 0:
            return tuple(), bool(any(context_replay_mutation_positions.values()))

        events: list[dict[str, object]] = []
        truncated = False
        for step_idx, step in enumerate(deferred_steps):
            if step.kind != "assignment":
                continue
            target = str(step.target)
            if (
                normalized_context_trace_targets
                and target.upper() not in normalized_context_trace_targets
            ):
                continue
            positions = sorted(context_replay_mutation_positions.get(step_idx, set()))
            if not positions:
                continue
            periods = [str(output.index[position]) for position in positions]
            if normalized_context_trace_periods:
                filtered_pairs = [
                    (int(position), period)
                    for position, period in zip(positions, periods, strict=False)
                    if period in normalized_context_trace_periods
                ]
                if not filtered_pairs:
                    continue
                positions = [int(position) for position, _ in filtered_pairs]
                periods = [period for _, period in filtered_pairs]
            if len(events) >= max_events:
                truncated = True
                break
            window_start = str(step.window.start) if step.window is not None else None
            window_end = str(step.window.end) if step.window is not None else None
            events.append({
                "line": int(step.record.line_number),
                "command": str(step.record.command.value),
                "target": target,
                "replay_window_start": window_start,
                "replay_window_end": window_end,
                "mutated_period_positions": positions,
                "mutated_periods": periods,
                "mutated_period_count": len(positions),
            })
        return tuple(events), bool(truncated)

    nonfinite_trace_fields = (
        "result",
        "structural",
        "ar_term",
        "residual_used",
        "state_residual_before",
        "state_residual_after",
        "state_transient_before",
        "state_transient_after",
        "lhs_value",
        "solved_value",
    )

    def _first_nonfinite_trace_detail(
        event: dict[str, object],
    ) -> tuple[str, float] | None:
        for field_name in nonfinite_trace_fields:
            raw = event.get(field_name)
            if raw is None or isinstance(raw, bool):
                continue
            try:
                value = float(raw)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(value):
                return field_name, value
        return None

    def _window_mask(
        index: pd.Index,
        window: SampleWindow | None,
        *,
        respect_windows_override: bool = True,
    ) -> pd.Series:
        if respect_windows_override and windows_override_bounds is not None:
            if not windows_override_bounds:
                return pd.Series(True, index=index)
            keys = [_period_key(item) for item in index]
            return pd.Series(
                [
                    any(start <= key <= end for start, end in windows_override_bounds)
                    for key in keys
                ],
                index=index,
            )

        if window is None:
            return pd.Series(True, index=index)

        start_key = _period_key(window.start)
        end_key = _period_key(window.end)
        if start_key > end_key:
            raise ValueError(
                f"invalid SMPL window: start {window.start!r} is after end {window.end!r}"
            )

        return pd.Series(
            [start_key <= _period_key(value) <= end_key for value in index], index=index
        )

    def _window_overlaps_windows_override(window: SampleWindow | None) -> bool:
        if not period_sequential_context_retain_overlap_only:
            return True
        if windows_override_bounds is None:
            return True
        if window is None:
            return True

        start_key = _period_key(window.start)
        end_key = _period_key(window.end)
        if start_key > end_key:
            raise ValueError(
                f"invalid SMPL window: start {window.start!r} is after end {window.end!r}"
            )
        return any(
            not (end_key < solve_start or start_key > solve_end)
            for solve_start, solve_end in windows_override_bounds
        )

    def _is_boundary_missing_value(value: object) -> bool:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return True
        if pd.isna(numeric):
            return True
        return numeric in _BOUNDARY_MISSING_SENTINELS

    def _seed_boundary_missing_from_lag() -> None:
        if not period_sequential_context_retain_overlap_only:
            return
        if not windows_override_bounds:
            return
        if not skipped_historical_create_targets:
            return

        index_keys = [_period_key(item) for item in output.index]
        boundary_positions: list[int] = []
        for position, key in enumerate(index_keys):
            in_solve_window = any(start <= key <= end for start, end in windows_override_bounds)
            if not in_solve_window:
                continue
            prev_in_solve_window = False
            if position > 0:
                prev_key = index_keys[position - 1]
                prev_in_solve_window = any(
                    start <= prev_key <= end for start, end in windows_override_bounds
                )
            if not prev_in_solve_window:
                boundary_positions.append(int(position))

        for position in boundary_positions:
            if position <= 0:
                continue
            for seed_target in sorted(skipped_historical_create_targets):
                resolved_target = _resolve_existing_column(output, seed_target)
                if resolved_target is None or resolved_target not in output.columns:
                    continue
                current_value = output[resolved_target].iat[position]
                if not _is_boundary_missing_value(current_value):
                    continue
                previous_value = output[resolved_target].iat[position - 1]
                if _is_boundary_missing_value(previous_value):
                    continue
                output.at[output.index[position], resolved_target] = previous_value

    for record in records:
        try:
            if record.command == FPCommand.SMPL:
                try:
                    active_window = parse_smpl_statement(record.statement)
                except Exception as exc:  # pragma: no cover - explicit parse fail path
                    if _append_issue(
                        record.line_number,
                        record.statement,
                        f"failed to parse SMPL statement: {exc}",
                    ):
                        break
                continue

            if record.command == FPCommand.EXOGENOUS:
                parsed = parse_runtime_command(record)
                if isinstance(parsed, ExogenousCommand) and parsed.variable:
                    target_key = str(parsed.variable).upper()
                    exogenous_windows_by_target.setdefault(target_key, []).append(active_window)
                    # Mirror the common Z/non-Z aliasing rules used elsewhere in the
                    # runtime so EXOGENOUS VARIABLE=FOO also protects FOOZ.
                    if target_key.endswith("Z") and len(target_key) > 1:
                        exogenous_windows_by_target.setdefault(target_key[:-1], []).append(
                            active_window
                        )
                    else:
                        exogenous_windows_by_target.setdefault(f"{target_key}Z", []).append(
                            active_window
                        )
                continue

            if (
                period_sequential
                # CREATE is data/setup in FP semantics (not part of the solve loop).
                # When we run EQ backfill scoped to a solve window (windows_override),
                # historical-only CREATE statements would otherwise be forced into the
                # solve window and incorrectly mutate forecast-boundary values.
                and record.command == FPCommand.CREATE
                and not _window_overlaps_windows_override(active_window)
            ):
                if period_sequential_context_retain_overlap_only:
                    try:
                        skipped_assignment = parse_assignment(record.statement)
                    except Exception:
                        skipped_assignment = None
                    if skipped_assignment is not None:
                        skipped_target = (
                            _resolve_existing_column(output, skipped_assignment.lhs)
                            or skipped_assignment.lhs
                        )
                        skipped_historical_create_targets.add(str(skipped_target))
                continue

            if record.command == FPCommand.EQ:
                pending_modeq_fsr_equation = None
                if record.line_number in eq_fsr_line_numbers:
                    eq_fsr_skipped += 1
                    _append_side_channel_event({
                        "kind": "eq_fsr_skip",
                        "line": int(record.line_number),
                        "statement": str(record.statement),
                    })
                    continue
                parsed_eq = _parse_eq_reference(record.statement)
                if parsed_eq is None:
                    if _append_issue(
                        record.line_number,
                        record.statement,
                        "unable to parse EQ statement",
                    ):
                        break
                    continue

                spec = _resolve_eq_spec(
                    parsed_eq, specs=active_specs, specs_by_number=spec_by_number
                )
                if spec is None:
                    if _is_none_equation_token(parsed_eq.token):
                        continue
                    if _append_issue(
                        record.line_number,
                        record.statement,
                        _missing_spec_error(parsed_eq),
                    ):
                        break
                    continue

                try:
                    if period_sequential:
                        deferred_steps.append(
                            _DeferredStep(
                                kind="eq",
                                record=record,
                                window=active_window,
                                target=_resolve_existing_column(output, spec.lhs) or spec.lhs,
                                eq_spec=spec,
                            )
                        )
                    else:
                        mask = _window_mask(output.index, active_window)
                        target = _resolve_existing_column(output, spec.lhs) or spec.lhs
                        values = _evaluate_eq_spec(
                            output,
                            spec,
                            strict_missing_inputs=strict_missing_inputs,
                            rho_aware=rho_aware,
                            rho_resid_ar1=rho_resid_ar1,
                            rho_resid_carry_lag=rho_resid_carry_lag,
                            rho_resid_carry_damp=rho_resid_carry_damp,
                            rho_resid_carry_damp_mode=rho_resid_carry_damp_mode,
                            rho_resid_carry_multipass=rho_resid_carry_multipass,
                            rho_resid_lag_gating=rho_resid_lag_gating,
                            rho_resid_update_source=rho_resid_update_source,
                            rho_resid_la257_update_rule="legacy",
                            rho_resid_la257_fortran_cycle_carry_style=(
                                rho_resid_la257_fortran_cycle_carry_style
                            ),
                            rho_resid_la257_u_phase_max_gain=rho_resid_la257_u_phase_max_gain,
                            rho_resid_la257_u_phase_max_gain_mode=(
                                rho_resid_la257_u_phase_max_gain_mode
                            ),
                            mask=mask.to_numpy(dtype=bool),
                            rho_resid_fpe_trap=rho_resid_fpe_trap,
                        )
                        if target not in output.columns:
                            output[target] = pd.Series(np.nan, index=output.index, dtype="float64")
                        scoped = values.loc[mask]
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore", PerformanceWarning)
                            if rho_aware or rho_resid_ar1:
                                existing = pd.to_numeric(output.loc[mask, target], errors="coerce")
                                output.loc[mask, target] = existing.where(scoped.isna(), scoped)
                            else:
                                output.loc[mask, target] = scoped
                    applied += 1
                except Exception as exc:  # pragma: no cover - explicit parse/eval path
                    if _append_issue(
                        record.line_number,
                        record.statement,
                        f"EQ evaluation failed: {exc}",
                    ):
                        break
                continue

            if record.command == FPCommand.MODEQ:
                parsed_modeq = _parse_modeq_update(record.statement)
                if parsed_modeq is None:
                    if _append_issue(
                        record.line_number,
                        record.statement,
                        "unable to parse MODEQ statement",
                    ):
                        break
                    continue
                (
                    modeq_number,
                    modeq_adds,
                    modeq_subs,
                    modeq_fsr_adds,
                    modeq_fsr_subs,
                    has_inline_fsr,
                ) = parsed_modeq
                base_spec = spec_by_number.get(modeq_number)
                if base_spec is None:
                    if _append_issue(
                        record.line_number,
                        record.statement,
                        (f"missing equation specification for MODEQ equation {modeq_number}"),
                    ):
                        break
                    continue
                modeq_spec = _apply_modeq_update_to_spec(
                    base_spec,
                    add_terms=modeq_adds,
                    sub_terms=modeq_subs,
                )
                base_term_keys = {
                    (str(term.variable).upper(), int(term.lag)) for term in base_spec.terms
                }
                updated_term_keys = {
                    (str(term.variable).upper(), int(term.lag)) for term in modeq_spec.terms
                }
                fsr_add_keys = {(str(name).upper(), int(lag)) for name, lag in modeq_fsr_adds}
                fsr_sub_keys = {(str(name).upper(), int(lag)) for name, lag in modeq_fsr_subs}
                rhs_fsr_overlap_add = sorted(
                    item for item in (updated_term_keys - base_term_keys) if item in fsr_add_keys
                )
                rhs_fsr_overlap_sub = sorted(
                    item for item in (base_term_keys - updated_term_keys) if item in fsr_sub_keys
                )
                rhs_fsr_mutation_detected = bool(rhs_fsr_overlap_add or rhs_fsr_overlap_sub)
                _append_side_channel_event(
                    {
                        "kind": "modeq",
                        "line": int(record.line_number),
                        "equation": int(modeq_number),
                        "has_inline_fsr": bool(has_inline_fsr),
                        "rhs_add_count": len(modeq_adds),
                        "rhs_sub_count": len(modeq_subs),
                        "fsr_add_count": len(modeq_fsr_adds),
                        "fsr_sub_count": len(modeq_fsr_subs),
                        "rhs_fsr_mutation_detected": rhs_fsr_mutation_detected,
                        "rhs_fsr_overlap_add": rhs_fsr_overlap_add,
                        "rhs_fsr_overlap_sub": rhs_fsr_overlap_sub,
                    },
                    breach=rhs_fsr_mutation_detected,
                    mutation=rhs_fsr_mutation_detected,
                )
                if rhs_fsr_mutation_detected:
                    _append_issue(
                        record.line_number,
                        record.statement,
                        "MODEQ/FSR side-channel invariant breach: FSR terms mutated RHS terms",
                    )
                    if side_channel_mode == "enforce":
                        break
                active_specs[modeq_spec.lhs] = modeq_spec
                if modeq_spec.equation_number is not None:
                    spec_by_number[modeq_spec.equation_number] = modeq_spec
                if has_inline_fsr:
                    modeq_fsr_terms_by_equation[modeq_number] = _apply_modeq_term_key_update(
                        modeq_fsr_terms_by_equation.get(modeq_number, tuple()),
                        add_terms=modeq_fsr_adds,
                        sub_terms=modeq_fsr_subs,
                    )
                    modeq_fsr_updates += len(modeq_fsr_adds) + len(modeq_fsr_subs)
                    pending_modeq_fsr_equation = None
                else:
                    pending_modeq_fsr_equation = modeq_number if not record.terminated else None
                continue

            if record.command == FPCommand.FSR:
                if pending_modeq_fsr_equation is not None:
                    fsr_adds, fsr_subs = _parse_fsr_terms(record.statement)
                    modeq_fsr_terms_by_equation[pending_modeq_fsr_equation] = (
                        _apply_modeq_term_key_update(
                            modeq_fsr_terms_by_equation.get(
                                pending_modeq_fsr_equation,
                                tuple(),
                            ),
                            add_terms=fsr_adds,
                            sub_terms=fsr_subs,
                        )
                    )
                    modeq_fsr_updates += len(fsr_adds) + len(fsr_subs)
                    _append_side_channel_event({
                        "kind": "modeq_fsr_clause",
                        "line": int(record.line_number),
                        "equation": int(pending_modeq_fsr_equation),
                        "fsr_add_count": len(fsr_adds),
                        "fsr_sub_count": len(fsr_subs),
                    })
                else:
                    _append_side_channel_event({
                        "kind": "fsr_passthrough",
                        "line": int(record.line_number),
                        "statement": str(record.statement),
                    })
                pending_modeq_fsr_equation = None
                continue

            if record.command in {FPCommand.LHS, FPCommand.GENR, FPCommand.IDENT}:
                pending_modeq_fsr_equation = None
                try:
                    assignment = parse_assignment(record.statement)
                    # In period-sequential mode, solver pass order is EQ -> LHS -> IDENT -> GENR.
                    # Replay all model-spec assignments every solve iteration.
                    defer_assignment = period_sequential and (
                        record.command == FPCommand.LHS
                        or period_sequential_all_assignments
                        or record.command in {FPCommand.GENR, FPCommand.IDENT}
                    )
                    if defer_assignment:
                        step_window = active_window
                        if (
                            record.command in {FPCommand.GENR, FPCommand.IDENT}
                            and assignment_window_override is not None
                            and not period_sequential_context_replay_command_time_smpl
                        ):
                            # Keep retained context assignments scoped to the solve
                            # window to avoid replaying them over historical samples.
                            step_window = assignment_window_override
                        target = (
                            _resolve_existing_column(output, assignment.lhs) or assignment.lhs
                        )
                        if target not in output.columns:
                            output[target] = pd.Series(np.nan, index=output.index, dtype="float64")
                        deferred_step = _DeferredStep(
                            kind="assignment",
                            record=record,
                            window=step_window,
                            target=target,
                            assignment=assignment,
                        )
                        deferred_steps.append(deferred_step)
                    else:
                        values = evaluate_expression(
                            assignment.rhs,
                            data=output,
                            raise_fp_errors=rho_resid_fpe_trap,
                            eval_context=eval_context,
                        )
                        mask = _window_mask(output.index, active_window)
                        target = _resolve_existing_column(output, assignment.lhs) or assignment.lhs
                        if target not in output.columns:
                            output[target] = pd.Series(np.nan, index=output.index, dtype="float64")
                        scoped = values.loc[mask]
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore", PerformanceWarning)
                            output.loc[mask, target] = scoped
                    applied += 1
                except Exception as exc:  # pragma: no cover - explicit parse/eval path
                    if not strict_missing_assignments and isinstance(exc, (KeyError, NameError)):
                        continue
                    if _append_issue(
                        record.line_number,
                        record.statement,
                        f"{record.command.value} evaluation failed: {exc}",
                    ):
                        break

            if record.command == FPCommand.CREATE:
                pending_modeq_fsr_equation = None
                try:
                    assignment = parse_assignment(record.statement)
                    # FP semantics: CREATE (TURBO type 1) is executed once at
                    # definition time and is NEVER replayed during SOLVE
                    # iterations.  Always evaluate immediately over the
                    # command-time SMPL window; never defer into the solve
                    # step list.  Use respect_windows_override=False so the
                    # mask is the original SMPL, not the solve window.
                    values = evaluate_expression(
                        assignment.rhs,
                        data=output,
                        raise_fp_errors=rho_resid_fpe_trap,
                        eval_context=eval_context,
                    )
                    mask = _window_mask(
                        output.index, active_window, respect_windows_override=False
                    )
                    target = _resolve_existing_column(output, assignment.lhs) or assignment.lhs
                    if target not in output.columns:
                        output[target] = pd.Series(np.nan, index=output.index, dtype="float64")
                    scoped = values.loc[mask]
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", PerformanceWarning)
                        output.loc[mask, target] = scoped
                    applied += 1
                except Exception as exc:  # pragma: no cover - explicit parse/eval path
                    if not strict_missing_assignments and isinstance(exc, (KeyError, NameError)):
                        continue
                    if _append_issue(
                        record.line_number,
                        record.statement,
                        f"{record.command.value} evaluation failed: {exc}",
                    ):
                        break

        except Exception as exc:  # pragma: no cover - defensive
            if _append_issue(record.line_number, record.statement, f"unexpected failure: {exc}"):
                break

    if period_sequential:
        _seed_boundary_missing_from_lag()

    if period_sequential:
        deferred_steps = tuple(deferred_steps)

    step_seed_keys: dict[int, tuple[int, int]] = {}
    if period_sequential and deferred_steps:
        eq_source_cache: dict[tuple[str, str], str | None] = {}
        exogenous_masks_by_target: dict[str, np.ndarray] = {}
        if exogenous_windows_by_target:
            for target_key, windows in exogenous_windows_by_target.items():
                mask = pd.Series(False, index=output.index)
                for window in windows:
                    # Exogenous windows are command-time SMPL windows; never
                    # override them with solve scoping.
                    mask = mask | _window_mask(
                        output.index,
                        window,
                        respect_windows_override=False,
                    )
                exogenous_masks_by_target[target_key] = mask.to_numpy(dtype=bool)
        step_masks = tuple(
            _window_mask(
                output.index,
                step.window,
                respect_windows_override=not (
                    period_sequential_context_replay_command_time_smpl
                    and step.kind == "assignment"
                ),
            ).to_numpy(dtype=bool)
            for step in deferred_steps
        )
        # FP solve pass order is EQ -> LHS -> IDENT -> GENR. The deck intermixes
        # these statements, but the legacy solver evaluates in pass-order during
        # each Gauss-Seidel iteration.  CREATE never enters the deferred list
        # (executed once at definition time per FP semantics).
        if period_sequential_fp_pass_order:
            order: list[tuple[int, int, int, int]] = []
            for step_idx, step in enumerate(deferred_steps):
                group = 9
                eq_number_rank = 0
                eq_number_value = 0
                if step.kind == "eq":
                    group = 0
                    # FP's RESID pass runs equations by equation-number order.
                    # Preserve deterministic deck-order fallback when missing.
                    if step.eq_spec is None or step.eq_spec.equation_number is None:
                        eq_number_rank = 1
                    else:
                        eq_number_value = int(step.eq_spec.equation_number)
                elif step.kind == "assignment":
                    cmd = step.record.command
                    if cmd == FPCommand.LHS:
                        group = 1
                    elif cmd == FPCommand.IDENT:
                        group = 2
                    elif cmd == FPCommand.GENR:
                        group = 3
                    elif cmd == FPCommand.CREATE:
                        group = 4
                    else:
                        group = 8
                order.append((group, eq_number_rank, eq_number_value, step_idx))
            step_order = [
                step_idx for _group, _eq_number_rank, _eq_number_value, step_idx in sorted(order)
            ]
        else:
            step_order = list(range(len(deferred_steps)))
        step_seed_keys = _build_rho_resid_step_seed_keys(deferred_steps)
        step_prev_active = [False] * len(deferred_steps)
        step_seen_active = [False] * len(deferred_steps)
        step_passes = [0] * len(deferred_steps)
        rho_resid_seed_values: dict[int, float] = {}
        normalized_seed = _normalize_rho_resid_iteration_seed(
            rho_resid_iteration_seed,
            mode=rho_resid_iteration_seed_mode,
        )
        if rho_resid_ar1:
            frame_length = len(output.index)
            for step_idx, step in enumerate(deferred_steps):
                if step.kind != "eq" or step.eq_spec is None:
                    continue
                rho_lag1 = _extract_rho_lag1(step.eq_spec)
                if rho_lag1 is not None:
                    state = _RhoResidualState(rho_lag1=float(rho_lag1))
                    seeded_residual = None
                    seeded_position = None
                    seed_entry = None
                    seed_key = step_seed_keys.get(step_idx)
                    if seed_key is not None:
                        seed_entry = normalized_seed.get(seed_key)
                    if seed_entry is None:
                        seed_entry = normalized_seed.get(int(step.record.line_number))
                    if seed_entry is not None:
                        seeded_residual, seeded_position = seed_entry
                    if seeded_residual is not None and not pd.isna(seeded_residual):
                        first_active_position = _first_true_position(step_masks[step_idx])
                        if first_active_position is not None:
                            state.residual = float(seeded_residual)
                            rho_resid_seed_values[step_idx] = float(seeded_residual)
                            # LA(257)-style mode treats inter-iteration seed as
                            # a pass-start carry value, so seed position should
                            # always map to one step before the first active
                            # period of the new pass.
                            if rho_resid_carry_damp_mode == "la257" and rho_resid_carry_multipass:
                                state.residual_position = first_active_position - 1
                            elif (
                                rho_resid_iteration_seed_mode == "positioned"
                                and seeded_position is not None
                                and 0 <= int(seeded_position) < frame_length
                            ):
                                state.residual_position = int(seeded_position)
                            else:
                                state.residual_position = first_active_position - int(
                                    rho_resid_iteration_seed_lag
                                )
                    rho_resid_states[step_idx] = state
                else:
                    rho_resid_states[step_idx] = None
        rho_resid_states_active = rho_resid_states
        if rho_resid_ar1 and rho_resid_commit_after_check:
            rho_resid_states_staged = {
                step_idx: _clone_rho_resid_state(state)
                for step_idx, state in rho_resid_states.items()
            }
        else:
            rho_resid_states_staged = rho_resid_states_active
        rho_resid_result_states = rho_resid_states_staged
        eq_read_frame = output
        if resolved_eq_read_mode == "frozen":
            # Jacobi-style probe: evaluate equations against the iteration-start state
            # so intra-pass writes are not visible to subsequent equation evaluations.
            eq_read_frame = output.copy(deep=True)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", PerformanceWarning)
            hf_line182_write_counts: dict[int, int] = {}
            solve_window_positions: list[int] | None = None
            if (
                period_sequential_context_replay_command_time_smpl_clip_to_solve_window
                and period_sequential_context_replay_command_time_smpl
                and windows_override_bounds
            ):
                index_keys = [_period_key(item) for item in output.index]
                solve_window_positions = [
                    int(position)
                    for position, key in enumerate(index_keys)
                    if any(start <= key <= end for start, end in windows_override_bounds)
                ]
            if solve_window_positions is None:
                period_positions = range(len(output.index))
            else:
                period_positions = solve_window_positions

            for period_position in period_positions:
                period = output.index[period_position]
                pending_resid_state_steps: dict[int, tuple[str, EqSpec, float]] = {}
                for step_idx in step_order:
                    step = deferred_steps[step_idx]
                    try:
                        is_hf_line182_assignment = False
                        is_active = bool(step_masks[step_idx][period_position])
                        if not is_active:
                            step_prev_active[step_idx] = False
                            continue
                        target_upper = str(step.target).upper()
                        exog_mask = exogenous_masks_by_target.get(target_upper)
                        if exog_mask is not None and bool(exog_mask[period_position]):
                            # EXOGENOUS variables should not be mutated by the solver.
                            continue
                        if not step_prev_active[step_idx]:
                            step_passes[step_idx] += 1
                        if (
                            rho_resid_ar1
                            and step.kind == "eq"
                            and rho_resid_boundary_reset == "window"
                            and not step_prev_active[step_idx]
                            and step_seen_active[step_idx]
                        ):
                            state_maps = [rho_resid_states_active]
                            if rho_resid_states_staged is not rho_resid_states_active:
                                state_maps.append(rho_resid_states_staged)
                            for state_map in state_maps:
                                state = state_map.get(step_idx)
                                if state is None:
                                    continue
                                seeded_value = rho_resid_seed_values.get(step_idx)
                                if (
                                    seeded_value is not None
                                    and rho_resid_carry_multipass
                                    and rho_resid_carry_damp_mode in {"state", "sol4", "la257"}
                                ):
                                    state.residual = seeded_value
                                    state.residual_position = period_position - 1
                                    _clear_rho_resid_transient_state(state)
                                else:
                                    _reset_rho_resid_state(state)
                        step_prev_active[step_idx] = True
                        step_seen_active[step_idx] = True
                        if step.kind == "eq":
                            if step.eq_spec is None:
                                continue
                            target = step.target
                            lhs_value = float("nan")
                            if rho_resid_ar1 and target in output.columns:
                                lhs_value = float(output[target].iat[period_position])
                            defer_resid_update = (
                                rho_resid_ar1
                                and rho_resid_update_source == "resid_pass"
                                and (
                                    rho_resid_carry_damp_mode != "la257"
                                    or rho_resid_la257_staged_lifecycle
                                )
                            )
                            line_number, occurrence = step_seed_keys.get(
                                step_idx,
                                (int(step.record.line_number), 0),
                            )
                            resolved_la257_rule = _resolve_la257_update_rule_for_line(
                                base_rule=rho_resid_la257_update_rule,
                                line_number=line_number,
                                occurrence=occurrence,
                                u_phase_lines=rho_resid_la257_u_phase_line_set,
                                u_phase_occurrences=rho_resid_la257_u_phase_occurrence_set,
                            )
                            trace_selected = bool(
                                rho_resid_ar1
                                and _selector_matches(
                                    line_number=line_number,
                                    occurrence=occurrence,
                                    line_selectors=rho_resid_trace_line_set,
                                    occurrence_selectors=rho_resid_trace_occurrence_set,
                                )
                            )
                            focus_selected = _focus_matches_target_period(
                                target=target,
                                period=period,
                            )
                            update_enabled = bool(
                                not defer_resid_update and not rho_resid_commit_after_check
                            )
                            debug_metrics: dict[str, object] | None = (
                                {}
                                if (
                                    trace_selected
                                    or focus_selected
                                    or (
                                        rho_resid_ar1
                                        and rho_resid_commit_after_check
                                        and not defer_resid_update
                                    )
                                )
                                else None
                            )
                            value = _evaluate_eq_spec_at_period(
                                eq_read_frame,
                                step.eq_spec,
                                period=period,
                                period_position=period_position,
                                eval_precision=period_sequential_eq_eval_precision,
                                term_order=resolved_term_order,
                                strict_missing_inputs=strict_missing_inputs,
                                source_cache=eq_source_cache,
                                rho_aware=rho_aware,
                                rho_resid_ar1=rho_resid_ar1,
                                rho_resid_carry_lag=rho_resid_carry_lag,
                                rho_resid_carry_damp=rho_resid_carry_damp,
                                rho_resid_carry_damp_mode=rho_resid_carry_damp_mode,
                                rho_resid_carry_multipass=rho_resid_carry_multipass,
                                rho_resid_lag_gating=rho_resid_lag_gating,
                                rho_resid_state=rho_resid_states_active.get(step_idx),
                                rho_resid_lhs_value=lhs_value,
                                rho_resid_update_source=rho_resid_update_source,
                                rho_resid_la257_update_rule=resolved_la257_rule,
                                rho_resid_la257_fortran_cycle_carry_style=(
                                    rho_resid_la257_fortran_cycle_carry_style
                                ),
                                rho_resid_la257_u_phase_max_gain=(
                                    rho_resid_la257_u_phase_max_gain
                                ),
                                rho_resid_la257_u_phase_max_gain_mode=(
                                    rho_resid_la257_u_phase_max_gain_mode
                                ),
                                rho_resid_update_enabled=update_enabled,
                                rho_resid_debug_metrics=debug_metrics,
                                rho_resid_fpe_trap=rho_resid_fpe_trap,
                            )
                            if (
                                rho_resid_ar1
                                and rho_resid_commit_after_check
                                and not defer_resid_update
                                and debug_metrics is not None
                            ):
                                staged_state = rho_resid_states_staged.get(step_idx)
                                if staged_state is not None:
                                    structural_raw = debug_metrics.get("structural")
                                    result_raw = debug_metrics.get("result")
                                    try:
                                        structural_value = float(structural_raw)
                                        result_value = float(result_raw)
                                    except (TypeError, ValueError):
                                        structural_value = None
                                        result_value = None
                                    residual_update: float | None = None
                                    if (
                                        structural_value is not None
                                        and result_value is not None
                                        and math.isfinite(structural_value)
                                        and math.isfinite(result_value)
                                    ):
                                        residual_update = _compute_rho_residual_update(
                                            structural_result=structural_value,
                                            result_value=result_value,
                                            lhs_value=lhs_value,
                                            carry_damp_mode=rho_resid_carry_damp_mode,
                                            update_source=rho_resid_update_source,
                                        )
                                    staged_before = (
                                        float(staged_state.residual)
                                        if staged_state.residual is not None
                                        else None
                                    )
                                    staged_position_before = (
                                        int(staged_state.residual_position)
                                        if staged_state.residual_position is not None
                                        else None
                                    )
                                    staged_transient_before = float(staged_state.transient_u)
                                    staged_transient_position_before = (
                                        int(staged_state.transient_u_position)
                                        if staged_state.transient_u_position is not None
                                        else None
                                    )
                                    staged_update_debug: dict[str, object] = {}
                                    if residual_update is not None:
                                        staged_step_distance: int | None = None
                                        if staged_state.residual_position is not None:
                                            staged_step_distance = period_position - int(
                                                staged_state.residual_position
                                            )
                                        _apply_rho_resid_state_update(
                                            state=staged_state,
                                            residual=float(residual_update),
                                            period_position=period_position,
                                            step_distance=staged_step_distance,
                                            carry_damp=rho_resid_carry_damp,
                                            carry_damp_mode=rho_resid_carry_damp_mode,
                                            carry_multipass=rho_resid_carry_multipass,
                                            la257_update_rule=resolved_la257_rule,
                                            la257_fortran_cycle_carry_style=(
                                                rho_resid_la257_fortran_cycle_carry_style
                                            ),
                                            la257_u_phase_max_gain=(
                                                rho_resid_la257_u_phase_max_gain
                                            ),
                                            la257_u_phase_max_gain_mode=(
                                                rho_resid_la257_u_phase_max_gain_mode
                                            ),
                                            debug_update=staged_update_debug,
                                        )
                                    debug_metrics["residual_used"] = (
                                        float(residual_update)
                                        if residual_update is not None
                                        else None
                                    )
                                    debug_metrics["state_residual_before"] = staged_before
                                    debug_metrics["state_position_before"] = staged_position_before
                                    debug_metrics["state_transient_before"] = (
                                        staged_transient_before
                                    )
                                    debug_metrics["state_transient_position_before"] = (
                                        staged_transient_position_before
                                    )
                                    debug_metrics["state_residual_after"] = (
                                        float(staged_state.residual)
                                        if staged_state.residual is not None
                                        else None
                                    )
                                    debug_metrics["state_position_after"] = (
                                        int(staged_state.residual_position)
                                        if staged_state.residual_position is not None
                                        else None
                                    )
                                    debug_metrics["state_transient_after"] = float(
                                        staged_state.transient_u
                                    )
                                    debug_metrics["state_transient_position_after"] = (
                                        int(staged_state.transient_u_position)
                                        if staged_state.transient_u_position is not None
                                        else None
                                    )
                                    if staged_update_debug:
                                        debug_metrics["rho"] = staged_update_debug.get("rho")
                                        debug_metrics["la257_rule"] = staged_update_debug.get(
                                            "la257_rule",
                                            resolved_la257_rule,
                                        )
                                        debug_metrics["u_raw"] = staged_update_debug.get("u_raw")
                                        debug_metrics["u_applied"] = staged_update_debug.get(
                                            "u_applied"
                                        )
                                        debug_metrics["gain_cap"] = staged_update_debug.get(
                                            "gain_cap"
                                        )
                                        debug_metrics["gain_mode"] = staged_update_debug.get(
                                            "gain_mode"
                                        )
                            commit_mode = (
                                "deferred" if rho_resid_commit_after_check else "immediate"
                            )
                            event_payload_base = {
                                "period_position": int(period_position),
                                "period": str(period),
                                "line": int(line_number),
                                "occurrence": int(occurrence),
                                "step_idx": int(step_idx),
                                "target": str(target),
                                "phase": "solve",
                                "pass": int(step_passes[step_idx]),
                                "la257_rule": str(resolved_la257_rule),
                                "carry_damp_mode": str(rho_resid_carry_damp_mode),
                                "update_source": str(rho_resid_update_source),
                                "update_enabled": bool(update_enabled),
                                "commit_mode": commit_mode,
                            }
                            if trace_selected and debug_metrics is not None:
                                event_kind = (
                                    "solve_deferred"
                                    if (defer_resid_update or rho_resid_commit_after_check)
                                    else "solve_and_update"
                                )
                                _append_trace_event({
                                    **event_payload_base,
                                    "kind": event_kind,
                                    **debug_metrics,
                                })
                            if debug_metrics is not None and focus_selected:
                                _append_focus_event({
                                    "iter": int(trace_iteration),
                                    "period": str(period),
                                    "period_position": int(period_position),
                                    "target": str(target),
                                    "stage": "resid_eval",
                                    "line": int(line_number),
                                    "occurrence": int(occurrence),
                                    "update_source": str(rho_resid_update_source),
                                    "resid": debug_metrics.get("residual_used"),
                                    "structural": debug_metrics.get("structural"),
                                    "result": debug_metrics.get("result"),
                                    "update_enabled": bool(update_enabled),
                                    "commit_mode": commit_mode,
                                    "la257_rule": str(
                                        debug_metrics.get(
                                            "la257_rule",
                                            resolved_la257_rule,
                                        )
                                    ),
                                })
                                if not defer_resid_update:
                                    _append_focus_event({
                                        "iter": int(trace_iteration),
                                        "period": str(period),
                                        "period_position": int(period_position),
                                        "target": str(target),
                                        "stage": "resid_eval",
                                        "line": int(line_number),
                                        "occurrence": int(occurrence),
                                        "update_source": str(rho_resid_update_source),
                                        "resid": debug_metrics.get("residual_used"),
                                        "structural": debug_metrics.get("structural"),
                                        "result": debug_metrics.get("result"),
                                        "update_enabled": bool(update_enabled),
                                        "commit_mode": commit_mode,
                                        "la257_rule": str(
                                            debug_metrics.get(
                                                "la257_rule",
                                                resolved_la257_rule,
                                            )
                                        ),
                                    })
                                    if not defer_resid_update:
                                        _append_focus_event({
                                            "iter": int(trace_iteration),
                                            "period": str(period),
                                            "period_position": int(period_position),
                                            "target": str(target),
                                            "stage": "ar1_state_write",
                                            "line": int(line_number),
                                            "occurrence": int(occurrence),
                                            "update_source": str(rho_resid_update_source),
                                            "resid": debug_metrics.get("residual_used"),
                                            "rho": debug_metrics.get("rho"),
                                            "state_before": debug_metrics.get(
                                                "state_residual_before"
                                            ),
                                            "state_after": debug_metrics.get(
                                                "state_residual_after"
                                            ),
                                            "state_position_before": debug_metrics.get(
                                                "state_position_before"
                                            ),
                                            "state_position_after": debug_metrics.get(
                                                "state_position_after"
                                            ),
                                            "seed_lag": int(rho_resid_iteration_seed_lag),
                                            "carry_lag": int(rho_resid_carry_lag),
                                            "carry_damp_mode": str(rho_resid_carry_damp_mode),
                                            "commit_mode": commit_mode,
                                        })
                                if str(debug_metrics.get("la257_rule", resolved_la257_rule)) in {
                                    "fortran_u_phase",
                                    "fortran_cycle",
                                }:
                                    _append_focus_event({
                                        "iter": int(trace_iteration),
                                        "period": str(period),
                                        "period_position": int(period_position),
                                        "target": str(target),
                                        "stage": "la257_u_phase_update",
                                        "line": int(line_number),
                                        "occurrence": int(occurrence),
                                        "update_source": str(rho_resid_update_source),
                                        "la257_rule": str(
                                            debug_metrics.get(
                                                "la257_rule",
                                                resolved_la257_rule,
                                            )
                                        ),
                                        "u_raw": debug_metrics.get("u_raw"),
                                        "u_applied": debug_metrics.get("u_applied"),
                                        "gain_cap": debug_metrics.get("gain_cap"),
                                        "gain_mode": debug_metrics.get("gain_mode"),
                                        "commit_mode": commit_mode,
                                    })
                            if rho_resid_ar1 and not trace_selected:
                                try:
                                    value_numeric = float(value)
                                except (TypeError, ValueError):
                                    value_numeric = None
                                if (
                                    value_numeric is not None
                                    and not math.isfinite(value_numeric)
                                    and not pd.isna(value)
                                ):
                                    _append_trace_event({
                                        **event_payload_base,
                                        "kind": "solve_nonfinite",
                                        "result": float(value_numeric),
                                        "nonfinite_field": "result",
                                        "nonfinite_value": float(value_numeric),
                                    })
                        else:
                            if step.assignment is None:
                                continue
                            target = step.target
                            is_hf_line182_assignment = _is_line182_hf_exp_lag_assignment(
                                step.record,
                                step.assignment,
                            )
                            if rho_resid_fpe_trap and is_hf_line182_assignment:
                                overflow_probe = _probe_line182_hf_pre_overflow(
                                    output,
                                    period_position=period_position,
                                )
                                if overflow_probe is not None:
                                    probe_event_base = {
                                        "period_position": int(period_position),
                                        "period": str(period),
                                        "line": int(step.record.line_number),
                                        "occurrence": 0,
                                        "step_idx": int(step_idx),
                                        "target": str(target),
                                        "phase": "assignment",
                                        "pass": int(step_passes[step_idx]),
                                        "command": str(step.record.command.value),
                                    }
                                    margin_to_overflow_raw = overflow_probe.get(
                                        "margin_to_overflow"
                                    )
                                    margin_to_overflow = (
                                        float(margin_to_overflow_raw)
                                        if margin_to_overflow_raw is not None
                                        else None
                                    )
                                    should_emit_probe = bool(
                                        overflow_probe.get("would_overflow")
                                    ) or (
                                        margin_to_overflow is not None
                                        and math.isfinite(margin_to_overflow)
                                        and margin_to_overflow <= 25.0
                                    )
                                    if should_emit_probe:
                                        _append_trace_event({
                                            **probe_event_base,
                                            "kind": "assignment_overflow_probe",
                                            **overflow_probe,
                                        })
                                    if bool(overflow_probe.get("would_overflow")):
                                        _append_trace_event({
                                            **probe_event_base,
                                            "kind": "assignment_pre_overflow",
                                            "op": "pre_overflow",
                                            **overflow_probe,
                                        })
                                        raise FloatingPointError(
                                            "pre-overflow check failed for line 182 HF assignment"
                                        )
                            value = evaluate_expression_at_period(
                                step.assignment.rhs,
                                data=output,
                                period=period,
                                period_position=period_position,
                                raise_fp_errors=rho_resid_fpe_trap,
                                eval_context=eval_context,
                                math_backend=resolved_assignment_math_backend,
                            )
                            if rho_resid_ar1:
                                try:
                                    value_numeric = float(value)
                                except (TypeError, ValueError):
                                    value_numeric = None
                                if (
                                    value_numeric is not None
                                    and not math.isfinite(value_numeric)
                                    and not pd.isna(value)
                                ):
                                    _append_trace_event({
                                        "period_position": int(period_position),
                                        "period": str(period),
                                        "line": int(step.record.line_number),
                                        "occurrence": 0,
                                        "step_idx": int(step_idx),
                                        "target": str(target),
                                        "kind": "assignment_nonfinite",
                                        "phase": "assignment",
                                        "pass": int(step_passes[step_idx]),
                                        "command": str(step.record.command.value),
                                        "result": float(value_numeric),
                                        "nonfinite_field": "result",
                                        "nonfinite_value": float(value_numeric),
                                    })
                        if pd.isna(value):
                            continue
                        try:
                            value_numeric = float(value)
                        except (TypeError, ValueError):
                            value_numeric = None
                        if value_numeric is not None and not math.isfinite(value_numeric):
                            # Treat non-finite results as missing to avoid contaminating
                            # downstream computations; legacy FP tends to propagate
                            # domain/overflow edges as missing rather than infinities.
                            continue
                        if (
                            step.kind == "assignment"
                            and rho_resid_fpe_trap
                            and is_hf_line182_assignment
                        ):
                            write_count = int(hf_line182_write_counts.get(period_position, 0)) + 1
                            hf_line182_write_counts[period_position] = write_count
                            if write_count > 1:
                                _append_trace_event({
                                    "period_position": int(period_position),
                                    "period": str(period),
                                    "line": int(step.record.line_number),
                                    "occurrence": 0,
                                    "step_idx": int(step_idx),
                                    "target": str(target),
                                    "kind": "assignment_double_write",
                                    "phase": "assignment",
                                    "pass": int(step_passes[step_idx]),
                                    "command": str(step.record.command.value),
                                    "write_count": int(write_count),
                                    "op": "duplicate_write",
                                })
                                raise FloatingPointError(
                                    "duplicate write detected for line 182 HF assignment"
                                )
                        if (
                            step.kind == "eq"
                            and period_sequential_eq_commit_quantize == "float32"
                            and value_numeric is not None
                        ):
                            output.at[period, target] = _quantize_float32(value_numeric)
                        else:
                            output.at[period, target] = value
                        if period_sequential_context_replay_trace and step.kind == "assignment":
                            context_replay_mutation_positions.setdefault(step_idx, set()).add(
                                int(period_position)
                            )
                        if (
                            step.kind == "eq"
                            and step.eq_spec is not None
                            and rho_resid_ar1
                            and rho_resid_update_source == "resid_pass"
                            and (
                                rho_resid_carry_damp_mode != "la257"
                                or rho_resid_la257_staged_lifecycle
                            )
                        ):
                            pending_resid_state_steps[step_idx] = (target, step.eq_spec, lhs_value)
                    except Exception as exc:  # pragma: no cover - explicit parse/eval path
                        if isinstance(exc, FloatingPointError) and (
                            rho_resid_ar1 or rho_resid_fpe_trap
                        ):
                            if step.kind == "eq":
                                line_number, occurrence = step_seed_keys.get(
                                    step_idx,
                                    (int(step.record.line_number), 0),
                                )
                            else:
                                line_number, occurrence = int(step.record.line_number), 0
                            _append_trace_event({
                                "period_position": int(period_position),
                                "period": str(period),
                                "line": int(line_number),
                                "occurrence": int(occurrence),
                                "step_idx": int(step_idx),
                                "target": str(step.target),
                                "kind": "fpe_exception",
                                "phase": "solve" if step.kind == "eq" else "assignment",
                                "pass": int(step_passes[step_idx]),
                                "command": str(step.record.command.value),
                                "op": "floating_point_error",
                                "message": str(exc),
                            })
                        if not strict_missing_assignments and isinstance(
                            exc, (KeyError, NameError)
                        ):
                            continue
                        if _append_issue(
                            step.record.line_number,
                            step.record.statement,
                            f"{step.record.command.value} evaluation failed: {exc}",
                        ):
                            modeq_fsr_active_equation_count = sum(
                                1 for terms in modeq_fsr_terms_by_equation.values() if terms
                            )
                            modeq_fsr_active_term_count = sum(
                                len(terms) for terms in modeq_fsr_terms_by_equation.values()
                            )
                            (
                                focus_trace_spec_payload,
                                focus_trace_events_payload,
                                focus_trace_events_truncated_payload,
                                focus_trace_no_match_payload,
                                focus_trace_stage_counts_payload,
                            ) = _build_focus_trace_result_fields()
                            (
                                context_replay_trace_events_payload,
                                context_replay_trace_events_truncated_payload,
                            ) = _build_context_replay_trace_result_fields()
                            side_channel_status = _resolve_side_channel_status()
                            return EqBackfillResult(
                                frame=output,
                                applied=applied,
                                failed=failed,
                                issues=tuple(issues),
                                modeq_fsr_updates=modeq_fsr_updates,
                                modeq_fsr_active_equation_count=modeq_fsr_active_equation_count,
                                modeq_fsr_active_term_count=modeq_fsr_active_term_count,
                                eq_fsr_skipped=eq_fsr_skipped,
                                modeq_fsr_side_channel_mode=side_channel_mode,
                                modeq_fsr_side_channel_status=side_channel_status,
                                modeq_fsr_side_channel_events=tuple(side_channel_events),
                                modeq_fsr_side_channel_events_truncated=(
                                    side_channel_events_truncated
                                ),
                                modeq_fsr_side_channel_breach_count=(side_channel_breach_count),
                                modeq_fsr_side_channel_mutation_count=(
                                    side_channel_mutation_count
                                ),
                                modeq_fsr_side_channel_first_breach=(
                                    dict(side_channel_first_breach)
                                    if side_channel_first_breach is not None
                                    else None
                                ),
                                modeq_fsr_side_channel_effective_limit=(
                                    side_channel_effective_limit if side_channel_enabled else 0
                                ),
                                rho_resid_iteration_state=_collect_rho_resid_iteration_state(
                                    deferred_steps,
                                    rho_resid_result_states,
                                ),
                                rho_resid_iteration_state_keyed=(
                                    _collect_rho_resid_iteration_state_keyed(
                                        deferred_steps,
                                        rho_resid_result_states,
                                        step_seed_keys,
                                    )
                                ),
                                rho_resid_iteration_state_positioned=(
                                    _collect_rho_resid_iteration_state_positioned(
                                        deferred_steps,
                                        rho_resid_result_states,
                                    )
                                ),
                                rho_resid_iteration_state_positioned_keyed=(
                                    _collect_rho_resid_iteration_state_positioned_keyed(
                                        deferred_steps,
                                        rho_resid_result_states,
                                        step_seed_keys,
                                    )
                                ),
                                rho_resid_ar1_trace_events=tuple(rho_resid_ar1_trace_events),
                                rho_resid_ar1_trace_events_truncated=(
                                    rho_resid_ar1_trace_events_truncated
                                ),
                                rho_resid_ar1_focus_trace_spec=focus_trace_spec_payload,
                                rho_resid_ar1_focus_trace_events=focus_trace_events_payload,
                                rho_resid_ar1_focus_trace_events_truncated=(
                                    focus_trace_events_truncated_payload
                                ),
                                rho_resid_ar1_focus_trace_no_match=(focus_trace_no_match_payload),
                                rho_resid_ar1_focus_trace_event_counts_by_stage=(
                                    focus_trace_stage_counts_payload
                                ),
                                context_replay_trace_events=(context_replay_trace_events_payload),
                                context_replay_trace_events_truncated=(
                                    context_replay_trace_events_truncated_payload
                                ),
                            )
                if pending_resid_state_steps:
                    for pending_step_idx, (
                        pending_target,
                        pending_spec,
                        pending_lhs_value,
                    ) in pending_resid_state_steps.items():
                        state = rho_resid_states_staged.get(pending_step_idx)
                        if state is None:
                            continue
                        structural_pass = _evaluate_eq_structural_value_at_period(
                            output,
                            pending_spec,
                            period_position=period_position,
                            strict_missing_inputs=strict_missing_inputs,
                            source_cache=eq_source_cache,
                            rho_resid_lag_gating=rho_resid_lag_gating,
                            rho_resid_fpe_trap=rho_resid_fpe_trap,
                        )
                        solved_value = float(output[pending_target].iat[period_position])
                        step_distance: int | None = None
                        if state.residual_position is not None:
                            step_distance = period_position - int(state.residual_position)
                        pending_line_number, pending_occurrence = step_seed_keys.get(
                            pending_step_idx,
                            (int(deferred_steps[pending_step_idx].record.line_number), 0),
                        )
                        resolved_la257_rule = _resolve_la257_update_rule_for_line(
                            base_rule=rho_resid_la257_update_rule,
                            line_number=pending_line_number,
                            occurrence=pending_occurrence,
                            u_phase_lines=rho_resid_la257_u_phase_line_set,
                            u_phase_occurrences=rho_resid_la257_u_phase_occurrence_set,
                        )
                        residual_update = float(solved_value - structural_pass)
                        if (
                            rho_resid_carry_damp_mode == "la257"
                            and rho_resid_la257_staged_lifecycle
                            and not pd.isna(pending_lhs_value)
                        ):
                            residual_update = float(pending_lhs_value - structural_pass)
                        state_residual_before = (
                            float(state.residual) if state.residual is not None else None
                        )
                        state_position_before = (
                            int(state.residual_position)
                            if state.residual_position is not None
                            else None
                        )
                        state_transient_before = float(state.transient_u)
                        state_transient_position_before = (
                            int(state.transient_u_position)
                            if state.transient_u_position is not None
                            else None
                        )
                        state_update_debug: dict[str, object] = {}
                        _apply_rho_resid_state_update(
                            state=state,
                            residual=residual_update,
                            period_position=period_position,
                            step_distance=step_distance,
                            carry_damp=rho_resid_carry_damp,
                            carry_damp_mode=rho_resid_carry_damp_mode,
                            carry_multipass=rho_resid_carry_multipass,
                            la257_update_rule=resolved_la257_rule,
                            la257_u_phase_max_gain=rho_resid_la257_u_phase_max_gain,
                            la257_u_phase_max_gain_mode=(rho_resid_la257_u_phase_max_gain_mode),
                            debug_update=state_update_debug,
                        )
                        transient_cleared = False
                        if (
                            rho_resid_carry_damp_mode == "la257"
                            and rho_resid_la257_staged_lifecycle
                        ):
                            _clear_rho_resid_transient_state(state)
                            transient_cleared = True
                        state_event = {
                            "period_position": int(period_position),
                            "period": str(period),
                            "line": int(pending_line_number),
                            "occurrence": int(pending_occurrence),
                            "step_idx": int(pending_step_idx),
                            "target": str(pending_target),
                            "kind": "state_update",
                            "phase": "state_update",
                            "pass": int(step_passes[pending_step_idx]),
                            "la257_rule": str(resolved_la257_rule),
                            "carry_damp_mode": str(rho_resid_carry_damp_mode),
                            "update_source": "resid_pass",
                            "update_enabled": True,
                            "commit_mode": (
                                "deferred" if rho_resid_commit_after_check else "immediate"
                            ),
                            "structural": float(structural_pass),
                            "lhs_value": (
                                float(pending_lhs_value)
                                if not pd.isna(pending_lhs_value)
                                else None
                            ),
                            "solved_value": float(solved_value),
                            "result": float(solved_value),
                            "residual_used": float(residual_update),
                            "step_distance": (
                                int(step_distance) if step_distance is not None else None
                            ),
                            "state_residual_before": state_residual_before,
                            "state_position_before": state_position_before,
                            "state_residual_after": (
                                float(state.residual) if state.residual is not None else None
                            ),
                            "state_position_after": (
                                int(state.residual_position)
                                if state.residual_position is not None
                                else None
                            ),
                            "state_transient_before": state_transient_before,
                            "state_transient_position_before": (state_transient_position_before),
                            "state_transient_after": float(state.transient_u),
                            "state_transient_position_after": (
                                int(state.transient_u_position)
                                if state.transient_u_position is not None
                                else None
                            ),
                            "state_transient_cleared": bool(transient_cleared),
                            "rho": state_update_debug.get("rho"),
                            "u_raw": state_update_debug.get("u_raw"),
                            "u_applied": state_update_debug.get("u_applied"),
                            "gain_cap": state_update_debug.get("gain_cap"),
                            "gain_mode": state_update_debug.get("gain_mode"),
                        }
                        trace_selected = _selector_matches(
                            line_number=pending_line_number,
                            occurrence=pending_occurrence,
                            line_selectors=rho_resid_trace_line_set,
                            occurrence_selectors=rho_resid_trace_occurrence_set,
                        )
                        nonfinite_detail = (
                            _first_nonfinite_trace_detail(state_event) if rho_resid_ar1 else None
                        )
                        if trace_selected or nonfinite_detail is not None:
                            if nonfinite_detail is not None:
                                field_name, nonfinite_value = nonfinite_detail
                                state_event["kind"] = (
                                    "state_update_nonfinite"
                                    if not trace_selected
                                    else str(state_event["kind"])
                                )
                                state_event["nonfinite_field"] = field_name
                                state_event["nonfinite_value"] = float(nonfinite_value)
                            _append_trace_event(state_event)
                        if _focus_matches_target_period(
                            target=pending_target,
                            period=period,
                        ):
                            _append_focus_event({
                                "iter": int(trace_iteration),
                                "period": str(period),
                                "period_position": int(period_position),
                                "target": str(pending_target),
                                "stage": "resid_eval",
                                "line": int(pending_line_number),
                                "occurrence": int(pending_occurrence),
                                "update_source": "resid_pass",
                                "resid": float(residual_update),
                                "structural": float(structural_pass),
                                "solved_value": float(solved_value),
                                "la257_rule": str(resolved_la257_rule),
                                "commit_mode": (
                                    "deferred" if rho_resid_commit_after_check else "immediate"
                                ),
                            })
                            _append_focus_event({
                                "iter": int(trace_iteration),
                                "period": str(period),
                                "period_position": int(period_position),
                                "target": str(pending_target),
                                "stage": "ar1_state_write",
                                "line": int(pending_line_number),
                                "occurrence": int(pending_occurrence),
                                "update_source": "resid_pass",
                                "resid": float(residual_update),
                                "rho": state_update_debug.get("rho"),
                                "state_before": state_residual_before,
                                "state_after": (
                                    float(state.residual) if state.residual is not None else None
                                ),
                                "state_position_before": state_position_before,
                                "state_position_after": (
                                    int(state.residual_position)
                                    if state.residual_position is not None
                                    else None
                                ),
                                "seed_lag": int(rho_resid_iteration_seed_lag),
                                "carry_lag": int(rho_resid_carry_lag),
                                "carry_damp_mode": str(rho_resid_carry_damp_mode),
                                "commit_mode": (
                                    "deferred" if rho_resid_commit_after_check else "immediate"
                                ),
                            })
                            if str(resolved_la257_rule) in {"fortran_u_phase", "fortran_cycle"}:
                                _append_focus_event({
                                    "iter": int(trace_iteration),
                                    "period": str(period),
                                    "period_position": int(period_position),
                                    "target": str(pending_target),
                                    "stage": "la257_u_phase_update",
                                    "line": int(pending_line_number),
                                    "occurrence": int(pending_occurrence),
                                    "update_source": "resid_pass",
                                    "la257_rule": str(resolved_la257_rule),
                                    "u_raw": state_update_debug.get("u_raw"),
                                    "u_applied": state_update_debug.get("u_applied"),
                                    "gain_cap": state_update_debug.get("gain_cap"),
                                    "gain_mode": state_update_debug.get("gain_mode"),
                                    "commit_mode": (
                                        "deferred" if rho_resid_commit_after_check else "immediate"
                                    ),
                                })

    modeq_fsr_active_equation_count = sum(
        1 for terms in modeq_fsr_terms_by_equation.values() if terms
    )
    modeq_fsr_active_term_count = sum(len(terms) for terms in modeq_fsr_terms_by_equation.values())
    (
        focus_trace_spec_payload,
        focus_trace_events_payload,
        focus_trace_events_truncated_payload,
        focus_trace_no_match_payload,
        focus_trace_stage_counts_payload,
    ) = _build_focus_trace_result_fields()
    (
        context_replay_trace_events_payload,
        context_replay_trace_events_truncated_payload,
    ) = _build_context_replay_trace_result_fields()
    side_channel_status = _resolve_side_channel_status()
    return EqBackfillResult(
        frame=output,
        applied=applied,
        failed=failed,
        issues=tuple(issues),
        modeq_fsr_updates=modeq_fsr_updates,
        modeq_fsr_active_equation_count=modeq_fsr_active_equation_count,
        modeq_fsr_active_term_count=modeq_fsr_active_term_count,
        eq_fsr_skipped=eq_fsr_skipped,
        modeq_fsr_side_channel_mode=side_channel_mode,
        modeq_fsr_side_channel_status=side_channel_status,
        modeq_fsr_side_channel_events=tuple(side_channel_events),
        modeq_fsr_side_channel_events_truncated=side_channel_events_truncated,
        modeq_fsr_side_channel_breach_count=side_channel_breach_count,
        modeq_fsr_side_channel_mutation_count=side_channel_mutation_count,
        modeq_fsr_side_channel_first_breach=(
            dict(side_channel_first_breach) if side_channel_first_breach is not None else None
        ),
        modeq_fsr_side_channel_effective_limit=(
            side_channel_effective_limit if side_channel_enabled else 0
        ),
        rho_resid_iteration_state=_collect_rho_resid_iteration_state(
            deferred_steps,
            rho_resid_result_states if period_sequential else {},
        ),
        rho_resid_iteration_state_keyed=_collect_rho_resid_iteration_state_keyed(
            deferred_steps,
            rho_resid_result_states if period_sequential else {},
            step_seed_keys if period_sequential else {},
        ),
        rho_resid_iteration_state_positioned=_collect_rho_resid_iteration_state_positioned(
            deferred_steps,
            rho_resid_result_states if period_sequential else {},
        ),
        rho_resid_iteration_state_positioned_keyed=(
            _collect_rho_resid_iteration_state_positioned_keyed(
                deferred_steps,
                rho_resid_result_states if period_sequential else {},
                step_seed_keys if period_sequential else {},
            )
        ),
        rho_resid_ar1_trace_events=tuple(rho_resid_ar1_trace_events),
        rho_resid_ar1_trace_events_truncated=rho_resid_ar1_trace_events_truncated,
        rho_resid_ar1_focus_trace_spec=focus_trace_spec_payload,
        rho_resid_ar1_focus_trace_events=focus_trace_events_payload,
        rho_resid_ar1_focus_trace_events_truncated=focus_trace_events_truncated_payload,
        rho_resid_ar1_focus_trace_no_match=focus_trace_no_match_payload,
        rho_resid_ar1_focus_trace_event_counts_by_stage=focus_trace_stage_counts_payload,
        context_replay_trace_events=context_replay_trace_events_payload,
        context_replay_trace_events_truncated=context_replay_trace_events_truncated_payload,
    )


@dataclass(frozen=True)
class _EqReference:
    number: int
    token: str | None


def _parse_eq_reference(statement: str) -> _EqReference | None:
    match = _EQ_RE.match(statement)
    if match is None:
        return None
    number = int(match.group("number"))
    token_raw = match.group("token")
    token = token_raw.upper() if token_raw else None
    return _EqReference(number=number, token=token)


def collect_eq_fsr_command_lines(records: Sequence[FPCommandRecord]) -> set[int]:
    """Collect EQ record line numbers used to configure first-stage regressors."""

    eq_fsr_lines: set[int] = set()
    pending_multiline_eq_line: int | None = None

    for record in records:
        if pending_multiline_eq_line is not None:
            if record.command == FPCommand.FSR:
                eq_fsr_lines.add(pending_multiline_eq_line)
                if record.terminated:
                    pending_multiline_eq_line = None
                continue
            pending_multiline_eq_line = None

        if record.command != FPCommand.EQ:
            continue

        parsed = _parse_eq_reference(record.statement)
        if parsed is None:
            continue

        if parsed.token == "FSR":
            eq_fsr_lines.add(record.line_number)
            if not record.terminated:
                pending_multiline_eq_line = record.line_number
            continue

        if parsed.token is None and not record.terminated:
            pending_multiline_eq_line = record.line_number

    return eq_fsr_lines


def _parse_modeq_update(
    statement: str,
) -> (
    tuple[
        int,
        tuple[tuple[str, int], ...],
        tuple[tuple[str, int], ...],
        tuple[tuple[str, int], ...],
        tuple[tuple[str, int], ...],
        bool,
    ]
    | None
):
    match = _MODEQ_RE.match(statement)
    if match is None:
        return None
    number = int(match.group("number"))
    body = str(match.group("body") or "").strip()
    body = body.rstrip(";").strip()
    if not body:
        return number, tuple(), tuple(), tuple(), tuple(), False

    # MODEQ supports a separate FSR section; deterministic replay currently
    # applies only RHS updates before the FSR token.
    fsr_split = re.split(r"\bFSR\b", body, maxsplit=1, flags=re.IGNORECASE)
    rhs_source = fsr_split[0].strip() if fsr_split else body
    fsr_source = fsr_split[1].strip() if len(fsr_split) > 1 else ""
    rhs_adds, rhs_subs = _parse_modeq_term_list(rhs_source)
    fsr_adds, fsr_subs = _parse_modeq_term_list(fsr_source)
    return number, rhs_adds, rhs_subs, fsr_adds, fsr_subs, len(fsr_split) > 1


def _parse_modeq_term_token(token: str) -> tuple[str, int] | None:
    match = _MODEQ_TERM_RE.match(token)
    if match is None:
        return None
    name = str(match.group("name")).upper()
    lag = int(match.group("lag") or 0)
    return name, lag


def _parse_modeq_term_list(
    source: str,
) -> tuple[tuple[tuple[str, int], ...], tuple[tuple[str, int], ...]]:
    tokens = [item for item in source.replace(";", " ").split() if item]
    add_terms: list[tuple[str, int]] = []
    sub_terms: list[tuple[str, int]] = []
    mode = "add"
    for token in tokens:
        if token == "-":
            mode = "sub"
            continue
        if token.upper().startswith("RHO"):
            continue
        parsed = _parse_modeq_term_token(token)
        if parsed is None:
            continue
        if mode == "add":
            add_terms.append(parsed)
        else:
            sub_terms.append(parsed)
    return tuple(add_terms), tuple(sub_terms)


def _parse_fsr_terms(
    statement: str,
) -> tuple[tuple[tuple[str, int], ...], tuple[tuple[str, int], ...]]:
    body = re.sub(r"^\s*FSR\b", "", statement, flags=re.IGNORECASE).strip()
    return _parse_modeq_term_list(body)


def _apply_modeq_update_to_spec(
    spec: EqSpec,
    *,
    add_terms: tuple[tuple[str, int], ...],
    sub_terms: tuple[tuple[str, int], ...],
) -> EqSpec:
    terms = list(spec.terms)
    sub_keys = {(name.upper(), int(lag)) for name, lag in sub_terms}
    if sub_keys:
        terms = [term for term in terms if (term.variable.upper(), int(term.lag)) not in sub_keys]

    existing_keys = {(term.variable.upper(), int(term.lag)) for term in terms}
    for name, lag in add_terms:
        key = (name.upper(), int(lag))
        if key in existing_keys:
            continue
        terms.append(EqTerm(variable=name.upper(), coefficient=0.0, lag=int(lag), index=None))
        existing_keys.add(key)

    return EqSpec(
        lhs=spec.lhs,
        terms=tuple(terms),
        equation_number=spec.equation_number,
    )


def _apply_modeq_term_key_update(
    existing: tuple[tuple[str, int], ...],
    *,
    add_terms: tuple[tuple[str, int], ...],
    sub_terms: tuple[tuple[str, int], ...],
) -> tuple[tuple[str, int], ...]:
    terms = [(name.upper(), int(lag)) for name, lag in existing]
    sub_keys = {(name.upper(), int(lag)) for name, lag in sub_terms}
    if sub_keys:
        terms = [term for term in terms if term not in sub_keys]

    existing_keys = set(terms)
    for name, lag in add_terms:
        key = (name.upper(), int(lag))
        if key in existing_keys:
            continue
        terms.append(key)
        existing_keys.add(key)

    return tuple(terms)


def _resolve_eq_spec(
    reference: _EqReference,
    *,
    specs: dict[str, EqSpec],
    specs_by_number: dict[int, EqSpec],
) -> EqSpec | None:
    if reference.token and reference.token not in {"FSR"}:
        direct = specs.get(reference.token)
        if direct is not None:
            return direct

    by_number = specs_by_number.get(reference.number)
    if by_number is not None:
        return by_number

    if reference.token:
        return specs.get(reference.token)
    return None


def _index_specs_by_number(specs: dict[str, EqSpec]) -> dict[int, EqSpec]:
    indexed: dict[int, EqSpec] = {}
    for spec in specs.values():
        if spec.equation_number is None:
            continue
        indexed.setdefault(spec.equation_number, spec)
    return indexed


def _is_none_equation_token(token: str | None) -> bool:
    return bool(token and token.startswith("NONE"))


def _missing_spec_error(reference: _EqReference) -> str:
    if reference.token:
        return (
            "missing equation specification for "
            f"equation {reference.number} token '{reference.token}'"
        )
    return f"missing equation specification for equation {reference.number}"


def _evaluate_eq_spec(
    data: pd.DataFrame,
    spec: EqSpec,
    *,
    strict_missing_inputs: bool = True,
    rho_aware: bool = False,
    rho_resid_ar1: bool = False,
    rho_resid_carry_lag: int = 0,
    rho_resid_carry_damp: float = 1.0,
    rho_resid_carry_damp_mode: str = "term",
    rho_resid_carry_multipass: bool = False,
    rho_resid_lag_gating: str = "off",
    rho_resid_update_source: str = "structural",
    rho_resid_la257_update_rule: str = "legacy",
    rho_resid_la257_fortran_cycle_carry_style: str = "legacy",
    rho_resid_la257_u_phase_max_gain: float | None = None,
    rho_resid_la257_u_phase_max_gain_mode: str = "relative",
    mask: Sequence[bool] | None = None,
    rho_resid_fpe_trap: bool = False,
) -> pd.Series:
    structural_result = _evaluate_eq_structural_series(
        data,
        spec,
        strict_missing_inputs=strict_missing_inputs,
        rho_resid_lag_gating=(rho_resid_lag_gating if rho_resid_ar1 else "off"),
        rho_resid_fpe_trap=rho_resid_fpe_trap,
    )
    if rho_resid_ar1:
        return _apply_resid_ar1_series(
            data,
            spec,
            structural_result=structural_result,
            strict_missing_inputs=strict_missing_inputs,
            carry_lag=rho_resid_carry_lag,
            carry_damp=rho_resid_carry_damp,
            carry_damp_mode=rho_resid_carry_damp_mode,
            carry_multipass=rho_resid_carry_multipass,
            update_source=rho_resid_update_source,
            la257_update_rule=rho_resid_la257_update_rule,
            la257_fortran_cycle_carry_style=rho_resid_la257_fortran_cycle_carry_style,
            la257_u_phase_max_gain=rho_resid_la257_u_phase_max_gain,
            la257_u_phase_max_gain_mode=rho_resid_la257_u_phase_max_gain_mode,
            mask=mask,
        )
    if not rho_aware:
        return structural_result

    rho_terms = _extract_rho_terms(spec)
    if not rho_terms:
        return structural_result
    lhs_source = _resolve_eq_term_column(data, spec.lhs)
    if lhs_source is None:
        if strict_missing_inputs:
            raise KeyError(f"missing variable '{spec.lhs}' for equation '{spec.lhs}'")
        return structural_result

    result = structural_result.copy()
    lhs_values = data[lhs_source]
    for order, coefficient in rho_terms:
        result = result + coefficient * lhs_values.shift(order)
        result = result - coefficient * structural_result.shift(order)
    return result


def _evaluate_eq_spec_at_period(
    data: pd.DataFrame,
    spec: EqSpec,
    *,
    period: object,
    period_position: int | None = None,
    eval_precision: str = "float64",
    term_order: str = "as_parsed",
    strict_missing_inputs: bool = True,
    source_cache: dict[tuple[str, str], str | None] | None = None,
    rho_aware: bool = False,
    rho_resid_ar1: bool = False,
    rho_resid_carry_lag: int = 0,
    rho_resid_carry_damp: float = 1.0,
    rho_resid_carry_damp_mode: str = "term",
    rho_resid_carry_multipass: bool = False,
    rho_resid_lag_gating: str = "off",
    rho_resid_state: _RhoResidualState | None = None,
    rho_resid_lhs_value: float | None = None,
    rho_resid_update_source: str = "structural",
    rho_resid_la257_update_rule: str = "legacy",
    rho_resid_la257_fortran_cycle_carry_style: str = "legacy",
    rho_resid_la257_u_phase_max_gain: float | None = None,
    rho_resid_la257_u_phase_max_gain_mode: str = "relative",
    rho_resid_update_enabled: bool = True,
    rho_resid_debug_metrics: dict[str, object] | None = None,
    rho_resid_fpe_trap: bool = False,
) -> float:
    if period_position is None:
        period_position = int(data.index.get_loc(period))
    structural_result = _evaluate_eq_structural_value_at_period(
        data,
        spec,
        period_position=period_position,
        eval_precision=eval_precision,
        term_order=term_order,
        strict_missing_inputs=strict_missing_inputs,
        source_cache=source_cache,
        rho_resid_lag_gating=(rho_resid_lag_gating if rho_resid_ar1 else "off"),
        rho_resid_fpe_trap=rho_resid_fpe_trap,
    )
    if rho_resid_ar1:
        return _apply_resid_ar1_value(
            structural_result=structural_result,
            period_position=period_position,
            lhs_value=rho_resid_lhs_value,
            state=rho_resid_state,
            carry_lag=rho_resid_carry_lag,
            carry_damp=rho_resid_carry_damp,
            carry_damp_mode=rho_resid_carry_damp_mode,
            carry_multipass=rho_resid_carry_multipass,
            update_source=rho_resid_update_source,
            la257_update_rule=rho_resid_la257_update_rule,
            la257_fortran_cycle_carry_style=rho_resid_la257_fortran_cycle_carry_style,
            la257_u_phase_max_gain=rho_resid_la257_u_phase_max_gain,
            la257_u_phase_max_gain_mode=rho_resid_la257_u_phase_max_gain_mode,
            update_enabled=rho_resid_update_enabled,
            debug_metrics=rho_resid_debug_metrics,
        )
    if not rho_aware:
        return structural_result

    rho_terms = _extract_rho_terms(spec)
    if not rho_terms:
        return structural_result

    lhs_source = _resolve_eq_term_column(data, spec.lhs)
    if lhs_source is None:
        if strict_missing_inputs:
            raise KeyError(f"missing variable '{spec.lhs}' for equation '{spec.lhs}'")
        return structural_result

    result = structural_result
    for order, coefficient in rho_terms:
        lagged_position = period_position - order
        if lagged_position < 0 or lagged_position >= len(data.index):
            lagged_lhs_value = float("nan")
            lagged_structural = float("nan")
        else:
            lagged_lhs_value = float(data[lhs_source].iat[lagged_position])
            lagged_structural = _evaluate_eq_structural_value_at_period(
                data,
                spec,
                period_position=lagged_position,
                eval_precision=eval_precision,
                term_order=term_order,
                strict_missing_inputs=strict_missing_inputs,
                source_cache=source_cache,
                rho_resid_fpe_trap=rho_resid_fpe_trap,
            )
        result = result + coefficient * lagged_lhs_value
        result = result - coefficient * lagged_structural

    return result


def _evaluate_eq_structural_series(
    data: pd.DataFrame,
    spec: EqSpec,
    *,
    strict_missing_inputs: bool = True,
    rho_resid_lag_gating: str = "off",
    rho_resid_fpe_trap: bool = False,
) -> pd.Series:
    result = pd.Series(0.0, index=data.index, dtype="float64")
    for term in spec.terms:
        # MODEQ may introduce placeholder terms with coefficient 0.0; skip
        # them so lag-boundary NaNs cannot contaminate the equation sum.
        if term.coefficient == 0.0:
            continue
        if not _is_resid_lag_term_enabled(
            term,
            spec=spec,
            rho_resid_lag_gating=rho_resid_lag_gating,
        ):
            continue
        variable = term.variable.upper()
        if variable == "C":
            result = result + term.coefficient
            continue

        if variable == "RHO":
            continue

        source = _resolve_eq_term_column(data, variable)
        if source is None:
            if variable == "TBL2" or variable.startswith("CNST2"):
                continue
            if strict_missing_inputs:
                raise KeyError(f"missing variable '{term.variable}' for equation '{spec.lhs}'")
            continue

        values = data[source]
        if term.lag != 0:
            values = values.shift(-term.lag)
        if rho_resid_fpe_trap:
            try:
                with np.errstate(divide="raise", invalid="raise", over="raise", under="ignore"):
                    scaled = np.multiply(
                        np.float64(term.coefficient),
                        values.to_numpy(dtype="float64"),
                    )
            except FloatingPointError as exc:
                raise FloatingPointError(
                    "floating-point error in structural series evaluation "
                    f"(lhs={spec.lhs}, term={variable}, lag={term.lag})"
                ) from exc
            result = result + pd.Series(scaled, index=result.index, dtype="float64")
        else:
            result = result + term.coefficient * values

    return result


def _evaluate_eq_structural_value_at_period(
    data: pd.DataFrame,
    spec: EqSpec,
    *,
    period_position: int,
    eval_precision: str = "float64",
    term_order: str = "as_parsed",
    strict_missing_inputs: bool = True,
    source_cache: dict[tuple[str, str], str | None] | None = None,
    rho_resid_lag_gating: str = "off",
    rho_resid_fpe_trap: bool = False,
) -> float:
    if eval_precision not in {"float64", "longdouble"}:
        raise ValueError("eval_precision must be 'float64' or 'longdouble'")
    resolved_term_order = str(term_order).strip().lower()
    if resolved_term_order not in {"as_parsed", "by_index"}:
        raise ValueError("term_order must be 'as_parsed' or 'by_index'")
    use_longdouble = eval_precision == "longdouble"
    result: float | np.longdouble = np.longdouble(0.0) if use_longdouble else 0.0

    terms = spec.terms
    if resolved_term_order == "by_index":

        def _key(term: EqTerm) -> tuple[int, int]:
            variable = term.variable.upper()
            if variable == "C":
                return (0, 0)
            if variable == "RHO":
                return (10**9, 0)
            idx = term.index if term.index is not None else 10**8
            return (int(idx), int(term.lag))

        terms = tuple(sorted(terms, key=_key))

    for term in terms:
        # MODEQ may introduce placeholder terms with coefficient 0.0; skip
        # them so lag-boundary NaNs cannot contaminate the equation sum.
        if term.coefficient == 0.0:
            continue
        if not _is_resid_lag_term_enabled(
            term,
            spec=spec,
            rho_resid_lag_gating=rho_resid_lag_gating,
        ):
            continue
        variable = term.variable.upper()
        if variable == "C":
            if rho_resid_fpe_trap:
                try:
                    with np.errstate(
                        divide="raise", invalid="raise", over="raise", under="ignore"
                    ):
                        if use_longdouble:
                            result = np.add(
                                np.longdouble(result),
                                np.longdouble(term.coefficient),
                            )
                        else:
                            result = float(
                                np.add(np.float64(result), np.float64(term.coefficient))
                            )
                except FloatingPointError as exc:
                    raise FloatingPointError(
                        "floating-point error in structural scalar evaluation "
                        f"(lhs={spec.lhs}, term={variable}, lag={term.lag}, period_position={period_position})"
                    ) from exc
            else:
                if use_longdouble:
                    result = np.longdouble(result) + np.longdouble(term.coefficient)
                else:
                    result = result + term.coefficient
            continue
        if variable == "RHO":
            continue

        cache_key = (spec.lhs, variable)
        if source_cache is not None and cache_key in source_cache:
            source = source_cache[cache_key]
        else:
            source = _resolve_eq_term_column(data, variable)
            if source_cache is not None:
                source_cache[cache_key] = source
        if source is None:
            if variable == "TBL2" or variable.startswith("CNST2"):
                continue
            if strict_missing_inputs:
                raise KeyError(f"missing variable '{term.variable}' for equation '{spec.lhs}'")
            continue

        shifted_position = period_position + term.lag
        if shifted_position < 0 or shifted_position >= len(data.index):
            value = float("nan")
        else:
            value = float(data[source].iat[shifted_position])
        if rho_resid_fpe_trap:
            try:
                with np.errstate(divide="raise", invalid="raise", over="raise", under="ignore"):
                    if use_longdouble:
                        contribution = np.multiply(
                            np.longdouble(term.coefficient),
                            np.longdouble(value),
                        )
                        result = np.add(np.longdouble(result), contribution)
                    else:
                        contribution = np.multiply(
                            np.float64(term.coefficient),
                            np.float64(value),
                        )
                        result = float(np.add(np.float64(result), contribution))
            except FloatingPointError as exc:
                raise FloatingPointError(
                    "floating-point error in structural scalar evaluation "
                    f"(lhs={spec.lhs}, term={variable}, lag={term.lag}, period_position={period_position})"
                ) from exc
        else:
            if use_longdouble:
                result = np.longdouble(result) + (
                    np.longdouble(term.coefficient) * np.longdouble(value)
                )
            else:
                result = result + term.coefficient * value

    return float(result)


def _extract_rho_terms(spec: EqSpec) -> tuple[tuple[int, float], ...]:
    rho_by_order: dict[int, float] = {}
    for term in spec.terms:
        if term.variable.upper() != "RHO":
            continue
        if term.coefficient == 0.0:
            continue
        if term.lag >= 0:
            continue
        order = int(-term.lag)
        rho_by_order[order] = rho_by_order.get(order, 0.0) + float(term.coefficient)
    return tuple(sorted(rho_by_order.items()))


def _is_resid_lag_term_enabled(
    term: EqTerm,
    *,
    spec: EqSpec,
    rho_resid_lag_gating: str,
) -> bool:
    if rho_resid_lag_gating == "off":
        return True
    if term.lag >= 0:
        return True
    variable = term.variable.upper()
    if variable in {"C", "RHO"}:
        return True
    if rho_resid_lag_gating == "all":
        return False
    if rho_resid_lag_gating == "lhs":
        return variable != spec.lhs.upper()
    return True


def _extract_rho_lag1(spec: EqSpec) -> float | None:
    for order, coefficient in _extract_rho_terms(spec):
        if order == 1 and coefficient != 0.0:
            return float(coefficient)
    return None


def _apply_resid_ar1_series(
    data: pd.DataFrame,
    spec: EqSpec,
    *,
    structural_result: pd.Series,
    strict_missing_inputs: bool,
    carry_lag: int,
    carry_damp: float,
    carry_damp_mode: str,
    carry_multipass: bool,
    update_source: str,
    la257_update_rule: str,
    la257_fortran_cycle_carry_style: str,
    la257_u_phase_max_gain: float | None,
    la257_u_phase_max_gain_mode: str,
    mask: Sequence[bool] | None,
) -> pd.Series:
    rho_lag1 = _extract_rho_lag1(spec)
    if rho_lag1 is None:
        return structural_result
    state = _RhoResidualState(rho_lag1=float(rho_lag1))

    lhs_source = _resolve_eq_term_column(data, spec.lhs)
    if lhs_source is None:
        if strict_missing_inputs:
            raise KeyError(f"missing variable '{spec.lhs}' for equation '{spec.lhs}'")
        return structural_result

    result = structural_result.copy()
    lhs_values = data[lhs_source]
    active_mask = list(mask) if mask is not None else [True] * len(data.index)
    if len(active_mask) != len(data.index):
        active_mask = [True] * len(data.index)

    for period_position in range(len(result.index)):
        if not active_mask[period_position]:
            continue
        structural_value = float(structural_result.iat[period_position])
        lhs_value = float(lhs_values.iat[period_position])
        result.iat[period_position] = _apply_resid_ar1_value(
            structural_result=structural_value,
            period_position=period_position,
            lhs_value=lhs_value,
            state=state,
            carry_lag=carry_lag,
            carry_damp=carry_damp,
            carry_damp_mode=carry_damp_mode,
            carry_multipass=carry_multipass,
            update_source=update_source,
            la257_update_rule=la257_update_rule,
            la257_fortran_cycle_carry_style=la257_fortran_cycle_carry_style,
            la257_u_phase_max_gain=la257_u_phase_max_gain,
            la257_u_phase_max_gain_mode=la257_u_phase_max_gain_mode,
        )
    return result


def _apply_resid_ar1_value(
    *,
    structural_result: float,
    period_position: int,
    lhs_value: float | None,
    state: _RhoResidualState | None,
    carry_lag: int,
    carry_damp: float,
    carry_damp_mode: str,
    carry_multipass: bool,
    update_source: str,
    la257_update_rule: str = "legacy",
    la257_fortran_cycle_carry_style: str = "legacy",
    la257_u_phase_max_gain: float | None = None,
    la257_u_phase_max_gain_mode: str = "relative",
    update_enabled: bool = True,
    debug_metrics: dict[str, object] | None = None,
) -> float:
    if state is None:
        if debug_metrics is not None:
            debug_metrics.update({
                "structural": float(structural_result),
                "result": float(structural_result),
                "ar_term": 0.0,
                "step_distance": None,
                "residual_used": None,
                "state_residual_before": None,
                "state_position_before": None,
                "state_transient_before": None,
                "state_transient_position_before": None,
                "state_residual_after": None,
                "state_position_after": None,
                "state_transient_after": None,
                "state_transient_position_after": None,
                "rho": None,
                "la257_rule": la257_update_rule,
                "u_raw": None,
                "u_applied": None,
                "gain_cap": None,
                "gain_mode": (
                    la257_u_phase_max_gain_mode if la257_u_phase_max_gain is not None else None
                ),
            })
        return structural_result
    if pd.isna(structural_result):
        if debug_metrics is not None:
            debug_metrics.update({
                "structural": float(structural_result),
                "result": float(structural_result),
                "ar_term": 0.0,
                "step_distance": None,
                "residual_used": None,
                "state_residual_before": (
                    float(state.residual) if state.residual is not None else None
                ),
                "state_position_before": (
                    int(state.residual_position) if state.residual_position is not None else None
                ),
                "state_transient_before": float(state.transient_u),
                "state_transient_position_before": (
                    int(state.transient_u_position)
                    if state.transient_u_position is not None
                    else None
                ),
                "state_residual_after": (
                    float(state.residual) if state.residual is not None else None
                ),
                "state_position_after": (
                    int(state.residual_position) if state.residual_position is not None else None
                ),
                "state_transient_after": float(state.transient_u),
                "state_transient_position_after": (
                    int(state.transient_u_position)
                    if state.transient_u_position is not None
                    else None
                ),
                "rho": float(state.rho_lag1),
                "la257_rule": la257_update_rule,
                "u_raw": None,
                "u_applied": None,
                "gain_cap": None,
                "gain_mode": (
                    la257_u_phase_max_gain_mode if la257_u_phase_max_gain is not None else None
                ),
            })
        return structural_result
    if state.residual is not None and not math.isfinite(float(state.residual)):
        _reset_rho_resid_state(state)

    state_residual_before = float(state.residual) if state.residual is not None else None
    state_position_before = (
        int(state.residual_position) if state.residual_position is not None else None
    )
    state_transient_before = float(state.transient_u)
    state_transient_position_before = (
        int(state.transient_u_position) if state.transient_u_position is not None else None
    )
    ar_term = 0.0
    step_distance: int | None = None
    carry_reject_reason: str | None = None
    if state.residual is not None and state.residual_position is not None:
        step_distance = period_position - state.residual_position
        carry_allowed = carry_lag <= 0 or step_distance <= carry_lag
        if carry_multipass and carry_damp_mode in {"state", "sol4", "la257"}:
            carry_allowed = step_distance == 1
        if step_distance > 0 and carry_allowed:
            if carry_damp_mode in {"state", "sol4", "la257"}:
                ar_term = (state.rho_lag1**step_distance) * state.residual
            else:
                ar_term = (state.rho_lag1**step_distance) * state.residual * carry_damp
            if carry_damp_mode == "la257":
                if carry_multipass or carry_lag <= 0:
                    carry_allowed = step_distance == 1
                else:
                    carry_allowed = step_distance <= carry_lag
                if not carry_allowed:
                    ar_term = 0.0
                    carry_reject_reason = "la257_carry_lag_gate"
        elif step_distance <= 0:
            carry_reject_reason = "nonpositive_step_distance"
        else:
            carry_reject_reason = "carry_lag_or_multipass_gate"
    else:
        carry_reject_reason = "missing_state_seed"

    result = float(structural_result + ar_term)
    if not update_enabled:
        if debug_metrics is not None:
            debug_metrics.update({
                "structural": float(structural_result),
                "result": float(result),
                "ar_term": float(ar_term),
                "step_distance": int(step_distance) if step_distance is not None else None,
                "residual_used": None,
                "state_residual_before": state_residual_before,
                "state_position_before": state_position_before,
                "state_transient_before": state_transient_before,
                "state_transient_position_before": state_transient_position_before,
                "state_residual_after": (
                    float(state.residual) if state.residual is not None else None
                ),
                "state_position_after": (
                    int(state.residual_position) if state.residual_position is not None else None
                ),
                "state_transient_after": float(state.transient_u),
                "state_transient_position_after": (
                    int(state.transient_u_position)
                    if state.transient_u_position is not None
                    else None
                ),
                "carry_reject_reason": carry_reject_reason,
                "rho": float(state.rho_lag1),
                "la257_rule": la257_update_rule,
                "u_raw": None,
                "u_applied": None,
                "gain_cap": None,
                "gain_mode": (
                    la257_u_phase_max_gain_mode if la257_u_phase_max_gain is not None else None
                ),
            })
        return result

    residual: float | None = None
    if carry_damp_mode == "la257":
        if lhs_value is not None and not pd.isna(lhs_value):
            # LA257 residual-state paths are anchored to structural innovation.
            # Rule variants only change how that innovation is persisted.
            residual = float(lhs_value - structural_result)
    elif update_source == "solved":
        residual = float(result - structural_result)
    elif lhs_value is not None and not pd.isna(lhs_value):
        if update_source == "result":
            residual = float(lhs_value - result)
        else:
            residual = float(lhs_value - structural_result)
    state_update_debug: dict[str, object] = {}
    if residual is not None:
        _apply_rho_resid_state_update(
            state=state,
            residual=residual,
            period_position=period_position,
            step_distance=step_distance,
            carry_damp=carry_damp,
            carry_damp_mode=carry_damp_mode,
            carry_multipass=carry_multipass,
            la257_update_rule=la257_update_rule,
            la257_fortran_cycle_carry_style=la257_fortran_cycle_carry_style,
            la257_u_phase_max_gain=la257_u_phase_max_gain,
            la257_u_phase_max_gain_mode=la257_u_phase_max_gain_mode,
            debug_update=state_update_debug,
        )
    if debug_metrics is not None:
        debug_metrics.update({
            "structural": float(structural_result),
            "result": float(result),
            "ar_term": float(ar_term),
            "step_distance": int(step_distance) if step_distance is not None else None,
            "residual_used": float(residual) if residual is not None else None,
            "state_residual_before": state_residual_before,
            "state_position_before": state_position_before,
            "state_transient_before": state_transient_before,
            "state_transient_position_before": state_transient_position_before,
            "state_residual_after": (
                float(state.residual) if state.residual is not None else None
            ),
            "state_position_after": (
                int(state.residual_position) if state.residual_position is not None else None
            ),
            "state_transient_after": float(state.transient_u),
            "state_transient_position_after": (
                int(state.transient_u_position) if state.transient_u_position is not None else None
            ),
            "carry_reject_reason": carry_reject_reason,
            "rho": state_update_debug.get("rho"),
            "la257_rule": state_update_debug.get("la257_rule", la257_update_rule),
            "u_raw": state_update_debug.get("u_raw"),
            "u_applied": state_update_debug.get("u_applied"),
            "gain_cap": state_update_debug.get("gain_cap"),
            "gain_mode": state_update_debug.get("gain_mode"),
        })
    return result


def _resolve_la257_update_rule_for_line(
    *,
    base_rule: str,
    line_number: int,
    occurrence: int | None = None,
    u_phase_lines: set[int] | None,
    u_phase_occurrences: set[tuple[int, int]] | None = None,
) -> str:
    if base_rule not in {"fortran_u_phase", "fortran_cycle"}:
        return base_rule
    if not u_phase_lines and not u_phase_occurrences:
        return base_rule
    if _selector_matches(
        line_number=line_number,
        occurrence=occurrence,
        line_selectors=u_phase_lines,
        occurrence_selectors=u_phase_occurrences,
    ):
        return base_rule
    return "legacy"


def _normalize_la257_u_phase_selectors(
    selectors: Sequence[object] | None,
) -> tuple[set[int] | None, set[tuple[int, int]] | None]:
    return _normalize_line_occurrence_selectors(
        selectors,
        field_name="rho_resid_la257_u_phase_lines",
    )


def _selector_matches(
    *,
    line_number: int,
    occurrence: int | None = None,
    line_selectors: set[int] | None = None,
    occurrence_selectors: set[tuple[int, int]] | None = None,
) -> bool:
    if not line_selectors and not occurrence_selectors:
        return False
    if line_selectors is not None and int(line_number) in line_selectors:
        return True
    return bool(
        occurrence is not None
        and occurrence_selectors is not None
        and (int(line_number), int(occurrence)) in occurrence_selectors
    )


def _normalize_line_occurrence_selectors(
    selectors: Sequence[object] | None,
    *,
    field_name: str,
) -> tuple[set[int] | None, set[tuple[int, int]] | None]:
    if selectors is None:
        return None, None

    line_set: set[int] = set()
    occurrence_set: set[tuple[int, int]] = set()
    error_message = (
        f"{field_name} must contain positive integers or "
        "line:occurrence selectors with non-negative occurrence"
    )
    for raw_selector in selectors:
        try:
            selector = _coerce_line_occurrence_selector(raw_selector)
        except (TypeError, ValueError) as exc:
            raise ValueError(error_message) from exc
        if selector is None:
            continue
        if isinstance(selector, tuple):
            line_number, occurrence = selector
            if line_number <= 0 or occurrence < 0:
                raise ValueError(error_message)
            occurrence_set.add((int(line_number), int(occurrence)))
            continue
        if selector <= 0:
            raise ValueError(error_message)
        line_set.add(int(selector))

    if not line_set and not occurrence_set:
        return None, None
    return (
        line_set if line_set else None,
        occurrence_set if occurrence_set else None,
    )


def _normalize_rho_resid_trace_focus_spec(
    spec: Ar1TraceFocusSpec | None,
) -> Ar1TraceFocusSpec | None:
    if spec is None:
        return None

    targets = frozenset(
        str(target).strip().upper() for target in spec.targets if str(target).strip()
    )
    periods = frozenset(str(period).strip() for period in spec.periods if str(period).strip())
    stages = frozenset(str(stage).strip() for stage in spec.stages if str(stage).strip())
    if not stages:
        stages = _RHO_RESID_TRACE_FOCUS_DEFAULT_STAGES
    unknown_stages = stages.difference(_RHO_RESID_TRACE_FOCUS_STAGE_ORDER.keys())
    if unknown_stages:
        raise ValueError(
            f"rho_resid_trace_focus_spec.stages contains unknown stages: {sorted(unknown_stages)}"
        )

    iter_min = int(spec.iter_min)
    iter_max = int(spec.iter_max)
    if iter_min < 1:
        raise ValueError("rho_resid_trace_focus_spec.iter_min must be >= 1")
    if iter_max < iter_min:
        raise ValueError("rho_resid_trace_focus_spec.iter_max must be >= iter_min")

    max_events = int(spec.max_events)
    if max_events < 0:
        raise ValueError("rho_resid_trace_focus_spec.max_events must be nonnegative")

    return Ar1TraceFocusSpec(
        targets=targets,
        periods=periods,
        iter_min=iter_min,
        iter_max=iter_max,
        max_events=max_events,
        stages=stages,
    )


def _coerce_line_occurrence_selector(raw_selector: object) -> _LineOccurrenceSelector | None:
    if raw_selector is None:
        return None

    if isinstance(raw_selector, str):
        token = raw_selector.strip()
        if not token:
            return None
        if ":" in token:
            left, sep, right = token.partition(":")
            if not sep or ":" in right:
                raise ValueError
            line_number = int(left.strip())
            occurrence = int(right.strip())
            return (line_number, occurrence)
        return int(token)

    if isinstance(raw_selector, (list, tuple)):
        if len(raw_selector) != 2:
            raise ValueError
        line_number = int(raw_selector[0])
        occurrence = int(raw_selector[1])
        return (line_number, occurrence)

    return int(raw_selector)


def _apply_rho_resid_state_update(
    *,
    state: _RhoResidualState,
    residual: float,
    period_position: int,
    step_distance: int | None,
    carry_damp: float,
    carry_damp_mode: str,
    carry_multipass: bool,
    la257_update_rule: str = "legacy",
    la257_fortran_cycle_carry_style: str = "legacy",
    la257_u_phase_max_gain: float | None = None,
    la257_u_phase_max_gain_mode: str = "relative",
    debug_update: dict[str, object] | None = None,
) -> None:
    if not math.isfinite(residual):
        _reset_rho_resid_state(state)
        if debug_update is not None:
            debug_update.update({
                "rho": float(state.rho_lag1),
                "la257_rule": la257_update_rule,
                "u_raw": None,
                "u_applied": None,
                "gain_cap": None,
                "gain_mode": (
                    la257_u_phase_max_gain_mode if la257_u_phase_max_gain is not None else None
                ),
            })
        return
    u_raw: float | None = None
    u_applied: float | None = None
    gain_cap: float | None = None
    state.transient_u = float(residual)
    state.transient_u_position = int(period_position)
    if carry_damp_mode == "state":
        if state.residual is not None:
            previous = float(state.residual)
            if carry_multipass and step_distance is not None and step_distance != 1:
                previous = 0.0
            state.residual = float(previous + carry_damp * (residual - previous))
        else:
            state.residual = residual
    elif carry_damp_mode == "sol4":
        previous = float(state.residual) if state.residual is not None else 0.0
        if carry_multipass and step_distance is not None and step_distance != 1:
            previous = 0.0
        state.residual = float(previous + carry_damp * (residual - previous))
    elif carry_damp_mode == "la257":
        previous = 0.0
        if (
            state.residual is not None
            and state.residual_position is not None
            and period_position - state.residual_position == 1
        ):
            previous = float(state.residual)
        if carry_multipass and step_distance is not None and step_distance != 1:
            previous = 0.0
        if la257_update_rule == "fortran_u_phase":
            if math.isfinite(state.rho_lag1) and abs(state.rho_lag1) >= _LA257_U_PHASE_RHO_MIN:
                u_phase = residual / state.rho_lag1
                u_raw = float(u_phase)
                candidate = carry_damp * u_phase
                if math.isfinite(candidate):
                    if la257_u_phase_max_gain is not None:
                        max_abs = _resolve_la257_u_phase_max_abs(
                            state=state,
                            carry_damp=carry_damp,
                            residual=residual,
                            gain=float(la257_u_phase_max_gain),
                            mode=la257_u_phase_max_gain_mode,
                        )
                        gain_cap = float(max_abs)
                        candidate = float(
                            min(abs(candidate), max_abs) * (1.0 if candidate >= 0 else -1.0)
                        )
                    u_applied = float(candidate)
                    state.residual = float(candidate)
                else:
                    state.residual = float(previous + carry_damp * (residual - previous))
            else:
                state.residual = float(previous + carry_damp * (residual - previous))
        elif la257_update_rule == "fortran_cycle":
            # Experimental cycle rule: treat residual input as post-carry
            # innovation and persist a transient U-like state for the next step.
            if math.isfinite(state.rho_lag1) and abs(state.rho_lag1) >= _LA257_U_PHASE_RHO_MIN:
                u_phase = residual / state.rho_lag1
                u_raw = float(u_phase)
                if la257_fortran_cycle_carry_style == "ema":
                    candidate = previous + carry_damp * (u_phase - previous)
                else:
                    candidate = carry_damp * u_phase
                if math.isfinite(candidate):
                    if la257_u_phase_max_gain is not None:
                        max_abs = _resolve_la257_u_phase_max_abs(
                            state=state,
                            carry_damp=carry_damp,
                            residual=residual,
                            gain=float(la257_u_phase_max_gain),
                            mode=la257_u_phase_max_gain_mode,
                        )
                        gain_cap = float(max_abs)
                        candidate = float(
                            min(abs(candidate), max_abs) * (1.0 if candidate >= 0 else -1.0)
                        )
                    u_applied = float(candidate)
                    state.residual = float(candidate)
                else:
                    state.residual = float(previous + carry_damp * (residual - previous))
            else:
                state.residual = float(previous + carry_damp * (residual - previous))
        else:
            state.residual = float(previous + carry_damp * (residual - previous))
    else:
        state.residual = residual
    state.residual_position = period_position
    if debug_update is not None:
        debug_update.update({
            "rho": float(state.rho_lag1),
            "la257_rule": la257_update_rule,
            "u_raw": u_raw,
            "u_applied": u_applied,
            "gain_cap": gain_cap,
            "gain_mode": (
                la257_u_phase_max_gain_mode if la257_u_phase_max_gain is not None else None
            ),
        })


def _resolve_la257_u_phase_max_abs(
    *,
    state: _RhoResidualState,
    carry_damp: float,
    residual: float,
    gain: float,
    mode: str,
) -> float:
    current_abs = abs(carry_damp * residual)
    if mode == "anchored":
        anchor = state.u_phase_gain_anchor_abs
        if anchor is None or not math.isfinite(anchor) or anchor < 0.0:
            anchor = current_abs
            state.u_phase_gain_anchor_abs = float(anchor)
        return float(anchor * gain)
    return float(current_abs * gain)


def _first_true_position(mask: Sequence[bool]) -> int | None:
    for idx, enabled in enumerate(mask):
        if bool(enabled):
            return idx
    return None


def _normalize_rho_resid_iteration_seed(
    seed: dict[object, object] | None,
    *,
    mode: str,
) -> dict[_RhoResidSeedKey, tuple[float, int | None]]:
    normalized: dict[_RhoResidSeedKey, tuple[float, int | None]] = {}
    if not isinstance(seed, dict):
        return normalized

    for key_raw, raw in seed.items():
        seed_key = _coerce_rho_resid_seed_key(key_raw)
        if seed_key is None:
            continue
        residual_raw: object = raw
        position_raw: object | None = None
        if isinstance(raw, dict):
            residual_raw = raw.get("residual")
            position_raw = raw.get("residual_position", raw.get("position"))
        elif isinstance(raw, (list, tuple)):
            if len(raw) >= 1:
                residual_raw = raw[0]
            if len(raw) >= 2:
                position_raw = raw[1]

        try:
            residual = float(residual_raw)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(residual):
            continue

        position: int | None = None
        if mode == "positioned" and position_raw is not None:
            try:
                position = int(position_raw)
            except (TypeError, ValueError):
                position = None
        normalized[seed_key] = (residual, position)
    return normalized


def _coerce_rho_resid_seed_key(key: object) -> _RhoResidSeedKey | None:
    if isinstance(key, (list, tuple)) and len(key) >= 2:
        try:
            line_number = int(key[0])
            occurrence = int(key[1])
        except (TypeError, ValueError):
            return None
        if occurrence < 0:
            return None
        return (line_number, occurrence)
    try:
        return int(key)
    except (TypeError, ValueError):
        return None


def _build_rho_resid_step_seed_keys(
    deferred_steps: Sequence[_DeferredStep],
) -> dict[int, tuple[int, int]]:
    line_counts: dict[int, int] = {}
    step_keys: dict[int, tuple[int, int]] = {}
    for step_idx, step in enumerate(deferred_steps):
        if step.kind != "eq" or step.eq_spec is None:
            continue
        line_number = int(step.record.line_number)
        occurrence = int(line_counts.get(line_number, 0))
        line_counts[line_number] = occurrence + 1
        step_keys[step_idx] = (line_number, occurrence)
    return step_keys


def _collect_rho_resid_iteration_state(
    deferred_steps: Sequence[_DeferredStep],
    rho_resid_states: dict[int, _RhoResidualState | None],
) -> tuple[tuple[int, float], ...]:
    by_line: dict[int, float] = {}
    for step_idx, state in rho_resid_states.items():
        if state is None or state.residual is None or not math.isfinite(float(state.residual)):
            continue
        if step_idx < 0 or step_idx >= len(deferred_steps):
            continue
        line_number = int(deferred_steps[step_idx].record.line_number)
        by_line[line_number] = float(state.residual)
    return tuple(sorted(by_line.items()))


def _collect_rho_resid_iteration_state_keyed(
    deferred_steps: Sequence[_DeferredStep],
    rho_resid_states: dict[int, _RhoResidualState | None],
    step_seed_keys: dict[int, tuple[int, int]],
) -> tuple[tuple[int, int, float], ...]:
    rows: list[tuple[int, int, float]] = []
    for step_idx, state in rho_resid_states.items():
        if state is None or state.residual is None or not math.isfinite(float(state.residual)):
            continue
        if step_idx < 0 or step_idx >= len(deferred_steps):
            continue
        line_number, occurrence = step_seed_keys.get(
            step_idx,
            (int(deferred_steps[step_idx].record.line_number), 0),
        )
        rows.append((line_number, occurrence, float(state.residual)))
    rows.sort(key=lambda item: (item[0], item[1]))
    return tuple(rows)


def _collect_rho_resid_iteration_state_positioned(
    deferred_steps: Sequence[_DeferredStep],
    rho_resid_states: dict[int, _RhoResidualState | None],
) -> tuple[tuple[int, float, int], ...]:
    rows: list[tuple[int, float, int]] = []
    for step_idx, state in rho_resid_states.items():
        if (
            state is None
            or state.residual is None
            or not math.isfinite(float(state.residual))
            or state.residual_position is None
        ):
            continue
        if step_idx < 0 or step_idx >= len(deferred_steps):
            continue
        line_number = int(deferred_steps[step_idx].record.line_number)
        rows.append((line_number, float(state.residual), int(state.residual_position)))
    rows.sort(key=lambda item: item[0])
    return tuple(rows)


def _collect_rho_resid_iteration_state_positioned_keyed(
    deferred_steps: Sequence[_DeferredStep],
    rho_resid_states: dict[int, _RhoResidualState | None],
    step_seed_keys: dict[int, tuple[int, int]],
) -> tuple[tuple[int, int, float, int], ...]:
    rows: list[tuple[int, int, float, int]] = []
    for step_idx, state in rho_resid_states.items():
        if (
            state is None
            or state.residual is None
            or not math.isfinite(float(state.residual))
            or state.residual_position is None
        ):
            continue
        if step_idx < 0 or step_idx >= len(deferred_steps):
            continue
        line_number, occurrence = step_seed_keys.get(
            step_idx,
            (int(deferred_steps[step_idx].record.line_number), 0),
        )
        rows.append((line_number, occurrence, float(state.residual), int(state.residual_position)))
    rows.sort(key=lambda item: (item[0], item[1]))
    return tuple(rows)


def _resolve_existing_column(data: pd.DataFrame, name: str) -> str | None:
    if name in data.columns:
        return name

    name_upper = str(name).upper()
    name_lower = str(name).lower()
    for column in data.columns:
        if str(column).upper() == name_upper or str(column).lower() == name_lower:
            return str(column)

    return None


def _resolve_eq_term_column(data: pd.DataFrame, variable: str) -> str | None:
    resolved = _resolve_existing_column(data, variable)
    if resolved is not None:
        return resolved

    # Some legacy terms use a Z suffix in coefficient tables while the series
    # in working data uses the unsuffixed base name.
    if variable.endswith("Z") and len(variable) > 1:
        return _resolve_existing_column(data, variable[:-1])
    return None


def _quantize_float32(value: float) -> float:
    # Deterministic float32 round-trip using a stable on-wire format.
    return struct.unpack("<f", struct.pack("<f", float(value)))[0]


def _period_key(value: object) -> tuple[int, int]:
    match = _PERIOD_RE.match(str(value).strip())
    if not match:
        raise ValueError(f"invalid period value: {value!r}")

    year = int(match.group("year"))
    subperiod = int(match.group("subperiod")) if match.group("subperiod") is not None else 0
    return year, subperiod
