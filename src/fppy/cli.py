from __future__ import annotations

import argparse
import json
import math
import os
import re
import time
from collections import Counter
from collections.abc import Sequence
from pathlib import Path

import numpy as np
import pandas as pd

from fppy.config import ModelInputConfig, load_model_config
from fppy.dependency import build_dependency_order
from fppy.eq_solver import (
    Ar1TraceFocusSpec,
    apply_eq_backfill,
    build_coef_table,
    collect_eq_fsr_command_lines,
    load_eq_specs_from_fmout,
)
from fppy.equation_search import DictionaryStore, search_explain
from fppy.executor import SampleWindow, build_execution_plan, parse_smpl_statement
from fppy.expressions import EvalContext, parse_assignment
from fppy.input_tree import parse_fminput_tree_file
from fppy.input_sources import load_named_sources
from fppy.io.input_bundle import load_execution_input_bundle
from fppy.io.legacy_data import parse_fmage_file, parse_fmdata_file, parse_fmexog_file
from fppy.io.template_loader import summarize_fminput
from fppy.mini_run import (
    replay_runtime_post_commands,
)
from fppy.mini_run import (
    run_mini_run as run_mini_run_engine,
)
from fppy.parity import (
    FmoutStructuredData,
    compare_eq_specs,
    compare_numeric_dataframes,
    load_fmout,
    load_fmout_structured,
)
from fppy.parser import FPCommand, count_commands, parse_fminput_file
from fppy.paths import FORTRAN_REFERENCE
from fppy.release import (
    detect_restricted_workspace_paths,
    format_artifact_validation_issues,
    format_release_check_report,
    validate_artifact_directory,
)
from fppy.release_export import ARCHIVE_FORMATS, archive_artifact_tree, export_artifact_tree
from fppy.runtime_commands import SolveCommand, parse_runtime_command
from fppy.solve_setup import extract_setupsolve_config

_RhoResidSeedKey = int | tuple[int, int]
_La257Selector = int | tuple[int, int]
_TRACE_FOCUS_STAGE_ORDER: dict[str, int] = {
    "resid_eval": 0,
    "ar1_state_write": 1,
    "la257_u_phase_update": 2,
    "convergence_check": 3,
}
_OUTSIDE_MISSING_SENTINELS = {-99.0}


def _coerce_backfill_int(value: object, *keys: str) -> int:
    for key in keys:
        raw = value.get(key) if isinstance(value, dict) else getattr(value, key, None)
        if isinstance(raw, int):
            return raw
        if isinstance(raw, float) and raw.is_integer():
            return int(raw)
    return 0


def _coerce_backfill_frame(value: object, fallback: pd.DataFrame) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value
    if isinstance(value, dict):
        frame = value.get("frame", value.get("output"))
    else:
        frame = getattr(value, "frame", getattr(value, "output", fallback))
    if isinstance(frame, pd.DataFrame):
        return frame
    if frame is None:
        return fallback
    return pd.DataFrame(frame)


def _coerce_backfill_issues(value: object) -> list[dict[str, object]]:
    if value is None:
        return []
    if not isinstance(value, (list, tuple, set)):
        return []

    issues = list(value) if not isinstance(value, list) else value
    normalized: list[dict[str, object]] = []
    for issue in issues:
        if isinstance(issue, dict):
            normalized.append({
                "line": issue.get("line", issue.get("line_number")),
                "statement": issue.get("statement"),
                "error": issue.get("error"),
            })
            continue

        normalized.append({
            "line": getattr(issue, "line", getattr(issue, "line_number", None)),
            "statement": getattr(issue, "statement", None),
            "error": getattr(issue, "error", None),
        })
    return normalized


def _coerce_backfill_rho_resid_iteration_state(value: object) -> dict[int, float]:
    if isinstance(value, dict):
        raw_state = value.get("rho_resid_iteration_state")
    else:
        raw_state = getattr(value, "rho_resid_iteration_state", None)

    if raw_state is None:
        return {}

    if isinstance(raw_state, dict):
        items = raw_state.items()
    elif isinstance(raw_state, (list, tuple, set)):
        items = raw_state
    else:
        return {}

    parsed: dict[int, float] = {}
    for item in items:
        line_number = None
        residual = None
        if isinstance(item, dict):
            line_number = item.get("line_number", item.get("line"))
            residual = item.get("residual")
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            line_number, residual = item[0], item[1]
        else:
            continue
        try:
            key = int(line_number)
            value_f = float(residual)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(value_f):
            continue
        parsed[key] = value_f
    return parsed


def _coerce_backfill_rho_resid_iteration_state_keyed(
    value: object,
) -> dict[_RhoResidSeedKey, float]:
    if isinstance(value, dict):
        raw_state = value.get("rho_resid_iteration_state_keyed")
    else:
        raw_state = getattr(value, "rho_resid_iteration_state_keyed", None)

    if raw_state is None:
        return {}

    if isinstance(raw_state, dict):
        items = raw_state.items()
    elif isinstance(raw_state, (list, tuple, set)):
        items = raw_state
    else:
        return {}

    parsed: dict[_RhoResidSeedKey, float] = {}
    for item in items:
        line_number = None
        occurrence = None
        residual = None
        if isinstance(item, dict):
            line_number = item.get("line_number", item.get("line"))
            occurrence = item.get("occurrence")
            residual = item.get("residual")
        elif isinstance(item, (list, tuple)) and len(item) >= 3:
            line_number, occurrence, residual = item[0], item[1], item[2]
        else:
            continue
        try:
            line_key = int(line_number)
            occurrence_key = int(occurrence)
            residual_f = float(residual)
        except (TypeError, ValueError):
            continue
        if occurrence_key < 0 or not math.isfinite(residual_f):
            continue
        parsed[line_key, occurrence_key] = residual_f
    return parsed


def _coerce_backfill_rho_resid_iteration_state_positioned_keyed(
    value: object,
) -> dict[tuple[int, int], tuple[float, int]]:
    if isinstance(value, dict):
        raw_state = value.get("rho_resid_iteration_state_positioned_keyed")
    else:
        raw_state = getattr(value, "rho_resid_iteration_state_positioned_keyed", None)

    if raw_state is None:
        return {}

    if isinstance(raw_state, dict):
        items = raw_state.items()
    elif isinstance(raw_state, (list, tuple, set)):
        items = raw_state
    else:
        return {}

    parsed: dict[tuple[int, int], tuple[float, int]] = {}
    for item in items:
        line_number = None
        occurrence = None
        residual = None
        residual_position = None
        if isinstance(item, dict):
            line_number = item.get("line_number", item.get("line"))
            occurrence = item.get("occurrence")
            residual = item.get("residual")
            residual_position = item.get("residual_position", item.get("position"))
        elif isinstance(item, (list, tuple)) and len(item) >= 4:
            line_number, occurrence, residual, residual_position = (
                item[0],
                item[1],
                item[2],
                item[3],
            )
        else:
            continue
        try:
            line_key = int(line_number)
            occurrence_key = int(occurrence)
            residual_f = float(residual)
            residual_position_key = int(residual_position)
        except (TypeError, ValueError):
            continue
        if occurrence_key < 0 or residual_position_key < 0 or not math.isfinite(residual_f):
            continue
        parsed[line_key, occurrence_key] = (residual_f, residual_position_key)
    return parsed


def _coerce_backfill_rho_resid_trace_events(value: object) -> list[dict[str, object]]:
    if isinstance(value, dict):
        raw_events = value.get("rho_resid_ar1_trace_events")
    else:
        raw_events = getattr(value, "rho_resid_ar1_trace_events", None)

    if raw_events is None:
        return []
    if not isinstance(raw_events, (list, tuple, set)):
        return []

    parsed: list[dict[str, object]] = []
    for entry in raw_events:
        if isinstance(entry, dict):
            parsed.append(dict(entry))
    return parsed


def _coerce_backfill_rho_resid_focus_trace_events(value: object) -> list[dict[str, object]]:
    if isinstance(value, dict):
        raw_events = value.get("rho_resid_ar1_focus_trace_events")
    else:
        raw_events = getattr(value, "rho_resid_ar1_focus_trace_events", None)
    if raw_events is None:
        return []
    if not isinstance(raw_events, (list, tuple, set)):
        return []
    parsed: list[dict[str, object]] = []
    for entry in raw_events:
        if isinstance(entry, dict):
            parsed.append(dict(entry))
    return parsed


def _coerce_backfill_rho_resid_focus_trace_events_truncated(value: object) -> bool:
    if isinstance(value, dict):
        raw = value.get("rho_resid_ar1_focus_trace_events_truncated")
    else:
        raw = getattr(value, "rho_resid_ar1_focus_trace_events_truncated", False)
    return bool(raw)


def _coerce_backfill_context_replay_trace_events(value: object) -> list[dict[str, object]]:
    if isinstance(value, dict):
        raw_events = value.get("context_replay_trace_events")
    else:
        raw_events = getattr(value, "context_replay_trace_events", None)
    if raw_events is None:
        return []
    if not isinstance(raw_events, (list, tuple, set)):
        return []
    parsed: list[dict[str, object]] = []
    for entry in raw_events:
        if isinstance(entry, dict):
            parsed.append(dict(entry))
    return parsed


def _coerce_backfill_context_replay_trace_events_truncated(value: object) -> bool:
    if isinstance(value, dict):
        raw = value.get("context_replay_trace_events_truncated")
    else:
        raw = getattr(value, "context_replay_trace_events_truncated", False)
    return bool(raw)


def _coerce_backfill_modeq_fsr_side_channel_events(value: object) -> list[dict[str, object]]:
    if isinstance(value, dict):
        raw_events = value.get("modeq_fsr_side_channel_events")
    else:
        raw_events = getattr(value, "modeq_fsr_side_channel_events", None)
    if raw_events is None:
        return []
    if not isinstance(raw_events, (list, tuple, set)):
        return []
    parsed: list[dict[str, object]] = []
    for entry in raw_events:
        if isinstance(entry, dict):
            parsed.append(dict(entry))
    return parsed


def _summarize_trace_events_by_field(
    events: list[dict[str, object]],
    field_name: str,
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for event in events:
        value = event.get(field_name)
        if value is None:
            continue
        text = str(value)
        if not text or text.lower() == "none":
            continue
        counts[text] += 1
    return {
        name: int(value)
        for name, value in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    }


def _summarize_trace_events_by_pass(
    events: list[dict[str, object]],
) -> list[dict[str, int]]:
    counts: Counter[int] = Counter()
    for event in events:
        raw_value = event.get("pass")
        if raw_value is None:
            continue
        try:
            pass_number = int(raw_value)
        except (TypeError, ValueError):
            continue
        if pass_number <= 0:
            continue
        counts[pass_number] += 1
    return [
        {"pass": int(pass_number), "events": int(count)}
        for pass_number, count in sorted(counts.items(), key=lambda item: int(item[0]))
    ]


def _slice_trace_events_for_report(
    events: list[dict[str, object]],
    *,
    head: int = 20,
    tail: int = 20,
) -> list[dict[str, object]]:
    if not events:
        return []
    max_head = max(0, int(head))
    max_tail = max(0, int(tail))
    total = max_head + max_tail
    if total <= 0 or len(events) <= total:
        return list(events)
    if max_head == 0:
        return list(events[-max_tail:])
    if max_tail == 0:
        return list(events[:max_head])
    return list(events[:max_head]) + list(events[-max_tail:])


def _env_flag_enabled(name: str) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _assert_fp_semantics_trace_invariants(
    *,
    trace_events: list[dict[str, object]],
    focus_events: list[dict[str, object]],
    iteration: int,
    expected_commit_mode: str,
    commit_after_check: bool,
    side_channel_mode: str,
    side_channel_status: str,
    side_channel_breach_count: int,
) -> str | None:
    for event in trace_events:
        raw_commit_mode = event.get("commit_mode")
        if raw_commit_mode is None:
            continue
        commit_mode = str(raw_commit_mode)
        if commit_mode != expected_commit_mode:
            return (
                f"iteration {iteration}: trace commit_mode mismatch "
                f"(expected={expected_commit_mode}, got={commit_mode})"
            )

    if commit_after_check:
        for event in trace_events:
            if str(event.get("kind", "")) == "solve_and_update":
                return (
                    f"iteration {iteration}: found solve_and_update while deferred commit "
                    "mode is active"
                )

    per_target_stage_seq: dict[tuple[str, str], dict[str, int]] = {}
    for event in focus_events:
        try:
            iter_value = int(event.get("iter", -1))
        except (TypeError, ValueError):
            continue
        if iter_value != iteration:
            continue
        stage = str(event.get("stage", ""))
        if stage not in {"resid_eval", "convergence_check"}:
            continue
        period = str(event.get("period", ""))
        target = str(event.get("target", ""))
        if not period or not target:
            continue
        raw_seq = event.get("seq")
        try:
            seq_value = int(raw_seq)
        except (TypeError, ValueError):
            seq_value = len(per_target_stage_seq) + 1
        key = (period, target)
        slot = per_target_stage_seq.setdefault(key, {})
        if stage not in slot:
            slot[stage] = seq_value
        else:
            slot[stage] = min(slot[stage], seq_value)

    for (period, target), stage_seq in per_target_stage_seq.items():
        conv_seq = stage_seq.get("convergence_check")
        if conv_seq is None:
            continue
        resid_seq = stage_seq.get("resid_eval")
        if resid_seq is None:
            return (
                "iteration "
                f"{iteration}: convergence_check emitted before resid_eval for "
                f"target={target} period={period}"
            )
        if resid_seq > conv_seq:
            return (
                "iteration "
                f"{iteration}: resid_eval appears after convergence_check for "
                f"target={target} period={period}"
            )

    if side_channel_mode == "enforce" and side_channel_breach_count > 0:
        if side_channel_status not in {"aborted", "violated"}:
            return (
                f"iteration {iteration}: enforce side-channel breach_count="
                f"{side_channel_breach_count} with unexpected status={side_channel_status}"
            )

    return None


_NONFINITE_TRACE_NUMERIC_FIELDS = (
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
_NONFINITE_TRACE_SNAPSHOT_FIELDS = (
    "structural",
    "result",
    "ar_term",
    "residual_used",
    "step_distance",
    "state_residual_before",
    "state_residual_after",
    "state_position_before",
    "state_position_after",
    "state_transient_before",
    "state_transient_after",
    "state_transient_position_before",
    "state_transient_position_after",
)
_FAILURE_TRACE_SNAPSHOT_FIELDS = (
    "op",
    "message",
    "statement",
    "error",
    "hf_lag",
    "lhf1",
    "log_abs_hf_lag",
    "log_product",
    "overflow_log_limit",
    "margin_to_overflow",
    "would_overflow",
    "hf_lag_source",
    "hf_lag_period_position",
    "command",
)
_FLOAT64_LOG_MAX = 709.782712893384


def _compute_hf_line182_probe_at_position(
    *,
    index: pd.Index,
    hf_values: pd.Series,
    lhf1_values: pd.Series,
    period_position: int,
) -> dict[str, object] | None:
    if period_position <= 0 or period_position >= len(index):
        return None
    try:
        hf_lag = float(hf_values.iat[period_position - 1])
        lhf1 = float(lhf1_values.iat[period_position])
    except (TypeError, ValueError):
        return None
    if not math.isfinite(hf_lag) or not math.isfinite(lhf1) or hf_lag == 0.0:
        return None

    log_abs_hf_lag = float(math.log(abs(hf_lag)))
    log_product = float(log_abs_hf_lag + lhf1)
    margin_to_overflow = float(_FLOAT64_LOG_MAX - log_product)
    return {
        "period": str(index[period_position]),
        "period_prev": str(index[period_position - 1]),
        "period_position": int(period_position),
        "period_prev_position": int(period_position - 1),
        "hf_lag": float(hf_lag),
        "lhf1": float(lhf1),
        "log_abs_hf_lag": float(log_abs_hf_lag),
        "log_product": float(log_product),
        "overflow_log_limit": float(_FLOAT64_LOG_MAX),
        "margin_to_overflow": float(margin_to_overflow),
        "would_overflow": bool(log_product > _FLOAT64_LOG_MAX),
    }


def _compute_hf_line182_overflow_probe(
    frame: pd.DataFrame,
    *,
    focus_period: str = "2024.2",
) -> dict[str, object] | None:
    if frame.empty:
        return None
    hf_column = _resolve_column_name(frame, "HF")
    lhf1_column = _resolve_column_name(frame, "LHF1")
    if hf_column is None or lhf1_column is None:
        return None

    hf_values = pd.to_numeric(frame[hf_column], errors="coerce")
    lhf1_values = pd.to_numeric(frame[lhf1_column], errors="coerce")
    if len(frame.index) < 2:
        return None

    max_probe: dict[str, object] | None = None
    for period_position in range(1, len(frame.index)):
        probe = _compute_hf_line182_probe_at_position(
            index=frame.index,
            hf_values=hf_values,
            lhf1_values=lhf1_values,
            period_position=period_position,
        )
        if probe is None:
            continue
        if max_probe is None:
            max_probe = probe
            continue
        current_log = float(probe["log_product"])
        max_log = float(max_probe["log_product"])
        if current_log > max_log:
            max_probe = probe

    focus_probe: dict[str, object] | None = None
    if focus_period in frame.index:
        focus_position = int(frame.index.get_loc(focus_period))
        focus_probe = _compute_hf_line182_probe_at_position(
            index=frame.index,
            hf_values=hf_values,
            lhf1_values=lhf1_values,
            period_position=focus_position,
        )

    if max_probe is None and focus_probe is None:
        return None
    return {
        "hf_column": str(hf_column),
        "lhf1_column": str(lhf1_column),
        "focus_period": str(focus_period),
        "focus_period_probe": focus_probe,
        "max_log_product_probe": max_probe,
    }


def _compute_hf_lhf1_ho_iteration_probe(
    frame: pd.DataFrame,
    *,
    focus_period: str = "2024.2",
) -> dict[str, object] | None:
    if frame.empty:
        return None

    focus_period_text = str(focus_period)
    focus_position: int | None = None
    focus_prev_period: str | None = None
    focus_positions = frame.index.get_indexer_for([focus_period_text])
    for candidate_position in focus_positions:
        if int(candidate_position) >= 0:
            focus_position = int(candidate_position)
            break
    if focus_position is not None and focus_position > 0:
        focus_prev_period = str(frame.index[focus_position - 1])

    probes: dict[str, dict[str, object]] = {}
    for target in ("HF", "LHF1", "HO"):
        column = _resolve_column_name(frame, target)
        if column is None:
            continue

        values = pd.to_numeric(frame[column], errors="coerce")
        finite_abs = values.abs()
        finite_abs = finite_abs[finite_abs.notna() & finite_abs.map(math.isfinite)]

        max_abs_period: str | None = None
        max_abs_value: float | None = None
        max_abs_source_value: float | None = None
        if not finite_abs.empty:
            max_index = finite_abs.idxmax()
            max_abs_period = str(max_index)
            max_abs_value = float(finite_abs.loc[max_index])
            source_value = values.loc[max_index]
            if source_value is not None:
                source_value_f = float(source_value)
                if math.isfinite(source_value_f):
                    max_abs_source_value = source_value_f

        focus_value: float | None = None
        if focus_position is not None:
            raw_focus = values.iat[focus_position]
            if raw_focus is not None:
                raw_focus_f = float(raw_focus)
                if math.isfinite(raw_focus_f):
                    focus_value = raw_focus_f

        focus_prev_value: float | None = None
        if focus_position is not None and focus_position > 0:
            raw_prev = values.iat[focus_position - 1]
            if raw_prev is not None:
                raw_prev_f = float(raw_prev)
                if math.isfinite(raw_prev_f):
                    focus_prev_value = raw_prev_f

        probes[target] = {
            "column": str(column),
            "focus_period_value": focus_value,
            "focus_prev_period_value": focus_prev_value,
            "max_abs_period": max_abs_period,
            "max_abs_value": max_abs_value,
            "max_abs_source_value": max_abs_source_value,
        }

    if not probes:
        return None

    return {
        "focus_period": focus_period_text,
        "focus_period_position": focus_position,
        "focus_prev_period": focus_prev_period,
        "variables": probes,
    }


def _extract_nonfinite_trace_detail(event: dict[str, object]) -> tuple[str, float] | None:
    for field_name in _NONFINITE_TRACE_NUMERIC_FIELDS:
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


def _build_first_nonfinite_blame_from_trace_events(
    events: Sequence[dict[str, object]],
    *,
    iteration: int,
) -> dict[str, object] | None:
    first_any: dict[str, object] | None = None
    for event in events:
        detail = _extract_nonfinite_trace_detail(event)
        if detail is None:
            continue
        field_name, nonfinite_value = detail
        snapshot: dict[str, object] = {}
        for key in _NONFINITE_TRACE_SNAPSHOT_FIELDS:
            if key not in event:
                continue
            snapshot[key] = event.get(key)
        candidate = {
            "iteration": int(iteration),
            "phase": str(event.get("phase", "unknown")),
            "kind": str(event.get("kind", "unknown")),
            "line": event.get("line"),
            "occurrence": event.get("occurrence"),
            "target": event.get("target"),
            "period": event.get("period"),
            "period_position": event.get("period_position"),
            "op": "nonfinite_value",
            "nonfinite_field": field_name,
            "nonfinite_value": float(nonfinite_value),
            "snapshot": snapshot,
        }
        if first_any is None:
            first_any = candidate
        if not math.isnan(float(nonfinite_value)):
            return candidate
    return first_any


def _build_first_failure_blame_from_issues(
    issues: Sequence[dict[str, object]],
    *,
    iteration: int,
    trace_events: Sequence[dict[str, object]] | None = None,
) -> dict[str, object] | None:
    trace_lookup = tuple(trace_events or ())

    def _build_trace_candidate(
        *,
        line: int | None,
    ) -> dict[str, object] | None:
        for event in trace_lookup:
            kind = str(event.get("kind", ""))
            if kind not in {"assignment_pre_overflow", "assignment_double_write", "fpe_exception"}:
                continue
            event_line_raw = event.get("line")
            try:
                event_line = int(event_line_raw) if event_line_raw is not None else None
            except (TypeError, ValueError):
                event_line = None
            if line is not None and event_line is not None and event_line != line:
                continue

            snapshot: dict[str, object] = {}
            for key in _FAILURE_TRACE_SNAPSHOT_FIELDS:
                if key in event:
                    snapshot[key] = event.get(key)
            return {
                "iteration": int(iteration),
                "phase": str(event.get("phase", "iteration")),
                "kind": kind,
                "line": event_line,
                "occurrence": event.get("occurrence"),
                "target": event.get("target"),
                "period": event.get("period"),
                "period_position": event.get("period_position"),
                "op": str(event.get("op", "failure")),
                "error": (str(event.get("message")) if event.get("message") is not None else None),
                "statement": None,
                "snapshot": snapshot,
            }
        return None

    for issue in issues:
        if not isinstance(issue, dict):
            continue

        line_raw = issue.get("line")
        statement = issue.get("statement")
        raw_error = issue.get("error")

        if line_raw is None and statement is None and raw_error is None:
            continue

        try:
            line = int(line_raw) if line_raw is not None else None
        except (TypeError, ValueError):
            line = None

        error = str(raw_error) if raw_error is not None else None
        trace_candidate = _build_trace_candidate(line=line)
        if trace_candidate is not None:
            trace_candidate["statement"] = statement
            if trace_candidate.get("error") is None:
                trace_candidate["error"] = error
            snapshot = trace_candidate.get("snapshot")
            if not isinstance(snapshot, dict):
                snapshot = {}
            if statement is not None:
                snapshot.setdefault("statement", statement)
            if error is not None:
                snapshot.setdefault("error", error)
            trace_candidate["snapshot"] = snapshot
            return trace_candidate
        return {
            "iteration": int(iteration),
            "phase": "iteration",
            "kind": "eq_backfill_failure",
            "line": line,
            "occurrence": issue.get("occurrence"),
            "target": None,
            "period": None,
            "period_position": None,
            "op": "failure",
            "error": error,
            "statement": statement,
            "snapshot": {
                "statement": statement,
                "error": error,
            },
        }

    return None


def _find_first_nonfinite_frame_cell(
    frame: pd.DataFrame,
    *,
    targets: tuple[str, ...],
    windows: tuple[tuple[str, str], ...],
) -> dict[str, object] | None:
    if frame.empty:
        return None

    scoped_index = _resolve_metric_index(frame, frame, windows=windows)
    if scoped_index.empty:
        return None

    columns: list[str] = []
    if targets:
        for target in targets:
            resolved = _resolve_column_name(frame, target)
            if resolved is not None and resolved not in columns:
                columns.append(resolved)
    if not columns:
        columns = [str(column) for column in frame.columns]

    for period in scoped_index:
        for column in columns:
            if column not in frame.columns:
                continue
            raw = frame.at[period, column]
            if raw is None or isinstance(raw, bool):
                continue
            try:
                value = float(raw)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(value):
                return {
                    "target": str(column),
                    "period": str(period),
                    "value": float(value),
                }
    return None


def _resolve_la257_update_rule_for_iteration(
    base_rule: str,
    *,
    iteration: int,
    start_iter: int,
    disable_iter: int,
) -> str:
    if base_rule == "legacy":
        return "legacy"
    if int(iteration) < int(start_iter):
        return "legacy"
    if int(disable_iter) > 0 and int(iteration) == int(disable_iter):
        return "legacy"
    return base_rule


def _resolve_rho_resid_ar1_enabled_for_iteration(
    enabled: bool,
    *,
    iteration: int,
    start_iter: int,
    disable_iter: int,
) -> bool:
    if not bool(enabled):
        return False
    if int(iteration) < int(start_iter):
        return False
    return not (int(disable_iter) > 0 and int(iteration) == int(disable_iter))


def _summarize_rho_resid_iteration_seed(
    seed: dict[object, object],
    *,
    top: int = 5,
) -> list[dict[str, object]]:
    if top <= 0 or not isinstance(seed, dict) or not seed:
        return []

    keyed_line_occ: set[tuple[int, int]] = set()
    for raw_key in seed:
        if not isinstance(raw_key, tuple) or len(raw_key) < 2:
            continue
        try:
            keyed_line_occ.add((int(raw_key[0]), int(raw_key[1])))
        except (TypeError, ValueError):
            continue

    rows: list[dict[str, object]] = []
    for raw_key, raw_value in seed.items():
        line_number: int | None = None
        occurrence: int | None = None
        if isinstance(raw_key, tuple) and len(raw_key) >= 2:
            try:
                line_number = int(raw_key[0])
                occurrence = int(raw_key[1])
            except (TypeError, ValueError):
                continue
        else:
            try:
                line_number = int(raw_key)
            except (TypeError, ValueError):
                continue
            if (line_number, 0) in keyed_line_occ:
                # Prefer keyed state records when both keyed and legacy line-only
                # entries are present for the same equation.
                continue

        residual_raw = raw_value
        residual_position: int | None = None
        if isinstance(raw_value, (list, tuple)) and len(raw_value) >= 1:
            residual_raw = raw_value[0]
            if len(raw_value) >= 2:
                try:
                    residual_position = int(raw_value[1])
                except (TypeError, ValueError):
                    residual_position = None

        try:
            residual = float(residual_raw)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(residual):
            continue

        row: dict[str, object] = {
            "line": int(line_number),
            "residual": residual,
            "abs_residual": abs(residual),
        }
        if occurrence is not None:
            row["occurrence"] = int(occurrence)
        if residual_position is not None:
            row["residual_position"] = int(residual_position)
        rows.append(row)

    rows.sort(
        key=lambda row: (
            -float(row.get("abs_residual", 0.0)),
            int(row.get("line", 0)),
            int(row.get("occurrence", -1)),
        )
    )
    return rows[:top]


def _trace_rho_resid_iteration_seed(
    seed: dict[object, object],
    *,
    lines: Sequence[_La257Selector],
) -> list[dict[str, object]]:
    if not isinstance(seed, dict) or not seed:
        return []

    ordered_selectors: list[_La257Selector] = []
    seen_selectors: set[_La257Selector] = set()
    for raw_selector in lines:
        selector: _La257Selector
        if isinstance(raw_selector, tuple):
            if len(raw_selector) < 2:
                continue
            try:
                line = int(raw_selector[0])
                occurrence = int(raw_selector[1])
            except (TypeError, ValueError):
                continue
            selector = (line, occurrence)
        else:
            try:
                selector = int(raw_selector)
            except (TypeError, ValueError):
                continue
        if selector in seen_selectors:
            continue
        seen_selectors.add(selector)
        ordered_selectors.append(selector)
    if not ordered_selectors:
        return []

    keyed: dict[tuple[int, int], tuple[float, int | None]] = {}
    legacy: dict[int, tuple[float, int | None]] = {}
    for raw_key, raw_value in seed.items():
        residual_raw = raw_value
        residual_position: int | None = None
        if isinstance(raw_value, (list, tuple)) and len(raw_value) >= 1:
            residual_raw = raw_value[0]
            if len(raw_value) >= 2:
                try:
                    residual_position = int(raw_value[1])
                except (TypeError, ValueError):
                    residual_position = None

        try:
            residual = float(residual_raw)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(residual):
            continue

        if isinstance(raw_key, tuple) and len(raw_key) >= 2:
            try:
                line_key = int(raw_key[0])
                occ_key = int(raw_key[1])
            except (TypeError, ValueError):
                continue
            keyed[line_key, occ_key] = (residual, residual_position)
            continue
        try:
            line_key = int(raw_key)
        except (TypeError, ValueError):
            continue
        legacy[line_key] = (residual, residual_position)

    rows: list[dict[str, object]] = []
    for selector in ordered_selectors:
        if isinstance(selector, tuple):
            line, occurrence = selector
            payload = keyed.get((line, occurrence))
            if payload is None:
                continue
            residual, residual_position = payload
            row = {
                "line": int(line),
                "occurrence": int(occurrence),
                "residual": float(residual),
                "abs_residual": abs(float(residual)),
            }
            if residual_position is not None:
                row["residual_position"] = int(residual_position)
            rows.append(row)
            continue

        line = int(selector)
        keyed_rows = sorted(
            (
                (occ, payload[0], payload[1])
                for (line_key, occ), payload in keyed.items()
                if line_key == line
            ),
            key=lambda item: item[0],
        )
        if keyed_rows:
            for occ, residual, residual_position in keyed_rows:
                row: dict[str, object] = {
                    "line": int(line),
                    "occurrence": int(occ),
                    "residual": float(residual),
                    "abs_residual": abs(float(residual)),
                }
                if residual_position is not None:
                    row["residual_position"] = int(residual_position)
                rows.append(row)
            continue
        if line in legacy:
            residual, residual_position = legacy[line]
            row = {
                "line": int(line),
                "residual": float(residual),
                "abs_residual": abs(float(residual)),
            }
            if residual_position is not None:
                row["residual_position"] = int(residual_position)
            rows.append(row)
    return rows


def _parse_line_number_csv(
    value: str | None,
    *,
    field_name: str = "eq_rho_resid_ar1_trace_lines",
) -> tuple[int, ...]:
    if value is None:
        return ()
    tokens = [token.strip() for token in str(value).split(",")]
    parsed: list[int] = []
    seen: set[int] = set()
    for token in tokens:
        if not token:
            continue
        try:
            line = int(token)
        except ValueError as exc:
            raise ValueError(
                f"{field_name} must be a comma-separated list of positive integers"
            ) from exc
        if line <= 0:
            raise ValueError(f"{field_name} must be a comma-separated list of positive integers")
        if line in seen:
            continue
        seen.add(line)
        parsed.append(line)
    return tuple(parsed)


def _parse_la257_selector_csv(
    value: str | None,
    *,
    field_name: str = "eq_rho_resid_ar1_la257_u_phase_lines",
) -> tuple[_La257Selector, ...]:
    if value is None:
        return ()
    tokens = [token.strip() for token in str(value).split(",")]
    parsed: list[_La257Selector] = []
    seen: set[_La257Selector] = set()
    error_message = (
        f"{field_name} must be a comma-separated list of positive integers or "
        "line:occurrence selectors with non-negative occurrence"
    )
    for token in tokens:
        if not token:
            continue
        selector: _La257Selector
        if ":" in token:
            left, sep, right = token.partition(":")
            if not sep or ":" in right:
                raise ValueError(error_message)
            try:
                line = int(left.strip())
                occurrence = int(right.strip())
            except ValueError as exc:
                raise ValueError(error_message) from exc
            if line <= 0 or occurrence < 0:
                raise ValueError(error_message)
            selector = (line, occurrence)
        else:
            try:
                line = int(token)
            except ValueError as exc:
                raise ValueError(error_message) from exc
            if line <= 0:
                raise ValueError(error_message)
            selector = line
        if selector in seen:
            continue
        seen.add(selector)
        parsed.append(selector)
    return tuple(parsed)


def _parse_focus_csv(
    value: str | None,
    *,
    field_name: str,
) -> tuple[str, ...]:
    if value is None:
        return ()
    tokens = [token.strip() for token in str(value).split(",")]
    parsed: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if not token:
            continue
        key = token.upper() if "target" in field_name else token
        if key in seen:
            continue
        seen.add(key)
        parsed.append(token if "target" not in field_name else key)
    return tuple(parsed)


def _parse_focus_iter_range(
    value: str | None,
    *,
    field_name: str = "eq_rho_resid_ar1_trace_focus_iters",
) -> tuple[int, int]:
    raw = "1..5" if value is None else str(value).strip()
    match = re.fullmatch(r"(\d+)\s*\.\.\s*(\d+)", raw)
    if match is None:
        raise ValueError(f"{field_name} must be in '<min>..<max>' format (for example: 1..5)")
    iter_min = int(match.group(1))
    iter_max = int(match.group(2))
    if iter_min < 1 or iter_max < iter_min:
        raise ValueError(f"{field_name} must satisfy 1 <= min <= max")
    return iter_min, iter_max


def _format_la257_selectors(selectors: Sequence[_La257Selector]) -> list[int | str]:
    formatted: list[int | str] = []
    for selector in selectors:
        if isinstance(selector, tuple):
            formatted.append(f"{int(selector[0])}:{int(selector[1])}")
        else:
            formatted.append(int(selector))
    return formatted


def _max_abs_numeric_delta(before: pd.DataFrame, after: pd.DataFrame) -> float:
    return _max_numeric_delta(
        before,
        after,
        relative_with_zero_absolute_fallback=False,
    )


def _max_tolerance_delta(
    before: pd.DataFrame,
    after: pd.DataFrame,
    *,
    relative_with_zero_absolute_fallback: bool,
) -> float:
    return _max_numeric_delta(
        before,
        after,
        relative_with_zero_absolute_fallback=relative_with_zero_absolute_fallback,
    )


def _max_numeric_delta(
    before: pd.DataFrame,
    after: pd.DataFrame,
    *,
    relative_with_zero_absolute_fallback: bool,
    targets: tuple[str, ...] = tuple(),
    windows: tuple[tuple[str, str], ...] = tuple(),
) -> float:
    if before.empty or after.empty:
        return 0.0
    shared = _resolve_metric_columns(before, after, targets=targets)
    if not shared:
        return 0.0

    scoped_index = _resolve_metric_index(before, after, windows=windows)
    if scoped_index.empty:
        return 0.0

    max_delta = 0.0
    for column in shared:
        left = pd.to_numeric(before.loc[scoped_index, column], errors="coerce")
        right = pd.to_numeric(after.loc[scoped_index, column], errors="coerce")
        delta = (left - right).abs()
        if relative_with_zero_absolute_fallback:
            scaled = delta.copy()
            denom = left.abs()
            ratio_mask = delta.notna() & denom.notna() & (denom != 0)
            scaled.loc[ratio_mask] = delta.loc[ratio_mask] / denom.loc[ratio_mask]
            candidate = scaled
        else:
            candidate = delta
        if not candidate.notna().any():
            continue
        column_max = float(candidate.max(skipna=True))
        if column_max > max_delta:
            max_delta = column_max
    return max_delta


def _resolve_metric_columns(
    before: pd.DataFrame,
    after: pd.DataFrame,
    *,
    targets: tuple[str, ...],
) -> list[str]:
    shared = [column for column in before.columns if column in after.columns]
    if not shared:
        return []
    if not targets:
        return shared

    shared_upper = {str(column).upper(): str(column) for column in shared}
    scoped: list[str] = []
    for target in targets:
        match = shared_upper.get(str(target).upper())
        if match is None:
            continue
        scoped.append(match)

    if not scoped:
        return shared
    return sorted(set(scoped))


def _resolve_metric_index(
    before: pd.DataFrame,
    after: pd.DataFrame,
    *,
    windows: tuple[tuple[str, str], ...],
) -> pd.Index:
    shared_index = before.index.intersection(after.index)
    if not windows or shared_index.empty:
        return shared_index

    scope_mask = _build_scope_mask(shared_index, windows)
    scoped_index = shared_index[scope_mask.to_numpy()]
    if scoped_index.empty:
        return shared_index
    return scoped_index


def _compute_convergence_metrics(
    before: pd.DataFrame,
    after: pd.DataFrame,
    *,
    targets: tuple[str, ...] = tuple(),
    windows: tuple[tuple[str, str], ...] = tuple(),
    default_tol: float,
    default_relative_mode: bool,
    tolerance_overrides: dict[str, float] | None = None,
    absolute_mode_overrides: set[str] | None = None,
) -> tuple[float, float, float]:
    if before.empty or after.empty:
        return 0.0, 0.0, 0.0
    shared = _resolve_metric_columns(before, after, targets=targets)
    if not shared:
        return 0.0, 0.0, 0.0

    scoped_index = _resolve_metric_index(before, after, windows=windows)
    if scoped_index.empty:
        return 0.0, 0.0, 0.0

    tol_map = {
        str(key).upper(): float(value) for key, value in (tolerance_overrides or {}).items()
    }
    abs_mode = {str(key).upper() for key in (absolute_mode_overrides or set())}
    max_abs_delta = 0.0
    max_mode_delta = 0.0
    max_ratio = 0.0

    for column in shared:
        left = pd.to_numeric(before.loc[scoped_index, column], errors="coerce")
        right = pd.to_numeric(after.loc[scoped_index, column], errors="coerce")
        delta_abs = (left - right).abs()
        if not delta_abs.notna().any():
            continue

        col_key = str(column).upper()
        use_relative = default_relative_mode and col_key not in abs_mode
        if use_relative:
            mode_delta = delta_abs.copy()
            denom = left.abs()
            ratio_mask = delta_abs.notna() & denom.notna() & (denom != 0)
            mode_delta.loc[ratio_mask] = delta_abs.loc[ratio_mask] / denom.loc[ratio_mask]
        else:
            mode_delta = delta_abs

        abs_max = float(delta_abs.max(skipna=True))
        mode_max = float(mode_delta.max(skipna=True))
        tol = float(tol_map.get(col_key, default_tol))
        ratio = (0.0 if mode_max == 0.0 else float("inf")) if tol == 0.0 else mode_max / tol

        max_abs_delta = max(max_abs_delta, abs_max)
        max_mode_delta = max(max_mode_delta, mode_max)
        max_ratio = max(max_ratio, ratio)

    return max_abs_delta, max_mode_delta, max_ratio


def _compute_top_target_deltas(
    before: pd.DataFrame,
    after: pd.DataFrame,
    *,
    targets: tuple[str, ...] = tuple(),
    windows: tuple[tuple[str, str], ...] = tuple(),
    default_tol: float,
    default_relative_mode: bool,
    top: int,
    tolerance_overrides: dict[str, float] | None = None,
    absolute_mode_overrides: set[str] | None = None,
) -> list[dict[str, object]]:
    if top <= 0 or before.empty or after.empty:
        return []
    shared = _resolve_metric_columns(before, after, targets=targets)
    if not shared:
        return []

    scoped_index = _resolve_metric_index(before, after, windows=windows)
    if scoped_index.empty:
        return []

    tol_map = {
        str(key).upper(): float(value) for key, value in (tolerance_overrides or {}).items()
    }
    abs_mode = {str(key).upper() for key in (absolute_mode_overrides or set())}
    rows: list[dict[str, object]] = []

    for column in shared:
        left = pd.to_numeric(before.loc[scoped_index, column], errors="coerce")
        right = pd.to_numeric(after.loc[scoped_index, column], errors="coerce")
        delta_abs = (left - right).abs()
        if not delta_abs.notna().any():
            continue

        col_key = str(column).upper()
        use_relative = default_relative_mode and col_key not in abs_mode
        if use_relative:
            mode_delta = delta_abs.copy()
            denom = left.abs()
            ratio_mask = delta_abs.notna() & denom.notna() & (denom != 0)
            mode_delta.loc[ratio_mask] = delta_abs.loc[ratio_mask] / denom.loc[ratio_mask]
        else:
            mode_delta = delta_abs
        mode_valid = mode_delta.dropna()
        if mode_valid.empty:
            continue

        mode_max = float(mode_valid.max())
        abs_max = float(delta_abs.max(skipna=True))
        tol = float(tol_map.get(col_key, default_tol))
        ratio = (0.0 if mode_max == 0.0 else float("inf")) if tol == 0.0 else mode_max / tol

        worst_period = str(mode_valid.idxmax())
        rows.append({
            "target": str(column),
            "max_abs_delta": abs_max,
            "max_mode_delta": mode_max,
            "ratio_to_tol": ratio,
            "worst_period": worst_period,
        })

    rows.sort(
        key=lambda item: (
            -float(item.get("ratio_to_tol", 0.0)),
            -float(item.get("max_mode_delta", 0.0)),
            str(item.get("target", "")),
        )
    )
    return rows[:top]


def _compute_target_period_delta(
    before: pd.DataFrame,
    after: pd.DataFrame,
    *,
    target: str,
    period: str,
    default_tol: float,
    default_relative_mode: bool,
    tolerance_overrides: dict[str, float] | None = None,
    absolute_mode_overrides: set[str] | None = None,
) -> dict[str, object] | None:
    if before.empty or after.empty:
        return None
    shared_columns = [column for column in before.columns if column in after.columns]
    if not shared_columns:
        return None
    target_lookup = {str(column).upper(): str(column) for column in shared_columns}
    column = target_lookup.get(str(target).upper())
    if column is None:
        return None

    shared_index = before.index.intersection(after.index)
    period_key = str(period)
    period_label = next((label for label in shared_index if str(label) == period_key), None)
    if period_label is None:
        return None

    left_value = pd.to_numeric(pd.Series([before.at[period_label, column]]), errors="coerce").iat[
        0
    ]
    right_value = pd.to_numeric(pd.Series([after.at[period_label, column]]), errors="coerce").iat[
        0
    ]
    if pd.isna(left_value) or pd.isna(right_value):
        return None

    delta_abs = float(abs(float(left_value) - float(right_value)))
    col_key = str(column).upper()
    tol_map = {
        str(key).upper(): float(value) for key, value in (tolerance_overrides or {}).items()
    }
    abs_mode = {str(key).upper() for key in (absolute_mode_overrides or set())}
    use_relative = default_relative_mode and col_key not in abs_mode
    if use_relative:
        denom = abs(float(left_value))
        mode_delta = delta_abs / denom if denom != 0.0 else delta_abs
    else:
        mode_delta = delta_abs
    tol = float(tol_map.get(col_key, default_tol))
    ratio = (
        0.0
        if tol == 0.0 and mode_delta == 0.0
        else (float("inf") if tol == 0.0 else mode_delta / tol)
    )
    return {
        "target": str(column),
        "period": str(period_label),
        "delta_abs": float(delta_abs),
        "delta_mode": float(mode_delta),
        "tol": float(tol),
        "ratio_to_tol": float(ratio),
        "value_before": float(left_value),
        "value_after": float(right_value),
    }


def _sort_focus_events(events: list[dict[str, object]]) -> list[dict[str, object]]:
    return sorted(
        events,
        key=lambda event: (
            int(event.get("iter", 0)),
            str(event.get("period", "")),
            str(event.get("target", "")),
            _TRACE_FOCUS_STAGE_ORDER.get(
                str(event.get("stage", "")), len(_TRACE_FOCUS_STAGE_ORDER)
            ),
            int(event.get("seq", 0)),
        ),
    )


def _compute_fixed_point_residual(
    records: list,
    frame: pd.DataFrame,
    specs: dict[str, object],
    *,
    on_error: str,
    strict_missing_inputs: bool,
    strict_missing_assignments: bool,
    dampall: float,
    damp_overrides: dict[str, float] | None,
    targets: tuple[str, ...],
    windows: tuple[tuple[str, str], ...],
    default_tol: float,
    default_relative_mode: bool,
    tolerance_overrides: dict[str, float] | None = None,
    absolute_mode_overrides: set[str] | None = None,
    period_sequential: bool = False,
    period_sequential_fp_pass_order: bool = True,
    period_sequential_all_assignments: bool = False,
    period_sequential_defer_create: bool = False,
    period_sequential_context_replay_command_time_smpl: bool = False,
    period_sequential_context_retain_overlap_only: bool = False,
    period_sequential_assignment_targets: frozenset[str] = frozenset(),
    windows_override: tuple[SampleWindow, ...] | None = None,
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
    rho_resid_la257_fortran_cycle_carry_style: str = "legacy",
    rho_resid_la257_u_phase_max_gain: float | None = None,
    rho_resid_la257_u_phase_max_gain_mode: str = "relative",
    rho_resid_la257_staged_lifecycle: bool = False,
    rho_resid_commit_after_check: bool = False,
    rho_resid_la257_u_phase_lines: tuple[_La257Selector, ...] = tuple(),
    rho_resid_trace_lines: tuple[_La257Selector, ...] = tuple(),
    rho_resid_trace_event_limit: int = 200,
    rho_resid_fpe_trap: bool = False,
    eval_context: object | None = None,
) -> tuple[float, float, int, int, bool, bool, bool]:
    backfill_kwargs: dict[str, object] = {"on_error": on_error}
    if not strict_missing_inputs:
        backfill_kwargs["strict_missing_inputs"] = False
    if not strict_missing_assignments:
        backfill_kwargs["strict_missing_assignments"] = False
    probe_result = apply_eq_backfill(
        records,
        frame.copy(deep=True),
        specs,
        period_sequential=period_sequential,
        period_sequential_fp_pass_order=period_sequential_fp_pass_order,
        period_sequential_all_assignments=period_sequential_all_assignments,
        period_sequential_defer_create=period_sequential_defer_create,
        period_sequential_context_replay_command_time_smpl=(
            period_sequential_context_replay_command_time_smpl
        ),
        period_sequential_context_retain_overlap_only=(
            period_sequential_context_retain_overlap_only
        ),
        period_sequential_assignment_targets=period_sequential_assignment_targets,
        windows_override=windows_override,
        rho_aware=rho_aware,
        rho_resid_ar1=rho_resid_ar1,
        rho_resid_iteration_seed=rho_resid_iteration_seed,
        rho_resid_iteration_seed_lag=rho_resid_iteration_seed_lag,
        rho_resid_iteration_seed_mode=rho_resid_iteration_seed_mode,
        rho_resid_boundary_reset=rho_resid_boundary_reset,
        rho_resid_carry_lag=rho_resid_carry_lag,
        rho_resid_carry_damp=rho_resid_carry_damp,
        rho_resid_carry_damp_mode=rho_resid_carry_damp_mode,
        rho_resid_carry_multipass=rho_resid_carry_multipass,
        rho_resid_lag_gating=rho_resid_lag_gating,
        rho_resid_update_source=rho_resid_update_source,
        rho_resid_la257_update_rule=rho_resid_la257_update_rule,
        rho_resid_la257_fortran_cycle_carry_style=rho_resid_la257_fortran_cycle_carry_style,
        rho_resid_la257_u_phase_max_gain=rho_resid_la257_u_phase_max_gain,
        rho_resid_la257_u_phase_max_gain_mode=rho_resid_la257_u_phase_max_gain_mode,
        rho_resid_la257_staged_lifecycle=rho_resid_la257_staged_lifecycle,
        rho_resid_commit_after_check=rho_resid_commit_after_check,
        rho_resid_la257_u_phase_lines=rho_resid_la257_u_phase_lines,
        rho_resid_trace_lines=rho_resid_trace_lines,
        rho_resid_trace_event_limit=rho_resid_trace_event_limit,
        rho_resid_fpe_trap=rho_resid_fpe_trap,
        eval_context=eval_context,
        **backfill_kwargs,
    )
    probe_frame = _coerce_backfill_frame(probe_result, frame)
    probe_frame = _apply_damping(
        frame,
        probe_frame,
        dampall,
        overrides=damp_overrides,
    )
    probe_failed = _coerce_backfill_int(probe_result, "failed", "failed_steps")
    probe_issue_count = len(
        _coerce_backfill_issues(
            probe_result.issues
            if hasattr(probe_result, "issues")
            else probe_result.get("issues")
            if isinstance(probe_result, dict)
            else []
        )
    )
    residual_abs_delta, residual_mode_delta, residual_ratio = _compute_convergence_metrics(
        frame,
        probe_frame,
        targets=targets,
        windows=windows,
        default_tol=default_tol,
        default_relative_mode=default_relative_mode,
        tolerance_overrides=tolerance_overrides,
        absolute_mode_overrides=absolute_mode_overrides,
    )
    residual_has_nonfinite_delta = not (
        math.isfinite(residual_abs_delta) and math.isfinite(residual_mode_delta)
    )
    residual_has_nonfinite_ratio = not math.isfinite(residual_ratio)
    residual_has_nonfinite = residual_has_nonfinite_delta or residual_has_nonfinite_ratio
    return (
        residual_abs_delta,
        residual_ratio,
        int(probe_failed),
        int(probe_issue_count),
        residual_has_nonfinite_delta,
        residual_has_nonfinite_ratio,
        residual_has_nonfinite,
    )


def _tokenize_override_line(raw_line: str) -> list[str]:
    text = str(raw_line).strip()
    if not text:
        return []
    if text.startswith("#") or text.startswith("!"):
        return []
    return [token for token in re.split(r"[\s,]+", text) if token]


def _resolve_setupsolve_override_path(
    raw_path: str | None, *, base_dir: Path
) -> Path | str | None:
    if raw_path is None:
        return None
    cleaned = str(raw_path).strip().strip("'\"")
    if not cleaned:
        return None
    if cleaned.upper() == "KEYBOARD":
        return "KEYBOARD"
    path = Path(cleaned)
    if not path.is_absolute():
        path = base_dir / path
    return path


def _parse_setupsolve_name_value_file(
    path: Path,
    *,
    option_name: str,
) -> tuple[dict[str, float], list[dict[str, object]]]:
    entries: dict[str, float] = {}
    issues: list[dict[str, object]] = []
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError as exc:
        issues.append({
            "line": None,
            "statement": f"SETUPSOLVE {option_name}",
            "error": f"failed to read override file {path}: {exc}",
        })
        return entries, issues

    for line_number, raw_line in enumerate(lines, start=1):
        tokens = _tokenize_override_line(raw_line)
        if not tokens:
            continue
        head = tokens[0].strip()
        if head == ";" or head.startswith(";"):
            break
        if len(tokens) < 2:
            issues.append({
                "line": None,
                "statement": f"SETUPSOLVE {option_name}",
                "error": (f"{path}:{line_number} expected '<variable> <value>' or ';' sentinel"),
            })
            continue
        try:
            value = float(tokens[1])
        except ValueError:
            issues.append({
                "line": None,
                "statement": f"SETUPSOLVE {option_name}",
                "error": (f"{path}:{line_number} invalid numeric override value: {tokens[1]!r}"),
            })
            continue
        entries[head.upper()] = value

    return entries, issues


def _parse_setupsolve_name_flag_file(
    path: Path,
    *,
    option_name: str,
) -> tuple[set[str], list[dict[str, object]]]:
    flags: set[str] = set()
    issues: list[dict[str, object]] = []
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError as exc:
        issues.append({
            "line": None,
            "statement": f"SETUPSOLVE {option_name}",
            "error": f"failed to read override file {path}: {exc}",
        })
        return flags, issues

    for line_number, raw_line in enumerate(lines, start=1):
        tokens = _tokenize_override_line(raw_line)
        if not tokens:
            continue
        head = tokens[0].strip()
        if head == ";" or head.startswith(";"):
            break
        if len(head) == 0:
            issues.append({
                "line": None,
                "statement": f"SETUPSOLVE {option_name}",
                "error": f"{path}:{line_number} expected variable name or ';' sentinel",
            })
            continue
        flags.add(head.upper())

    return flags, issues


def _load_setupsolve_override_files(
    *,
    base_dir: Path,
    filedamp: str | None,
    filetol: str | None,
    filetolabs: str | None,
) -> tuple[dict[str, float], dict[str, float], set[str], list[dict[str, object]]]:
    damp_overrides: dict[str, float] = {}
    tol_overrides: dict[str, float] = {}
    tolabs_overrides: set[str] = set()
    issues: list[dict[str, object]] = []

    damp_path = _resolve_setupsolve_override_path(filedamp, base_dir=base_dir)
    tol_path = _resolve_setupsolve_override_path(filetol, base_dir=base_dir)
    abs_path = _resolve_setupsolve_override_path(filetolabs, base_dir=base_dir)

    for option_name, resolved in (
        ("FILEDAMP", damp_path),
        ("FILETOL", tol_path),
        ("FILETOLABS", abs_path),
    ):
        if resolved != "KEYBOARD":
            continue
        issues.append({
            "line": None,
            "statement": f"SETUPSOLVE {option_name}",
            "error": (f"{option_name}=KEYBOARD is not supported in non-interactive fair-py runs"),
        })

    if isinstance(damp_path, Path):
        parsed, parse_issues = _parse_setupsolve_name_value_file(
            damp_path,
            option_name="FILEDAMP",
        )
        damp_overrides.update(parsed)
        issues.extend(parse_issues)
    if isinstance(tol_path, Path):
        parsed, parse_issues = _parse_setupsolve_name_value_file(
            tol_path,
            option_name="FILETOL",
        )
        tol_overrides.update(parsed)
        issues.extend(parse_issues)
    if isinstance(abs_path, Path):
        parsed, parse_issues = _parse_setupsolve_name_flag_file(
            abs_path,
            option_name="FILETOLABS",
        )
        tolabs_overrides.update(parsed)
        issues.extend(parse_issues)

    return damp_overrides, tol_overrides, tolabs_overrides, issues


def _apply_damping(
    before: pd.DataFrame,
    after: pd.DataFrame,
    dampall: float,
    *,
    overrides: dict[str, float] | None = None,
) -> pd.DataFrame:
    override_map = {str(key).upper(): float(value) for key, value in (overrides or {}).items()}
    if dampall == 1.0 and not override_map:
        return after
    if dampall == 0.0 and not override_map:
        return before.copy(deep=True)
    if before.empty or after.empty:
        return after

    result = after.copy(deep=True)
    shared = [column for column in before.columns if column in after.columns]
    for column in shared:
        effective_damp = float(override_map.get(str(column).upper(), dampall))
        if effective_damp == 1.0:
            continue
        left = pd.to_numeric(before[column], errors="coerce")
        right = pd.to_numeric(after[column], errors="coerce")
        mask = left.notna() & right.notna()
        if not mask.any():
            continue
        blended = right.copy()
        blended.loc[mask] = left.loc[mask] + effective_damp * (right.loc[mask] - left.loc[mask])
        result[column] = blended
    return result


_EQ_REFERENCE_RE = re.compile(
    r"^\s*EQ\s+(?P<number>\d+)(?:\s+(?P<token>[A-Za-z_][A-Za-z0-9_]*))?",
    re.IGNORECASE,
)
_PERIOD_TOKEN_RE = re.compile(r"^\s*(?P<year>\d+)(?:\.(?P<subperiod>\d+))?\s*$")
_LAG_TOKEN_RE = re.compile(
    r"\b[A-Za-z_][A-Za-z0-9_]*\s*\(\s*(?P<lag>[+-]?\d+)\s*\)",
    re.IGNORECASE,
)
_REFERENCE_TOKEN_RE = re.compile(
    r"\b(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*(?:\(\s*(?P<lag>[+-]?\d+)\s*\))?",
    re.IGNORECASE,
)
_MODEQ_HEADER_RE = re.compile(
    r"^\s*MODEQ\s+(?P<number>\d+)\s*(?P<body>.*)$",
    re.IGNORECASE | re.DOTALL,
)
_FSR_PREFIX_RE = re.compile(r"^\s*FSR\s*(?P<body>.*)$", re.IGNORECASE | re.DOTALL)
_RESERVED_REFERENCE_NAMES = {
    "LOG",
    "EXP",
    "ABS",
    "COEF",
    "FSR",
    "MODEQ",
    "EQ",
    "GENR",
    "IDENT",
    "LHS",
    "CREATE",
    "SMPL",
    "LOADDATA",
    "RHO",
    "C",
    "T",
}
_NOMISS_BOUNDARY_PROPAGATION_CAP = 16


def _resolve_column_name(frame: pd.DataFrame, name: str) -> str | None:
    if name in frame.columns:
        return name
    target = str(name).upper()
    for column in frame.columns:
        if str(column).upper() == target:
            return str(column)
    return None


def _build_eq_spec_number_map(specs: dict[str, object]) -> dict[int, str]:
    specs_by_number: dict[int, str] = {}
    for lhs, spec in specs.items():
        equation_number = getattr(spec, "equation_number", None)
        if isinstance(equation_number, int):
            specs_by_number.setdefault(equation_number, str(lhs))
    return specs_by_number


def _resolve_eq_target(
    record,
    *,
    specs: dict[str, object],
    specs_by_number: dict[int, str],
) -> str | None:
    match = _EQ_REFERENCE_RE.match(record.statement)
    if match is None:
        return None
    token_raw = match.group("token")
    token = token_raw.upper() if token_raw else None
    if token and token.startswith("NONE"):
        return None
    if token == "FSR":
        return None

    number = int(match.group("number"))
    if token and token in specs:
        return str(token)
    if number in specs_by_number:
        return specs_by_number[number]
    if token:
        return str(token)
    return None


def _collect_eq_backfill_targets(records: list, specs: dict[str, object]) -> tuple[str, ...]:
    targets: list[str] = []
    seen_targets: set[str] = set()
    specs_by_number = _build_eq_spec_number_map(specs)
    eq_fsr_lines = collect_eq_fsr_command_lines(records)

    def add_target(value: str) -> None:
        normalized = str(value).upper()
        if normalized in seen_targets:
            return
        seen_targets.add(normalized)
        targets.append(str(value))

    for record in records:
        if record.command == FPCommand.EQ:
            if record.line_number in eq_fsr_lines:
                continue
            eq_target = _resolve_eq_target(
                record,
                specs=specs,
                specs_by_number=specs_by_number,
            )
            if eq_target is not None:
                add_target(eq_target)
            continue

        if record.command == FPCommand.LHS:
            try:
                add_target(parse_assignment(record.statement).lhs)
            except Exception:
                continue

    return tuple(targets)


def _collect_outside_seed_targets(records: list, specs: dict[str, object]) -> tuple[str, ...]:
    """Collect OUTSIDE seeding targets in stable record order.

    Legacy OUTSIDE initialization applies to endogenous/current values, not
    only EQ/LHS targets. Extend the seeding scope with assignment LHS targets
    while preserving first-seen order.
    """

    targets: list[str] = list(_collect_eq_backfill_targets(records, specs))
    seen_targets: set[str] = {str(item).upper() for item in targets}

    for record in records:
        if record.command not in {
            FPCommand.GENR,
            FPCommand.IDENT,
            FPCommand.CREATE,
            FPCommand.LHS,
        }:
            continue
        try:
            lhs = str(parse_assignment(record.statement).lhs)
        except Exception:
            continue
        normalized = lhs.upper()
        if normalized in seen_targets:
            continue
        seen_targets.add(normalized)
        targets.append(lhs)

    return tuple(targets)


def _collect_structural_assignment_targets(
    specs: dict[str, object],
    *,
    extra_targets: tuple[str, ...] = tuple(),
) -> frozenset[str]:
    """Collect assignment targets needed for selected structural dependency paths."""

    if not extra_targets:
        return frozenset()

    specs_by_lhs = {str(lhs).upper(): spec for lhs, spec in specs.items() if str(lhs).strip()}
    retained: set[str] = set()
    pending: list[str] = []
    for target in extra_targets:
        if not str(target).strip():
            continue
        for expanded in _expand_dependency_names(str(target)):
            pending.append(str(expanded).upper())

    while pending:
        current = pending.pop()
        if current in retained:
            continue
        if current in _RESERVED_REFERENCE_NAMES:
            continue
        retained.add(current)

        for candidate_lhs in _expand_dependency_names(current):
            spec = specs_by_lhs.get(str(candidate_lhs).upper())
            if spec is None:
                continue
            terms = getattr(spec, "terms", tuple())
            if not isinstance(terms, (tuple, list)):
                continue
            for term in terms:
                variable = getattr(term, "variable", None)
                if not isinstance(variable, str) or not variable.strip():
                    continue
                if variable.upper() in _RESERVED_REFERENCE_NAMES:
                    continue
                for expanded in _expand_dependency_names(variable):
                    normalized = str(expanded).upper()
                    if normalized not in retained:
                        pending.append(normalized)

    return frozenset(retained)


def _collect_eq_backfill_windows(records: list) -> tuple[tuple[str, str], ...]:
    windows: set[tuple[str, str]] = set()
    active_window = None
    eq_fsr_lines = collect_eq_fsr_command_lines(records)
    for record in records:
        if record.command == FPCommand.SMPL:
            try:
                active_window = parse_smpl_statement(record.statement)
            except Exception:
                active_window = None
            continue
        if (
            record.command in {FPCommand.EQ, FPCommand.LHS}
            and active_window is not None
            and not (record.command == FPCommand.EQ and record.line_number in eq_fsr_lines)
        ):
            windows.add((active_window.start, active_window.end))
    return tuple(sorted(windows))


def _collect_setupsolve_windows(records: list) -> tuple[tuple[str, str], ...]:
    windows: set[tuple[str, str]] = set()
    active_window = None
    for record in records:
        if record.command == FPCommand.SMPL:
            try:
                active_window = parse_smpl_statement(record.statement)
            except Exception:
                active_window = None
            continue
        if record.command != FPCommand.SETUPSOLVE:
            continue
        if active_window is None:
            continue
        windows.add((active_window.start, active_window.end))
    return tuple(sorted(windows))


def _eq_specs_have_rho_terms(specs: dict[str, object] | None) -> bool:
    if not specs:
        return False
    for spec in specs.values():
        terms = getattr(spec, "terms", None)
        if not isinstance(terms, (tuple, list)):
            continue
        for term in terms:
            variable = getattr(term, "variable", None)
            if isinstance(variable, str) and variable.upper() == "RHO":
                return True
    return False


def _collect_solve_line_numbers(records: list) -> tuple[int, ...]:
    return tuple(
        int(record.line_number) for record in records if record.command == FPCommand.SOLVE
    )


def _collect_solve_runtime_directives(records: list) -> tuple[dict[str, object], ...]:
    active_window: SampleWindow | None = None
    directives: list[dict[str, object]] = []
    for record in records:
        if record.command == FPCommand.SMPL:
            try:
                active_window = parse_smpl_statement(record.statement)
            except Exception:
                active_window = None
            continue
        if record.command != FPCommand.SOLVE:
            continue

        parsed = parse_runtime_command(record)
        if not isinstance(parsed, SolveCommand):
            continue
        directives.append({
            "line_number": int(record.line_number),
            "window": active_window,
            "outside": bool(parsed.outside),
            "noreset": bool(parsed.noreset),
            "filevar": parsed.filevar,
            "keyboard_targets": tuple(str(name) for name in parsed.keyboard_targets),
        })
    return tuple(directives)


def _resolve_active_solve_runtime_directive(
    *,
    directives: tuple[dict[str, object], ...],
    max_steps: int | None,
    executed_line_numbers: list[int],
) -> dict[str, object] | None:
    if not directives:
        return None
    if max_steps is None:
        return dict(directives[-1])
    if not executed_line_numbers:
        return dict(directives[-1])
    cutoff_line = max(int(line) for line in executed_line_numbers)
    reached = [
        directive
        for directive in directives
        if int(directive.get("line_number", 0)) <= cutoff_line
    ]
    if not reached:
        return None
    return dict(reached[-1])


def _is_outside_missing_value(value: object) -> bool:
    # Post-parse missing conventions for FM numeric inputs:
    # - parser preserves explicit numeric sentinels (for example -99.0)
    # - pandas coercion represents non-numeric/unset cells as NaN
    # Treat only these conventions as "missing-like" for OUTSIDE seeding.
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return True
    if pd.isna(numeric):
        return True
    return numeric in _OUTSIDE_MISSING_SENTINELS


def _resolve_auto_keyboard_solve_stub_flags(
    *,
    auto_enable_eq_from_solve: bool,
    solve_active_outside: bool,
    solve_active_filevar: str | None,
    solve_active_keyboard_targets_set: frozenset[str],
    eq_use_setupsolve: bool,
    eq_period_sequential: bool,
    eq_period_scoped: str,
) -> tuple[bool, bool, str, bool, bool]:
    """Resolve auto-stub flags for SOLVE OUTSIDE FILEVAR=KEYBOARD runs.

    Returns:
      (auto_stub_active, period_sequential_effective, period_scoped_effective,
       period_sequential_auto_enabled, period_scoped_auto_enabled)
    """

    auto_stub_active = bool(
        auto_enable_eq_from_solve
        and solve_active_outside
        and isinstance(solve_active_filevar, str)
        and solve_active_filevar.upper() == "KEYBOARD"
        and bool(solve_active_keyboard_targets_set)
        and not bool(eq_use_setupsolve)
    )
    period_sequential_effective = bool(eq_period_sequential)
    period_sequential_auto_enabled = False
    period_scoped_effective = str(eq_period_scoped).strip().lower()
    period_scoped_auto_enabled = False

    if auto_stub_active:
        if not period_sequential_effective:
            period_sequential_effective = True
            period_sequential_auto_enabled = True
        if period_scoped_effective == "auto":
            period_scoped_effective = "on"
            period_scoped_auto_enabled = True

    return (
        auto_stub_active,
        period_sequential_effective,
        period_scoped_effective,
        period_sequential_auto_enabled,
        period_scoped_auto_enabled,
    )


def _resolve_auto_keyboard_min_iters(
    *,
    auto_stub_active: bool,
    eq_use_setupsolve: bool,
    eq_iters: int | None,
    eq_backfill_min_iters: int,
    eq_backfill_max_iters: int,
) -> tuple[int, int, bool]:
    """Resolve minimum iteration policy for keyboard auto-stub runs.

    Returns:
      (min_iters_effective, max_iters_effective, auto_enabled)
    """
    min_iters_effective = max(1, int(eq_backfill_min_iters))
    max_iters_effective = max(min_iters_effective, int(eq_backfill_max_iters))
    auto_enabled = False
    if (
        auto_stub_active
        and not bool(eq_use_setupsolve)
        and eq_iters is None
        and max_iters_effective <= 1
    ):
        min_iters_effective = max(min_iters_effective, 2)
        max_iters_effective = max(max_iters_effective, min_iters_effective)
        auto_enabled = True
    return min_iters_effective, max_iters_effective, auto_enabled


def _resolve_auto_keyboard_all_assignments_flag(
    *,
    auto_stub_active: bool,
    eq_period_sequential_all_assignments: bool,
) -> tuple[bool, bool]:
    """Resolve period-sequential all-assignments behavior for keyboard stubs.

    Returns:
      (all_assignments_effective, auto_enabled)
    """
    effective = bool(eq_period_sequential_all_assignments)
    auto_enabled = False
    if auto_stub_active and not effective:
        effective = True
        auto_enabled = True
    return effective, auto_enabled


def _resolve_auto_keyboard_context_first_iter_flag(
    *,
    auto_stub_active: bool,
    eq_period_sequential_all_assignments: bool,
    eq_use_setupsolve: bool,
    eq_period_sequential_context_assignments_first_iter_only: bool,
) -> tuple[bool, bool]:
    """Resolve context-assignment replay policy for keyboard auto-stub runs.

    Returns:
      (context_first_iter_effective, auto_enabled)
    """
    effective = bool(eq_period_sequential_context_assignments_first_iter_only)
    auto_enabled = False
    if (
        auto_stub_active
        and bool(eq_period_sequential_all_assignments)
        and not bool(eq_use_setupsolve)
        and not effective
    ):
        effective = True
        auto_enabled = True
    return effective, auto_enabled


def _resolve_auto_keyboard_rho_aware_flag(
    *,
    auto_stub_active: bool,
    auto_stub_min_iters_enabled: bool,
    eq_use_setupsolve: bool,
    eq_backfill_specs_has_rho_terms: bool,
    eq_backfill_rho_aware: bool,
    eq_backfill_rho_resid_ar1: bool,
) -> tuple[bool, bool, str]:
    """Resolve rho-aware mode when keyboard auto-stub uses bounded iterations.

    Returns:
      (rho_aware_effective, auto_selected, auto_reason)
    """
    effective = bool(eq_backfill_rho_aware)
    auto_selected = False
    auto_reason = "none"
    if (
        bool(eq_backfill_specs_has_rho_terms)
        and not effective
        and not bool(eq_backfill_rho_resid_ar1)
    ):
        if bool(eq_use_setupsolve):
            effective = True
            auto_selected = True
            auto_reason = "setupsolve_rho_terms"
        elif bool(auto_stub_active) and bool(auto_stub_min_iters_enabled):
            effective = True
            auto_selected = True
            auto_reason = "keyboard_stub_min_iters_rho_terms"
        elif bool(auto_stub_active):
            effective = True
            auto_selected = True
            auto_reason = "keyboard_stub_rho_terms"
    return effective, auto_selected, auto_reason


def _normalize_eq_flags_preset_label(eq_flags_preset_label: str | None) -> str:
    text = str(eq_flags_preset_label or "").strip().lower()
    if text in {"", "none", "off", "default"}:
        return "default"
    return text


def _resolve_eq_flags_preset_label(
    *,
    eq_flags_preset_label: str | None,
    enable_eq: bool,
    eq_use_setupsolve: bool,
    eq_period_sequential: bool,
    eq_period_scoped: str,
    eq_period_sequential_context_assignments_first_iter_only: bool,
) -> str:
    normalized = _normalize_eq_flags_preset_label(eq_flags_preset_label)
    if normalized != "default":
        return normalized
    if not bool(enable_eq):
        return "default"
    if not bool(eq_use_setupsolve):
        return "default"
    period_scoped_mode = str(eq_period_scoped).strip().lower()
    if bool(eq_period_sequential) and period_scoped_mode == "on":
        if bool(eq_period_sequential_context_assignments_first_iter_only):
            return "iss02_baseline"
        return "parity"
    return "custom"


def _serialize_setupsolve_for_summary(setupsolve: object | None) -> dict[str, object] | None:
    if setupsolve is None:
        return None
    return {
        "miniters": getattr(setupsolve, "miniters", None),
        "maxiters": getattr(setupsolve, "maxiters", None),
        "maxcheck": getattr(setupsolve, "maxcheck", None),
        "nomiss": bool(getattr(setupsolve, "nomiss", False)),
        "tolall": getattr(setupsolve, "tolall", None),
        "tolallabs": bool(getattr(setupsolve, "tolallabs", False)),
        "dampall": getattr(setupsolve, "dampall", None),
        "filedamp": getattr(setupsolve, "filedamp", None),
        "filetol": getattr(setupsolve, "filetol", None),
        "filetolabs": getattr(setupsolve, "filetolabs", None),
    }


def _apply_outside_seed_frame(
    frame: pd.DataFrame,
    *,
    targets: tuple[str, ...],
    windows: tuple[tuple[str, str], ...],
) -> tuple[pd.DataFrame, int]:
    if frame.empty or not targets:
        return frame, 0

    working = frame.copy(deep=True)
    index_positions = {str(period): idx for idx, period in enumerate(working.index)}
    seeded_cells = 0
    scoped_windows = _resolve_scope_windows(working.index, windows)
    for target in targets:
        column = _resolve_eq_seed_column(working, str(target))
        if column is None or column not in working.columns:
            continue
        series = pd.to_numeric(working[column], errors="coerce").copy()
        for scoped_window in scoped_windows:
            for period in scoped_window:
                period_key = str(period)
                position = index_positions.get(period_key)
                if position is None or position <= 0:
                    continue
                current_value = float(series.iat[position])
                previous_value = float(series.iat[position - 1])
                current_missing = _is_outside_missing_value(current_value)
                if not current_missing:
                    continue
                if _is_outside_missing_value(previous_value):
                    continue
                series.iat[position] = previous_value
                seeded_cells += 1
        working[column] = series
    return working, seeded_cells


def _collect_outside_seed_stats(
    frame: pd.DataFrame,
    *,
    targets: tuple[str, ...],
    windows: tuple[tuple[str, str], ...],
) -> dict[str, int]:
    if frame.empty or not targets:
        return {
            "resolved_target_count": 0,
            "inspected_cells": 0,
            "missing_like_cells": 0,
            "prior_missing_like_cells": 0,
            "candidate_cells": 0,
        }

    index_positions = {str(period): idx for idx, period in enumerate(frame.index)}
    resolved_target_count = 0
    inspected_cells = 0
    missing_like_cells = 0
    prior_missing_like_cells = 0
    candidate_cells = 0
    scoped_windows = _resolve_scope_windows(frame.index, windows)

    for target in targets:
        column = _resolve_eq_seed_column(frame, str(target))
        if column is None or column not in frame.columns:
            continue
        resolved_target_count += 1
        series = pd.to_numeric(frame[column], errors="coerce")
        for scoped_window in scoped_windows:
            for period in scoped_window:
                period_key = str(period)
                position = index_positions.get(period_key)
                if position is None or position <= 0:
                    continue
                inspected_cells += 1
                current_value = float(series.iat[position])
                previous_value = float(series.iat[position - 1])
                current_missing = _is_outside_missing_value(current_value)
                previous_missing = _is_outside_missing_value(previous_value)
                if current_missing:
                    missing_like_cells += 1
                if previous_missing:
                    prior_missing_like_cells += 1
                if current_missing and not previous_missing:
                    candidate_cells += 1

    return {
        "resolved_target_count": int(resolved_target_count),
        "inspected_cells": int(inspected_cells),
        "missing_like_cells": int(missing_like_cells),
        "prior_missing_like_cells": int(prior_missing_like_cells),
        "candidate_cells": int(candidate_cells),
    }


def _solve_reached_for_mini_run(
    *,
    solve_lines: tuple[int, ...],
    max_steps: int | None,
    executed_line_numbers: list[int],
) -> bool:
    if not solve_lines:
        return False
    if max_steps is None:
        return True
    if not executed_line_numbers:
        # When no assignment/identity steps were executed (for example,
        # command filters that retain workflow controls only), treat SOLVE
        # as reachable in the selected runtime slice.
        return True
    cutoff_line = max(int(line) for line in executed_line_numbers)
    return any(int(line) <= cutoff_line for line in solve_lines)


def _collect_eq_backfill_windows_by_target(
    records: list,
    specs: dict[str, object],
) -> dict[str, tuple[tuple[str, str], ...]]:
    specs_by_number = _build_eq_spec_number_map(specs)
    eq_fsr_lines = collect_eq_fsr_command_lines(records)
    active_window = None
    windows_by_target: dict[str, set[tuple[str, str]]] = {}

    for record in records:
        if record.command == FPCommand.SMPL:
            try:
                active_window = parse_smpl_statement(record.statement)
            except Exception:
                active_window = None
            continue

        if active_window is None:
            continue

        target: str | None = None
        if record.command == FPCommand.EQ:
            if record.line_number in eq_fsr_lines:
                continue
            target = _resolve_eq_target(
                record,
                specs=specs,
                specs_by_number=specs_by_number,
            )
        elif record.command == FPCommand.LHS:
            try:
                target = parse_assignment(record.statement).lhs
            except Exception:
                target = None

        if target is None:
            continue

        target_key = str(target).upper()
        windows_by_target.setdefault(target_key, set()).add((
            active_window.start,
            active_window.end,
        ))

    return {target: tuple(sorted(window_set)) for target, window_set in windows_by_target.items()}


def _extract_reference_shifts(expression: str) -> list[tuple[str, int, int]]:
    shifts: list[tuple[str, int, int]] = []
    seen: set[tuple[str, int, int]] = set()
    for match in _REFERENCE_TOKEN_RE.finditer(expression):
        dep_key = str(match.group("name")).upper()
        if dep_key in _RESERVED_REFERENCE_NAMES:
            continue
        lag = int(match.group("lag") or 0)
        shift = (dep_key, max(-lag, 0), max(lag, 0))
        if shift in seen:
            continue
        seen.add(shift)
        shifts.append(shift)
    return shifts


def _period_key(value: object) -> tuple[int, int] | None:
    match = _PERIOD_TOKEN_RE.match(str(value).strip())
    if match is None:
        return None
    return (int(match.group("year")), int(match.group("subperiod") or 0))


def _apply_eq_store_quantization(
    frame: pd.DataFrame,
    *,
    targets: Sequence[str],
    windows: tuple[tuple[str, str], ...],
    mode: str,
) -> tuple[pd.DataFrame, int]:
    """Quantize selected EQ target cells in-place for next-iteration storage."""
    quantize_mode = str(mode).strip().lower()
    if quantize_mode == "off":
        return frame, 0
    if quantize_mode != "float32":
        raise ValueError("eq_quantize_store must be 'off' or 'float32'")
    if frame.empty or not targets or not windows:
        return frame, 0

    window_bounds: list[tuple[tuple[int, int], tuple[int, int]]] = []
    for start, end in windows:
        start_key = _period_key(start)
        end_key = _period_key(end)
        if start_key is None or end_key is None or start_key > end_key:
            continue
        window_bounds.append((start_key, end_key))
    if not window_bounds:
        return frame, 0

    index_keys = [_period_key(period) for period in frame.index]
    row_mask = np.array(
        [
            key is not None and any(start <= key <= end for start, end in window_bounds)
            for key in index_keys
        ],
        dtype=bool,
    )
    if not bool(row_mask.any()):
        return frame, 0

    unique_targets = tuple(
        dict.fromkeys(str(target).upper() for target in targets if str(target).strip())
    )
    quantized_cells = 0
    for target in unique_targets:
        if target not in frame.columns:
            continue
        column_values = pd.to_numeric(frame[target], errors="coerce").to_numpy(
            dtype=np.float64, copy=True
        )
        selected_values = column_values[row_mask]
        quantized_values = selected_values.astype(np.float32).astype(np.float64)
        if quantized_values.size == 0:
            continue
        column_values[row_mask] = quantized_values
        frame[target] = column_values
        quantized_cells += int(quantized_values.size)
    return frame, quantized_cells


def _build_scope_mask(
    index: pd.Index,
    windows: tuple[tuple[str, str], ...],
) -> pd.Series:
    if not windows:
        return pd.Series(True, index=index)

    keys = [_period_key(item) for item in index]
    mask = pd.Series(False, index=index)
    for start, end in windows:
        start_key = _period_key(start)
        end_key = _period_key(end)
        if start_key is None or end_key is None or start_key > end_key:
            continue
        window_mask = [key is not None and start_key <= key <= end_key for key in keys]
        mask = mask | pd.Series(window_mask, index=index)

    if not mask.any():
        return pd.Series(True, index=index)
    return mask


def _resolve_scope_windows(
    index: pd.Index,
    windows: tuple[tuple[str, str], ...],
) -> list[pd.Index]:
    if index.empty:
        return []
    if not windows:
        return [index]

    keys = [_period_key(item) for item in index]
    resolved: list[pd.Index] = []
    seen_windows: set[tuple[object, ...]] = set()
    for start, end in windows:
        start_key = _period_key(start)
        end_key = _period_key(end)
        if start_key is None or end_key is None or start_key > end_key:
            continue
        window_mask = [key is not None and start_key <= key <= end_key for key in keys]
        scoped_window = index[window_mask]
        if len(scoped_window) == 0:
            continue
        signature = tuple(scoped_window.tolist())
        if signature in seen_windows:
            continue
        seen_windows.add(signature)
        resolved.append(scoped_window)

    if not resolved:
        return [index]
    return resolved


def _count_missing_target_cells(
    frame: pd.DataFrame,
    targets: tuple[str, ...],
    *,
    windows: tuple[tuple[str, str], ...] = tuple(),
    windows_by_target: dict[str, tuple[tuple[str, str], ...]] | None = None,
    exclude_index_by_target: dict[str, set[object]] | None = None,
) -> int:
    if frame.empty or not targets:
        return 0

    missing = 0
    for target in targets:
        target_key = str(target).upper()
        target_windows = (windows_by_target or {}).get(target_key, windows)
        scope_mask = _build_scope_mask(frame.index, target_windows)
        scoped = frame.loc[scope_mask]
        scoped_index_set = set(scoped.index)
        exclude = {
            item
            for item in (exclude_index_by_target or {}).get(target_key, set())
            if item in scoped_index_set
        }
        resolved = _resolve_column_name(scoped, target)
        if resolved is None:
            missing += max(0, len(scoped.index) - len(exclude))
            continue
        series = scoped[resolved]
        if not exclude:
            missing += int(series.isna().sum())
            continue
        valid_mask = ~series.index.isin(exclude)
        missing += int(series.loc[valid_mask].isna().sum())
    return missing


_NOMISS_TARGET_DIAGNOSTICS_LIMIT = 5


def _count_missing_target_cells_by_target(
    frame: pd.DataFrame,
    targets: tuple[str, ...],
    *,
    windows: tuple[tuple[str, str], ...] = tuple(),
    windows_by_target: dict[str, tuple[tuple[str, str], ...]] | None = None,
    exclude_index_by_target: dict[str, set[object]] | None = None,
) -> list[dict[str, int | str]]:
    if frame.empty or not targets:
        return []

    diagnostic_pairs: list[tuple[str, int]] = []
    for target in targets:
        target_key = str(target).upper()
        target_windows = (windows_by_target or {}).get(target_key, windows)
        scope_mask = _build_scope_mask(frame.index, target_windows)
        scoped = frame.loc[scope_mask]
        scoped_index_set = set(scoped.index)
        exclude = {
            item
            for item in (exclude_index_by_target or {}).get(target_key, set())
            if item in scoped_index_set
        }
        resolved = _resolve_column_name(scoped, target)
        if resolved is None:
            missing = max(0, len(scoped.index) - len(exclude))
        else:
            series = scoped[resolved]
            if not exclude:
                missing = int(series.isna().sum())
            else:
                valid_mask = ~series.index.isin(exclude)
                missing = int(series.loc[valid_mask].isna().sum())
        if missing <= 0:
            continue
        diagnostic_pairs.append((str(target), missing))

    diagnostic_pairs.sort(key=lambda item: (-item[1], item[0]))
    diagnostic_pairs = diagnostic_pairs[:_NOMISS_TARGET_DIAGNOSTICS_LIMIT]
    return [{"target": target, "missing_cells": missing} for target, missing in diagnostic_pairs]


def _build_nomiss_boundary_exclusions(
    *,
    frame: pd.DataFrame,
    targets: tuple[str, ...],
    specs: dict[str, object],
    records: list | None = None,
    windows: tuple[tuple[str, str], ...] = tuple(),
    windows_by_target: dict[str, tuple[tuple[str, str], ...]] | None = None,
) -> dict[str, set[object]]:
    if frame.empty or not targets or not specs:
        return {}

    exclusions: dict[str, set[object]] = {}
    lag_bounds: dict[str, tuple[int, int]] = {}
    upper_specs = {str(name).upper(): spec for name, spec in specs.items()}
    specs_by_number = _build_eq_spec_number_map(specs)
    support_keys: set[str] = {str(target).upper() for target in targets}
    support_keys.update(upper_specs.keys())
    for record in records or []:
        if record.command in {FPCommand.LHS, FPCommand.GENR, FPCommand.IDENT, FPCommand.CREATE}:
            try:
                assignment = parse_assignment(record.statement)
            except Exception:
                continue
            support_keys.add(str(assignment.lhs).upper())
            continue
        if record.command != FPCommand.MODEQ:
            continue
        modeq_match = _MODEQ_HEADER_RE.match(record.statement)
        if modeq_match is None:
            continue
        equation_number = int(modeq_match.group("number"))
        modeq_target = str(specs_by_number.get(equation_number, "")).upper()
        if modeq_target:
            support_keys.add(modeq_target)

    for target_key in support_keys:
        spec = upper_specs.get(target_key)
        start_exclude = 0
        end_exclude = 0
        if spec is not None:
            terms = getattr(spec, "terms", ())
            start_exclude = max(
                (
                    abs(int(getattr(term, "lag", 0)))
                    for term in terms
                    if int(getattr(term, "lag", 0)) < 0
                ),
                default=0,
            )
            end_exclude = max(
                (
                    int(getattr(term, "lag", 0))
                    for term in terms
                    if int(getattr(term, "lag", 0)) > 0
                ),
                default=0,
            )
        lag_bounds[target_key] = (start_exclude, end_exclude)

    dependencies: dict[str, set[tuple[str, int, int]]] = {key: set() for key in lag_bounds}
    for target_key, spec in upper_specs.items():
        if target_key not in lag_bounds:
            continue
        for term in getattr(spec, "terms", ()):
            dep_key = str(getattr(term, "variable", "")).upper()
            if not dep_key or dep_key == target_key or dep_key not in lag_bounds:
                continue
            lag = int(getattr(term, "lag", 0))
            dependencies[target_key].add((dep_key, max(-lag, 0), max(lag, 0)))

    fsr_refs: tuple[tuple[str, int, int], ...] = tuple()
    pending_modeq_target: str | None = None

    def _apply_target_refs(
        target_key: str, refs: list[tuple[str, int, int]] | tuple[tuple[str, int, int], ...]
    ) -> None:
        lhs_start, lhs_end = lag_bounds.get(target_key, (0, 0))
        for dep_key, start_shift, end_shift in refs:
            lhs_start = max(lhs_start, start_shift)
            lhs_end = max(lhs_end, end_shift)
            if dep_key == target_key or dep_key not in lag_bounds:
                continue
            dependencies[target_key].add((dep_key, start_shift, end_shift))
        lag_bounds[target_key] = (lhs_start, lhs_end)

    for record in records or []:
        if record.command == FPCommand.FSR:
            fsr_match = _FSR_PREFIX_RE.match(record.statement)
            fsr_body = fsr_match.group("body") if fsr_match is not None else record.statement
            fsr_refs = tuple(_extract_reference_shifts(fsr_body))
            if pending_modeq_target is not None and pending_modeq_target in lag_bounds:
                _apply_target_refs(pending_modeq_target, fsr_refs)
                pending_modeq_target = None
            continue

        if record.command in {FPCommand.LHS, FPCommand.GENR, FPCommand.IDENT, FPCommand.CREATE}:
            try:
                assignment = parse_assignment(record.statement)
            except Exception:
                continue
            target_key = str(assignment.lhs).upper()
            refs = _extract_reference_shifts(assignment.rhs)
            pending_modeq_target = None
        elif record.command == FPCommand.MODEQ:
            modeq_match = _MODEQ_HEADER_RE.match(record.statement)
            if modeq_match is None:
                continue
            equation_number = int(modeq_match.group("number"))
            target_key = str(specs_by_number.get(equation_number, "")).upper()
            if not target_key:
                continue
            modeq_body = modeq_match.group("body")
            refs = _extract_reference_shifts(modeq_body)
            if re.search(r"\bFSR\b", modeq_body, re.IGNORECASE):
                refs = [*refs, *fsr_refs]
            pending_modeq_target = None if record.statement.rstrip().endswith(";") else target_key
        else:
            pending_modeq_target = None
            continue

        if target_key not in lag_bounds:
            continue

        _apply_target_refs(target_key, refs)

    for _ in range(len(lag_bounds)):
        changed = False
        for target_key, dep_refs in dependencies.items():
            start_exclude, end_exclude = lag_bounds[target_key]
            new_start = start_exclude
            new_end = end_exclude
            for dep_key, start_shift, end_shift in dep_refs:
                dep_start, dep_end = lag_bounds.get(dep_key, (0, 0))
                new_start = max(
                    new_start,
                    min(_NOMISS_BOUNDARY_PROPAGATION_CAP, dep_start + start_shift),
                )
                new_end = max(
                    new_end,
                    min(_NOMISS_BOUNDARY_PROPAGATION_CAP, dep_end + end_shift),
                )
            if new_start != start_exclude or new_end != end_exclude:
                lag_bounds[target_key] = (new_start, new_end)
                changed = True
        if not changed:
            break

    for target in targets:
        target_key = str(target).upper()
        start_exclude, end_exclude = lag_bounds.get(target_key, (0, 0))
        start_exclude = min(start_exclude, _NOMISS_BOUNDARY_PROPAGATION_CAP)
        end_exclude = min(end_exclude, _NOMISS_BOUNDARY_PROPAGATION_CAP)
        if start_exclude <= 0 and end_exclude <= 0:
            continue

        target_windows = (windows_by_target or {}).get(target_key, windows)
        scoped_windows = _resolve_scope_windows(frame.index, target_windows)
        if not scoped_windows:
            continue

        excluded: set[object] = set()
        for scoped_index in scoped_windows:
            if len(scoped_index) == 0:
                continue
            if start_exclude > 0:
                excluded.update(scoped_index[:start_exclude])
            if end_exclude > 0:
                excluded.update(scoped_index[-end_exclude:])
        if excluded:
            exclusions[target_key] = excluded

    return exclusions


def run_mini_run(
    records: list,
    bundle,
    *,
    max_steps: int | None = None,
    on_error: str = "continue",
    eval_context: EvalContext | None = None,
) -> dict[str, object]:
    runtime_base_dir = getattr(bundle, "legacy_base_dir", None)
    result = run_mini_run_engine(
        records=records,
        data=bundle.merged_data,
        max_steps=max_steps,
        on_error=on_error,
        eval_context=eval_context,
        runtime_base_dir=runtime_base_dir,
    )
    unsupported = sum(result.unsupported_counts.values())
    return {
        "summary": {
            "records": result.total_records,
            "planned": result.planned_steps,
            "executed": result.executed_steps,
            "failed": result.failed_steps,
            "unsupported": unsupported,
            "unsupported_counts": result.unsupported_counts,
        },
        "issues": [
            {
                "line": issue.line_number,
                "statement": issue.statement,
                "error": issue.error,
            }
            for issue in result.issues
        ],
        "unsupported_examples": [
            {
                "line": example.line_number,
                "command": example.command,
                "statement": example.statement,
            }
            for example in result.unsupported_examples
        ],
        "executed_line_numbers": list(result.executed_line_numbers),
        "output": result.frame,
    }


def _cmd_bundle_summary(
    config_path: str | Path | None = None,
    *,
    merge_external: bool = False,
    conflict: str = "error",
) -> int:
    bundle = load_execution_input_bundle(
        config_path=config_path,
        merge_external=merge_external,
        conflict=conflict,
    )
    source_names = ", ".join(bundle.external_sources) if bundle.external_sources else "(none)"
    print("Execution input bundle summary:")
    print(f"- fmdata: rows={len(bundle.fmdata)}, columns={len(bundle.fmdata.columns)}")
    print(f"- fmage: rows={len(bundle.fmage)}, columns={len(bundle.fmage.columns)}")
    print(f"- fmexog: rows={len(bundle.fmexog)}, columns={len(bundle.fmexog.columns)}")
    print(f"- external source names: {source_names}")
    print(f"- external source count: {len(bundle.external_sources)}")
    print(
        f"- merged_data: rows={len(bundle.merged_data)}, columns={len(bundle.merged_data.columns)}"
    )
    return 0


def _load_model_config(config_path: str | Path | None = None) -> ModelInputConfig:
    return load_model_config(config_path)


def _legacy_file_rows(config: ModelInputConfig) -> tuple[tuple[str, Path], ...]:
    return (
        ("fminput", config.legacy.fminput),
        ("fmdata", config.legacy.fmdata),
        ("fmage", config.legacy.fmage),
        ("fmexog", config.legacy.fmexog),
        ("fmout", config.legacy.fmout),
    )


def _cmd_summary(config_path: str | Path | None = None) -> int:
    config = _load_model_config(config_path)
    summary = summarize_fminput(config.legacy.fminput)
    print(f"Template directory: {config.legacy.fminput.parent}")
    print(f"Model header: {summary.model_header}")
    print(f"Total lines: {summary.total_lines}")
    print(f"Equations: {summary.equation_count}")
    print(f"Identities: {summary.identity_count}")
    print(f"Fortran reference: {FORTRAN_REFERENCE}")
    return 0


def _cmd_check_files(config_path: str | Path | None = None) -> int:
    config = _load_model_config(config_path)
    required_paths = tuple(path for _, path in _legacy_file_rows(config))
    source_paths = tuple(source.path for source in config.external_sources)
    missing = [path for path in required_paths if not path.exists()]
    missing.extend(path for path in source_paths if not path.exists())
    if not FORTRAN_REFERENCE.exists():
        missing.append(FORTRAN_REFERENCE)

    if missing:
        print("Missing required files:")
        for path in missing:
            print(f"- {path}")
        return 1

    print("All required template and reference files are present.")
    for path in required_paths:
        size = Path(path).stat().st_size
        print(f"- {path.name}: {size} bytes")
    for path in source_paths:
        size = Path(path).stat().st_size
        print(f"- {path.name}: {size} bytes (external source)")
    print(f"- {FORTRAN_REFERENCE.name}: {FORTRAN_REFERENCE.stat().st_size} bytes")
    return 0


def _cmd_parse_summary(
    include_comments: bool,
    config_path: str | Path | None = None,
) -> int:
    config = _load_model_config(config_path)
    records = parse_fminput_file(config.legacy.fminput)
    frequencies = count_commands(records, include_comments=include_comments)

    print(f"Parsed records: {len(records)}")
    print(f"Include comments: {include_comments}")
    print("Command frequencies:")
    for command in FPCommand:
        if not include_comments and command == FPCommand.COMMENT:
            continue
        print(f"- {command.value}: {frequencies[command]}")

    multiline_records = sum(1 for record in records if len(record.raw_lines) > 1)
    print(f"Multiline statements: {multiline_records}")
    return 0


def _cmd_plan_summary(config_path: str | Path | None = None) -> int:
    config = _load_model_config(config_path)
    records = parse_fminput_file(config.legacy.fminput)
    plan = build_execution_plan(records)
    genr_steps = sum(1 for step in plan if step.command == FPCommand.GENR)
    ident_steps = sum(1 for step in plan if step.command == FPCommand.IDENT)
    reordered_steps = sum(1 for step in plan if getattr(step, "dependency_reordered", False))
    windows = {(step.window.start, step.window.end) for step in plan if step.window is not None}

    print(f"Planned steps: {len(plan)}")
    print(f"- GENR steps: {genr_steps}")
    print(f"- IDENT steps: {ident_steps}")
    print(f"- Dependency-reordered steps: {reordered_steps}")
    print(f"Distinct active windows: {len(windows)}")
    return 0


def _command_label(command: object) -> str:
    if isinstance(command, FPCommand):
        return command.value
    return str(command)


def _parse_command_filter(include_commands: str | None) -> set[FPCommand] | None:
    if include_commands is None:
        return None
    raw_tokens = [token.strip().upper() for token in include_commands.split(",") if token.strip()]
    if not raw_tokens:
        return None

    by_name = {command.value.upper(): command for command in FPCommand}
    unknown = [token for token in raw_tokens if token not in by_name]
    if unknown:
        raise ValueError(f"unknown command names: {', '.join(sorted(unknown))}")
    return {by_name[token] for token in raw_tokens}


def _filter_records_for_mini_run(
    records: list,
    *,
    line_start: int | None = None,
    line_end: int | None = None,
    include_commands: str | None = None,
) -> list:
    if line_start is not None and line_start <= 0:
        raise ValueError("line_start must be positive when provided")
    if line_end is not None and line_end <= 0:
        raise ValueError("line_end must be positive when provided")
    if line_start is not None and line_end is not None and line_start > line_end:
        raise ValueError("line_start cannot be greater than line_end")

    command_filter = _parse_command_filter(include_commands)
    filtered: list = []
    for record in records:
        if line_start is not None and record.line_number < line_start:
            continue
        if line_end is not None and record.line_number > line_end:
            continue
        if command_filter is not None and record.command not in command_filter:
            continue
        filtered.append(record)
    return filtered


def _is_filtered_run(
    *,
    line_start: int | None,
    line_end: int | None,
    include_commands: str | None,
    max_steps: int | None,
) -> bool:
    return any(
        value is not None and value != ""
        for value in (line_start, line_end, include_commands, max_steps)
    )


def _build_eq_backfill_records(
    unfiltered_records: list,
    filtered_records: list,
    *,
    line_start: int | None,
    line_end: int | None,
    include_command_filter: set[FPCommand] | None,
    eq_filter_use_full_context: bool = False,
    eq_specs: dict[str, object] | None = None,
) -> list:
    if include_command_filter is None or include_command_filter != {FPCommand.EQ}:
        return filtered_records

    selected_eq_lines = [
        int(record.line_number) for record in filtered_records if record.command == FPCommand.EQ
    ]
    max_selected_eq_line = max(selected_eq_lines) if selected_eq_lines else line_end

    needed_post_assignment_targets: set[str] = set()
    if eq_specs:
        specs_by_number = _build_eq_spec_number_map(eq_specs)
        for record in filtered_records:
            if record.command != FPCommand.EQ:
                continue
            target = _resolve_eq_target(
                record,
                specs=eq_specs,
                specs_by_number=specs_by_number,
            )
            if not target:
                continue
            for target_name in _expand_dependency_names(str(target)):
                needed_post_assignment_targets.add(target_name)
            spec = eq_specs.get(str(target).upper()) or eq_specs.get(str(target))
            if spec is None:
                continue
            for term in getattr(spec, "terms", ()):
                dep_name = str(getattr(term, "variable", "")).upper()
                if dep_name in _RESERVED_REFERENCE_NAMES:
                    continue
                if dep_name.startswith("CNST2") or dep_name == "TBL2":
                    continue
                for expanded_name in _expand_dependency_names(dep_name):
                    needed_post_assignment_targets.add(expanded_name)

    eq_records: list = []
    for record in unfiltered_records:
        if eq_filter_use_full_context and record.command in {FPCommand.MODEQ, FPCommand.FSR}:
            eq_records.append(record)
            continue

        if line_end is not None and record.line_number > line_end:
            continue

        context_commands = {
            FPCommand.SMPL,
            FPCommand.SETUPSOLVE,
            FPCommand.EXOGENOUS,
            FPCommand.GENR,
            FPCommand.IDENT,
            FPCommand.CREATE,
        }
        if record.command in context_commands:
            if (
                not eq_filter_use_full_context
                and max_selected_eq_line is not None
                and record.line_number > max_selected_eq_line
                and record.command in {FPCommand.GENR, FPCommand.IDENT, FPCommand.CREATE}
            ):
                if not needed_post_assignment_targets:
                    continue
                try:
                    assignment = parse_assignment(record.statement)
                except Exception:
                    continue
                lhs_key = str(assignment.lhs).upper()
                if lhs_key not in needed_post_assignment_targets:
                    continue
                for dep_name, _dep_lag, _dep_lead in _extract_reference_shifts(assignment.rhs):
                    for expanded_name in _expand_dependency_names(dep_name):
                        needed_post_assignment_targets.add(expanded_name)
            eq_records.append(record)
            continue

        if record.command == FPCommand.FSR:
            if line_start is not None and record.line_number < line_start:
                continue
            eq_records.append(record)
            continue

        if line_start is not None and record.line_number < line_start:
            continue
        if record.command in {FPCommand.EQ, FPCommand.LHS}:
            eq_records.append(record)

    return eq_records


def _records_for_eq_iteration(
    records: list,
    *,
    iteration: int,
    context_assignments_first_iter_only: bool,
    context_keep_create_all_iters: bool = False,
    retain_assignment_targets: frozenset[str] = frozenset(),
) -> list:
    # FP semantics: CREATE is executed once at definition time and never
    # replayed during solve iterations.  Always strip CREATE on iteration > 1
    # regardless of the context_keep_create_all_iters flag.
    #
    # GENR/IDENT/LHS are part of the SOLA pass order (EQ -> LHS -> IDENT ->
    # GENR) and MUST be re-evaluated on every iteration.  In Fortran FP these
    # are TURBO types 2/4/5 executed each Gauss-Seidel pass.  Stripping them
    # after iteration 1 freezes derived variables (e.g. TFG, TFS, PIEFA) and
    # causes large forecast parity gaps.
    strip_create = iteration > 1
    if strip_create:
        return [r for r in records if r.command != FPCommand.CREATE]
    return records


def _expand_dependency_names(name: str) -> tuple[str, ...]:
    key = str(name).upper()
    names = {key}
    if key.endswith("Z") and len(key) > 1:
        names.add(key[:-1])
    else:
        names.add(f"{key}Z")
    return tuple(sorted(names))


def _build_keyboard_stub_lag_probe_targets(
    records: list,
    *,
    keyboard_targets: tuple[str, ...],
    max_depth: int = 3,
    max_targets: int = 64,
) -> tuple[str, ...]:
    if not records or not keyboard_targets or max_depth <= 0 or max_targets <= 0:
        return tuple()

    reverse_dependencies: dict[str, set[str]] = {}
    for record in records:
        if record.command not in {
            FPCommand.LHS,
            FPCommand.IDENT,
            FPCommand.GENR,
            FPCommand.CREATE,
        }:
            continue
        try:
            assignment = parse_assignment(record.statement)
        except Exception:
            continue
        lhs = str(assignment.lhs).upper()
        if not lhs or lhs in _RESERVED_REFERENCE_NAMES:
            continue
        refs = _extract_reference_shifts(str(assignment.rhs))
        for dep_name, _, _ in refs:
            if dep_name in _RESERVED_REFERENCE_NAMES:
                continue
            for expanded in _expand_dependency_names(str(dep_name)):
                dep_key = str(expanded).upper()
                if dep_key in _RESERVED_REFERENCE_NAMES:
                    continue
                reverse_dependencies.setdefault(dep_key, set()).add(lhs)

    seed_keys: list[str] = []
    for target in keyboard_targets:
        for expanded in _expand_dependency_names(str(target)):
            key = str(expanded).upper()
            if key and key not in _RESERVED_REFERENCE_NAMES:
                seed_keys.append(key)

    visited: set[str] = set(seed_keys)
    queue: list[tuple[str, int]] = [(key, 0) for key in seed_keys]
    selected: list[str] = []
    selected_set: set[str] = set()

    while queue and len(selected) < max_targets:
        current, depth = queue.pop(0)
        if depth >= max_depth:
            continue
        for dependent in sorted(reverse_dependencies.get(current, set())):
            if dependent in _RESERVED_REFERENCE_NAMES:
                continue
            for expanded in _expand_dependency_names(dependent):
                expanded_key = str(expanded).upper()
                if expanded_key in _RESERVED_REFERENCE_NAMES:
                    continue
                if expanded_key in selected_set:
                    continue
                selected.append(expanded_key)
                selected_set.add(expanded_key)
                if len(selected) >= max_targets:
                    break
            if len(selected) >= max_targets:
                break
            if dependent not in visited:
                visited.add(dependent)
                queue.append((dependent, depth + 1))

    return tuple(selected)


def _collect_assignment_records_by_lhs(
    records: list,
    *,
    commands: frozenset[FPCommand],
    lhs_targets: frozenset[str],
) -> list:
    if not records or not commands or not lhs_targets:
        return []
    normalized_targets = {str(item).upper() for item in lhs_targets if str(item).strip()}
    if not normalized_targets:
        return []

    selected: list = []
    for record in records:
        if record.command not in commands:
            continue
        if record.command == FPCommand.EQ:
            match = _EQ_REFERENCE_RE.match(record.statement)
            if match is None:
                continue
            token_raw = match.group("token")
            token = token_raw.upper() if token_raw else ""
            if not token or token.startswith("NONE") or token == "FSR":
                continue
            lhs_expanded = {str(item).upper() for item in _expand_dependency_names(token)}
            if lhs_expanded.isdisjoint(normalized_targets):
                continue
            selected.append(record)
            continue
        try:
            assignment = parse_assignment(record.statement)
        except Exception:
            continue
        lhs_key = str(assignment.lhs).upper()
        lhs_expanded = {str(item).upper() for item in _expand_dependency_names(lhs_key)}
        if lhs_expanded.isdisjoint(normalized_targets):
            continue
        selected.append(record)
    return selected


def _snapshot_probe_values(
    frame: pd.DataFrame,
    *,
    period: str,
    targets: tuple[str, ...],
) -> dict[str, float | None]:
    if frame.empty or not targets:
        return {}
    row_label = None
    if period in frame.index:
        row_label = period
    else:
        for candidate in frame.index:
            if str(candidate) == str(period):
                row_label = candidate
                break
    if row_label is None:
        return {}

    row = frame.loc[row_label]
    values: dict[str, float | None] = {}
    for target in targets:
        column = _resolve_eq_seed_column(frame, str(target))
        if column is None:
            values[str(target)] = None
            continue
        raw = row.get(column)
        if raw is None:
            values[str(target)] = None
            continue
        try:
            numeric = float(raw)
        except (TypeError, ValueError):
            values[str(target)] = None
            continue
        values[str(target)] = numeric if math.isfinite(numeric) else None
    return values


def _resolve_eq_seed_column(frame: pd.DataFrame, name: str) -> str | None:
    resolved = _resolve_column_name(frame, name)
    if resolved is not None:
        return resolved
    if name.endswith("Z") and len(name) > 1:
        return _resolve_column_name(frame, name[:-1])
    return _resolve_column_name(frame, f"{name}Z")


def _collect_eq_seed_targets(records: list, specs: dict[str, object]) -> tuple[str, ...]:
    targets: set[str] = set(_collect_eq_backfill_targets(records, specs))
    if not specs:
        return tuple(sorted(targets))
    eq_fsr_lines = collect_eq_fsr_command_lines(records)

    specs_by_number: dict[int, object] = {}
    for spec in specs.values():
        number = getattr(spec, "equation_number", None)
        if isinstance(number, int):
            specs_by_number.setdefault(number, spec)

    for record in records:
        if record.command != FPCommand.EQ:
            continue
        if record.line_number in eq_fsr_lines:
            continue
        match = _EQ_REFERENCE_RE.match(record.statement)
        if match is None:
            continue
        token_raw = match.group("token")
        token = token_raw.upper() if token_raw else None
        if token and token.startswith("NONE"):
            continue
        number = int(match.group("number"))

        spec = None
        if token and token not in {"FSR"}:
            spec = specs.get(token)
        if spec is None:
            spec = specs_by_number.get(number)
        if spec is None and token:
            spec = specs.get(token)
        if spec is None:
            continue

        for term in getattr(spec, "terms", ()):
            variable = str(getattr(term, "variable", "")).upper()
            if not variable or variable in {"C", "RHO"}:
                continue
            if variable == "TBL2" or variable.startswith("CNST2"):
                continue
            targets.add(variable)

    return tuple(sorted(targets))


def _seed_eq_frame_from_fmout(
    frame: pd.DataFrame,
    *,
    fmout_path: Path,
    metric: str,
    fill: str,
    targets: tuple[str, ...],
) -> tuple[pd.DataFrame, dict[str, object]]:
    report: dict[str, object] = {
        "enabled": True,
        "status": "disabled",
        "metric": metric,
        "fill": fill,
        "variables_total": len(targets),
        "variables_seeded": 0,
        "variables_added": 0,
        "variables_missing_source": 0,
        "overlap_periods": 0,
        "cells_written": 0,
    }
    if not targets:
        report["status"] = "no_targets"
        return frame, report
    if fill not in {"missing", "overwrite"}:
        report["status"] = "invalid_fill"
        return frame, report
    payload = load_fmout_structured(fmout_path)
    if payload is None:
        report["status"] = "no_structured_forecast"
        return frame, report

    metric_by_name = {
        "lv": payload.levels,
        "ch": payload.changes,
        "pct": payload.pct_changes,
    }
    source_frame = metric_by_name.get(metric)
    if source_frame is None or source_frame.empty:
        report["status"] = "empty_metric_frame"
        return frame, report

    overlap = frame.index.intersection(source_frame.index)
    report["overlap_periods"] = len(overlap)
    if len(overlap) == 0:
        report["status"] = "no_overlap_periods"
        return frame, report

    working = frame.copy(deep=True)
    for target in targets:
        source_column = _resolve_eq_seed_column(source_frame, target)
        if source_column is None:
            report["variables_missing_source"] = int(report["variables_missing_source"]) + 1
            continue

        target_column = _resolve_eq_seed_column(working, target)
        if target_column is None:
            target_column = str(target)
            working[target_column] = float("nan")
            report["variables_added"] = int(report["variables_added"]) + 1

        source_values = pd.to_numeric(source_frame.loc[overlap, source_column], errors="coerce")
        current_values = pd.to_numeric(working.loc[overlap, target_column], errors="coerce")
        if fill == "overwrite":
            write_mask = source_values.notna()
        else:
            write_mask = current_values.isna() & source_values.notna()

        write_count = int(write_mask.sum())
        if write_count <= 0:
            continue

        working.loc[overlap[write_mask], target_column] = source_values.loc[write_mask]
        report["cells_written"] = int(report["cells_written"]) + write_count
        report["variables_seeded"] = int(report["variables_seeded"]) + 1

    report["status"] = "seeded" if int(report["cells_written"]) > 0 else "no_writes"
    return working, report


def _seed_state_frame_from_pabev(
    frame: pd.DataFrame,
    *,
    pabev_path: Path,
    period: str,
    fill: str,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Overwrite (or fill) a single period's cells from a reference PABEV.TXT.

    This is a diagnostic lever to distinguish "initial state / ingestion seeds the drift"
    from "solve loop numeric path causes the drift".
    """

    report: dict[str, object] = {
        "enabled": True,
        "status": "disabled",
        "fill": fill,
        "pabev": str(pabev_path),
        "period": str(period),
        "variables_total": 0,
        "variables_matched": 0,
        "cells_written": 0,
    }
    if fill not in {"missing", "overwrite"}:
        report["status"] = "invalid_fill"
        return frame, report
    if frame.empty:
        report["status"] = "no_frame"
        return frame, report
    if not str(period).strip():
        report["status"] = "invalid_period"
        return frame, report

    from fppy.pabev_parity import parse_pabev

    try:
        pabev_periods, pabev_series = parse_pabev(pabev_path)
    except Exception as exc:
        report["status"] = "load_error"
        report["error"] = str(exc)
        return frame, report
    report["variables_total"] = len(pabev_series)

    target_period = str(period).strip()
    pabev_idx: int | None = None
    for idx, p in enumerate(pabev_periods):
        if str(p).strip() == target_period:
            pabev_idx = idx
            break
    if pabev_idx is None:
        report["status"] = "period_not_found_in_pabev"
        return frame, report

    row_label = None
    for idx_val in frame.index:
        if str(idx_val).strip() == target_period:
            row_label = idx_val
            break
    if row_label is None:
        report["status"] = "period_not_found_in_frame"
        return frame, report

    working = frame.copy(deep=True)
    for name, values in pabev_series.items():
        column = _resolve_column_name(working, str(name))
        if column is None:
            continue
        report["variables_matched"] = int(report["variables_matched"]) + 1
        source_value = float(values[pabev_idx])
        if fill == "missing":
            current = working.at[row_label, column]
            if not pd.isna(current):
                continue
        working.at[row_label, column] = source_value
        report["cells_written"] = int(report["cells_written"]) + 1

    report["status"] = "seeded" if int(report["cells_written"]) > 0 else "no_writes"
    return working, report


def _compute_output_parity_report(
    output_frame: pd.DataFrame,
    *,
    fmout_path: Path,
    atol: float,
    top: int = 10,
) -> dict[str, object]:
    report: dict[str, object] = {
        "enabled": True,
        "status": "disabled",
        "fmout_path": str(fmout_path),
        "atol": float(atol),
        "shared_periods": 0,
        "shared_variables": 0,
        "comparable_cells": 0,
        "mismatch_count": 0,
        "mismatch_rate": 0.0,
        "top_mismatches": [],
    }
    if output_frame.empty:
        report["status"] = "no_output"
        return report

    try:
        payload = load_fmout_structured(fmout_path)
    except Exception as exc:
        report["status"] = "load_error"
        report["error"] = str(exc)
        return report

    if payload is None or payload.levels.empty:
        report["status"] = "no_baseline_levels"
        return report

    candidate = output_frame.copy(deep=False)
    baseline = payload.levels.copy(deep=False)
    candidate.index = candidate.index.astype(str)
    baseline.index = baseline.index.astype(str)

    shared_index = candidate.index.intersection(baseline.index)
    shared_columns = [column for column in baseline.columns if column in candidate.columns]
    report["shared_periods"] = len(shared_index)
    report["shared_variables"] = len(shared_columns)
    if len(shared_index) == 0 or len(shared_columns) == 0:
        report["status"] = "no_overlap"
        return report

    left = candidate.loc[shared_index, shared_columns].apply(pd.to_numeric, errors="coerce")
    right = baseline.loc[shared_index, shared_columns].apply(pd.to_numeric, errors="coerce")
    delta = (left - right).abs()
    valid_mask = delta.notna()
    comparable_cells = int(valid_mask.values.sum())
    report["comparable_cells"] = comparable_cells
    if comparable_cells == 0:
        report["status"] = "no_comparable_cells"
        return report

    mismatch_mask = valid_mask & (delta > float(atol))
    mismatch_count = int(mismatch_mask.values.sum())
    report["mismatch_count"] = mismatch_count
    report["mismatch_rate"] = float(mismatch_count) / float(comparable_cells)

    flattened = delta.stack().dropna().sort_values(ascending=False)
    top_rows: list[dict[str, object]] = []
    for (period, variable), abs_delta in flattened.head(max(0, int(top))).items():
        top_rows.append({
            "period": str(period),
            "variable": str(variable),
            "abs_delta": float(abs_delta),
            "candidate": float(left.loc[period, variable]),
            "baseline": float(right.loc[period, variable]),
        })
    report["top_mismatches"] = top_rows
    report["status"] = "ok"
    return report


def _cmd_dependency_summary(config_path: str | Path | None = None) -> int:
    config = _load_model_config(config_path)
    records = parse_fminput_file(config.legacy.fminput)
    result = build_dependency_order(records)
    steps = result.steps
    order = result.order
    command_counts = Counter(_command_label(step.command) for step in steps)

    print(f"Dependency steps: {len(steps)}")
    print(f"Dependency order: {len(order)}")
    formatted = ", ".join(f"{k}: {command_counts[k]}" for k in sorted(command_counts))
    print(f"Dependency command counts: {formatted}")
    print(f"Dependency edges: {result.edge_count}")
    print(f"Cyclic dependencies detected: {result.cyclic}")
    print(f"Unresolved references: {sum(len(v) for v in result.unresolved_references.values())}")
    return 0


def _cmd_mini_run(
    config_path: str | Path | None = None,
    *,
    merge_external: bool = False,
    conflict: str = "error",
    max_steps: int | None = None,
    on_error: str = "continue",
    enable_eq: bool = False,
    eq_coefs_fmout: str | Path | None = None,
    eq_flags_preset_label: str | None = None,
    eq_use_setupsolve: bool = False,
    eq_iters: int | None = None,
    eq_atol: float | None = None,
    eq_dampall: float | None = None,
    eq_period_sequential: bool = False,
    eq_period_scoped: str = "auto",
    eq_period_scoped_range: Sequence[str] | None = None,
    eq_period_sequential_fp_pass_order: bool = True,
    eq_period_sequential_all_assignments: bool = False,
    eq_period_sequential_defer_create: bool = False,
    eq_period_sequential_context_assignments_first_iter_only: bool = False,
    eq_period_sequential_context_keep_create_all_iters: bool = False,
    eq_period_sequential_context_replay_command_time_smpl: bool = False,
    eq_period_sequential_context_replay_command_time_smpl_clip_to_solve_window: bool = False,
    eq_period_sequential_context_retain_overlap_only: bool = False,
    eq_period_sequential_context_replay_trace: bool = False,
    eq_period_sequential_context_replay_trace_max_events: int = 200,
    eq_period_sequential_context_replay_trace_targets: str | None = None,
    eq_period_sequential_context_replay_trace_periods: str | None = None,
    eq_iter_trace: bool = False,
    eq_iter_trace_period: str | None = None,
    eq_iter_trace_max_events: int = 200,
    eq_iter_trace_targets: str | None = None,
    eq_quantize_store: str = "off",
    eq_rho_aware: bool = False,
    eq_rho_resid_ar1: bool = False,
    eq_rho_resid_ar1_carry_iterations: bool = False,
    eq_rho_resid_ar1_residual_probe_seed_mode: str = "carry",
    eq_rho_resid_ar1_start_iter: int = 1,
    eq_rho_resid_ar1_disable_iter: int = 0,
    eq_rho_resid_ar1_seed_lag: int = 1,
    eq_rho_resid_ar1_seed_mode: str = "legacy",
    eq_rho_resid_ar1_boundary_reset: str = "off",
    eq_rho_resid_ar1_carry_lag: int = 0,
    eq_rho_resid_ar1_carry_damp: float = 1.0,
    eq_rho_resid_ar1_carry_damp_mode: str = "term",
    eq_rho_resid_ar1_carry_multipass: bool = False,
    eq_rho_resid_ar1_lag_gating: str = "off",
    eq_rho_resid_ar1_update_source: str = "structural",
    eq_rho_resid_ar1_la257_update_rule: str = "legacy",
    eq_rho_resid_ar1_la257_fortran_cycle_carry_style: str = "legacy",
    eq_rho_resid_ar1_la257_u_phase_max_gain: float | None = None,
    eq_rho_resid_ar1_la257_u_phase_max_gain_mode: str = "relative",
    eq_rho_resid_ar1_la257_start_iter: int = 1,
    eq_rho_resid_ar1_la257_disable_iter: int = 0,
    eq_rho_resid_ar1_la257_lifecycle_staging: bool = False,
    eq_rho_resid_ar1_commit_after_check: bool | None = None,
    eq_rho_resid_ar1_la257_u_phase_lines: str | None = None,
    eq_rho_resid_ar1_trace_lines: str | None = None,
    eq_rho_resid_ar1_trace_event_limit: int = 200,
    eq_rho_resid_ar1_trace_focus_targets: str | None = None,
    eq_rho_resid_ar1_trace_focus_periods: str | None = None,
    eq_rho_resid_ar1_trace_focus_iters: str | None = None,
    eq_rho_resid_ar1_trace_focus_max_events: int = 200,
    eq_rho_resid_ar1_fpe_trap: bool = False,
    eq_require_convergence: bool = False,
    eq_fail_on_diverged: bool = False,
    eq_fail_on_residual: bool = False,
    eq_fail_on_residual_all_targets: bool = False,
    eq_residual_ratio_max: float = 1.0,
    eq_top_target_deltas: int = 0,
    eq_seed: str = "off",
    eq_seed_fmout: str | Path | None = None,
    eq_seed_metric: str = "lv",
    eq_seed_fill: str = "missing",
    eq_state_seed_pabev: str | Path | None = None,
    eq_state_seed_period: str | None = None,
    eq_state_seed_fill: str = "overwrite",
    eq_eval_precision: str = "float64",
    eq_term_order: str = "as_parsed",
    eq_eq_read_mode: str = "live",
    eq_math_backend: str = "numpy",
    eq_quantize_eq_commit: str = "off",
    eq_filter_use_full_context: bool = False,
    eq_modeq_fsr_side_channel: str = "off",
    eq_modeq_fsr_side_channel_max_events: int = 200,
    parity_baseline_fmout: str | Path | None = None,
    parity_atol: float = 1e-6,
    parity_top: int = 10,
    line_start: int | None = None,
    line_end: int | None = None,
    include_commands: str | None = None,
    output_csv: str | Path | None = None,
    report_json: str | Path | None = None,
    solver_trace: str | Path | None = None,
) -> int:
    mini_run_started = time.monotonic()
    config = _load_model_config(config_path)
    bundle = load_execution_input_bundle(
        config_path=config_path,
        merge_external=merge_external,
        conflict=conflict,
    )
    runtime_base_dir = getattr(bundle, "legacy_base_dir", None)
    unfiltered_records = parse_fminput_tree_file(
        config.legacy.fminput,
        runtime_base_dir=runtime_base_dir,
    )
    try:
        include_command_filter = _parse_command_filter(include_commands)
        records = _filter_records_for_mini_run(
            unfiltered_records,
            line_start=line_start,
            line_end=line_end,
            include_commands=include_commands,
        )
    except ValueError as exc:
        print(f"Mini-run filter error: {exc}")
        return 1
    if eq_seed not in {"off", "filtered", "all"}:
        print("Mini-run option error: eq_seed must be one of off, filtered, all")
        return 1
    if eq_seed_metric not in {"lv", "ch", "pct"}:
        print("Mini-run option error: eq_seed_metric must be one of lv, ch, pct")
        return 1
    if eq_seed_fill not in {"missing", "overwrite"}:
        print("Mini-run option error: eq_seed_fill must be one of missing, overwrite")
        return 1
    if eq_state_seed_fill not in {"missing", "overwrite"}:
        print("Mini-run option error: eq_state_seed_fill must be one of missing, overwrite")
        return 1
    if (eq_state_seed_pabev is None) != (eq_state_seed_period is None):
        print(
            "Mini-run option error: eq_state_seed_pabev and eq_state_seed_period "
            "must either both be set or both be omitted"
        )
        return 1
    if str(eq_quantize_eq_commit).strip().lower() not in {"off", "float32"}:
        print("Mini-run option error: eq_quantize_eq_commit must be one of off, float32")
        return 1
    if str(eq_eval_precision).strip().lower() not in {"float64", "longdouble"}:
        print("Mini-run option error: eq_eval_precision must be one of float64, longdouble")
        return 1
    if str(eq_term_order).strip().lower() not in {"as_parsed", "by_index"}:
        print("Mini-run option error: eq_term_order must be one of as_parsed, by_index")
        return 1
    if str(eq_eq_read_mode).strip().lower() not in {"live", "frozen"}:
        print("Mini-run option error: eq_eq_read_mode must be one of live, frozen")
        return 1
    if str(eq_math_backend).strip().lower() not in {"numpy", "math"}:
        print("Mini-run option error: eq_math_backend must be one of numpy, math")
        return 1
    if eq_modeq_fsr_side_channel not in {"off", "capture", "enforce"}:
        print(
            "Mini-run option error: eq_modeq_fsr_side_channel must be one of off, capture, enforce"
        )
        return 1
    if eq_modeq_fsr_side_channel_max_events < 0:
        print("Mini-run option error: eq_modeq_fsr_side_channel_max_events must be nonnegative")
        return 1
    if eq_dampall is not None and eq_dampall < 0:
        print("Mini-run option error: eq_dampall must be nonnegative")
        return 1
    if eq_atol is not None and eq_atol < 0:
        print("Mini-run option error: eq_atol must be nonnegative")
        return 1
    if eq_residual_ratio_max < 0:
        print("Mini-run option error: eq_residual_ratio_max must be nonnegative")
        return 1
    if eq_top_target_deltas < 0:
        print("Mini-run option error: eq_top_target_deltas must be nonnegative")
        return 1
    if str(eq_quantize_store).strip().lower() not in {"off", "float32"}:
        print("Mini-run option error: eq_quantize_store must be one of off, float32")
        return 1
    if eq_period_sequential_context_replay_trace_max_events < 0:
        print(
            "Mini-run option error: "
            "eq_period_sequential_context_replay_trace_max_events must be nonnegative"
        )
        return 1
    eq_period_scoped_range_parsed: tuple[str, str] | None = None
    eq_period_scoped_range_keys: tuple[tuple[int, int], tuple[int, int]] | None = None
    if eq_period_scoped_range is not None:
        if str(eq_period_scoped).strip().lower() == "off":
            print(
                "Mini-run option error: eq_period_scoped_range requires "
                "eq_period_scoped set to auto or on"
            )
            return 1
        if not eq_period_sequential:
            print("Mini-run option error: eq_period_scoped_range requires eq_period_sequential")
            return 1
        if len(eq_period_scoped_range) != 2:
            print(
                "Mini-run option error: eq_period_scoped_range expects two period "
                "tokens: <start> <end>"
            )
            return 1
        scoped_start = str(eq_period_scoped_range[0]).strip()
        scoped_end = str(eq_period_scoped_range[1]).strip()
        scoped_start_key = _period_key(scoped_start)
        scoped_end_key = _period_key(scoped_end)
        if scoped_start_key is None or scoped_end_key is None:
            print(
                "Mini-run option error: eq_period_scoped_range expects period labels "
                "formatted like YYYY(.subperiod)"
            )
            return 1
        if scoped_start_key > scoped_end_key:
            print("Mini-run option error: eq_period_scoped_range start must be <= end")
            return 1
        eq_period_scoped_range_parsed = (scoped_start, scoped_end)
        eq_period_scoped_range_keys = (scoped_start_key, scoped_end_key)
    if eq_rho_resid_ar1_trace_event_limit < 0:
        print("Mini-run option error: eq_rho_resid_ar1_trace_event_limit must be nonnegative")
        return 1
    if eq_rho_resid_ar1_trace_focus_max_events < 0:
        print("Mini-run option error: eq_rho_resid_ar1_trace_focus_max_events must be nonnegative")
        return 1
    if eq_period_sequential_all_assignments and not eq_period_sequential:
        print(
            "Mini-run option error: eq_period_sequential_all_assignments requires "
            "eq_period_sequential"
        )
        return 1
    if eq_period_sequential_defer_create and not eq_period_sequential:
        print(
            "Mini-run option error: eq_period_sequential_defer_create requires "
            "eq_period_sequential"
        )
        return 1
    if eq_period_sequential_context_assignments_first_iter_only and not eq_period_sequential:
        print(
            "Mini-run option error: "
            "eq_period_sequential_context_assignments_first_iter_only requires "
            "eq_period_sequential"
        )
        return 1
    # fp_pass_order defaults to True; silently ignore when period_sequential
    # is False rather than raising an error.
    if (
        eq_period_sequential_context_keep_create_all_iters
        and not eq_period_sequential_context_assignments_first_iter_only
    ):
        print(
            "Mini-run option error: "
            "eq_period_sequential_context_keep_create_all_iters requires "
            "eq_period_sequential_context_assignments_first_iter_only"
        )
        return 1
    if eq_period_sequential_context_replay_command_time_smpl and not eq_period_sequential:
        print(
            "Mini-run option error: "
            "eq_period_sequential_context_replay_command_time_smpl requires "
            "eq_period_sequential"
        )
        return 1
    if (
        eq_period_sequential_context_replay_command_time_smpl
        and not eq_period_sequential_context_assignments_first_iter_only
    ):
        print(
            "Mini-run option error: "
            "eq_period_sequential_context_replay_command_time_smpl requires "
            "eq_period_sequential_context_assignments_first_iter_only"
        )
        return 1
    if (
        eq_period_sequential_context_replay_command_time_smpl_clip_to_solve_window
        and not eq_period_sequential_context_replay_command_time_smpl
    ):
        print(
            "Mini-run option error: "
            "eq_period_sequential_context_replay_command_time_smpl_clip_to_solve_window "
            "requires eq_period_sequential_context_replay_command_time_smpl"
        )
        return 1
    if eq_period_sequential_context_retain_overlap_only and not eq_period_sequential:
        print(
            "Mini-run option error: "
            "eq_period_sequential_context_retain_overlap_only requires "
            "eq_period_sequential"
        )
        return 1
    if eq_period_sequential_context_replay_trace and not eq_period_sequential:
        print(
            "Mini-run option error: "
            "eq_period_sequential_context_replay_trace requires eq_period_sequential"
        )
        return 1
    context_replay_trace_filter_enabled = any((
        eq_period_sequential_context_replay_trace_targets,
        eq_period_sequential_context_replay_trace_periods,
    ))
    eq_period_sequential_context_replay_trace_targets_parsed: tuple[str, ...] = tuple()
    eq_period_sequential_context_replay_trace_periods_parsed: tuple[str, ...] = tuple()
    if context_replay_trace_filter_enabled:
        if not eq_period_sequential_context_replay_trace:
            print(
                "Mini-run option error: "
                "eq_period_sequential_context_replay_trace_targets/periods require "
                "eq_period_sequential_context_replay_trace"
            )
            return 1
        try:
            eq_period_sequential_context_replay_trace_targets_parsed = _parse_focus_csv(
                eq_period_sequential_context_replay_trace_targets,
                field_name="eq_period_sequential_context_replay_trace_targets",
            )
            eq_period_sequential_context_replay_trace_periods_parsed = _parse_focus_csv(
                eq_period_sequential_context_replay_trace_periods,
                field_name="eq_period_sequential_context_replay_trace_periods",
            )
        except ValueError as exc:
            print(f"Mini-run option error: {exc}")
            return 1
        if (
            eq_period_sequential_context_replay_trace_targets is not None
            and not eq_period_sequential_context_replay_trace_targets_parsed
        ):
            print(
                "Mini-run option error: "
                "eq_period_sequential_context_replay_trace_targets must include at least one target"
            )
            return 1
        if (
            eq_period_sequential_context_replay_trace_periods is not None
            and not eq_period_sequential_context_replay_trace_periods_parsed
        ):
            print(
                "Mini-run option error: "
                "eq_period_sequential_context_replay_trace_periods must include at least one period"
            )
            return 1
    if eq_iter_trace_max_events < 0:
        print("Mini-run option error: eq_iter_trace_max_events must be nonnegative")
        return 1
    if not eq_iter_trace and (
        eq_iter_trace_period is not None or eq_iter_trace_targets is not None
    ):
        print("Mini-run option error: eq_iter_trace_period/targets require eq_iter_trace")
        return 1
    eq_iter_trace_period_resolved = (
        str(eq_iter_trace_period).strip() if eq_iter_trace_period is not None else "2025.4"
    )
    if eq_iter_trace and not eq_iter_trace_period_resolved:
        print("Mini-run option error: eq_iter_trace_period must be non-empty")
        return 1
    eq_iter_trace_targets_parsed: tuple[str, ...] = tuple()
    try:
        eq_iter_trace_targets_parsed = _parse_focus_csv(
            eq_iter_trace_targets,
            field_name="eq_iter_trace_targets",
        )
    except ValueError as exc:
        print(f"Mini-run option error: {exc}")
        return 1
    if eq_iter_trace_targets is not None and not eq_iter_trace_targets_parsed:
        print("Mini-run option error: eq_iter_trace_targets must include at least one target")
        return 1
    eq_top_target_deltas_effective = int(eq_top_target_deltas)
    if eq_iter_trace and eq_top_target_deltas_effective <= 0:
        eq_top_target_deltas_effective = 10
    if eq_rho_aware and eq_rho_resid_ar1:
        print("Mini-run option error: eq_rho_aware and eq_rho_resid_ar1 are mutually exclusive")
        return 1
    if eq_rho_resid_ar1_carry_iterations and not eq_rho_resid_ar1:
        print("Mini-run option error: eq_rho_resid_ar1_carry_iterations requires eq_rho_resid_ar1")
        return 1
    if eq_rho_resid_ar1_carry_iterations and not eq_period_sequential:
        print(
            "Mini-run option error: eq_rho_resid_ar1_carry_iterations requires eq_period_sequential"
        )
        return 1
    if eq_rho_resid_ar1_residual_probe_seed_mode not in {"carry", "clear"}:
        print(
            "Mini-run option error: eq_rho_resid_ar1_residual_probe_seed_mode must be one of carry, clear"
        )
        return 1
    if eq_rho_resid_ar1_start_iter < 1:
        print("Mini-run option error: eq_rho_resid_ar1_start_iter must be >= 1")
        return 1
    if eq_rho_resid_ar1_disable_iter < 0:
        print("Mini-run option error: eq_rho_resid_ar1_disable_iter must be nonnegative")
        return 1
    if eq_rho_resid_ar1_seed_lag < 0:
        print("Mini-run option error: eq_rho_resid_ar1_seed_lag must be nonnegative")
        return 1
    if eq_rho_resid_ar1_carry_lag < 0:
        print("Mini-run option error: eq_rho_resid_ar1_carry_lag must be nonnegative")
        return 1
    if (
        not math.isfinite(float(eq_rho_resid_ar1_carry_damp))
        or eq_rho_resid_ar1_carry_damp < 0
        or eq_rho_resid_ar1_carry_damp > 1
    ):
        print(
            "Mini-run option error: eq_rho_resid_ar1_carry_damp must be finite and between 0 and 1"
        )
        return 1
    if eq_rho_resid_ar1_carry_damp_mode not in {"term", "state", "sol4", "la257"}:
        print(
            "Mini-run option error: eq_rho_resid_ar1_carry_damp_mode must be one of term, state, sol4, la257"
        )
        return 1
    if eq_rho_resid_ar1_seed_mode not in {"legacy", "positioned"}:
        print(
            "Mini-run option error: eq_rho_resid_ar1_seed_mode must be one of legacy, positioned"
        )
        return 1
    if eq_rho_resid_ar1_boundary_reset not in {"off", "window"}:
        print("Mini-run option error: eq_rho_resid_ar1_boundary_reset must be one of off, window")
        return 1
    if eq_rho_resid_ar1_lag_gating not in {"off", "lhs", "all"}:
        print("Mini-run option error: eq_rho_resid_ar1_lag_gating must be one of off, lhs, all")
        return 1
    if eq_rho_resid_ar1_update_source not in {"structural", "result", "solved", "resid_pass"}:
        print(
            "Mini-run option error: eq_rho_resid_ar1_update_source must be one of structural, result, solved, resid_pass"
        )
        return 1
    if eq_rho_resid_ar1_la257_update_rule not in {"legacy", "fortran_u_phase", "fortran_cycle"}:
        print(
            "Mini-run option error: eq_rho_resid_ar1_la257_update_rule must be one of legacy, fortran_u_phase, fortran_cycle"
        )
        return 1
    if eq_rho_resid_ar1_la257_fortran_cycle_carry_style not in {"legacy", "ema"}:
        print(
            "Mini-run option error: eq_rho_resid_ar1_la257_fortran_cycle_carry_style must be one of legacy or ema"
        )
        return 1
    if (
        eq_rho_resid_ar1_la257_fortran_cycle_carry_style != "legacy"
        and eq_rho_resid_ar1_la257_update_rule != "fortran_cycle"
    ):
        print(
            "Mini-run option error: "
            "eq_rho_resid_ar1_la257_fortran_cycle_carry_style requires "
            "eq_rho_resid_ar1_la257_update_rule=fortran_cycle"
        )
        return 1
    if eq_rho_resid_ar1_la257_u_phase_max_gain is not None:
        if (
            not math.isfinite(float(eq_rho_resid_ar1_la257_u_phase_max_gain))
            or float(eq_rho_resid_ar1_la257_u_phase_max_gain) < 0.0
        ):
            print(
                "Mini-run option error: eq_rho_resid_ar1_la257_u_phase_max_gain must be finite and nonnegative"
            )
            return 1
    if eq_rho_resid_ar1_la257_u_phase_max_gain_mode not in {"relative", "anchored"}:
        print(
            "Mini-run option error: eq_rho_resid_ar1_la257_u_phase_max_gain_mode must be one of relative, anchored"
        )
        return 1
    if eq_rho_resid_ar1_la257_start_iter < 1:
        print("Mini-run option error: eq_rho_resid_ar1_la257_start_iter must be >= 1")
        return 1
    if eq_rho_resid_ar1_la257_disable_iter < 0:
        print("Mini-run option error: eq_rho_resid_ar1_la257_disable_iter must be nonnegative")
        return 1
    if eq_rho_resid_ar1_la257_lifecycle_staging:
        if not eq_rho_resid_ar1:
            print(
                "Mini-run option error: eq_rho_resid_ar1_la257_lifecycle_staging requires eq_rho_resid_ar1"
            )
            return 1
        if not eq_period_sequential:
            print(
                "Mini-run option error: eq_rho_resid_ar1_la257_lifecycle_staging requires eq_period_sequential"
            )
            return 1
        if eq_rho_resid_ar1_carry_damp_mode != "la257":
            print(
                "Mini-run option error: eq_rho_resid_ar1_la257_lifecycle_staging requires "
                "eq_rho_resid_ar1_carry_damp_mode=la257"
            )
            return 1
        if eq_rho_resid_ar1_update_source != "resid_pass":
            print(
                "Mini-run option error: eq_rho_resid_ar1_la257_lifecycle_staging requires "
                "eq_rho_resid_ar1_update_source=resid_pass"
            )
            return 1
    if eq_rho_resid_ar1_commit_after_check is not None:
        if not eq_rho_resid_ar1:
            print(
                "Mini-run option error: eq_rho_resid_ar1_commit_after_check requires eq_rho_resid_ar1"
            )
            return 1
        if not eq_period_sequential:
            print(
                "Mini-run option error: eq_rho_resid_ar1_commit_after_check requires eq_period_sequential"
            )
            return 1
    if eq_rho_resid_ar1_la257_update_rule != "legacy":
        if not eq_rho_resid_ar1:
            print(
                "Mini-run option error: eq_rho_resid_ar1_la257_update_rule requires eq_rho_resid_ar1"
            )
            return 1
        if not eq_period_sequential:
            print(
                "Mini-run option error: eq_rho_resid_ar1_la257_update_rule requires eq_period_sequential"
            )
            return 1
        if eq_rho_resid_ar1_carry_damp_mode != "la257":
            print(
                "Mini-run option error: eq_rho_resid_ar1_la257_update_rule requires eq_rho_resid_ar1_carry_damp_mode=la257"
            )
            return 1
    try:
        eq_rho_resid_ar1_la257_u_phase_lines_parsed = _parse_la257_selector_csv(
            eq_rho_resid_ar1_la257_u_phase_lines,
            field_name="eq_rho_resid_ar1_la257_u_phase_lines",
        )
    except ValueError as exc:
        print(f"Mini-run option error: {exc}")
        return 1
    if eq_rho_resid_ar1_la257_u_phase_lines_parsed:
        if eq_rho_resid_ar1_la257_update_rule not in {"fortran_u_phase", "fortran_cycle"}:
            print(
                "Mini-run option error: eq_rho_resid_ar1_la257_u_phase_lines requires "
                "eq_rho_resid_ar1_la257_update_rule in {fortran_u_phase, fortran_cycle}"
            )
            return 1
        if not eq_rho_resid_ar1:
            print(
                "Mini-run option error: eq_rho_resid_ar1_la257_u_phase_lines requires eq_rho_resid_ar1"
            )
            return 1
        if not eq_period_sequential:
            print(
                "Mini-run option error: eq_rho_resid_ar1_la257_u_phase_lines requires eq_period_sequential"
            )
            return 1
    if (
        eq_rho_resid_ar1_la257_update_rule == "fortran_cycle"
        and not eq_rho_resid_ar1_la257_u_phase_lines_parsed
    ):
        print(
            "Mini-run option error: eq_rho_resid_ar1_la257_update_rule=fortran_cycle "
            "requires eq_rho_resid_ar1_la257_u_phase_lines"
        )
        return 1
    try:
        eq_rho_resid_ar1_trace_lines_parsed = _parse_la257_selector_csv(
            eq_rho_resid_ar1_trace_lines,
            field_name="eq_rho_resid_ar1_trace_lines",
        )
    except ValueError as exc:
        print(f"Mini-run option error: {exc}")
        return 1
    if eq_rho_resid_ar1_trace_lines_parsed and not eq_rho_resid_ar1:
        print("Mini-run option error: eq_rho_resid_ar1_trace_lines requires eq_rho_resid_ar1")
        return 1
    if eq_rho_resid_ar1_trace_lines_parsed and not eq_rho_resid_ar1_carry_iterations:
        print(
            "Mini-run option error: eq_rho_resid_ar1_trace_lines requires eq_rho_resid_ar1_carry_iterations"
        )
        return 1
    focus_trace_enabled = any((
        eq_rho_resid_ar1_trace_focus_targets,
        eq_rho_resid_ar1_trace_focus_periods,
        eq_rho_resid_ar1_trace_focus_iters,
    ))
    eq_rho_resid_ar1_focus_trace_spec: Ar1TraceFocusSpec | None = None
    if focus_trace_enabled:
        if not eq_rho_resid_ar1:
            print(
                "Mini-run option error: eq_rho_resid_ar1_trace_focus_* requires eq_rho_resid_ar1"
            )
            return 1
        if not eq_period_sequential:
            print(
                "Mini-run option error: eq_rho_resid_ar1_trace_focus_* requires eq_period_sequential"
            )
            return 1
        try:
            focus_targets = _parse_focus_csv(
                eq_rho_resid_ar1_trace_focus_targets,
                field_name="eq_rho_resid_ar1_trace_focus_targets",
            )
            focus_periods = _parse_focus_csv(
                eq_rho_resid_ar1_trace_focus_periods,
                field_name="eq_rho_resid_ar1_trace_focus_periods",
            )
            focus_iter_min, focus_iter_max = _parse_focus_iter_range(
                eq_rho_resid_ar1_trace_focus_iters,
                field_name="eq_rho_resid_ar1_trace_focus_iters",
            )
        except ValueError as exc:
            print(f"Mini-run option error: {exc}")
            return 1
        if not focus_targets:
            print(
                "Mini-run option error: eq_rho_resid_ar1_trace_focus_targets must include at least one target"
            )
            return 1
        if not focus_periods:
            print(
                "Mini-run option error: eq_rho_resid_ar1_trace_focus_periods must include at least one period"
            )
            return 1
        eq_rho_resid_ar1_focus_trace_spec = Ar1TraceFocusSpec(
            targets=frozenset(str(target).upper() for target in focus_targets),
            periods=frozenset(str(period) for period in focus_periods),
            iter_min=int(focus_iter_min),
            iter_max=int(focus_iter_max),
            max_events=int(eq_rho_resid_ar1_trace_focus_max_events),
            stages=frozenset(_TRACE_FOCUS_STAGE_ORDER.keys()),
        )
    if parity_atol < 0:
        print("Mini-run option error: parity_atol must be nonnegative")
        return 1
    if parity_top < 0:
        print("Mini-run option error: parity_top must be nonnegative")
        return 1
    enable_eq_requested = bool(enable_eq)
    auto_enable_eq_from_solve = False
    # Use unfiltered records for SOLVE directive collection so that
    # --include-commands EQ doesn't exclude the SOLVE command itself.
    solve_runtime_directives = _collect_solve_runtime_directives(unfiltered_records)
    solve_line_numbers = tuple(
        int(item.get("line_number", 0)) for item in solve_runtime_directives
    )
    eq_eval_context_requested = bool(enable_eq_requested or solve_line_numbers)
    eq_specs: dict[str, object] | None = None
    eval_context: EvalContext | None = None
    if eq_eval_context_requested:
        eq_coefs_path = config.legacy.fmout if eq_coefs_fmout is None else Path(eq_coefs_fmout)
        eq_specs = load_eq_specs_from_fmout(eq_coefs_path)
        try:
            coef_values = build_coef_table(eq_specs)
        except Exception:
            coef_values = {}
        if coef_values:
            eval_context = EvalContext(coef_values=coef_values)

    pre_solve_records = records
    if enable_eq_requested and solve_line_numbers:
        # When EQ backfill is enabled, treat `SOLVE` as a phase boundary:
        # - pre-solve mini-run should prepare data/GENR/CREATE state up to SOLVE
        # - identities/LHS should be deferred to the solve loop (and may not be
        #   stored historically, matching fp.exe boundary behavior)
        pre_solve_line_end = max(int(line) for line in solve_line_numbers)
        pre_solve_records = [
            record
            for record in records
            if int(record.line_number) <= pre_solve_line_end
            and record.command not in {FPCommand.IDENT, FPCommand.LHS}
        ]

    result = run_mini_run(
        pre_solve_records,
        bundle,
        max_steps=max_steps,
        on_error=on_error,
        eval_context=eval_context,
    )

    if isinstance(result, dict):
        result_summary = result.get("summary")
        if not isinstance(result_summary, dict):
            result_summary = result
        issues = result.get("issues", [])
        unsupported_examples = result.get("unsupported_examples", [])
        executed_line_numbers = result.get("executed_line_numbers", [])
        output_data = result.get("output")
    else:
        result_summary = getattr(result, "summary", {})
        if not isinstance(result_summary, dict):
            result_summary = {}
        issues = getattr(result, "issues", [])
        unsupported_examples = getattr(result, "unsupported_examples", [])
        executed_line_numbers = getattr(result, "executed_line_numbers", [])
        output_data = getattr(result, "output", None)

    if not isinstance(issues, list):
        issues = list(issues) if issues is not None else []
    if not isinstance(unsupported_examples, list):
        unsupported_examples = (
            list(unsupported_examples) if unsupported_examples is not None else []
        )
    if not isinstance(executed_line_numbers, list):
        executed_line_numbers = (
            list(executed_line_numbers) if executed_line_numbers is not None else []
        )
    if not enable_eq_requested and solve_line_numbers:
        enable_eq = _solve_reached_for_mini_run(
            solve_lines=solve_line_numbers,
            max_steps=max_steps,
            executed_line_numbers=executed_line_numbers,
        )
        auto_enable_eq_from_solve = bool(enable_eq)

    active_solve_runtime_directive = _resolve_active_solve_runtime_directive(
        directives=solve_runtime_directives,
        max_steps=max_steps,
        executed_line_numbers=executed_line_numbers,
    )

    if not isinstance(result_summary, dict):
        result_summary = {}

    records_count = int(result_summary.get("records", len(records)))
    planned = int(result_summary.get("planned", 0))
    executed = int(result_summary.get("executed", 0))
    failed = int(result_summary.get("failed", 0))
    unsupported_counts = result_summary.get("unsupported_counts", {})
    if not isinstance(unsupported_counts, dict):
        unsupported_counts = {}
    if unsupported_counts is None:
        unsupported_counts = {}
    unsupported_command_count = int(
        result_summary.get(
            "unsupported",
            sum(int(value) for value in unsupported_counts.values() if isinstance(value, int)),
        )
    )

    output_frame = _coerce_backfill_frame(output_data, pd.DataFrame())
    solve_active_line = (
        int(active_solve_runtime_directive.get("line_number", 0))
        if isinstance(active_solve_runtime_directive, dict)
        else None
    )
    solve_active_window = (
        active_solve_runtime_directive.get("window")
        if isinstance(active_solve_runtime_directive, dict)
        else None
    )
    solve_active_outside = bool(
        isinstance(active_solve_runtime_directive, dict)
        and active_solve_runtime_directive.get("outside")
    )
    solve_active_noreset = bool(
        isinstance(active_solve_runtime_directive, dict)
        and active_solve_runtime_directive.get("noreset")
    )
    solve_active_filevar = (
        str(active_solve_runtime_directive.get("filevar"))
        if isinstance(active_solve_runtime_directive, dict)
        and active_solve_runtime_directive.get("filevar") is not None
        else None
    )
    solve_active_keyboard_targets = (
        tuple(
            str(item) for item in active_solve_runtime_directive.get("keyboard_targets", tuple())
        )
        if isinstance(active_solve_runtime_directive, dict)
        else tuple()
    )
    solve_active_keyboard_targets_normalized = tuple(
        dict.fromkeys(
            str(item).upper() for item in solve_active_keyboard_targets if str(item).strip()
        )
    )
    solve_active_keyboard_targets_set = frozenset(solve_active_keyboard_targets_normalized)
    (
        eq_backfill_auto_keyboard_stub_active,
        eq_period_sequential,
        eq_period_scoped,
        eq_backfill_auto_keyboard_stub_period_sequential_enabled,
        eq_backfill_auto_keyboard_stub_period_scoped_enabled,
    ) = _resolve_auto_keyboard_solve_stub_flags(
        auto_enable_eq_from_solve=bool(auto_enable_eq_from_solve),
        solve_active_outside=bool(solve_active_outside),
        solve_active_filevar=solve_active_filevar,
        solve_active_keyboard_targets_set=solve_active_keyboard_targets_set,
        eq_use_setupsolve=bool(eq_use_setupsolve),
        eq_period_sequential=bool(eq_period_sequential),
        eq_period_scoped=str(eq_period_scoped),
    )
    (
        eq_period_sequential_all_assignments,
        eq_backfill_auto_keyboard_stub_all_assignments_enabled,
    ) = _resolve_auto_keyboard_all_assignments_flag(
        auto_stub_active=bool(eq_backfill_auto_keyboard_stub_active),
        eq_period_sequential_all_assignments=bool(eq_period_sequential_all_assignments),
    )
    (
        eq_period_sequential_context_assignments_first_iter_only,
        eq_backfill_auto_keyboard_stub_context_first_iter_enabled,
    ) = _resolve_auto_keyboard_context_first_iter_flag(
        auto_stub_active=bool(eq_backfill_auto_keyboard_stub_active),
        eq_period_sequential_all_assignments=bool(eq_period_sequential_all_assignments),
        eq_use_setupsolve=bool(eq_use_setupsolve),
        eq_period_sequential_context_assignments_first_iter_only=bool(
            eq_period_sequential_context_assignments_first_iter_only
        ),
    )
    if (
        eq_backfill_auto_keyboard_stub_period_sequential_enabled
        or eq_backfill_auto_keyboard_stub_period_scoped_enabled
        or eq_backfill_auto_keyboard_stub_all_assignments_enabled
        or eq_backfill_auto_keyboard_stub_context_first_iter_enabled
    ):
        print(
            "Mini-run info: auto-enabled keyboard SOLVE OUTSIDE stub "
            f"(eq_period_sequential={eq_period_sequential}, "
            f"eq_period_scoped={eq_period_scoped}, "
            "eq_period_sequential_all_assignments="
            f"{bool(eq_period_sequential_all_assignments)}, "
            "eq_period_sequential_context_assignments_first_iter_only="
            f"{bool(eq_period_sequential_context_assignments_first_iter_only)})"
        )
    solve_outside_seeded_cells = 0
    solve_outside_seed_inspected_cells = 0
    solve_outside_seed_candidate_cells = 0
    eq_backfill_outside_post_seed_cells = 0
    eq_backfill_outside_post_seed_inspected_cells = 0
    eq_backfill_outside_post_seed_candidate_cells = 0
    solve_post_commands_replayed = False
    solve_post_replay_issue_count = 0
    eq_backfill_applied = 0
    eq_backfill_failed = 0
    eq_backfill_iterations = 0
    eq_backfill_iterations_total = 0
    eq_backfill_converged = False
    eq_backfill_max_abs_delta = 0.0
    eq_backfill_iteration_history: list[dict[str, object]] = []
    eq_backfill_min_iters = 0
    eq_backfill_max_iters = 0
    eq_backfill_auto_keyboard_stub_min_iters_enabled = False
    eq_backfill_auto_keyboard_stub_context_first_iter_disabled_for_min_iters = False
    eq_backfill_maxcheck: int | None = None
    eq_backfill_nomiss_required = False
    eq_backfill_nomiss_missing_cells = 0
    eq_backfill_nomiss_target_diagnostics: list[dict[str, object]] = []
    eq_backfill_record_count = 0
    eq_backfill_attempted_records = 0
    eq_backfill_modeq_fsr_updates = 0
    eq_backfill_modeq_fsr_active_equation_count = 0
    eq_backfill_modeq_fsr_active_term_count = 0
    eq_backfill_eq_fsr_skipped = 0
    eq_backfill_modeq_fsr_side_channel = str(eq_modeq_fsr_side_channel)
    eq_backfill_modeq_fsr_side_channel_status = (
        "disabled" if eq_backfill_modeq_fsr_side_channel == "off" else "ok"
    )
    eq_backfill_modeq_fsr_side_channel_effective_limit = int(eq_modeq_fsr_side_channel_max_events)
    eq_backfill_modeq_fsr_side_channel_events: list[dict[str, object]] = []
    eq_backfill_modeq_fsr_side_channel_events_count = 0
    eq_backfill_modeq_fsr_side_channel_events_truncated = False
    eq_backfill_modeq_fsr_side_channel_breach_count = 0
    eq_backfill_modeq_fsr_side_channel_mutation_count = 0
    eq_backfill_modeq_fsr_side_channel_first_breach: dict[str, object] | None = None
    eq_backfill_stop_reason = "disabled"
    eq_backfill_min_abs_delta = 0.0
    eq_backfill_last_abs_delta = 0.0
    eq_backfill_delta_ratio = 0.0
    eq_backfill_diverged = False
    eq_backfill_dampall = 1.0
    eq_backfill_tol = 1e-8
    eq_backfill_tol_source = "default_eq_atol"
    eq_backfill_tol_mode = "absolute"
    eq_backfill_tolallabs = False
    eq_backfill_convergence_delta = 0.0
    eq_backfill_convergence_ratio = 0.0
    eq_backfill_convergence_delta_all_targets = 0.0
    eq_backfill_convergence_ratio_all_targets = 0.0
    eq_backfill_residual_abs_delta = 0.0
    eq_backfill_residual_ratio = 0.0
    eq_backfill_residual_failed = 0
    eq_backfill_residual_issue_count = 0
    eq_backfill_residual_probe_has_nonfinite_delta = False
    eq_backfill_residual_probe_has_nonfinite_ratio = False
    eq_backfill_residual_probe_has_nonfinite = False
    eq_backfill_residual_abs_delta_all_targets = 0.0
    eq_backfill_residual_ratio_all_targets = 0.0
    eq_backfill_residual_failed_all_targets = 0
    eq_backfill_residual_issue_count_all_targets = 0
    eq_backfill_residual_probe_has_nonfinite_delta_all_targets = False
    eq_backfill_residual_probe_has_nonfinite_ratio_all_targets = False
    eq_backfill_residual_probe_has_nonfinite_all_targets = False
    eq_backfill_iteration_has_nonfinite = False
    eq_backfill_iteration_first_nonfinite_iteration: int | None = None
    eq_backfill_residual_ratio_max = float(eq_residual_ratio_max)
    eq_backfill_damp_overrides: dict[str, float] = {}
    eq_backfill_tol_overrides: dict[str, float] = {}
    eq_backfill_tolabs_overrides: set[str] = set()
    eq_backfill_override_issues: list[dict[str, object]] = []
    eq_backfill_checked_targets: tuple[str, ...] = tuple()
    eq_backfill_all_targets: tuple[str, ...] = tuple()
    eq_backfill_keyboard_targets_missing = False
    eq_backfill_keyboard_targets_unmatched: tuple[str, ...] = tuple()
    eq_backfill_keyboard_stop_classification = "inactive"
    eq_backfill_keyboard_nonconvergence_reason: str | None = None
    eq_backfill_keyboard_stub_lag_probe_targets: tuple[str, ...] = tuple()
    eq_backfill_keyboard_stub_lag_probe_period: str | None = None
    eq_backfill_keyboard_stub_lag_probe_events: list[dict[str, object]] = []
    eq_backfill_runtime_total_seconds = 0.0
    eq_backfill_runtime_build_records_seconds = 0.0
    eq_backfill_runtime_setupsolve_seconds = 0.0
    eq_backfill_runtime_specs_seconds = 0.0
    eq_backfill_runtime_window_prep_seconds = 0.0
    eq_backfill_runtime_iteration_seconds = 0.0
    eq_backfill_runtime_iteration_apply_seconds = 0.0
    eq_backfill_runtime_post_solve_replay_seconds = 0.0
    eq_backfill_runtime_iteration_apply_calls = 0
    setupsolve_summary: dict[str, object] | None = None
    eq_backfill_converged_checked_only = False
    eq_backfill_period_sequential = bool(eq_period_sequential)
    eq_backfill_period_sequential_all_assignments = bool(eq_period_sequential_all_assignments)
    eq_backfill_period_sequential_defer_create = bool(eq_period_sequential_defer_create)
    eq_backfill_period_sequential_context_assignments_first_iter_only = bool(
        eq_period_sequential_context_assignments_first_iter_only
    )
    eq_backfill_period_sequential_context_keep_create_all_iters = bool(
        eq_period_sequential_context_keep_create_all_iters
    )
    eq_backfill_period_sequential_context_replay_command_time_smpl = bool(
        eq_period_sequential_context_replay_command_time_smpl
    )
    eq_backfill_period_sequential_context_replay_command_time_smpl_clip_to_solve_window = bool(
        eq_period_sequential_context_replay_command_time_smpl_clip_to_solve_window
    )
    eq_backfill_period_sequential_context_retain_overlap_only = bool(
        eq_period_sequential_context_retain_overlap_only
    )
    eq_backfill_period_sequential_context_replay_trace = bool(
        eq_period_sequential_context_replay_trace
    )
    eq_backfill_period_sequential_context_replay_trace_max_events = int(
        eq_period_sequential_context_replay_trace_max_events
    )
    eq_backfill_period_sequential_context_replay_trace_targets = tuple(
        eq_period_sequential_context_replay_trace_targets_parsed
    )
    eq_backfill_period_sequential_context_replay_trace_periods = tuple(
        eq_period_sequential_context_replay_trace_periods_parsed
    )
    eq_backfill_quantize_store = str(eq_quantize_store).strip().lower()
    eq_backfill_quantize_store_cells = 0
    eq_backfill_top_target_deltas = int(eq_top_target_deltas_effective)
    eq_backfill_specs_has_rho_terms = False
    eq_backfill_rho_aware = bool(eq_rho_aware)
    eq_backfill_rho_resid_ar1 = bool(eq_rho_resid_ar1)
    eq_backfill_rho_mode_auto_selected = False
    eq_backfill_rho_mode_auto_reason = "none"
    eq_backfill_rho_mode_warnings: list[str] = []
    eq_backfill_rho_resid_ar1_carry_iterations = bool(eq_rho_resid_ar1_carry_iterations)
    eq_backfill_rho_resid_ar1_residual_probe_seed_mode = str(
        eq_rho_resid_ar1_residual_probe_seed_mode
    )
    eq_backfill_rho_resid_ar1_start_iter = int(eq_rho_resid_ar1_start_iter)
    eq_backfill_rho_resid_ar1_disable_iter = int(eq_rho_resid_ar1_disable_iter)
    eq_backfill_rho_resid_ar1_effective_last = bool(eq_backfill_rho_resid_ar1)
    eq_backfill_rho_resid_ar1_seed_lag = int(eq_rho_resid_ar1_seed_lag)
    eq_backfill_rho_resid_ar1_carry_lag = int(eq_rho_resid_ar1_carry_lag)
    eq_backfill_rho_resid_ar1_carry_damp = float(eq_rho_resid_ar1_carry_damp)
    eq_backfill_rho_resid_ar1_carry_damp_mode = str(eq_rho_resid_ar1_carry_damp_mode)
    eq_backfill_rho_resid_ar1_carry_multipass = bool(eq_rho_resid_ar1_carry_multipass)
    eq_backfill_rho_resid_ar1_seed_mode = str(eq_rho_resid_ar1_seed_mode)
    eq_backfill_rho_resid_ar1_boundary_reset = str(eq_rho_resid_ar1_boundary_reset)
    eq_backfill_rho_resid_ar1_lag_gating = str(eq_rho_resid_ar1_lag_gating)
    eq_backfill_rho_resid_ar1_update_source = str(eq_rho_resid_ar1_update_source)
    eq_backfill_rho_resid_ar1_la257_update_rule = str(eq_rho_resid_ar1_la257_update_rule)
    eq_backfill_rho_resid_ar1_la257_fortran_cycle_carry_style = str(
        eq_rho_resid_ar1_la257_fortran_cycle_carry_style
    )
    eq_backfill_rho_resid_ar1_la257_u_phase_max_gain = (
        float(eq_rho_resid_ar1_la257_u_phase_max_gain)
        if eq_rho_resid_ar1_la257_u_phase_max_gain is not None
        else None
    )
    eq_backfill_rho_resid_ar1_la257_u_phase_max_gain_mode = str(
        eq_rho_resid_ar1_la257_u_phase_max_gain_mode
    )
    eq_backfill_rho_resid_ar1_la257_start_iter = int(eq_rho_resid_ar1_la257_start_iter)
    eq_backfill_rho_resid_ar1_la257_disable_iter = int(eq_rho_resid_ar1_la257_disable_iter)
    eq_backfill_rho_resid_ar1_la257_update_rule_effective_last = str(
        eq_backfill_rho_resid_ar1_la257_update_rule
    )
    eq_backfill_rho_resid_ar1_la257_lifecycle_staging = bool(
        eq_rho_resid_ar1_la257_lifecycle_staging
    )
    if eq_rho_resid_ar1_commit_after_check is None:
        # Residual-carry period-sequential runs default to deferred commit semantics.
        eq_backfill_rho_resid_ar1_commit_after_check = bool(
            eq_rho_resid_ar1 and eq_period_sequential
        )
    else:
        eq_backfill_rho_resid_ar1_commit_after_check = bool(eq_rho_resid_ar1_commit_after_check)
    eq_backfill_rho_resid_ar1_commit_mode = (
        "deferred" if eq_backfill_rho_resid_ar1_commit_after_check else "immediate"
    )
    eq_backfill_rho_resid_ar1_la257_u_phase_lines = eq_rho_resid_ar1_la257_u_phase_lines_parsed
    eq_backfill_rho_resid_ar1_la257_u_phase_lines_display = _format_la257_selectors(
        eq_backfill_rho_resid_ar1_la257_u_phase_lines
    )
    eq_backfill_rho_resid_ar1_trace_lines = eq_rho_resid_ar1_trace_lines_parsed
    eq_backfill_rho_resid_ar1_trace_lines_display = _format_la257_selectors(
        eq_backfill_rho_resid_ar1_trace_lines
    )
    eq_backfill_rho_resid_ar1_trace_event_limit = int(eq_rho_resid_ar1_trace_event_limit)
    eq_backfill_rho_resid_ar1_fpe_trap = bool(eq_rho_resid_ar1_fpe_trap)
    eq_backfill_rho_resid_iteration_state_size = 0
    eq_backfill_rho_resid_iteration_state_top: list[dict[str, object]] = []
    eq_backfill_rho_resid_iteration_trace_last: list[dict[str, object]] = []
    eq_backfill_rho_resid_ar1_trace_event_count = 0
    eq_backfill_rho_resid_ar1_trace_events: list[dict[str, object]] = []
    eq_backfill_rho_resid_ar1_trace_events_truncated = False
    eq_backfill_rho_resid_ar1_trace_events_total_count = 0
    eq_backfill_rho_resid_ar1_trace_events_serialized_count = 0
    eq_backfill_rho_resid_ar1_trace_events_serialization_policy = "head_tail_sample"
    eq_backfill_rho_resid_ar1_trace_events_sample_head_count = 20
    eq_backfill_rho_resid_ar1_trace_events_sample_tail_count = 20
    eq_backfill_rho_resid_ar1_trace_events_truncated_by_event_limit = False
    eq_backfill_rho_resid_ar1_trace_events_sampled = False
    eq_backfill_rho_resid_ar1_trace_event_kind_counts: dict[str, int] = {}
    eq_backfill_rho_resid_ar1_trace_event_phase_counts: dict[str, int] = {}
    eq_backfill_rho_resid_ar1_trace_event_pass_counts: list[dict[str, int]] = []
    eq_backfill_rho_resid_ar1_trace_event_carry_reject_reason_counts: dict[str, int] = {}
    eq_backfill_rho_resid_ar1_trace_focus_spec_payload: dict[str, object] | None = (
        {
            "targets": sorted(eq_rho_resid_ar1_focus_trace_spec.targets),
            "periods": sorted(eq_rho_resid_ar1_focus_trace_spec.periods),
            "iter_min": int(eq_rho_resid_ar1_focus_trace_spec.iter_min),
            "iter_max": int(eq_rho_resid_ar1_focus_trace_spec.iter_max),
            "max_events": int(eq_rho_resid_ar1_focus_trace_spec.max_events),
            "stages": sorted(
                eq_rho_resid_ar1_focus_trace_spec.stages,
                key=lambda key: _TRACE_FOCUS_STAGE_ORDER.get(key, 99),
            ),
        }
        if eq_rho_resid_ar1_focus_trace_spec is not None
        else None
    )
    eq_backfill_rho_resid_ar1_trace_focus_events: list[dict[str, object]] = []
    eq_backfill_rho_resid_ar1_trace_focus_events_count = 0
    eq_backfill_rho_resid_ar1_trace_focus_events_truncated_by_limit = False
    eq_backfill_rho_resid_ar1_trace_focus_no_match = False
    eq_backfill_rho_resid_ar1_trace_focus_event_counts_by_stage: dict[str, int] = {}
    eq_backfill_rho_resid_ar1_trace_focus_seq = 0
    eq_backfill_first_nonfinite_blame: dict[str, object] | None = None
    eq_backfill_first_failure_blame: dict[str, object] | None = None
    eq_backfill_hf_line182_overflow_probe_history: list[dict[str, object]] = []
    eq_backfill_hf_lhf1_ho_probe_history: list[dict[str, object]] = []
    eq_backfill_context_replay_trace_events: list[dict[str, object]] = []
    eq_backfill_context_replay_trace_events_count = 0
    eq_backfill_context_replay_trace_events_truncated = False
    eq_backfill_context_replay_trace_path: str | None = None
    eq_iter_trace_events: list[dict[str, object]] = []
    eq_iter_trace_events_count = 0
    eq_iter_trace_events_truncated = False
    eq_iter_trace_path: str | None = None
    eq_seed_report: dict[str, object] = {
        "enabled": False,
        "mode": eq_seed,
        "status": "disabled",
        "metric": eq_seed_metric,
        "fill": eq_seed_fill,
        "variables_total": 0,
        "variables_seeded": 0,
        "variables_added": 0,
        "variables_missing_source": 0,
        "overlap_periods": 0,
        "cells_written": 0,
    }
    state_seed_report: dict[str, object] = {
        "enabled": False,
        "status": "disabled",
        "fill": str(eq_state_seed_fill),
        "pabev": str(eq_state_seed_pabev) if eq_state_seed_pabev is not None else None,
        "period": str(eq_state_seed_period) if eq_state_seed_period is not None else None,
        "variables_total": 0,
        "variables_matched": 0,
        "cells_written": 0,
    }
    parity_report: dict[str, object] = {
        "enabled": False,
        "status": "disabled",
        "fmout_path": None,
        "atol": float(parity_atol),
        "shared_periods": 0,
        "shared_variables": 0,
        "comparable_cells": 0,
        "mismatch_count": 0,
        "mismatch_rate": 0.0,
        "top_mismatches": [],
    }
    solver_trace_events: list[dict[str, object]] = []
    solver_trace_seq = 0
    solver_trace_path = str(Path(solver_trace)) if solver_trace is not None else None
    solver_trace_enabled = solver_trace_path is not None
    solver_trace_event_count = 0

    def _append_solver_trace_event(
        event_type: str,
        *,
        iteration: int | None = None,
        period: object | None = None,
        target: object | None = None,
        payload: dict[str, object] | None = None,
    ) -> None:
        nonlocal solver_trace_seq
        if not solver_trace_enabled:
            return
        solver_trace_seq += 1
        event: dict[str, object] = {
            "seq": int(solver_trace_seq),
            "event_type": str(event_type),
        }
        if iteration is not None:
            event["iter"] = int(iteration)
        if period is not None:
            event["period"] = str(period)
        if target is not None:
            event["target"] = str(target)
        if payload is not None:
            for key, raw_value in payload.items():
                if raw_value is None:
                    event[str(key)] = None
                    continue
                if isinstance(raw_value, (dict, list, tuple, set)):
                    continue
                if isinstance(raw_value, bool):
                    event[str(key)] = bool(raw_value)
                    continue
                if isinstance(raw_value, int):
                    event[str(key)] = int(raw_value)
                    continue
                if isinstance(raw_value, float):
                    if math.isnan(raw_value):
                        event[str(key)] = "nan"
                    elif math.isinf(raw_value):
                        event[str(key)] = "inf" if raw_value > 0 else "-inf"
                    else:
                        event[str(key)] = float(raw_value)
                    continue
                event[str(key)] = str(raw_value)
        solver_trace_events.append(event)

    if solver_trace_enabled:
        _append_solver_trace_event(
            "run_start",
            payload={
                "modeq_side_channel": eq_backfill_modeq_fsr_side_channel,
                "commit_mode": eq_backfill_rho_resid_ar1_commit_mode,
            },
        )

    exit_code = 0
    semantics_assertions_enabled = _env_flag_enabled("FAIR_PY_ASSERT_FP_SEMANTICS")

    if enable_eq:
        eq_backfill_runtime_started = time.monotonic()
        eq_backfill_stop_reason = "not_started"
        eq_build_records_started = time.monotonic()
        eq_records = _build_eq_backfill_records(
            unfiltered_records,
            records,
            line_start=line_start,
            line_end=line_end,
            include_command_filter=include_command_filter,
            eq_filter_use_full_context=eq_filter_use_full_context,
            eq_specs=eq_specs,
        )
        eq_backfill_runtime_build_records_seconds += time.monotonic() - eq_build_records_started
        if max_steps is not None and executed_line_numbers:
            cutoff_line = max(int(line) for line in executed_line_numbers)
            eq_records = [record for record in eq_records if record.line_number <= cutoff_line]
        eq_backfill_record_count = sum(
            1 for record in eq_records if record.command in {FPCommand.EQ, FPCommand.LHS}
        )

        if eq_use_setupsolve:
            eq_setupsolve_started = time.monotonic()
            setupsolve = extract_setupsolve_config(eq_records)
            setupsolve_summary = _serialize_setupsolve_for_summary(setupsolve)
            eq_backfill_min_iters = setupsolve.miniters
            eq_backfill_max_iters = setupsolve.maxiters or 100
            eq_backfill_maxcheck = setupsolve.maxcheck
            eq_backfill_nomiss_required = setupsolve.nomiss
            eq_backfill_tolallabs = bool(setupsolve.tolallabs)
            eq_backfill_tol_mode = (
                "absolute" if eq_backfill_tolallabs else "relative_with_zero_absolute_fallback"
            )
            if setupsolve.dampall is not None:
                eq_backfill_dampall = float(setupsolve.dampall)
            if setupsolve.tolall is not None and eq_atol is None:
                eq_backfill_tol = float(setupsolve.tolall)
                eq_backfill_tol_source = "setupsolve_tolall"
            elif setupsolve.tolall is None and eq_atol is None:
                # Legacy default when TOLALL is omitted under SETUPSOLVE.
                eq_backfill_tol = 1e-3
                eq_backfill_tol_source = "setupsolve_default_tolall"
            (
                eq_backfill_damp_overrides,
                eq_backfill_tol_overrides,
                eq_backfill_tolabs_overrides,
                eq_backfill_override_issues,
            ) = _load_setupsolve_override_files(
                base_dir=config.legacy.fminput.parent,
                filedamp=setupsolve.filedamp,
                filetol=setupsolve.filetol,
                filetolabs=setupsolve.filetolabs,
            )
            issues.extend(eq_backfill_override_issues)
            eq_backfill_runtime_setupsolve_seconds += time.monotonic() - eq_setupsolve_started
        else:
            eq_backfill_min_iters = 1
            eq_backfill_max_iters = 1

        if eq_iters is not None:
            eq_backfill_min_iters = int(eq_iters)
            eq_backfill_max_iters = int(eq_iters)
        if eq_atol is not None:
            eq_backfill_tol = float(eq_atol)
            eq_backfill_tol_source = "cli_eq_atol"
            eq_backfill_tol_mode = "absolute"
        if eq_dampall is not None:
            eq_backfill_dampall = float(eq_dampall)
        eq_backfill_min_iters = max(1, int(eq_backfill_min_iters))
        eq_backfill_max_iters = max(eq_backfill_min_iters, int(eq_backfill_max_iters))
        (
            eq_backfill_min_iters,
            eq_backfill_max_iters,
            eq_backfill_auto_keyboard_stub_min_iters_enabled,
        ) = _resolve_auto_keyboard_min_iters(
            auto_stub_active=bool(eq_backfill_auto_keyboard_stub_active),
            eq_use_setupsolve=bool(eq_use_setupsolve),
            eq_iters=eq_iters,
            eq_backfill_min_iters=int(eq_backfill_min_iters),
            eq_backfill_max_iters=int(eq_backfill_max_iters),
        )
        if (
            eq_backfill_auto_keyboard_stub_min_iters_enabled
            and eq_backfill_auto_keyboard_stub_context_first_iter_enabled
        ):
            eq_backfill_period_sequential_context_assignments_first_iter_only = False
            eq_backfill_auto_keyboard_stub_context_first_iter_enabled = False
            eq_backfill_auto_keyboard_stub_context_first_iter_disabled_for_min_iters = True
        if eq_backfill_auto_keyboard_stub_min_iters_enabled:
            print(
                "Mini-run info: auto-raised keyboard SOLVE OUTSIDE iteration floor "
                f"to min_iters={eq_backfill_min_iters}, max_iters={eq_backfill_max_iters}"
            )
        if eq_backfill_auto_keyboard_stub_context_first_iter_disabled_for_min_iters:
            print(
                "Mini-run info: disabled auto context-first-iter replay for "
                "keyboard SOLVE OUTSIDE multi-iteration mode"
            )

        eq_specs_started = time.monotonic()
        specs = (
            eq_specs
            if eq_specs is not None
            else load_eq_specs_from_fmout(
                config.legacy.fmout if eq_coefs_fmout is None else Path(eq_coefs_fmout)
            )
        )
        eq_backfill_runtime_specs_seconds += time.monotonic() - eq_specs_started
        eq_backfill_specs_has_rho_terms = _eq_specs_have_rho_terms(specs)
        (
            eq_backfill_rho_aware,
            eq_backfill_rho_mode_auto_selected,
            eq_backfill_rho_mode_auto_reason,
        ) = _resolve_auto_keyboard_rho_aware_flag(
            auto_stub_active=bool(eq_backfill_auto_keyboard_stub_active),
            auto_stub_min_iters_enabled=bool(eq_backfill_auto_keyboard_stub_min_iters_enabled),
            eq_use_setupsolve=bool(eq_use_setupsolve),
            eq_backfill_specs_has_rho_terms=bool(eq_backfill_specs_has_rho_terms),
            eq_backfill_rho_aware=bool(eq_backfill_rho_aware),
            eq_backfill_rho_resid_ar1=bool(eq_backfill_rho_resid_ar1),
        )
        if eq_backfill_rho_mode_auto_selected:
            if eq_backfill_rho_mode_auto_reason == "setupsolve_rho_terms":
                auto_mode_message = (
                    "Mini-run info: auto-selected eq_rho_aware because eq_use_setupsolve "
                    "is enabled and equation specs contain RHO terms"
                )
            elif eq_backfill_rho_mode_auto_reason == "keyboard_stub_min_iters_rho_terms":
                auto_mode_message = (
                    "Mini-run info: auto-selected eq_rho_aware for keyboard SOLVE OUTSIDE "
                    "min-iter mode because equation specs contain RHO terms"
                )
            else:
                auto_mode_message = (
                    "Mini-run info: auto-selected eq_rho_aware for keyboard SOLVE OUTSIDE "
                    "because equation specs contain RHO terms"
                )
            print(auto_mode_message)
        if eq_use_setupsolve and eq_backfill_rho_resid_ar1:
            resid_warning = (
                "Mini-run warning: eq_rho_resid_ar1 under eq_use_setupsolve is "
                "parity-risky; prefer eq_rho_aware unless explicitly probing "
                "residual-carry behavior"
            )
            eq_backfill_rho_mode_warnings.append(resid_warning)
            print(resid_warning)
            if eq_backfill_rho_resid_ar1_carry_iterations:
                carry_warning = (
                    "Mini-run warning: eq_rho_resid_ar1_carry_iterations under "
                    "eq_use_setupsolve can introduce iteration-dependent state drift"
                )
                eq_backfill_rho_mode_warnings.append(carry_warning)
                print(carry_warning)
        seed_active = False
        if eq_seed == "all":
            seed_active = True
        elif eq_seed == "filtered":
            seed_active = _is_filtered_run(
                line_start=line_start,
                line_end=line_end,
                include_commands=include_commands,
                max_steps=max_steps,
            )

        working = output_frame
        solve_pre_reset_frame = working.copy(deep=True)
        active_solve_window = (
            solve_active_window if isinstance(solve_active_window, SampleWindow) else None
        )
        if seed_active:
            seed_targets = _collect_eq_seed_targets(eq_records, specs)
            seed_fmout_path = config.legacy.fmout if eq_seed_fmout is None else Path(eq_seed_fmout)
            working, seeded_report = _seed_eq_frame_from_fmout(
                working,
                fmout_path=seed_fmout_path,
                metric=eq_seed_metric,
                fill=eq_seed_fill,
                targets=seed_targets,
            )
            eq_seed_report.update(seeded_report)
            eq_seed_report["enabled"] = True
            eq_seed_report["mode"] = eq_seed
        else:
            eq_seed_report["status"] = "disabled"

        state_seed_report.update({
            "enabled": bool(eq_state_seed_pabev is not None),
            "status": "disabled" if eq_state_seed_pabev is None else "pending",
            "fill": str(eq_state_seed_fill),
            "pabev": str(eq_state_seed_pabev) if eq_state_seed_pabev is not None else None,
            "period": str(eq_state_seed_period) if eq_state_seed_period is not None else None,
        })
        if eq_state_seed_pabev is not None and eq_state_seed_period is not None:
            working, seeded_state_report = _seed_state_frame_from_pabev(
                working,
                pabev_path=Path(eq_state_seed_pabev),
                period=str(eq_state_seed_period),
                fill=str(eq_state_seed_fill),
            )
            state_seed_report.update(seeded_state_report)

        eq_window_prep_started = time.monotonic()
        eq_targets = _collect_eq_backfill_targets(eq_records, specs)
        eq_windows = _collect_eq_backfill_windows(eq_records)
        eq_windows_by_target = _collect_eq_backfill_windows_by_target(eq_records, specs)
        eq_backfill_windows_override: tuple[SampleWindow, ...] | None = None
        eq_metric_windows: tuple[tuple[str, str], ...] = eq_windows
        eq_nomiss_windows = eq_windows
        eq_nomiss_windows_by_target: dict[str, tuple[tuple[str, str], ...]] | None = (
            eq_windows_by_target
        )
        setupsolve_windows = (
            _collect_setupsolve_windows(eq_records) if eq_use_setupsolve else tuple()
        )
        if active_solve_window is not None:
            eq_backfill_windows_override = (
                SampleWindow(
                    start=str(active_solve_window.start),
                    end=str(active_solve_window.end),
                ),
            )
        elif setupsolve_windows:
            eq_backfill_windows_override = tuple(
                SampleWindow(start=start, end=end) for start, end in setupsolve_windows
            )
        if eq_backfill_nomiss_required and setupsolve_windows:
            eq_nomiss_windows = setupsolve_windows
            eq_nomiss_windows_by_target = None
        if eq_backfill_windows_override:
            eq_metric_windows = tuple(
                (str(window.start), str(window.end)) for window in eq_backfill_windows_override
            )
        eq_nomiss_exclusions = _build_nomiss_boundary_exclusions(
            frame=working,
            targets=eq_targets,
            specs=specs,
            records=eq_records,
            windows=eq_nomiss_windows,
            windows_by_target=eq_nomiss_windows_by_target,
        )
        eq_metric_targets: tuple[str, ...] = eq_targets
        keyboard_filtered_targets: tuple[str, ...] = tuple()
        keyboard_matched_targets: tuple[str, ...] = tuple()
        solve_keyboard_targets_active = bool(
            solve_active_filevar is not None
            and str(solve_active_filevar).upper() == "KEYBOARD"
            and solve_active_keyboard_targets_set
        )
        if solve_keyboard_targets_active:
            filtered_targets: list[str] = []
            eq_target_name_pool: set[str] = set()
            solve_target_name_pool: set[str] = set()
            for target in eq_targets:
                target_names = {
                    expanded.upper() for expanded in _expand_dependency_names(str(target))
                }
                eq_target_name_pool.update(target_names)
                solve_target_name_pool.update(target_names)
                if target_names.isdisjoint(solve_active_keyboard_targets_set):
                    continue
                filtered_targets.append(str(target))
            if solve_active_outside:
                for target in _collect_outside_seed_targets(eq_records, specs):
                    solve_target_name_pool.update(
                        expanded.upper() for expanded in _expand_dependency_names(str(target))
                    )
            keyboard_filtered_targets = tuple(filtered_targets)
            eq_metric_targets = keyboard_filtered_targets
            eq_backfill_keyboard_targets_unmatched = tuple(
                target
                for target in solve_active_keyboard_targets_normalized
                if target not in solve_target_name_pool
            )
            keyboard_matched_targets = tuple(
                target
                for target in solve_active_keyboard_targets_normalized
                if target in solve_target_name_pool
            )
            if not keyboard_filtered_targets and keyboard_matched_targets:
                # When KEYBOARD targets exist only in assignment/outside scope (not EQ targets),
                # use KEYBOARD-matched series for convergence/residual checked-target metrics.
                eq_metric_targets = keyboard_matched_targets
            keyboard_has_match = bool(
                solve_active_keyboard_targets_normalized
                and len(eq_backfill_keyboard_targets_unmatched)
                < len(solve_active_keyboard_targets_normalized)
            )
            eq_backfill_keyboard_targets_missing = bool(
                solve_active_outside and not keyboard_has_match
            )
        # In parity mode, convergence must be assessed against the full solve target set.
        # If SOLVE uses FILEVAR=KEYBOARD, the solve target set is the KEYBOARD list.
        # Otherwise, it is the full EQ target set.
        preset_label = str(eq_flags_preset_label or "").strip().lower()
        if preset_label == "parity" and eq_targets and not solve_keyboard_targets_active:
            eq_metric_targets = eq_targets
        if eq_backfill_maxcheck is not None and eq_backfill_maxcheck > 0:
            eq_metric_targets = eq_metric_targets[:eq_backfill_maxcheck]
        if not eq_metric_targets and eq_targets and not eq_backfill_keyboard_targets_missing:
            eq_metric_targets = eq_targets
        eq_backfill_checked_targets = eq_metric_targets
        eq_backfill_all_targets = eq_targets if eq_targets else eq_metric_targets
        eq_nomiss_targets = eq_targets
        if solve_keyboard_targets_active and keyboard_filtered_targets:
            eq_nomiss_targets = keyboard_filtered_targets
        structural_assignment_targets: frozenset[str] = frozenset()
        if (
            eq_backfill_period_sequential_context_assignments_first_iter_only
            and solve_keyboard_targets_active
        ):
            structural_assignment_targets = solve_active_keyboard_targets_set
        outside_seed_targets = tuple()
        if solve_active_outside:
            if solve_keyboard_targets_active and preset_label != "parity":
                outside_seed_targets = solve_active_keyboard_targets_normalized
            else:
                # FP's OUTSIDE initialization seeds a broad set of endogenous/current
                # values at the solve-window boundary. When FILEVAR=KEYBOARD is used,
                # the list is primarily a convergence/check selection, not a seeding
                # limiter. In parity mode, always seed the structural target set.
                outside_seed_targets = _collect_outside_seed_targets(eq_records, specs)
                if not outside_seed_targets:
                    outside_seed_targets = (
                        eq_backfill_all_targets if eq_backfill_all_targets else eq_targets
                    )
        if solve_keyboard_targets_active and eq_backfill_auto_keyboard_stub_active:
            eq_backfill_keyboard_stub_lag_probe_targets = _build_keyboard_stub_lag_probe_targets(
                eq_records,
                keyboard_targets=solve_active_keyboard_targets_normalized,
            )
        outside_seed_windows = tuple()
        if solve_active_outside:
            if active_solve_window is not None:
                outside_seed_windows = (
                    (
                        str(active_solve_window.start),
                        str(active_solve_window.end),
                    ),
                )
            elif eq_backfill_windows_override:
                outside_seed_windows = tuple(
                    (str(window.start), str(window.end)) for window in eq_backfill_windows_override
                )
            elif eq_windows:
                outside_seed_windows = eq_windows
        eq_had_failures = False
        strict_missing_inputs = include_command_filter != {FPCommand.EQ}
        strict_missing_assignments = strict_missing_inputs
        rho_resid_iteration_seed: dict[object, object] = {}
        focus_trace_max_events = (
            int(eq_rho_resid_ar1_focus_trace_spec.max_events)
            if eq_rho_resid_ar1_focus_trace_spec is not None
            else 0
        )
        eq_backfill_runtime_window_prep_seconds += time.monotonic() - eq_window_prep_started

        def _append_focus_event_from_cli(event: dict[str, object]) -> None:
            nonlocal eq_backfill_rho_resid_ar1_trace_focus_seq
            nonlocal eq_backfill_rho_resid_ar1_trace_focus_events_truncated_by_limit
            if eq_rho_resid_ar1_focus_trace_spec is None:
                return
            if len(eq_backfill_rho_resid_ar1_trace_focus_events) >= focus_trace_max_events:
                eq_backfill_rho_resid_ar1_trace_focus_events_truncated_by_limit = True
                return
            stage_name = str(event.get("stage", ""))
            if stage_name not in eq_rho_resid_ar1_focus_trace_spec.stages:
                return
            eq_backfill_rho_resid_ar1_trace_focus_seq += 1
            payload = dict(event)
            payload["seq"] = int(eq_backfill_rho_resid_ar1_trace_focus_seq)
            eq_backfill_rho_resid_ar1_trace_focus_events.append(payload)

        if eq_backfill_keyboard_targets_missing:
            # Legacy fp.exe semantics: `FILEVAR=KEYBOARD` provides a *checked-target*
            # list (convergence reporting), not a required equation-target list.
            #
            # When no EQ targets match the KEYBOARD list (common in real decks where
            # the check variables are identities/derived series), continue solving
            # against the structural target set instead of aborting the EQ backfill.
            unmatched_display = (
                list(eq_backfill_keyboard_targets_unmatched)
                if eq_backfill_keyboard_targets_unmatched
                else list(solve_active_keyboard_targets_normalized)
            )
            issues.append({
                "line": solve_active_line,
                "statement": "SOLVE FILEVAR=KEYBOARD",
                "error": (
                    "SOLVE FILEVAR=KEYBOARD targets did not match any EQ targets; "
                    "continuing with structural targets. Unmatched: "
                    f"{unmatched_display}"
                ),
            })
            eq_backfill_keyboard_targets_missing = False
        # Per-period Gauss-Seidel iteration: converge period T before
        # advancing to T+1, matching FP's SOLA subroutine behavior.
        # "auto" enables when period_sequential + SOLVE window present.
        # "on" forces it, "off" disables it (legacy full-window sweep).
        period_scoped_windows: list[tuple[tuple[str, str], ...]] = [tuple()]
        period_scoped_mode = str(eq_period_scoped).strip().lower()
        if period_scoped_mode == "auto":
            period_scoped_requested = (
                eq_backfill_period_sequential
                and active_solve_window is not None
                and eq_use_setupsolve
            )
        elif period_scoped_mode == "on":
            period_scoped_requested = True
        else:
            period_scoped_requested = False
        # Also respect the legacy env var as an override.
        if _env_flag_enabled("FAIR_PY_SOLVE_PERIOD_SCOPED"):
            period_scoped_requested = True
        if eq_period_scoped_range_keys is not None:
            period_scoped_requested = True
        period_scoped_enabled = False
        if eq_period_scoped_range_keys is not None and active_solve_window is None:
            print("Mini-run option error: eq_period_scoped_range requires an active SOLVE window")
            return 1
        if period_scoped_requested:
            if eq_backfill_period_sequential and active_solve_window is not None:
                start_key = _period_key(str(active_solve_window.start))
                end_key = _period_key(str(active_solve_window.end))
                if start_key is not None and end_key is not None and start_key <= end_key:
                    scoped_start_key = start_key
                    scoped_end_key = end_key
                    if eq_period_scoped_range_keys is not None:
                        requested_start, requested_end = eq_period_scoped_range_keys
                        if requested_end < start_key or requested_start > end_key:
                            scoped_start, scoped_end = (
                                eq_period_scoped_range_parsed
                                if eq_period_scoped_range_parsed is not None
                                else ("", "")
                            )
                            print(
                                "Mini-run option error: eq_period_scoped_range "
                                f"({scoped_start}..{scoped_end}) does not overlap active "
                                f"SOLVE window ({active_solve_window.start}..{active_solve_window.end})"
                            )
                            return 1
                        scoped_start_key = max(start_key, requested_start)
                        scoped_end_key = min(end_key, requested_end)
                    solve_periods = [
                        str(period)
                        for period in working.index
                        if (
                            (key := _period_key(period)) is not None
                            and scoped_start_key <= key <= scoped_end_key
                        )
                    ]
                    if solve_periods:
                        period_scoped_enabled = True
                        period_scoped_windows = [((period, period),) for period in solve_periods]

        eq_iteration_phase_started = time.monotonic()
        for solve_scope_windows in period_scoped_windows:
            # Track iterations per solve-period (matching fp.exe's "ITERS=..." behavior).
            eq_backfill_iterations = 0
            if not eq_backfill_keyboard_targets_missing:
                eq_backfill_converged = False
                eq_backfill_stop_reason = "not_started"
                eq_backfill_min_abs_delta = 0.0
                eq_backfill_last_abs_delta = 0.0
                eq_backfill_delta_ratio = 0.0
                eq_backfill_diverged = False
            scope_max_iters = eq_backfill_max_iters
            # In period-scoped mode, MAXITERS (default 100) is the iteration
            # cap per period.  MINITERS is a minimum: convergence is only
            # checked once iteration >= eq_backfill_min_iters (see the
            # convergence gate at the bottom of this loop).
            iteration_windows_override = eq_backfill_windows_override
            iteration_metric_windows = eq_metric_windows
            iteration_outside_seed_windows = outside_seed_windows
            if solve_scope_windows:
                iteration_windows_override = tuple(
                    SampleWindow(start, end) for start, end in solve_scope_windows
                )
                iteration_metric_windows = solve_scope_windows
                iteration_outside_seed_windows = solve_scope_windows
            if (
                eq_backfill_keyboard_stub_lag_probe_period is None
                and solve_scope_windows
                and eq_backfill_keyboard_stub_lag_probe_targets
            ):
                eq_backfill_keyboard_stub_lag_probe_period = str(solve_scope_windows[0][0])

            for iteration_index in range(
                0 if eq_backfill_keyboard_targets_missing else scope_max_iters
            ):
                iteration = iteration_index + 1
                before = working.copy(deep=True)
                probe_before_values: dict[str, float | None] | None = None
                if (
                    eq_backfill_keyboard_stub_lag_probe_period is not None
                    and eq_backfill_keyboard_stub_lag_probe_targets
                    and solve_scope_windows
                    and str(solve_scope_windows[0][0])
                    == eq_backfill_keyboard_stub_lag_probe_period
                    and iteration <= 2
                ):
                    probe_before_values = _snapshot_probe_values(
                        before,
                        period=eq_backfill_keyboard_stub_lag_probe_period,
                        targets=eq_backfill_keyboard_stub_lag_probe_targets,
                    )
                if solve_active_outside and iteration == 1:
                    pre_seed_stats = _collect_outside_seed_stats(
                        before,
                        targets=outside_seed_targets,
                        windows=iteration_outside_seed_windows,
                    )
                    solve_outside_seed_inspected_cells += int(pre_seed_stats["inspected_cells"])
                    solve_outside_seed_candidate_cells += int(pre_seed_stats["candidate_cells"])
                    before, seeded_cells = _apply_outside_seed_frame(
                        before,
                        targets=outside_seed_targets,
                        windows=iteration_outside_seed_windows,
                    )
                    solve_outside_seeded_cells += int(seeded_cells)
                iteration_assignment_targets = (
                    structural_assignment_targets
                    if (
                        eq_backfill_period_sequential_context_assignments_first_iter_only
                        and iteration > 1
                    )
                    else frozenset()
                )
                iteration_eq_records = _records_for_eq_iteration(
                    eq_records,
                    iteration=iteration,
                    context_assignments_first_iter_only=(
                        eq_backfill_period_sequential_context_assignments_first_iter_only
                    ),
                    context_keep_create_all_iters=(
                        eq_backfill_period_sequential_context_keep_create_all_iters
                    ),
                    retain_assignment_targets=iteration_assignment_targets,
                )
                effective_rho_resid_ar1 = _resolve_rho_resid_ar1_enabled_for_iteration(
                    eq_backfill_rho_resid_ar1,
                    iteration=iteration,
                    start_iter=eq_backfill_rho_resid_ar1_start_iter,
                    disable_iter=eq_backfill_rho_resid_ar1_disable_iter,
                )
                eq_backfill_rho_resid_ar1_effective_last = bool(effective_rho_resid_ar1)
                effective_la257_update_rule = _resolve_la257_update_rule_for_iteration(
                    eq_backfill_rho_resid_ar1_la257_update_rule,
                    iteration=iteration,
                    start_iter=eq_backfill_rho_resid_ar1_la257_start_iter,
                    disable_iter=eq_backfill_rho_resid_ar1_la257_disable_iter,
                )
                if not effective_rho_resid_ar1:
                    effective_la257_update_rule = "legacy"
                eq_backfill_rho_resid_ar1_la257_update_rule_effective_last = (
                    effective_la257_update_rule
                )
                backfill_kwargs: dict[str, object] = {"on_error": on_error}
                if not strict_missing_inputs:
                    backfill_kwargs["strict_missing_inputs"] = False
                if not strict_missing_assignments:
                    backfill_kwargs["strict_missing_assignments"] = False
                backfill_kwargs["period_sequential_eq_commit_quantize"] = (
                    str(eq_quantize_eq_commit).strip().lower()
                )
                backfill_kwargs["period_sequential_eq_eval_precision"] = (
                    str(eq_eval_precision).strip().lower()
                )
                backfill_kwargs["period_sequential_eq_term_order"] = (
                    str(eq_term_order).strip().lower()
                )
                backfill_kwargs["period_sequential_eq_read_mode"] = (
                    str(eq_eq_read_mode).strip().lower()
                )
                backfill_kwargs["period_sequential_assignment_math_backend"] = (
                    str(eq_math_backend).strip().lower()
                )
                rho_resid_iteration_seed_before_iter: dict[object, object] = {}
                if eq_backfill_rho_resid_ar1_carry_iterations:
                    rho_resid_iteration_seed_before_iter = dict(rho_resid_iteration_seed)
                if eq_backfill_rho_resid_ar1_carry_iterations and rho_resid_iteration_seed:
                    backfill_kwargs["rho_resid_iteration_seed"] = dict(rho_resid_iteration_seed)
                iteration_focus_spec: Ar1TraceFocusSpec | None = None
                if eq_rho_resid_ar1_focus_trace_spec is not None and effective_rho_resid_ar1:
                    remaining_focus_events = max(
                        0,
                        focus_trace_max_events - len(eq_backfill_rho_resid_ar1_trace_focus_events),
                    )
                    if remaining_focus_events > 0:
                        iteration_focus_spec = Ar1TraceFocusSpec(
                            targets=eq_rho_resid_ar1_focus_trace_spec.targets,
                            periods=eq_rho_resid_ar1_focus_trace_spec.periods,
                            iter_min=eq_rho_resid_ar1_focus_trace_spec.iter_min,
                            iter_max=eq_rho_resid_ar1_focus_trace_spec.iter_max,
                            max_events=remaining_focus_events,
                            stages=eq_rho_resid_ar1_focus_trace_spec.stages,
                        )
                    else:
                        eq_backfill_rho_resid_ar1_trace_focus_events_truncated_by_limit = True
                eq_apply_started = time.monotonic()
                eq_backfill_result = apply_eq_backfill(
                    iteration_eq_records,
                    before,
                    specs,
                    period_sequential=eq_backfill_period_sequential,
                    period_sequential_fp_pass_order=bool(eq_period_sequential_fp_pass_order),
                    period_sequential_all_assignments=eq_backfill_period_sequential_all_assignments,
                    period_sequential_defer_create=eq_backfill_period_sequential_defer_create,
                    period_sequential_context_replay_command_time_smpl=(
                        eq_backfill_period_sequential_context_replay_command_time_smpl
                    ),
                    period_sequential_context_replay_command_time_smpl_clip_to_solve_window=(
                        eq_backfill_period_sequential_context_replay_command_time_smpl_clip_to_solve_window
                    ),
                    period_sequential_context_retain_overlap_only=(
                        eq_backfill_period_sequential_context_retain_overlap_only
                    ),
                    period_sequential_context_replay_trace=(
                        eq_backfill_period_sequential_context_replay_trace
                    ),
                    period_sequential_context_replay_trace_max_events=(
                        eq_backfill_period_sequential_context_replay_trace_max_events
                    ),
                    period_sequential_context_replay_trace_targets=frozenset(
                        str(target).upper()
                        for target in eq_backfill_period_sequential_context_replay_trace_targets
                    ),
                    period_sequential_context_replay_trace_periods=frozenset(
                        str(period)
                        for period in eq_backfill_period_sequential_context_replay_trace_periods
                    ),
                    period_sequential_assignment_targets=iteration_assignment_targets,
                    windows_override=iteration_windows_override,
                    modeq_fsr_side_channel_mode=eq_backfill_modeq_fsr_side_channel,
                    modeq_fsr_side_channel_max_events=(
                        eq_backfill_modeq_fsr_side_channel_effective_limit
                    ),
                    rho_aware=eq_backfill_rho_aware,
                    rho_resid_ar1=effective_rho_resid_ar1,
                    rho_resid_iteration_seed_lag=eq_backfill_rho_resid_ar1_seed_lag,
                    rho_resid_iteration_seed_mode=eq_backfill_rho_resid_ar1_seed_mode,
                    rho_resid_boundary_reset=eq_backfill_rho_resid_ar1_boundary_reset,
                    rho_resid_carry_lag=eq_backfill_rho_resid_ar1_carry_lag,
                    rho_resid_carry_damp=eq_backfill_rho_resid_ar1_carry_damp,
                    rho_resid_carry_damp_mode=eq_backfill_rho_resid_ar1_carry_damp_mode,
                    rho_resid_carry_multipass=eq_backfill_rho_resid_ar1_carry_multipass,
                    rho_resid_lag_gating=eq_backfill_rho_resid_ar1_lag_gating,
                    rho_resid_update_source=eq_backfill_rho_resid_ar1_update_source,
                    rho_resid_la257_update_rule=effective_la257_update_rule,
                    rho_resid_la257_fortran_cycle_carry_style=(
                        eq_backfill_rho_resid_ar1_la257_fortran_cycle_carry_style
                    ),
                    rho_resid_la257_u_phase_max_gain=(
                        eq_backfill_rho_resid_ar1_la257_u_phase_max_gain
                    ),
                    rho_resid_la257_u_phase_max_gain_mode=(
                        eq_backfill_rho_resid_ar1_la257_u_phase_max_gain_mode
                    ),
                    rho_resid_la257_staged_lifecycle=(
                        eq_backfill_rho_resid_ar1_la257_lifecycle_staging
                    ),
                    rho_resid_commit_after_check=(eq_backfill_rho_resid_ar1_commit_after_check),
                    rho_resid_la257_u_phase_lines=(eq_backfill_rho_resid_ar1_la257_u_phase_lines),
                    rho_resid_trace_lines=eq_backfill_rho_resid_ar1_trace_lines,
                    rho_resid_trace_event_limit=eq_backfill_rho_resid_ar1_trace_event_limit,
                    rho_resid_trace_iteration=int(iteration),
                    rho_resid_trace_focus_spec=iteration_focus_spec,
                    rho_resid_fpe_trap=eq_backfill_rho_resid_ar1_fpe_trap,
                    eval_context=eval_context,
                    **backfill_kwargs,
                )
                eq_backfill_runtime_iteration_apply_seconds += time.monotonic() - eq_apply_started
                eq_backfill_runtime_iteration_apply_calls += 1
                iteration_seed_candidate: dict[object, object] | None = None
                if eq_backfill_rho_resid_ar1_carry_iterations:
                    if eq_backfill_rho_resid_ar1_seed_mode == "positioned":
                        seed_map: dict[object, object] = dict(
                            _coerce_backfill_rho_resid_iteration_state_positioned_keyed(
                                eq_backfill_result
                            )
                        )
                        positioned = (
                            eq_backfill_result.rho_resid_iteration_state_positioned
                            if hasattr(eq_backfill_result, "rho_resid_iteration_state_positioned")
                            else eq_backfill_result.get("rho_resid_iteration_state_positioned")
                            if isinstance(eq_backfill_result, dict)
                            else None
                        )
                        if isinstance(positioned, (list, tuple, set)):
                            for entry in positioned:
                                if not isinstance(entry, (list, tuple)) or len(entry) < 3:
                                    continue
                                try:
                                    line_key = int(entry[0])
                                    residual = float(entry[1])
                                    residual_position = int(entry[2])
                                except (TypeError, ValueError):
                                    continue
                                if not math.isfinite(residual):
                                    continue
                                if residual_position < 0:
                                    continue
                                seed_map.setdefault(line_key, (residual, residual_position))
                        if not seed_map:
                            # Backward-compatible fallback for older/partial payloads
                            # that only expose legacy residual carry state.
                            seed_map = dict(
                                _coerce_backfill_rho_resid_iteration_state(eq_backfill_result)
                            )
                        iteration_seed_candidate = dict(seed_map)
                    else:
                        seed_map_legacy: dict[object, object] = dict(
                            _coerce_backfill_rho_resid_iteration_state_keyed(eq_backfill_result)
                        )
                        for key, residual in _coerce_backfill_rho_resid_iteration_state(
                            eq_backfill_result
                        ).items():
                            seed_map_legacy.setdefault(key, residual)
                        iteration_seed_candidate = dict(seed_map_legacy)
                    seed_state_for_report = (
                        iteration_seed_candidate
                        if iteration_seed_candidate is not None
                        else rho_resid_iteration_seed
                    )
                    eq_backfill_rho_resid_iteration_state_size = len(seed_state_for_report)
                    eq_backfill_rho_resid_iteration_state_top = (
                        _summarize_rho_resid_iteration_seed(
                            seed_state_for_report,
                            top=5,
                        )
                    )
                    eq_backfill_rho_resid_iteration_trace_last = _trace_rho_resid_iteration_seed(
                        seed_state_for_report,
                        lines=eq_backfill_rho_resid_ar1_trace_lines,
                    )
                residual_probe_seed: dict[object, object] | None = None
                probe_seed_source = (
                    rho_resid_iteration_seed_before_iter
                    if eq_backfill_rho_resid_ar1_commit_after_check
                    else (
                        iteration_seed_candidate
                        if iteration_seed_candidate is not None
                        else rho_resid_iteration_seed
                    )
                )
                if (
                    effective_rho_resid_ar1
                    and eq_backfill_rho_resid_ar1_carry_iterations
                    and probe_seed_source
                    and eq_backfill_rho_resid_ar1_residual_probe_seed_mode == "carry"
                ):
                    residual_probe_seed = dict(probe_seed_source)
                trace_events = _coerce_backfill_rho_resid_trace_events(eq_backfill_result)
                eq_backfill_rho_resid_ar1_trace_event_count = len(trace_events)
                eq_backfill_rho_resid_ar1_trace_events_total_count = len(trace_events)
                eq_backfill_rho_resid_ar1_trace_events = _slice_trace_events_for_report(
                    trace_events,
                    head=eq_backfill_rho_resid_ar1_trace_events_sample_head_count,
                    tail=eq_backfill_rho_resid_ar1_trace_events_sample_tail_count,
                )
                eq_backfill_rho_resid_ar1_trace_events_serialized_count = len(
                    eq_backfill_rho_resid_ar1_trace_events
                )
                eq_backfill_rho_resid_ar1_trace_events_truncated = bool(
                    eq_backfill_result.get("rho_resid_ar1_trace_events_truncated")
                    if isinstance(eq_backfill_result, dict)
                    else getattr(eq_backfill_result, "rho_resid_ar1_trace_events_truncated", False)
                )
                eq_backfill_rho_resid_ar1_trace_events_truncated_by_event_limit = bool(
                    eq_backfill_rho_resid_ar1_trace_events_truncated
                )
                eq_backfill_rho_resid_ar1_trace_events_sampled = (
                    eq_backfill_rho_resid_ar1_trace_events_serialized_count
                    < eq_backfill_rho_resid_ar1_trace_events_total_count
                )
                eq_backfill_rho_resid_ar1_trace_event_kind_counts = (
                    _summarize_trace_events_by_field(trace_events, "kind")
                )
                eq_backfill_rho_resid_ar1_trace_event_phase_counts = (
                    _summarize_trace_events_by_field(trace_events, "phase")
                )
                eq_backfill_rho_resid_ar1_trace_event_pass_counts = (
                    _summarize_trace_events_by_pass(trace_events)
                )
                eq_backfill_rho_resid_ar1_trace_event_carry_reject_reason_counts = (
                    _summarize_trace_events_by_field(trace_events, "carry_reject_reason")
                )
                focus_trace_events = _coerce_backfill_rho_resid_focus_trace_events(
                    eq_backfill_result
                )
                for focus_event in focus_trace_events:
                    _append_focus_event_from_cli(focus_event)
                if _coerce_backfill_rho_resid_focus_trace_events_truncated(eq_backfill_result):
                    eq_backfill_rho_resid_ar1_trace_focus_events_truncated_by_limit = True
                context_replay_trace_events = _coerce_backfill_context_replay_trace_events(
                    eq_backfill_result
                )
                if context_replay_trace_events:
                    for event in context_replay_trace_events:
                        payload = dict(event)
                        payload["iter"] = int(iteration)
                        eq_backfill_context_replay_trace_events.append(payload)
                if _coerce_backfill_context_replay_trace_events_truncated(eq_backfill_result):
                    eq_backfill_context_replay_trace_events_truncated = True
                if solver_trace_enabled:
                    resid_eval_focus_events = [
                        event
                        for event in focus_trace_events
                        if str(event.get("stage", "")) == "resid_eval"
                    ]
                    if resid_eval_focus_events:
                        for focus_event in resid_eval_focus_events:
                            _append_solver_trace_event(
                                "resid_eval",
                                iteration=int(iteration),
                                period=focus_event.get("period"),
                                target=focus_event.get("target"),
                                payload={
                                    "line": focus_event.get("line"),
                                    "occurrence": focus_event.get("occurrence"),
                                    "resid": focus_event.get("resid"),
                                    "structural": focus_event.get("structural"),
                                    "result": focus_event.get("result"),
                                    "update_source": focus_event.get("update_source"),
                                    "commit_mode": focus_event.get("commit_mode"),
                                },
                            )
                    else:
                        resid_eval_kinds = {"solve_and_update", "solve_deferred", "state_update"}
                        resid_eval_count = sum(
                            1
                            for event in trace_events
                            if str(event.get("kind", "")) in resid_eval_kinds
                        )
                        _append_solver_trace_event(
                            "resid_eval",
                            iteration=int(iteration),
                            payload={
                                "event_count": int(resid_eval_count),
                                "commit_mode": eq_backfill_rho_resid_ar1_commit_mode,
                                "source": "aggregated",
                            },
                        )
                commit_like_event_count = sum(
                    1
                    for event in trace_events
                    if str(event.get("kind", ""))
                    in {"solve_and_update", "state_update", "state_update_nonfinite"}
                )
                if (
                    solver_trace_enabled
                    and effective_rho_resid_ar1
                    and not eq_backfill_rho_resid_ar1_commit_after_check
                ):
                    _append_solver_trace_event(
                        "commit_rho_resid_ar1",
                        iteration=int(iteration),
                        payload={
                            "commit_mode": eq_backfill_rho_resid_ar1_commit_mode,
                            "carry_iterations": bool(eq_backfill_rho_resid_ar1_carry_iterations),
                            "state_size": int(eq_backfill_rho_resid_iteration_state_size),
                            "state_seed_candidate": bool(iteration_seed_candidate is not None),
                            "event_count": int(commit_like_event_count),
                        },
                    )
                working = _coerce_backfill_frame(eq_backfill_result, before)
                working = _apply_damping(
                    before,
                    working,
                    eq_backfill_dampall,
                    overrides=eq_backfill_damp_overrides,
                )
                # FP semantics: after damping, re-evaluate GENR transforms
                # (ZFYX + TURBO(2)) before saving YZ for the next iteration.
                # This ensures derived quantities reflect damped values.
                effective_damp = eq_backfill_dampall
                if eq_backfill_damp_overrides:
                    effective_damp = min(
                        eq_backfill_dampall,
                        min(float(v) for v in eq_backfill_damp_overrides.values())
                        if eq_backfill_damp_overrides
                        else eq_backfill_dampall,
                    )
                if effective_damp != 1.0:
                    genr_records = [r for r in iteration_eq_records if r.command == FPCommand.GENR]
                    if genr_records:
                        genr_result = apply_eq_backfill(
                            genr_records,
                            working,
                            specs,
                            period_sequential=eq_backfill_period_sequential,
                            period_sequential_fp_pass_order=False,
                            windows_override=iteration_windows_override,
                            on_error=on_error,
                            strict_missing_inputs=strict_missing_inputs,
                            strict_missing_assignments=strict_missing_assignments,
                            eval_context=eval_context,
                        )
                        working = _coerce_backfill_frame(genr_result, working)
                if (
                    solve_active_outside
                    and solve_keyboard_targets_active
                    and eq_backfill_auto_keyboard_stub_active
                ):
                    post_seed_stats = _collect_outside_seed_stats(
                        working,
                        targets=outside_seed_targets,
                        windows=iteration_outside_seed_windows,
                    )
                    solve_outside_seed_inspected_cells += int(post_seed_stats["inspected_cells"])
                    solve_outside_seed_candidate_cells += int(post_seed_stats["candidate_cells"])
                    eq_backfill_outside_post_seed_inspected_cells += int(
                        post_seed_stats["inspected_cells"]
                    )
                    eq_backfill_outside_post_seed_candidate_cells += int(
                        post_seed_stats["candidate_cells"]
                    )
                    working, post_seeded_cells = _apply_outside_seed_frame(
                        working,
                        targets=outside_seed_targets,
                        windows=iteration_outside_seed_windows,
                    )
                    solve_outside_seeded_cells += int(post_seeded_cells)
                    eq_backfill_outside_post_seed_cells += int(post_seeded_cells)
                if (
                    probe_before_values is not None
                    and eq_backfill_keyboard_stub_lag_probe_period is not None
                ):
                    probe_after_values = _snapshot_probe_values(
                        working,
                        period=eq_backfill_keyboard_stub_lag_probe_period,
                        targets=eq_backfill_keyboard_stub_lag_probe_targets,
                    )
                    probe_delta_values: dict[str, float | None] = {}
                    for target in eq_backfill_keyboard_stub_lag_probe_targets:
                        before_value = probe_before_values.get(target)
                        after_value = probe_after_values.get(target)
                        if before_value is None or after_value is None:
                            probe_delta_values[target] = None
                            continue
                        delta = float(after_value) - float(before_value)
                        probe_delta_values[target] = delta if math.isfinite(delta) else None
                    eq_backfill_keyboard_stub_lag_probe_events.append({
                        "period": eq_backfill_keyboard_stub_lag_probe_period,
                        "iteration": int(iteration),
                        "before": probe_before_values,
                        "after": probe_after_values,
                        "delta": probe_delta_values,
                    })
                if eq_backfill_quantize_store != "off":
                    working, iter_quantized_cells = _apply_eq_store_quantization(
                        working,
                        targets=eq_targets,
                        windows=iteration_metric_windows,
                        mode=eq_backfill_quantize_store,
                    )
                    eq_backfill_quantize_store_cells += int(iter_quantized_cells)
                iter_applied = _coerce_backfill_int(eq_backfill_result, "applied")
                eq_backfill_applied += iter_applied
                eq_backfill_modeq_fsr_updates += _coerce_backfill_int(
                    eq_backfill_result,
                    "modeq_fsr_updates",
                )
                eq_backfill_modeq_fsr_active_equation_count = _coerce_backfill_int(
                    eq_backfill_result,
                    "modeq_fsr_active_equation_count",
                )
                eq_backfill_modeq_fsr_active_term_count = _coerce_backfill_int(
                    eq_backfill_result,
                    "modeq_fsr_active_term_count",
                )
                eq_backfill_eq_fsr_skipped += _coerce_backfill_int(
                    eq_backfill_result,
                    "eq_fsr_skipped",
                )
                iter_side_channel_mode = (
                    eq_backfill_result.get("modeq_fsr_side_channel_mode")
                    if isinstance(eq_backfill_result, dict)
                    else getattr(eq_backfill_result, "modeq_fsr_side_channel_mode", None)
                )
                if isinstance(iter_side_channel_mode, str) and iter_side_channel_mode:
                    eq_backfill_modeq_fsr_side_channel = iter_side_channel_mode
                iter_side_channel_status = (
                    eq_backfill_result.get("modeq_fsr_side_channel_status")
                    if isinstance(eq_backfill_result, dict)
                    else getattr(eq_backfill_result, "modeq_fsr_side_channel_status", None)
                )
                if isinstance(iter_side_channel_status, str) and iter_side_channel_status:
                    eq_backfill_modeq_fsr_side_channel_status = iter_side_channel_status
                iter_side_channel_limit = (
                    eq_backfill_result.get("modeq_fsr_side_channel_effective_limit")
                    if isinstance(eq_backfill_result, dict)
                    else getattr(
                        eq_backfill_result, "modeq_fsr_side_channel_effective_limit", None
                    )
                )
                try:
                    if iter_side_channel_limit is not None:
                        eq_backfill_modeq_fsr_side_channel_effective_limit = max(
                            0, int(iter_side_channel_limit)
                        )
                except (TypeError, ValueError):
                    pass
                iter_side_channel_events = _coerce_backfill_modeq_fsr_side_channel_events(
                    eq_backfill_result
                )
                if iter_side_channel_events:
                    if eq_backfill_modeq_fsr_side_channel_effective_limit <= 0:
                        eq_backfill_modeq_fsr_side_channel_events_truncated = True
                    else:
                        remaining = eq_backfill_modeq_fsr_side_channel_effective_limit - len(
                            eq_backfill_modeq_fsr_side_channel_events
                        )
                        if remaining <= 0:
                            eq_backfill_modeq_fsr_side_channel_events_truncated = True
                        else:
                            eq_backfill_modeq_fsr_side_channel_events.extend(
                                iter_side_channel_events[:remaining]
                            )
                            if len(iter_side_channel_events) > remaining:
                                eq_backfill_modeq_fsr_side_channel_events_truncated = True
                iter_side_channel_events_truncated = bool(
                    eq_backfill_result.get("modeq_fsr_side_channel_events_truncated")
                    if isinstance(eq_backfill_result, dict)
                    else getattr(
                        eq_backfill_result,
                        "modeq_fsr_side_channel_events_truncated",
                        False,
                    )
                )
                if iter_side_channel_events_truncated:
                    eq_backfill_modeq_fsr_side_channel_events_truncated = True
                eq_backfill_modeq_fsr_side_channel_breach_count += _coerce_backfill_int(
                    eq_backfill_result,
                    "modeq_fsr_side_channel_breach_count",
                )
                eq_backfill_modeq_fsr_side_channel_mutation_count += _coerce_backfill_int(
                    eq_backfill_result,
                    "modeq_fsr_side_channel_mutation_count",
                )
                if eq_backfill_modeq_fsr_side_channel_first_breach is None:
                    iter_first_breach = (
                        eq_backfill_result.get("modeq_fsr_side_channel_first_breach")
                        if isinstance(eq_backfill_result, dict)
                        else getattr(
                            eq_backfill_result,
                            "modeq_fsr_side_channel_first_breach",
                            None,
                        )
                    )
                    if isinstance(iter_first_breach, dict):
                        eq_backfill_modeq_fsr_side_channel_first_breach = dict(iter_first_breach)
                eq_backfill_modeq_fsr_side_channel_events_count = len(
                    eq_backfill_modeq_fsr_side_channel_events
                )
                if solver_trace_enabled and eq_backfill_modeq_fsr_side_channel in {
                    "capture",
                    "enforce",
                }:
                    modeq_event_count = sum(
                        1
                        for event in iter_side_channel_events
                        if str(event.get("kind", "")).startswith("modeq")
                    )
                    _append_solver_trace_event(
                        "modeq_call",
                        iteration=int(iteration),
                        payload={
                            "mode": eq_backfill_modeq_fsr_side_channel,
                            "status": eq_backfill_modeq_fsr_side_channel_status,
                            "event_count": int(modeq_event_count),
                            "breach_count_total": (
                                eq_backfill_modeq_fsr_side_channel_breach_count
                            ),
                        },
                    )
                iter_failed = _coerce_backfill_int(
                    eq_backfill_result,
                    "failed",
                    "failed_steps",
                )
                eq_backfill_failed += iter_failed
                backfill_issues = _coerce_backfill_issues(
                    eq_backfill_result.issues
                    if hasattr(eq_backfill_result, "issues")
                    else eq_backfill_result.get("issues")
                    if isinstance(eq_backfill_result, dict)
                    else []
                )
                issues.extend(backfill_issues)
                if eq_backfill_first_failure_blame is None and iter_failed > 0 and backfill_issues:
                    eq_backfill_first_failure_blame = _build_first_failure_blame_from_issues(
                        backfill_issues,
                        iteration=iteration,
                        trace_events=trace_events,
                    )
                eq_backfill_iterations += 1
                (
                    iter_delta,
                    eq_backfill_convergence_delta,
                    eq_backfill_convergence_ratio,
                ) = _compute_convergence_metrics(
                    before,
                    working,
                    targets=eq_metric_targets,
                    windows=iteration_metric_windows,
                    default_tol=eq_backfill_tol,
                    default_relative_mode=(
                        eq_backfill_tol_mode == "relative_with_zero_absolute_fallback"
                    ),
                    tolerance_overrides=eq_backfill_tol_overrides,
                    absolute_mode_overrides=eq_backfill_tolabs_overrides,
                )
                if eq_backfill_all_targets == eq_metric_targets:
                    (
                        _iter_delta_all,
                        eq_backfill_convergence_delta_all_targets,
                        eq_backfill_convergence_ratio_all_targets,
                    ) = (iter_delta, eq_backfill_convergence_delta, eq_backfill_convergence_ratio)
                else:
                    (
                        _iter_delta_all,
                        eq_backfill_convergence_delta_all_targets,
                        eq_backfill_convergence_ratio_all_targets,
                    ) = _compute_convergence_metrics(
                        before,
                        working,
                        targets=eq_backfill_all_targets,
                        windows=iteration_metric_windows,
                        default_tol=eq_backfill_tol,
                        default_relative_mode=(
                            eq_backfill_tol_mode == "relative_with_zero_absolute_fallback"
                        ),
                        tolerance_overrides=eq_backfill_tol_overrides,
                        absolute_mode_overrides=eq_backfill_tolabs_overrides,
                    )
                iter_has_nonfinite = not math.isfinite(iter_delta)
                if iter_has_nonfinite:
                    eq_backfill_iteration_has_nonfinite = True
                    if eq_backfill_iteration_first_nonfinite_iteration is None:
                        eq_backfill_iteration_first_nonfinite_iteration = iteration
                if eq_backfill_top_target_deltas > 0:
                    top_target_deltas_checked = _compute_top_target_deltas(
                        before,
                        working,
                        targets=eq_metric_targets,
                        windows=iteration_metric_windows,
                        default_tol=eq_backfill_tol,
                        default_relative_mode=(
                            eq_backfill_tol_mode == "relative_with_zero_absolute_fallback"
                        ),
                        top=eq_backfill_top_target_deltas,
                        tolerance_overrides=eq_backfill_tol_overrides,
                        absolute_mode_overrides=eq_backfill_tolabs_overrides,
                    )
                    if eq_backfill_all_targets == eq_metric_targets:
                        top_target_deltas_all_targets = top_target_deltas_checked
                    else:
                        top_target_deltas_all_targets = _compute_top_target_deltas(
                            before,
                            working,
                            targets=eq_backfill_all_targets,
                            windows=iteration_metric_windows,
                            default_tol=eq_backfill_tol,
                            default_relative_mode=(
                                eq_backfill_tol_mode == "relative_with_zero_absolute_fallback"
                            ),
                            top=eq_backfill_top_target_deltas,
                            tolerance_overrides=eq_backfill_tol_overrides,
                            absolute_mode_overrides=eq_backfill_tolabs_overrides,
                        )
                else:
                    top_target_deltas_checked = []
                    top_target_deltas_all_targets = []
                if (
                    eq_rho_resid_ar1_focus_trace_spec is not None
                    and eq_rho_resid_ar1_focus_trace_spec.iter_min
                    <= int(iteration)
                    <= eq_rho_resid_ar1_focus_trace_spec.iter_max
                ):
                    for focus_target in sorted(eq_rho_resid_ar1_focus_trace_spec.targets):
                        for focus_period in sorted(eq_rho_resid_ar1_focus_trace_spec.periods):
                            row = _compute_target_period_delta(
                                before,
                                working,
                                target=str(focus_target),
                                period=str(focus_period),
                                default_tol=eq_backfill_tol,
                                default_relative_mode=(
                                    eq_backfill_tol_mode == "relative_with_zero_absolute_fallback"
                                ),
                                tolerance_overrides=eq_backfill_tol_overrides,
                                absolute_mode_overrides=eq_backfill_tolabs_overrides,
                            )
                            if row is None:
                                continue
                            _append_focus_event_from_cli({
                                "iter": int(iteration),
                                "period": str(row["period"]),
                                "target": str(row["target"]),
                                "stage": "convergence_check",
                                "line": None,
                                "occurrence": None,
                                "update_source": str(eq_backfill_rho_resid_ar1_update_source),
                                "commit_mode": (
                                    "deferred"
                                    if eq_backfill_rho_resid_ar1_commit_after_check
                                    else "immediate"
                                ),
                                "delta": float(row["delta_mode"]),
                                "delta_abs": float(row["delta_abs"]),
                                "tol": float(row["tol"]),
                                "ratio_to_tol": float(row["ratio_to_tol"]),
                                "value_prev": float(row["value_before"]),
                                "value": float(row["value_after"]),
                            })
                if semantics_assertions_enabled and effective_rho_resid_ar1:
                    invariant_error = _assert_fp_semantics_trace_invariants(
                        trace_events=trace_events,
                        focus_events=eq_backfill_rho_resid_ar1_trace_focus_events,
                        iteration=int(iteration),
                        expected_commit_mode=eq_backfill_rho_resid_ar1_commit_mode,
                        commit_after_check=eq_backfill_rho_resid_ar1_commit_after_check,
                        side_channel_mode=eq_backfill_modeq_fsr_side_channel,
                        side_channel_status=eq_backfill_modeq_fsr_side_channel_status,
                        side_channel_breach_count=eq_backfill_modeq_fsr_side_channel_breach_count,
                    )
                    if invariant_error is not None:
                        print(f"Mini-run semantic assertion failed: {invariant_error}")
                        return 1
                if iter_has_nonfinite and eq_backfill_first_nonfinite_blame is None:
                    trace_blame = _build_first_nonfinite_blame_from_trace_events(
                        trace_events,
                        iteration=iteration,
                    )
                    if trace_blame is not None:
                        eq_backfill_first_nonfinite_blame = trace_blame
                    else:
                        top_row: dict[str, object] | None = None
                        for candidate in top_target_deltas_checked:
                            ratio_raw = candidate.get("ratio_to_tol")
                            try:
                                ratio_value = float(ratio_raw)
                            except (TypeError, ValueError):
                                continue
                            if not math.isfinite(ratio_value):
                                top_row = candidate
                                break
                        if top_row is None and top_target_deltas_checked:
                            top_row = top_target_deltas_checked[0]
                        if top_row is not None:
                            ratio_raw = top_row.get("ratio_to_tol")
                            ratio_value = float(ratio_raw) if ratio_raw is not None else None
                            eq_backfill_first_nonfinite_blame = {
                                "iteration": int(iteration),
                                "phase": "iteration_delta",
                                "kind": "top_target_delta",
                                "line": None,
                                "occurrence": None,
                                "target": top_row.get("target"),
                                "period": top_row.get("worst_period"),
                                "period_position": None,
                                "op": "nonfinite_value",
                                "nonfinite_field": "ratio_to_tol",
                                "nonfinite_value": ratio_value,
                                "snapshot": {
                                    "target": top_row.get("target"),
                                    "worst_period": top_row.get("worst_period"),
                                    "ratio_to_tol": ratio_value,
                                    "max_mode_delta": top_row.get("max_mode_delta"),
                                    "max_abs_delta": top_row.get("max_abs_delta"),
                                },
                            }
                        else:
                            nonfinite_cell = _find_first_nonfinite_frame_cell(
                                working,
                                targets=(
                                    eq_backfill_all_targets
                                    if eq_backfill_all_targets
                                    else eq_metric_targets
                                ),
                                windows=iteration_metric_windows,
                            )
                            if nonfinite_cell is not None:
                                eq_backfill_first_nonfinite_blame = {
                                    "iteration": int(iteration),
                                    "phase": "iteration_delta",
                                    "kind": "frame",
                                    "line": None,
                                    "occurrence": None,
                                    "target": nonfinite_cell["target"],
                                    "period": nonfinite_cell["period"],
                                    "period_position": None,
                                    "op": "nonfinite_value",
                                    "nonfinite_field": "frame_value",
                                    "nonfinite_value": float(nonfinite_cell["value"]),
                                    "snapshot": {
                                        "target": nonfinite_cell["target"],
                                        "period": nonfinite_cell["period"],
                                        "value": float(nonfinite_cell["value"]),
                                    },
                                }
                            else:
                                eq_backfill_first_nonfinite_blame = {
                                    "iteration": int(iteration),
                                    "phase": "iteration_delta",
                                    "kind": "unknown",
                                    "line": None,
                                    "occurrence": None,
                                    "target": None,
                                    "period": None,
                                    "period_position": None,
                                    "op": "nonfinite_value",
                                    "nonfinite_field": "unknown",
                                    "nonfinite_value": None,
                                    "snapshot": {},
                                }
                (
                    eq_backfill_residual_abs_delta,
                    eq_backfill_residual_ratio,
                    eq_backfill_residual_failed,
                    eq_backfill_residual_issue_count,
                    eq_backfill_residual_probe_has_nonfinite_delta,
                    eq_backfill_residual_probe_has_nonfinite_ratio,
                    eq_backfill_residual_probe_has_nonfinite,
                ) = _compute_fixed_point_residual(
                    iteration_eq_records,
                    working,
                    specs,
                    on_error=on_error,
                    strict_missing_inputs=strict_missing_inputs,
                    strict_missing_assignments=strict_missing_assignments,
                    dampall=eq_backfill_dampall,
                    damp_overrides=eq_backfill_damp_overrides,
                    targets=eq_metric_targets,
                    windows=iteration_metric_windows,
                    default_tol=eq_backfill_tol,
                    default_relative_mode=(
                        eq_backfill_tol_mode == "relative_with_zero_absolute_fallback"
                    ),
                    tolerance_overrides=eq_backfill_tol_overrides,
                    absolute_mode_overrides=eq_backfill_tolabs_overrides,
                    period_sequential=eq_backfill_period_sequential,
                    period_sequential_fp_pass_order=bool(eq_period_sequential_fp_pass_order),
                    period_sequential_all_assignments=(
                        eq_backfill_period_sequential_all_assignments
                    ),
                    period_sequential_defer_create=(eq_backfill_period_sequential_defer_create),
                    period_sequential_context_replay_command_time_smpl=(
                        eq_backfill_period_sequential_context_replay_command_time_smpl
                    ),
                    period_sequential_context_retain_overlap_only=(
                        eq_backfill_period_sequential_context_retain_overlap_only
                    ),
                    period_sequential_assignment_targets=iteration_assignment_targets,
                    windows_override=eq_backfill_windows_override,
                    rho_aware=eq_backfill_rho_aware,
                    rho_resid_ar1=effective_rho_resid_ar1,
                    rho_resid_iteration_seed=residual_probe_seed,
                    rho_resid_iteration_seed_lag=eq_backfill_rho_resid_ar1_seed_lag,
                    rho_resid_iteration_seed_mode=eq_backfill_rho_resid_ar1_seed_mode,
                    rho_resid_boundary_reset=eq_backfill_rho_resid_ar1_boundary_reset,
                    rho_resid_carry_lag=eq_backfill_rho_resid_ar1_carry_lag,
                    rho_resid_carry_damp=eq_backfill_rho_resid_ar1_carry_damp,
                    rho_resid_carry_damp_mode=eq_backfill_rho_resid_ar1_carry_damp_mode,
                    rho_resid_carry_multipass=eq_backfill_rho_resid_ar1_carry_multipass,
                    rho_resid_lag_gating=eq_backfill_rho_resid_ar1_lag_gating,
                    rho_resid_update_source=eq_backfill_rho_resid_ar1_update_source,
                    rho_resid_la257_update_rule=effective_la257_update_rule,
                    rho_resid_la257_fortran_cycle_carry_style=(
                        eq_backfill_rho_resid_ar1_la257_fortran_cycle_carry_style
                    ),
                    rho_resid_la257_u_phase_max_gain=(
                        eq_backfill_rho_resid_ar1_la257_u_phase_max_gain
                    ),
                    rho_resid_la257_u_phase_max_gain_mode=(
                        eq_backfill_rho_resid_ar1_la257_u_phase_max_gain_mode
                    ),
                    rho_resid_la257_staged_lifecycle=(
                        eq_backfill_rho_resid_ar1_la257_lifecycle_staging
                    ),
                    rho_resid_commit_after_check=(eq_backfill_rho_resid_ar1_commit_after_check),
                    rho_resid_la257_u_phase_lines=(eq_backfill_rho_resid_ar1_la257_u_phase_lines),
                    rho_resid_trace_lines=eq_backfill_rho_resid_ar1_trace_lines,
                    rho_resid_trace_event_limit=eq_backfill_rho_resid_ar1_trace_event_limit,
                    rho_resid_fpe_trap=eq_backfill_rho_resid_ar1_fpe_trap,
                    eval_context=eval_context,
                )
                if eq_backfill_all_targets == eq_metric_targets:
                    (
                        eq_backfill_residual_abs_delta_all_targets,
                        eq_backfill_residual_ratio_all_targets,
                        eq_backfill_residual_failed_all_targets,
                        eq_backfill_residual_issue_count_all_targets,
                    ) = (
                        eq_backfill_residual_abs_delta,
                        eq_backfill_residual_ratio,
                        eq_backfill_residual_failed,
                        eq_backfill_residual_issue_count,
                    )
                    (
                        eq_backfill_residual_probe_has_nonfinite_delta_all_targets,
                        eq_backfill_residual_probe_has_nonfinite_ratio_all_targets,
                        eq_backfill_residual_probe_has_nonfinite_all_targets,
                    ) = (
                        eq_backfill_residual_probe_has_nonfinite_delta,
                        eq_backfill_residual_probe_has_nonfinite_ratio,
                        eq_backfill_residual_probe_has_nonfinite,
                    )
                else:
                    (
                        eq_backfill_residual_abs_delta_all_targets,
                        eq_backfill_residual_ratio_all_targets,
                        eq_backfill_residual_failed_all_targets,
                        eq_backfill_residual_issue_count_all_targets,
                        eq_backfill_residual_probe_has_nonfinite_delta_all_targets,
                        eq_backfill_residual_probe_has_nonfinite_ratio_all_targets,
                        eq_backfill_residual_probe_has_nonfinite_all_targets,
                    ) = _compute_fixed_point_residual(
                        iteration_eq_records,
                        working,
                        specs,
                        on_error=on_error,
                        strict_missing_inputs=strict_missing_inputs,
                        strict_missing_assignments=strict_missing_assignments,
                        dampall=eq_backfill_dampall,
                        damp_overrides=eq_backfill_damp_overrides,
                        targets=eq_backfill_all_targets,
                        windows=iteration_metric_windows,
                        default_tol=eq_backfill_tol,
                        default_relative_mode=(
                            eq_backfill_tol_mode == "relative_with_zero_absolute_fallback"
                        ),
                        tolerance_overrides=eq_backfill_tol_overrides,
                        absolute_mode_overrides=eq_backfill_tolabs_overrides,
                        period_sequential=eq_backfill_period_sequential,
                        period_sequential_fp_pass_order=bool(eq_period_sequential_fp_pass_order),
                        period_sequential_all_assignments=(
                            eq_backfill_period_sequential_all_assignments
                        ),
                        period_sequential_defer_create=(
                            eq_backfill_period_sequential_defer_create
                        ),
                        period_sequential_context_replay_command_time_smpl=(
                            eq_backfill_period_sequential_context_replay_command_time_smpl
                        ),
                        period_sequential_context_retain_overlap_only=(
                            eq_backfill_period_sequential_context_retain_overlap_only
                        ),
                        period_sequential_assignment_targets=iteration_assignment_targets,
                        windows_override=eq_backfill_windows_override,
                        rho_aware=eq_backfill_rho_aware,
                        rho_resid_ar1=effective_rho_resid_ar1,
                        rho_resid_iteration_seed=residual_probe_seed,
                        rho_resid_iteration_seed_lag=eq_backfill_rho_resid_ar1_seed_lag,
                        rho_resid_iteration_seed_mode=eq_backfill_rho_resid_ar1_seed_mode,
                        rho_resid_boundary_reset=eq_backfill_rho_resid_ar1_boundary_reset,
                        rho_resid_carry_lag=eq_backfill_rho_resid_ar1_carry_lag,
                        rho_resid_carry_damp=eq_backfill_rho_resid_ar1_carry_damp,
                        rho_resid_carry_damp_mode=eq_backfill_rho_resid_ar1_carry_damp_mode,
                        rho_resid_carry_multipass=eq_backfill_rho_resid_ar1_carry_multipass,
                        rho_resid_lag_gating=eq_backfill_rho_resid_ar1_lag_gating,
                        rho_resid_update_source=eq_backfill_rho_resid_ar1_update_source,
                        rho_resid_la257_update_rule=effective_la257_update_rule,
                        rho_resid_la257_fortran_cycle_carry_style=(
                            eq_backfill_rho_resid_ar1_la257_fortran_cycle_carry_style
                        ),
                        rho_resid_la257_u_phase_max_gain=(
                            eq_backfill_rho_resid_ar1_la257_u_phase_max_gain
                        ),
                        rho_resid_la257_u_phase_max_gain_mode=(
                            eq_backfill_rho_resid_ar1_la257_u_phase_max_gain_mode
                        ),
                        rho_resid_la257_staged_lifecycle=(
                            eq_backfill_rho_resid_ar1_la257_lifecycle_staging
                        ),
                        rho_resid_commit_after_check=(
                            eq_backfill_rho_resid_ar1_commit_after_check
                        ),
                        rho_resid_la257_u_phase_lines=(
                            eq_backfill_rho_resid_ar1_la257_u_phase_lines
                        ),
                        rho_resid_trace_lines=eq_backfill_rho_resid_ar1_trace_lines,
                        rho_resid_trace_event_limit=(eq_backfill_rho_resid_ar1_trace_event_limit),
                        rho_resid_fpe_trap=eq_backfill_rho_resid_ar1_fpe_trap,
                        eval_context=eval_context,
                    )
                if solver_trace_enabled:
                    _append_solver_trace_event(
                        "convergence_check",
                        iteration=int(iteration),
                        payload={
                            "convergence_ratio": eq_backfill_convergence_ratio,
                            "convergence_ratio_all_targets": (
                                eq_backfill_convergence_ratio_all_targets
                            ),
                            "residual_ratio": eq_backfill_residual_ratio,
                            "residual_ratio_all_targets": (eq_backfill_residual_ratio_all_targets),
                            "residual_ratio_max": eq_backfill_residual_ratio_max,
                            "residual_failed": eq_backfill_residual_failed,
                            "iter_nonfinite": bool(iter_has_nonfinite),
                        },
                    )
                eq_backfill_max_abs_delta = iter_delta
                eq_backfill_last_abs_delta = iter_delta
                if eq_backfill_iterations == 1:
                    eq_backfill_min_abs_delta = iter_delta
                else:
                    eq_backfill_min_abs_delta = min(eq_backfill_min_abs_delta, iter_delta)
                hf_line182_probe = _compute_hf_line182_overflow_probe(working)
                hf_lhf1_ho_probe = _compute_hf_lhf1_ho_iteration_probe(working)
                if hf_line182_probe is not None:
                    eq_backfill_hf_line182_overflow_probe_history.append({
                        "iteration": int(iteration),
                        **hf_line182_probe,
                    })
                if hf_lhf1_ho_probe is not None:
                    eq_backfill_hf_lhf1_ho_probe_history.append({
                        "iteration": int(iteration),
                        **hf_lhf1_ho_probe,
                    })
                eq_backfill_iteration_history.append({
                    "iteration": iteration,
                    "applied": iter_applied,
                    "failed": iter_failed,
                    "max_abs_delta": iter_delta,
                    "rho_resid_la257_update_rule_effective": effective_la257_update_rule,
                    **(
                        {
                            "rho_resid_iteration_state_top": (
                                eq_backfill_rho_resid_iteration_state_top
                            ),
                        }
                        if eq_backfill_rho_resid_iteration_state_top
                        else {}
                    ),
                    **(
                        {
                            "rho_resid_iteration_trace_lines": (
                                eq_backfill_rho_resid_iteration_trace_last
                            ),
                        }
                        if eq_backfill_rho_resid_iteration_trace_last
                        else {}
                    ),
                    **(
                        {
                            "rho_resid_ar1_trace_event_count": (
                                eq_backfill_rho_resid_ar1_trace_event_count
                            ),
                            "rho_resid_ar1_trace_events": (eq_backfill_rho_resid_ar1_trace_events),
                            "rho_resid_ar1_trace_events_truncated": (
                                eq_backfill_rho_resid_ar1_trace_events_truncated
                            ),
                        }
                        if eq_backfill_rho_resid_ar1_trace_event_count > 0
                        else {}
                    ),
                    **(
                        {
                            "hf_line182_overflow_probe": hf_line182_probe,
                        }
                        if hf_line182_probe is not None
                        else {}
                    ),
                    **(
                        {
                            "hf_lhf1_ho_probe": hf_lhf1_ho_probe,
                        }
                        if hf_lhf1_ho_probe is not None
                        else {}
                    ),
                    **(
                        {
                            "top_target_deltas_checked": top_target_deltas_checked,
                            "top_target_deltas_all_targets": top_target_deltas_all_targets,
                        }
                        if eq_backfill_top_target_deltas > 0
                        else {}
                    ),
                })
                if (
                    solver_trace_enabled
                    and effective_rho_resid_ar1
                    and eq_backfill_rho_resid_ar1_commit_after_check
                ):
                    _append_solver_trace_event(
                        "commit_rho_resid_ar1",
                        iteration=int(iteration),
                        payload={
                            "commit_mode": eq_backfill_rho_resid_ar1_commit_mode,
                            "carry_iterations": bool(eq_backfill_rho_resid_ar1_carry_iterations),
                            "state_size": int(eq_backfill_rho_resid_iteration_state_size),
                            "state_seed_candidate": bool(iteration_seed_candidate is not None),
                            "event_count": int(commit_like_event_count),
                        },
                    )

                if iter_failed and on_error == "stop":
                    eq_backfill_stop_reason = "eq_failure_stop"
                    break
                if iter_failed:
                    eq_had_failures = True
                if eq_backfill_nomiss_required and eq_nomiss_targets:
                    # In period-scoped mode, only check NOMISS for the
                    # current period — later periods haven't been solved yet.
                    nomiss_windows = (
                        iteration_metric_windows
                        if period_scoped_enabled and solve_scope_windows
                        else eq_nomiss_windows
                    )
                    nomiss_windows_by_target = (
                        None
                        if period_scoped_enabled and solve_scope_windows
                        else eq_nomiss_windows_by_target
                    )
                    eq_backfill_nomiss_target_diagnostics = _count_missing_target_cells_by_target(
                        working,
                        eq_nomiss_targets,
                        windows=nomiss_windows,
                        windows_by_target=nomiss_windows_by_target,
                        exclude_index_by_target=eq_nomiss_exclusions,
                    )
                    eq_backfill_nomiss_missing_cells = _count_missing_target_cells(
                        working,
                        eq_nomiss_targets,
                        windows=nomiss_windows,
                        windows_by_target=nomiss_windows_by_target,
                        exclude_index_by_target=eq_nomiss_exclusions,
                    )
                    if eq_backfill_nomiss_missing_cells > 0:
                        eq_backfill_failed += 1
                        eq_backfill_stop_reason = "nomiss_missing"
                        issues.append({
                            "line": None,
                            "statement": "SETUPSOLVE NOMISS",
                            "error": (
                                "NOMISS violation after EQ backfill: "
                                f"{eq_backfill_nomiss_missing_cells} missing cells across "
                                f"{len(eq_nomiss_targets)} targets"
                            ),
                        })
                        break
                if (
                    not eq_had_failures
                    and eq_backfill_iterations >= eq_backfill_min_iters
                    and eq_backfill_convergence_ratio <= 1.0
                    and eq_backfill_residual_ratio <= eq_backfill_residual_ratio_max
                    and eq_backfill_residual_failed == 0
                ):
                    eq_backfill_converged = True
                    eq_backfill_stop_reason = "converged"
                    break
                if (
                    eq_backfill_rho_resid_ar1_carry_iterations
                    and iteration_seed_candidate is not None
                ):
                    rho_resid_iteration_seed = dict(iteration_seed_candidate)

            eq_backfill_iterations_total += int(eq_backfill_iterations)
            if period_scoped_enabled and solver_trace_enabled and solve_scope_windows:
                _append_solver_trace_event(
                    "period_end",
                    payload={
                        "stop": eq_backfill_stop_reason,
                        "iterations": int(eq_backfill_iterations),
                        "converged": bool(eq_backfill_converged),
                        "converged_checked_only": bool(eq_backfill_converged_checked_only),
                        "period": str(solve_scope_windows[0][0]),
                        "period_scoped": True,
                    },
                )

        if period_scoped_enabled:
            eq_backfill_iterations = int(eq_backfill_iterations_total)

        if eq_rho_resid_ar1_focus_trace_spec is not None:
            eq_backfill_rho_resid_ar1_trace_focus_events = _sort_focus_events(
                eq_backfill_rho_resid_ar1_trace_focus_events
            )
            eq_backfill_rho_resid_ar1_trace_focus_events_count = len(
                eq_backfill_rho_resid_ar1_trace_focus_events
            )
            stage_counts: Counter[str] = Counter(
                str(event.get("stage", ""))
                for event in eq_backfill_rho_resid_ar1_trace_focus_events
                if event.get("stage")
            )
            eq_backfill_rho_resid_ar1_trace_focus_event_counts_by_stage = {
                stage: int(stage_counts[stage])
                for stage in sorted(
                    stage_counts.keys(),
                    key=lambda key: _TRACE_FOCUS_STAGE_ORDER.get(key, 99),
                )
            }
            eq_backfill_rho_resid_ar1_trace_focus_no_match = (
                eq_backfill_rho_resid_ar1_trace_focus_events_count == 0
            )

        if (
            active_solve_runtime_directive is not None
            and not solve_active_outside
            and not solve_active_noreset
        ):
            # Legacy SOLVE default resets predicted/current state to actuals.
            working = solve_pre_reset_frame

        output_frame = working
        failed += eq_backfill_failed
        eq_backfill_attempted_records = sum(
            int(item.get("applied", 0)) + int(item.get("failed", 0))
            for item in eq_backfill_iteration_history
        )
        if not eq_had_failures and not eq_backfill_converged and eq_backfill_iterations >= 2:
            baseline_delta = max(eq_backfill_min_abs_delta, eq_backfill_tol)
            if baseline_delta > 0:
                eq_backfill_delta_ratio = eq_backfill_last_abs_delta / baseline_delta
                eq_backfill_diverged = eq_backfill_delta_ratio > 1.25
        if eq_backfill_stop_reason == "not_started":
            if eq_had_failures:
                eq_backfill_stop_reason = "eq_failure"
            elif eq_backfill_diverged:
                eq_backfill_stop_reason = "diverged"
            elif (
                eq_backfill_iterations >= eq_backfill_min_iters
                and eq_backfill_residual_ratio > eq_backfill_residual_ratio_max
                and eq_backfill_residual_failed == 0
            ):
                eq_backfill_stop_reason = "residual_not_fixed_point"
            elif eq_backfill_iterations >= eq_backfill_max_iters and eq_backfill_max_iters > 0:
                eq_backfill_stop_reason = "max_iters_reached"
            elif eq_backfill_iterations > 0:
                eq_backfill_stop_reason = "completed"
            else:
                eq_backfill_stop_reason = "no_iterations"
        eq_backfill_converged_checked_only = (
            eq_backfill_converged
            and len(eq_backfill_all_targets) > len(eq_metric_targets)
            and (
                eq_backfill_convergence_ratio_all_targets > 1.0
                or eq_backfill_residual_ratio_all_targets > eq_backfill_residual_ratio_max
                or eq_backfill_residual_failed_all_targets > 0
            )
        )
        if solve_keyboard_targets_active:
            if eq_backfill_keyboard_targets_missing:
                eq_backfill_keyboard_stop_classification = "missing_targets"
            elif eq_backfill_converged_checked_only:
                eq_backfill_keyboard_stop_classification = "converged_checked_only"
            elif eq_backfill_converged:
                eq_backfill_keyboard_stop_classification = "converged"
            else:
                eq_backfill_keyboard_stop_classification = "nonconverged"
                eq_backfill_keyboard_nonconvergence_reason = str(eq_backfill_stop_reason)
        if solver_trace_enabled:
            period_end_payload: dict[str, object] = {
                "stop": eq_backfill_stop_reason,
                "iterations": int(eq_backfill_iterations),
                "converged": bool(eq_backfill_converged),
                "converged_checked_only": bool(eq_backfill_converged_checked_only),
            }
            if period_scoped_enabled and solve_scope_windows:
                period_end_payload["period"] = str(solve_scope_windows[0][0])
                period_end_payload["period_scoped"] = True
            _append_solver_trace_event(
                "period_end",
                payload=period_end_payload,
            )
        eq_backfill_runtime_iteration_seconds += time.monotonic() - eq_iteration_phase_started

    if enable_eq and active_solve_runtime_directive is not None and solve_active_line is not None:
        eq_post_solve_replay_started = time.monotonic()
        replay_issues = replay_runtime_post_commands(
            records,
            frame=output_frame,
            runtime_base_dir=runtime_base_dir,
            on_error=on_error,
            line_number_min=int(solve_active_line),
        )
        eq_backfill_runtime_post_solve_replay_seconds += (
            time.monotonic() - eq_post_solve_replay_started
        )
        solve_post_commands_replayed = True
        solve_post_replay_issue_count = len(replay_issues)
        if replay_issues:
            failed += len(replay_issues)
            issues.extend(
                {
                    "line": issue.line_number,
                    "statement": issue.statement,
                    "error": issue.error,
                }
                for issue in replay_issues
            )

    if enable_eq:
        eq_backfill_runtime_total_seconds = time.monotonic() - eq_backfill_runtime_started

    eq_only_counter_promoted = (
        enable_eq and include_command_filter == {FPCommand.EQ} and eq_backfill_record_count > 0
    )
    if eq_only_counter_promoted:
        planned = max(planned, eq_backfill_record_count)
        executed = max(executed, eq_backfill_attempted_records)

    if eq_only_counter_promoted:
        effective_planned = planned
        effective_executed = executed
    else:
        effective_planned = planned + eq_backfill_record_count
        effective_executed = executed + eq_backfill_attempted_records

    if parity_baseline_fmout is not None:
        parity_report = _compute_output_parity_report(
            output_frame,
            fmout_path=Path(parity_baseline_fmout),
            atol=float(parity_atol),
            top=int(parity_top),
        )
    if eq_backfill_period_sequential_context_replay_trace:
        max_events = max(0, eq_backfill_period_sequential_context_replay_trace_max_events)
        if len(eq_backfill_context_replay_trace_events) > max_events:
            eq_backfill_context_replay_trace_events = eq_backfill_context_replay_trace_events[
                :max_events
            ]
            eq_backfill_context_replay_trace_events_truncated = True
        eq_backfill_context_replay_trace_events_count = len(
            eq_backfill_context_replay_trace_events
        )
        if report_json is not None:
            eq_backfill_context_replay_trace_path = str(
                Path(report_json).parent / "context-replay-trace.json"
            )
        elif solver_trace_path is not None:
            eq_backfill_context_replay_trace_path = str(
                Path(solver_trace_path).parent / "context-replay-trace.json"
            )
        elif output_csv is not None:
            eq_backfill_context_replay_trace_path = str(
                Path(output_csv).parent / "context-replay-trace.json"
            )
        else:
            eq_backfill_context_replay_trace_path = str(Path.cwd() / "context-replay-trace.json")
    if eq_iter_trace:
        iter_trace_target_allowlist = set(eq_iter_trace_targets_parsed)
        iteration_summary: list[dict[str, object]] = []
        for item in eq_backfill_iteration_history:
            if not isinstance(item, dict):
                continue
            try:
                iteration_value = int(item.get("iteration", 0))
            except (TypeError, ValueError):
                continue
            if iteration_value <= 0:
                continue
            iteration_summary.append({
                "iteration": iteration_value,
                "applied": int(item.get("applied", 0)),
                "failed": int(item.get("failed", 0)),
                "max_abs_delta": float(item.get("max_abs_delta", 0.0)),
            })
            for field_name, scope_name in (
                ("top_target_deltas_checked", "checked"),
                ("top_target_deltas_all_targets", "all_targets"),
            ):
                raw_rows = item.get(field_name)
                if not isinstance(raw_rows, list):
                    continue
                for row in raw_rows:
                    if not isinstance(row, dict):
                        continue
                    target_name = str(row.get("target", "")).strip().upper()
                    if not target_name:
                        continue
                    if (
                        iter_trace_target_allowlist
                        and target_name not in iter_trace_target_allowlist
                    ):
                        continue
                    worst_period = str(row.get("worst_period", "")).strip()
                    if worst_period != eq_iter_trace_period_resolved:
                        continue
                    try:
                        max_abs_delta = float(row.get("max_abs_delta", 0.0))
                    except (TypeError, ValueError):
                        max_abs_delta = 0.0
                    try:
                        max_mode_delta = float(row.get("max_mode_delta", 0.0))
                    except (TypeError, ValueError):
                        max_mode_delta = 0.0
                    try:
                        ratio_to_tol = float(row.get("ratio_to_tol", 0.0))
                    except (TypeError, ValueError):
                        ratio_to_tol = 0.0
                    eq_iter_trace_events.append({
                        "iteration": iteration_value,
                        "scope": scope_name,
                        "target": target_name,
                        "worst_period": worst_period,
                        "max_abs_delta": max_abs_delta,
                        "max_mode_delta": max_mode_delta,
                        "ratio_to_tol": ratio_to_tol,
                    })
        eq_iter_trace_events.sort(
            key=lambda item: (
                int(item.get("iteration", 0)),
                -float(item.get("max_abs_delta", 0.0)),
                str(item.get("target", "")),
            )
        )
        max_iter_trace_events = max(0, int(eq_iter_trace_max_events))
        if len(eq_iter_trace_events) > max_iter_trace_events:
            eq_iter_trace_events = eq_iter_trace_events[:max_iter_trace_events]
            eq_iter_trace_events_truncated = True
        eq_iter_trace_events_count = len(eq_iter_trace_events)
        if report_json is not None:
            eq_iter_trace_path = str(Path(report_json).parent / "eq_iter_trace.json")
        elif output_csv is not None:
            eq_iter_trace_path = str(Path(output_csv).parent / "eq_iter_trace.json")
        else:
            eq_iter_trace_path = str(Path.cwd() / "eq_iter_trace.json")
        iter_trace_payload = {
            "period": eq_iter_trace_period_resolved,
            "target_allowlist": list(eq_iter_trace_targets_parsed),
            "max_events": max_iter_trace_events,
            "events_count": eq_iter_trace_events_count,
            "events_truncated": bool(eq_iter_trace_events_truncated),
            "iteration_summary": iteration_summary,
            "events": eq_iter_trace_events,
        }
        Path(eq_iter_trace_path).parent.mkdir(parents=True, exist_ok=True)
        Path(eq_iter_trace_path).write_text(
            json.dumps(iter_trace_payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    solver_trace_event_count = len(solver_trace_events)
    mini_run_runtime_total_seconds = time.monotonic() - mini_run_started

    print("Mini-run summary:")
    print(f"- mini_run_runtime_total_seconds: {mini_run_runtime_total_seconds:.6g}")
    print(f"- records: {records_count}")
    print(f"- planned: {planned}")
    print(f"- executed: {executed}")
    print(f"- effective_planned: {effective_planned}")
    print(f"- effective_executed: {effective_executed}")
    print(f"- failed: {failed}")
    print(f"- unsupported command counts: {unsupported_counts}")
    if auto_enable_eq_from_solve:
        print("- eq_backfill_trigger: solve_command")
    if solve_active_line is not None:
        print(f"- solve_active_line: {solve_active_line}")
        print(f"- solve_active_outside: {solve_active_outside}")
        print(f"- solve_active_noreset: {solve_active_noreset}")
        print(f"- solve_active_filevar: {solve_active_filevar}")
        print(f"- solve_active_keyboard_target_count: {len(solve_active_keyboard_targets)}")
        print(f"- solve_outside_seeded_cells: {solve_outside_seeded_cells}")
        print(f"- solve_outside_seed_inspected_cells: {solve_outside_seed_inspected_cells}")
        print(f"- solve_outside_seed_candidate_cells: {solve_outside_seed_candidate_cells}")
        print(f"- eq_backfill_auto_keyboard_stub_active: {eq_backfill_auto_keyboard_stub_active}")
        print(
            "- eq_backfill_auto_keyboard_stub_period_scoped_enabled: "
            f"{eq_backfill_auto_keyboard_stub_period_scoped_enabled}"
        )
        print(
            "- eq_backfill_auto_keyboard_stub_period_sequential_enabled: "
            f"{eq_backfill_auto_keyboard_stub_period_sequential_enabled}"
        )
        print(
            "- eq_backfill_auto_keyboard_stub_min_iters_enabled: "
            f"{eq_backfill_auto_keyboard_stub_min_iters_enabled}"
        )
        print(
            "- eq_backfill_auto_keyboard_stub_all_assignments_enabled: "
            f"{eq_backfill_auto_keyboard_stub_all_assignments_enabled}"
        )
        print(
            "- eq_backfill_auto_keyboard_stub_context_first_iter_enabled: "
            f"{eq_backfill_auto_keyboard_stub_context_first_iter_enabled}"
        )
        print(
            "- eq_backfill_auto_keyboard_stub_context_first_iter_disabled_for_min_iters: "
            f"{eq_backfill_auto_keyboard_stub_context_first_iter_disabled_for_min_iters}"
        )
        print(f"- eq_backfill_outside_post_seed_cells: {eq_backfill_outside_post_seed_cells}")
        print(
            "- eq_backfill_outside_post_seed_inspected_cells: "
            f"{eq_backfill_outside_post_seed_inspected_cells}"
        )
        print(
            "- eq_backfill_outside_post_seed_candidate_cells: "
            f"{eq_backfill_outside_post_seed_candidate_cells}"
        )
        print(f"- solve_post_commands_replayed: {solve_post_commands_replayed}")
        print(f"- solve_post_replay_issue_count: {solve_post_replay_issue_count}")
    if enable_eq:
        print(f"- eq_backfill_applied: {eq_backfill_applied}")
        print(f"- eq_backfill_failed: {eq_backfill_failed}")
        print(f"- eq_backfill_modeq_fsr_updates: {eq_backfill_modeq_fsr_updates}")
        print(
            "- eq_backfill_modeq_fsr_active_equation_count: "
            f"{eq_backfill_modeq_fsr_active_equation_count}"
        )
        print(
            f"- eq_backfill_modeq_fsr_active_term_count: {eq_backfill_modeq_fsr_active_term_count}"
        )
        print(f"- eq_backfill_eq_fsr_skipped: {eq_backfill_eq_fsr_skipped}")
        print(f"- eq_backfill_modeq_fsr_side_channel: {eq_backfill_modeq_fsr_side_channel}")
        print(
            "- eq_backfill_modeq_fsr_side_channel_status: "
            f"{eq_backfill_modeq_fsr_side_channel_status}"
        )
        print(
            "- eq_backfill_modeq_fsr_side_channel_effective_limit: "
            f"{eq_backfill_modeq_fsr_side_channel_effective_limit}"
        )
        print(
            "- eq_backfill_modeq_fsr_side_channel_events_count: "
            f"{eq_backfill_modeq_fsr_side_channel_events_count}"
        )
        print(
            "- eq_backfill_modeq_fsr_side_channel_events_truncated: "
            f"{eq_backfill_modeq_fsr_side_channel_events_truncated}"
        )
        print(
            "- eq_backfill_modeq_fsr_side_channel_breach_count: "
            f"{eq_backfill_modeq_fsr_side_channel_breach_count}"
        )
        print(
            "- eq_backfill_modeq_fsr_side_channel_mutation_count: "
            f"{eq_backfill_modeq_fsr_side_channel_mutation_count}"
        )
        if eq_backfill_modeq_fsr_side_channel_first_breach is not None:
            print(
                "- eq_backfill_modeq_fsr_side_channel_first_breach: "
                f"{eq_backfill_modeq_fsr_side_channel_first_breach}"
            )
        print(f"- eq_backfill_iterations: {eq_backfill_iterations}")
        print(f"- eq_backfill_runtime_total_seconds: {eq_backfill_runtime_total_seconds:.6g}")
        print(
            "- eq_backfill_runtime_build_records_seconds: "
            f"{eq_backfill_runtime_build_records_seconds:.6g}"
        )
        print(
            "- eq_backfill_runtime_setupsolve_seconds: "
            f"{eq_backfill_runtime_setupsolve_seconds:.6g}"
        )
        print(f"- eq_backfill_runtime_specs_seconds: {eq_backfill_runtime_specs_seconds:.6g}")
        print(
            "- eq_backfill_runtime_window_prep_seconds: "
            f"{eq_backfill_runtime_window_prep_seconds:.6g}"
        )
        print(
            f"- eq_backfill_runtime_iteration_seconds: {eq_backfill_runtime_iteration_seconds:.6g}"
        )
        print(
            "- eq_backfill_runtime_iteration_apply_seconds: "
            f"{eq_backfill_runtime_iteration_apply_seconds:.6g}"
        )
        print(
            "- eq_backfill_runtime_post_solve_replay_seconds: "
            f"{eq_backfill_runtime_post_solve_replay_seconds:.6g}"
        )
        print(
            "- eq_backfill_runtime_iteration_apply_calls: "
            f"{eq_backfill_runtime_iteration_apply_calls}"
        )
        print(f"- eq_backfill_record_count: {eq_backfill_record_count}")
        print(f"- eq_backfill_converged: {eq_backfill_converged}")
        print(f"- eq_backfill_max_abs_delta: {eq_backfill_max_abs_delta:.6g}")
        print(f"- eq_backfill_min_abs_delta: {eq_backfill_min_abs_delta:.6g}")
        print(f"- eq_backfill_convergence_delta: {eq_backfill_convergence_delta:.6g}")
        print(f"- eq_backfill_convergence_ratio: {eq_backfill_convergence_ratio:.6g}")
        print(
            "- eq_backfill_convergence_delta_all_targets: "
            f"{eq_backfill_convergence_delta_all_targets:.6g}"
        )
        print(
            "- eq_backfill_convergence_ratio_all_targets: "
            f"{eq_backfill_convergence_ratio_all_targets:.6g}"
        )
        print(f"- eq_backfill_residual_abs_delta: {eq_backfill_residual_abs_delta:.6g}")
        print(f"- eq_backfill_residual_ratio: {eq_backfill_residual_ratio:.6g}")
        print(
            "- eq_backfill_residual_abs_delta_all_targets: "
            f"{eq_backfill_residual_abs_delta_all_targets:.6g}"
        )
        print(
            "- eq_backfill_residual_ratio_all_targets: "
            f"{eq_backfill_residual_ratio_all_targets:.6g}"
        )
        print(f"- eq_backfill_residual_ratio_max: {eq_backfill_residual_ratio_max:.6g}")
        print(f"- eq_backfill_residual_failed: {eq_backfill_residual_failed}")
        print(f"- eq_backfill_residual_issue_count: {eq_backfill_residual_issue_count}")
        print(
            "- eq_backfill_residual_probe_has_nonfinite_delta: "
            f"{eq_backfill_residual_probe_has_nonfinite_delta}"
        )
        print(
            "- eq_backfill_residual_probe_has_nonfinite_ratio: "
            f"{eq_backfill_residual_probe_has_nonfinite_ratio}"
        )
        print(
            "- eq_backfill_residual_probe_has_nonfinite: "
            f"{eq_backfill_residual_probe_has_nonfinite}"
        )
        print(
            f"- eq_backfill_residual_failed_all_targets: {eq_backfill_residual_failed_all_targets}"
        )
        print(
            "- eq_backfill_residual_issue_count_all_targets: "
            f"{eq_backfill_residual_issue_count_all_targets}"
        )
        print(
            "- eq_backfill_residual_probe_has_nonfinite_delta_all_targets: "
            f"{eq_backfill_residual_probe_has_nonfinite_delta_all_targets}"
        )
        print(
            "- eq_backfill_residual_probe_has_nonfinite_ratio_all_targets: "
            f"{eq_backfill_residual_probe_has_nonfinite_ratio_all_targets}"
        )
        print(
            "- eq_backfill_residual_probe_has_nonfinite_all_targets: "
            f"{eq_backfill_residual_probe_has_nonfinite_all_targets}"
        )
        print(f"- eq_backfill_iteration_has_nonfinite: {eq_backfill_iteration_has_nonfinite}")
        print(
            "- eq_backfill_iteration_first_nonfinite_iteration: "
            f"{eq_backfill_iteration_first_nonfinite_iteration}"
        )
        print(f"- eq_backfill_tol: {eq_backfill_tol:.6g}")
        print(f"- eq_backfill_tol_mode: {eq_backfill_tol_mode}")
        print(f"- eq_backfill_tol_source: {eq_backfill_tol_source}")
        print(f"- eq_backfill_tolallabs: {eq_backfill_tolallabs}")
        print(f"- eq_backfill_delta_ratio: {eq_backfill_delta_ratio:.6g}")
        print(f"- eq_backfill_diverged: {eq_backfill_diverged}")
        print(f"- eq_backfill_dampall: {eq_backfill_dampall:.6g}")
        print(f"- eq_backfill_damp_override_count: {len(eq_backfill_damp_overrides)}")
        print(f"- eq_backfill_tol_override_count: {len(eq_backfill_tol_overrides)}")
        print(f"- eq_backfill_tolabs_override_count: {len(eq_backfill_tolabs_overrides)}")
        print(f"- eq_backfill_override_issue_count: {len(eq_backfill_override_issues)}")
        print(f"- eq_backfill_period_sequential: {eq_backfill_period_sequential}")
        print(
            "- eq_backfill_period_sequential_all_assignments: "
            f"{eq_backfill_period_sequential_all_assignments}"
        )
        print(
            "- eq_backfill_period_sequential_defer_create: "
            f"{eq_backfill_period_sequential_defer_create}"
        )
        print(
            "- eq_backfill_period_sequential_context_assignments_first_iter_only: "
            f"{eq_backfill_period_sequential_context_assignments_first_iter_only}"
        )
        print(
            "- eq_backfill_period_sequential_context_keep_create_all_iters: "
            f"{eq_backfill_period_sequential_context_keep_create_all_iters}"
        )
        print(
            "- eq_backfill_period_sequential_context_replay_command_time_smpl: "
            f"{eq_backfill_period_sequential_context_replay_command_time_smpl}"
        )
        print(
            "- eq_backfill_period_sequential_context_replay_command_time_smpl_clip_to_solve_window: "
            f"{eq_backfill_period_sequential_context_replay_command_time_smpl_clip_to_solve_window}"
        )
        print(
            "- eq_backfill_period_sequential_context_retain_overlap_only: "
            f"{eq_backfill_period_sequential_context_retain_overlap_only}"
        )
        print(
            "- eq_backfill_period_sequential_context_replay_trace: "
            f"{eq_backfill_period_sequential_context_replay_trace}"
        )
        print(
            "- eq_backfill_period_sequential_context_replay_trace_targets: "
            f"{list(eq_backfill_period_sequential_context_replay_trace_targets)}"
        )
        print(
            "- eq_backfill_period_sequential_context_replay_trace_periods: "
            f"{list(eq_backfill_period_sequential_context_replay_trace_periods)}"
        )
        print(
            "- eq_backfill_context_replay_trace_events_count: "
            f"{eq_backfill_context_replay_trace_events_count}"
        )
        print(
            "- eq_backfill_context_replay_trace_events_truncated: "
            f"{eq_backfill_context_replay_trace_events_truncated}"
        )
        if eq_backfill_context_replay_trace_path is not None:
            print(
                f"- eq_backfill_context_replay_trace_path: {eq_backfill_context_replay_trace_path}"
            )
        if eq_iter_trace_path is not None:
            print(f"- eq_iter_trace_path: {eq_iter_trace_path}")
        print(f"- eq_backfill_maxcheck: {eq_backfill_maxcheck}")
        print(f"- eq_backfill_checked_target_count: {len(eq_backfill_checked_targets)}")
        print(f"- eq_backfill_all_target_count: {len(eq_backfill_all_targets)}")
        print(f"- eq_backfill_keyboard_targets_missing: {eq_backfill_keyboard_targets_missing}")
        if eq_backfill_keyboard_targets_unmatched:
            print(
                "- eq_backfill_keyboard_targets_unmatched: "
                f"{list(eq_backfill_keyboard_targets_unmatched)}"
            )
        print(
            "- eq_backfill_keyboard_stop_classification: "
            f"{eq_backfill_keyboard_stop_classification}"
        )
        if eq_backfill_keyboard_nonconvergence_reason is not None:
            print(
                "- eq_backfill_keyboard_nonconvergence_reason: "
                f"{eq_backfill_keyboard_nonconvergence_reason}"
            )
        if eq_backfill_keyboard_stub_lag_probe_targets:
            print(
                "- eq_backfill_keyboard_stub_lag_probe_targets: "
                f"{list(eq_backfill_keyboard_stub_lag_probe_targets)}"
            )
            print(
                "- eq_backfill_keyboard_stub_lag_probe_period: "
                f"{eq_backfill_keyboard_stub_lag_probe_period}"
            )
            print(
                "- eq_backfill_keyboard_stub_lag_probe_events_count: "
                f"{len(eq_backfill_keyboard_stub_lag_probe_events)}"
            )
        print(f"- eq_backfill_top_target_deltas: {eq_backfill_top_target_deltas}")
        print(f"- eq_backfill_specs_has_rho_terms: {eq_backfill_specs_has_rho_terms}")
        print(f"- eq_backfill_rho_aware: {eq_backfill_rho_aware}")
        print(f"- eq_backfill_rho_mode_auto_selected: {eq_backfill_rho_mode_auto_selected}")
        print(f"- eq_backfill_rho_mode_auto_reason: {eq_backfill_rho_mode_auto_reason}")
        print(f"- eq_backfill_rho_resid_ar1: {eq_backfill_rho_resid_ar1}")
        if eq_backfill_rho_mode_warnings:
            print(f"- eq_backfill_rho_mode_warnings: {eq_backfill_rho_mode_warnings}")
        print(f"- eq_backfill_rho_resid_ar1_start_iter: {eq_backfill_rho_resid_ar1_start_iter}")
        print(
            f"- eq_backfill_rho_resid_ar1_disable_iter: {eq_backfill_rho_resid_ar1_disable_iter}"
        )
        print(
            "- eq_backfill_rho_resid_ar1_effective_last: "
            f"{eq_backfill_rho_resid_ar1_effective_last}"
        )
        print(
            "- eq_backfill_rho_resid_ar1_carry_iterations: "
            f"{eq_backfill_rho_resid_ar1_carry_iterations}"
        )
        print(
            "- eq_backfill_rho_resid_ar1_commit_after_check: "
            f"{eq_backfill_rho_resid_ar1_commit_after_check}"
        )
        print(f"- eq_backfill_rho_resid_ar1_commit_mode: {eq_backfill_rho_resid_ar1_commit_mode}")
        print(
            "- eq_backfill_rho_resid_ar1_residual_probe_seed_mode: "
            f"{eq_backfill_rho_resid_ar1_residual_probe_seed_mode}"
        )
        print(
            "- eq_backfill_rho_resid_ar1_trace_event_limit: "
            f"{eq_backfill_rho_resid_ar1_trace_event_limit}"
        )
        print(f"- eq_backfill_rho_resid_ar1_fpe_trap: {eq_backfill_rho_resid_ar1_fpe_trap}")
        print(
            "- eq_backfill_rho_resid_iteration_state_size: "
            f"{eq_backfill_rho_resid_iteration_state_size}"
        )
        if eq_backfill_rho_resid_iteration_state_top:
            print(
                "- eq_backfill_rho_resid_iteration_state_top: "
                f"{eq_backfill_rho_resid_iteration_state_top}"
            )
        if eq_backfill_rho_resid_iteration_trace_last:
            print(
                "- eq_backfill_rho_resid_iteration_trace_lines: "
                f"{eq_backfill_rho_resid_iteration_trace_last}"
            )
        if eq_backfill_rho_resid_ar1_trace_event_count > 0:
            print(
                "- eq_backfill_rho_resid_ar1_trace_event_count: "
                f"{eq_backfill_rho_resid_ar1_trace_event_count}"
            )
            print(
                "- eq_backfill_rho_resid_ar1_trace_events_total_count: "
                f"{eq_backfill_rho_resid_ar1_trace_events_total_count}"
            )
            print(
                "- eq_backfill_rho_resid_ar1_trace_events_serialized_count: "
                f"{eq_backfill_rho_resid_ar1_trace_events_serialized_count}"
            )
            print(
                "- eq_backfill_rho_resid_ar1_trace_events_serialization_policy: "
                f"{eq_backfill_rho_resid_ar1_trace_events_serialization_policy}"
            )
            if eq_backfill_rho_resid_ar1_trace_event_kind_counts:
                print(
                    "- eq_backfill_rho_resid_ar1_trace_event_kind_counts: "
                    f"{eq_backfill_rho_resid_ar1_trace_event_kind_counts}"
                )
            if eq_backfill_rho_resid_ar1_trace_event_phase_counts:
                print(
                    "- eq_backfill_rho_resid_ar1_trace_event_phase_counts: "
                    f"{eq_backfill_rho_resid_ar1_trace_event_phase_counts}"
                )
            if eq_backfill_rho_resid_ar1_trace_event_pass_counts:
                print(
                    "- eq_backfill_rho_resid_ar1_trace_event_pass_counts: "
                    f"{eq_backfill_rho_resid_ar1_trace_event_pass_counts}"
                )
            if eq_backfill_rho_resid_ar1_trace_event_carry_reject_reason_counts:
                print(
                    "- eq_backfill_rho_resid_ar1_trace_event_carry_reject_reason_counts: "
                    f"{eq_backfill_rho_resid_ar1_trace_event_carry_reject_reason_counts}"
                )
            if eq_backfill_rho_resid_ar1_trace_events_truncated:
                print("- eq_backfill_rho_resid_ar1_trace_events_truncated: True")
        if eq_backfill_rho_resid_ar1_trace_focus_spec_payload is not None:
            print(
                "- eq_backfill_rho_resid_ar1_trace_focus_spec: "
                f"{eq_backfill_rho_resid_ar1_trace_focus_spec_payload}"
            )
            print(
                "- eq_backfill_rho_resid_ar1_trace_focus_events_count: "
                f"{eq_backfill_rho_resid_ar1_trace_focus_events_count}"
            )
            print(
                "- eq_backfill_rho_resid_ar1_trace_focus_events_truncated_by_limit: "
                f"{eq_backfill_rho_resid_ar1_trace_focus_events_truncated_by_limit}"
            )
            print(
                "- eq_backfill_rho_resid_ar1_trace_focus_no_match: "
                f"{eq_backfill_rho_resid_ar1_trace_focus_no_match}"
            )
            if eq_backfill_rho_resid_ar1_trace_focus_event_counts_by_stage:
                print(
                    "- eq_backfill_rho_resid_ar1_trace_focus_event_counts_by_stage: "
                    f"{eq_backfill_rho_resid_ar1_trace_focus_event_counts_by_stage}"
                )
        print(f"- eq_backfill_rho_resid_ar1_seed_lag: {eq_backfill_rho_resid_ar1_seed_lag}")
        print(f"- eq_backfill_rho_resid_ar1_carry_lag: {eq_backfill_rho_resid_ar1_carry_lag}")
        print(
            f"- eq_backfill_rho_resid_ar1_carry_damp: {eq_backfill_rho_resid_ar1_carry_damp:.6g}"
        )
        print(
            "- eq_backfill_rho_resid_ar1_carry_damp_mode: "
            f"{eq_backfill_rho_resid_ar1_carry_damp_mode}"
        )
        print(
            "- eq_backfill_rho_resid_ar1_carry_multipass: "
            f"{eq_backfill_rho_resid_ar1_carry_multipass}"
        )
        print(f"- eq_backfill_rho_resid_ar1_seed_mode: {eq_backfill_rho_resid_ar1_seed_mode}")
        print(
            "- eq_backfill_rho_resid_ar1_boundary_reset: "
            f"{eq_backfill_rho_resid_ar1_boundary_reset}"
        )
        print(f"- eq_backfill_rho_resid_ar1_lag_gating: {eq_backfill_rho_resid_ar1_lag_gating}")
        print(
            f"- eq_backfill_rho_resid_ar1_update_source: {eq_backfill_rho_resid_ar1_update_source}"
        )
        print(
            "- eq_backfill_rho_resid_ar1_la257_update_rule: "
            f"{eq_backfill_rho_resid_ar1_la257_update_rule}"
        )
        print(
            "- eq_backfill_rho_resid_ar1_la257_start_iter: "
            f"{eq_backfill_rho_resid_ar1_la257_start_iter}"
        )
        print(
            "- eq_backfill_rho_resid_ar1_la257_disable_iter: "
            f"{eq_backfill_rho_resid_ar1_la257_disable_iter}"
        )
        print(
            "- eq_backfill_rho_resid_ar1_la257_update_rule_effective_last: "
            f"{eq_backfill_rho_resid_ar1_la257_update_rule_effective_last}"
        )
        print(
            "- eq_backfill_rho_resid_ar1_la257_fortran_cycle_carry_style: "
            f"{eq_backfill_rho_resid_ar1_la257_fortran_cycle_carry_style}"
        )
        print(
            "- eq_backfill_rho_resid_ar1_la257_u_phase_max_gain: "
            f"{eq_backfill_rho_resid_ar1_la257_u_phase_max_gain}"
        )
        print(
            "- eq_backfill_rho_resid_ar1_la257_u_phase_max_gain_mode: "
            f"{eq_backfill_rho_resid_ar1_la257_u_phase_max_gain_mode}"
        )
        print(
            "- eq_backfill_rho_resid_ar1_la257_lifecycle_staging: "
            f"{eq_backfill_rho_resid_ar1_la257_lifecycle_staging}"
        )
        if eq_backfill_rho_resid_ar1_la257_u_phase_lines:
            print(
                "- eq_backfill_rho_resid_ar1_la257_u_phase_lines: "
                f"{eq_backfill_rho_resid_ar1_la257_u_phase_lines_display}"
            )
        if eq_backfill_first_nonfinite_blame is not None:
            print(f"- eq_backfill_first_nonfinite_blame: {eq_backfill_first_nonfinite_blame}")
        if eq_backfill_first_failure_blame is not None:
            print(f"- eq_backfill_first_failure_blame: {eq_backfill_first_failure_blame}")
        if eq_backfill_hf_lhf1_ho_probe_history:
            print(
                f"- eq_backfill_hf_lhf1_ho_probe_last: {eq_backfill_hf_lhf1_ho_probe_history[-1]}"
            )
        print(f"- eq_backfill_converged_checked_only: {eq_backfill_converged_checked_only}")
        print(f"- eq_backfill_stop_reason: {eq_backfill_stop_reason}")
        print(
            "- eq_seed: "
            f"mode={eq_seed_report.get('mode')} "
            f"status={eq_seed_report.get('status')} "
            f"cells_written={eq_seed_report.get('cells_written')}"
        )
        print(f"- eq_filter_use_full_context: {bool(eq_filter_use_full_context)}")
        if eq_backfill_nomiss_required:
            print(f"- eq_backfill_nomiss_missing_cells: {eq_backfill_nomiss_missing_cells}")
            print(
                f"- eq_backfill_nomiss_target_diagnostics: {eq_backfill_nomiss_target_diagnostics}"
            )
    if parity_report.get("enabled"):
        print(f"- parity_status: {parity_report.get('status')}")
        print(f"- parity_shared_periods: {parity_report.get('shared_periods')}")
        print(f"- parity_shared_variables: {parity_report.get('shared_variables')}")
        print(f"- parity_comparable_cells: {parity_report.get('comparable_cells')}")
        print(f"- parity_mismatch_count: {parity_report.get('mismatch_count')}")
        print(f"- parity_mismatch_rate: {parity_report.get('mismatch_rate'):.6g}")
        print(f"- parity_atol: {parity_report.get('atol')}")
    if solver_trace_enabled:
        print(f"- solver_trace_events_count: {solver_trace_event_count}")
        print(f"- solver_trace_path: {solver_trace_path}")

    if unsupported_command_count:
        print(f"- unsupported commands: {unsupported_command_count}")
        print("- unsupported examples:")
        for example in unsupported_examples[:10]:
            line = example.get("line") if isinstance(example, dict) else None
            command = example.get("command") if isinstance(example, dict) else None
            statement = example.get("statement") if isinstance(example, dict) else None
            print(f"  line={line} command={command} statement={statement}")
        if len(unsupported_examples) > 10:
            print(f"  ... and {len(unsupported_examples) - 10} more")

    if output_csv is not None:
        output_path = Path(output_csv)
        output_frame.to_csv(output_path)

    if enable_eq and eq_require_convergence and not eq_backfill_converged:
        issues.append({
            "line": None,
            "statement": "EQ_CONVERGENCE_GATE",
            "error": (
                "EQ backfill did not converge "
                f"(stop_reason={eq_backfill_stop_reason}, "
                f"iterations={eq_backfill_iterations}, "
                f"max_abs_delta={eq_backfill_max_abs_delta:.6g})"
            ),
        })
        print("Mini-run convergence gate: failed (eq backfill did not converge)")
        exit_code = 1
    if enable_eq and eq_fail_on_diverged and eq_backfill_diverged:
        issues.append({
            "line": None,
            "statement": "EQ_DIVERGENCE_GATE",
            "error": (
                "EQ backfill diverged "
                f"(delta_ratio={eq_backfill_delta_ratio:.6g}, "
                f"iterations={eq_backfill_iterations}, "
                f"max_abs_delta={eq_backfill_max_abs_delta:.6g})"
            ),
        })
        print("Mini-run divergence gate: failed (eq backfill diverged)")
        exit_code = 1
    if (
        enable_eq
        and eq_fail_on_residual
        and (
            eq_backfill_residual_failed > 0
            or eq_backfill_residual_ratio > eq_backfill_residual_ratio_max
        )
    ):
        issues.append({
            "line": None,
            "statement": "EQ_RESIDUAL_GATE",
            "error": (
                "EQ backfill residual gate failed "
                f"(residual_ratio={eq_backfill_residual_ratio:.6g}, "
                f"residual_ratio_max={eq_backfill_residual_ratio_max:.6g}, "
                f"residual_failed={eq_backfill_residual_failed})"
            ),
        })
        print("Mini-run residual gate: failed (eq backfill residual exceeds threshold)")
        exit_code = 1
    if (
        enable_eq
        and eq_fail_on_residual_all_targets
        and (
            eq_backfill_residual_failed_all_targets > 0
            or eq_backfill_residual_ratio_all_targets > eq_backfill_residual_ratio_max
        )
    ):
        issues.append({
            "line": None,
            "statement": "EQ_RESIDUAL_ALL_TARGETS_GATE",
            "error": (
                "EQ backfill all-target residual gate failed "
                f"(residual_ratio_all_targets={eq_backfill_residual_ratio_all_targets:.6g}, "
                f"residual_ratio_max={eq_backfill_residual_ratio_max:.6g}, "
                f"residual_failed_all_targets={eq_backfill_residual_failed_all_targets})"
            ),
        })
        print(
            "Mini-run all-target residual gate: failed "
            "(eq backfill residual exceeds threshold outside MAXCHECK scope)"
        )
        exit_code = 1
    if (
        enable_eq
        and eq_backfill_modeq_fsr_side_channel == "enforce"
        and eq_backfill_modeq_fsr_side_channel_breach_count > 0
    ):
        issues.append({
            "line": None,
            "statement": "EQ_MODEQ_FSR_SIDE_CHANNEL_ENFORCE_GATE",
            "error": (
                "MODEQ/FSR side-channel enforce gate failed "
                f"(breach_count={eq_backfill_modeq_fsr_side_channel_breach_count}, "
                f"status={eq_backfill_modeq_fsr_side_channel_status})"
            ),
        })
        print("Mini-run MODEQ/FSR side-channel gate: failed (enforce mode breach detected)")
        exit_code = 1

    if solver_trace_enabled and solver_trace_path is not None:
        solver_trace_event_count = len(solver_trace_events)
        trace_path = Path(solver_trace_path)
        trace_path.parent.mkdir(parents=True, exist_ok=True)
        with trace_path.open("w", encoding="utf-8") as handle:
            for event in solver_trace_events:
                handle.write(
                    json.dumps(
                        event,
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                )
                handle.write("\n")

    if (
        eq_backfill_period_sequential_context_replay_trace
        and eq_backfill_context_replay_trace_path is not None
    ):
        context_trace_path = Path(eq_backfill_context_replay_trace_path)
        context_trace_path.parent.mkdir(parents=True, exist_ok=True)
        context_trace_payload = {
            "events_count": eq_backfill_context_replay_trace_events_count,
            "events_truncated": bool(eq_backfill_context_replay_trace_events_truncated),
            "events": eq_backfill_context_replay_trace_events,
        }
        context_trace_path.write_text(
            json.dumps(context_trace_payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    eq_flags_preset_resolved = _resolve_eq_flags_preset_label(
        eq_flags_preset_label=eq_flags_preset_label,
        enable_eq=bool(enable_eq),
        eq_use_setupsolve=bool(eq_use_setupsolve),
        eq_period_sequential=bool(eq_backfill_period_sequential),
        eq_period_scoped=str(eq_period_scoped),
        eq_period_sequential_context_assignments_first_iter_only=bool(
            eq_backfill_period_sequential_context_assignments_first_iter_only
        ),
    )

    if report_json is not None:

        def _resolve_producer_version() -> str:
            # fppy is vendored inside fp-wraptr; prefer fp-wraptr's version.
            try:
                from fp_wraptr import __version__ as _v

                return str(_v)
            except Exception:
                pass
            try:
                import importlib.metadata as _md

                return str(_md.version("fp-wraptr"))
            except Exception:
                return "unknown"

        report_path = Path(report_json)
        report_payload = {
            "schema_version": 1,
            "producer_version": _resolve_producer_version(),
            "summary": {
                "records": records_count,
                "planned": planned,
                "executed": executed,
                "effective_planned": effective_planned,
                "effective_executed": effective_executed,
                "mini_run_runtime_total_seconds": mini_run_runtime_total_seconds,
                "failed": failed,
                "unsupported": unsupported_command_count,
                "unsupported_counts": unsupported_counts,
                "unsupported_examples": unsupported_examples,
                "eq_requested": enable_eq_requested,
                "eq_auto_enabled_from_solve": auto_enable_eq_from_solve,
                "eq_flags_preset": eq_flags_preset_resolved,
                "eq_use_setupsolve": bool(eq_use_setupsolve),
                "setupsolve": setupsolve_summary,
                "eq_solve_line_count": len(solve_line_numbers),
                "solve_active_line": solve_active_line,
                "solve_active_window": (
                    {
                        "start": str(solve_active_window.start),
                        "end": str(solve_active_window.end),
                    }
                    if isinstance(solve_active_window, SampleWindow)
                    else None
                ),
                "solve_active_outside": solve_active_outside,
                "solve_active_noreset": solve_active_noreset,
                "solve_active_filevar": solve_active_filevar,
                "solve_active_keyboard_targets": list(solve_active_keyboard_targets),
                "solve_outside_seeded_cells": solve_outside_seeded_cells,
                "solve_outside_seed_inspected_cells": solve_outside_seed_inspected_cells,
                "solve_outside_seed_candidate_cells": solve_outside_seed_candidate_cells,
                "eq_backfill_auto_keyboard_stub_active": (eq_backfill_auto_keyboard_stub_active),
                "eq_backfill_auto_keyboard_stub_period_sequential_enabled": (
                    eq_backfill_auto_keyboard_stub_period_sequential_enabled
                ),
                "eq_backfill_auto_keyboard_stub_period_scoped_enabled": (
                    eq_backfill_auto_keyboard_stub_period_scoped_enabled
                ),
                "eq_backfill_auto_keyboard_stub_min_iters_enabled": (
                    eq_backfill_auto_keyboard_stub_min_iters_enabled
                ),
                "eq_backfill_auto_keyboard_stub_all_assignments_enabled": (
                    eq_backfill_auto_keyboard_stub_all_assignments_enabled
                ),
                "eq_backfill_auto_keyboard_stub_context_first_iter_enabled": (
                    eq_backfill_auto_keyboard_stub_context_first_iter_enabled
                ),
                "eq_backfill_auto_keyboard_stub_context_first_iter_disabled_for_min_iters": (
                    eq_backfill_auto_keyboard_stub_context_first_iter_disabled_for_min_iters
                ),
                "eq_backfill_outside_post_seed_cells": eq_backfill_outside_post_seed_cells,
                "eq_backfill_outside_post_seed_inspected_cells": (
                    eq_backfill_outside_post_seed_inspected_cells
                ),
                "eq_backfill_outside_post_seed_candidate_cells": (
                    eq_backfill_outside_post_seed_candidate_cells
                ),
                "solve_post_commands_replayed": solve_post_commands_replayed,
                "solve_post_replay_issue_count": solve_post_replay_issue_count,
                "eq_backfill_applied": eq_backfill_applied,
                "eq_backfill_failed": eq_backfill_failed,
                "eq_backfill_modeq_fsr_updates": eq_backfill_modeq_fsr_updates,
                "eq_backfill_modeq_fsr_active_equation_count": (
                    eq_backfill_modeq_fsr_active_equation_count
                ),
                "eq_backfill_modeq_fsr_active_term_count": (
                    eq_backfill_modeq_fsr_active_term_count
                ),
                "eq_backfill_eq_fsr_skipped": eq_backfill_eq_fsr_skipped,
                "eq_backfill_modeq_fsr_side_channel": (eq_backfill_modeq_fsr_side_channel),
                "eq_backfill_modeq_fsr_side_channel_status": (
                    eq_backfill_modeq_fsr_side_channel_status
                ),
                "eq_backfill_modeq_fsr_side_channel_effective_limit": (
                    eq_backfill_modeq_fsr_side_channel_effective_limit
                ),
                "eq_backfill_modeq_fsr_side_channel_events_count": (
                    eq_backfill_modeq_fsr_side_channel_events_count
                ),
                "eq_backfill_modeq_fsr_side_channel_events_truncated": (
                    eq_backfill_modeq_fsr_side_channel_events_truncated
                ),
                "eq_backfill_modeq_fsr_side_channel_breach_count": (
                    eq_backfill_modeq_fsr_side_channel_breach_count
                ),
                "eq_backfill_modeq_fsr_side_channel_mutation_count": (
                    eq_backfill_modeq_fsr_side_channel_mutation_count
                ),
                "eq_backfill_modeq_fsr_side_channel_first_breach": (
                    eq_backfill_modeq_fsr_side_channel_first_breach
                ),
                "eq_backfill_modeq_fsr_side_channel_events": (
                    eq_backfill_modeq_fsr_side_channel_events
                ),
                "eq_backfill_iterations": eq_backfill_iterations,
                "eq_backfill_runtime_total_seconds": eq_backfill_runtime_total_seconds,
                "eq_backfill_runtime_build_records_seconds": (
                    eq_backfill_runtime_build_records_seconds
                ),
                "eq_backfill_runtime_setupsolve_seconds": eq_backfill_runtime_setupsolve_seconds,
                "eq_backfill_runtime_specs_seconds": eq_backfill_runtime_specs_seconds,
                "eq_backfill_runtime_window_prep_seconds": eq_backfill_runtime_window_prep_seconds,
                "eq_backfill_runtime_iteration_seconds": eq_backfill_runtime_iteration_seconds,
                "eq_backfill_runtime_iteration_apply_seconds": (
                    eq_backfill_runtime_iteration_apply_seconds
                ),
                "eq_backfill_runtime_post_solve_replay_seconds": (
                    eq_backfill_runtime_post_solve_replay_seconds
                ),
                "eq_backfill_runtime_iteration_apply_calls": (
                    eq_backfill_runtime_iteration_apply_calls
                ),
                "eq_backfill_record_count": eq_backfill_record_count,
                "eq_backfill_attempted_records": eq_backfill_attempted_records,
                "eq_backfill_iteration_history": eq_backfill_iteration_history,
                "eq_backfill_hf_line182_overflow_probe_history": (
                    eq_backfill_hf_line182_overflow_probe_history
                ),
                "eq_backfill_hf_line182_overflow_probe_last": (
                    eq_backfill_hf_line182_overflow_probe_history[-1]
                    if eq_backfill_hf_line182_overflow_probe_history
                    else None
                ),
                "eq_backfill_hf_lhf1_ho_probe_history": (eq_backfill_hf_lhf1_ho_probe_history),
                "eq_backfill_hf_lhf1_ho_probe_last": (
                    eq_backfill_hf_lhf1_ho_probe_history[-1]
                    if eq_backfill_hf_lhf1_ho_probe_history
                    else None
                ),
                "eq_backfill_converged": eq_backfill_converged,
                "eq_backfill_max_abs_delta": eq_backfill_max_abs_delta,
                "eq_backfill_min_abs_delta": eq_backfill_min_abs_delta,
                "eq_backfill_convergence_delta": eq_backfill_convergence_delta,
                "eq_backfill_convergence_ratio": eq_backfill_convergence_ratio,
                "eq_backfill_convergence_delta_all_targets": (
                    eq_backfill_convergence_delta_all_targets
                ),
                "eq_backfill_convergence_ratio_all_targets": (
                    eq_backfill_convergence_ratio_all_targets
                ),
                "eq_backfill_residual_abs_delta": eq_backfill_residual_abs_delta,
                "eq_backfill_residual_ratio": eq_backfill_residual_ratio,
                "eq_backfill_residual_abs_delta_all_targets": (
                    eq_backfill_residual_abs_delta_all_targets
                ),
                "eq_backfill_residual_ratio_all_targets": (eq_backfill_residual_ratio_all_targets),
                "eq_backfill_residual_ratio_max": eq_backfill_residual_ratio_max,
                "eq_backfill_residual_failed": eq_backfill_residual_failed,
                "eq_backfill_residual_issue_count": eq_backfill_residual_issue_count,
                "eq_backfill_residual_probe_has_nonfinite_delta": (
                    eq_backfill_residual_probe_has_nonfinite_delta
                ),
                "eq_backfill_residual_probe_has_nonfinite_ratio": (
                    eq_backfill_residual_probe_has_nonfinite_ratio
                ),
                "eq_backfill_residual_probe_has_nonfinite": (
                    eq_backfill_residual_probe_has_nonfinite
                ),
                "eq_backfill_residual_failed_all_targets": (
                    eq_backfill_residual_failed_all_targets
                ),
                "eq_backfill_residual_issue_count_all_targets": (
                    eq_backfill_residual_issue_count_all_targets
                ),
                "eq_backfill_residual_probe_has_nonfinite_delta_all_targets": (
                    eq_backfill_residual_probe_has_nonfinite_delta_all_targets
                ),
                "eq_backfill_residual_probe_has_nonfinite_ratio_all_targets": (
                    eq_backfill_residual_probe_has_nonfinite_ratio_all_targets
                ),
                "eq_backfill_residual_probe_has_nonfinite_all_targets": (
                    eq_backfill_residual_probe_has_nonfinite_all_targets
                ),
                "eq_backfill_iteration_has_nonfinite": eq_backfill_iteration_has_nonfinite,
                "eq_backfill_iteration_first_nonfinite_iteration": (
                    eq_backfill_iteration_first_nonfinite_iteration
                ),
                "eq_backfill_tol": eq_backfill_tol,
                "eq_backfill_tol_mode": eq_backfill_tol_mode,
                "eq_backfill_tol_source": eq_backfill_tol_source,
                "eq_backfill_tolallabs": eq_backfill_tolallabs,
                "eq_backfill_last_abs_delta": eq_backfill_last_abs_delta,
                "eq_backfill_delta_ratio": eq_backfill_delta_ratio,
                "eq_backfill_diverged": eq_backfill_diverged,
                "eq_backfill_dampall": eq_backfill_dampall,
                "eq_backfill_damp_overrides": eq_backfill_damp_overrides,
                "eq_backfill_tol_overrides": eq_backfill_tol_overrides,
                "eq_backfill_tolabs_overrides": sorted(eq_backfill_tolabs_overrides),
                "eq_backfill_override_issues": eq_backfill_override_issues,
                "eq_backfill_period_sequential": eq_backfill_period_sequential,
                "eq_backfill_period_sequential_all_assignments": (
                    eq_backfill_period_sequential_all_assignments
                ),
                "eq_backfill_period_sequential_defer_create": (
                    eq_backfill_period_sequential_defer_create
                ),
                "eq_backfill_period_sequential_context_assignments_first_iter_only": (
                    eq_backfill_period_sequential_context_assignments_first_iter_only
                ),
                "eq_backfill_period_sequential_context_keep_create_all_iters": (
                    eq_backfill_period_sequential_context_keep_create_all_iters
                ),
                "eq_backfill_period_sequential_context_replay_command_time_smpl": (
                    eq_backfill_period_sequential_context_replay_command_time_smpl
                ),
                "eq_backfill_period_sequential_context_replay_command_time_smpl_clip_to_solve_window": (
                    eq_backfill_period_sequential_context_replay_command_time_smpl_clip_to_solve_window
                ),
                "eq_backfill_period_sequential_context_retain_overlap_only": (
                    eq_backfill_period_sequential_context_retain_overlap_only
                ),
                "eq_backfill_period_sequential_context_replay_trace": (
                    eq_backfill_period_sequential_context_replay_trace
                ),
                "eq_backfill_period_sequential_context_replay_trace_max_events": (
                    eq_backfill_period_sequential_context_replay_trace_max_events
                ),
                "eq_backfill_period_sequential_context_replay_trace_targets": (
                    list(eq_backfill_period_sequential_context_replay_trace_targets)
                ),
                "eq_backfill_period_sequential_context_replay_trace_periods": (
                    list(eq_backfill_period_sequential_context_replay_trace_periods)
                ),
                "eq_backfill_quantize_store": eq_backfill_quantize_store,
                "eq_backfill_quantize_store_cells": eq_backfill_quantize_store_cells,
                "eq_backfill_context_replay_trace_events_count": (
                    eq_backfill_context_replay_trace_events_count
                ),
                "eq_backfill_context_replay_trace_events_truncated": (
                    eq_backfill_context_replay_trace_events_truncated
                ),
                "eq_backfill_context_replay_trace_path": (eq_backfill_context_replay_trace_path),
                "eq_backfill_context_replay_trace_events": (
                    eq_backfill_context_replay_trace_events
                ),
                "eq_iter_trace_path": eq_iter_trace_path,
                "eq_iter_trace_period": (eq_iter_trace_period_resolved if eq_iter_trace else None),
                "eq_iter_trace_targets": (
                    list(eq_iter_trace_targets_parsed) if eq_iter_trace else []
                ),
                "eq_iter_trace_events_count": (eq_iter_trace_events_count if eq_iter_trace else 0),
                "eq_iter_trace_events_truncated": (
                    eq_iter_trace_events_truncated if eq_iter_trace else False
                ),
                "eq_backfill_stop_reason": eq_backfill_stop_reason,
                "eq_backfill_min_iters": eq_backfill_min_iters,
                "eq_backfill_max_iters": eq_backfill_max_iters,
                "eq_backfill_maxcheck": eq_backfill_maxcheck,
                "eq_backfill_checked_targets": list(eq_backfill_checked_targets),
                "eq_backfill_all_targets": list(eq_backfill_all_targets),
                "eq_backfill_keyboard_targets_missing": (eq_backfill_keyboard_targets_missing),
                "eq_backfill_keyboard_targets_unmatched": list(
                    eq_backfill_keyboard_targets_unmatched
                ),
                "eq_backfill_keyboard_stop_classification": (
                    eq_backfill_keyboard_stop_classification
                ),
                "eq_backfill_keyboard_nonconvergence_reason": (
                    eq_backfill_keyboard_nonconvergence_reason
                ),
                "eq_backfill_keyboard_stub_lag_probe_targets": list(
                    eq_backfill_keyboard_stub_lag_probe_targets
                ),
                "eq_backfill_keyboard_stub_lag_probe_period": (
                    eq_backfill_keyboard_stub_lag_probe_period
                ),
                "eq_backfill_keyboard_stub_lag_probe_events_count": len(
                    eq_backfill_keyboard_stub_lag_probe_events
                ),
                "eq_backfill_keyboard_stub_lag_probe_events": (
                    eq_backfill_keyboard_stub_lag_probe_events
                ),
                "eq_backfill_top_target_deltas": eq_backfill_top_target_deltas,
                "eq_backfill_specs_has_rho_terms": eq_backfill_specs_has_rho_terms,
                "eq_backfill_rho_aware": eq_backfill_rho_aware,
                "eq_backfill_rho_mode_auto_selected": (eq_backfill_rho_mode_auto_selected),
                "eq_backfill_rho_mode_auto_reason": eq_backfill_rho_mode_auto_reason,
                "eq_backfill_rho_mode_warnings": eq_backfill_rho_mode_warnings,
                "eq_backfill_rho_resid_ar1": eq_backfill_rho_resid_ar1,
                "eq_backfill_rho_resid_ar1_start_iter": eq_backfill_rho_resid_ar1_start_iter,
                "eq_backfill_rho_resid_ar1_disable_iter": (eq_backfill_rho_resid_ar1_disable_iter),
                "eq_backfill_rho_resid_ar1_effective_last": (
                    eq_backfill_rho_resid_ar1_effective_last
                ),
                "eq_backfill_rho_resid_ar1_carry_iterations": (
                    eq_backfill_rho_resid_ar1_carry_iterations
                ),
                "eq_backfill_rho_resid_ar1_commit_after_check": (
                    eq_backfill_rho_resid_ar1_commit_after_check
                ),
                "eq_backfill_rho_resid_ar1_commit_mode": (eq_backfill_rho_resid_ar1_commit_mode),
                "eq_backfill_rho_resid_ar1_residual_probe_seed_mode": (
                    eq_backfill_rho_resid_ar1_residual_probe_seed_mode
                ),
                "eq_backfill_rho_resid_iteration_state_size": (
                    eq_backfill_rho_resid_iteration_state_size
                ),
                "eq_backfill_rho_resid_iteration_state_top": (
                    eq_backfill_rho_resid_iteration_state_top
                ),
                "eq_backfill_rho_resid_ar1_trace_lines": (
                    eq_backfill_rho_resid_ar1_trace_lines_display
                ),
                "eq_backfill_rho_resid_ar1_trace_event_limit": (
                    eq_backfill_rho_resid_ar1_trace_event_limit
                ),
                "eq_backfill_rho_resid_ar1_fpe_trap": (eq_backfill_rho_resid_ar1_fpe_trap),
                "eq_backfill_rho_resid_iteration_trace_lines": (
                    eq_backfill_rho_resid_iteration_trace_last
                ),
                "eq_backfill_rho_resid_ar1_trace_event_count": (
                    eq_backfill_rho_resid_ar1_trace_event_count
                ),
                "eq_backfill_rho_resid_ar1_trace_events": (eq_backfill_rho_resid_ar1_trace_events),
                "eq_backfill_rho_resid_ar1_trace_events_total_count": (
                    eq_backfill_rho_resid_ar1_trace_events_total_count
                ),
                "eq_backfill_rho_resid_ar1_trace_events_serialized_count": (
                    eq_backfill_rho_resid_ar1_trace_events_serialized_count
                ),
                "eq_backfill_rho_resid_ar1_trace_events_serialization_policy": (
                    eq_backfill_rho_resid_ar1_trace_events_serialization_policy
                ),
                "eq_backfill_rho_resid_ar1_trace_events_sample_head_count": (
                    eq_backfill_rho_resid_ar1_trace_events_sample_head_count
                ),
                "eq_backfill_rho_resid_ar1_trace_events_sample_tail_count": (
                    eq_backfill_rho_resid_ar1_trace_events_sample_tail_count
                ),
                "eq_backfill_rho_resid_ar1_trace_events_truncated_by_event_limit": (
                    eq_backfill_rho_resid_ar1_trace_events_truncated_by_event_limit
                ),
                "eq_backfill_rho_resid_ar1_trace_events_sampled": (
                    eq_backfill_rho_resid_ar1_trace_events_sampled
                ),
                "eq_backfill_rho_resid_ar1_trace_event_kind_counts": (
                    eq_backfill_rho_resid_ar1_trace_event_kind_counts
                ),
                "eq_backfill_rho_resid_ar1_trace_event_phase_counts": (
                    eq_backfill_rho_resid_ar1_trace_event_phase_counts
                ),
                "eq_backfill_rho_resid_ar1_trace_event_pass_counts": (
                    eq_backfill_rho_resid_ar1_trace_event_pass_counts
                ),
                "eq_backfill_rho_resid_ar1_trace_event_carry_reject_reason_counts": (
                    eq_backfill_rho_resid_ar1_trace_event_carry_reject_reason_counts
                ),
                "eq_backfill_rho_resid_ar1_trace_events_truncated": (
                    eq_backfill_rho_resid_ar1_trace_events_truncated
                ),
                "eq_backfill_rho_resid_ar1_trace_focus_spec": (
                    eq_backfill_rho_resid_ar1_trace_focus_spec_payload
                ),
                "eq_backfill_rho_resid_ar1_trace_focus_events_count": (
                    eq_backfill_rho_resid_ar1_trace_focus_events_count
                ),
                "eq_backfill_rho_resid_ar1_trace_focus_events_truncated_by_limit": (
                    eq_backfill_rho_resid_ar1_trace_focus_events_truncated_by_limit
                ),
                "eq_backfill_rho_resid_ar1_trace_focus_no_match": (
                    eq_backfill_rho_resid_ar1_trace_focus_no_match
                ),
                "eq_backfill_rho_resid_ar1_trace_focus_event_counts_by_stage": (
                    eq_backfill_rho_resid_ar1_trace_focus_event_counts_by_stage
                ),
                "eq_backfill_rho_resid_ar1_trace_focus_events": (
                    eq_backfill_rho_resid_ar1_trace_focus_events
                ),
                "eq_backfill_rho_resid_ar1_seed_lag": eq_backfill_rho_resid_ar1_seed_lag,
                "eq_backfill_rho_resid_ar1_carry_lag": eq_backfill_rho_resid_ar1_carry_lag,
                "eq_backfill_rho_resid_ar1_carry_damp": eq_backfill_rho_resid_ar1_carry_damp,
                "eq_backfill_rho_resid_ar1_carry_damp_mode": (
                    eq_backfill_rho_resid_ar1_carry_damp_mode
                ),
                "eq_backfill_rho_resid_ar1_carry_multipass": (
                    eq_backfill_rho_resid_ar1_carry_multipass
                ),
                "eq_backfill_rho_resid_ar1_seed_mode": eq_backfill_rho_resid_ar1_seed_mode,
                "eq_backfill_rho_resid_ar1_boundary_reset": (
                    eq_backfill_rho_resid_ar1_boundary_reset
                ),
                "eq_backfill_rho_resid_ar1_lag_gating": (eq_backfill_rho_resid_ar1_lag_gating),
                "eq_backfill_rho_resid_ar1_update_source": (
                    eq_backfill_rho_resid_ar1_update_source
                ),
                "eq_backfill_rho_resid_ar1_la257_update_rule": (
                    eq_backfill_rho_resid_ar1_la257_update_rule
                ),
                "eq_backfill_rho_resid_ar1_la257_start_iter": (
                    eq_backfill_rho_resid_ar1_la257_start_iter
                ),
                "eq_backfill_rho_resid_ar1_la257_disable_iter": (
                    eq_backfill_rho_resid_ar1_la257_disable_iter
                ),
                "eq_backfill_rho_resid_ar1_la257_update_rule_effective_last": (
                    eq_backfill_rho_resid_ar1_la257_update_rule_effective_last
                ),
                "eq_backfill_rho_resid_ar1_la257_fortran_cycle_carry_style": (
                    eq_backfill_rho_resid_ar1_la257_fortran_cycle_carry_style
                ),
                "eq_backfill_rho_resid_ar1_la257_u_phase_max_gain": (
                    eq_backfill_rho_resid_ar1_la257_u_phase_max_gain
                ),
                "eq_backfill_rho_resid_ar1_la257_u_phase_max_gain_mode": (
                    eq_backfill_rho_resid_ar1_la257_u_phase_max_gain_mode
                ),
                "eq_backfill_rho_resid_ar1_la257_lifecycle_staging": (
                    eq_backfill_rho_resid_ar1_la257_lifecycle_staging
                ),
                "eq_backfill_rho_resid_ar1_la257_u_phase_lines": (
                    eq_backfill_rho_resid_ar1_la257_u_phase_lines_display
                ),
                "eq_backfill_first_nonfinite_blame": eq_backfill_first_nonfinite_blame,
                "eq_backfill_first_failure_blame": eq_backfill_first_failure_blame,
                "eq_backfill_converged_checked_only": eq_backfill_converged_checked_only,
                "eq_backfill_nomiss_required": eq_backfill_nomiss_required,
                "eq_backfill_nomiss_missing_cells": eq_backfill_nomiss_missing_cells,
                "eq_backfill_nomiss_target_diagnostics": (eq_backfill_nomiss_target_diagnostics),
                "eq_require_convergence": eq_require_convergence,
                "eq_fail_on_diverged": eq_fail_on_diverged,
                "eq_fail_on_residual": eq_fail_on_residual,
                "eq_fail_on_residual_all_targets": eq_fail_on_residual_all_targets,
                "eq_only_counter_promoted": eq_only_counter_promoted,
                "eq_seed_mode": eq_seed_report.get("mode"),
                "eq_seed_enabled": eq_seed_report.get("enabled"),
                "eq_seed_status": eq_seed_report.get("status"),
                "eq_seed_metric": eq_seed_report.get("metric"),
                "eq_seed_fill": eq_seed_report.get("fill"),
                "eq_filter_use_full_context": bool(eq_filter_use_full_context),
                "eq_seed_variables_total": eq_seed_report.get("variables_total"),
                "eq_seed_variables_seeded": eq_seed_report.get("variables_seeded"),
                "eq_seed_variables_added": eq_seed_report.get("variables_added"),
                "eq_seed_variables_missing_source": eq_seed_report.get("variables_missing_source"),
                "eq_seed_overlap_periods": eq_seed_report.get("overlap_periods"),
                "eq_seed_cells_written": eq_seed_report.get("cells_written"),
                "eq_state_seed_enabled": state_seed_report.get("enabled"),
                "eq_state_seed_status": state_seed_report.get("status"),
                "eq_state_seed_fill": state_seed_report.get("fill"),
                "eq_state_seed_pabev": state_seed_report.get("pabev"),
                "eq_state_seed_period": state_seed_report.get("period"),
                "eq_state_seed_variables_total": state_seed_report.get("variables_total"),
                "eq_state_seed_variables_matched": state_seed_report.get("variables_matched"),
                "eq_state_seed_cells_written": state_seed_report.get("cells_written"),
                "eq_eval_precision": str(eq_eval_precision).strip().lower(),
                "eq_quantize_eq_commit": str(eq_quantize_eq_commit).strip().lower(),
                "parity_enabled": parity_report.get("enabled"),
                "parity_status": parity_report.get("status"),
                "parity_fmout_path": parity_report.get("fmout_path"),
                "parity_atol": parity_report.get("atol"),
                "parity_shared_periods": parity_report.get("shared_periods"),
                "parity_shared_variables": parity_report.get("shared_variables"),
                "parity_comparable_cells": parity_report.get("comparable_cells"),
                "parity_mismatch_count": parity_report.get("mismatch_count"),
                "parity_mismatch_rate": parity_report.get("mismatch_rate"),
                "parity_top_mismatches": parity_report.get("top_mismatches"),
                "solver_trace_enabled": solver_trace_enabled,
                "solver_trace_path": solver_trace_path,
                "solver_trace_events_count": solver_trace_event_count,
            },
            "issues": issues,
            "unsupported_examples": unsupported_examples,
        }
        report_path.write_text(
            json.dumps(report_payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    return exit_code


def _build_synthetic_baseline_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame({"SYNTH": [1.0, 2.0], "SYNTH_2": [1.0, 1.0]})

    synthetic = frame.copy(deep=True)
    first_column = synthetic.columns[0]
    values = pd.to_numeric(synthetic[first_column], errors="coerce")
    if values.empty:
        return synthetic

    if pd.isna(values.iloc[0]):
        synthetic.iloc[0, 0] = 1.0
    else:
        synthetic.iloc[0, 0] = values.iloc[0] + 1.0

    return synthetic


def _cmd_parity_summary(config_path: str | Path | None = None) -> int:
    config = _load_model_config(config_path)
    frame = load_fmout() if config_path is None else load_fmout(config.legacy.fmout)
    synthetic = _build_synthetic_baseline_frame(frame)
    comparison = compare_numeric_dataframes(frame, synthetic, atol=1e-8)

    print(f"Parsed fmout rows: {len(frame)}")
    print(f"Parsed fmout columns: {len(frame.columns)}")

    if frame.empty:
        print("No key-value series parsed from fmout.")
    else:
        print(f"Parsed fmout columns: {', '.join(frame.columns)}")

    print("Comparison summary:")
    if comparison.empty:
        print("- Shared numeric columns: 0")
        return 0

    print(f"- Shared numeric columns: {len(comparison)}")
    for column, row in comparison.iterrows():
        max_error = row["max_abs_error"]
        mismatch_rows = int(row["mismatch_rows"])
        error_text = "nan" if pd.isna(max_error) else f"{max_error:.6g}"
        print(f"- {column}: mismatches={mismatch_rows}, max_abs_error={error_text}")

    return 0


def _cmd_parity_report(
    config_path: str | Path | None = None,
    *,
    baseline: str | Path = Path(__file__).resolve().parents[2] / "FM" / "fmout.txt",
    metric: str = "lv",
    atol: float = 1e-8,
    top: int = 20,
    include_eq: bool = False,
    eq_coef_atol: float = 1e-10,
    eq_top: int = 20,
) -> int:
    config = _load_model_config(config_path)
    candidate_path = config.legacy.fmout
    baseline_path = Path(baseline)

    candidate_payload = load_fmout_structured(candidate_path)
    baseline_payload = load_fmout_structured(baseline_path)

    if candidate_payload is None:
        print(f"Unable to read structured sections from candidate fmout: {candidate_path}")
        return 1
    if baseline_payload is None:
        print(f"Unable to read structured sections from baseline fmout: {baseline_path}")
        return 1

    candidate_frame = _select_parity_metric_frame(candidate_payload, metric)
    baseline_frame = _select_parity_metric_frame(baseline_payload, metric)
    if candidate_frame.empty or len(candidate_frame.columns) == 0:
        print(f"Missing structured section '{metric}' in candidate fmout: {candidate_path}")
        return 1
    if baseline_frame.empty or len(baseline_frame.columns) == 0:
        print(f"Missing structured section '{metric}' in baseline fmout: {baseline_path}")
        return 1

    comparison = compare_numeric_dataframes(candidate_frame, baseline_frame, atol=atol)
    mismatches = (
        comparison[comparison["mismatch_rows"] > 0] if not comparison.empty else comparison
    )
    mismatch_columns = len(mismatches)

    print(f"Metric: {metric}")
    print(f"Candidate fmout rows: {len(candidate_frame)}")
    print(f"Baseline fmout rows: {len(baseline_frame)}")
    print(f"Shared numeric columns: {len(comparison)}")
    print(f"Mismatch count: {mismatch_columns}")

    numeric_status = 0
    if mismatches.empty:
        print("No mismatches found.")
    else:
        ordered = mismatches.sort_values("max_abs_error", ascending=False)
        for column, row in ordered.head(max(top, 0)).iterrows():
            max_error = row["max_abs_error"]
            error_text = "nan" if pd.isna(max_error) else f"{max_error:.6g}"
            print(
                f"- {column}: mismatches={int(row['mismatch_rows'])}, max_abs_error={error_text}"
            )
        numeric_status = 1

    eq_status = 0
    if include_eq:
        candidate_eq_specs = load_eq_specs_from_fmout(candidate_path)
        baseline_eq_specs = load_eq_specs_from_fmout(baseline_path)
        eq_comparison = compare_eq_specs(
            candidate_eq_specs,
            baseline_eq_specs,
            coef_atol=eq_coef_atol,
        )
        eq_mismatches = (
            eq_comparison[
                (eq_comparison["candidate_missing_terms"] > 0)
                | (eq_comparison["candidate_extra_terms"] > 0)
                | (eq_comparison["coef_mismatch_terms"] > 0)
                | (~eq_comparison["equation_number_match"])
            ]
            if not eq_comparison.empty
            else eq_comparison
        )

        print("Equation diagnostics:")
        print(f"- candidate equations: {len(candidate_eq_specs)}")
        print(f"- baseline equations: {len(baseline_eq_specs)}")
        print(f"- equation mismatch count: {len(eq_mismatches)}")

        if eq_mismatches.empty:
            print("- no equation-table mismatches found.")
        else:
            ordered_eq = eq_mismatches.sort_values(
                [
                    "candidate_missing_terms",
                    "candidate_extra_terms",
                    "coef_mismatch_terms",
                    "max_coef_abs_error",
                ],
                ascending=False,
            )
            for lhs, row in ordered_eq.head(max(eq_top, 0)).iterrows():
                max_error = row["max_coef_abs_error"]
                error_text = "nan" if pd.isna(max_error) else f"{max_error:.6g}"
                print(
                    f"- {lhs}: missing_terms={int(row['candidate_missing_terms'])}, "
                    f"extra_terms={int(row['candidate_extra_terms'])}, "
                    f"coef_mismatch_terms={int(row['coef_mismatch_terms'])}, "
                    f"max_coef_abs_error={error_text}"
                )
            eq_status = 1

    return 1 if (numeric_status or eq_status) else 0


def _select_parity_metric_frame(payload: FmoutStructuredData, metric: str) -> pd.DataFrame:
    if metric == "lv":
        return payload.levels
    if metric == "ch":
        return payload.changes
    if metric == "pct":
        return payload.pct_changes
    raise ValueError(f"Unsupported metric: {metric!r}")


def _cmd_input_summary(
    config_path: str | Path | None = None,
    *,
    load_sources: bool = False,
) -> int:
    config = _load_model_config(config_path)

    print("Legacy file paths:")
    for name, path in _legacy_file_rows(config):
        print(f"- {name}: {path}")

    print("External source declarations:")
    if config.external_sources:
        for source in config.external_sources:
            print(f"- {source.name}: {source.path} (format={source.format})")
    else:
        print("- (none)")

    if not load_sources:
        return 0

    loaded_sources = load_named_sources(config.external_sources)
    print("Loaded source shapes:")
    for source in config.external_sources:
        frame = loaded_sources[source.name]
        print(f"- {source.name}: {frame.shape[0]}x{frame.shape[1]}")

    return 0


def _cmd_legacy_data_summary(config_path: str | Path | None = None) -> int:
    config = _load_model_config(config_path)

    fmdata = parse_fmdata_file(config.legacy.fmdata)
    fmage = parse_fmage_file(config.legacy.fmage)
    fmexog = parse_fmexog_file(config.legacy.fmexog)

    print("Legacy data summary:")
    print(f"- fmdata: rows={len(fmdata)}, columns={len(fmdata.columns)}")
    print(f"- fmage: rows={len(fmage)}, columns={len(fmage.columns)}")
    print(
        "- fmexog: "
        f"rows={len(fmexog)}, variables={fmexog['variable'].nunique() if not fmexog.empty else 0}, "
        f"windows={fmexog[['window_start', 'window_end']].drop_duplicates().shape[0] if not fmexog.empty else 0}"
    )
    return 0


def _cmd_equation_search(
    *,
    equations_json: str | Path,
    variables_json: str | Path,
    query: str,
    limit: int = 5,
    pretty: bool = False,
    include_variable_details: bool = True,
) -> int:
    try:
        store = DictionaryStore.from_json_paths(
            equations_json=Path(equations_json),
            variables_json=Path(variables_json),
        )
    except (FileNotFoundError, OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        print(f"Equation search load error: {exc}")
        return 1

    payload = search_explain(
        query=query,
        store=store,
        limit=max(limit, 0),
        include_variable_details=include_variable_details,
    )
    print(
        json.dumps(
            payload,
            indent=2 if pretty else None,
            ensure_ascii=False,
        )
    )
    return 0


def _cmd_release_check() -> int:
    detected_paths = detect_restricted_workspace_paths(Path.cwd())
    print(format_release_check_report(detected_paths))
    return 1 if detected_paths else 0


def _cmd_export_artifacts(
    *,
    output: str | Path,
    overwrite: bool = False,
    dry_run: bool = False,
    archive_format: str | None = None,
    archive_output: str | Path | None = None,
    validate_output: bool = True,
) -> int:
    output_dir = Path(output)
    if dry_run and archive_format is not None:
        print("Export failed: --archive-format is not supported with --dry-run")
        return 1
    if archive_output is not None and archive_format is None:
        print("Export failed: --archive-output requires --archive-format")
        return 1

    try:
        exported_files = export_artifact_tree(
            Path.cwd(),
            output_dir,
            overwrite=overwrite,
            dry_run=dry_run,
        )
    except FileExistsError as exc:
        print(f"Export failed: {exc}")
        return 1

    print(f"Destination: {output_dir}")
    print(f"Total file count: {len(exported_files)}")

    if dry_run:
        print("Planned files:")
        for path in exported_files[:20]:
            try:
                relative_path = path.relative_to(output_dir)
            except ValueError:
                relative_path = path
            print(f"- {relative_path}")
        remaining = max(0, len(exported_files) - 20)
        if remaining:
            print(f"... and {remaining} more files not shown")
        return 0

    if validate_output:
        issues = validate_artifact_directory(output_dir)
        print(format_artifact_validation_issues(issues))
        if issues:
            return 1

    if archive_format is not None:
        archive_target = (
            Path(archive_output)
            if archive_output is not None
            else output_dir.parent / f"{output_dir.name}.{archive_format}"
        )
        try:
            archive_path = archive_artifact_tree(
                output_dir,
                archive_target,
                archive_format=archive_format,
                overwrite=overwrite,
            )
        except (FileExistsError, IsADirectoryError, NotADirectoryError, ValueError) as exc:
            print(f"Archive failed: {exc}")
            return 1
        print(f"Archive: {archive_path}")

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="fair-py")
    subparsers = parser.add_subparsers(dest="command", required=True)

    bundle_summary_parser = subparsers.add_parser(
        "bundle-summary",
        help="print execution-input bundle summary",
    )
    bundle_summary_parser.add_argument(
        "--config",
        help="path to TOML/JSON model configuration",
    )
    bundle_summary_parser.add_argument(
        "--merge-external",
        action="store_true",
        help="merge external sources into the legacy execution input table",
    )
    bundle_summary_parser.add_argument(
        "--conflict",
        choices=("error", "prefix", "overwrite"),
        default="error",
        help="merge conflict strategy: error/prefix/overwrite",
    )
    mini_run_parser = subparsers.add_parser(
        "mini-run",
        help="run execution input bundle through the mini-run engine",
    )
    mini_run_parser.add_argument(
        "--config",
        help="path to TOML/JSON model configuration",
    )
    mini_run_parser.add_argument(
        "--merge-external",
        action="store_true",
        help="merge external sources into the legacy execution input table",
    )
    mini_run_parser.add_argument(
        "--conflict",
        choices=("error", "prefix", "overwrite"),
        default="error",
        help="merge conflict strategy: error/prefix/overwrite",
    )
    mini_run_parser.add_argument(
        "--max-steps",
        type=int,
        default=None,
        help="maximum number of planned steps to execute",
    )
    mini_run_parser.add_argument(
        "--on-error",
        choices=("continue", "stop"),
        default="continue",
        help="execution behavior on failure",
    )
    mini_run_parser.add_argument(
        "--enable-eq",
        action="store_true",
        help="apply EQ backfill from coefficient fmout after mini-run execution",
    )
    mini_run_parser.add_argument(
        "--eq-coefs-fmout",
        type=Path,
        help="fmout path with coefficient table for EQ backfill",
    )
    mini_run_parser.add_argument(
        "--eq-use-setupsolve",
        action="store_true",
        help="use SETUPSOLVE MINITERS/MAXCHECK to control EQ backfill iteration loop",
    )
    mini_run_parser.add_argument(
        "--eq-flags-preset-label",
        type=str,
        default=None,
        help="optional preset label propagated into fppy_report summary (for example: parity)",
    )
    mini_run_parser.add_argument(
        "--eq-iters",
        type=int,
        default=None,
        help="override EQ iteration count (defaults to SETUPSOLVE MINITERS/MAXCHECK)",
    )
    mini_run_parser.add_argument(
        "--eq-atol",
        type=float,
        default=None,
        help=(
            "override convergence tolerance for iterative EQ backfill; "
            "when omitted with --eq-use-setupsolve, uses SETUPSOLVE TOLALL "
            "(or legacy default 1e-3)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-dampall",
        type=float,
        default=None,
        help="override SETUPSOLVE DAMPALL for iterative EQ backfill (nonnegative)",
    )
    mini_run_parser.add_argument(
        "--eq-period-sequential",
        action="store_true",
        help=(
            "apply EQ updates period-by-period (closer to legacy solve sequencing) "
            "instead of vectorized full-frame updates"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-period-scoped",
        default="auto",
        choices=["auto", "on", "off"],
        help=(
            "per-period Gauss-Seidel iteration matching FP SOLA: converge period T "
            "before advancing to T+1. 'auto' enables when --eq-period-sequential "
            "and a SOLVE window is active (default: auto)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-period-scoped-range",
        nargs=2,
        metavar=("START", "END"),
        help=(
            "optional period range limiter used with period-scoped iteration "
            "(for example: --eq-period-scoped-range 2029.4 2029.4)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-quantize-store",
        choices=("off", "float32"),
        default="off",
        help=(
            "optional end-of-iteration quantization for stored EQ state "
            "(off/default or float32) scoped to EQ targets within active EQ windows"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-period-sequential-fp-pass-order",
        action="store_true",
        default=True,
        help=(
            "with --eq-period-sequential, evaluate steps in FP solver pass order "
            "(EQ -> LHS -> IDENT -> GENR) per iteration (default: enabled)"
        ),
    )
    mini_run_parser.add_argument(
        "--no-eq-period-sequential-fp-pass-order",
        dest="eq_period_sequential_fp_pass_order",
        action="store_false",
        help="disable FP pass order and use deck order instead",
    )
    mini_run_parser.add_argument(
        "--eq-period-sequential-all-assignments",
        action="store_true",
        help=(
            "with --eq-period-sequential, also replay GENR/IDENT/CREATE "
            "period-by-period (debug/diagnostic mode)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-period-sequential-defer-create",
        action="store_true",
        help=(
            "with --eq-period-sequential, replay CREATE assignments "
            "period-by-period (opt-in lifecycle probe)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-period-sequential-context-assignments-first-iter-only",
        action="store_true",
        help=(
            "with --eq-period-sequential, replay GENR/IDENT/CREATE context "
            "assignments only on iteration 1"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-period-sequential-context-keep-create-all-iters",
        action="store_true",
        help=(
            "with --eq-period-sequential-context-assignments-first-iter-only, "
            "keep CREATE context assignments on iterations >1"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-period-sequential-context-replay-command-time-smpl",
        action="store_true",
        help=(
            "with --eq-period-sequential-context-assignments-first-iter-only, "
            "replay deferred context assignments under their command-time SMPL windows"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-period-sequential-context-replay-command-time-smpl-clip-to-solve-window",
        action="store_true",
        help=(
            "with command-time SMPL replay enabled, clip period-sequential iteration "
            "to active solve-window positions only"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-period-sequential-context-retain-overlap-only",
        action="store_true",
        help=(
            "with --eq-period-sequential-context-assignments-first-iter-only, "
            "retain/replay only context records whose command-time SMPL window overlaps "
            "the solve window"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-period-sequential-context-replay-trace",
        action="store_true",
        help=(
            "emit compact context-replay mutation trace JSON (line/command/window/"
            "target/mutated periods)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-period-sequential-context-replay-trace-max-events",
        type=int,
        default=200,
        help=("maximum context-replay trace events retained across the mini-run (default: 200)"),
    )
    mini_run_parser.add_argument(
        "--eq-period-sequential-context-replay-trace-targets",
        help=(
            "optional comma-separated context replay trace target allowlist "
            "(for example: D1,D2,BETA1)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-period-sequential-context-replay-trace-periods",
        help=(
            "optional comma-separated context replay trace period allowlist (for example: 2025.4)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-aware",
        action="store_true",
        help=(
            "enable experimental AR/RHO-aware EQ replay "
            "(applies lagged-LHS and lagged-structural recurrence terms)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1",
        action="store_true",
        help=(
            "enable experimental residual-carry AR(1) EQ replay "
            "(uses first RHO(-1) term with rolling residual state)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-carry-iterations",
        action="store_true",
        help=(
            "with --eq-rho-resid-ar1 and --eq-period-sequential, "
            "seed each iteration from prior iteration residual state"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-residual-probe-seed-mode",
        choices=("carry", "clear"),
        default="carry",
        help=(
            "residual-probe seed behavior in carry-iterations mode: "
            "carry (reuse current iteration seed state) or clear (no seed)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-start-iter",
        type=int,
        default=1,
        help=(
            "iteration index where rho_resid_ar1 first activates (>=1; "
            "earlier iterations run without residual-carry)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-disable-iter",
        type=int,
        default=0,
        help=("optional single iteration to disable rho_resid_ar1 (0 disables this probe)"),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-seed-lag",
        type=int,
        default=1,
        help=(
            "period lag used when seeding residual state across iterations "
            "(default: 1; nonnegative)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-carry-lag",
        type=int,
        default=0,
        help=(
            "maximum carry distance (in periods) for residual AR(1) replay "
            "(default: 0 for unlimited; nonnegative)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-carry-damp",
        type=float,
        default=1.0,
        help=("damping factor applied to residual AR(1) carry term (0..1, default: 1.0)"),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-carry-damp-mode",
        choices=("term", "state", "sol4", "la257"),
        default="term",
        help=(
            "how carry damping is applied: term (scale carry term) "
            "or state (blend residual state), or sol4 "
            "(SOL4-like damped state update from zero baseline), or la257 "
            "(Fortran-inspired one-step carry + structural residual update)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-carry-multipass",
        action="store_true",
        help=(
            "restrict residual carry to adjacent active periods across passes "
            "for state/sol4/la257 damp modes"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-seed-mode",
        choices=("legacy", "positioned"),
        default="legacy",
        help=(
            "inter-iteration residual seed interpretation: legacy line->residual "
            "or positioned line->(residual, residual_position)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-boundary-reset",
        choices=("off", "window"),
        default="off",
        help=("optional residual-state reset at period-sequential window boundaries (off/window)"),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-lag-gating",
        choices=("off", "lhs", "all"),
        default="off",
        help=(
            "optional lag-term gating in residual-carry mode: off, lhs "
            "(drop lagged lhs terms), or all (drop all lagged non-RHO terms)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-update-source",
        choices=("structural", "result", "solved", "resid_pass"),
        default="structural",
        help=(
            "residual update source for residual-carry mode: structural "
            "(lhs-structural), result (lhs-final_result), or solved "
            "(final_result-structural); resid_pass performs a second structural "
            "evaluation after period assignments in period-sequential mode"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-la257-update-rule",
        choices=("legacy", "fortran_u_phase", "fortran_cycle"),
        default="legacy",
        help=(
            "la257 residual-state update rule: legacy (default) or "
            "fortran_u_phase or fortran_cycle "
            "(period-sequential residual-phase experiments)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-la257-fortran-cycle-carry-style",
        choices=("legacy", "ema"),
        default="legacy",
        help=(
            "carry persistence style for fortran_cycle: legacy (new_state = carry_damp * u_phase) "
            "or ema (new_state = previous + carry_damp * (u_phase - previous))"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-la257-u-phase-max-gain",
        type=float,
        default=None,
        help=(
            "optional nonnegative gain cap for fortran_u_phase/fortran_cycle "
            "residual-state updates (relative to |carry_damp * residual|)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-la257-u-phase-max-gain-mode",
        choices=("relative", "anchored"),
        default="relative",
        help=(
            "gain-cap reference mode for fortran_u_phase/fortran_cycle updates: "
            "relative (per-step |carry_damp * residual|) or anchored "
            "(first-step |carry_damp * residual| for each residual state)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-la257-start-iter",
        type=int,
        default=1,
        help=(
            "iteration index where non-legacy la257 update rules first activate "
            "(>=1; earlier iterations force legacy behavior)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-la257-disable-iter",
        type=int,
        default=0,
        help=("optional single iteration to force legacy la257 behavior (0 disables this probe)"),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-la257-lifecycle-staging",
        action="store_true",
        help=(
            "opt in to staged la257 residual lifecycle "
            "(solve pass first, then deferred resid/state update in resid_pass mode)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-commit-after-check",
        dest="eq_rho_resid_ar1_commit_after_check",
        action="store_true",
        default=None,
        help=(
            "defer AR(1)/la257 state commits until after iteration convergence "
            "check; this is the default for residual-carry period-sequential runs"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-no-commit-after-check",
        dest="eq_rho_resid_ar1_commit_after_check",
        action="store_false",
        help=(
            "opt out of deferred commit semantics and use immediate AR(1)/la257 "
            "state commits in residual-carry period-sequential runs"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-la257-u-phase-lines",
        default=None,
        help=(
            "optional comma-separated selector allowlist for fortran_u_phase/"
            "fortran_cycle: "
            "line or line:occurrence (0-based occurrence within a line) when "
            "la257 update-rule is enabled; non-listed selectors use legacy "
            "la257 updates"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-trace-lines",
        default=None,
        help=(
            "optional comma-separated trace selectors (line or "
            "line:occurrence with 0-based occurrence) for residual-carry "
            "iteration-state diagnostics (requires carry-iterations mode)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-trace-event-limit",
        type=int,
        default=200,
        help=(
            "maximum number of residual-carry trace events stored per "
            "apply_eq_backfill call (0 disables event capture)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-trace-focus-targets",
        default=None,
        help=(
            "optional comma-separated target allowlist for compact lifecycle "
            "focus tracing (for example: Y)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-trace-focus-periods",
        default=None,
        help=(
            "optional comma-separated period allowlist for compact lifecycle "
            "focus tracing (for example: 1953.4)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-trace-focus-iters",
        default=None,
        help=(
            "optional iteration range for focus tracing in '<min>..<max>' format (default: 1..5)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-trace-focus-max-events",
        type=int,
        default=200,
        help=(
            "maximum number of serialized focus-trace events across the full "
            "mini-run iteration loop (default: 200)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-rho-resid-ar1-fpe-trap",
        action="store_true",
        help=(
            "enable fail-fast floating-point exceptions during EQ/assignment "
            "evaluation for residual-carry diagnostics"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-require-convergence",
        action="store_true",
        help="return non-zero when EQ backfill does not converge",
    )
    mini_run_parser.add_argument(
        "--eq-fail-on-diverged",
        action="store_true",
        help="return non-zero when iterative EQ backfill divergence is detected",
    )
    mini_run_parser.add_argument(
        "--eq-fail-on-residual",
        action="store_true",
        help="return non-zero when fixed-point residual ratio exceeds threshold",
    )
    mini_run_parser.add_argument(
        "--eq-fail-on-residual-all-targets",
        action="store_true",
        help=(
            "return non-zero when fixed-point residual ratio exceeds threshold "
            "across all EQ targets (outside MAXCHECK scope)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-residual-ratio-max",
        type=float,
        default=1.0,
        help="maximum allowed fixed-point residual ratio before residual gate failure",
    )
    mini_run_parser.add_argument(
        "--eq-top-target-deltas",
        type=int,
        default=0,
        help=(
            "include top-N per-target convergence deltas in each "
            "eq_backfill_iteration_history entry (0 disables)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-iter-trace",
        action="store_true",
        help=(
            "emit eq_iter_trace.json with per-iteration top target deltas at a single period "
            "(defaults to 2025.4)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-iter-trace-period",
        type=str,
        default=None,
        help="period key to filter eq_iter_trace events (default: 2025.4)",
    )
    mini_run_parser.add_argument(
        "--eq-iter-trace-max-events",
        type=int,
        default=200,
        help="maximum number of eq_iter_trace events to serialize (default: 200)",
    )
    mini_run_parser.add_argument(
        "--eq-iter-trace-targets",
        type=str,
        default=None,
        help="optional comma-separated target allowlist for eq_iter_trace",
    )
    mini_run_parser.add_argument(
        "--eq-seed",
        choices=("off", "filtered", "all"),
        default="off",
        help="optionally seed EQ variables from fmout forecast levels (off/filtered/all)",
    )
    mini_run_parser.add_argument(
        "--eq-seed-fmout",
        type=Path,
        default=None,
        help="fmout path for EQ seeding (defaults to config legacy fmout)",
    )
    mini_run_parser.add_argument(
        "--eq-seed-metric",
        choices=("lv", "ch", "pct"),
        default="lv",
        help="forecast metric frame to use for EQ seeding",
    )
    mini_run_parser.add_argument(
        "--eq-seed-fill",
        choices=("missing", "overwrite"),
        default="missing",
        help="EQ seeding write mode: fill only missing or overwrite existing values",
    )
    mini_run_parser.add_argument(
        "--eq-state-seed-pabev",
        type=Path,
        default=None,
        help=(
            "optional path to a reference PABEV.TXT used to overwrite one period's "
            "state before EQ backfill iteration (diagnostic probe)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-state-seed-period",
        type=str,
        default=None,
        help="period to seed from eq-state-seed-pabev (for example: 2025.3)",
    )
    mini_run_parser.add_argument(
        "--eq-state-seed-fill",
        choices=("missing", "overwrite"),
        default="overwrite",
        help="state seeding write mode: fill only missing or overwrite existing values",
    )
    mini_run_parser.add_argument(
        "--eq-eval-precision",
        choices=("float64", "longdouble"),
        default="float64",
        help=(
            "precision for period-sequential EQ scalar evaluation intermediates: "
            "float64 (default) or longdouble (diagnostic parity probe)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-term-order",
        choices=("as_parsed", "by_index"),
        default="as_parsed",
        help=(
            "term accumulation order for EQ scalar evaluation in period-sequential mode: "
            "as_parsed (default) or by_index (diagnostic parity probe)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-eq-read-mode",
        choices=("live", "frozen"),
        default="live",
        help=(
            "in period-sequential mode, evaluate EQ terms against the live in-iteration "
            "state (live/default, Gauss-Seidel style) or the iteration-start snapshot "
            "(frozen, Jacobi-style parity probe)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-math-backend",
        choices=("numpy", "math"),
        default="numpy",
        help=(
            "scalar math backend for period-sequential GENR/IDENT/LHS expression evaluation: "
            "numpy (default) or math (diagnostic parity probe)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-quantize-eq-commit",
        choices=("off", "float32"),
        default="off",
        help=(
            "optional quantization applied to EQ results at end-of-equation commit "
            "in period-sequential mode (diagnostic parity probe)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-filter-use-full-context",
        action="store_true",
        help=(
            "for EQ-only filtered runs, include full-file MODEQ context "
            "in addition to in-window SMPL/SETUPSOLVE/GENR/IDENT context"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-modeq-fsr-side-channel",
        choices=("off", "capture", "enforce"),
        default="off",
        help=(
            "MODEQ/FSR side-channel mode: off (default), capture "
            "(diagnostic events only), or enforce (non-zero on invariant breach)"
        ),
    )
    mini_run_parser.add_argument(
        "--eq-modeq-fsr-side-channel-max-events",
        type=int,
        default=200,
        help="maximum MODEQ/FSR side-channel events retained in mini-run reports",
    )
    mini_run_parser.add_argument(
        "--parity-baseline-fmout",
        type=Path,
        default=None,
        help="optional fmout baseline path to compare mini-run output levels against",
    )
    mini_run_parser.add_argument(
        "--parity-atol",
        type=float,
        default=1e-6,
        help="absolute tolerance for mini-run output parity mismatch counting",
    )
    mini_run_parser.add_argument(
        "--parity-top",
        type=int,
        default=10,
        help="number of largest absolute parity deltas to include in report payload",
    )
    mini_run_parser.add_argument(
        "--line-start",
        type=int,
        default=None,
        help="optional inclusive minimum fminput line number to execute",
    )
    mini_run_parser.add_argument(
        "--line-end",
        type=int,
        default=None,
        help="optional inclusive maximum fminput line number to execute",
    )
    mini_run_parser.add_argument(
        "--include-commands",
        type=str,
        default=None,
        help="optional comma-separated FP command names to include (for example: GENR,IDENT,LHS,CREATE)",
    )
    mini_run_parser.add_argument(
        "--output-csv",
        type=Path,
        help="write executed output table to CSV",
    )
    mini_run_parser.add_argument(
        "--report-json",
        type=Path,
        help="write mini-run summary and issues as JSON",
    )
    mini_run_parser.add_argument(
        "--solver-trace",
        type=Path,
        default=None,
        help=(
            "optional path for deterministic solver lifecycle JSONL trace "
            "(resid_eval, convergence_check, commit_rho_resid_ar1, modeq_call, period_end)"
        ),
    )

    summary_parser = subparsers.add_parser("summary", help="print baseline template summary")
    summary_parser.add_argument(
        "--config",
        help="path to TOML/JSON model configuration",
    )

    check_files_parser = subparsers.add_parser(
        "check-files", help="verify required baseline files exist"
    )
    check_files_parser.add_argument(
        "--config",
        help="path to TOML/JSON model configuration",
    )

    parse_summary_parser = subparsers.add_parser(
        "parse-summary", help="parse fminput and print command frequencies"
    )
    parse_summary_parser.add_argument(
        "--include-comments",
        action="store_true",
        help="include comment command counts",
    )
    parse_summary_parser.add_argument(
        "--config",
        help="path to TOML/JSON model configuration",
    )

    plan_summary_parser = subparsers.add_parser(
        "plan-summary", help="build execution plan summary from fminput"
    )
    plan_summary_parser.add_argument(
        "--config",
        help="path to TOML/JSON model configuration",
    )

    dependency_summary_parser = subparsers.add_parser(
        "dependency-summary",
        help="build dependency summary from fminput",
    )
    dependency_summary_parser.add_argument(
        "--config",
        help="path to TOML/JSON model configuration",
    )

    parity_summary_parser = subparsers.add_parser(
        "parity-summary", help="print fmout parity summary"
    )
    parity_summary_parser.add_argument(
        "--config",
        help="path to TOML/JSON model configuration",
    )
    parity_report_parser = subparsers.add_parser(
        "parity-report",
        help="compare candidate and baseline structured fmout payloads",
    )
    parity_report_parser.add_argument(
        "--config",
        help="path to TOML/JSON model configuration",
    )
    parity_report_parser.add_argument(
        "--baseline",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "FM" / "fmout.txt",
        help="path to baseline structured fmout output",
    )
    parity_report_parser.add_argument(
        "--metric",
        choices=("lv", "ch", "pct"),
        default="lv",
        help="structured section to compare: lv, ch, or pct",
    )
    parity_report_parser.add_argument(
        "--atol",
        type=float,
        default=1e-8,
        help="absolute tolerance for numeric mismatch detection",
    )
    parity_report_parser.add_argument(
        "--top",
        type=int,
        default=20,
        help="maximum number of top mismatches to print",
    )
    parity_report_parser.add_argument(
        "--include-eq",
        action="store_true",
        help="also compare equation coefficient tables parsed from candidate/baseline fmout files",
    )
    parity_report_parser.add_argument(
        "--eq-coef-atol",
        type=float,
        default=1e-10,
        help="absolute tolerance for equation coefficient mismatch detection",
    )
    parity_report_parser.add_argument(
        "--eq-top",
        type=int,
        default=20,
        help="maximum number of top equation mismatches to print when --include-eq is set",
    )
    equation_search_parser = subparsers.add_parser(
        "equation-search",
        help="search and explain equations/variables from dictionary JSON payloads",
    )
    equation_search_parser.add_argument(
        "--equations-json",
        required=True,
        type=Path,
        help="path to equations dictionary JSON file",
    )
    equation_search_parser.add_argument(
        "--variables-json",
        required=True,
        type=Path,
        help="path to variables dictionary JSON file",
    )
    equation_search_parser.add_argument(
        "--query",
        required=True,
        help="user query text (for example: 'Eq 82' or 'GDP equation')",
    )
    equation_search_parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="maximum number of ranked results to return",
    )
    equation_search_parser.add_argument(
        "--pretty",
        action="store_true",
        help="pretty-print response JSON with indentation",
    )
    equation_search_parser.add_argument(
        "--no-variable-details",
        action="store_true",
        help="omit full variable payloads in equation result rows",
    )

    input_summary_parser = subparsers.add_parser(
        "input-summary",
        help="print resolved legacy and source input paths",
    )
    input_summary_parser.add_argument(
        "--config",
        help="path to TOML/JSON model configuration",
    )
    input_summary_parser.add_argument(
        "--load-sources",
        action="store_true",
        help="load external source tables and print each shape",
    )
    legacy_data_summary_parser = subparsers.add_parser(
        "legacy-data-summary",
        help="parse legacy fmdata/fmage/fmexog and print shape summary",
    )
    legacy_data_summary_parser.add_argument(
        "--config",
        help="path to TOML/JSON model configuration",
    )

    subparsers.add_parser("release-check", help="check workspace for restricted upstream paths")
    export_artifacts_parser = subparsers.add_parser(
        "export-artifacts",
        help="export share-safe release artifacts",
    )
    export_artifacts_parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="destination directory for exported artifacts",
    )
    export_artifacts_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="only show planned files and counts without writing",
    )
    export_artifacts_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="overwrite existing destination directory",
    )
    export_artifacts_parser.add_argument(
        "--archive-format",
        choices=ARCHIVE_FORMATS,
        default=None,
        help="optionally archive exported artifacts as zip or tar.gz",
    )
    export_artifacts_parser.add_argument(
        "--archive-output",
        type=Path,
        default=None,
        help="optional archive output path (requires --archive-format)",
    )
    export_artifacts_parser.add_argument(
        "--skip-validate",
        action="store_true",
        help="skip post-export artifact validation checks",
    )

    args = parser.parse_args()
    if args.command == "summary":
        return _cmd_summary(args.config)
    if args.command == "bundle-summary":
        return _cmd_bundle_summary(
            config_path=args.config,
            merge_external=args.merge_external,
            conflict=args.conflict,
        )
    if args.command == "mini-run":
        return _cmd_mini_run(
            config_path=args.config,
            merge_external=args.merge_external,
            conflict=args.conflict,
            max_steps=args.max_steps,
            on_error=args.on_error,
            enable_eq=args.enable_eq,
            eq_coefs_fmout=args.eq_coefs_fmout,
            eq_flags_preset_label=args.eq_flags_preset_label,
            eq_use_setupsolve=args.eq_use_setupsolve,
            eq_iters=args.eq_iters,
            eq_atol=args.eq_atol,
            eq_dampall=args.eq_dampall,
            eq_period_sequential=args.eq_period_sequential,
            eq_period_scoped=args.eq_period_scoped,
            eq_period_scoped_range=args.eq_period_scoped_range,
            eq_period_sequential_fp_pass_order=args.eq_period_sequential_fp_pass_order,
            eq_period_sequential_all_assignments=(args.eq_period_sequential_all_assignments),
            eq_period_sequential_defer_create=(args.eq_period_sequential_defer_create),
            eq_period_sequential_context_assignments_first_iter_only=(
                args.eq_period_sequential_context_assignments_first_iter_only
            ),
            eq_period_sequential_context_keep_create_all_iters=(
                args.eq_period_sequential_context_keep_create_all_iters
            ),
            eq_period_sequential_context_replay_command_time_smpl=(
                args.eq_period_sequential_context_replay_command_time_smpl
            ),
            eq_period_sequential_context_replay_command_time_smpl_clip_to_solve_window=(
                args.eq_period_sequential_context_replay_command_time_smpl_clip_to_solve_window
            ),
            eq_period_sequential_context_retain_overlap_only=(
                args.eq_period_sequential_context_retain_overlap_only
            ),
            eq_period_sequential_context_replay_trace=(
                args.eq_period_sequential_context_replay_trace
            ),
            eq_period_sequential_context_replay_trace_max_events=(
                args.eq_period_sequential_context_replay_trace_max_events
            ),
            eq_period_sequential_context_replay_trace_targets=(
                args.eq_period_sequential_context_replay_trace_targets
            ),
            eq_period_sequential_context_replay_trace_periods=(
                args.eq_period_sequential_context_replay_trace_periods
            ),
            eq_quantize_store=args.eq_quantize_store,
            eq_rho_aware=args.eq_rho_aware,
            eq_rho_resid_ar1=args.eq_rho_resid_ar1,
            eq_rho_resid_ar1_carry_iterations=args.eq_rho_resid_ar1_carry_iterations,
            eq_rho_resid_ar1_residual_probe_seed_mode=(
                args.eq_rho_resid_ar1_residual_probe_seed_mode
            ),
            eq_rho_resid_ar1_start_iter=args.eq_rho_resid_ar1_start_iter,
            eq_rho_resid_ar1_disable_iter=args.eq_rho_resid_ar1_disable_iter,
            eq_rho_resid_ar1_seed_lag=args.eq_rho_resid_ar1_seed_lag,
            eq_rho_resid_ar1_carry_lag=args.eq_rho_resid_ar1_carry_lag,
            eq_rho_resid_ar1_carry_damp=args.eq_rho_resid_ar1_carry_damp,
            eq_rho_resid_ar1_carry_damp_mode=args.eq_rho_resid_ar1_carry_damp_mode,
            eq_rho_resid_ar1_carry_multipass=args.eq_rho_resid_ar1_carry_multipass,
            eq_rho_resid_ar1_seed_mode=args.eq_rho_resid_ar1_seed_mode,
            eq_rho_resid_ar1_boundary_reset=args.eq_rho_resid_ar1_boundary_reset,
            eq_rho_resid_ar1_lag_gating=args.eq_rho_resid_ar1_lag_gating,
            eq_rho_resid_ar1_update_source=args.eq_rho_resid_ar1_update_source,
            eq_rho_resid_ar1_la257_update_rule=args.eq_rho_resid_ar1_la257_update_rule,
            eq_rho_resid_ar1_la257_fortran_cycle_carry_style=(
                args.eq_rho_resid_ar1_la257_fortran_cycle_carry_style
            ),
            eq_rho_resid_ar1_la257_u_phase_max_gain=(args.eq_rho_resid_ar1_la257_u_phase_max_gain),
            eq_rho_resid_ar1_la257_u_phase_max_gain_mode=(
                args.eq_rho_resid_ar1_la257_u_phase_max_gain_mode
            ),
            eq_rho_resid_ar1_la257_start_iter=(args.eq_rho_resid_ar1_la257_start_iter),
            eq_rho_resid_ar1_la257_disable_iter=(args.eq_rho_resid_ar1_la257_disable_iter),
            eq_rho_resid_ar1_la257_lifecycle_staging=(
                args.eq_rho_resid_ar1_la257_lifecycle_staging
            ),
            eq_rho_resid_ar1_commit_after_check=(args.eq_rho_resid_ar1_commit_after_check),
            eq_rho_resid_ar1_la257_u_phase_lines=(args.eq_rho_resid_ar1_la257_u_phase_lines),
            eq_rho_resid_ar1_trace_lines=args.eq_rho_resid_ar1_trace_lines,
            eq_rho_resid_ar1_trace_event_limit=(args.eq_rho_resid_ar1_trace_event_limit),
            eq_rho_resid_ar1_trace_focus_targets=(args.eq_rho_resid_ar1_trace_focus_targets),
            eq_rho_resid_ar1_trace_focus_periods=(args.eq_rho_resid_ar1_trace_focus_periods),
            eq_rho_resid_ar1_trace_focus_iters=(args.eq_rho_resid_ar1_trace_focus_iters),
            eq_rho_resid_ar1_trace_focus_max_events=(args.eq_rho_resid_ar1_trace_focus_max_events),
            eq_rho_resid_ar1_fpe_trap=args.eq_rho_resid_ar1_fpe_trap,
            eq_require_convergence=args.eq_require_convergence,
            eq_fail_on_diverged=args.eq_fail_on_diverged,
            eq_fail_on_residual=args.eq_fail_on_residual,
            eq_fail_on_residual_all_targets=args.eq_fail_on_residual_all_targets,
            eq_residual_ratio_max=args.eq_residual_ratio_max,
            eq_top_target_deltas=args.eq_top_target_deltas,
            eq_iter_trace=args.eq_iter_trace,
            eq_iter_trace_period=args.eq_iter_trace_period,
            eq_iter_trace_max_events=args.eq_iter_trace_max_events,
            eq_iter_trace_targets=args.eq_iter_trace_targets,
            eq_seed=args.eq_seed,
            eq_seed_fmout=args.eq_seed_fmout,
            eq_seed_metric=args.eq_seed_metric,
            eq_seed_fill=args.eq_seed_fill,
            eq_state_seed_pabev=args.eq_state_seed_pabev,
            eq_state_seed_period=args.eq_state_seed_period,
            eq_state_seed_fill=args.eq_state_seed_fill,
            eq_eval_precision=args.eq_eval_precision,
            eq_term_order=args.eq_term_order,
            eq_eq_read_mode=args.eq_eq_read_mode,
            eq_math_backend=args.eq_math_backend,
            eq_quantize_eq_commit=args.eq_quantize_eq_commit,
            eq_filter_use_full_context=args.eq_filter_use_full_context,
            eq_modeq_fsr_side_channel=args.eq_modeq_fsr_side_channel,
            eq_modeq_fsr_side_channel_max_events=(args.eq_modeq_fsr_side_channel_max_events),
            parity_baseline_fmout=args.parity_baseline_fmout,
            parity_atol=args.parity_atol,
            parity_top=args.parity_top,
            line_start=args.line_start,
            line_end=args.line_end,
            include_commands=args.include_commands,
            output_csv=args.output_csv,
            report_json=args.report_json,
            solver_trace=args.solver_trace,
        )
    if args.command == "check-files":
        return _cmd_check_files(args.config)
    if args.command == "parse-summary":
        return _cmd_parse_summary(
            include_comments=args.include_comments,
            config_path=args.config,
        )
    if args.command == "plan-summary":
        return _cmd_plan_summary(args.config)
    if args.command == "dependency-summary":
        return _cmd_dependency_summary(args.config)
    if args.command == "parity-summary":
        return _cmd_parity_summary(args.config)
    if args.command == "parity-report":
        return _cmd_parity_report(
            args.config,
            baseline=args.baseline,
            metric=args.metric,
            atol=args.atol,
            top=args.top,
            include_eq=args.include_eq,
            eq_coef_atol=args.eq_coef_atol,
            eq_top=args.eq_top,
        )
    if args.command == "equation-search":
        return _cmd_equation_search(
            equations_json=args.equations_json,
            variables_json=args.variables_json,
            query=args.query,
            limit=args.limit,
            pretty=args.pretty,
            include_variable_details=not args.no_variable_details,
        )
    if args.command == "input-summary":
        return _cmd_input_summary(args.config, load_sources=args.load_sources)
    if args.command == "legacy-data-summary":
        return _cmd_legacy_data_summary(args.config)
    if args.command == "release-check":
        return _cmd_release_check()
    if args.command == "export-artifacts":
        return _cmd_export_artifacts(
            output=args.output,
            dry_run=args.dry_run,
            overwrite=args.overwrite,
            archive_format=args.archive_format,
            archive_output=args.archive_output,
            validate_output=not args.skip_validate,
        )

    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
