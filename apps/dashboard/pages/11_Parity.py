"""Parity dashboard page: PABEV delta views for parity_report artifacts."""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from fp_wraptr.dashboard import _common as dashboard_common
from fp_wraptr.dashboard._common import artifacts_dir_from_query, page_favicon
from fp_wraptr.dashboard.agent_handoff import render_agent_handoff
from fp_wraptr.dashboard.artifacts import (
    ParityRunArtifact,
    scan_parity_artifacts,
)
from fp_wraptr.dashboard.plotly_theme import apply_white_theme
from fp_wraptr.runtime.solve_errors import scan_solution_errors
from fp_wraptr.viz.period_labels import format_period_label
from fppy.pabev_parity import PabevPeriod, parse_pabev

DEFAULT_START_PERIOD = "2025.4"
apply_data_editor_checkbox_edits = getattr(
    dashboard_common,
    "apply_data_editor_checkbox_edits",
    lambda table, **_kwargs: table,
)


def _scenario_filter_from_query() -> str:
    params = st.query_params.get("scenario_filter", "")
    if isinstance(params, list):
        params = params[0] if params else ""
    return str(params or "").strip()


def _default_parity_root(artifacts_root: Path) -> Path:
    """Prefer a dedicated parity folder when present."""
    dedicated = artifacts_root / "parity"
    return dedicated if dedicated.exists() and dedicated.is_dir() else artifacts_root


def _filter_parity_runs(
    runs: list[ParityRunArtifact],
    *,
    scenario_filter: str,
    max_runs: int,
) -> list[ParityRunArtifact]:
    text = str(scenario_filter or "").strip().lower()
    filtered = runs
    if text:
        filtered = [r for r in filtered if text in str(r.scenario_name).lower()]
    if max_runs > 0:
        filtered = filtered[: int(max_runs)]
    return filtered


def _cache_key(paths: list[Path]) -> str:
    parts: list[str] = []
    for path in paths:
        if path.exists():
            stat = path.stat()
            parts.append(f"{path}:{stat.st_size}:{stat.st_mtime_ns}")
        else:
            parts.append(f"{path}:missing")
    return "|".join(parts)


def _resolve_report_path(
    run: ParityRunArtifact, value: str | None, fallback_rel: str
) -> Path | None:
    candidates: list[Path] = []
    value_text = str(value).strip() if value is not None else ""
    if value_text and value_text.lower() != "none":
        raw = Path(value_text)
        candidates.append(raw)
        candidates.append(Path.cwd() / raw)
        candidates.append(run.run_dir / raw)
    candidates.append(run.run_dir / fallback_rel)

    for candidate in candidates:
        if candidate.exists():
            return candidate

    # backend=both layout fallback: parent run_dir contains parity_report.json while the
    # engine work dirs live under run_dir/parity/<inner_run>/...
    parity_root = run.run_dir / "parity"
    if parity_root.exists() and parity_root.is_dir():
        for child in sorted(parity_root.iterdir()):
            candidate = child / fallback_rel
            if candidate.exists():
                return candidate
    return None


def _resolve_dir_path(run: ParityRunArtifact, value: str | None, fallback_rel: str) -> Path | None:
    candidates: list[Path] = []
    value_text = str(value).strip() if value is not None else ""
    if value_text and value_text.lower() != "none":
        raw = Path(value_text)
        candidates.append(raw)
        candidates.append(Path.cwd() / raw)
        candidates.append(run.run_dir / raw)
    candidates.append(run.run_dir / fallback_rel)

    for candidate in candidates:
        if candidate.exists() and candidate.is_dir():
            return candidate

    parity_root = run.run_dir / "parity"
    if parity_root.exists() and parity_root.is_dir():
        for child in sorted(parity_root.iterdir()):
            candidate = child / fallback_rel
            if candidate.exists() and candidate.is_dir():
                return candidate
    return None


@st.cache_data(ttl=60)
def _load_json(path_str: str, cache_key: str) -> dict[str, Any]:
    _ = cache_key
    path = Path(path_str)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return payload


@st.cache_data(ttl=60)
def _load_pabev_frame(path_str: str, cache_key: str) -> pd.DataFrame:
    _ = cache_key
    periods, series = parse_pabev(Path(path_str))
    labels = [str(p) for p in periods]
    return pd.DataFrame.from_dict(
        {k: list(v) for k, v in series.items()}, orient="index", columns=labels
    )


@st.cache_data(ttl=60)
def _load_csv_frame(path_str: str, cache_key: str) -> pd.DataFrame:
    _ = cache_key
    return pd.read_csv(Path(path_str))


@st.cache_data(ttl=60)
def _load_text(path_str: str, cache_key: str) -> str:
    _ = cache_key
    return Path(path_str).read_text(encoding="utf-8")


@st.cache_data(ttl=60)
def _load_json_any(path_str: str, cache_key: str) -> Any:
    _ = cache_key
    return json.loads(Path(path_str).read_text(encoding="utf-8"))


def _compute_diff_slices(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    *,
    start_period: str,
    end_period: str | None,
    missing_sentinels: frozenset[float],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    shared_vars = sorted(set(left_df.index) & set(right_df.index))
    if not shared_vars:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    shared_periods = [col for col in left_df.columns if col in right_df.columns]
    if not shared_periods:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    selected_periods = _window_columns(shared_periods, start=start_period, end=end_period)
    if not selected_periods:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    left = left_df.loc[shared_vars, selected_periods].astype(float)
    right = right_df.loc[shared_vars, selected_periods].astype(float)
    # Mask missing-sentinel cells before abs-diff so heatmaps do not present
    # missing-value artifacts as real divergences.
    if missing_sentinels:
        left_missing = left.isin(missing_sentinels)
        right_missing = right.isin(missing_sentinels)
        mask = left_missing | right_missing
        left = left.mask(mask)
        right = right.mask(mask)
    abs_diff = (left - right).abs()
    left_abs = left.abs()
    right_abs = right.abs()
    denom = left_abs.mask(left_abs < right_abs, right_abs).replace({0.0: pd.NA})
    pct_diff = abs_diff.div(denom).fillna(0.0)
    return left, right, abs_diff, pct_diff


def _compute_abs_diff(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    *,
    start_period: str,
    end_period: str | None,
    missing_sentinels: frozenset[float],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Backward-compatible helper retained for tests and older imports."""
    left, right, abs_diff, _pct_diff = _compute_diff_slices(
        left_df,
        right_df,
        start_period=start_period,
        end_period=end_period,
        missing_sentinels=missing_sentinels,
    )
    return left, right, abs_diff


@st.cache_data(ttl=60)
def _load_diff_slices(
    left_path_str: str,
    right_path_str: str,
    cache_key: str,
    *,
    start_period: str,
    end_period: str | None,
    missing_sentinels: tuple[float, ...],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    left_df = _load_pabev_frame(left_path_str, cache_key + ":left")
    right_df = _load_pabev_frame(right_path_str, cache_key + ":right")
    return _compute_diff_slices(
        left_df,
        right_df,
        start_period=start_period,
        end_period=end_period,
        missing_sentinels=frozenset(missing_sentinels),
    )


def _normalize_missing_sentinels(value: Any) -> frozenset[float]:
    if not isinstance(value, list):
        return frozenset((-99.0,))
    out: set[float] = set()
    for item in value:
        try:
            out.add(float(item))
        except (TypeError, ValueError):
            continue
    return frozenset(out) if out else frozenset((-99.0,))


def _pick_default_series_variable(all_vars: list[str], hard_fail_vars: list[str]) -> str:
    if not all_vars:
        return ""
    for candidate in hard_fail_vars:
        if candidate in all_vars:
            return candidate
    return all_vars[0]


def _sort_parity_runs_latest_first(
    parity_runs: list[ParityRunArtifact],
) -> list[ParityRunArtifact]:
    return sorted(
        parity_runs,
        key=lambda run: run.timestamp if run.timestamp else "00000000_000000",
        reverse=True,
    )


def _safe_percent_diff(left: pd.Series, right: pd.Series) -> pd.Series:
    left_values = pd.to_numeric(left, errors="coerce")
    right_values = pd.to_numeric(right, errors="coerce")
    denom = pd.DataFrame({
        "left": left_values.abs(),
        "right": right_values.abs(),
    }).max(axis=1)
    denom = denom.replace({0.0: pd.NA})
    diff = (left_values - right_values).abs()
    pct = diff.div(denom)
    return pct.fillna(0.0)


def _top_first_diff_variables(top_first_diffs: Any) -> list[str]:
    if not isinstance(top_first_diffs, list):
        return []
    out: list[str] = []
    for item in top_first_diffs[:5]:
        if not isinstance(item, dict):
            continue
        value = item.get("variable")
        if isinstance(value, str) and value.strip():
            out.append(value.strip())
    return out


def _choose_series_variable_from_hard_fail_table(
    hard_fail_df: pd.DataFrame,
    *,
    series_vars: list[str],
) -> str | None:
    if hard_fail_df.empty:
        return None
    if "variable" not in hard_fail_df.columns:
        return None
    view = hard_fail_df.copy()
    view.insert(0, "jump", False)
    hard_fail_editor_state = st.session_state.get("parity_hard_fail_editor")
    st.data_editor(
        view,
        hide_index=True,
        use_container_width=True,
        key="parity_hard_fail_editor",
        column_config={
            "jump": st.column_config.CheckboxColumn(
                "Jump",
                help="Check one row to jump series plot to its variable.",
                default=False,
            )
        },
    )
    edited = apply_data_editor_checkbox_edits(
        view,
        widget_key="parity_hard_fail_editor",
        checkbox_column="jump",
        raw_state=hard_fail_editor_state,
    )
    if not isinstance(edited, pd.DataFrame) or "jump" not in edited.columns:
        return None
    chosen = edited[edited["jump"]]
    if chosen.empty:
        return None
    candidate = str(chosen.iloc[0].get("variable", "")).strip()
    if candidate in series_vars:
        return candidate
    return None


def _extract_triage_buckets(triage: dict[str, Any]) -> dict[str, int]:
    for key in ("error_buckets", "bucket_counts", "counts", "status_breakdown", "buckets"):
        value = triage.get(key)
        if isinstance(value, dict):
            out = {str(k): int(v) for k, v in value.items() if isinstance(v, int)}
            if out:
                return out

    # Fallback: find a shallow int-count map if present.
    for value in triage.values():
        if isinstance(value, dict):
            out = {str(k): int(v) for k, v in value.items() if isinstance(v, int)}
            if out:
                return out

    return {}


def _lightweight_fppy_buckets(fppy_report: dict[str, Any]) -> dict[str, int]:
    summary = fppy_report.get("summary")
    if not isinstance(summary, dict):
        return {}

    buckets: dict[str, int] = {}
    unsupported_counts = summary.get("unsupported_counts")
    if isinstance(unsupported_counts, dict):
        for key, value in unsupported_counts.items():
            if isinstance(value, int):
                buckets[f"unsupported:{key}"] = int(value)

    for key in ("failed", "unsupported", "eq_backfill_failed", "eq_backfill_attempted_records"):
        value = summary.get(key)
        if isinstance(value, int) and value > 0:
            buckets[key] = int(value)

    return buckets


def _top_buckets_table(buckets: dict[str, int], *, limit: int = 12) -> pd.DataFrame:
    rows = sorted(buckets.items(), key=lambda item: item[1], reverse=True)[:limit]
    return pd.DataFrame(rows, columns=["bucket", "count"])


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _safe_bool(value: Any, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes", "y"}:
            return True
        if text in {"false", "0", "no", "n"}:
            return False
    if isinstance(value, (int, float)):
        return bool(value)
    return default


def _try_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _try_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes", "y"}:
            return True
        if text in {"false", "0", "no", "n"}:
            return False
    if isinstance(value, (int, float)):
        return bool(value)
    return None


def _extract_fpexe_preflight_report(
    detail: dict[str, Any],
    fpexe_meta: dict[str, Any],
) -> dict[str, Any] | None:
    details = fpexe_meta.get("details")
    if isinstance(details, dict):
        preflight = details.get("preflight_report")
        if isinstance(preflight, dict):
            return preflight
    fpexe_detail = detail.get("fpexe_details")
    if isinstance(fpexe_detail, dict):
        preflight = fpexe_detail.get("preflight_report")
        if isinstance(preflight, dict):
            return preflight
    return None


def _extract_solution_error_records(details: dict[str, Any] | Any) -> list[dict[str, str]]:
    if not isinstance(details, dict):
        return []
    raw_errors = details.get("solution_errors")
    if not isinstance(raw_errors, list):
        return []

    records: list[dict[str, str]] = []
    for item in raw_errors:
        if len(records) >= 3:
            break
        if not isinstance(item, dict):
            continue

        solve = ""
        for key in ("solve_name", "solve", "name"):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                solve = value.strip()
                break

        iters = ""
        for key in ("iters", "iterations", "iteration_count", "n_iter", "n_iters"):
            value = item.get(key)
            if isinstance(value, (int, float, str)) and str(value).strip():
                iters = str(value).strip()
                break

        period = ""
        for key in ("period", "solve_period", "forecast_period", "quarter"):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                period = value.strip()
                break

        raw_match = ""
        for key in ("match", "raw", "message", "detail", "error"):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                raw_match = value.strip()
                break

        if not raw_match:
            raw_match = str(item)
        records.append({
            "solve": solve or "n/a",
            "iters": iters or "n/a",
            "period": period or "n/a",
            "raw_match": raw_match,
        })

    return records


@st.cache_data(ttl=60)
def _scan_solution_errors(work_dir_str: str, cache_key: str) -> list[dict[str, Any]]:
    _ = cache_key
    work_dir = Path(work_dir_str)
    matches = scan_solution_errors(work_dir)
    return [m.to_dict() for m in matches]


def _solve_error_periods(matches: list[dict[str, Any]]) -> list[str]:
    out: list[str] = []
    for item in matches:
        if not isinstance(item, dict):
            continue
        period = item.get("period")
        if isinstance(period, str) and period.strip():
            out.append(period.strip())
    # Unique + stable ordering (chronological, best-effort).
    uniq = sorted(set(out))
    sortable: list[tuple[PabevPeriod, str]] = []
    for p in uniq:
        try:
            sortable.append((PabevPeriod.parse(p), p))
        except Exception:
            continue
    sortable.sort(key=lambda t: t[0])
    return [p for _pp, p in sortable]


def _prev_quarter(period: str) -> str | None:
    try:
        p = PabevPeriod.parse(period)
    except Exception:
        return None
    ordinal = int(p.year) * 4 + (int(p.quarter) - 1)
    if ordinal <= 0:
        return None
    prev = ordinal - 1
    year = prev // 4
    q = (prev % 4) + 1
    return f"{year}.{q}"


def _present_int(value: int | None) -> str:
    return "n/a" if value is None else str(value)


def _present_bool(value: bool | None) -> str:
    if value is None:
        return "unknown"
    return "true" if value else "false"


def _extract_eq_flags_preset_from_stdout(stdout_text: str) -> str | None:
    for line in stdout_text.splitlines():
        if "eq_flags_preset" not in line.lower():
            continue
        match = re.search(r"eq_flags_preset\s*[:=]\s*([A-Za-z0-9_.-]+)", line, re.IGNORECASE)
        if not match:
            continue
        value = match.group(1).strip()
        if value:
            return value
    return None


def _resolve_eq_flags_preset(
    run: ParityRunArtifact,
    fppy_meta: dict[str, Any],
) -> tuple[str | None, str]:
    details = fppy_meta.get("details")
    if isinstance(details, dict):
        value = details.get("eq_flags_preset")
        if isinstance(value, str) and value.strip():
            return value.strip(), "engine_runs.fppy.details"

    raw_stdout = fppy_meta.get("stdout_path")
    stdout_source = (
        "engine_runs.fppy.stdout_path"
        if isinstance(raw_stdout, str) and raw_stdout.strip()
        else "work_fppy/fppy.stdout.txt"
    )
    stdout_path = _resolve_report_path(run, raw_stdout, "work_fppy/fppy.stdout.txt")
    if stdout_path is not None:
        try:
            text = _load_text(str(stdout_path), _cache_key([stdout_path]))
        except Exception:
            return None, "not found"
        parsed = _extract_eq_flags_preset_from_stdout(text)
        if parsed:
            return parsed, stdout_source
    return None, "not found"


def _load_unsupported_impact(
    fppy_report: dict[str, Any],
) -> tuple[dict[str, int], pd.DataFrame]:
    summary = fppy_report.get("summary")
    unsupported_counts: dict[str, int] = {}
    if isinstance(summary, dict):
        raw_counts = summary.get("unsupported_counts")
        if isinstance(raw_counts, dict):
            unsupported_counts = {
                str(key): int(value) for key, value in raw_counts.items() if isinstance(value, int)
            }
    rows: list[dict[str, Any]] = []
    raw_examples = fppy_report.get("unsupported_examples")
    if isinstance(raw_examples, list):
        for item in raw_examples[:20]:
            if not isinstance(item, dict):
                continue
            rows.append({
                "command": item.get("command", ""),
                "line": item.get("line"),
                "statement": item.get("statement", ""),
            })
    return unsupported_counts, pd.DataFrame(rows)


def _is_missing_variable_failure(error_text: str | None) -> bool:
    if not isinstance(error_text, str):
        return False
    text = error_text.strip().lower()
    return "missing variable" in text or "undefined variable" in text


def _regression_totals(payload: dict[str, Any]) -> tuple[int, int]:
    counts = payload.get("counts")
    if not isinstance(counts, dict):
        return 0, 0
    new_total = (
        _safe_int(counts.get("new_missing_left"))
        + _safe_int(counts.get("new_missing_right"))
        + _safe_int(counts.get("new_hard_fail_cells"))
        + _safe_int(counts.get("new_diff_variables"))
    )
    resolved_total = (
        _safe_int(counts.get("resolved_missing_left"))
        + _safe_int(counts.get("resolved_missing_right"))
        + _safe_int(counts.get("resolved_hard_fail_cells"))
        + _safe_int(counts.get("resolved_diff_variables"))
    )
    return new_total, resolved_total


def _parse_period(text: str) -> PabevPeriod | None:
    try:
        return PabevPeriod.parse(str(text))
    except Exception:
        return None


def _window_columns(
    columns: list[str],
    *,
    start: str | None,
    end: str | None,
) -> list[str]:
    start_p = _parse_period(start) if start else None
    end_p = _parse_period(end) if end else None
    out: list[str] = []
    for col in columns:
        p = _parse_period(col)
        if p is None:
            continue
        if start_p is not None:
            if p.year < start_p.year or (p.year == start_p.year and p.quarter < start_p.quarter):
                continue
        if end_p is not None:
            if p.year > end_p.year or (p.year == end_p.year and p.quarter > end_p.quarter):
                continue
        out.append(col)
    return out


def _extract_eq_iter_trace_summary(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {
            "period": "n/a",
            "events_count": 0,
            "events_truncated": False,
            "target_allowlist_count": 0,
            "target_allowlist_preview": [],
            "iteration_rows": [],
            "event_rows": [],
        }

    period = str(payload.get("period") or "n/a")
    events = payload.get("events") if isinstance(payload.get("events"), list) else []
    iteration_summary = (
        payload.get("iteration_summary")
        if isinstance(payload.get("iteration_summary"), list)
        else []
    )
    target_allowlist_raw = (
        payload.get("target_allowlist")
        if isinstance(payload.get("target_allowlist"), list)
        else []
    )
    target_allowlist = [
        str(item).strip() for item in target_allowlist_raw if str(item).strip()
    ]
    events_count = _try_int(payload.get("events_count"))
    if events_count is None:
        events_count = len(events)
    events_truncated = bool(payload.get("events_truncated"))

    def _fmt_float(value: Any) -> str:
        try:
            return f"{float(value):.6g}"
        except (TypeError, ValueError):
            return "n/a"

    iteration_rows: list[dict[str, Any]] = []
    for row in iteration_summary[:5]:
        if not isinstance(row, dict):
            continue
        iteration_rows.append({
            "iteration": _present_int(_try_int(row.get("iteration"))),
            "max_abs_delta": _fmt_float(row.get("max_abs_delta", 0.0)),
            "applied": _present_int(_try_int(row.get("applied"))),
            "failed": _present_int(_try_int(row.get("failed"))),
        })

    event_rows: list[dict[str, Any]] = []
    for row in events[:8]:
        if not isinstance(row, dict):
            continue
        event_rows.append({
            "iteration": _present_int(_try_int(row.get("iteration"))),
            "scope": str(row.get("scope") or "n/a"),
            "target": str(row.get("target") or "n/a"),
            "max_abs_delta": _fmt_float(row.get("max_abs_delta", 0.0)),
            "ratio_to_tol": _fmt_float(row.get("ratio_to_tol", 0.0)),
        })

    return {
        "period": period,
        "events_count": events_count,
        "events_truncated": events_truncated,
        "target_allowlist_count": len(target_allowlist),
        "target_allowlist_preview": target_allowlist[:6],
        "iteration_rows": iteration_rows,
        "event_rows": event_rows,
    }


def _detect_zero_filled_forecast_vars(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    *,
    window_start: str | None,
    window_end: str | None,
    eps: float = 1e-12,
) -> pd.DataFrame:
    shared_vars = sorted(set(left_df.index) & set(right_df.index))
    if not shared_vars:
        return pd.DataFrame(columns=["variable", "fppy_max_abs", "fpexe_max_abs"])
    shared_cols = [col for col in left_df.columns if col in right_df.columns]
    cols = _window_columns(shared_cols, start=window_start, end=window_end)
    if not cols:
        return pd.DataFrame(columns=["variable", "fppy_max_abs", "fpexe_max_abs"])
    left = left_df.loc[shared_vars, cols].astype(float)
    right = right_df.loc[shared_vars, cols].astype(float)
    rows: list[dict[str, Any]] = []
    for var in shared_vars:
        left_max = float(left.loc[var].abs().max(skipna=True))
        right_max = float(right.loc[var].abs().max(skipna=True))
        if right_max <= eps and left_max > eps:
            rows.append({
                "variable": var,
                "fppy_max_abs": right_max,
                "fpexe_max_abs": left_max,
            })
    if not rows:
        return pd.DataFrame(columns=["variable", "fppy_max_abs", "fpexe_max_abs"])
    return pd.DataFrame(rows).sort_values("fpexe_max_abs", ascending=False).head(100)


def main() -> None:
    st.set_page_config(page_title="fp-wraptr Parity", page_icon=page_favicon(), layout="wide")
    dashboard_common.render_sidebar_logo_toggle(width=56, height=56)
    dashboard_common.render_page_title(
        "Parity Dashboard",
        caption="Compare fpexe vs fppy outputs with PABEV delta heatmaps and triage tools.",
    )
    cwd = Path.cwd()
    unsafe_paths = [cwd / ".venv", cwd / ".wine"]
    present_unsafe = [str(path) for path in unsafe_paths if path.exists()]
    if present_unsafe:
        st.warning(
            "Unsafe repo-local runtime directories detected:\n"
            + "\n".join(f"- {path}" for path in present_unsafe)
            + "\nUse `UV_PROJECT_ENVIRONMENT=/Users/shanewray/venvs/fp-wraptr` "
            "and a Wine prefix outside the repo (for example "
            "`/Users/shanewray/.wine-fp-wraptr`)."
        )

    artifacts_dir = artifacts_dir_from_query()
    default_parity_root = _default_parity_root(artifacts_dir)
    st.sidebar.markdown("---")
    st.sidebar.subheader("Scan")
    scan_mode = st.sidebar.radio(
        "Parity scan root",
        options=["Default", "All artifacts", "Custom"],
        index=0,
        help=(
            "Default prefers `artifacts/parity/` when it exists; otherwise uses the dashboard artifacts root. "
            "All artifacts scans the full artifacts root (noisy if you have many experiments)."
        ),
    )
    if scan_mode == "All artifacts":
        parity_root = artifacts_dir
    elif scan_mode == "Custom":
        custom = st.sidebar.text_input(
            "Custom parity root",
            value=str(default_parity_root),
            help="Directory to scan for parity_report.json (kept shallow for performance).",
        )
        parity_root = Path(custom).expanduser()
    else:
        parity_root = default_parity_root

    st.sidebar.caption(f"Scanning: `{parity_root}`")

    parity_runs = scan_parity_artifacts(parity_root)
    if not parity_runs:
        st.info(f"No parity runs found under {parity_root}.")
        return

    parity_runs = _sort_parity_runs_latest_first(parity_runs)
    st.sidebar.markdown("---")
    st.sidebar.subheader("Filter")
    query_filter = _scenario_filter_from_query()
    if query_filter:
        st.session_state["parity_scenario_filter"] = query_filter
    default_filter = str(st.session_state.get("parity_scenario_filter", "") or "")
    scenario_filter = st.sidebar.text_input(
        "Scenario contains",
        value=default_filter,
        placeholder="optional substring filter",
    )
    st.session_state["parity_scenario_filter"] = scenario_filter
    max_runs_upper = min(400, len(parity_runs))
    if max_runs_upper <= 10:
        max_runs = max_runs_upper
        st.sidebar.caption(f"Max runs: {max_runs} (only {len(parity_runs)} found)")
    else:
        max_runs = int(
            st.sidebar.slider(
                "Max runs",
                min_value=10,
                max_value=max_runs_upper,
                value=min(80, max_runs_upper),
                step=10,
                help="Limit the run picker to the most recent N matches.",
            )
        )
    filtered = _filter_parity_runs(parity_runs, scenario_filter=scenario_filter, max_runs=max_runs)
    if not filtered:
        st.warning("No parity runs match the current filters.")
        return
    run = st.sidebar.selectbox(
        "Parity run",
        options=filtered,
        format_func=lambda item: item.display_name,
        index=0,
    )
    render_agent_handoff(
        title="Agent Handoff",
        run_dir=run.run_dir,
        prompt=(
            f"Inspect parity run `{run.run_dir.name}`, summarize the main divergence signals, "
            "and prepare follow-up comparison or visualization steps for the dashboard."
        ),
    )

    report_cache = _cache_key([run.parity_report_path])
    try:
        report = _load_json(str(run.parity_report_path), report_cache)
    except Exception as exc:
        st.error(f"Failed to load parity_report.json: {exc}")
        return

    detail = report.get("pabev_detail") if isinstance(report.get("pabev_detail"), dict) else {}
    if not isinstance(detail, dict):
        detail = {}

    status = str(report.get("status", "unknown")).strip().lower()
    missing_left = (
        detail.get("missing_left") if isinstance(detail.get("missing_left"), list) else []
    )
    missing_right = (
        detail.get("missing_right") if isinstance(detail.get("missing_right"), list) else []
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("status", str(report.get("status", "unknown")))
    c2.metric("exit_code", str(report.get("exit_code", "?")))
    c3.metric(
        "hard_fail_cells",
        str(detail.get("hard_fail_cell_count", detail.get("hard_fail_cells_count", 0))),
    )
    c4.metric("max_abs_diff", f"{float(detail.get('max_abs_diff', 0.0)):.6g}")

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("median_abs_diff", f"{float(detail.get('median_abs_diff', 0.0)):.6g}")
    c6.metric("p90_abs_diff", f"{float(detail.get('p90_abs_diff', 0.0)):.6g}")
    c7.metric("missing_left", str(len(missing_left)))
    c8.metric("missing_right", str(len(missing_right)))

    engine_runs = report.get("engine_runs") if isinstance(report.get("engine_runs"), dict) else {}
    fpexe_meta = engine_runs.get("fpexe") if isinstance(engine_runs.get("fpexe"), dict) else {}
    fppy_meta = engine_runs.get("fppy") if isinstance(engine_runs.get("fppy"), dict) else {}
    fpexe_dir = _resolve_dir_path(run, fpexe_meta.get("work_dir"), "work_fpexe")
    fppy_dir = _resolve_dir_path(run, fppy_meta.get("work_dir"), "work_fppy")
    fpexe_stdout_path = _resolve_report_path(run, fpexe_meta.get("stdout_path"), "work_fpexe/fp-exe.stdout.txt")
    fpexe_stderr_path = _resolve_report_path(run, fpexe_meta.get("stderr_path"), "work_fpexe/fp-exe.stderr.txt")
    fppy_stdout_path = _resolve_report_path(run, fppy_meta.get("stdout_path"), "work_fppy/fppy.stdout.txt")
    fppy_stderr_path = _resolve_report_path(run, fppy_meta.get("stderr_path"), "work_fppy/fppy.stderr.txt")
    fpexe_preflight_report = _extract_fpexe_preflight_report(detail, fpexe_meta)
    triage_base_dir = fppy_dir if fppy_dir is not None else (run.run_dir / "work_fppy")
    triage_summary_path = triage_base_dir / "triage_summary.json"
    triage_issues_path = triage_base_dir / "triage_issues.csv"
    triage_hardfails_work_path = triage_base_dir / "triage_hardfails.csv"
    triage_hardfails_root_path = run.run_dir / "triage_hardfails.csv"
    triage_hardfails_path = (
        triage_hardfails_work_path
        if triage_hardfails_work_path.exists()
        else triage_hardfails_root_path
    )
    top_first_diff_vars = _top_first_diff_variables(detail.get("top_first_diffs"))
    hard_fail_cell_count = int(
        _safe_int(detail.get("hard_fail_cell_count"))
        or _safe_int(detail.get("hard_fail_cells_count"))
        or 0
    )

    # Solve errors: prefer engine metadata, but fall back to scanning work_fpexe/fmout.txt.
    raw_solution_errors: list[dict[str, Any]] = []
    details = fpexe_meta.get("details") if isinstance(fpexe_meta.get("details"), dict) else {}
    meta_errors = details.get("solution_errors")
    if isinstance(meta_errors, list):
        raw_solution_errors = [item for item in meta_errors if isinstance(item, dict)]
    if not raw_solution_errors and fpexe_dir is not None:
        raw_solution_errors = _scan_solution_errors(
            str(fpexe_dir),
            _cache_key([fpexe_dir / "fmout.txt"]),
        )

    fpexe_solution_errors = (
        _extract_solution_error_records({"solution_errors": raw_solution_errors})
        if raw_solution_errors
        else []
    )

    solve_error_periods = _solve_error_periods(raw_solution_errors)
    first_solve_error_period = solve_error_periods[0] if solve_error_periods else None
    suggested_end_period = _prev_quarter(first_solve_error_period) if first_solve_error_period else None
    trim_default = bool(first_solve_error_period)
    trim_to_converged = st.sidebar.toggle(
        "Trim before fp.exe solve error",
        value=trim_default,
        help=(
            "If fp.exe reports a solve error, diffs after that period are often unreliable. "
            "This sets the default End period to the quarter before the first solve error."
        ),
    )

    if fpexe_solution_errors:
        st.warning(
            "fp.exe reported solve errors (numeric comparisons may be unreliable). "
            "Actionable hint: fp.exe did not converge; treat numeric diffs in this run as unreliable."
        )
        for idx, item in enumerate(fpexe_solution_errors, start=1):
            st.caption(
                f"{idx}. solve={item['solve']}, iters={item['iters']}, "
                f"period={item['period']}, match={item['raw_match'][:120]}"
            )
        if first_solve_error_period:
            st.caption(
                f"First solve-error period: `{first_solve_error_period}`"
                + (f" (suggested End: `{suggested_end_period}`)" if suggested_end_period else "")
            )

    if status != "ok":
        st.subheader("Next action")
        if status in {"engine_failure", "missing_output"}:
            st.caption("Inspect runtime logs before changing inputs.")
            if fpexe_preflight_report is not None:
                st.warning("fp.exe preflight diagnostics are available for this run.")
                p1, p2, p3 = st.columns(3)
                p1.metric(
                    "wine_required",
                    _present_bool(_try_bool(fpexe_preflight_report.get("wine_required"))),
                )
                p2.metric(
                    "wine_available",
                    _present_bool(_try_bool(fpexe_preflight_report.get("wine_available"))),
                )
                wineprefix = str(fpexe_preflight_report.get("wineprefix") or "n/a")
                p3.metric("wineprefix", wineprefix)
                if (
                    _try_bool(fpexe_preflight_report.get("wine_required")) is True
                    and _try_bool(fpexe_preflight_report.get("wine_available")) is False
                ):
                    st.info(
                        "Wine is required but unavailable. Install hint: `brew install --cask "
                        "wine-stable` (or your platform equivalent), then rerun parity."
                    )
            if fpexe_stdout_path is not None:
                st.markdown(f"- fp.exe stdout: `{fpexe_stdout_path}`")
            if fpexe_stderr_path is not None:
                st.markdown(f"- fp.exe stderr: `{fpexe_stderr_path}`")
            if fppy_stdout_path is not None:
                st.markdown(f"- fppy stdout: `{fppy_stdout_path}`")
            if fppy_stderr_path is not None:
                st.markdown(f"- fppy stderr: `{fppy_stderr_path}`")
            if not any((fpexe_stdout_path, fpexe_stderr_path, fppy_stdout_path, fppy_stderr_path)):
                st.caption("Stdout/stderr paths are not present in `engine_runs.*` metadata.")

        elif status == "hard_fail":
            st.caption("Run triage artifacts for fast localization:")
            triage_targets = [("triage_summary", triage_summary_path)]
            if triage_hardfails_path is not None:
                triage_targets.append(("triage_hardfails.csv", triage_hardfails_path))
            for name, path in triage_targets:
                if path is not None and path.exists():
                    st.markdown(f"- {name}: `{path}`")
                elif path is not None:
                    st.caption(f"Missing expected {name}: `{path}` (generate via `fp triage`).")

        elif status == "gate_failed" and hard_fail_cell_count == 0:
            if top_first_diff_vars:
                st.caption(
                    "No hard-fail cells; check top first diffs: "
                    + ", ".join(f"`{name}`" for name in top_first_diff_vars)
                )
            else:
                st.caption("No hard-fail cells; inspect top-first diffs in report and `support_gap` artifacts.")
            if fpexe_solution_errors:
                st.caption(
                    "`solution_errors` is present in fp.exe metadata; this usually indicates non-convergence and "
                    "percent diffs should be interpreted cautiously."
                )

    fppy_report_payload: dict[str, Any] | None = None
    fppy_report_path = (
        _resolve_report_path(run, str(fppy_dir / "fppy_report.json"), "work_fppy/fppy_report.json")
        if fppy_dir is not None
        else _resolve_report_path(run, None, "work_fppy/fppy_report.json")
    )
    if fppy_report_path is not None:
        try:
            fppy_report_payload = _load_json(
                str(fppy_report_path),
                _cache_key([fppy_report_path]),
            )
        except Exception:
            fppy_report_payload = None

    seeded_cells: int | None = None
    has_seed_inspected_cells = False
    has_seed_candidate_cells = False
    seed_inspected_cells: int | None = None
    seed_candidate_cells: int | None = None
    solve_window_start = None
    solve_window_end = None
    eq_backfill_converged: bool | None = None
    eq_backfill_iterations: int | None = None
    eq_backfill_min_iters: int | None = None
    eq_backfill_max_iters: int | None = None
    eq_backfill_stop_reason = "unknown"
    eq_backfill_period_sequential: bool | None = None
    eq_backfill_failed: int | None = None
    eq_backfill_first_failure_error: str | None = None
    eq_use_setupsolve: bool | None = None
    if fppy_report_payload and isinstance(fppy_report_payload.get("summary"), dict):
        summary = fppy_report_payload["summary"]
        if "solve_outside_seeded_cells" in summary:
            seeded_cells = _try_int(summary.get("solve_outside_seeded_cells"))
        has_seed_inspected_cells = "solve_outside_seed_inspected_cells" in summary
        has_seed_candidate_cells = "solve_outside_seed_candidate_cells" in summary
        if has_seed_inspected_cells:
            seed_inspected_cells = _try_int(summary.get("solve_outside_seed_inspected_cells"))
        if has_seed_candidate_cells:
            seed_candidate_cells = _try_int(summary.get("solve_outside_seed_candidate_cells"))
        solve_window = summary.get("solve_active_window")
        if isinstance(solve_window, dict):
            solve_window_start = (
                str(solve_window.get("start")) if solve_window.get("start") else None
            )
            solve_window_end = str(solve_window.get("end")) if solve_window.get("end") else None
        if "eq_backfill_converged" in summary:
            eq_backfill_converged = _try_bool(summary.get("eq_backfill_converged"))
        if "eq_backfill_iterations" in summary:
            eq_backfill_iterations = _try_int(summary.get("eq_backfill_iterations"))
        if "eq_backfill_min_iters" in summary:
            eq_backfill_min_iters = _try_int(summary.get("eq_backfill_min_iters"))
        if "eq_backfill_max_iters" in summary:
            eq_backfill_max_iters = _try_int(summary.get("eq_backfill_max_iters"))
        if "eq_backfill_period_sequential" in summary:
            eq_backfill_period_sequential = _try_bool(summary.get("eq_backfill_period_sequential"))
        if "eq_backfill_failed" in summary:
            eq_backfill_failed = _try_int(summary.get("eq_backfill_failed"))
        first_failure_blame = summary.get("eq_backfill_first_failure_blame")
        if isinstance(first_failure_blame, dict):
            first_error = first_failure_blame.get("error")
            if isinstance(first_error, str) and first_error.strip():
                eq_backfill_first_failure_error = first_error.strip()
        if "eq_use_setupsolve" in summary:
            eq_use_setupsolve = _try_bool(summary.get("eq_use_setupsolve"))
        stop_reason = summary.get("eq_backfill_stop_reason")
        if stop_reason is not None and str(stop_reason).strip():
            eq_backfill_stop_reason = str(stop_reason).strip()

    eq_flags_preset, eq_flags_source = _resolve_eq_flags_preset(run, fppy_meta)
    parity_preset_active = (eq_flags_preset or "").strip().lower() == "parity" or (
        eq_use_setupsolve is True
    )
    parity_mode = "parity" if parity_preset_active else "legacy/unknown"

    if parity_preset_active:
        metric_count = 1 + int(has_seed_inspected_cells) + int(has_seed_candidate_cells)
        cols = st.columns(metric_count)
        idx = 0
        cols[idx].metric("solve_outside_seeded_cells", _present_int(seeded_cells))
        idx += 1
        if has_seed_inspected_cells:
            cols[idx].metric("seed_inspected_cells", _present_int(seed_inspected_cells))
            idx += 1
        if has_seed_candidate_cells:
            cols[idx].metric("seed_candidate_cells", _present_int(seed_candidate_cells))
        st.caption(
            "`solve_outside_seeded_cells` is informational under parity preset/setupsolve; "
            "a value of 0 is expected and not a failure signal."
        )
    else:
        status_badge = "OK" if (seeded_cells is not None and seeded_cells > 0) else "MISSING"
        metric_count = 2 + int(has_seed_inspected_cells) + int(has_seed_candidate_cells)
        cols = st.columns(metric_count)
        idx = 0
        c9 = cols[idx]
        idx += 1
        c10 = cols[idx]
        idx += 1
        c9.metric("solve_outside_seeded_cells", _present_int(seeded_cells))
        c10.metric("solve_outside_seeded_status", status_badge)
        if has_seed_inspected_cells:
            cols[idx].metric("seed_inspected_cells", _present_int(seed_inspected_cells))
            idx += 1
        if has_seed_candidate_cells:
            cols[idx].metric("seed_candidate_cells", _present_int(seed_candidate_cells))
    st.subheader("fppy Parity Mode")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("eq_flags_preset", eq_flags_preset or "unknown")
    m2.metric("preset_source", eq_flags_source)
    m3.metric("solve_mode", parity_mode)
    m4.metric("eq_use_setupsolve", _present_bool(eq_use_setupsolve))
    if eq_backfill_failed is not None or eq_backfill_first_failure_error:
        st.subheader("fppy prereq diagnostics")
        d1, d2 = st.columns(2)
        d1.metric("eq_backfill_failed", _present_int(eq_backfill_failed))
        d2.metric("first_failure_error", eq_backfill_first_failure_error or "n/a")
        if _is_missing_variable_failure(eq_backfill_first_failure_error):
            identity_overlay_path = _resolve_report_path(
                run,
                str(fppy_dir / "fppy_identity_overlay.txt") if fppy_dir is not None else None,
                "work_fppy/fppy_identity_overlay.txt",
            )
            overlay_display = (
                str(identity_overlay_path)
                if identity_overlay_path is not None
                else str(run.run_dir / "work_fppy" / "fppy_identity_overlay.txt")
            )
            st.info(
                "Missing-prereq failure detected. Inspect "
                f"`{overlay_display}` for identity overlay details."
            )

    eq_iter_trace_path = _resolve_report_path(
        run,
        str(fppy_dir / "eq_iter_trace.json") if fppy_dir is not None else None,
        "work_fppy/eq_iter_trace.json",
    )
    if eq_iter_trace_path is not None:
        st.subheader("fppy iteration trace (eq_iter_trace.json)")
        st.caption(f"Trace artifact: `{eq_iter_trace_path}`")
        try:
            eq_iter_trace_payload = _load_json_any(
                str(eq_iter_trace_path),
                _cache_key([eq_iter_trace_path]),
            )
            trace_summary = _extract_eq_iter_trace_summary(eq_iter_trace_payload)
            t1, t2, t3, t4 = st.columns(4)
            t1.metric("period", str(trace_summary["period"]))
            t2.metric("events_count", str(trace_summary["events_count"]))
            t3.metric("events_truncated", _present_bool(bool(trace_summary["events_truncated"])))
            t4.metric("targets", str(trace_summary["target_allowlist_count"]))
            target_preview = trace_summary.get("target_allowlist_preview") or []
            if target_preview:
                st.caption("Target allowlist preview: " + ", ".join(f"`{v}`" for v in target_preview))
            iter_rows = trace_summary.get("iteration_rows") or []
            if iter_rows:
                st.caption("First iterations (max delta + applied/failed counts):")
                st.dataframe(pd.DataFrame(iter_rows), hide_index=True, use_container_width=True)
            event_rows = trace_summary.get("event_rows") or []
            if event_rows:
                st.caption("First trace events:")
                st.dataframe(pd.DataFrame(event_rows), hide_index=True, use_container_width=True)
        except Exception as exc:
            st.caption(f"Could not parse eq_iter_trace.json: {exc}")
        st.info(
            "Enable hint: keep `fppy.eq_flags_preset: parity` in scenario YAML. "
            "Iteration trace capture is currently driven by parity mini-run/CLI knobs "
            "(`--eq-iter-trace`, `--eq-iter-trace-period`, `--eq-iter-trace-targets`); "
            "scenario YAML knobs can be used when they are wired."
        )

    regression_path = run.run_dir / "parity_regression.json"
    if regression_path.exists():
        st.subheader("Regression Check")
        try:
            regression_payload = _load_json(
                str(regression_path),
                _cache_key([regression_path]),
            )
            reg_status = str(regression_payload.get("status", "unknown"))
            reg_reason = str(regression_payload.get("reason", "n/a"))
            reg_new_total, reg_resolved_total = _regression_totals(regression_payload)
            r1, r2, r3, r4 = st.columns(4)
            r1.metric("regression_status", reg_status)
            r2.metric("new_findings_total", str(reg_new_total))
            r3.metric("resolved_findings_total", str(reg_resolved_total))
            r4.metric("regression_reason", reg_reason)
            st.download_button(
                "Download parity_regression.json",
                data=regression_path.read_bytes(),
                file_name=regression_path.name,
                mime="application/json",
                key="download_parity_regression_json",
            )
        except Exception as exc:
            st.caption(f"Could not load parity_regression.json: {exc}")

    left_path = _resolve_report_path(run, fpexe_meta.get("pabev_path"), "work_fpexe/PABEV.TXT")
    right_path = _resolve_report_path(run, fppy_meta.get("pabev_path"), "work_fppy/PABEV.TXT")

    st.caption(f"Run directory: {run.run_dir}")
    st.caption(f"fp.exe PABEV: {left_path if left_path else 'not found'}")
    st.caption(f"fppy PABEV: {right_path if right_path else 'not found'}")
    st.caption(f"fppy report: {fppy_report_path if fppy_report_path else 'not found'}")

    if left_path is None or right_path is None:
        st.warning("Both engine PABEV files are required for delta views.")
        return

    pabev_cache = _cache_key([left_path, right_path, run.parity_report_path])
    try:
        left_df = _load_pabev_frame(str(left_path), pabev_cache + ":left")
        right_df = _load_pabev_frame(str(right_path), pabev_cache + ":right")
    except Exception as exc:
        st.error(f"Failed to parse PABEV files: {exc}")
        return

    period_options = [str(col) for col in left_df.columns if col in right_df.columns]
    if not period_options:
        st.warning("No shared periods across fp.exe and fppy PABEV files.")
        return

    start_idx = (
        period_options.index(DEFAULT_START_PERIOD) if DEFAULT_START_PERIOD in period_options else 0
    )
    start_period = st.sidebar.selectbox("Start period", options=period_options, index=start_idx)
    end_options = period_options[start_idx:]
    end_default = end_options[-1] if end_options else period_options[-1]
    if trim_to_converged and suggested_end_period and suggested_end_period in end_options:
        end_default = suggested_end_period
    end_period = st.sidebar.selectbox(
        "End period",
        options=end_options,
        index=max(0, end_options.index(end_default)) if end_default in end_options else max(0, len(end_options) - 1),
    )
    st.caption("PABEV parse and diff slices are cached by artifact path+mtime.")

    missing_sentinels = _normalize_missing_sentinels(detail.get("missing_sentinels"))
    left_slice, right_slice, abs_diff, pct_diff = _load_diff_slices(
        str(left_path),
        str(right_path),
        pabev_cache,
        start_period=start_period,
        end_period=end_period,
        missing_sentinels=tuple(sorted(missing_sentinels)),
    )
    if pct_diff.empty:
        st.warning("No shared variables/periods available after start-period filter.")
        return

    hard_fail_rows = (
        detail.get("hard_fail_cells") if isinstance(detail.get("hard_fail_cells"), list) else []
    )
    hard_fail_df = pd.DataFrame(hard_fail_rows)
    hard_fail_vars = (
        sorted(hard_fail_df["variable"].dropna().astype(str).unique().tolist())
        if not hard_fail_df.empty
        else []
    )

    all_vars = pct_diff.max(axis=1).sort_values(ascending=False).index.tolist()
    hard_fail_in_shared = [var for var in hard_fail_vars if var in all_vars]
    show_hard_fail_only = False
    if hard_fail_in_shared:
        show_hard_fail_only = st.sidebar.toggle("Hard-fail first mode", value=False)
    plot_vars = hard_fail_in_shared if show_hard_fail_only else all_vars

    if not plot_vars:
        st.warning("No variables available for heatmap after current filters.")
        return

    if len(plot_vars) <= 1:
        var_cap = len(plot_vars)
        st.sidebar.caption(f"Heatmap variables: {var_cap}")
    else:
        var_cap_default = min(80, len(plot_vars))
        var_cap = st.sidebar.slider(
            "Heatmap variables",
            min_value=1,
            max_value=len(plot_vars),
            value=var_cap_default,
        )
    top_vars = (
        pct_diff
        .loc[plot_vars]
        .max(axis=1)
        .sort_values(ascending=False)
        .head(var_cap)
        .index.tolist()
    )
    heatmap_df = (pct_diff.loc[top_vars] * 100).round(6)

    st.subheader("Percent-diff heatmap (variables x periods)")
    st.caption("Percent diff uses |fp.exe - fppy| / max(|fp.exe|, |fppy|).")
    try:
        import plotly.express as px

        heatmap_period_labels = [format_period_label(p) for p in heatmap_df.columns.tolist()]
        fig = px.imshow(
            heatmap_df.values,
            x=heatmap_period_labels,
            y=heatmap_df.index.tolist(),
            aspect="auto",
            color_continuous_scale="YlOrRd",
            labels={"x": "period", "y": "variable", "color": "% diff"},
        )
        fig.update_layout(margin=dict(l=10, r=10, t=30, b=10), height=700)
        fig.update_xaxes(type="category", categoryorder="array", categoryarray=heatmap_period_labels)
        apply_white_theme(fig)
        st.plotly_chart(fig, use_container_width=True)
    except ModuleNotFoundError:
        st.warning("Plotly is not installed. Install fp-wraptr[dashboard] to view charts.")

    st.subheader("Hard-fail sample")
    series_vars = plot_vars if show_hard_fail_only else all_vars
    default_var = _pick_default_series_variable(series_vars, hard_fail_vars)
    if not default_var:
        st.warning("No variables available for series view.")
        return

    selected_var = st.sidebar.selectbox(
        "Series variable",
        options=series_vars,
        index=series_vars.index(default_var) if default_var in series_vars else 0,
    )

    if not hard_fail_df.empty:
        hf_filter = st.multiselect(
            "Filter hard-fail variable", options=hard_fail_vars, default=hard_fail_vars[:5]
        )
        filtered = (
            hard_fail_df[hard_fail_df["variable"].isin(hf_filter)] if hf_filter else hard_fail_df
        )
        jump_var = _choose_series_variable_from_hard_fail_table(
            filtered,
            series_vars=series_vars,
        )
        if jump_var is not None:
            selected_var = jump_var
            st.caption(f"Jumped series view to `{selected_var}`.")
    else:
        st.caption("No hard-fail sample rows in parity report.")

    st.subheader(f"Series compare: {selected_var}")
    series_df = pd.DataFrame({
        "period": left_slice.columns.tolist(),
        "fpexe": left_slice.loc[selected_var].tolist(),
        "fppy": right_slice.loc[selected_var].tolist(),
        "abs_diff": abs_diff.loc[selected_var].tolist(),
    })
    series_period_labels = [format_period_label(p) for p in series_df["period"].tolist()]
    series_df_display = series_df.copy()
    series_df_display["period"] = series_period_labels
    series_df["% diff"] = (_safe_percent_diff(series_df["fpexe"], series_df["fppy"]) * 100).round(
        6
    )
    series_df_display["% diff"] = series_df["% diff"]
    st.dataframe(series_df_display, use_container_width=True, height=400)

    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots

        fig_series = make_subplots(specs=[[{"secondary_y": True}]])
        fig_series.add_trace(
            go.Scatter(x=series_period_labels, y=series_df["fpexe"], mode="lines", name="fp.exe"),
            secondary_y=False,
        )
        fig_series.add_trace(
            go.Scatter(x=series_period_labels, y=series_df["fppy"], mode="lines", name="fppy"),
            secondary_y=False,
        )
        fig_series.add_trace(
            go.Bar(x=series_period_labels, y=series_df["abs_diff"], name="abs diff", opacity=0.35),
            secondary_y=True,
        )
        fig_series.update_layout(margin=dict(l=10, r=10, t=20, b=10), height=420)
        fig_series.update_yaxes(title_text="value", secondary_y=False)
        fig_series.update_yaxes(title_text="abs diff", secondary_y=True)
        fig_series.update_xaxes(
            type="category", categoryorder="array", categoryarray=series_period_labels
        )
        apply_white_theme(fig_series)
        st.plotly_chart(fig_series, use_container_width=True)
    except ModuleNotFoundError:
        pass

    triage_path = triage_summary_path
    support_gap_map_path = triage_base_dir / "support_gap_map.csv"
    support_gap_top_path = triage_base_dir / "support_gap_top.md"

    with st.expander("fppy triage buckets + artifacts (advanced)", expanded=False):
        st.subheader("fppy error buckets")
        triage_missing: list[str] = []
        if not triage_path.exists():
            triage_missing.append("triage_summary.json")
        if not triage_issues_path.exists():
            triage_missing.append("triage_issues.csv")
        if not triage_hardfails_path.exists():
            triage_missing.append("triage_hardfails.csv")

        if triage_missing:
            st.subheader("One-Click Triage")
            st.caption(
                "Missing artifacts: "
                + ", ".join(f"`{name}`" for name in triage_missing)
                + ". Generate via `fp triage`."
            )
            if st.button("Generate triage artifacts (fp triage)", key="run_fp_triage"):
                if fppy_dir is None:
                    st.error(
                        "Cannot locate fppy work directory for this run (engine_runs.fppy.work_dir)."
                    )
                    st.stop()
                commands = [
                    [
                        "fp",
                        "triage",
                        "fppy-report",
                        str(fppy_dir),
                        "--out-dir",
                        str(fppy_dir),
                    ],
                    ["fp", "triage", "parity-hardfails", str(run.run_dir)],
                ]
                failed = False
                for cmd in commands:
                    try:
                        proc = subprocess.run(
                            cmd,
                            capture_output=True,
                            text=True,
                            timeout=30,
                            check=False,
                        )
                    except subprocess.TimeoutExpired:
                        failed = True
                        st.error("Triage command timed out after 30s: " + " ".join(cmd))
                        break
                    except FileNotFoundError:
                        failed = True
                        st.error("Triage command failed: `fp` executable not found in PATH.")
                        break
                    if proc.returncode != 0:
                        failed = True
                        stderr_text = proc.stderr.strip() or proc.stdout.strip() or "no output"
                        st.error(
                            "Triage command failed: "
                            + " ".join(cmd)
                            + f" (exit={proc.returncode})\n{stderr_text}"
                        )
                        break
                if not failed:
                    st.cache_data.clear()
                    st.success("Triage artifacts generated. Refreshing view.")
                    st.rerun()

        triage_buckets: dict[str, int] = {}
        if triage_path.exists():
            try:
                triage_payload = _load_json(str(triage_path), _cache_key([triage_path]))
                triage_buckets = _extract_triage_buckets(triage_payload)
            except Exception as exc:
                st.caption(f"Could not parse triage_summary.json: {exc}")

        if not triage_buckets and fppy_report_path is not None and fppy_report_path.exists():
            try:
                if fppy_report_payload is None:
                    fppy_report_payload = _load_json(
                        str(fppy_report_path),
                        _cache_key([fppy_report_path]),
                    )
                triage_buckets = _lightweight_fppy_buckets(fppy_report_payload)
            except Exception as exc:
                st.caption(f"Could not parse fppy_report.json: {exc}")

        if triage_buckets:
            st.dataframe(_top_buckets_table(triage_buckets), use_container_width=True)
        else:
            st.caption("No triage summary buckets found for this run.")

        st.subheader("Triage Hard-Fails CSV")
        if triage_hardfails_path.exists():
            try:
                hardfails_df = _load_csv_frame(
                    str(triage_hardfails_path),
                    _cache_key([triage_hardfails_path]),
                )
                st.dataframe(hardfails_df, use_container_width=True)
                st.download_button(
                    "Download triage_hardfails.csv",
                    data=triage_hardfails_path.read_bytes(),
                    file_name=triage_hardfails_path.name,
                    mime="text/csv",
                    key="download_triage_hardfails_csv",
                )
            except Exception as exc:
                st.caption(f"Could not load triage_hardfails.csv: {exc}")
        else:
            st.caption("No triage_hardfails.csv found for this run.")

        st.subheader("Triage Artifact Links")
        if support_gap_map_path.exists():
            st.download_button(
                "Download support_gap_map.csv",
                data=support_gap_map_path.read_bytes(),
                file_name=support_gap_map_path.name,
                mime="text/csv",
                key="download_support_gap_map_csv",
            )

        st.subheader("Support Gap Top")
        if support_gap_top_path.exists():
            try:
                support_gap_text = _load_text(
                    str(support_gap_top_path),
                    _cache_key([support_gap_top_path]),
                )
                st.markdown(support_gap_text)
                st.download_button(
                    "Download support_gap_top.md",
                    data=support_gap_top_path.read_bytes(),
                    file_name=support_gap_top_path.name,
                    mime="text/markdown",
                    key="download_support_gap_top_md",
                )
            except Exception as exc:
                st.caption(f"Could not load support_gap_top.md: {exc}")
        else:
            st.caption("No support_gap_top.md found for this run.")

    with st.expander("Unsupported command impact", expanded=False):
        if fppy_report_payload is not None:
            unsupported_counts, unsupported_examples_df = _load_unsupported_impact(fppy_report_payload)
            if unsupported_counts:
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("unsupported total", str(sum(unsupported_counts.values())))
                c2.metric("unsupported EQ", str(unsupported_counts.get("EQ", 0)))
                c3.metric("unsupported MODEQ", str(unsupported_counts.get("MODEQ", 0)))
                c4.metric("unsupported FSR", str(unsupported_counts.get("FSR", 0)))
                st.dataframe(_top_buckets_table(unsupported_counts), use_container_width=True)
            else:
                st.caption("No unsupported command counts found in fppy_report summary.")

            if not unsupported_examples_df.empty:
                st.caption("Unsupported examples (top N)")
                st.dataframe(unsupported_examples_df, use_container_width=True)
            else:
                st.caption("No unsupported_examples entries found in fppy_report.json.")
        else:
            st.caption("No fppy_report.json found for unsupported command impact.")

    with st.expander("Zero-filled forecast detector", expanded=False):
        st.subheader("Solve Iteration Stats")
        s1, s2, s3, s4 = st.columns(4)
        s5, s6, s7 = st.columns(3)
        s1.metric("eq_backfill_iterations", _present_int(eq_backfill_iterations))
        s2.metric("eq_backfill_converged", _present_bool(eq_backfill_converged))
        s3.metric("eq_backfill_min_iters", _present_int(eq_backfill_min_iters))
        s4.metric("eq_backfill_max_iters", _present_int(eq_backfill_max_iters))
        s5.metric("eq_backfill_stop_reason", eq_backfill_stop_reason)
        s6.metric("eq_backfill_period_sequential", _present_bool(eq_backfill_period_sequential))
        s7.metric("eq_use_setupsolve", _present_bool(eq_use_setupsolve))

        zero_df = _detect_zero_filled_forecast_vars(
            left_df,
            right_df,
            window_start=solve_window_start or start_period,
            window_end=solve_window_end or end_period,
        )
        if not zero_df.empty:
            st.warning(
                "fppy forecast window has near-zero-only variables while fp.exe is non-zero "
                f"({len(zero_df)} vars)"
            )
            st.caption(f"Solve window: {solve_window_start or 'n/a'} .. {solve_window_end or 'n/a'}")
            st.dataframe(zero_df, use_container_width=True)
            if eq_backfill_iterations == 1:
                st.error(
                    "Potential 1-iteration record-order trap detected: "
                    "`eq_backfill_iterations == 1` with zero-filled forecast variables. "
                    "Likely root cause: derived GENR terms evaluated before EQ updates and not recomputed. "
                    "Recommended next step: apply URSA-0111 fix path and rerun parity."
                )
        else:
            st.caption("No zero-filled forecast variables detected for current solve window.")

    has_any_triage = any(
        path.exists()
        for path in (
            triage_path,
            triage_issues_path,
            triage_hardfails_path,
            support_gap_map_path,
            support_gap_top_path,
        )
    )
    if fppy_report_path is not None and fppy_report_path.exists() and not has_any_triage:
        st.info(
            "fppy_report.json is present but triage outputs are missing. "
            "Run: `python scripts/triage_fppy_report.py --run-dir "
            f"{triage_base_dir} --out-dir {triage_base_dir}`"
        )

    with st.expander("Raw parity_report.json"):
        st.json(report)


if __name__ == "__main__":
    main()
