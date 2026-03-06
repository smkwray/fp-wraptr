"""Run Panels: small multi-run charts over selected completed runs."""

from __future__ import annotations

import hashlib
import math
import re
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from fp_wraptr.dashboard._common import (
    artifacts_dir_from_query,
    logo_html,
    logo_path,
    page_favicon,
    reset_requested_from_query,
)
from fp_wraptr.dashboard.artifacts import (
    ParityRunArtifact,
    RunArtifact,
    backend_name,
    bundle_label,
    filter_runs_by_keys,
    has_multi_period_forecast,
    latest_preferred_runs,
    overlay_paths_for_runs,
    run_dir_key,
    scan_artifacts,
    scan_parity_artifacts,
)
from fp_wraptr.dashboard.mini_dash_helpers import (
    DEFAULT_TRANSFORM_DENOMINATOR,
    PanelExportRun,
    TRANSFORM_LEVEL,
    TRANSFORM_LVL_CHANGE,
    TRANSFORM_PCT_CHANGE,
    TRANSFORM_PCT_OF,
    apply_inversion_safe_hover_style,
    apply_mini_chart_layout,
    build_multi_panel_png,
    build_plotly_export_config,
    default_selected_preset_names,
    delete_mini_dash_preset,
    load_mini_dash_presets,
    reset_mini_dash_presets,
    save_mini_dash_presets,
    upsert_mini_dash_preset,
)
from fp_wraptr.dashboard.plotly_theme import apply_white_theme
from fp_wraptr.dashboard.reset import soft_reset_dashboard_state
from fp_wraptr.data import ModelDictionary
from fp_wraptr.data.dictionary_overlays import load_dictionary_with_overlays
from fp_wraptr.io.loadformat import add_derived_series, read_loadformat
from fp_wraptr.io.parser import parse_fp_output
from fp_wraptr.viz.period_labels import format_period_label

_PERIOD_TOKEN_RE = re.compile(r"^(?P<year>\d{4})\.(?P<sub>\d+)$")
RESET_MENU_URL = "http://localhost:8501/?artifacts-dir=artifacts&reset=1"
_RESET_KEYS = (
    "artifacts_dir",
    "runs",
    "dashboard_active_run_dirs",
    "dashboard_backend_filter",
    "dashboard_bundle_filter",
    "dashboard_scenario_filter",
    "mini_dash_run_keys",
    "mini_dash_data_source",
    "mini_dash_vars",
    "mini_dash_selected_presets",
    "mini_dash_selected_presets_pending",
    "mini_dash_last_selected_presets",
    "mini_dash_export_vars",
    "mini_dash_var_transforms",
    "mini_dash_var_run_comparisons",
)
_TRANSFORM_MODE_TO_LABEL = {
    TRANSFORM_LEVEL: "Level",
    TRANSFORM_PCT_OF: "% of denominator",
    TRANSFORM_LVL_CHANGE: "Lvl change",
    TRANSFORM_PCT_CHANGE: "% change",
}


def _sidebar_logo_markup() -> str:
    from fp_wraptr.dashboard import _common as common

    toggle = getattr(common, "sidebar_logo_toggle_html", None)
    if callable(toggle):
        return toggle(width=56, height=56)
    return logo_html("sidebar-raptr.png", 56, "Raptr")


def _render_sidebar_logo() -> None:
    from fp_wraptr.dashboard import _common as common

    render_toggle = getattr(common, "render_sidebar_logo_toggle", None)
    if callable(render_toggle):
        render_toggle(width=56, height=56)
        return
    st.sidebar.markdown(_sidebar_logo_markup(), unsafe_allow_html=True)


_TRANSFORM_LABEL_TO_MODE = {
    "Level": TRANSFORM_LEVEL,
    "% of denominator": TRANSFORM_PCT_OF,
    "Lvl change": TRANSFORM_LVL_CHANGE,
    "% change": TRANSFORM_PCT_CHANGE,
}
_COMPARE_NONE = "none"
_COMPARE_DIFF_VS_RUN = "diff_vs_run"
_COMPARE_PCT_DIFF_VS_RUN = "pct_diff_vs_run"
_COMPARE_MODE_TO_LABEL = {
    _COMPARE_NONE: "None",
    _COMPARE_DIFF_VS_RUN: "Diff vs run",
    _COMPARE_PCT_DIFF_VS_RUN: "% diff vs run",
}
_COMPARE_LABEL_TO_MODE = {
    "None": _COMPARE_NONE,
    "Diff vs run": _COMPARE_DIFF_VS_RUN,
    "% diff vs run": _COMPARE_PCT_DIFF_VS_RUN,
}


def _reset_dashboard_state() -> None:
    soft_reset_dashboard_state(reset_keys=_RESET_KEYS, artifacts_dir="artifacts")


def _period_key(token: str) -> tuple[int, int, str]:
    match = _PERIOD_TOKEN_RE.match(str(token).strip())
    if not match:
        return (9999, 9999, str(token))
    return (int(match.group("year")), int(match.group("sub")), str(token))


def _sorted_periods(tokens: set[str]) -> list[str]:
    return sorted(tokens, key=_period_key)


def _slice_periods(tokens: list[str], *, start: str | None, end: str | None) -> list[str]:
    if not tokens:
        return []
    start_key = _period_key(start) if start else None
    end_key = _period_key(end) if end else None
    out: list[str] = []
    for tok in tokens:
        key = _period_key(tok)
        if start_key is not None and key < start_key:
            continue
        if end_key is not None and key > end_key:
            continue
        out.append(tok)
    return out


def _cache_key(path: Path) -> str:
    if not path.exists():
        return "missing"
    stat = path.stat()
    return f"{stat.st_size}:{stat.st_mtime_ns}"


def _run_path_label(run: RunArtifact, artifacts_dir: Path) -> str:
    try:
        return str(run.run_dir.relative_to(artifacts_dir))
    except Exception:
        return str(run.run_dir)


def _trace_label(run: RunArtifact, artifacts_dir: Path) -> str:
    group = bundle_label(run, artifacts_dir)
    backend = backend_name(run)
    label = run.scenario_name
    if backend and backend != "unknown":
        label = f"{label} [{backend}]"
    if group and group != "(root)":
        label = f"{label} · {group}"
    return label


def _trace_short_label(run: RunArtifact) -> str:
    return str(run.scenario_name)


def _sync_selected_presets(preset_names: list[str]) -> None:
    current = [
        str(name)
        for name in st.session_state.get("mini_dash_selected_presets", [])
        if str(name).strip()
    ]
    filtered: list[str] = []
    seen: set[str] = set()
    for name in current:
        if name not in preset_names or name in seen:
            continue
        seen.add(name)
        filtered.append(name)
    st.session_state["mini_dash_selected_presets"] = filtered


def _queue_selected_presets_update(preset_names: list[str]) -> None:
    st.session_state["mini_dash_selected_presets_pending"] = [str(name) for name in preset_names if str(name).strip()]


def _ordered_unique(tokens: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for token in tokens:
        value = str(token).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _preset_union_vars(
    preset_names: list[str],
    preset_map: dict[str, list[str]],
    *,
    available_vars: list[str],
) -> list[str]:
    allowed = set(available_vars)
    combined: list[str] = []
    for preset_name in preset_names:
        combined.extend(preset_map.get(preset_name, []))
    return [token for token in _ordered_unique(combined) if token in allowed]


def _run_table(
    runs: list[RunArtifact],
    *,
    selected_keys: set[str],
    artifacts_dir: Path,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for run in runs:
        key = run_dir_key(run.run_dir)
        rows.append(
            {
                "Use": key in selected_keys,
                "Run Key": key,
                "Scenario": run.scenario_name,
                "Timestamp": run.timestamp or "",
                "Backend": backend_name(run),
                "Bundle": bundle_label(run, artifacts_dir),
                "Path": _run_path_label(run, artifacts_dir),
            }
        )
    table = pd.DataFrame(rows)
    if not table.empty:
        table = table.set_index("Run Key", drop=False)
    return table


def _sort_runs_for_table(runs: list[RunArtifact]) -> list[RunArtifact]:
    """Ensure stable ordering so Streamlit row identity doesn't bounce between reruns."""
    by_name_then_path = sorted(runs, key=lambda run: (str(run.scenario_name or ""), str(run.run_dir)))
    return sorted(by_name_then_path, key=lambda run: str(run.timestamp or ""), reverse=True)


def _mini_dash_run_checkbox_key(run_key: str) -> str:
    digest = hashlib.sha1(str(run_key).encode("utf-8")).hexdigest()[:12]
    return f"mini_dash_run_pick_{digest}"


@st.cache_data(ttl=60)
def _file_sha256(path: str, cache_key: str) -> str:
    _ = cache_key
    target = Path(path)
    if not target.exists():
        return ""
    digest = hashlib.sha256()
    with target.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@st.cache_resource
def _load_dictionary(
    overlay_paths_text: tuple[str, ...],
    overlay_cache_keys: tuple[str, ...],
) -> ModelDictionary:
    _ = overlay_cache_keys
    overlay_paths = [Path(text) for text in overlay_paths_text if str(text).strip()]
    return load_dictionary_with_overlays(overlay_paths=overlay_paths)


def _infer_units(var_name: str, dictionary: ModelDictionary) -> str:
    record = dictionary.get_variable(var_name)
    if record is not None and record.units:
        return str(record.units)

    upper = str(var_name).upper()
    if upper in {"UR", "PCY", "PCPF"} or upper.startswith("PC"):
        return "%"

    return ""


def _infer_short_name(var_name: str, dictionary: ModelDictionary) -> str:
    record = dictionary.get_variable(var_name)
    if record is None:
        return str(var_name)
    short_name = str(getattr(record, "short_name", "") or "").strip()
    return short_name or str(var_name)


def _default_denominator(available_vars: list[str]) -> str:
    if "GDP" in available_vars:
        return "GDP"
    if available_vars:
        return str(available_vars[0])
    return DEFAULT_TRANSFORM_DENOMINATOR


def _sanitize_var_transform_config(
    raw_config: object,
    *,
    selected_vars: list[str],
    available_vars: list[str],
    preset_defaults: dict[str, dict[str, str]] | None = None,
) -> dict[str, dict[str, str]]:
    available = {str(token).upper() for token in available_vars}
    selected = [str(token).upper() for token in selected_vars]
    default_denom = _default_denominator(available_vars)
    defaults = preset_defaults if isinstance(preset_defaults, dict) else {}
    source = raw_config if isinstance(raw_config, dict) else {}

    out: dict[str, dict[str, str]] = {}
    for var_name in selected:
        raw_entry = source.get(var_name)
        if not isinstance(raw_entry, dict):
            raw_entry = defaults.get(var_name, {})
        mode = str(raw_entry.get("mode", TRANSFORM_LEVEL) or "").strip().lower() or TRANSFORM_LEVEL
        if mode not in (TRANSFORM_LEVEL, TRANSFORM_PCT_OF, TRANSFORM_LVL_CHANGE, TRANSFORM_PCT_CHANGE):
            mode = TRANSFORM_LEVEL
        denominator = (
            str(raw_entry.get("denominator", default_denom) or "").strip().upper() or default_denom
        )
        if denominator not in available:
            denominator = default_denom
        out[var_name] = {"mode": mode, "denominator": denominator}
    return out


def _sanitize_var_run_compare_config(
    raw_config: object,
    *,
    selected_vars: list[str],
    selected_run_keys: list[str],
    preset_defaults: dict[str, dict[str, object]] | None = None,
) -> dict[str, dict[str, object]]:
    selected = [str(token).upper() for token in selected_vars]
    run_keys = [str(token) for token in selected_run_keys if str(token).strip()]
    run_set = set(run_keys)
    defaults = preset_defaults if isinstance(preset_defaults, dict) else {}
    source = raw_config if isinstance(raw_config, dict) else {}
    default_ref = run_keys[0] if run_keys else ""
    can_compare = len(run_keys) >= 2

    out: dict[str, dict[str, object]] = {}
    for var_name in selected:
        raw_entry = source.get(var_name)
        if not isinstance(raw_entry, dict):
            raw_entry = defaults.get(var_name, {})
        mode = str(raw_entry.get("mode", _COMPARE_NONE) or "").strip().lower() or _COMPARE_NONE
        if mode not in (_COMPARE_NONE, _COMPARE_DIFF_VS_RUN, _COMPARE_PCT_DIFF_VS_RUN):
            mode = _COMPARE_NONE
        if not can_compare:
            mode = _COMPARE_NONE
        reference_run_key = str(raw_entry.get("reference_run_key", default_ref) or "").strip()
        if reference_run_key not in run_set:
            reference_run_key = default_ref
        out[var_name] = {
            "mode": mode,
            "reference_run_key": reference_run_key,
        }
    return out


def _coerce_float(value: object) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float("nan")
    return parsed


def _value_map(periods: list[str], values: list[float]) -> dict[str, float]:
    return {
        str(period): _coerce_float(raw)
        for period, raw in zip(periods, values, strict=False)
    }


def _pct_value(numerator: float, denominator: float) -> float:
    if not (math.isfinite(numerator) and math.isfinite(denominator)):
        return float("nan")
    if denominator == 0.0:
        return float("nan")
    return 100.0 * numerator / denominator


def _level_change_value(current: float, previous: float) -> float:
    if not (math.isfinite(current) and math.isfinite(previous)):
        return float("nan")
    return current - previous


def _pct_change_value(current: float, previous: float) -> float:
    if not (math.isfinite(current) and math.isfinite(previous)):
        return float("nan")
    if previous == 0.0:
        return float("nan")
    return 100.0 * (current / previous - 1.0)


def _transform_series_values(
    *,
    mode: str,
    level_values: list[float],
    denominator_values: list[float] | None = None,
) -> list[float]:
    if mode == TRANSFORM_PCT_OF:
        if not isinstance(denominator_values, list):
            return [float("nan")] * len(level_values)
        out: list[float] = []
        for idx, numer in enumerate(level_values):
            denom = denominator_values[idx] if idx < len(denominator_values) else float("nan")
            out.append(_pct_value(numer, denom))
        return out
    if mode == TRANSFORM_LVL_CHANGE:
        out: list[float] = []
        for idx, current in enumerate(level_values):
            if idx == 0:
                out.append(float("nan"))
                continue
            out.append(_level_change_value(current, level_values[idx - 1]))
        return out
    if mode == TRANSFORM_PCT_CHANGE:
        out: list[float] = []
        for idx, current in enumerate(level_values):
            if idx == 0:
                out.append(float("nan"))
                continue
            out.append(_pct_change_value(current, level_values[idx - 1]))
        return out
    return list(level_values)


def _apply_run_comparison(
    *,
    mode: str,
    values: list[float],
    reference_values: list[float] | None,
) -> list[float]:
    if mode == _COMPARE_NONE:
        return list(values)
    if not isinstance(reference_values, list):
        return [float("nan")] * len(values)
    out: list[float] = []
    for idx, current in enumerate(values):
        reference = reference_values[idx] if idx < len(reference_values) else float("nan")
        if mode == _COMPARE_DIFF_VS_RUN:
            out.append(_level_change_value(current, reference))
        elif mode == _COMPARE_PCT_DIFF_VS_RUN:
            out.append(_pct_change_value(current, reference))
        else:
            out.append(current)
    return out


def _series_values_for_chart(
    *,
    periods: list[str],
    series: dict[str, list[float]],
    variable: str,
    period_tokens: list[str],
    transform: dict[str, str],
) -> list[float] | None:
    values = series.get(variable)
    if not isinstance(values, list):
        return None
    numer = _value_map(periods, values)
    mode = str(transform.get("mode", TRANSFORM_LEVEL))
    level_values = [numer.get(token, float("nan")) for token in period_tokens]
    if mode == TRANSFORM_PCT_OF:
        denominator_var = str(transform.get("denominator", DEFAULT_TRANSFORM_DENOMINATOR)).upper()
        denom_values = series.get(denominator_var)
        if not isinstance(denom_values, list):
            return [float("nan")] * len(period_tokens)
        denom = _value_map(periods, denom_values)
        denominator_series = [denom.get(token, float("nan")) for token in period_tokens]
        return _transform_series_values(
            mode=mode,
            level_values=level_values,
            denominator_values=denominator_series,
        )
    return _transform_series_values(mode=mode, level_values=level_values)


def _transformed_export_values(
    *,
    mode: str,
    level_values: list[float],
    denominator_values: list[float] | None,
) -> list[float]:
    clean_levels = [_coerce_float(raw) for raw in level_values]
    clean_denominator = (
        [_coerce_float(raw) for raw in denominator_values]
        if isinstance(denominator_values, list)
        else None
    )
    return _transform_series_values(
        mode=str(mode or TRANSFORM_LEVEL),
        level_values=clean_levels,
        denominator_values=clean_denominator,
    )


@st.cache_data(ttl=60)
def _load_loadformat(run_dir: str, cache_key: str) -> tuple[list[str], dict[str, list[float]]]:
    _ = cache_key
    periods, series = read_loadformat(Path(run_dir) / "LOADFORMAT.DAT")
    add_derived_series(series)
    return periods, series


@st.cache_data(ttl=60)
def _load_pabev(pabev_path: str, cache_key: str) -> tuple[list[str], dict[str, list[float]]]:
    _ = cache_key
    periods, series = read_loadformat(Path(pabev_path))
    add_derived_series(series)
    return periods, series


@st.cache_data(ttl=60)
def _load_fmout(run_dir: str, cache_key: str):
    _ = cache_key
    return parse_fp_output(Path(run_dir) / "fmout.txt")


def _parity_engine_views(
    parity_run: ParityRunArtifact,
    config_by_run_dir: dict[Path, object],
) -> list[RunArtifact]:
    out: list[RunArtifact] = []
    config = config_by_run_dir.get(parity_run.run_dir)
    base_name = parity_run.scenario_name
    timestamp = parity_run.timestamp

    for engine, rel in (("fpexe", "work_fpexe"), ("fppy", "work_fppy")):
        view_dir = parity_run.run_dir / rel
        pabev_path = view_dir / "PABEV.TXT"
        if not pabev_path.exists():
            continue
        out.append(
            RunArtifact(
                run_dir=view_dir,
                scenario_name=f"{base_name} · parity {engine}",
                timestamp=timestamp,
                has_output=True,
                has_chart=False,
                config=config,  # ScenarioConfig | None (best-effort)
                backend_hint=engine,
            )
        )
    return out


def main() -> None:
    st.set_page_config(
        page_title="fp-wraptr Run Panels",
        page_icon=page_favicon(),
        layout="wide",
        menu_items={"Get Help": RESET_MENU_URL},
    )
    if reset_requested_from_query():
        _reset_dashboard_state()
        st.rerun()
    logo_col, title_col, reset_col = st.columns([1, 7, 2])
    from fp_wraptr.dashboard import _common as common

    render_title_logo = getattr(common, "render_title_logo_toggle", None)
    if callable(render_title_logo):
        with logo_col:
            render_title_logo(width=72, height=72)
    else:
        _header_img = logo_path("header-pair.png")
        if _header_img.exists():
            logo_col.markdown(logo_html("header-pair.png", 72, "fp-wraptr"), unsafe_allow_html=True)
    title_col.title("Run Panels")
    if reset_col.button("Reset Dashboard", use_container_width=True, key="mini_dash_reset_btn"):
        _reset_dashboard_state()
        st.rerun()
    st.caption("Multi-run forecast charts with preset panels and PNG export.")
    st.divider()

    _sidebar_logo = logo_path("sidebar-raptr.png")
    if _sidebar_logo.exists():
        _render_sidebar_logo()

    artifacts_dir = Path(st.session_state.get("artifacts_dir") or artifacts_dir_from_query())
    st.caption(f"Artifacts directory: `{artifacts_dir}`")

    all_runs = scan_artifacts(artifacts_dir)
    st.session_state["runs"] = all_runs

    config_by_run_dir: dict[Path, object] = {run.run_dir: run.config for run in all_runs}
    parity_views: list[RunArtifact] = []
    for parity_run in scan_parity_artifacts(artifacts_dir):
        parity_views.extend(_parity_engine_views(parity_run, config_by_run_dir))

    runs = [run for run in all_runs if run.has_output] + parity_views
    if not runs:
        st.info("No completed runs with output found. Run scenarios from Home or New Run first.")
        return

    active_keys = set(st.session_state.get("dashboard_active_run_dirs", []))
    active_runs = filter_runs_by_keys(runs, active_keys)

    if "mini_dash_run_keys" not in st.session_state:
        seeded = active_runs or latest_preferred_runs(runs, limit=min(3, len(runs)), has_output=True)
        st.session_state["mini_dash_run_keys"] = [run_dir_key(run.run_dir) for run in seeded]

    run_keys_ordered = [str(key) for key in st.session_state.get("mini_dash_run_keys", []) if str(key).strip()]
    selected_keys = set(run_keys_ordered)

    st.subheader("Runs (1-6)")
    st.caption("Compact run picker. Expand to edit, collapse to save space.")
    show_all = st.sidebar.checkbox("Show all runs in picker", value=False)

    runs_for_table = _sort_runs_for_table(
        runs if show_all else (active_runs or latest_preferred_runs(runs, limit=min(8, len(runs)), has_output=True))
    )
    visible_order_keys = [run_dir_key(run.run_dir) for run in runs_for_table]
    visible_keys = set(visible_order_keys)
    st.caption(f"Selected run(s): {len(run_keys_ordered)}")

    hidden_selected = selected_keys - visible_keys
    picker_expanded = bool(st.session_state.get("mini_dash_picker_expanded", False))
    picker_force_sync = bool(st.session_state.get("mini_dash_picker_force_sync", False))
    with st.expander("Run picker (show/hide)", expanded=picker_expanded):
        quick_cols = st.columns([2, 2, 2, 6])
        if quick_cols[0].button("Use Home Active", key="mini_dash_use_home_active"):
            st.session_state["mini_dash_run_keys"] = [run_dir_key(run.run_dir) for run in list(active_runs)[:6]]
            run_keys_ordered = [str(key) for key in st.session_state["mini_dash_run_keys"] if str(key).strip()]
            selected_keys = set(run_keys_ordered)
            hidden_selected = selected_keys - visible_keys
            picker_force_sync = True
            st.session_state["mini_dash_picker_force_sync"] = True
        if quick_cols[1].button("Select all (shown)", key="mini_dash_select_all_shown"):
            hidden_first = [key for key in run_keys_ordered if key not in visible_keys]
            new_order: list[str] = []
            for key in hidden_first:
                if key not in new_order:
                    new_order.append(key)
                if len(new_order) >= 6:
                    break
            for key in visible_order_keys:
                if key not in new_order:
                    new_order.append(key)
                if len(new_order) >= 6:
                    break
            st.session_state["mini_dash_run_keys"] = new_order
            run_keys_ordered = list(new_order)
            selected_keys = set(run_keys_ordered)
            hidden_selected = selected_keys - visible_keys
            picker_force_sync = True
            st.session_state["mini_dash_picker_force_sync"] = True
        if quick_cols[2].button("Select none", key="mini_dash_select_none"):
            st.session_state["mini_dash_run_keys"] = []
            run_keys_ordered = []
            selected_keys = set()
            hidden_selected = set()
            picker_force_sync = True
            st.session_state["mini_dash_picker_force_sync"] = True

        if hidden_selected and not show_all:
            st.caption(
                f"{len(hidden_selected)} selected run(s) not shown. "
                "Enable 'Show all runs in picker' to edit the full selection."
            )

        with st.container(height=min(320, 44 * max(4, len(runs_for_table) + 1))):
            header_cols = st.columns([1, 3, 2, 2, 2, 5])
            for col, label in zip(
                header_cols,
                ["Use", "Scenario", "Timestamp", "Backend", "Bundle", "Path"],
                strict=False,
            ):
                col.caption(label)

            selected_visible: set[str] = set()
            visible_edited_order: list[str] = []
            for run in runs_for_table:
                run_key = run_dir_key(run.run_dir)
                widget_key = _mini_dash_run_checkbox_key(run_key)
                if picker_force_sync or widget_key not in st.session_state:
                    st.session_state[widget_key] = run_key in selected_keys
                row_cols = st.columns([1, 3, 2, 2, 2, 5])
                checked = row_cols[0].checkbox(
                    f"Use {run.scenario_name}",
                    key=widget_key,
                    label_visibility="collapsed",
                )
                row_cols[1].caption(str(run.scenario_name))
                row_cols[2].caption(str(run.timestamp or ""))
                row_cols[3].caption(str(backend_name(run)))
                row_cols[4].caption(str(bundle_label(run, artifacts_dir)))
                row_cols[5].caption(_run_path_label(run, artifacts_dir))
                visible_edited_order.append(run_key)
                if checked:
                    selected_visible.add(run_key)
        st.session_state["mini_dash_picker_force_sync"] = False
        combined_selected = hidden_selected | selected_visible
        new_ordered_keys: list[str] = []
        for key in run_keys_ordered:
            if key in combined_selected and key not in new_ordered_keys:
                new_ordered_keys.append(key)
        for key in visible_edited_order:
            if key in selected_visible and key not in new_ordered_keys:
                new_ordered_keys.append(key)
        if len(new_ordered_keys) > 6:
            st.warning("Up to 6 runs are supported. Keeping the first 6 selected rows.")
            new_ordered_keys = new_ordered_keys[:6]
        if new_ordered_keys != run_keys_ordered:
            st.session_state["mini_dash_run_keys"] = new_ordered_keys
            run_keys_ordered = new_ordered_keys
            selected_keys = set(run_keys_ordered)

    by_key_all = {run_dir_key(run.run_dir): run for run in runs}
    selected_runs = [by_key_all[key] for key in run_keys_ordered if key in by_key_all]
    if len(selected_runs) != len(run_keys_ordered):
        st.session_state["mini_dash_run_keys"] = [run_dir_key(run.run_dir) for run in selected_runs]
        st.session_state["mini_dash_picker_force_sync"] = True
        run_keys_ordered = st.session_state["mini_dash_run_keys"]
        selected_keys = set(run_keys_ordered)

    if len(selected_runs) < 1:
        st.info("Select at least one run to view charts.")
        return
    if len(selected_runs) > 6:
        st.warning("Select up to 6 runs for readability/performance.")
        selected_runs = list(selected_runs)[:6]
        st.session_state["mini_dash_run_keys"] = [run_dir_key(run.run_dir) for run in selected_runs]
        st.session_state["mini_dash_picker_force_sync"] = True
        run_keys_ordered = st.session_state["mini_dash_run_keys"]
        selected_keys = set(run_keys_ordered)
    short_window_runs = [run for run in selected_runs if not has_multi_period_forecast(run)]
    if short_window_runs:
        st.info(
            "Some selected runs have a one-period forecast window. "
            "Use longer-horizon runs if you want visible trend lines."
        )

    force_white = st.sidebar.checkbox("Force white charts", value=True)
    columns = st.sidebar.radio("Columns", options=[1, 2, 3], index=1, horizontal=True)
    chart_height = st.sidebar.slider("Chart height", min_value=200, max_value=520, value=320, step=10)

    has_loadformat = [((run.run_dir / "LOADFORMAT.DAT").exists()) for run in selected_runs]
    can_use_loadformat = all(has_loadformat)
    has_pabev = [((run.run_dir / "PABEV.TXT").exists()) for run in selected_runs]
    can_use_pabev = all(has_pabev)
    has_fmout = [((run.run_dir / "fmout.txt").exists()) for run in selected_runs]
    can_use_fmout = all(has_fmout)
    source_options = ["LOADFORMAT", "PABEV", "fmout"]
    if can_use_loadformat:
        default_source = "LOADFORMAT"
    elif can_use_pabev:
        default_source = "PABEV"
    else:
        default_source = "fmout"

    fmout_hashes = {
        _file_sha256(str(run.run_dir / "fmout.txt"), _cache_key(run.run_dir / "fmout.txt"))
        for run in selected_runs
        if (run.run_dir / "fmout.txt").exists()
    }
    loadformat_hashes = {
        _file_sha256(str(run.run_dir / "LOADFORMAT.DAT"), _cache_key(run.run_dir / "LOADFORMAT.DAT"))
        for run in selected_runs
        if (run.run_dir / "LOADFORMAT.DAT").exists()
    }
    stale_fmout_detected = (
        can_use_loadformat
        and len(fmout_hashes) == 1
        and len(loadformat_hashes) > 1
    )
    if stale_fmout_detected:
        st.sidebar.warning(
            "Selected runs have identical `fmout.txt` but different `LOADFORMAT.DAT`; "
            "using `LOADFORMAT` so scenarios stay distinct."
        )
        st.session_state["mini_dash_data_source"] = "LOADFORMAT"
    elif "mini_dash_data_source" not in st.session_state:
        st.session_state["mini_dash_data_source"] = default_source

    data_source = st.sidebar.radio(
        "Data source",
        options=source_options,
        key="mini_dash_data_source",
    )
    if data_source == "LOADFORMAT" and not can_use_loadformat:
        st.warning("One or more selected runs are missing `LOADFORMAT.DAT`; switch to fmout or rerun.")
        return
    if data_source == "PABEV" and not can_use_pabev:
        st.warning("One or more selected runs are missing `PABEV.TXT`; switch sources or rerun.")
        return
    if data_source == "fmout" and not can_use_fmout:
        st.warning("One or more selected runs are missing `fmout.txt`; switch sources or rerun.")
        return

    presets, preset_warning = load_mini_dash_presets(artifacts_dir)
    if preset_warning:
        st.warning(preset_warning)
    preset_names = [str(item["name"]) for item in presets]
    pending_selected_presets = st.session_state.pop("mini_dash_selected_presets_pending", None)
    if isinstance(pending_selected_presets, list):
        st.session_state["mini_dash_selected_presets"] = [
            str(name) for name in pending_selected_presets if str(name).strip()
        ]
    if "mini_dash_selected_presets" not in st.session_state:
        st.session_state["mini_dash_selected_presets"] = default_selected_preset_names(preset_names)
    _sync_selected_presets(preset_names)

    selected_presets = st.sidebar.multiselect(
        "Presets",
        options=preset_names,
        key="mini_dash_selected_presets",
        help=(
            "Select saved variable groups for quick chart loading. "
            "Only a preset literally named `Default` auto-loads on first open; "
            "other presets stay unselected until you choose them."
        ),
    )

    # Load series to discover available variables + periods.
    series_by_run: dict[str, dict[str, object]] = {}
    all_period_tokens: set[str] = set()
    all_variables: set[str] = set()
    forecast_starts: list[str] = []
    forecast_ends: list[str] = []

    for run in selected_runs:
        if run.config is not None:
            if run.config.forecast_start:
                forecast_starts.append(str(run.config.forecast_start))
            if run.config.forecast_end:
                forecast_ends.append(str(run.config.forecast_end))

        if data_source == "LOADFORMAT":
            lf_path = run.run_dir / "LOADFORMAT.DAT"
            periods, series = _load_loadformat(str(run.run_dir), _cache_key(lf_path))
            series_by_run[str(run.run_dir)] = {"periods": periods, "series": series}
            all_period_tokens.update(periods)
            all_variables.update(series.keys())
        elif data_source == "PABEV":
            pabev_path = run.run_dir / "PABEV.TXT"
            periods, series = _load_pabev(str(pabev_path), _cache_key(pabev_path))
            series_by_run[str(run.run_dir)] = {"periods": periods, "series": series}
            all_period_tokens.update(periods)
            all_variables.update(series.keys())
        else:
            fmout_path = run.run_dir / "fmout.txt"
            data = _load_fmout(str(run.run_dir), _cache_key(fmout_path))
            if data is None or not getattr(data, "variables", None):
                continue
            periods = list(getattr(data, "periods", []) or [])
            series: dict[str, list[float]] = {}
            for name, var in data.variables.items():
                series[name] = list(getattr(var, "levels", []) or [])
            add_derived_series(series)
            series_by_run[str(run.run_dir)] = {"periods": periods, "series": series}
            all_period_tokens.update(periods)
            all_variables.update(series.keys())

    if not series_by_run:
        st.error("No usable data found for the selected runs.")
        return

    all_periods_sorted = _sorted_periods(all_period_tokens)

    # Period controls
    mode = st.sidebar.radio("Period range", options=["Forecast only", "Custom"], index=0)
    start_token: str | None = None
    end_token: str | None = None
    if mode == "Forecast only" and forecast_starts and forecast_ends:
        start_token = min(forecast_starts, key=_period_key)
        end_token = max(forecast_ends, key=_period_key)
    elif mode == "Forecast only" and forecast_starts:
        start_token = min(forecast_starts, key=_period_key)
        end_token = None
    elif mode == "Forecast only" and forecast_ends:
        start_token = None
        end_token = max(forecast_ends, key=_period_key)
    else:
        label_map = {tok: format_period_label(tok) for tok in all_periods_sorted}
        start_token = st.sidebar.selectbox(
            "Start",
            options=all_periods_sorted,
            format_func=lambda tok: label_map.get(tok, tok),
            index=0,
        )
        end_token = st.sidebar.selectbox(
            "End",
            options=all_periods_sorted,
            format_func=lambda tok: label_map.get(tok, tok),
            index=len(all_periods_sorted) - 1,
        )

    period_tokens = _slice_periods(all_periods_sorted, start=start_token, end=end_token)
    if not period_tokens:
        st.warning("No periods in the selected range.")
        return
    if len(period_tokens) == 1:
        st.caption(
            "Selected range currently has one period only "
            f"(`{format_period_label(period_tokens[0])}`)."
        )

    x_labels = [format_period_label(tok) for tok in period_tokens]
    category_axis = dict(type="category", categoryorder="array", categoryarray=x_labels)

    # Variables selector
    available_vars = sorted(all_variables)
    preset_map = {str(item["name"]): list(item["variables"]) for item in presets}
    preset_transform_map = {
        str(item["name"]): (
            item.get("transforms", {}) if isinstance(item.get("transforms"), dict) else {}
        )
        for item in presets
    }
    preset_run_comparison_map = {
        str(item["name"]): (
            item.get("run_comparisons", {}) if isinstance(item.get("run_comparisons"), dict) else {}
        )
        for item in presets
    }
    requested_preset_vars: list[str] = []
    for preset_name in selected_presets:
        requested_preset_vars.extend(preset_map.get(preset_name, []))
    missing_preset_vars = sorted({token for token in requested_preset_vars if token not in all_variables})
    if missing_preset_vars:
        preview = ", ".join(f"`{token}`" for token in missing_preset_vars[:12])
        suffix = " (and more)" if len(missing_preset_vars) > 12 else ""
        st.warning(f"Preset variables unavailable in selected runs were skipped: {preview}{suffix}")

    preset_vars = _preset_union_vars(selected_presets, preset_map, available_vars=available_vars)
    default_vars = preset_vars or available_vars[: min(6, len(available_vars))]

    if "mini_dash_vars" not in st.session_state:
        st.session_state["mini_dash_vars"] = list(default_vars)
    else:
        previous_selected_presets = [
            str(name)
            for name in st.session_state.get("mini_dash_last_selected_presets", [])
            if str(name).strip()
        ]
        current_vars = [str(token) for token in st.session_state.get("mini_dash_vars", [])]
        sanitized = [token for token in current_vars if token in available_vars]
        previous_preset_vars = _preset_union_vars(
            previous_selected_presets,
            preset_map,
            available_vars=available_vars,
        )
        if previous_selected_presets != selected_presets:
            manual_vars = [token for token in sanitized if token not in set(previous_preset_vars)]
            sanitized = _ordered_unique([*preset_vars, *manual_vars])
        if not sanitized and default_vars:
            sanitized = list(default_vars)
        st.session_state["mini_dash_vars"] = sanitized
    st.session_state["mini_dash_last_selected_presets"] = list(selected_presets)

    selected_vars = st.sidebar.multiselect(
        "Variables",
        options=available_vars,
        key="mini_dash_vars",
    )
    if not selected_vars:
        st.info("Select at least one variable.")
        return

    preset_transform_defaults: dict[str, dict[str, str]] = {}
    for preset_name in selected_presets:
        raw_map = preset_transform_map.get(preset_name, {})
        if not isinstance(raw_map, dict):
            continue
        for raw_var, raw_cfg in raw_map.items():
            var_name = str(raw_var or "").strip().upper()
            if not var_name or not isinstance(raw_cfg, dict):
                continue
            mode = str(raw_cfg.get("mode", TRANSFORM_LEVEL) or "").strip().lower() or TRANSFORM_LEVEL
            if mode not in (TRANSFORM_LEVEL, TRANSFORM_PCT_OF, TRANSFORM_LVL_CHANGE, TRANSFORM_PCT_CHANGE):
                mode = TRANSFORM_LEVEL
            denominator = (
                str(raw_cfg.get("denominator", _default_denominator(available_vars)) or "")
                .strip()
                .upper()
                or _default_denominator(available_vars)
            )
            preset_transform_defaults[var_name] = {"mode": mode, "denominator": denominator}

    selected_var_transforms = _sanitize_var_transform_config(
        st.session_state.get("mini_dash_var_transforms", {}),
        selected_vars=[str(token).upper() for token in selected_vars],
        available_vars=available_vars,
        preset_defaults=preset_transform_defaults,
    )
    st.session_state["mini_dash_var_transforms"] = selected_var_transforms

    selected_run_keys_ordered = [run_dir_key(run.run_dir) for run in selected_runs]
    run_short_label_by_key = {
        run_dir_key(run.run_dir): _trace_short_label(run)
        for run in selected_runs
    }
    run_reference_label_by_key: dict[str, str] = {}
    seen_reference_labels: dict[str, int] = {}
    for run_key in selected_run_keys_ordered:
        base_label = run_short_label_by_key.get(run_key, run_key)
        count = seen_reference_labels.get(base_label, 0) + 1
        seen_reference_labels[base_label] = count
        run_reference_label_by_key[run_key] = (
            base_label if count == 1 else f"{base_label} ({count})"
        )
    run_reference_key_by_label = {
        label: run_key
        for run_key, label in run_reference_label_by_key.items()
    }
    default_reference_label = (
        run_reference_label_by_key.get(selected_run_keys_ordered[0], "")
        if selected_run_keys_ordered
        else ""
    )
    preset_run_compare_defaults: dict[str, dict[str, object]] = {}
    for preset_name in selected_presets:
        raw_map = preset_run_comparison_map.get(preset_name, {})
        if not isinstance(raw_map, dict):
            continue
        for raw_var, raw_cfg in raw_map.items():
            var_name = str(raw_var or "").strip().upper()
            if not var_name or not isinstance(raw_cfg, dict):
                continue
            mode = str(raw_cfg.get("mode", _COMPARE_NONE) or "").strip().lower() or _COMPARE_NONE
            if mode not in (_COMPARE_NONE, _COMPARE_DIFF_VS_RUN, _COMPARE_PCT_DIFF_VS_RUN):
                mode = _COMPARE_NONE
            if mode == _COMPARE_NONE:
                continue
            ref_label = str(raw_cfg.get("reference_run_label", "") or "").strip()
            reference_run_key = run_reference_key_by_label.get(ref_label, selected_run_keys_ordered[0])
            preset_run_compare_defaults[var_name] = {
                "mode": mode,
                "reference_run_key": reference_run_key,
            }
    selected_var_run_comparisons = _sanitize_var_run_compare_config(
        st.session_state.get("mini_dash_var_run_comparisons", {}),
        selected_vars=[str(token).upper() for token in selected_vars],
        selected_run_keys=selected_run_keys_ordered,
        preset_defaults=preset_run_compare_defaults,
    )
    st.session_state["mini_dash_var_run_comparisons"] = selected_var_run_comparisons

    mark_forecast_start = st.sidebar.checkbox("Mark forecast start", value=True)
    inversion_safe_hover = st.sidebar.checkbox("Inversion-safe hover labels", value=True)

    st.sidebar.markdown("#### Download quality")
    export_width_px = st.sidebar.number_input(
        "Download width (px)",
        min_value=600,
        max_value=5000,
        value=1600,
        step=100,
    )
    export_height_px = st.sidebar.number_input(
        "Download height (px)",
        min_value=400,
        max_value=5000,
        value=900,
        step=100,
    )
    export_scale = st.sidebar.slider("Download scale", min_value=1, max_value=6, value=2)
    plotly_export_config = build_plotly_export_config(
        width_px=int(export_width_px),
        height_px=int(export_height_px),
        scale=int(export_scale),
    )

    vline_x = format_period_label(start_token) if (mark_forecast_start and start_token) else None

    overlay_paths = overlay_paths_for_runs(selected_runs)
    dictionary = _load_dictionary(
        tuple(str(path) for path in overlay_paths),
        tuple(_cache_key(path) for path in overlay_paths),
    )
    if overlay_paths:
        st.caption(
            "Dictionary overlays applied: "
            + ", ".join(f"`{path}`" for path in overlay_paths[:3])
            + (f" (+{len(overlay_paths) - 3} more)" if len(overlay_paths) > 3 else "")
        )

    st.subheader("Charts")
    cols = st.columns(int(columns))
    for idx, var_name in enumerate(selected_vars):
        var_code = str(var_name).upper()
        transform = selected_var_transforms.get(
            var_code,
            {"mode": TRANSFORM_LEVEL, "denominator": _default_denominator(available_vars)},
        )
        compare_cfg = selected_var_run_comparisons.get(
            var_code,
            {"mode": _COMPARE_NONE, "reference_run_key": ""},
        )
        compare_mode = str(compare_cfg.get("mode", _COMPARE_NONE))
        reference_run_key = str(compare_cfg.get("reference_run_key", ""))
        reference_run_label = run_short_label_by_key.get(reference_run_key, reference_run_key or "reference")
        transform_mode = str(transform.get("mode", TRANSFORM_LEVEL))
        denominator_var = str(transform.get("denominator", _default_denominator(available_vars))).upper()
        title = _infer_short_name(var_code, dictionary)
        value_label = "Value"
        units = _infer_units(var_code, dictionary)
        hover_value_format = "%{y:,.4f}<extra></extra>"
        if transform_mode == TRANSFORM_PCT_OF:
            title = f"{title} (% of {denominator_var})"
            units = f"% of {denominator_var}"
            value_label = f"% of {denominator_var}"
            hover_value_format = "%{y:,.3f}<extra></extra>"
        elif transform_mode == TRANSFORM_LVL_CHANGE:
            title = f"{title} (Lvl change)"
            value_label = "Lvl change"
        elif transform_mode == TRANSFORM_PCT_CHANGE:
            title = f"{title} (% change)"
            units = "%"
            value_label = "% change"
            hover_value_format = "%{y:,.3f}<extra></extra>"
        if compare_mode == _COMPARE_DIFF_VS_RUN:
            title = f"{title} (Diff vs {reference_run_label})"
            value_label = f"Diff vs {reference_run_label}"
        elif compare_mode == _COMPARE_PCT_DIFF_VS_RUN:
            title = f"{title} (% diff vs {reference_run_label})"
            value_label = f"% diff vs {reference_run_label}"
            units = "%"
            hover_value_format = "%{y:,.3f}<extra></extra>"
        fig = go.Figure()
        transformed_by_run_key: dict[str, list[float]] = {}
        traces: list[tuple[RunArtifact, str, list[float]]] = []
        for run in selected_runs:
            run_key = run_dir_key(run.run_dir)
            payload = series_by_run.get(str(run.run_dir))
            if not payload:
                continue
            periods = payload["periods"]
            series = payload["series"]
            if not isinstance(periods, list) or not isinstance(series, dict):
                continue
            y = _series_values_for_chart(
                periods=[str(token) for token in periods],
                series=series,
                variable=var_code,
                period_tokens=period_tokens,
                transform=transform,
            )
            if y is None:
                continue
            transformed_by_run_key[run_key] = y
            traces.append((run, run_key, y))
        reference_values = transformed_by_run_key.get(reference_run_key)
        for run, run_key, y_base in traces:
            y = _apply_run_comparison(
                mode=compare_mode,
                values=y_base,
                reference_values=reference_values,
            )
            if compare_mode != _COMPARE_NONE and run_key == reference_run_key:
                continue
            short_label = _trace_short_label(run)
            full_label = _trace_label(run, artifacts_dir)
            fig.add_trace(
                go.Scatter(
                    x=x_labels,
                    y=y,
                    mode="lines+markers",
                    name=short_label,
                    customdata=[full_label] * len(x_labels),
                    hovertemplate=(
                        "<b>%{customdata}</b><br>"
                        "Period: %{x}<br>"
                        f"{value_label}: "
                        + hover_value_format
                    ),
                )
            )
        apply_mini_chart_layout(
            fig,
            title=title,
            height=int(chart_height),
        )
        if units:
            fig.update_yaxes(title_text=units)
        fig.update_xaxes(**category_axis)
        if vline_x and vline_x in x_labels:
            fig.add_vline(x=vline_x, line_width=1, line_dash="dot", line_color="#111827")
        apply_white_theme(fig, enabled=bool(force_white))
        apply_inversion_safe_hover_style(fig, enabled=bool(inversion_safe_hover))

        cols[idx % int(columns)].plotly_chart(
            fig,
            use_container_width=True,
            config=plotly_export_config,
        )

    transform_rows = []
    for var_name in [str(token).upper() for token in selected_vars]:
        cfg = selected_var_transforms.get(
            var_name,
            {"mode": TRANSFORM_LEVEL, "denominator": _default_denominator(available_vars)},
        )
        transform_rows.append(
            {
                "Variable": var_name,
                "Transform": _TRANSFORM_MODE_TO_LABEL.get(str(cfg.get("mode")), "Level"),
                "Denominator": str(cfg.get("denominator", _default_denominator(available_vars))),
            }
        )
    with st.expander("Per-series transform (show/hide)", expanded=False):
        st.caption("Set each chart to Level, % of denominator, Lvl change, or % change.")
        edited_transforms = st.data_editor(
            pd.DataFrame(transform_rows),
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            key="mini_dash_transform_table",
            column_config={
                "Variable": st.column_config.TextColumn("Variable", width="small"),
                "Transform": st.column_config.SelectboxColumn(
                    "Transform",
                    options=list(_TRANSFORM_LABEL_TO_MODE.keys()),
                    width="medium",
                ),
                "Denominator": st.column_config.SelectboxColumn(
                    "Denominator",
                    options=available_vars,
                    width="small",
                ),
            },
            disabled=["Variable"],
        )
        next_transforms: dict[str, dict[str, str]] = {}
        for row in edited_transforms.to_dict("records"):
            var_name = str(row.get("Variable", "") or "").strip().upper()
            if not var_name:
                continue
            mode_label = str(row.get("Transform", "Level") or "Level")
            mode = _TRANSFORM_LABEL_TO_MODE.get(mode_label, TRANSFORM_LEVEL)
            denominator = str(
                row.get("Denominator", _default_denominator(available_vars)) or ""
            ).strip().upper()
            if not denominator:
                denominator = _default_denominator(available_vars)
            next_transforms[var_name] = {"mode": mode, "denominator": denominator}
        next_selected_var_transforms = _sanitize_var_transform_config(
            next_transforms,
            selected_vars=[str(token).upper() for token in selected_vars],
            available_vars=available_vars,
            preset_defaults=preset_transform_defaults,
        )
        if next_selected_var_transforms != selected_var_transforms:
            st.session_state["mini_dash_var_transforms"] = next_selected_var_transforms
            st.rerun()

    compare_rows = []
    for var_name in [str(token).upper() for token in selected_vars]:
        cfg = selected_var_run_comparisons.get(
            var_name,
            {"mode": _COMPARE_NONE, "reference_run_key": ""},
        )
        ref_key = str(cfg.get("reference_run_key", ""))
        compare_rows.append(
            {
                "Variable": var_name,
                "Compare": _COMPARE_MODE_TO_LABEL.get(str(cfg.get("mode", _COMPARE_NONE)), "None"),
                "Reference run": run_reference_label_by_key.get(ref_key, default_reference_label),
            }
        )
    with st.expander("Per-series run comparison (show/hide)", expanded=False):
        st.caption("Compare each series to a selected run after value transforms.")
        edited_compare = st.data_editor(
            pd.DataFrame(compare_rows),
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            key="mini_dash_run_compare_table",
            column_config={
                "Variable": st.column_config.TextColumn("Variable", width="small"),
                "Compare": st.column_config.SelectboxColumn(
                    "Compare",
                    options=list(_COMPARE_LABEL_TO_MODE.keys()),
                    width="small",
                ),
                "Reference run": st.column_config.SelectboxColumn(
                    "Reference run",
                    options=list(run_reference_key_by_label.keys()),
                    width="large",
                ),
            },
            disabled=["Variable"],
        )
        next_compare: dict[str, dict[str, object]] = {}
        for row in edited_compare.to_dict("records"):
            var_name = str(row.get("Variable", "") or "").strip().upper()
            if not var_name:
                continue
            mode_label = str(row.get("Compare", "None") or "None")
            mode = _COMPARE_LABEL_TO_MODE.get(mode_label, _COMPARE_NONE)
            ref_label = str(row.get("Reference run", default_reference_label) or default_reference_label)
            reference_run_key = run_reference_key_by_label.get(ref_label, selected_run_keys_ordered[0])
            next_compare[var_name] = {
                "mode": mode,
                "reference_run_key": reference_run_key,
            }
        next_selected_var_run_comparisons = _sanitize_var_run_compare_config(
            next_compare,
            selected_vars=[str(token).upper() for token in selected_vars],
            selected_run_keys=selected_run_keys_ordered,
            preset_defaults=preset_run_compare_defaults,
        )
        if next_selected_var_run_comparisons != selected_var_run_comparisons:
            st.session_state["mini_dash_var_run_comparisons"] = next_selected_var_run_comparisons
            st.rerun()

    st.subheader("Save Current Preset")
    st.caption("Save the current variables, per-series transforms, and per-series run comparisons.")
    save_target = st.selectbox(
        "Preset target",
        options=["New preset", *preset_names],
        key="mini_dash_save_target",
        help="Choose an existing preset to overwrite, or create a new named preset.",
    )
    save_name = ""
    original_name: str | None = None
    if save_target == "New preset":
        save_name = st.text_input("New preset name", key="mini_dash_save_name")
    else:
        save_name = save_target
        original_name = save_target
        st.caption(f"Saving will overwrite `{save_target}`.")
    if st.button("Save current preset", key="mini_dash_save_current_preset_btn"):
        try:
            current_transform_payload = {
                var_name: dict(cfg)
                for var_name, cfg in selected_var_transforms.items()
                if var_name in {str(token).upper() for token in selected_vars}
                and str(cfg.get("mode", TRANSFORM_LEVEL)) != TRANSFORM_LEVEL
            }
            current_run_comparison_payload = {}
            for var_name, cfg in selected_var_run_comparisons.items():
                if var_name not in {str(token).upper() for token in selected_vars}:
                    continue
                mode = str(cfg.get("mode", _COMPARE_NONE) or _COMPARE_NONE)
                if mode == _COMPARE_NONE:
                    continue
                reference_run_key = str(cfg.get("reference_run_key", "") or "")
                current_run_comparison_payload[var_name] = {
                    "mode": mode,
                    "reference_run_label": run_reference_label_by_key.get(
                        reference_run_key,
                        default_reference_label,
                    ),
                }
            updated = upsert_mini_dash_preset(
                presets,
                new_name=save_name,
                variables=selected_vars,
                transforms=current_transform_payload,
                run_comparisons=current_run_comparison_payload,
                original_name=original_name,
            )
            save_mini_dash_presets(artifacts_dir, updated)
            if save_name:
                next_selected_presets = [
                    name
                    for name in st.session_state.get("mini_dash_selected_presets", [])
                    if name in [item["name"] for item in updated] and name != original_name
                ]
                next_selected_presets.append(save_name)
                _queue_selected_presets_update(next_selected_presets)
            else:
                _queue_selected_presets_update(st.session_state.get("mini_dash_selected_presets", []))
            st.success(f"Preset saved: {save_name}")
            st.rerun()
        except ValueError as exc:
            st.error(str(exc))

    st.subheader("Panel Export (PNG)")
    st.caption("Build one combined PNG with up to 6 selected variables.")

    if "mini_dash_export_vars" not in st.session_state:
        st.session_state["mini_dash_export_vars"] = selected_vars[: min(6, len(selected_vars))]
    else:
        export_vars_state = [str(token) for token in st.session_state.get("mini_dash_export_vars", [])]
        st.session_state["mini_dash_export_vars"] = [
            token for token in export_vars_state if token in available_vars
        ][:6]

    export_vars = st.multiselect(
        "Export variables (1-6)",
        options=available_vars,
        key="mini_dash_export_vars",
    )
    export_title = st.text_input(
        "Export title",
        value="Run Panels Export",
        key="mini_dash_export_title",
    )
    export_dpi = st.slider("PNG DPI", min_value=120, max_value=600, value=300, step=10)
    export_var_transforms = _sanitize_var_transform_config(
        st.session_state.get("mini_dash_var_transforms", {}),
        selected_vars=[str(token).upper() for token in export_vars],
        available_vars=available_vars,
        preset_defaults=preset_transform_defaults,
    )
    export_var_run_comparisons = _sanitize_var_run_compare_config(
        st.session_state.get("mini_dash_var_run_comparisons", {}),
        selected_vars=[str(token).upper() for token in export_vars],
        selected_run_keys=selected_run_keys_ordered,
    )

    export_var_codes = [str(token).upper() for token in export_vars]
    base_export_by_run: dict[str, dict[str, list[float]]] = {}
    for run in selected_runs:
        run_key = run_dir_key(run.run_dir)
        payload = series_by_run.get(str(run.run_dir))
        if not payload:
            continue
        periods = [str(token) for token in list(payload.get("periods", []) or [])]
        raw_series = payload.get("series")
        if not isinstance(raw_series, dict):
            continue
        clean_series: dict[str, list[float]] = {}
        for name, values in raw_series.items():
            if not isinstance(values, list):
                continue
            clean_values: list[float] = []
            for raw in values:
                try:
                    clean_values.append(float(raw))
                except (TypeError, ValueError):
                    clean_values.append(float("nan"))
            clean_series[str(name)] = clean_values
        export_series_for_run: dict[str, list[float]] = {}
        for var_name in export_var_codes:
            level_values_full = clean_series.get(var_name)
            if not isinstance(level_values_full, list):
                continue
            level_map = _value_map(periods, level_values_full)
            level_values = [level_map.get(token, float("nan")) for token in period_tokens]
            transform = export_var_transforms.get(
                var_name,
                {"mode": TRANSFORM_LEVEL, "denominator": _default_denominator(available_vars)},
            )
            transform_mode = str(transform.get("mode", TRANSFORM_LEVEL))
            denominator_values: list[float] | None = None
            if transform_mode == TRANSFORM_PCT_OF:
                denominator_var = str(
                    transform.get("denominator", _default_denominator(available_vars))
                ).upper()
                denom_values_full = clean_series.get(denominator_var)
                if isinstance(denom_values_full, list):
                    denom_map = _value_map(periods, denom_values_full)
                    denominator_values = [denom_map.get(token, float("nan")) for token in period_tokens]
            export_series_for_run[var_name] = _transformed_export_values(
                mode=transform_mode,
                level_values=level_values,
                denominator_values=denominator_values,
            )
        base_export_by_run[run_key] = export_series_for_run

    panel_runs: list[PanelExportRun] = []
    for run in selected_runs:
        run_key = run_dir_key(run.run_dir)
        run_export_base = base_export_by_run.get(run_key, {})
        if not run_export_base:
            continue
        compared_series: dict[str, list[float]] = {}
        for var_name in export_var_codes:
            base_values = run_export_base.get(var_name)
            if not isinstance(base_values, list):
                continue
            compare_cfg = export_var_run_comparisons.get(
                var_name,
                {"mode": _COMPARE_NONE, "reference_run_key": ""},
            )
            compare_mode = str(compare_cfg.get("mode", _COMPARE_NONE))
            reference_run_key = str(compare_cfg.get("reference_run_key", ""))
            reference_values = base_export_by_run.get(reference_run_key, {}).get(var_name)
            values = _apply_run_comparison(
                mode=compare_mode,
                values=base_values,
                reference_values=reference_values if isinstance(reference_values, list) else None,
            )
            if compare_mode != _COMPARE_NONE and run_key == reference_run_key:
                values = [float("nan")] * len(values)
            compared_series[var_name] = values
        panel_runs.append(
            PanelExportRun(
                legend_label=_trace_short_label(run),
                periods=list(period_tokens),
                series=compared_series,
            )
        )

    if not export_vars:
        st.info("Select at least one export variable.")
    elif len(export_vars) > 6:
        st.warning("Panel export supports up to 6 variables.")
    elif not panel_runs:
        st.warning("No run data available for panel export.")
    else:
        title_by_var: dict[str, str] = {}
        units_by_var: dict[str, str] = {}
        for token in export_vars:
            var_name = str(token).upper()
            transform = export_var_transforms.get(
                var_name,
                {"mode": TRANSFORM_LEVEL, "denominator": _default_denominator(available_vars)},
            )
            compare_cfg = export_var_run_comparisons.get(
                var_name,
                {"mode": _COMPARE_NONE, "reference_run_key": ""},
            )
            title = _infer_short_name(var_name, dictionary)
            units = _infer_units(var_name, dictionary)
            transform_mode = str(transform.get("mode", TRANSFORM_LEVEL))
            if transform_mode == TRANSFORM_PCT_OF:
                denominator_var = str(
                    transform.get("denominator", _default_denominator(available_vars))
                ).upper()
                title = f"{title} (% of {denominator_var})"
                units = f"% of {denominator_var}"
            elif transform_mode == TRANSFORM_LVL_CHANGE:
                title = f"{title} (Lvl change)"
            elif transform_mode == TRANSFORM_PCT_CHANGE:
                title = f"{title} (% change)"
                units = "%"
            compare_mode = str(compare_cfg.get("mode", _COMPARE_NONE))
            reference_run_key = str(compare_cfg.get("reference_run_key", ""))
            reference_run_label = run_short_label_by_key.get(reference_run_key, reference_run_key or "reference")
            if compare_mode == _COMPARE_DIFF_VS_RUN:
                title = f"{title} (Diff vs {reference_run_label})"
            elif compare_mode == _COMPARE_PCT_DIFF_VS_RUN:
                title = f"{title} (% diff vs {reference_run_label})"
                units = "%"
            title_by_var[var_name] = title
            units_by_var[var_name] = units
        panel_png_bytes = build_multi_panel_png(
            runs=panel_runs,
            variables=export_var_codes,
            period_tokens=period_tokens,
            x_labels=x_labels,
            title_by_var=title_by_var,
            units_by_var=units_by_var,
            figure_title=export_title or "Run Panels Export",
            dpi=int(export_dpi),
            forecast_start_label=vline_x if mark_forecast_start else None,
        )
        st.download_button(
            "Download combined PNG",
            data=panel_png_bytes,
            file_name="run_panels.png",
            mime="image/png",
            key="mini_dash_panel_export_download",
        )

    st.subheader("Preset Manager")
    st.caption(f"Saved at `{(artifacts_dir / '.dashboard' / 'mini_dash_presets.json')}`.")
    tab_delete, tab_reset = st.tabs(["Delete", "Reset"])

    with tab_delete:
        if not preset_names:
            st.info("No presets available to delete.")
        else:
            delete_target = st.selectbox(
                "Preset to delete",
                options=preset_names,
                key="mini_dash_delete_target",
            )
            confirm_delete = st.checkbox(
                f"Confirm delete '{delete_target}'",
                value=False,
                key="mini_dash_delete_confirm",
            )
            if st.button(
                "Delete preset",
                key="mini_dash_delete_btn",
                disabled=not confirm_delete,
            ):
                try:
                    updated = delete_mini_dash_preset(presets, name=delete_target)
                    save_mini_dash_presets(artifacts_dir, updated)
                    retained = [
                        name
                        for name in st.session_state.get("mini_dash_selected_presets", [])
                        if name != delete_target
                    ]
                    _queue_selected_presets_update(retained)
                    st.success(f"Deleted preset: {delete_target}")
                    st.rerun()
                except ValueError as exc:
                    st.error(str(exc))

    with tab_reset:
        confirm_reset = st.checkbox(
            "Confirm reset all presets to defaults",
            value=False,
            key="mini_dash_reset_presets_confirm",
        )
        if st.button(
            "Reset presets",
            key="mini_dash_reset_presets_btn",
            disabled=not confirm_reset,
        ):
            reset = reset_mini_dash_presets(artifacts_dir)
            _queue_selected_presets_update(default_selected_preset_names([item["name"] for item in reset]))
            st.success("Presets reset to defaults.")
            st.rerun()


if __name__ == "__main__":
    main()
