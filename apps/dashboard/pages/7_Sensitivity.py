"""Run sensitivity sweeps from completed runs."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from fp_wraptr.analysis.sensitivity import run_sensitivity
from fp_wraptr.dashboard import _common as common
from fp_wraptr.dashboard._common import page_favicon
from fp_wraptr.dashboard.artifacts import RunArtifact, backend_name
from fp_wraptr.dashboard.plotly_theme import apply_white_theme
from fp_wraptr.io.input_parser import (
    parse_fmexog,
    parse_fmexog_text,
    parse_fp_input_text,
)

_INPUT_FILE_RE = re.compile(r"\bFILE\s*=\s*(?P<name>[^\s;]+)", re.IGNORECASE)


@st.cache_data(ttl=300)
def _cached_run(
    scenario_path: str,
    artifacts_dir: str,
    sweep_variable: str,
    min_val: float,
    max_val: float,
    steps: int,
    method: str,
) -> dict[str, list[float | None] | list[float] | list[str]]:
    from fp_wraptr.scenarios.config import ScenarioConfig

    config = ScenarioConfig.from_yaml(scenario_path)
    if steps <= 1:
        sweep_values = [min_val]
    else:
        sweep_values = [min_val + i * (max_val - min_val) / (steps - 1) for i in range(steps)]

    result = run_sensitivity(
        config,
        sweep_variable,
        sweep_values,
        method=method,
        output_dir=Path(artifacts_dir) / ".dashboard" / "sensitivity",
        track_variables=config.track_variables,
    )

    return result.to_dict()


@st.cache_data(ttl=300)
def _available_sweep_variables(fp_home: str) -> list[str]:
    fmexog_path = Path(fp_home) / "fmexog.txt"
    if not fmexog_path.exists():
        return []
    try:
        payload = parse_fmexog(fmexog_path)
    except Exception:
        return []
    changes = payload.get("changes", [])
    variables: list[str] = []
    seen: set[str] = set()
    for item in changes:
        if not isinstance(item, dict):
            continue
        name = str(item.get("variable", "") or "").strip().upper()
        if not name or name in seen:
            continue
        seen.add(name)
        variables.append(name)
    return sorted(variables)


def _scenario_override_variables(config: object) -> list[str]:
    overrides = getattr(config, "overrides", {}) or {}
    if not isinstance(overrides, dict):
        return []
    variables: list[str] = []
    seen: set[str] = set()
    for raw_name in overrides.keys():
        name = str(raw_name or "").strip().upper()
        if not name or name in seen:
            continue
        seen.add(name)
        variables.append(name)
    return sorted(variables)


def _clean_filename(token: str) -> str:
    return str(token or "").strip().strip("\"'").rstrip(";")


def _resolve_case_insensitive(directory: Path, name: str) -> Path | None:
    candidate = directory / name
    if candidate.exists():
        return candidate
    want = name.lower()
    try:
        for child in directory.iterdir():
            if child.name.lower() == want:
                return child
    except OSError:
        return None
    return None


def _find_source_path(name: str, search_dirs: list[Path]) -> Path | None:
    for directory in search_dirs:
        resolved = _resolve_case_insensitive(directory, name)
        if resolved is not None:
            return resolved
    return None


def _input_tree_changevar_variables(
    fp_home: str,
    input_overlay_dir: str,
    input_file: str,
) -> list[str]:
    search_dirs: list[Path] = []
    overlay_raw = str(input_overlay_dir or "").strip()
    if overlay_raw:
        search_dirs.append(Path(overlay_raw))
    fp_home_raw = str(fp_home or "").strip()
    if fp_home_raw:
        search_dirs.append(Path(fp_home_raw))
    if not search_dirs:
        return []

    queue: list[str] = [str(input_file or "fminput.txt")]
    visited: set[str] = set()
    variables: set[str] = set()
    while queue:
        current = _clean_filename(queue.pop(0))
        if not current:
            continue
        key = current.lower()
        if key in visited:
            continue
        visited.add(key)

        source_path = _find_source_path(current, search_dirs)
        if source_path is None:
            continue

        try:
            text = source_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue

        try:
            parsed = parse_fp_input_text(text)
        except Exception:
            parsed = {}

        for block in (parsed.get("changevar_blocks", []) if isinstance(parsed, dict) else []):
            if not isinstance(block, list):
                continue
            for item in block:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("variable", "") or "").strip().upper()
                if name:
                    variables.add(name)

        # Also parse fmexog-style decks where `CHANGEVAR` blocks are not inline
        # command bodies (e.g. `CHANGEVAR;` followed by variable lines).
        if "CHANGEVAR" in text.upper():
            try:
                exog_payload = parse_fmexog_text(text)
            except Exception:
                exog_payload = {}
            for item in exog_payload.get("changes", []) if isinstance(exog_payload, dict) else []:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("variable", "") or "").strip().upper()
                if name:
                    variables.add(name)

        for command in (parsed.get("control_commands", []) if isinstance(parsed, dict) else []):
            if not isinstance(command, dict):
                continue
            if str(command.get("name", "")).upper() != "INPUT":
                continue
            body = str(command.get("body", "") or "")
            match = _INPUT_FILE_RE.search(body)
            if match is None:
                continue
            include_name = _clean_filename(match.group("name"))
            if include_name:
                queue.append(include_name)

    return sorted(variables)


def _input_tree_sweep_variables(config: object) -> list[str]:
    fp_home = str(getattr(config, "fp_home", "") or "").strip()
    input_overlay_dir = str(getattr(config, "input_overlay_dir", "") or "").strip()
    input_file = str(getattr(config, "input_file", "fminput.txt") or "fminput.txt")
    return _input_tree_changevar_variables(fp_home, input_overlay_dir, input_file)


def _sweep_variable_candidates(config: object) -> list[str]:
    fp_home = str(getattr(config, "fp_home", "") or "").strip()
    stock = _available_sweep_variables(fp_home) if fp_home else []
    scenario = _scenario_override_variables(config)
    input_tree = _input_tree_sweep_variables(config)
    merged = sorted({*stock, *scenario, *input_tree})
    return merged


def _chart_response(
    result: dict[str, list[float | None] | list[str] | list[float | None]],
    selected_vars: list[str],
) -> go.Figure:
    sweep_values = result.get("sweep_values", [])
    response_table = result.get("response_table", {})  # type: ignore[assignment]
    figure = go.Figure()

    for var_name in selected_vars:
        values = response_table.get(var_name, [])  # type: ignore[assignment]
        if not values:
            continue
        figure.add_trace(
            go.Scatter(
                x=sweep_values,
                y=values,
                mode="lines+markers",
                name=var_name,
            )
        )

    figure.update_layout(
        title="Sensitivity fan chart",
        xaxis_title="Sweep value",
        yaxis_title="Final-period level",
    )
    return apply_white_theme(figure)


def main() -> None:
    st.set_page_config(page_title="fp-wraptr Sensitivity", page_icon=page_favicon(), layout="wide")
    common.render_sidebar_logo_toggle(width=56, height=56)
    common.render_page_title(
        "Sensitivity Analysis",
        caption="Parametric sweep over one exogenous variable with tracked forecast responses.",
    )

    runs = [run for run in st.session_state.get("runs", []) if getattr(run, "has_output", False)]
    if not runs:
        st.info("No completed runs available. Run a scenario first.")
        return

    base_run: RunArtifact = st.sidebar.selectbox(
        "Base run",
        options=runs,
        format_func=lambda item: f"{item.display_name} [{backend_name(item)}]",
    )
    if base_run.config is None:
        st.warning("Selected run is missing a valid scenario config.")
        return

    st.sidebar.markdown("---")
    sweep_candidates = _sweep_variable_candidates(base_run.config)
    if not sweep_candidates:
        st.error(
            "No sweep variables available. Could not read exogenous variables from `fmexog.txt` "
            "or scenario overrides for the selected run."
        )
        return

    default_sweep = "YS" if "YS" in sweep_candidates else sweep_candidates[0]
    sweep_variable = st.sidebar.selectbox(
        "Sweep variable",
        options=sweep_candidates,
        index=sweep_candidates.index(default_sweep),
    )
    min_val = st.sidebar.number_input("Min value", value=0.0, step=0.001, format="%.6f")
    max_val = st.sidebar.number_input("Max value", value=0.03, step=0.001, format="%.6f")
    steps = st.sidebar.slider("Steps", min_value=3, max_value=11, value=5)
    method = st.sidebar.selectbox(
        "Override method",
        options=["CHGSAMEPCT", "SAMEVALUE", "CHGSAMEABS"],
        index=0,
    )

    if steps < 1:
        st.warning("Steps must be at least one.")
        return

    run_btn = st.button("Run Sensitivity")

    if not run_btn:
        return

    if max_val < min_val:
        st.error("Maximum value must be greater than minimum value.")
        return

    result = _cached_run(
        str(base_run.run_dir / "scenario.yaml"),
        str(st.session_state.get("artifacts_dir", "artifacts")),
        sweep_variable,
        float(min_val),
        float(max_val),
        int(steps),
        method,
    )

    sweep_values = result.get("sweep_values", [])
    response_table: dict[str, list[float | None]] = result.get("response_table", {})  # type: ignore[assignment]
    selected_variables = [
        name for name in base_run.config.track_variables if name in response_table
    ]
    if not selected_variables:
        selected_variables = list(response_table.keys())

    table_rows = {
        "sweep": sweep_values,
    }
    for name in selected_variables:
        values = response_table.get(name, [])
        table_rows[name] = values

    table = pd.DataFrame(table_rows)
    table = table.rename(columns={"sweep": "Sweep Value"})
    st.subheader("Response table")
    st.dataframe(table, use_container_width=True)

    chart_vars = [
        var
        for var in selected_variables
        if any(value is not None for value in response_table.get(var, []))
    ]
    if chart_vars:
        st.plotly_chart(_chart_response(result, chart_vars), use_container_width=True)
    else:
        st.info("No numeric outputs were available to plot.")


if __name__ == "__main__":
    main()
