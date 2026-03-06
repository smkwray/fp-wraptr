"""Compare two completed fp-wraptr runs."""

from __future__ import annotations

from pathlib import Path
from tempfile import NamedTemporaryFile

import pandas as pd
import streamlit as st

from fp_wraptr.analysis.diff import diff_outputs, export_diff_csv
from fp_wraptr.dashboard import _common as common
from fp_wraptr.dashboard._common import artifacts_dir_from_query, cache_key_for_path, page_favicon
from fp_wraptr.dashboard.agent_handoff import render_agent_handoff
from fp_wraptr.dashboard.artifacts import (
    RunArtifact,
    backend_name,
    filter_runs_by_keys,
    scan_artifacts,
)


@st.cache_data(ttl=60)
def _load_output(run_dir: str, cache_key: str):
    _ = cache_key  # included to invalidate cache when output files change
    artifact = RunArtifact(
        run_dir=Path(run_dir),
        scenario_name="",
        timestamp="",
        has_output=True,
        has_chart=False,
        config=None,
    )
    return artifact.load_series_output()


def _cache_key_for_run(run_dir: Path) -> str:
    key_parts: list[str] = []
    for path in (run_dir / "fmout.txt", run_dir / "LOADFORMAT.DAT", run_dir / "PABEV.TXT", run_dir / "scenario.yaml"):
        key_parts.append(f"{path.name}:{cache_key_for_path(path)}")
    return "|".join(key_parts)


def main() -> None:
    st.set_page_config(page_title="fp-wraptr Compare Runs", page_icon=page_favicon(), layout="wide")
    common.render_sidebar_logo_toggle(width=56, height=56)
    common.render_page_title("Compare Runs", caption="Side-by-side comparison of two scenario runs with delta analysis.")

    all_runs = [run for run in st.session_state.get("runs", []) if getattr(run, "has_output", False)]
    if len(all_runs) < 2:
        all_runs = [run for run in scan_artifacts(artifacts_dir_from_query()) if run.has_output]
        st.session_state["runs"] = all_runs
    active_keys = set(st.session_state.get("dashboard_active_run_dirs", []))
    runs = filter_runs_by_keys(all_runs, active_keys)
    if len(runs) < 2:
        runs = list(all_runs)
    if len(runs) < 2:
        st.warning("Need at least two runs with output to compare.")
        return

    baseline = st.sidebar.selectbox(
        "Baseline run",
        options=runs,
        format_func=lambda item: f"{item.display_name} [{backend_name(item)}]",
        index=0,
    )
    scenario = st.sidebar.selectbox(
        "Scenario run",
        options=runs,
        format_func=lambda item: f"{item.display_name} [{backend_name(item)}]",
        index=min(1, len(runs) - 1),
    )

    if baseline.run_dir == scenario.run_dir:
        st.warning("Select two different runs to compare.")
        return

    try:
        baseline_data = _load_output(str(baseline.run_dir), _cache_key_for_run(baseline.run_dir))
        scenario_data = _load_output(str(scenario.run_dir), _cache_key_for_run(scenario.run_dir))
    except (FileNotFoundError, ValueError, KeyError) as exc:
        st.error(f"Failed to load one or both run outputs: {exc}")
        return

    if not baseline_data or not scenario_data:
        st.error("Unable to load one or both run outputs.")
        return

    top_n = st.sidebar.slider("Top N deltas", min_value=5, max_value=50, value=15)

    common = sorted(set(baseline_data.variables).intersection(scenario_data.variables))
    selected = st.sidebar.multiselect(
        "Variables",
        options=common,
        default=common[: min(5, len(common))],
    )
    if not common:
        st.info("No common variables to compare across the selected runs.")
        return

    full_diff_result = diff_outputs(baseline_data, scenario_data, top_n=max(len(common), 1))
    full_deltas = full_diff_result.get("deltas", {})
    if not full_deltas:
        st.info("No common variables to compare.")
        return

    diff_result = dict(full_diff_result)
    diff_result["deltas"] = dict(list(full_deltas.items())[:top_n])
    deltas = diff_result.get("deltas", {})

    if not deltas:
        st.info("No common variables to compare.")
        return

    biggest = max(full_deltas.items(), key=lambda item: abs(item[1].get("abs_delta", 0.0)))
    biggest_name, biggest_value = biggest
    biggest_abs_delta = biggest_value.get("abs_delta", 0.0)
    biggest_pct = biggest_value.get("pct_delta")
    biggest_pct_text = (
        f"{float(biggest_pct):.2f}%" if isinstance(biggest_pct, (int, float)) else "N/A"
    )

    st.markdown(
        f"Comparing {baseline.scenario_name} vs {scenario.scenario_name} | "
        f"{len(common)} common variables | Largest mover: "
        f"{biggest_name} ({biggest_pct_text})"
    )
    render_agent_handoff(
        title="Agent Handoff",
        run_dir=scenario.run_dir,
        prompt=(
            f"Compare `{baseline.run_dir.name}` against `{scenario.run_dir.name}`, summarize the largest movers, "
            "and prepare a visualization view for the tracked variables or top deltas."
        ),
    )

    csv_text = _make_diff_csv(full_diff_result)

    col1, col2 = st.columns(2)
    col1.metric("Variables compared", diff_result.get("total_compared", len(full_deltas)))
    col2.metric("Largest mover", biggest_name, f"{biggest_abs_delta:.4f}")

    st.download_button(
        "Download Diff CSV",
        data=csv_text,
        file_name=f"{baseline.scenario_name}_vs_{scenario.scenario_name}_diff.csv",
        mime="text/csv",
        key="download_diff_csv",
    )

    try:
        from fp_wraptr.dashboard.charts import comparison_figure, delta_bar_chart
    except ModuleNotFoundError:
        st.warning("Plotly is not installed. Install fp-wraptr[dashboard] to view charts.")
    else:
        st.plotly_chart(
            delta_bar_chart(diff_result, top_n=top_n),
            use_container_width=True,
        )
        st.plotly_chart(
            comparison_figure(baseline_data, scenario_data, variables=selected),
            use_container_width=True,
        )

    with st.expander("Difference table"):
        rows = []
        for var_name, values in deltas.items():
            row = {"Variable": var_name}
            row.update(values)
            rows.append(row)
        st.dataframe(pd.DataFrame(rows), use_container_width=True)


def _make_diff_csv(diff_result: dict) -> str:
    """Create a CSV payload using existing export logic."""
    with NamedTemporaryFile("w", suffix=".csv", delete=False) as temp:
        temp_path = Path(temp.name)
    export_diff_csv(diff_result, temp_path)
    payload = temp_path.read_text(encoding="utf-8")
    temp_path.unlink(missing_ok=True)
    return payload


if __name__ == "__main__":
    main()
