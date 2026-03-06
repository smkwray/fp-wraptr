"""Run-level historical fit analysis for estimated equations."""

from __future__ import annotations

import importlib
from pathlib import Path

import pandas as pd
import streamlit as st

from fp_wraptr.analysis.historical_fit import FitReport, build_fit_report
from fp_wraptr.dashboard import _common as common
from fp_wraptr.dashboard._common import artifacts_dir_from_query, page_favicon
from fp_wraptr.dashboard.artifacts import RunArtifact, backend_name, scan_artifacts


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
    return artifact.load_output()


def _cache_key_for_run(run_dir: Path) -> str:
    output_path = run_dir / "fmout.txt"
    scenario_path = run_dir / "scenario.yaml"

    key_parts: list[str] = []
    for path in (output_path, scenario_path):
        if path.exists():
            stat = path.stat()
            key_parts.append(f"{path.name}:{stat.st_size}:{stat.st_mtime_ns}")
        else:
            key_parts.append(f"{path.name}:missing")
    return "|".join(key_parts)


def _style_fit_table(row: pd.Series) -> list[str]:
    styles = ["" for _ in range(len(row))]
    if row["R²"] < 0.8:
        styles[row.index.get_loc("R²")] = "background-color: #fecaca"
    if row["DW"] < 1.5 or row["DW"] > 2.5:
        styles[row.index.get_loc("DW")] = "background-color: #fef08a"
    return styles


def _fit_table(report: FitReport) -> pd.DataFrame:
    return pd.DataFrame([
        {
            "Eq #": eq.equation_number,
            "Dependent Var": eq.dependent_var,
            "R²": round(eq.r_squared, 4),
            "SE": round(eq.se_equation, 4),
            "DW": round(eq.durbin_watson, 4),
            "N Obs": eq.n_obs,
            "% Significant Coefficients": round(eq.pct_significant, 1),
        }
        for eq in report.equations
    ])


def _matplotlib_r2_chart(report: FitReport):
    if importlib.util.find_spec("matplotlib") is None:
        return None

    from matplotlib import pyplot as plt

    if not report.equations:
        return None

    fig, ax = plt.subplots()
    ax.bar(
        [str(eq.equation_number) for eq in report.equations],
        [eq.r_squared for eq in report.equations],
    )
    ax.set_title("R² by Equation")
    ax.set_xlabel("Equation")
    ax.set_ylabel("R²")
    return fig


def main() -> None:
    st.set_page_config(page_title="fp-wraptr Historical Fit", page_icon=page_favicon(), layout="wide")
    common.render_sidebar_logo_toggle(width=56, height=56)
    common.render_page_title("Historical Fit Analysis", caption="Compare model forecasts against actual historical data.")

    artifact_dir = artifacts_dir_from_query()
    st.session_state["runs"] = scan_artifacts(artifact_dir)
    runs = [run for run in st.session_state.get("runs", []) if run.has_output]

    if not runs:
        st.info("No completed runs with output found.")
        return

    run = st.sidebar.selectbox(
        "Run",
        options=runs,
        format_func=lambda item: f"{item.display_name} [{backend_name(item)}]",
    )
    try:
        data = _load_output(str(run.run_dir), _cache_key_for_run(run.run_dir))
    except (FileNotFoundError, ValueError, KeyError) as exc:
        st.error(f"Failed to load run output: {exc}")
        return
    if data is None or not data.estimations:
        st.warning("Selected run has no estimated equations.")
        return

    report = build_fit_report(data)

    st.subheader(f"{run.scenario_name} ({run.timestamp})")
    st.write(f"Timestamp: {run.timestamp}")
    if run.config:
        st.write(f"Description: {run.config.description or '<none>'}")

    c1, c2, c3 = st.columns(3)
    c1.metric("Equations", report.n_equations)
    c2.metric("Avg R²", f"{report.avg_r_squared:.4f}")
    c3.metric("Avg DW", f"{report.avg_durbin_watson:.4f}")

    c4, c5 = st.columns(2)
    c4.metric("Weak Equations", len(report.weakest_equations))
    c5.metric("DW Flagged", len(report.dw_flagged))

    table = _fit_table(report)
    if table.empty:
        st.info("No equations to display.")
        return

    st.subheader("Equation Fit Metrics")
    st.dataframe(
        table.style.apply(_style_fit_table, axis=1),
        column_config={
            "Eq #": st.column_config.NumberColumn("Eq #", format="%d"),
            "R²": st.column_config.NumberColumn("R²", format="%.4f"),
            "SE": st.column_config.NumberColumn("SE", format="%.4f"),
            "DW": st.column_config.NumberColumn("DW", format="%.4f"),
            "N Obs": st.column_config.NumberColumn("N Obs", format="%d"),
            "% Significant Coefficients": st.column_config.NumberColumn(
                "% Significant Coefficients",
                format="%.1f",
            ),
        },
        hide_index=True,
        use_container_width=True,
    )

    if importlib.util.find_spec("matplotlib") is not None:
        fig = _matplotlib_r2_chart(report)
        if fig is not None:
            st.subheader("R² Distribution")
            st.pyplot(fig)


if __name__ == "__main__":
    main()
