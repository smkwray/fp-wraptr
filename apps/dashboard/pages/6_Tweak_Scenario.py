"""Tweak a completed run's overrides and re-run to compare."""

from __future__ import annotations

import os
from pathlib import Path

import streamlit as st

from fp_wraptr.analysis.diff import diff_outputs
from fp_wraptr.dashboard import _common as common
from fp_wraptr.dashboard._common import artifacts_dir_from_query, page_favicon
from fp_wraptr.dashboard.artifacts import RunArtifact, backend_name, scan_artifacts
from fp_wraptr.dashboard.charts import comparison_figure, delta_bar_chart, forecast_figure
from fp_wraptr.dashboard.scenario_tools import build_tweaked_config
from fp_wraptr.scenarios.config import VariableOverride
from fp_wraptr.scenarios.runner import run_scenario


def _default_thread_count() -> int:
    cores = os.cpu_count()
    if isinstance(cores, int) and cores > 0:
        return max(1, cores // 2)
    return 4


def main() -> None:
    st.set_page_config(page_title="fp-wraptr Tweak Scenario", page_icon=page_favicon(), layout="wide")
    common.render_sidebar_logo_toggle(width=56, height=56)
    common.render_page_title(
        "Tweak Scenario",
        caption="Pick a completed run, adjust overrides, re-run, and compare results.",
    )

    runs = [run for run in st.session_state.get("runs", []) if getattr(run, "has_output", False)]
    if not runs:
        st.info("No completed runs available. Run a scenario first.")
        return

    base_run: RunArtifact = st.sidebar.selectbox(
        "Starting run",
        options=runs,
        format_func=lambda item: f"{item.display_name} [{backend_name(item)}]",
    )

    base_config = base_run.config
    if base_config is None:
        st.warning("Selected run has no scenario config. Cannot tweak.")
        return

    # Load baseline output
    try:
        base_output = base_run.load_output()
    except (FileNotFoundError, ValueError, KeyError) as exc:
        st.error(f"Failed to load baseline output: {exc}")
        return

    if not base_output or not base_output.variables:
        st.warning("Selected run has no parsed output data.")
        return

    st.sidebar.markdown("---")
    st.sidebar.markdown("**Scenario settings**")
    new_name = st.sidebar.text_input("New scenario name", value=f"{base_config.name}_tweaked")
    backend_choice = st.sidebar.selectbox(
        "Backend",
        options=["fpexe", "fppy", "both"],
        index=["fpexe", "fppy", "both"].index(
            str(getattr(base_config, "backend", "fpexe") or "fpexe").strip().lower()
            if str(getattr(base_config, "backend", "fpexe") or "fpexe").strip().lower()
            in {"fpexe", "fppy", "both"}
            else "fpexe"
        ),
        help="Choose execution engine for the tweaked run.",
    )
    fppy_num_threads = int(
        st.sidebar.number_input(
            "fppy threads",
            min_value=1,
            max_value=256,
            value=_default_thread_count(),
            step=1,
            help=(
                "Sets OMP/BLAS thread env vars for fppy runs "
                "(also used by backend=both parity path)."
            ),
        )
    )

    methods = ["CHGSAMEPCT", "SAMEVALUE", "CHGSAMEABS"]

    # Display current overrides and let user adjust
    st.subheader("Overrides")
    st.markdown(
        f"Starting from **{base_config.name}** "
        f"({len(base_config.overrides)} override{'s' if len(base_config.overrides) != 1 else ''})."
    )
    st.caption("Nothing is applied until you click **Run Tweaked Scenario** or **Save as YAML**.")

    # Initialize override state from base config
    existing_overrides = list(base_config.overrides.items())
    num_rows = st.number_input(
        "Number of overrides",
        min_value=0,
        max_value=20,
        value=len(existing_overrides),
        step=1,
        key="tweak_num_overrides",
    )

    override_entries: list[tuple[str, str, float]] = []
    for idx in range(int(num_rows)):
        c1, c2, c3 = st.columns([2, 2, 1])
        # Pre-fill from existing overrides
        default_var = ""
        default_method = "CHGSAMEPCT"
        default_value = 0.0
        if idx < len(existing_overrides):
            default_var = existing_overrides[idx][0]
            default_method = existing_overrides[idx][1].method
            default_value = float(existing_overrides[idx][1].value)

        var_name = c1.text_input(
            "Variable",
            value=default_var,
            key=f"tweak_var_{idx}",
        )
        method = c2.selectbox(
            "Method",
            options=methods,
            index=methods.index(default_method) if default_method in methods else 0,
            key=f"tweak_method_{idx}",
        )
        value = c3.number_input(
            "Value",
            value=default_value,
            format="%.6f",
            key=f"tweak_value_{idx}",
        )
        if var_name.strip():
            override_entries.append((var_name.strip(), method, value))

    # Build the tweaked config
    new_overrides = {
        name: VariableOverride(method=method, value=value)
        for name, method, value in override_entries
    }

    # Show what changed
    added = set(new_overrides.keys()) - set(base_config.overrides.keys())
    removed = set(base_config.overrides.keys()) - set(new_overrides.keys())
    changed = {
        k
        for k in set(new_overrides.keys()) & set(base_config.overrides.keys())
        if new_overrides[k] != base_config.overrides[k]
    }

    if added or removed or changed:
        changes = []
        for v in sorted(added):
            changes.append(f"+ **{v}**: {new_overrides[v].method} = {new_overrides[v].value}")
        for v in sorted(removed):
            changes.append(f"- ~~{v}~~")
        for v in sorted(changed):
            old = base_config.overrides[v]
            new = new_overrides[v]
            changes.append(f"~ **{v}**: {old.method}={old.value} -> {new.method}={new.value}")
        st.markdown("**Changes from baseline:**\n" + "\n".join(f"- {c}" for c in changes))

    artifacts_dir = artifacts_dir_from_query()

    col1, col2 = st.columns(2)
    run_btn = col1.button("Run Tweaked Scenario")
    save_btn = col2.button("Save as YAML")

    if save_btn:
        tweaked_config = build_tweaked_config(
            base_config=base_config,
            new_name=new_name,
            description=f"Tweaked from {base_config.name}",
            backend=backend_choice,
            fppy_num_threads=int(fppy_num_threads),
            overrides=new_overrides,
        )
        yaml_text = tweaked_config.to_yaml(artifacts_dir / f"{new_name}.yaml")
        st.success(f"Saved scenario YAML to: {yaml_text}")

    if run_btn:
        tweaked_config = build_tweaked_config(
            base_config=base_config,
            new_name=new_name,
            description=f"Tweaked from {base_config.name}",
            backend=backend_choice,
            fppy_num_threads=int(fppy_num_threads),
            overrides=new_overrides,
        )

        with st.spinner("Running tweaked scenario..."):
            try:
                result = run_scenario(config=tweaked_config, output_dir=artifacts_dir)
            except Exception as exc:
                st.error(f"Run failed: {exc}")
                return

        st.success("Tweaked scenario complete.")

        if result.parsed_output and result.parsed_output.variables:
            st.subheader("Comparison: Baseline vs Tweaked")

            # Delta summary
            diff_result = diff_outputs(base_output, result.parsed_output, top_n=15)
            deltas = diff_result.get("deltas", {})
            if deltas:
                st.plotly_chart(
                    delta_bar_chart(diff_result, top_n=15),
                    use_container_width=True,
                )

            # Side-by-side chart
            track = base_config.track_variables or list(base_output.variables.keys())[:5]
            common = [
                v
                for v in track
                if v in base_output.variables and v in result.parsed_output.variables
            ]
            if common:
                st.plotly_chart(
                    comparison_figure(base_output, result.parsed_output, variables=common),
                    use_container_width=True,
                )

            st.subheader("Tweaked Run Output")
            st.plotly_chart(
                forecast_figure(
                    result.parsed_output,
                    variables=tweaked_config.track_variables,
                    title=f"Tweaked: {new_name}",
                ),
                use_container_width=True,
            )
        else:
            st.info("No output data from tweaked run (fp.exe may be unavailable).")

        # Refresh artifacts list
        st.session_state["runs"] = scan_artifacts(artifacts_dir)


if __name__ == "__main__":
    main()
