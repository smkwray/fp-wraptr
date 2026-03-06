"""Explore FP equation dependencies from base/scenario input files."""

from __future__ import annotations

from pathlib import Path
from typing import Any, NamedTuple

import streamlit as st

from fp_wraptr.analysis.graph import (
    build_dependency_graph,
    get_downstream,
    get_upstream,
    summarize_graph,
)
from fp_wraptr.dashboard import _common as common
from fp_wraptr.dashboard._common import artifacts_dir_from_query, page_favicon
from fp_wraptr.dashboard.artifacts import RunArtifact, scan_artifacts
from fp_wraptr.io.input_parser import parse_fp_input, parse_fp_input_text


class InputSource(NamedTuple):
    label: str
    path: Path


def _parse_uploaded_input(file_obj) -> dict[str, Any] | None:
    if file_obj is None:
        return None
    text = file_obj.getvalue().decode("utf-8", errors="replace")
    return parse_fp_input_text(text)


def _parse_path_input(raw_path: str) -> dict[str, Any] | None:
    if not raw_path:
        return None
    path = Path(raw_path)
    if not path.exists():
        st.error(f"Path not found: {path}")
        return None
    return parse_fp_input(path)


def _run_fminput_path(run: RunArtifact) -> Path | None:
    candidates = (
        run.run_dir / "work" / "fminput.txt",
        run.run_dir / "bundle" / "fminput.txt",
        run.run_dir / "work_fpexe" / "fminput.txt",
        run.run_dir / "work_fppy" / "fppy_fminput.txt",
        run.run_dir / "fminput.txt",
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _latest_runs_with_inputs(
    runs: list[RunArtifact], *, limit: int = 8
) -> list[tuple[RunArtifact, Path]]:
    latest: list[tuple[RunArtifact, Path]] = []
    seen_names: set[str] = set()
    ordered = sorted(
        runs,
        key=lambda run: run.timestamp if run.timestamp else "00000000_000000",
        reverse=True,
    )
    for run in ordered:
        if run.scenario_name in seen_names:
            continue
        run_input = _run_fminput_path(run)
        if run_input is None:
            continue
        latest.append((run, run_input))
        seen_names.add(run.scenario_name)
        if len(latest) >= max(1, int(limit)):
            break
    return latest


def _discover_input_sources(artifacts_dir: Path) -> list[InputSource]:
    sources: list[InputSource] = [InputSource(label="FM baseline (FM/fminput.txt)", path=Path("FM/fminput.txt"))]
    runs = scan_artifacts(artifacts_dir)
    for run, run_input in _latest_runs_with_inputs(runs, limit=8):
        timestamp = f" ({run.timestamp})" if run.timestamp else ""
        sources.append(
            InputSource(
                label=f"{run.scenario_name}{timestamp}",
                path=run_input,
            )
        )
    return sources


def _canonical_expr(value: str) -> str:
    return " ".join(str(value or "").upper().split())


def _collect_definitions(parsed_input: dict[str, Any]) -> dict[str, dict[str, str]]:
    equations: dict[str, str] = {}
    identities: dict[str, str] = {}
    generated_vars: dict[str, str] = {}

    for equation in parsed_input.get("equations", []):
        lhs = str(equation.get("lhs", "")).strip().upper()
        rhs = str(equation.get("rhs", "")).strip()
        if lhs:
            equations[lhs] = rhs

    for identity in parsed_input.get("identities", []):
        name = str(identity.get("name", "")).strip().upper()
        expr = str(identity.get("expression", "")).strip()
        if name:
            identities[name] = expr

    for generated in parsed_input.get("generated_vars", []):
        name = str(generated.get("name", "")).strip().upper()
        expr = str(generated.get("expression", "")).strip()
        if name:
            generated_vars[name] = expr

    return {
        "equations": equations,
        "identities": identities,
        "generated_vars": generated_vars,
    }


def _diff_definition_maps(left: dict[str, str], right: dict[str, str]) -> dict[str, Any]:
    left_keys = set(left)
    right_keys = set(right)
    added = sorted(right_keys - left_keys)
    removed = sorted(left_keys - right_keys)
    changed = sorted(
        key
        for key in (left_keys & right_keys)
        if _canonical_expr(left.get(key, "")) != _canonical_expr(right.get(key, ""))
    )
    changed_rows = [
        {
            "variable": key,
            "left": left.get(key, ""),
            "right": right.get(key, ""),
        }
        for key in changed
    ]
    return {
        "added": added,
        "removed": removed,
        "changed": changed_rows,
    }


def _compare_parsed_inputs(left_input: dict[str, Any], right_input: dict[str, Any]) -> dict[str, dict[str, Any]]:
    left_defs = _collect_definitions(left_input)
    right_defs = _collect_definitions(right_input)
    return {
        "equations": _diff_definition_maps(left_defs["equations"], right_defs["equations"]),
        "identities": _diff_definition_maps(left_defs["identities"], right_defs["identities"]),
        "generated_vars": _diff_definition_maps(
            left_defs["generated_vars"], right_defs["generated_vars"]
        ),
    }


def _graph_diff(left_graph, right_graph) -> dict[str, Any]:
    left_nodes = set(left_graph.nodes())
    right_nodes = set(right_graph.nodes())
    left_edges = set(left_graph.edges())
    right_edges = set(right_graph.edges())
    return {
        "added_nodes": sorted(right_nodes - left_nodes),
        "removed_nodes": sorted(left_nodes - right_nodes),
        "added_edges": sorted(right_edges - left_edges),
        "removed_edges": sorted(left_edges - right_edges),
    }


def _render_compact_list(items: list[str], *, max_items: int = 25) -> str:
    if not items:
        return "<none>"
    shown = items[:max_items]
    suffix = "" if len(items) <= max_items else f" ... (+{len(items) - max_items} more)"
    return ", ".join(shown) + suffix


def _pick_source(
    *,
    label: str,
    sources: list[InputSource],
    key_prefix: str,
    allow_upload: bool,
) -> tuple[dict[str, Any] | None, str]:
    source_labels = [source.label for source in sources]
    options = [*source_labels, "Custom path"]
    if allow_upload:
        options.append("Upload file")

    choice = st.sidebar.selectbox(label, options=options, key=f"{key_prefix}_choice")
    source_by_label = {source.label: source for source in sources}

    if choice == "Custom path":
        custom_path = st.sidebar.text_input(
            f"{label} path",
            value="FM/fminput.txt",
            key=f"{key_prefix}_custom_path",
        )
        return _parse_path_input(custom_path), custom_path

    if choice == "Upload file" and allow_upload:
        uploaded = st.sidebar.file_uploader(
            f"{label} upload",
            type=["txt"],
            key=f"{key_prefix}_upload",
        )
        if uploaded is None:
            return None, "uploaded file"
        return _parse_uploaded_input(uploaded), uploaded.name

    selected_source = source_by_label[choice]
    return _parse_path_input(str(selected_source.path)), str(selected_source.path)


def _render_definition_diff_section(title: str, payload: dict[str, Any]) -> None:
    st.markdown(f"**{title}**")
    c1, c2, c3 = st.columns(3)
    c1.metric("Added", len(payload["added"]))
    c2.metric("Removed", len(payload["removed"]))
    c3.metric("Changed", len(payload["changed"]))
    st.caption(f"Added keys: {_render_compact_list(payload['added'])}")
    st.caption(f"Removed keys: {_render_compact_list(payload['removed'])}")
    if payload["changed"]:
        st.dataframe(payload["changed"], use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title="fp-wraptr Equation Graph", page_icon=page_favicon(), layout="wide")
    common.render_sidebar_logo_toggle(width=56, height=56)
    common.render_page_title(
        "Equation Dependency Graph",
        caption="Load fminput from baseline or scenario runs, inspect dependencies, and compare sources.",
    )

    default_artifacts = Path(st.session_state.get("artifacts_dir", artifacts_dir_from_query()))
    artifacts_dir_text = st.sidebar.text_input(
        "Artifacts directory",
        value=str(default_artifacts),
        help="Used to discover recent scenario run inputs.",
    )
    artifacts_dir = Path(artifacts_dir_text).expanduser()
    st.session_state["artifacts_dir"] = str(artifacts_dir)

    sources = _discover_input_sources(artifacts_dir)
    parsed_input, source_name = _pick_source(
        label="Primary source",
        sources=sources,
        key_prefix="eq_graph_primary",
        allow_upload=True,
    )

    if parsed_input is None:
        st.info("Pick a primary source (or upload) to build the dependency graph.")
        return

    try:
        dependency_graph = build_dependency_graph(parsed_input)
    except ModuleNotFoundError:
        st.error("NetworkX is required for dependency graphs. Install fp-wraptr[graph].")
        return

    summary = summarize_graph(dependency_graph)
    st.success(f"Loaded dependencies from {source_name}")

    st.subheader("Summary")
    c1, c2, c3 = st.columns(3)
    c1.metric("Nodes", summary["nodes"])
    c2.metric("Edges", summary["edges"])
    c3.metric("Most connected", ", ".join(summary["most_connected"]) or "<none>")

    st.write(f"Roots: {', '.join(summary['roots']) or '<none>'}")
    st.write(f"Leaves: {', '.join(summary['leaves']) or '<none>'}")

    compare_enabled = st.sidebar.checkbox(
        "Compare with second source",
        value=False,
        key="eq_graph_compare_enabled",
    )
    if compare_enabled:
        comparison_input, comparison_name = _pick_source(
            label="Comparison source",
            sources=sources,
            key_prefix="eq_graph_comparison",
            allow_upload=False,
        )
        if comparison_input is not None:
            try:
                comparison_graph = build_dependency_graph(comparison_input)
            except ModuleNotFoundError:
                st.error("NetworkX is required for dependency graphs. Install fp-wraptr[graph].")
                return

            st.subheader("Source Differences")
            st.caption(f"Comparing `{source_name}` → `{comparison_name}`")

            definition_diff = _compare_parsed_inputs(parsed_input, comparison_input)
            graph_delta = _graph_diff(dependency_graph, comparison_graph)

            summary_cols = st.columns(4)
            summary_cols[0].metric("Eq changed", len(definition_diff["equations"]["changed"]))
            summary_cols[1].metric("Ident changed", len(definition_diff["identities"]["changed"]))
            summary_cols[2].metric("GENR changed", len(definition_diff["generated_vars"]["changed"]))
            summary_cols[3].metric(
                "Edge delta",
                len(graph_delta["added_edges"]) + len(graph_delta["removed_edges"]),
            )

            with st.expander("Behavioral equation differences", expanded=True):
                _render_definition_diff_section("EQ (LHS)", definition_diff["equations"])
            with st.expander("Identity differences", expanded=False):
                _render_definition_diff_section("IDENT", definition_diff["identities"])
            with st.expander("Generated-variable differences", expanded=False):
                _render_definition_diff_section("GENR", definition_diff["generated_vars"])
            with st.expander("Dependency graph differences", expanded=False):
                g1, g2 = st.columns(2)
                g1.metric("Nodes added", len(graph_delta["added_nodes"]))
                g2.metric("Nodes removed", len(graph_delta["removed_nodes"]))
                g3, g4 = st.columns(2)
                g3.metric("Edges added", len(graph_delta["added_edges"]))
                g4.metric("Edges removed", len(graph_delta["removed_edges"]))
                st.caption(f"Added nodes: {_render_compact_list(graph_delta['added_nodes'])}")
                st.caption(f"Removed nodes: {_render_compact_list(graph_delta['removed_nodes'])}")

    nodes = sorted(dependency_graph.nodes())
    if nodes:
        selected = st.selectbox("Inspect variable", options=nodes)
        upstream = get_upstream(dependency_graph, selected)
        downstream = get_downstream(dependency_graph, selected)
        col1, col2 = st.columns(2)
        col1.metric("Upstream count", len(upstream))
        col2.metric("Downstream count", len(downstream))

        with st.expander("Dependency lists"):
            st.markdown(f"**{selected} upstream:** {' '.join(sorted(upstream)) or '<none>'}")
            st.markdown(f"**{selected} downstream:** {' '.join(sorted(downstream)) or '<none>'}")

    st.subheader("Adjacency list")
    for node in nodes:
        outgoing = sorted(dependency_graph.successors(node))
        st.text(f"{node}: {', '.join(outgoing) if outgoing else '<none>'}")


if __name__ == "__main__":
    main()
