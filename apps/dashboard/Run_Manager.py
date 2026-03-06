"""Streamlit dashboard run manager and launchpad for all pages."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import shutil
from pathlib import Path

import pandas as pd
import streamlit as st

from fp_wraptr.dashboard import _common as common
from fp_wraptr.dashboard.artifacts import (
    ParityRunArtifact,
    RunArtifact,
    backend_name,
    bundle_label,
    filter_runs_by_keys,
    latest_preferred_runs,
    latest_runs,
    parse_run_timestamp,
    run_dir_key,
    scan_artifacts,
    scan_parity_artifacts,
)
from fp_wraptr.dashboard.reset import soft_reset_dashboard_state
from fp_wraptr.scenarios.bundle import BundleConfig, run_bundle
from fp_wraptr.scenarios.catalog import CatalogEntry, load_scenario_catalog
from fp_wraptr.scenarios.runner import load_scenario_config, run_scenario

artifacts_dir_from_query = common.artifacts_dir_from_query
logo_html = common.logo_html
logo_path = common.logo_path
page_favicon = common.page_favicon
reset_requested_from_query = common.reset_requested_from_query

ACTIVE_RUN_DIRS_KEY = "dashboard_active_run_dirs"
BACKEND_FILTER_KEY = "dashboard_backend_filter"
BUNDLE_FILTER_KEY = "dashboard_bundle_filter"
SCENARIO_FILTER_KEY = "dashboard_scenario_filter"
RESET_MENU_URL = "http://localhost:8501/?artifacts-dir=artifacts&reset=1"
_RESET_KEYS = (
    "artifacts_dir",
    "runs",
    ACTIVE_RUN_DIRS_KEY,
    BACKEND_FILTER_KEY,
    BUNDLE_FILTER_KEY,
    SCENARIO_FILTER_KEY,
    "mini_dash_run_keys",
    "mini_dash_data_source",
    "mini_dash_vars",
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _sidebar_logo_markup() -> str:
    toggle = getattr(common, "sidebar_logo_toggle_html", None)
    if callable(toggle):
        return toggle(width=56, height=56)
    return logo_html("sidebar-raptr.png", 56, "Raptr")


def _render_sidebar_logo() -> None:
    render_toggle = getattr(common, "render_sidebar_logo_toggle", None)
    if callable(render_toggle):
        render_toggle(width=56, height=56)
        return
    st.sidebar.markdown(_sidebar_logo_markup(), unsafe_allow_html=True)


def _render_artifacts_picker() -> Path:
    picker = getattr(common, "render_artifacts_dir_picker", None)
    if callable(picker):
        return picker(repo_root=_repo_root())

    # Back-compat fallback for environments that have not picked up the helper yet.
    st.session_state.setdefault("artifacts_dir", str(artifacts_dir_from_query()))
    raw = st.sidebar.text_input(
        "Artifacts directory",
        value=st.session_state["artifacts_dir"],
        help="Root folder containing run artifacts",
    )
    resolved = Path(str(raw or "").strip() or "artifacts").expanduser()
    if not resolved.is_absolute():
        resolved = (_repo_root() / resolved).resolve()
    st.session_state["artifacts_dir"] = str(resolved)
    try:
        st.query_params["artifacts-dir"] = str(resolved)
    except Exception:
        pass
    return resolved


def _default_thread_count() -> int:
    cores = os.cpu_count()
    if isinstance(cores, int) and cores > 0:
        return max(1, cores // 2)
    return 4


def _as_relative(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def _resolve_yaml_path(raw: str, *, repo_root: Path) -> Path:
    value = str(raw or "").strip()
    if not value:
        return Path()
    candidate = Path(value).expanduser()
    if candidate.is_absolute():
        return candidate
    in_repo = (repo_root / candidate).resolve()
    if in_repo.exists():
        return in_repo
    return candidate


def _reset_dashboard_state() -> None:
    soft_reset_dashboard_state(reset_keys=_RESET_KEYS, artifacts_dir="artifacts")


def _run_label(run: RunArtifact, artifacts_dir: Path) -> str:
    group = bundle_label(run, artifacts_dir)
    return f"{run.display_name} [{backend_name(run)}] · {group}"


def _run_path_label(run: RunArtifact, artifacts_dir: Path) -> str:
    try:
        return str(run.run_dir.relative_to(artifacts_dir))
    except Exception:
        return str(run.run_dir)


def _default_parity_root(artifacts_root: Path) -> Path:
    dedicated = artifacts_root / "parity"
    return dedicated if dedicated.exists() and dedicated.is_dir() else artifacts_root


def _latest_parity_run_for_filter(
    artifacts_dir: Path,
    *,
    scenario_filter: str,
) -> tuple[ParityRunArtifact | None, float | None]:
    parity_root = _default_parity_root(artifacts_dir)
    parity_runs = scan_parity_artifacts(parity_root)
    token = str(scenario_filter or "").strip().lower()
    candidates = [
        run
        for run in parity_runs
        if (token in str(run.scenario_name).lower() if token else True)
    ]
    candidates.sort(key=lambda item: item.timestamp if item.timestamp else "00000000_000000", reverse=True)
    for run in candidates:
        max_abs_diff: float | None = None
        try:
            payload = json.loads(run.parity_report_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                detail = payload.get("pabev_detail")
                if isinstance(detail, dict):
                    raw = detail.get("max_abs_diff")
                    if raw is not None:
                        max_abs_diff = float(raw)
        except Exception:
            max_abs_diff = None
        return run, max_abs_diff
    return None, None


def _open_parity(*, scenario_filter: str) -> None:
    st.session_state["parity_scenario_filter"] = scenario_filter
    try:
        st.query_params["scenario_filter"] = scenario_filter
    except Exception:
        pass
    st.switch_page("pages/11_Parity.py")


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
    return pd.DataFrame(
        rows,
        columns=["Use", "Run Key", "Scenario", "Timestamp", "Backend", "Bundle", "Path"],
    )


def _bundle_filter_table(
    rows: list[dict[str, object]],
    *,
    selected: set[str],
) -> pd.DataFrame:
    out: list[dict[str, object]] = []
    for row in rows:
        bundle = str(row.get("Bundle", ""))
        out.append(
            {
                "Use": bundle in selected,
                "Bundle": bundle,
                "Runs": int(row.get("Runs", 0)),
                "With output": int(row.get("With output", 0)),
                "Backends": str(row.get("Backends", "")),
                "Latest": str(row.get("Latest", "")),
            }
        )
    return pd.DataFrame(
        out,
        columns=["Use", "Bundle", "Runs", "With output", "Backends", "Latest"],
    )


def _home_bundle_checkbox_key(bundle_name: str) -> str:
    digest = hashlib.sha1(str(bundle_name).encode("utf-8")).hexdigest()[:12]
    return f"home_bundle_pick_{digest}"


def _home_run_checkbox_key(run_key: str) -> str:
    digest = hashlib.sha1(str(run_key).encode("utf-8")).hexdigest()[:12]
    return f"home_run_pick_{digest}"


def _delete_runs(runs: list[RunArtifact], artifacts_dir: Path) -> int:
    removed = 0
    for run in runs:
        if run.run_dir.exists():
            shutil.rmtree(run.run_dir)
            removed += 1
            parent = run.run_dir.parent
            if parent != artifacts_dir and parent.exists() and not any(parent.iterdir()):
                parent.rmdir()
    return removed


def _bundle_rows(runs: list[RunArtifact], artifacts_dir: Path) -> list[dict[str, object]]:
    grouped: dict[str, dict[str, object]] = {}
    for run in runs:
        label = bundle_label(run, artifacts_dir)
        row = grouped.setdefault(
            label,
            {
                "Bundle": label,
                "Runs": 0,
                "With output": 0,
                "Backends": set(),
                "Latest": "",
            },
        )
        row["Runs"] = int(row["Runs"]) + 1
        row["With output"] = int(row["With output"]) + (1 if run.has_output else 0)
        cast = row["Backends"]
        if isinstance(cast, set):
            cast.add(backend_name(run))
        latest = str(row["Latest"])
        if run.timestamp and run.timestamp > latest:
            row["Latest"] = run.timestamp

    rows: list[dict[str, object]] = []
    for row in grouped.values():
        backends = row["Backends"]
        rows.append(
            {
                "Bundle": row["Bundle"],
                "Runs": row["Runs"],
                "With output": row["With output"],
                "Backends": ", ".join(sorted(backends)) if isinstance(backends, set) else "",
                "Latest": row["Latest"],
            }
        )
    rows.sort(key=lambda item: str(item["Latest"]), reverse=True)
    return rows


def main() -> None:
    st.set_page_config(
        page_title="fp-wraptr Run Manager",
        page_icon=page_favicon(),
        layout="wide",
        menu_items={"Get Help": RESET_MENU_URL},
    )
    if reset_requested_from_query():
        _reset_dashboard_state()
        st.rerun()
    logo_col, title_col, reset_col = st.columns([1, 7, 2])
    render_title_logo = getattr(common, "render_title_logo_toggle", None)
    if callable(render_title_logo):
        with logo_col:
            render_title_logo(width=72, height=72)
    else:
        _header_img = logo_path("header-pair.png")
        if _header_img.exists():
            logo_col.markdown(logo_html("header-pair.png", 72, "fp-wraptr"), unsafe_allow_html=True)
    title_col.title("Run Manager")
    if reset_col.button("Reset Dashboard", use_container_width=True, key="home_dash_reset_btn"):
        _reset_dashboard_state()
        st.rerun()
    st.caption("Select and manage scenario runs and bundles used across dashboard pages.")
    st.divider()

    repo_root = _repo_root()

    _sidebar_logo = logo_path("sidebar-raptr.png")
    if _sidebar_logo.exists():
        _render_sidebar_logo()

    st.session_state.setdefault("artifacts_dir", str(artifacts_dir_from_query()))
    artifacts_path = _render_artifacts_picker()
    st.session_state["artifacts_dir"] = str(artifacts_path)
    if not artifacts_path.exists():
        st.warning(f"Artifacts directory does not exist: {artifacts_path}")

    runs = scan_artifacts(artifacts_path)
    st.session_state["runs"] = runs
    runs_with_output = [run for run in runs if run.has_output]

    c1, c2 = st.columns(2)
    c1.metric("Total runs", len(runs))
    c2.metric("Runs with output", len(runs_with_output))

    with st.expander("Quick Parity Access", expanded=False):
        helper_col, status_col = st.columns([2, 3])
        helper_col.caption("Jump to Parity with an optional scenario substring filter.")
        parity_quick_filter = helper_col.text_input(
            "Parity scenario contains",
            value=str(st.session_state.get("parity_scenario_filter", "") or ""),
            placeholder="optional substring filter",
            key="home_parity_quick_filter",
            label_visibility="collapsed",
        )
        if helper_col.button(
            "Open Parity page",
            key="home_open_parity",
            use_container_width=True,
        ):
            _open_parity(scenario_filter=parity_quick_filter)
        helper_col.caption("Hint: on Parity page, select a series variable (for example `PIEF` or `PCPF`).")

        latest_parity_run, latest_max_abs_diff = _latest_parity_run_for_filter(
            artifacts_path,
            scenario_filter=parity_quick_filter,
        )
        if latest_parity_run is None:
            status_col.info("No parity runs found for the current filter under the scanned root.")
        else:
            m1, m2, m3 = status_col.columns(3)
            m1.metric("Latest parity", latest_parity_run.timestamp or "unknown")
            m2.metric("Status", latest_parity_run.status or "unknown")
            m3.metric(
                "Max abs diff",
                f"{latest_max_abs_diff:.6g}" if latest_max_abs_diff is not None else "n/a",
            )
            status_col.caption(f"Run: `{latest_parity_run.display_name}`")

    if not runs:
        st.info("No runs found yet. Use the launchers below or the New Run page.")

    st.subheader("Active Run Selection")

    all_backends = sorted({backend_name(run) for run in runs}) or ["unknown"]
    st.session_state.setdefault(BACKEND_FILTER_KEY, all_backends)
    st.session_state.setdefault(
        BUNDLE_FILTER_KEY, sorted({bundle_label(run, artifacts_path) for run in runs})
    )
    st.session_state.setdefault(SCENARIO_FILTER_KEY, "")

    with st.expander("Filters", expanded=False):
        backend_default = [
            value
            for value in st.session_state.get(BACKEND_FILTER_KEY, all_backends)
            if value in all_backends
        ]
        if not backend_default:
            backend_default = list(all_backends)
        selected_backends = st.multiselect(
            "Backends",
            options=all_backends,
            default=backend_default,
        )
        st.session_state[BACKEND_FILTER_KEY] = selected_backends

        bundle_rows_for_filter = _bundle_rows(runs, artifacts_path)
        bundle_options = [str(item["Bundle"]) for item in bundle_rows_for_filter]
        bundle_default = [
            value
            for value in st.session_state.get(BUNDLE_FILTER_KEY, bundle_options)
            if value in bundle_options
        ]
        if not bundle_default:
            bundle_default = list(bundle_options)
        st.caption("Bundles/Groups")
        bundle_force_sync = bool(st.session_state.get("home_bundle_filter_force_sync", False))
        selected_bundles = []
        with st.container(height=min(280, 42 * max(4, len(bundle_rows_for_filter) + 1))):
            header_cols = st.columns([1, 3, 1.2, 1.4, 2, 1.6])
            for col, label in zip(
                header_cols,
                ["Use", "Bundle", "Runs", "Output", "Backends", "Latest"],
                strict=False,
            ):
                col.caption(label)
            for row in bundle_rows_for_filter:
                bundle_name = str(row.get("Bundle", ""))
                widget_key = _home_bundle_checkbox_key(bundle_name)
                if bundle_force_sync or widget_key not in st.session_state:
                    st.session_state[widget_key] = bundle_name in set(bundle_default)
                row_cols = st.columns([1, 3, 1.2, 1.4, 2, 1.6])
                checked = row_cols[0].checkbox(
                    f"Use bundle {bundle_name}",
                    key=widget_key,
                    label_visibility="collapsed",
                )
                row_cols[1].caption(bundle_name)
                row_cols[2].caption(str(int(row.get("Runs", 0))))
                row_cols[3].caption(str(int(row.get("With output", 0))))
                row_cols[4].caption(str(row.get("Backends", "")))
                row_cols[5].caption(str(row.get("Latest", "")))
                if checked:
                    selected_bundles.append(bundle_name)
        st.session_state["home_bundle_filter_force_sync"] = False
        st.session_state[BUNDLE_FILTER_KEY] = selected_bundles

        scenario_text = st.text_input(
            "Scenario name contains",
            value=str(st.session_state.get(SCENARIO_FILTER_KEY, "")),
            placeholder="optional substring filter",
        ).strip()
        st.session_state[SCENARIO_FILTER_KEY] = scenario_text

    filtered = [
        run
        for run in runs
        if backend_name(run) in set(selected_backends or all_backends)
        and bundle_label(run, artifacts_path) in set(selected_bundles or bundle_options)
        and (scenario_text.lower() in run.scenario_name.lower() if scenario_text else True)
    ]
    filtered.sort(key=lambda item: item.timestamp if item.timestamp else "00000000_000000", reverse=True)

    bundle_rows = _bundle_rows(filtered, artifacts_path)
    if bundle_rows:
        st.dataframe(bundle_rows, use_container_width=True, hide_index=True, height=min(400, 35 * len(bundle_rows) + 40))
    else:
        st.caption("No bundle/run groups detected for the current filters.")

    selected_keys = set(st.session_state.get(ACTIVE_RUN_DIRS_KEY, []))
    default_active = filter_runs_by_keys(filtered, selected_keys)
    if not default_active:
        default_active = latest_preferred_runs(
            filtered,
            limit=min(6, max(1, len(filtered))),
            has_output=True,
        )
    if not default_active:
        default_active = latest_runs(filtered, limit=min(6, max(1, len(filtered))), has_output=False)

    default_active_keys = {run_dir_key(run.run_dir) for run in default_active}
    active_force_sync = bool(st.session_state.get("home_active_runs_force_sync", False))
    selected_run_keys: set[str] = set()
    ordered_keys = [run_dir_key(run.run_dir) for run in filtered]
    with st.container(height=min(340, 42 * max(4, len(filtered) + 1))):
        header_cols = st.columns([1, 3, 2, 1.4, 1.8, 4])
        for col, label in zip(
            header_cols,
            ["Use", "Scenario", "Timestamp", "Backend", "Bundle", "Path"],
            strict=False,
        ):
            col.caption(label)
        for run in filtered:
            run_key = run_dir_key(run.run_dir)
            widget_key = _home_run_checkbox_key(run_key)
            if active_force_sync or widget_key not in st.session_state:
                st.session_state[widget_key] = run_key in default_active_keys
            row_cols = st.columns([1, 3, 2, 1.4, 1.8, 4])
            checked = row_cols[0].checkbox(
                f"Use run {run.scenario_name} {run.timestamp or ''}",
                key=widget_key,
                label_visibility="collapsed",
            )
            row_cols[1].caption(str(run.scenario_name))
            row_cols[2].caption(str(run.timestamp or ""))
            row_cols[3].caption(str(backend_name(run)))
            row_cols[4].caption(str(bundle_label(run, artifacts_path)))
            row_cols[5].caption(_run_path_label(run, artifacts_path))
            if checked:
                selected_run_keys.add(run_key)
    st.session_state["home_active_runs_force_sync"] = False
    by_key = {run_dir_key(run.run_dir): run for run in filtered}
    active_runs = [by_key[key] for key in ordered_keys if key in selected_run_keys and key in by_key]
    st.session_state[ACTIVE_RUN_DIRS_KEY] = [run_dir_key(run.run_dir) for run in active_runs]
    st.caption(f"Loaded {len(active_runs)} active run(s) for dashboard pages.")

    actions_left, actions_mid, actions_right = st.columns([2, 2, 2])
    if actions_left.button("Use latest 3 with output"):
        chosen = latest_preferred_runs(filtered, limit=3, has_output=True)
        st.session_state[ACTIVE_RUN_DIRS_KEY] = [run_dir_key(run.run_dir) for run in chosen]
        st.session_state["home_active_runs_force_sync"] = True
        st.rerun()
    if actions_mid.button("Clear active selection"):
        st.session_state[ACTIVE_RUN_DIRS_KEY] = []
        st.session_state["home_active_runs_force_sync"] = True
        st.rerun()
    if actions_right.button("Refresh run scan"):
        st.rerun()

    st.divider()
    st.subheader("Launch Runs")
    run_tab, bundle_tab, cleanup_tab = st.tabs(["Run Scenario YAML", "Run Bundle YAML", "Cleanup"])
    try:
        catalog = load_scenario_catalog(repo_root=repo_root)
    except Exception as exc:
        catalog = None
        st.warning(f"Catalog unavailable; quick-pick lists are hidden: {exc}")

    with run_tab:
        scenario_entries = (
            catalog.for_surface("home", kind="scenario", public_only=True)
            if catalog is not None
            else []
        )
        scenario_options: list[CatalogEntry | None] = [None, *scenario_entries]
        picked_example = st.selectbox(
            "Curated scenario",
            options=scenario_options,
            index=0,
            help="Optional quick picker; you can also provide any YAML path below.",
            format_func=lambda item: "" if item is None else item.label,
        )
        first_example = (
            _as_relative(scenario_entries[0].resolved_path(repo_root=repo_root), repo_root)
            if scenario_entries
            else ""
        )
        default_scenario_path = (
            _as_relative(picked_example.resolved_path(repo_root=repo_root), repo_root)
            if picked_example is not None
            else first_example
        )
        scenario_path_text = st.text_input("Scenario YAML path", value=default_scenario_path)
        scenario_backend = st.selectbox(
            "Backend",
            options=["yaml default", "fpexe", "fppy", "both"],
            index=0,
        )
        scenario_fppy_preset = st.selectbox(
            "fppy EQ preset",
            options=["parity", "default"],
            index=0,
            help="Applied only when backend is overridden to fppy.",
        )
        scenario_fppy_threads = int(
            st.number_input(
                "fppy threads",
                min_value=1,
                max_value=256,
                value=_default_thread_count(),
                step=1,
                help=(
                    "Sets OMP/BLAS thread env vars for fppy runs "
                    "(OMP_NUM_THREADS, OPENBLAS_NUM_THREADS, MKL_NUM_THREADS, "
                    "NUMEXPR_NUM_THREADS, VECLIB_MAXIMUM_THREADS). Ignored unless "
                    "the effective backend is fppy or both."
                ),
            )
        )
        if st.button("Run scenario", type="primary"):
            scenario_path = _resolve_yaml_path(scenario_path_text, repo_root=repo_root)
            if not scenario_path.exists():
                st.error(f"Scenario YAML not found: {scenario_path}")
            else:
                with st.spinner(f"Running scenario from {scenario_path}..."):
                    try:
                        cfg = load_scenario_config(scenario_path)
                        cfg.fppy = {**(cfg.fppy or {}), "num_threads": int(scenario_fppy_threads)}
                        if scenario_backend != "yaml default":
                            cfg.backend = scenario_backend
                            if scenario_backend == "fppy":
                                cfg.fppy = {**(cfg.fppy or {}), "eq_flags_preset": scenario_fppy_preset}
                        result = run_scenario(config=cfg, output_dir=artifacts_path)
                    except Exception as exc:
                        st.error(f"Scenario run failed: {exc}")
                    else:
                        st.success(f"Scenario run complete: {result.output_dir}")
                        st.session_state[ACTIVE_RUN_DIRS_KEY] = [str(result.output_dir.resolve())]
                        st.rerun()

    with bundle_tab:
        bundle_entries = (
            catalog.for_surface("home", kind="bundle", public_only=True)
            if catalog is not None
            else []
        )
        bundle_options: list[CatalogEntry | None] = [None, *bundle_entries]
        picked_bundle = st.selectbox(
            "Curated bundle",
            options=bundle_options,
            index=0,
            help="Optional quick picker; provide a path below for any custom bundle YAML.",
            format_func=lambda item: "" if item is None else item.label,
        )
        first_bundle = (
            _as_relative(bundle_entries[0].resolved_path(repo_root=repo_root), repo_root)
            if bundle_entries
            else ""
        )
        default_bundle_path = (
            _as_relative(picked_bundle.resolved_path(repo_root=repo_root), repo_root)
            if picked_bundle is not None
            else first_bundle
        )
        bundle_path_text = st.text_input("Bundle YAML path", value=default_bundle_path)
        bundle_backend = st.selectbox(
            "Backend override",
            options=["bundle default", "fpexe", "fppy", "both"],
            index=0,
        )
        bundle_fppy_preset = st.selectbox(
            "Bundle fppy EQ preset",
            options=["parity", "default"],
            index=0,
            help="Applied only when backend override is fppy.",
        )
        bundle_fppy_threads = int(
            st.number_input(
                "Bundle fppy threads",
                min_value=1,
                max_value=256,
                value=_default_thread_count(),
                step=1,
                help=(
                    "Sets OMP/BLAS thread env vars for fppy runs in bundle variants "
                    "(including backend=both parity variants). Ignored unless the "
                    "effective backend is fppy or both."
                ),
            )
        )
        bundle_tag = st.text_input(
            "Bundle output prefix",
            value="bundle",
            help="Runs are written under artifacts/<prefix>_<timestamp>/",
        )
        if st.button("Run bundle", type="primary"):
            bundle_path = _resolve_yaml_path(bundle_path_text, repo_root=repo_root)
            if not bundle_path.exists():
                st.error(f"Bundle YAML not found: {bundle_path}")
            else:
                timestamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d_%H%M%S")
                safe_tag = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in bundle_tag).strip("_")
                if not safe_tag:
                    safe_tag = "bundle"
                run_root = artifacts_path / f"{safe_tag}_{timestamp}"
                with st.spinner(f"Running bundle from {bundle_path}..."):
                    try:
                        cfg = BundleConfig.from_yaml(bundle_path)
                        cfg.base = dict(cfg.base)
                        cfg.base["fppy"] = {
                            **(cfg.base.get("fppy", {}) or {}),
                            "num_threads": int(bundle_fppy_threads),
                        }
                        if bundle_backend != "bundle default":
                            cfg.base["backend"] = bundle_backend
                            if bundle_backend == "fppy":
                                cfg.base["fppy"] = {
                                    **(cfg.base.get("fppy", {}) or {}),
                                    "eq_flags_preset": bundle_fppy_preset,
                                }
                        result = run_bundle(cfg, output_dir=run_root)
                    except Exception as exc:
                        st.error(f"Bundle run failed: {exc}")
                    else:
                        succeeded = [entry for entry in result.entries if entry.success]
                        failed = [entry for entry in result.entries if not entry.success]
                        st.success(
                            f"Bundle complete: {result.n_succeeded}/{result.n_variants} succeeded "
                            f"(failed: {result.n_failed}). Output root: {run_root}"
                        )
                        if failed:
                            st.warning("Failed variants:\n" + "\n".join(
                                f"- {entry.variant_name}: {entry.error}" for entry in failed
                            ))
                        st.session_state[ACTIVE_RUN_DIRS_KEY] = [
                            run_dir_key(Path(entry.output_dir))
                            for entry in succeeded
                            if entry.output_dir is not None
                        ]
                        st.rerun()

    with cleanup_tab:
        retention_days = int(
            st.number_input("Older than N days", min_value=1, max_value=3650, value=30, step=1)
        )
        now = dt.datetime.now()
        old_runs: list[RunArtifact] = []
        for run in runs:
            parsed = parse_run_timestamp(run.timestamp)
            if parsed is None:
                continue
            if (now - parsed).days >= retention_days:
                old_runs.append(run)
        old_runs.sort(key=lambda item: item.timestamp if item.timestamp else "00000000_000000")
        st.caption(f"Old runs matching filter: {len(old_runs)}")
        purge_runs = st.multiselect(
            "Runs to delete",
            options=old_runs,
            format_func=lambda item: _run_label(item, artifacts_path),
        )
        confirm_delete = st.checkbox(
            "Confirm permanent deletion",
            value=False,
            help="Deletes selected run directories from disk.",
        )
        if st.button("Delete selected runs", type="secondary"):
            if not purge_runs:
                st.info("Select one or more runs to delete.")
            elif not confirm_delete:
                st.error("Enable confirmation before deleting.")
            else:
                removed = _delete_runs(purge_runs, artifacts_path)
                selected_after_delete = set(st.session_state.get(ACTIVE_RUN_DIRS_KEY, []))
                selected_after_delete -= {run_dir_key(run.run_dir) for run in purge_runs}
                st.session_state[ACTIVE_RUN_DIRS_KEY] = sorted(selected_after_delete)
                st.success(f"Deleted {removed} run directory(ies).")
                st.rerun()

    st.caption("Use the sidebar to navigate to Compare, Run Panels, and other pages.")


if __name__ == "__main__":
    main()
