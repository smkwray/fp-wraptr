"""Create, author, compile, and run scenarios from the dashboard."""

from __future__ import annotations

import os
from pathlib import Path

import streamlit as st
import yaml
from pydantic import ValidationError

from fp_wraptr.dashboard import _common as common
from fp_wraptr.dashboard._common import artifacts_dir_from_query, page_favicon
from fp_wraptr.dashboard.agent_handoff import render_advanced_toggle, render_agent_handoff
from fp_wraptr.dashboard.artifacts import scan_artifacts
from fp_wraptr.dashboard.charts import forecast_figure
from fp_wraptr.dashboard.overlay_editors import render_scenario_cards
from fp_wraptr.dashboard.scenario_tools import ScenarioInputPreflight, preflight_scenario_input
from fp_wraptr.runtime.fp_exe import FPExecutable
from fp_wraptr.scenarios.authoring import (
    BundleDraft,
    CardInstance,
    DraftSourceRef,
    ScenarioDraft,
    compile_bundle_workspace,
    compile_scenario_workspace,
    create_bundle_draft_from_source,
    create_scenario_draft_from_source,
    initialize_card_instances,
    list_workspaces,
    load_card_specs,
    load_series_points_from_text,
    load_workspace_draft,
    normalize_series_points,
    resolve_card_defaults,
    save_workspace_draft,
    slugify,
    workspace_paths,
)
from fp_wraptr.scenarios.bundle import run_bundle
from fp_wraptr.scenarios.catalog import CatalogEntry, load_scenario_catalog
from fp_wraptr.scenarios.config import ScenarioConfig, VariableOverride
from fp_wraptr.scenarios.runner import load_scenario_config, run_scenario


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _default_thread_count() -> int:
    cores = os.cpu_count()
    if isinstance(cores, int) and cores > 0:
        return max(1, cores // 2)
    return 4


def _load_uploaded_config(uploaded: object) -> ScenarioConfig:
    payload = yaml.safe_load(uploaded.getvalue().decode("utf-8"))
    if payload is None:
        payload = {}
    if not isinstance(payload, dict):
        raise ValueError("Uploaded YAML must define a mapping/object.")
    return ScenarioConfig(**payload)


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


def _build_scratch_config(
    *,
    name: str,
    description: str,
    fp_home: str,
    input_file: str,
    input_overlay_dir: str,
    forecast_start: str,
    forecast_end: str,
    track_variables: str,
    override_values: list[tuple[str, str, float]],
) -> ScenarioConfig:
    track_vars = [value.strip() for value in track_variables.split(",") if value.strip()]
    overrides = {
        name_: VariableOverride(method=method_name, value=value)
        for name_, method_name, value in override_values
    }
    overlay_value = input_overlay_dir.strip()
    overlay_dir = Path(overlay_value) if overlay_value else None
    return ScenarioConfig(
        name=name,
        description=description,
        fp_home=Path(fp_home),
        input_file=input_file.strip() or "fminput.txt",
        input_overlay_dir=overlay_dir,
        forecast_start=forecast_start,
        forecast_end=forecast_end,
        track_variables=track_vars,
        overrides=overrides,
    )


def _render_input_deck_readiness(
    config: ScenarioConfig,
    preflight: ScenarioInputPreflight,
) -> None:
    with st.expander("Input Deck Readiness", expanded=True):
        st.caption("Read-only preflight of the staged FP input tree.")
        st.write(f"Input file: `{config.input_file}`")
        st.write(f"fp_home: `{config.fp_home}`")
        st.write(
            "input_overlay_dir: "
            + (f"`{config.input_overlay_dir}`" if config.input_overlay_dir else "_none_")
        )
        if preflight.entry_source_path is not None:
            st.write(
                "Entry source: "
                f"`{preflight.entry_source_path}` ({preflight.entry_source_kind or 'unknown'})"
            )

        if preflight.error:
            st.error(preflight.error)
            return

        st.success("Input tree resolved successfully.")
        st.write(
            "Included scripts: "
            + (
                ", ".join(f"`{name}`" for name in preflight.include_files)
                if preflight.include_files
                else "_none_"
            )
        )
        st.write(
            "LOADDATA dependencies: "
            + (
                ", ".join(f"`{name}`" for name in preflight.load_data_files)
                if preflight.load_data_files
                else "_none_"
            )
        )
        st.write(
            "Expected outputs: "
            + (
                ", ".join(f"`{name}`" for name in preflight.expected_output_files)
                if preflight.expected_output_files
                else "_none_"
            )
        )


def _workspace_label(path_text: str) -> str:
    draft = load_workspace_draft(path_text)
    kind = "Bundle" if isinstance(draft, BundleDraft) else "Scenario"
    return f"{draft.family} / {draft.slug} ({kind})"


def _number_input_for_constant(
    *,
    key: str,
    label: str,
    value: float,
    help_text: str,
    step: float | None,
    min_value: float | None,
    max_value: float | None,
    number_format: str,
) -> float:
    kwargs: dict[str, object] = {
        "label": label,
        "value": float(value),
        "key": key,
        "help": help_text or None,
        "format": number_format,
    }
    if step is not None:
        kwargs["step"] = float(step)
    if min_value is not None:
        kwargs["min_value"] = float(min_value)
    if max_value is not None:
        kwargs["max_value"] = float(max_value)
    return float(st.number_input(**kwargs))


def _track_variables_text(values: list[str]) -> str:
    return ", ".join(values)


def _parse_track_variables(raw: str) -> list[str]:
    return [token.strip().upper() for token in str(raw).split(",") if token.strip()]


def _current_workspace_path(existing_paths: list[str]) -> str:
    current = str(st.session_state.get("authoring_workspace_path", "")).strip()
    if current in existing_paths:
        return current
    if existing_paths:
        return existing_paths[0]
    return ""


def _render_card_editor(
    *,
    workspace_slug: str,
    scope_key: str,
    specs: list[object],
    existing_cards: list[CardInstance],
    defaults: dict[str, dict[str, float]],
    upload_payloads: dict[str, tuple[str, bytes]],
) -> list[CardInstance]:
    updated_cards: list[CardInstance] = []
    instances = initialize_card_instances(specs, existing_cards)
    for spec, existing in zip(specs, instances, strict=False):
        with st.container(border=True):
            enabled = st.checkbox(
                spec.label,
                value=bool(existing.enabled),
                key=f"{scope_key}_enabled_{workspace_slug}_{spec.card_id}",
                help=getattr(spec, "description", ""),
            )
            if getattr(spec, "description", ""):
                st.caption(getattr(spec, "description", ""))
            if spec.kind == "deck_constants":
                constants = dict(defaults.get(spec.card_id, {}))
                constants.update(existing.constants)
                updated_constants: dict[str, float] = {}
                for file_spec in spec.files:
                    st.markdown(f"**{file_spec.label or file_spec.path}**")
                    for group in sorted(file_spec.groups, key=lambda item: item.order):
                        st.markdown(f"*{group.label}*")
                        if group.description:
                            st.caption(group.description)
                        columns = st.columns(2)
                        for idx, field in enumerate(sorted(group.fields, key=lambda item: item.order)):
                            col = columns[idx % 2]
                            with col:
                                updated_constants[field.symbol] = _number_input_for_constant(
                                    key=f"{scope_key}_const_{workspace_slug}_{spec.card_id}_{field.symbol}",
                                    label=field.label,
                                    value=float(constants.get(field.symbol, 0.0)),
                                    help_text=field.help_text,
                                    step=field.step,
                                    min_value=field.min_value,
                                    max_value=field.max_value,
                                    number_format=field.number_format,
                                )
                updated_cards.append(
                    CardInstance(
                        card_id=spec.card_id,
                        enabled=enabled,
                        constants=updated_constants,
                    )
                )
                continue

            target_labels = {target.kind: (target.label or target.kind) for target in spec.targets}
            target = st.selectbox(
                "Output target",
                options=[target.kind for target in spec.targets],
                index=[target.kind for target in spec.targets].index(existing.selected_target or spec.default_target),
                format_func=lambda item, labels=target_labels: labels[item],
                key=f"{scope_key}_target_{workspace_slug}_{spec.card_id}",
            )
            input_mode = st.radio(
                "Input mode",
                options=list(spec.input_modes),
                index=list(spec.input_modes).index(existing.input_mode or spec.input_modes[0]),
                horizontal=True,
                key=f"{scope_key}_input_mode_{workspace_slug}_{spec.card_id}",
            )
            pasted_text = existing.pasted_text
            preview_points = dict(existing.series_points)
            import_path = existing.import_path
            upload_key = f"{scope_key}:{spec.card_id}"
            if input_mode == "csv":
                upload = st.file_uploader(
                    "CSV or TXT upload",
                    type=["csv", "txt"],
                    key=f"{scope_key}_upload_{workspace_slug}_{spec.card_id}",
                )
                if upload is not None:
                    payload = upload.getvalue()
                    upload_payloads[upload_key] = (upload.name, payload)
                    try:
                        preview_points = normalize_series_points(
                            load_series_points_from_text(payload.decode("utf-8"))
                        )
                    except Exception as exc:
                        st.error(f"Import parse failed: {exc}")
                        preview_points = {}
                    import_path = upload.name
                elif import_path:
                    st.caption(f"Current import: `{import_path}`")
            else:
                pasted_text = st.text_area(
                    "Quarterly series (period,value)",
                    value=existing.pasted_text,
                    height=140,
                    key=f"{scope_key}_paste_{workspace_slug}_{spec.card_id}",
                )
                if pasted_text.strip():
                    try:
                        preview_points = normalize_series_points(load_series_points_from_text(pasted_text))
                    except Exception as exc:
                        st.error(f"Pasted series parse failed: {exc}")
                        preview_points = {}
                        import_path = None
                else:
                    import_path = None
            if preview_points:
                st.dataframe(
                    [{"period": period, "value": value} for period, value in preview_points.items()],
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.caption("No normalized quarterly series loaded yet.")
            updated_cards.append(
                CardInstance(
                    card_id=spec.card_id,
                    enabled=enabled,
                    selected_target=target,
                    input_mode=input_mode,
                    import_path=import_path,
                    pasted_text=pasted_text if input_mode == "paste" else "",
                    series_points=preview_points,
                )
            )
    return updated_cards


def _persist_uploaded_card_payloads(
    *,
    cards: list[CardInstance],
    upload_payloads: dict[str, tuple[str, bytes]],
    imports_dir: Path,
    scope_key: str,
) -> list[CardInstance]:
    saved_cards: list[CardInstance] = []
    for card in cards:
        payload = upload_payloads.get(f"{scope_key}:{card.card_id}")
        if payload is None:
            if card.input_mode == "paste":
                card = card.model_copy(update={"import_path": None})
            saved_cards.append(card)
            continue
        original_name, raw = payload
        suffix = Path(original_name).suffix or ".csv"
        saved_name = f"{slugify(scope_key)}-{slugify(card.card_id)}{suffix}"
        target = imports_dir / saved_name
        target.write_bytes(raw)
        saved_cards.append(card.model_copy(update={"import_path": saved_name}))
    return saved_cards


def _render_authoring_mode(*, artifacts_dir: Path) -> None:
    repo_root = _repo_root()
    catalog = load_scenario_catalog(repo_root=repo_root)
    scenario_entries = catalog.filtered(kind="scenario", surface="new_run", public_only=True)
    bundle_entries = catalog.filtered(kind="bundle", surface="new_run", public_only=True)
    workspaces = list_workspaces(repo_root)

    st.subheader("Managed Authoring")
    st.caption(
        "Create or reopen a managed workspace, edit curated cards, compile overlay-backed scenarios or bundles, and run them."
    )

    create_col, existing_col = st.columns([1, 1])
    with create_col:
        st.markdown("**Create Workspace**")
        if scenario_entries:
            scenario_entry = st.selectbox(
                "Scenario seed",
                options=[None, *scenario_entries],
                format_func=lambda item: "" if item is None else item.label,
                key="authoring_seed_scenario",
            )
            if st.button("Create Scenario Workspace", key="authoring_create_scenario"):
                if scenario_entry is None:
                    st.warning("Choose a scenario seed first.")
                else:
                    draft = create_scenario_draft_from_source(
                        DraftSourceRef(kind="catalog", value=scenario_entry.entry_id),
                        repo_root=repo_root,
                    )
                    save_path = save_workspace_draft(draft, repo_root=repo_root)
                    st.session_state["authoring_workspace_path"] = str(save_path)
                    st.rerun()
        if bundle_entries:
            bundle_entry = st.selectbox(
                "Bundle seed",
                options=[None, *bundle_entries],
                format_func=lambda item: "" if item is None else item.label,
                key="authoring_seed_bundle",
            )
            if st.button("Create Bundle Workspace", key="authoring_create_bundle"):
                if bundle_entry is None:
                    st.warning("Choose a bundle seed first.")
                else:
                    draft = create_bundle_draft_from_source(
                        DraftSourceRef(kind="catalog", value=bundle_entry.entry_id),
                        repo_root=repo_root,
                    )
                    save_path = save_workspace_draft(draft, repo_root=repo_root)
                    st.session_state["authoring_workspace_path"] = str(save_path)
                    st.rerun()

    with existing_col:
        st.markdown("**Open Workspace**")
        existing_paths = [str(info.draft_path) for info in workspaces]
        selected_path = st.selectbox(
            "Existing workspaces",
            options=["", *existing_paths],
            index=(["", *existing_paths].index(_current_workspace_path(existing_paths)) if _current_workspace_path(existing_paths) in ["", *existing_paths] else 0),
            format_func=lambda item: "" if not item else _workspace_label(item),
            key="authoring_existing_workspace",
        )
        if selected_path:
            st.session_state["authoring_workspace_path"] = selected_path

    workspace_path = str(st.session_state.get("authoring_workspace_path", "")).strip()
    if not workspace_path:
        st.info("Create a new workspace or open an existing one to begin authoring.")
        return

    draft = load_workspace_draft(workspace_path)
    workspace_dir = Path(workspace_path).parent
    paths = workspace_paths(workspace_dir)
    specs = load_card_specs(repo_root=repo_root, family=draft.family)
    shared_specs = specs
    variant_specs: list[object] = []
    if isinstance(draft, BundleDraft):
        shared_specs = [spec for spec in specs if spec.kind == "deck_constants"]
        variant_specs = [spec for spec in specs if spec.kind == "series_card"]
    defaults = resolve_card_defaults(draft, repo_root=repo_root) if specs else {}

    st.markdown("---")
    st.markdown(f"**Workspace** `{_as_relative(workspace_dir, repo_root)}`")
    metadata_col, compile_col = st.columns([2, 1])
    with metadata_col:
        label = st.text_input("Label", value=draft.label, key="authoring_label")
        description = st.text_input(
            "Description",
            value=draft.description,
            key="authoring_description",
        )
        if isinstance(draft, ScenarioDraft):
            scenario_name = st.text_input(
                "Scenario name",
                value=draft.scenario_name,
                key="authoring_scenario_name",
            )
        else:
            bundle_name = st.text_input(
                "Bundle name",
                value=draft.bundle_name,
                key="authoring_bundle_name",
            )
        forecast_cols = st.columns(2)
        forecast_start = forecast_cols[0].text_input(
            "Forecast start",
            value=draft.forecast_start,
            key="authoring_forecast_start",
        )
        forecast_end = forecast_cols[1].text_input(
            "Forecast end",
            value=draft.forecast_end,
            key="authoring_forecast_end",
        )
        backend = st.selectbox(
            "Backend",
            options=["fpexe", "fppy", "both"],
            index=["fpexe", "fppy", "both"].index(draft.backend if draft.backend in {"fpexe", "fppy", "both"} else "fpexe"),
            key="authoring_backend",
        )
        track_variables = st.text_input(
            "Track variables",
            value=_track_variables_text(draft.track_variables),
            help="Comma-separated output variables for authored runs.",
            key="authoring_track_variables",
        )
    with compile_col:
        report_path = paths["report"]
        if report_path.exists():
            st.caption("Latest compile report")
            st.code(_as_relative(report_path, repo_root))
            try:
                report = yaml.safe_load(report_path.read_text(encoding="utf-8"))
            except Exception:
                report = None
            if isinstance(report, dict):
                st.write(f"Generated files: {len(report.get('generated_files', []))}")
                errors = report.get("errors", [])
                if errors:
                    st.error("\n".join(str(item) for item in errors))
                else:
                    st.success("Last compile reported no errors.")
        else:
            st.caption("No compile report yet.")

    updated_cards: list[CardInstance] = []
    upload_payloads: dict[str, tuple[str, bytes]] = {}
    if shared_specs:
        st.markdown("### Cards")
        updated_cards = _render_card_editor(
            workspace_slug=draft.slug,
            scope_key="shared",
            specs=shared_specs,
            existing_cards=draft.cards,
            defaults=defaults,
            upload_payloads=upload_payloads,
        )
    else:
        st.info("No curated card specs are available for this workspace family yet.")

    if isinstance(draft, BundleDraft):
        st.markdown("### Variants")
        updated_variants = []
        for variant in draft.variants:
            with st.container(border=True):
                cols = st.columns([1.2, 1.2, 0.6])
                label_value = cols[0].text_input(
                    "Label",
                    value=variant.label,
                    key=f"variant_label_{draft.slug}_{variant.variant_id}",
                )
                input_value = cols[1].text_input(
                    "Input file",
                    value=variant.input_file or "",
                    key=f"variant_input_{draft.slug}_{variant.variant_id}",
                )
                enabled_value = cols[2].checkbox(
                    "Enabled",
                    value=variant.enabled,
                    key=f"variant_enabled_{draft.slug}_{variant.variant_id}",
                )
                variant_cards = _render_card_editor(
                    workspace_slug=draft.slug,
                    scope_key=f"variant_{variant.variant_id}",
                    specs=variant_specs,
                    existing_cards=variant.cards,
                    defaults={},
                    upload_payloads=upload_payloads,
                )
                updated_variants.append(
                    variant.model_copy(
                        update={
                            "label": label_value,
                            "input_file": input_value.strip() or None,
                            "enabled": enabled_value,
                            "cards": variant_cards,
                        }
                    )
                )
    else:
        updated_variants = None

    if isinstance(draft, ScenarioDraft):
        draft_updated: ScenarioDraft | BundleDraft = draft.model_copy(
            update={
                "label": label.strip() or draft.label,
                "description": description,
                "scenario_name": scenario_name.strip() or draft.scenario_name,
                "forecast_start": forecast_start.strip() or draft.forecast_start,
                "forecast_end": forecast_end.strip() or draft.forecast_end,
                "backend": backend,
                "track_variables": _parse_track_variables(track_variables),
                "cards": updated_cards,
            }
        )
    else:
        draft_updated = draft.model_copy(
            update={
                "label": label.strip() or draft.label,
                "description": description,
                "bundle_name": bundle_name.strip() or draft.bundle_name,
                "forecast_start": forecast_start.strip() or draft.forecast_start,
                "forecast_end": forecast_end.strip() or draft.forecast_end,
                "backend": backend,
                "track_variables": _parse_track_variables(track_variables),
                "cards": updated_cards,
                "variants": updated_variants or draft.variants,
            }
        )

    action_cols = st.columns(3)
    save_clicked = action_cols[0].button("Save Draft", key="authoring_save")
    compile_clicked = action_cols[1].button("Compile Draft", key="authoring_compile")
    run_clicked = action_cols[2].button("Run Compiled", key="authoring_run")

    if save_clicked or compile_clicked or run_clicked:
        imports_dir = paths["imports"]
        imports_dir.mkdir(parents=True, exist_ok=True)
        cards_for_save = _persist_uploaded_card_payloads(
            cards=draft_updated.cards,
            upload_payloads=upload_payloads,
            imports_dir=imports_dir,
            scope_key="shared",
        )
        if isinstance(draft_updated, BundleDraft):
            variants_for_save = [
                variant.model_copy(
                    update={
                        "cards": _persist_uploaded_card_payloads(
                            cards=variant.cards,
                            upload_payloads=upload_payloads,
                            imports_dir=imports_dir,
                            scope_key=f"variant_{variant.variant_id}",
                        )
                    }
                )
                for variant in draft_updated.variants
            ]
            draft_updated = draft_updated.model_copy(
                update={
                    "cards": cards_for_save,
                    "variants": variants_for_save,
                }
            )
        else:
            draft_updated = draft_updated.model_copy(update={"cards": cards_for_save})
        save_workspace_draft(draft_updated, repo_root=repo_root, workspace=workspace_dir)
        st.success(f"Saved draft: {_as_relative(workspace_dir, repo_root)}")

    compile_result = None
    if compile_clicked or run_clicked:
        with st.spinner("Compiling authored workspace..."):
            if isinstance(draft_updated, ScenarioDraft):
                compile_result = compile_scenario_workspace(
                    draft_updated,
                    repo_root=repo_root,
                    workspace_dir=workspace_dir,
                )
            else:
                compile_result = compile_bundle_workspace(
                    draft_updated,
                    repo_root=repo_root,
                    workspace_dir=workspace_dir,
                )
        if compile_result.errors:
            st.error("\n".join(compile_result.errors))
        else:
            st.success(f"Compiled to `{_as_relative(compile_result.compiled_path, repo_root)}`")
        with st.expander("Generated Overlay Files", expanded=True):
            for path in compile_result.generated_files:
                st.code(_as_relative(path, repo_root))

    if run_clicked and compile_result is not None:
        if compile_result.errors:
            return
        with st.spinner("Running compiled workspace..."):
            if compile_result.scenario_config is not None:
                result = run_scenario(config=compile_result.scenario_config, output_dir=artifacts_dir)
                st.success("Compiled scenario run complete.")
                st.write(f"Output directory: {result.output_dir}")
                if result.parsed_output and result.parsed_output.variables:
                    st.plotly_chart(
                        forecast_figure(
                            result.parsed_output,
                            variables=compile_result.scenario_config.track_variables,
                            title=f"Run output: {compile_result.scenario_config.name}",
                        ),
                        use_container_width=True,
                    )
            elif compile_result.bundle_config is not None:
                bundle_result = run_bundle(compile_result.bundle_config, output_dir=artifacts_dir)
                st.success("Compiled bundle run complete.")
                st.write(f"Variants run: {bundle_result.n_variants}")
                for entry in bundle_result.entries:
                    status = "ok" if entry.success else "failed"
                    st.write(f"`{entry.variant_name}`: {status}")
        st.session_state["runs"] = scan_artifacts(artifacts_dir)


def main() -> None:
    st.set_page_config(page_title="fp-wraptr New Run", page_icon=page_favicon(), layout="wide")
    common.render_sidebar_logo_toggle(width=56, height=56)
    common.render_page_title(
        "New Run",
        caption="Agent-first scenario authoring with advanced manual tools available on demand.",
    )

    repo_root = _repo_root()
    show_advanced = render_advanced_toggle(key="new_run_show_advanced")
    render_agent_handoff(
        title="Agent Handoff",
        prompt=(
            "Create or load a managed workspace from the relevant catalog entry, mutate cards or series via "
            "workspace tools, compile, run, and prepare a visualization payload for the dashboard."
        ),
    )
    if not show_advanced:
        st.info(
            "Advanced dashboard authoring is hidden by default. Use MCP workspace tools for scenario edits, "
            "then return here or to Compare/Run Panels to inspect results."
        )
        return

    mode = st.sidebar.radio("Mode", ["Authoring Workspace", "From YAML", "From scratch"], index=0)
    fp_home = st.sidebar.text_input("fp_home", value="FM")
    exe = FPExecutable(fp_home=Path(fp_home))
    if exe.check_available():
        st.sidebar.success("fp.exe is available")
    else:
        st.sidebar.error("fp.exe unavailable for this fp_home")
        st.info("If fp.exe is unavailable, the run uses a synthetic fallback when possible.")

    artifacts_dir = artifacts_dir_from_query()
    st.sidebar.markdown("---")

    if mode == "Authoring Workspace":
        _render_authoring_mode(artifacts_dir=artifacts_dir)
        return

    backend_choice = st.sidebar.selectbox(
        "Backend",
        options=["fpexe", "fppy", "both"],
        index=0,
        help="Choose execution engine. `both` runs parity (fp.exe + fppy) and stores parity artifacts.",
    )
    fppy_timeout = int(st.sidebar.number_input("fppy timeout (seconds)", value=2400, step=60))
    fppy_preset = st.sidebar.selectbox(
        "fppy EQ preset",
        options=["parity", "default"],
        index=0,
        help="`parity` enables FP-style solve semantics; `default` uses fppy defaults.",
    )
    fppy_num_threads = int(
        st.sidebar.number_input(
            "fppy threads",
            min_value=1,
            max_value=256,
            value=_default_thread_count(),
            step=1,
            help=(
                "Sets OMP/BLAS thread env vars for fppy "
                "(OMP_NUM_THREADS, OPENBLAS_NUM_THREADS, MKL_NUM_THREADS, "
                "NUMEXPR_NUM_THREADS, VECLIB_MAXIMUM_THREADS)."
            ),
        )
    )
    override_yaml_backend = False
    if mode == "From YAML":
        override_yaml_backend = st.sidebar.checkbox(
            "Override YAML backend",
            value=False,
            help="When enabled, the selected Backend replaces any backend specified in the YAML.",
        )

    uploaded = None
    selected_example: CatalogEntry | None = None
    selected_scratch_example: CatalogEntry | None = None
    scratch_methods = ["CHGSAMEPCT", "SAMEVALUE", "CHGSAMEABS"]
    try:
        catalog = load_scenario_catalog(repo_root=repo_root)
    except Exception as exc:
        catalog = None
        st.warning(f"Catalog unavailable; curated quick-picks are hidden: {exc}")
    scenario_entries = (
        catalog.for_surface("new_run", kind="scenario", public_only=True)
        if catalog is not None
        else []
    )
    scenario_options: list[CatalogEntry | None] = [None, *scenario_entries]

    if mode == "From YAML":
        st.subheader("Load scenario from YAML")
        uploaded = st.file_uploader("Upload YAML", type=["yaml", "yml"])
        if scenario_entries:
            selected_example = st.selectbox(
                "Curated scenario",
                options=scenario_options,
                format_func=lambda item: "" if item is None else item.label,
            )
        st.text_input(
            "Or load scenario YAML path",
            value=(
                _as_relative(selected_example.resolved_path(repo_root=repo_root), repo_root)
                if selected_example
                else ""
            ),
            key="new_run_yaml_path",
        )
    else:
        st.subheader("Create scenario from scratch")
        if scenario_entries:
            selected_scratch_example = st.selectbox(
                "Preload from example",
                options=scenario_options,
                format_func=lambda item: "" if item is None else item.label,
                key="scratch_example",
            )
            if st.button("Load from existing example", key="scratch_load_example"):
                if selected_scratch_example is None:
                    st.warning("Choose an example before loading.")
                else:
                    try:
                        template = load_scenario_config(
                            selected_scratch_example.resolved_path(repo_root=repo_root)
                        )
                    except Exception as exc:
                        st.error(f"Failed to preload example: {exc}")
                    else:
                        st.session_state["new_run_name"] = template.name
                        st.session_state["new_run_description"] = template.description
                        st.session_state["new_run_input_file"] = template.input_file
                        st.session_state["new_run_input_overlay_dir"] = (
                            str(template.input_overlay_dir) if template.input_overlay_dir else ""
                        )
                        st.session_state["new_run_forecast_start"] = template.forecast_start
                        st.session_state["new_run_forecast_end"] = template.forecast_end
                        st.session_state["new_run_track_variables"] = ", ".join(
                            template.track_variables
                        )
                        st.session_state["new_run_override_rows"] = min(
                            len(template.overrides), 10
                        )
                        for idx, (name_, override) in enumerate(template.overrides.items()):
                            if idx >= 10:
                                break
                            st.session_state[f"new_run_override_var_{idx}"] = name_
                            st.session_state[f"new_run_override_method_{idx}"] = override.method
                            st.session_state[f"new_run_override_value_{idx}"] = float(
                                override.value
                            )
                        st.success(f"Loaded scenario template: {template.name}")
                        st.rerun()

        defaults = {
            "name": st.session_state.get("new_run_name", "new_scenario"),
            "description": st.session_state.get("new_run_description", ""),
            "input_file": st.session_state.get("new_run_input_file", "fminput.txt"),
            "input_overlay_dir": st.session_state.get("new_run_input_overlay_dir", ""),
            "forecast_start": st.session_state.get("new_run_forecast_start", "2025.4"),
            "forecast_end": st.session_state.get("new_run_forecast_end", "2029.4"),
            "track_variables": st.session_state.get(
                "new_run_track_variables", "PCY, PCPF, UR, PIEF, GDPR"
            ),
            "override_rows": int(st.session_state.get("new_run_override_rows", 0)),
        }

        name = st.text_input("Scenario name", value=defaults["name"], key="new_run_name")
        description = st.text_input(
            "Description", value=defaults["description"], key="new_run_description"
        )
        input_file = st.text_input(
            "Input file",
            value=defaults["input_file"],
            key="new_run_input_file",
            help="Entry FP input script to stage into the run work directory.",
        )
        input_overlay_dir = st.text_input(
            "Input overlay dir",
            value=defaults["input_overlay_dir"],
            key="new_run_input_overlay_dir",
            help="Optional directory for scenario-specific include scripts such as PSE decks.",
        )
        forecast_start = st.text_input(
            "Forecast start", value=defaults["forecast_start"], key="new_run_forecast_start"
        )
        forecast_end = st.text_input(
            "Forecast end", value=defaults["forecast_end"], key="new_run_forecast_end"
        )
        track_variables = st.text_input(
            "Track variables (comma-separated)",
            value=defaults["track_variables"],
            key="new_run_track_variables",
        )
        override_rows = st.number_input(
            "Overrides",
            min_value=0,
            max_value=10,
            value=defaults["override_rows"],
            step=1,
            key="new_run_override_rows",
        )

        override_values: list[tuple[str, str, float]] = []
        duplicate_overrides: list[str] = []
        seen_variables: set[str] = set()
        for idx in range(int(override_rows)):
            c1, c2, c3 = st.columns(3)
            var_name = c1.text_input("Variable", key=f"new_run_override_var_{idx}")
            method_name = c2.selectbox(
                "Method",
                options=scratch_methods,
                key=f"new_run_override_method_{idx}",
            )
            value = c3.number_input(
                "Value",
                value=0.0,
                key=f"new_run_override_value_{idx}",
            )
            if var_name.strip():
                if var_name.strip() in seen_variables:
                    duplicate_overrides.append(var_name.strip())
                seen_variables.add(var_name.strip())
                override_values.append((var_name.strip(), method_name, value))

    config: ScenarioConfig | None = None
    config_error: str | None = None
    if mode == "From YAML":
        if uploaded is not None:
            try:
                config = _load_uploaded_config(uploaded)
            except ValidationError as exc:
                config_error = f"Validation failed: {exc}"
            except Exception as exc:
                config_error = str(exc) or "Uploaded file is not a valid YAML scenario."
        elif st.session_state.get("new_run_yaml_path"):
            try:
                config = load_scenario_config(
                    _resolve_yaml_path(st.session_state["new_run_yaml_path"], repo_root=repo_root)
                )
            except Exception as exc:
                config_error = f"Failed to load example: {exc}"
    else:
        if duplicate_overrides:
            st.warning(
                "Duplicate override variables found: "
                + ", ".join(sorted(set(duplicate_overrides)))
            )
        try:
            config = _build_scratch_config(
                name=name,
                description=description,
                fp_home=fp_home,
                input_file=input_file,
                input_overlay_dir=input_overlay_dir,
                forecast_start=forecast_start,
                forecast_end=forecast_end,
                track_variables=track_variables,
                override_values=override_values,
            )
        except ValidationError as exc:
            config_error = f"Validation failed: {exc}"

    if config is not None:
        config.fp_home = Path(fp_home)

    preflight: ScenarioInputPreflight | None = None
    if config is not None:
        preflight = preflight_scenario_input(config)

    if config_error:
        st.warning(config_error)
    elif config is not None and preflight is not None:
        _render_input_deck_readiness(config, preflight)
        render_scenario_cards(config, preflight)

    if st.button("Run Scenario"):
        if config is None:
            if config_error:
                st.error(config_error)
            else:
                st.info("Build or load a scenario first.")
            return

        if preflight is None:
            preflight = preflight_scenario_input(config)
        if preflight.error:
            st.error(preflight.error)
            return

        if mode == "From YAML" and uploaded is None and selected_example is None:
            st.info("Build or load a scenario first.")
            return

        config.fp_home = Path(fp_home)
        config.fppy = {
            **(config.fppy or {}),
            "num_threads": int(fppy_num_threads),
        }
        if override_yaml_backend or mode != "From YAML":
            config.backend = backend_choice
            config.fppy = {
                **(config.fppy or {}),
                "timeout_seconds": fppy_timeout,
                "eq_flags_preset": fppy_preset,
            }

        with st.spinner(f"Running scenario (backend={config.backend})..."):
            try:
                result = run_scenario(config=config, output_dir=artifacts_dir)
            except Exception as exc:
                st.error(f"Run failed: {exc}")
                return

        st.success("Scenario run complete.")
        st.page_link("pages/0_Run_Panels.py", label="View in Run Panels")
        st.write(f"Output directory: {result.output_dir}")
        if result.parsed_output and result.parsed_output.variables:
            st.plotly_chart(
                forecast_figure(
                    result.parsed_output,
                    variables=config.track_variables,
                    title=f"Run output: {config.name}",
                ),
                use_container_width=True,
            )
        st.session_state["runs"] = scan_artifacts(artifacts_dir)


if __name__ == "__main__":
    main()
