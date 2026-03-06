"""Search and explain model dictionary variables/equations."""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path

import streamlit as st

from fp_wraptr.dashboard import _common as common
from fp_wraptr.dashboard._common import artifacts_dir_from_query, page_favicon
from fp_wraptr.dashboard.artifacts import (
    RunArtifact,
    backend_name,
    existing_overlay_paths,
    recommended_overlay_path,
    scan_artifacts,
)
try:  # Backward-compatible import for environments with older artifacts module.
    from fp_wraptr.dashboard.artifacts import shared_extension_paths
except ImportError:  # pragma: no cover - compatibility fallback
    def shared_extension_paths(_run: RunArtifact | None) -> list[Path]:
        return []

from fp_wraptr.data import ModelDictionary
from fp_wraptr.data.dictionary_overlays import (
    load_dictionary_with_overlays,
    read_dictionary_overlay,
    write_dictionary_overlay,
)
from fp_wraptr.scenarios.input_tree import scan_input_tree_symbols


@st.cache_resource
def _load_dictionary(
    base_path_text: str,
    overlay_paths_text: tuple[str, ...],
    overlay_cache_keys: tuple[str, ...],
) -> ModelDictionary:
    _ = overlay_cache_keys
    base_path = Path(base_path_text) if base_path_text else None
    overlay_paths = [Path(text) for text in overlay_paths_text if str(text).strip()]
    return load_dictionary_with_overlays(base_path=base_path, overlay_paths=overlay_paths)


def _cache_key(path: Path | None) -> str:
    if path is None or not path.exists():
        return "missing"
    stat = path.stat()
    return f"{stat.st_size}:{stat.st_mtime_ns}"


def _latest_run(runs: list[RunArtifact]) -> RunArtifact | None:
    ordered = sorted(
        runs,
        key=lambda item: item.timestamp if item.timestamp else "00000000_000000",
        reverse=True,
    )
    return ordered[0] if ordered else None


def _sync_overlay_var_code_from_selection() -> None:
    selected = str(st.session_state.get("dictionary_overlay_selected_var", "") or "").strip()
    st.session_state["dictionary_overlay_var_code"] = selected.upper()


def _overlay_payload(overlay_path: Path) -> dict:
    raw = read_dictionary_overlay(overlay_path)
    return raw if isinstance(raw, dict) else {}


def _parse_path_list(raw_text: str) -> list[Path]:
    out: list[Path] = []
    for raw_line in str(raw_text or "").splitlines():
        text = raw_line.strip()
        if not text:
            continue
        out.append(Path(text))
    return out


def _dedupe_paths(paths: list[Path]) -> list[Path]:
    seen: set[Path] = set()
    out: list[Path] = []
    for path in paths:
        try:
            resolved = path.expanduser().resolve()
        except Exception:
            resolved = path
        if resolved in seen:
            continue
        seen.add(resolved)
        out.append(path)
    return out


def _scenario_overlay_candidates(paths: list[Path]) -> list[Path]:
    out: list[Path] = []
    for path in paths:
        if path.name == "dictionary_overlay.json" or path.parent.name == "dictionary_overlays":
            out.append(path)
    return out


def _equation_payload_to_csv(payload: dict) -> str:
    equation = payload.get("equation", {}) or {}
    cross_links = payload.get("cross_links", {}) or {}
    variable_to_equations = cross_links.get("variable_to_equations", {}) or {}

    rows: list[dict[str, str]] = []
    for variable in payload.get("variables", []):
        name = str(variable.get("name", ""))
        variable_link = variable_to_equations.get(name, {}) or {}
        used_in = variable_link.get("used_in_equations")
        rows.append({
            "equation_id": str(equation.get("id", "")),
            "equation_type": str(equation.get("type", "")),
            "equation_label": str(equation.get("label", "")),
            "lhs_expr": str(equation.get("lhs_expr", "")),
            "formula": str(equation.get("formula", "")),
            "variable_name": name,
            "role": str(variable.get("role", "")),
            "description": str(variable.get("description", "")),
            "description_source": str(variable.get("description_source", "")),
            "category": str(variable.get("category", "")),
            "defined_by_equation": str(variable_link.get("defined_by_equation", "")),
            "used_in_equations": ";".join(str(x) for x in (used_in or [])),
        })

    headers = [
        "equation_id",
        "equation_type",
        "equation_label",
        "lhs_expr",
        "formula",
        "variable_name",
        "role",
        "description",
        "description_source",
        "category",
        "defined_by_equation",
        "used_in_equations",
    ]
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=headers)
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue()


def main() -> None:
    st.set_page_config(page_title="fp-wraptr Dictionary", page_icon=page_favicon(), layout="wide")
    common.render_sidebar_logo_toggle(width=56, height=56)
    common.render_page_title(
        "Dictionary & Equation Explorer",
        caption="Search variables and equations, inspect meanings, and manage dictionary overlays.",
    )

    default_path = ""
    dictionary_path = st.sidebar.text_input(
        "Dictionary JSON path (optional)",
        value=default_path,
        help="Leave blank to use bundled dictionary.json",
    )

    st.sidebar.markdown("---")
    st.sidebar.subheader("Scenario Overlays")
    artifacts_dir = Path(st.session_state.get("artifacts_dir") or artifacts_dir_from_query())
    st.sidebar.caption(f"Artifacts: `{artifacts_dir}`")
    runs = scan_artifacts(artifacts_dir)
    overlay_run_options: list[RunArtifact | None] = [None] + runs
    default_run = _latest_run(runs)
    default_run_index = overlay_run_options.index(default_run) if default_run in overlay_run_options else 0
    selected_run = st.sidebar.selectbox(
        "Run (optional)",
        options=overlay_run_options,
        index=default_run_index,
        format_func=lambda item: "None"
        if item is None
        else f"{item.display_name} [{backend_name(item)}]",
    )

    auto_sync_overlay = st.sidebar.checkbox(
        "Auto-sync overlay path to run",
        value=True,
        help="When enabled, selecting a different run updates the overlay path to the recommended scenario-specific file.",
    )

    recommended_overlay = recommended_overlay_path(selected_run)
    existing_overlays = existing_overlay_paths(selected_run)
    existing_scenario_overlays = _scenario_overlay_candidates(existing_overlays)
    detected_shared_extensions = shared_extension_paths(selected_run)
    recommended_text = str(recommended_overlay) if recommended_overlay else ""
    selected_run_key = str(selected_run.run_dir) if selected_run is not None else ""
    previous_run_key = str(st.session_state.get("dictionary_overlay_selected_run_dir", ""))
    previous_recommended = str(st.session_state.get("dictionary_overlay_recommended_path", ""))
    current_overlay = str(st.session_state.get("dictionary_overlay_path", ""))

    # Keep overlays intuitive: when the selected run changes, update the overlay path
    # automatically unless the operator has explicitly chosen a different file.
    if previous_run_key != selected_run_key:
        if auto_sync_overlay or current_overlay in ("", previous_recommended):
            st.session_state["dictionary_overlay_path"] = recommended_text
        st.session_state["dictionary_overlay_selected_run_dir"] = selected_run_key
        st.session_state["dictionary_overlay_recommended_path"] = recommended_text

    if recommended_text:
        st.sidebar.caption(f"Recommended: `{recommended_text}`")
        if existing_scenario_overlays:
            st.sidebar.caption("Detected existing overlays:")
            for path in existing_scenario_overlays[:3]:
                st.sidebar.caption(f"- `{path}`")
            if len(existing_scenario_overlays) > 3:
                st.sidebar.caption(f"...and {len(existing_scenario_overlays) - 3} more")
            if st.sidebar.button("Use detected overlay"):
                st.session_state["dictionary_overlay_path"] = str(existing_scenario_overlays[0])
                st.session_state["dictionary_overlay_recommended_path"] = str(existing_scenario_overlays[0])
                st.rerun()
        else:
            st.sidebar.caption("No existing overlay file detected for this run yet.")
        if st.sidebar.button("Sync overlay path to selected run"):
            st.session_state["dictionary_overlay_path"] = recommended_text
            st.session_state["dictionary_overlay_recommended_path"] = recommended_text
            st.rerun()

    overlay_path_text = st.sidebar.text_input(
        "Scenario overlay JSON path (optional)",
        key="dictionary_overlay_path",
        help="Scenario-specific dictionary overrides (variables/units/equations). Applied after shared extensions.",
    )
    overlay_path = Path(overlay_path_text) if overlay_path_text else None

    auto_include_shared_extensions = st.sidebar.checkbox(
        "Auto-include shared dictionary extensions",
        value=True,
        help=(
            "Loads all `*.json` files from `<input_overlay_dir>/dictionary_extensions/` and "
            "`projects_local/dictionary_extensions/` before applying the scenario overlay."
        ),
    )
    if detected_shared_extensions:
        st.sidebar.caption("Detected shared extensions:")
        for path in detected_shared_extensions[:4]:
            st.sidebar.caption(f"- `{path}`")
        if len(detected_shared_extensions) > 4:
            st.sidebar.caption(f"...and {len(detected_shared_extensions) - 4} more")

    extra_extension_paths_text = st.sidebar.text_area(
        "Extra extension JSON paths (optional)",
        value=str(st.session_state.get("dictionary_extra_extension_paths", "") or ""),
        key="dictionary_extra_extension_paths",
        height=90,
        help="One path per line. Applied before the scenario overlay.",
    )
    extra_extension_paths = _parse_path_list(extra_extension_paths_text)

    effective_overlay_paths: list[Path] = []
    if auto_include_shared_extensions:
        effective_overlay_paths.extend(detected_shared_extensions)
    effective_overlay_paths.extend(extra_extension_paths)
    if overlay_path is not None:
        effective_overlay_paths.append(overlay_path)
    effective_overlay_paths = _dedupe_paths(effective_overlay_paths)

    try:
        overlay_keys = tuple(_cache_key(path) for path in effective_overlay_paths)
        dictionary = _load_dictionary(
            dictionary_path,
            tuple(str(path) for path in effective_overlay_paths),
            overlay_keys,
        )
    except Exception as exc:
        st.error(f"Failed to load dictionary: {exc}")
        return

    st.caption(
        f"Model version: {dictionary.model_version} | "
        f"{len(dictionary.variables)} variables | {len(dictionary.equations)} equations"
    )
    st.caption("Base stock dictionary is always loaded first; extensions/overlays only patch fields.")
    if effective_overlay_paths:
        st.caption(
            "Applied extensions/overlays (in order): "
            + ", ".join(f"`{path}`" for path in effective_overlay_paths[:4])
            + (f" (+{len(effective_overlay_paths) - 4} more)" if len(effective_overlay_paths) > 4 else "")
        )

    st.markdown("---")
    st.subheader("Variable Metadata Editor")
    st.caption("Edit `short_name`, `description`, and `units` for any variable via overlay.")
    if overlay_path is None:
        st.info("Set `Scenario overlay JSON path` in the sidebar to enable editing.")
    else:
        st.write(f"Overlay path: `{overlay_path}` ({'exists' if overlay_path.exists() else 'missing'})")
        if not overlay_path.exists():
            if st.button("Create overlay file", key="dict_meta_create_overlay"):
                write_dictionary_overlay(
                    overlay_path,
                    {
                        "meta": {"generated_from_run": getattr(selected_run, "display_name", "")},
                        "variables": {},
                        "equations": {},
                    },
                )
                st.success(f"Created overlay file: {overlay_path}")
                st.rerun()

        filter_text = st.text_input(
            "Filter variables",
            value="",
            key="dict_meta_filter",
            placeholder="Type code or description text",
        )
        all_codes = sorted(dictionary.variables.keys())
        if filter_text.strip():
            needle = filter_text.strip().upper()
            filtered_codes = [
                code
                for code in all_codes
                if needle in code
                or needle in str(getattr(dictionary.get_variable(code), "short_name", "") or "").upper()
                or needle in str(getattr(dictionary.get_variable(code), "description", "") or "").upper()
            ]
        else:
            filtered_codes = all_codes

        if not filtered_codes:
            st.warning("No variables matched the filter.")
        else:
            selected_code = st.selectbox(
                "Variable code",
                options=filtered_codes,
                key="dict_meta_selected_var",
            )
            merged_var = dictionary.get_variable(str(selected_code).upper())
            current_desc = str(getattr(merged_var, "description", "") or "") if merged_var else ""
            current_units = str(getattr(merged_var, "units", "") or "") if merged_var else ""
            current_short = str(getattr(merged_var, "short_name", "") or "") if merged_var else ""

            with st.form(key=f"dict_meta_form_{selected_code}"):
                edit_cols = st.columns(2)
                new_short = edit_cols[0].text_input("Short name", value=current_short)
                new_units = edit_cols[1].text_input("Units", value=current_units)
                new_desc = st.text_area("Description", value=current_desc, height=120)
                save_meta = st.form_submit_button("Save variable fields")

            if save_meta:
                payload_overlay = _overlay_payload(overlay_path)
                payload_overlay.setdefault("variables", {})
                payload_overlay.setdefault("equations", {})
                overlay_vars = (
                    payload_overlay.get("variables", {})
                    if isinstance(payload_overlay.get("variables"), dict)
                    else {}
                )
                code = str(selected_code).upper()
                overlay_vars[code] = {
                    **(overlay_vars.get(code, {}) or {}),
                    "description": new_desc,
                    "units": new_units,
                    "short_name": new_short,
                }
                payload_overlay["variables"] = overlay_vars
                payload_overlay.setdefault("meta", {})["updated_from_dashboard"] = True
                if selected_run is not None:
                    payload_overlay["meta"]["generated_from_run"] = selected_run.display_name
                write_dictionary_overlay(overlay_path, payload_overlay)
                st.success(f"Saved metadata override for `{code}`.")
                st.rerun()

    st.markdown("---")
    st.subheader("Search")
    query = st.text_input(
        "Search query",
        value="",
        placeholder="Try: eq 82, GDP, UR in equation 30, consumer expenditures",
    )
    limit = st.number_input("Max matches per section", min_value=1, max_value=50, value=10)

    if query.strip():
        payload = dictionary.query(query, limit=int(limit))

        st.subheader("Variable matches")
        _var_matches = [
            {
                "name": (m.get("variable") or {}).get("name"),
                "score": m.get("score"),
                "reason": m.get("reason"),
                "short_name": (m.get("variable") or {}).get("short_name", ""),
                "units": (m.get("variable") or {}).get("units", ""),
                "description": (m.get("variable") or {}).get("description"),
                "description_source": (m.get("variable") or {}).get("description_source"),
                "defined_by_equation": (m.get("variable") or {}).get("defined_by_equation"),
            }
            for m in payload.get("variable_matches", [])
        ]
        st.dataframe(_var_matches, use_container_width=True, height=min(400, 35 * len(_var_matches) + 40))

        st.subheader("Equation matches")
        st.dataframe(
            [
                {
                    "id": (m.get("equation") or {}).get("id"),
                    "score": m.get("score"),
                    "reason": m.get("reason"),
                    "label": (m.get("equation") or {}).get("label"),
                    "lhs_expr": (m.get("equation") or {}).get("lhs_expr"),
                }
                for m in payload.get("equation_matches", [])
            ],
            use_container_width=True,
        )

        st.subheader("Drill-down from search results")
        eq_options = [(m.get("equation") or {}).get("id") for m in payload.get("equation_matches", [])]
        eq_options = [x for x in eq_options if x is not None]
        var_options = [(m.get("variable") or {}).get("name") for m in payload.get("variable_matches", [])]
        var_options = [str(x).upper() for x in var_options if str(x or "").strip()]

        left, right = st.columns(2)
        selected_var = left.selectbox(
            "Variable match",
            options=var_options,
            index=None,
            placeholder="Select variable from search results",
        )
        selected_eq = right.selectbox(
            "Equation match",
            options=eq_options,
            index=None,
            placeholder="Select equation from search results",
        )

        copy_cols = st.columns(2)
        if selected_var is not None:
            copy_cols[0].caption("Copy variable code")
            copy_cols[0].code(str(selected_var))
        if selected_eq is not None:
            copy_cols[1].caption("Copy equation ID")
            copy_cols[1].code(str(selected_eq))

        if selected_var is not None:
            variable_record = dictionary.describe(str(selected_var).upper())
            if variable_record is not None:
                st.markdown(f"**Variable {selected_var} details**")
                st.json(variable_record)

                st.markdown("**Edit variable metadata (definition/units/short name)**")
                if overlay_path is None:
                    st.info("Set an Overlay JSON path (sidebar) to save edits.")
                else:
                    if not overlay_path.exists():
                        if st.button("Create overlay file for edits", key="dict_create_overlay_for_edit"):
                            write_dictionary_overlay(
                                overlay_path,
                                {
                                    "meta": {"generated_from_run": getattr(selected_run, "display_name", "")},
                                    "variables": {},
                                    "equations": {},
                                },
                            )
                            st.success(f"Created overlay file: {overlay_path}")
                            st.rerun()

                    merged_var = dictionary.get_variable(str(selected_var).upper())
                    current_desc = str(getattr(merged_var, "description", "") or "") if merged_var else ""
                    current_units = str(getattr(merged_var, "units", "") or "") if merged_var else ""
                    current_short = str(getattr(merged_var, "short_name", "") or "") if merged_var else ""
                    new_desc = st.text_area(
                        "Definition",
                        value=current_desc,
                        height=120,
                        key="dict_edit_desc",
                    )
                    edit_cols = st.columns(2)
                    new_units = edit_cols[0].text_input(
                        "Units",
                        value=current_units,
                        key="dict_edit_units",
                        help="Used for chart axis titles.",
                    )
                    new_short = edit_cols[1].text_input(
                        "Short name",
                        value=current_short,
                        key="dict_edit_short_name",
                        help="Used for chart titles (falls back to variable code when blank).",
                    )
                    if st.button("Save metadata override", key="dict_save_metadata_override"):
                        payload_overlay = _overlay_payload(overlay_path)
                        payload_overlay.setdefault("variables", {})
                        payload_overlay.setdefault("equations", {})
                        overlay_vars = (
                            payload_overlay.get("variables", {})
                            if isinstance(payload_overlay.get("variables"), dict)
                            else {}
                        )
                        code = str(selected_var).upper()
                        overlay_vars[code] = {
                            **(overlay_vars.get(code, {}) or {}),
                            "description": new_desc,
                            "units": new_units,
                            "short_name": new_short,
                        }
                        payload_overlay["variables"] = overlay_vars
                        payload_overlay.setdefault("meta", {})["updated_from_dashboard"] = True
                        if selected_run is not None:
                            payload_overlay["meta"]["generated_from_run"] = selected_run.display_name
                        write_dictionary_overlay(overlay_path, payload_overlay)
                        st.success("Saved override.")
                        st.rerun()

        if selected_eq is not None:
            explained_from_match = dictionary.explain_equation(int(selected_eq))
            if explained_from_match is not None:
                st.markdown(f"**Equation {selected_eq} details**")
                st.json(explained_from_match["equation"])
                st.dataframe(explained_from_match["variables"], use_container_width=True)

    # Overlay editor (optional)
    if selected_run is not None and overlay_path is not None:
        st.markdown("---")
        st.subheader("Scenario Overlay Editor")
        st.write(f"Selected run: `{selected_run.display_name}`")
        st.write(f"Overlay path: `{overlay_path}` ({'exists' if overlay_path.exists() else 'missing'})")
        if not overlay_path.exists():
            if st.button("Create empty overlay file"):
                write_dictionary_overlay(
                    overlay_path,
                    {
                        "meta": {"generated_from_run": selected_run.display_name},
                        "variables": {},
                        "equations": {},
                    },
                )
                st.success(f"Created overlay file: {overlay_path}")
                st.rerun()

        try:
            base_dictionary = ModelDictionary.load(Path(dictionary_path) if dictionary_path else None)
        except Exception as exc:
            st.warning(f"Unable to load base dictionary for diffing: {exc}")
            base_dictionary = dictionary

        scan_result = None
        scan_state = st.session_state.get("dictionary_input_scan")
        if isinstance(scan_state, dict) and scan_state.get("run_dir") == str(selected_run.run_dir):
            scan_result = scan_state.get("result")

        scan_cols = st.columns(2)
        if scan_cols[0].button("Scan scenario input tree"):
            if selected_run.config is None:
                st.error("Selected run is missing scenario config; cannot scan input tree.")
            else:
                with st.spinner("Scanning scenario input tree for symbols..."):
                    try:
                        result = scan_input_tree_symbols(
                            entry_input_file=str(selected_run.config.input_file),
                            overlay_dir=getattr(selected_run.config, "input_overlay_dir", None),
                            fp_home=Path(selected_run.config.fp_home),
                        )
                    except Exception as exc:
                        st.error(f"Input-tree scan failed: {exc}")
                    else:
                        st.session_state["dictionary_input_scan"] = {
                            "run_dir": str(selected_run.run_dir),
                            "result": result,
                        }
                        scan_result = result
                        st.rerun()

        if scan_result is not None:
            scanned_vars = set(getattr(scan_result, "variables", []) or [])
            missing_from_base = sorted(v for v in scanned_vars if v not in base_dictionary.variables)
            missing_from_merged = sorted(v for v in scanned_vars if v not in dictionary.variables)

            scan_cols[1].metric("Symbols scanned", len(scanned_vars))
            if missing_from_base:
                st.info(f"{len(missing_from_base)} symbols missing from base dictionary.")
            if missing_from_merged:
                st.warning(f"{len(missing_from_merged)} symbols still missing after overlay.")

            with st.expander("Missing-from-base symbols"):
                st.code("\n".join(missing_from_base) if missing_from_base else "(none)")

            if st.button("Create/update overlay stubs for missing symbols"):
                payload = _overlay_payload(overlay_path)
                payload.setdefault("meta", {})
                payload.setdefault("variables", {})
                var_overrides = payload["variables"] if isinstance(payload.get("variables"), dict) else {}
                added: list[str] = []
                for code in missing_from_base:
                    if code in var_overrides:
                        continue
                    var_overrides[code] = {"description": "", "units": "", "short_name": ""}
                    added.append(code)
                payload["variables"] = var_overrides
                payload["meta"]["generated_from_run"] = selected_run.display_name
                write_dictionary_overlay(overlay_path, payload)
                st.success(f"Overlay updated ({len(added)} new stubs).")
                st.rerun()

        payload = _overlay_payload(overlay_path)
        payload.setdefault("variables", {})
        payload.setdefault("equations", {})

        tab_vars, tab_eqs = st.tabs(["Variables", "Equations"])
        with tab_vars:
            overlay_vars = payload.get("variables", {}) if isinstance(payload.get("variables"), dict) else {}
            candidates = sorted({*overlay_vars.keys(), *(scan_result.variables if scan_result else [])})
            add_cols = st.columns([2, 1])
            new_code = add_cols[0].text_input(
                "Add/edit variable code",
                value=str(st.session_state.get("dictionary_overlay_var_code", "") or "").strip(),
                placeholder="e.g. JGJ",
                help="Type a variable code to create/edit an overlay record (even if it is missing from base dictionary).",
                key="dictionary_overlay_var_code",
            )
            if add_cols[1].button("Add stub"):
                code = str(new_code).strip().upper()
                if not code:
                    st.warning("Enter a variable code first.")
                else:
                    payload = _overlay_payload(overlay_path)
                    payload.setdefault("variables", {})
                    overlay_vars = payload.get("variables", {}) if isinstance(payload.get("variables"), dict) else {}
                    overlay_vars.setdefault(code, {"description": "", "units": "", "short_name": ""})
                    payload["variables"] = overlay_vars
                    payload.setdefault("meta", {})["updated_from_dashboard"] = True
                    write_dictionary_overlay(overlay_path, payload)
                    st.success(f"Stub ready: {code}")
                    st.rerun()

            # Candidate list for pickers (overlay vars + scanned vars).
            overlay_vars = payload.get("variables", {}) if isinstance(payload.get("variables"), dict) else {}
            candidates = sorted({*overlay_vars.keys(), *(scan_result.variables if scan_result else [])})
            if not candidates:
                st.info(
                    "No overlay variables yet. Add a stub above (or scan the scenario input tree, then create stubs)."
                )
            else:
                # Prefer whatever the user typed, if present.
                preferred = str(new_code).strip().upper()
                idx = candidates.index(preferred) if preferred in candidates else 0
                current_selected = str(
                    st.session_state.get("dictionary_overlay_selected_var", "") or ""
                ).strip().upper()
                if current_selected not in candidates:
                    st.session_state["dictionary_overlay_selected_var"] = candidates[idx]
                var_code = st.selectbox(
                    "Variable",
                    options=candidates,
                    index=idx,
                    key="dictionary_overlay_selected_var",
                    on_change=_sync_overlay_var_code_from_selection,
                )
                base = base_dictionary.describe(str(var_code).upper())
                if base is not None:
                    st.caption("Base dictionary record")
                    st.json(base)
                patch = overlay_vars.get(str(var_code).upper(), {}) if isinstance(overlay_vars, dict) else {}
                current_desc = str(patch.get("description", "") or "")
                current_units = str(patch.get("units", "") or "")
                current_short = str(patch.get("short_name", "") or "")
                new_desc = st.text_area("Overlay description", value=current_desc, height=120)
                edit_cols = st.columns(2)
                new_units = edit_cols[0].text_input("Overlay units", value=current_units)
                new_short = edit_cols[1].text_input("Overlay short name", value=current_short)
                if st.button("Save variable override"):
                    overlay_vars = payload.get("variables", {}) if isinstance(payload.get("variables"), dict) else {}
                    overlay_vars[str(var_code).upper()] = {
                        **(overlay_vars.get(str(var_code).upper(), {}) or {}),
                        "description": new_desc,
                        "units": new_units,
                        "short_name": new_short,
                    }
                    payload["variables"] = overlay_vars
                    payload.setdefault("meta", {})["updated_from_dashboard"] = True
                    write_dictionary_overlay(overlay_path, payload)
                    st.success("Saved.")
                    st.rerun()


        with tab_eqs:
            overlay_eqs = payload.get("equations", {}) if isinstance(payload.get("equations"), dict) else {}
            existing_ids = []
            for raw_id in overlay_eqs.keys():
                try:
                    existing_ids.append(int(str(raw_id)))
                except (TypeError, ValueError):
                    continue
            existing_ids = sorted(set(existing_ids))

            eq_id_text = st.text_input(
                "Equation id (overlay)",
                value=str(existing_ids[0]) if existing_ids else "",
                placeholder="82",
            )
            if eq_id_text.strip():
                try:
                    eq_id = int(eq_id_text.strip())
                except ValueError:
                    st.warning("Equation id must be an integer.")
                else:
                    base_eq = base_dictionary.get_equation(eq_id)
                    if base_eq is not None:
                        st.caption("Base dictionary record")
                        st.json(base_eq.model_dump())
                    patch = overlay_eqs.get(str(eq_id), {}) or overlay_eqs.get(eq_id, {}) or {}
                    patch = patch if isinstance(patch, dict) else {}
                    new_label = st.text_input("Overlay label", value=str(patch.get("label", "") or ""))
                    new_formula = st.text_area("Overlay formula", value=str(patch.get("formula", "") or ""), height=120)
                    if st.button("Save equation override"):
                        overlay_eqs = payload.get("equations", {}) if isinstance(payload.get("equations"), dict) else {}
                        overlay_eqs[str(eq_id)] = {
                            **(overlay_eqs.get(str(eq_id), {}) or {}),
                            "label": new_label,
                            "formula": new_formula,
                        }
                        payload["equations"] = overlay_eqs
                        payload.setdefault("meta", {})["updated_from_dashboard"] = True
                        write_dictionary_overlay(overlay_path, payload)
                        st.success("Saved.")
                        st.rerun()

    st.markdown("---")
    st.subheader("Equation explainer")
    eq_value = st.text_input("Equation id", value="", placeholder="82")
    if eq_value.strip():
        try:
            eq_id = int(eq_value.strip())
        except ValueError:
            st.warning("Equation id must be an integer.")
            return
        explained = dictionary.explain_equation(eq_id)
        if explained is None:
            st.info(f"Equation {eq_id} not found.")
            return

        explain_download_cols = st.columns(2)
        explain_download_cols[0].download_button(
            "Download equation JSON",
            data=json.dumps(explained, indent=2, default=str),
            file_name=f"equation_{eq_id}.json",
            mime="application/json",
            key="download_dictionary_equation_json",
        )
        explain_download_cols[1].download_button(
            "Download equation CSV",
            data=_equation_payload_to_csv(explained),
            file_name=f"equation_{eq_id}.csv",
            mime="text/csv",
            key="download_dictionary_equation_csv",
        )

        st.json(explained["equation"])
        st.dataframe(explained["variables"], use_container_width=True)


if __name__ == "__main__":
    main()
