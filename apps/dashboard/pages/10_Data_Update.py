"""Update model inputs (fmdata.txt) from FRED and write a new model bundle."""

from __future__ import annotations

import importlib.util
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import streamlit as st

from fp_wraptr.dashboard import _common as common
from fp_wraptr.dashboard._common import page_favicon

SUPPORTED_SOURCES = ("fred", "bea", "bls")
MAX_LOG_CHARS = 12000
DEFAULT_FAIR_BUNDLE_URL = "https://fairmodel.econ.yale.edu/fp/FMFP.ZIP"
SOURCE_REQUIREMENTS = {
    "fred": ("FRED_API_KEY", "fredapi"),
    "bea": ("BEA_API_KEY", None),
    "bls": ("BLS_API_KEY", None),
}


def _is_valid_end_period(end_period: str) -> bool:
    value = end_period.strip()
    if not value:
        return False
    match = re.fullmatch(r"(\d{4})\.(\d)", value)
    if not match:
        return False
    quarter = int(match.group(2))
    return 1 <= quarter <= 4


def _module_available(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def _default_out_dir() -> str:
    date = datetime.now().strftime("%Y-%m-%d")
    return str(Path("artifacts") / "model_updates" / f"{date}_dashboard")


def _source_mapping_is_eligible(entry: object, enabled_sources: set[str]) -> bool:
    source = str(getattr(entry, "source", "")).strip().lower()
    if source not in enabled_sources:
        return False
    if source == "fred":
        return bool(getattr(entry, "series_id", "") or getattr(entry, "fred_fallback", ""))
    if source == "bea":
        try:
            bea_line = int(getattr(entry, "bea_line", 0) or 0)
        except (TypeError, ValueError):
            bea_line = 0
        return bool(getattr(entry, "bea_table", "") and bea_line > 0)
    if source == "bls":
        return bool(getattr(entry, "series_id", ""))
    return False


def _load_mapped_variables(model_dir: Path, sources: list[str]) -> tuple[list[str], str | None]:
    try:
        from fp_wraptr.data.source_map import load_source_map
        from fp_wraptr.io.input_parser import parse_fm_data
    except Exception as exc:
        return [], f"Failed to import mapping helpers: {exc}"

    fmdata_path = model_dir / "fmdata.txt"
    if not fmdata_path.exists():
        return [], f"Missing fmdata.txt: {fmdata_path}"

    try:
        source_map = load_source_map()
        parsed = parse_fm_data(fmdata_path)
    except Exception as exc:
        return [], str(exc)

    fm_vars = sorted((parsed.get("series") or {}).keys())
    enabled_sources = {str(item).strip().lower() for item in sources if str(item).strip()}
    mapped = [
        name
        for name in fm_vars
        if (entry := source_map.get(name)) is not None
        and _source_mapping_is_eligible(entry, enabled_sources)
    ]
    return mapped, None


def _local_markdown_link(path: Path) -> str:
    try:
        uri = path.resolve().as_uri()
    except ValueError:
        return f"`{path}`"
    return f"[`{path}`]({uri})"


def _failure_hints(stdout_text: str, stderr_text: str) -> list[str]:
    text = f"{stdout_text}\n{stderr_text}".lower()
    hints: list[str] = []

    if "fred_api_key environment variable not set" in text:
        hints.append(
            "Missing FRED credentials: set the `FRED_API_KEY` environment variable before running."
        )
    if "fredapi is required" in text or "no module named 'fredapi'" in text:
        hints.append(
            "Missing dependency: install the FRED extras (`pip install fp-wraptr[fred]`) so `fredapi` is available."
        )
    if "bea_api_key environment variable not set" in text:
        hints.append(
            "Missing BEA credentials: set the `BEA_API_KEY` environment variable before running BEA source updates."
        )
    if "bls_api_key environment variable not set" in text:
        hints.append(
            "Missing BLS credentials: set the `BLS_API_KEY` environment variable before running BLS source updates."
        )
    if (
        "no variables with fred series mappings were eligible for update" in text
        or "no variables with eligible source mappings were eligible for update" in text
        or "missing from source-map" in text
        or "source map load failed" in text
    ):
        hints.append(
            "Source-map issue: verify `source_map.yaml` has valid source mappings for the selected variables."
        )
    if "bea request failed:" in text or "bls request failed:" in text or "read timeout" in text:
        hints.append(
            "Network/API issue: external source request failed; retry after checking connectivity and API status."
        )
    if "missing values in extended sample" in text or "allow-carry-forward" in text:
        hints.append(
            "Extend-sample gap: source data did not cover all new periods; retry with `allow_carry_forward` or reduce the end period."
        )
    if "invalid period" in text or "expected yyyy.q" in text:
        hints.append("Invalid period format: use `YYYY.Q` (for example `2025.4`).")

    deduped: list[str] = []
    seen: set[str] = set()
    for item in hints:
        if item not in seen:
            deduped.append(item)
            seen.add(item)
    return deduped


def _truncate_log(text: str, *, max_chars: int = MAX_LOG_CHARS) -> tuple[str, int]:
    if len(text) <= max_chars:
        return text, 0
    return text[:max_chars], len(text) - max_chars


def _normalize_text_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    items: list[str] = []
    seen: set[str] = set()
    for raw in value:
        text = str(raw).strip()
        if not text or text in seen:
            continue
        items.append(text)
        seen.add(text)
    return items


def _build_preflight_checks(
    model_dir: str,
    end_period: str,
    selected_sources: list[str],
) -> list[tuple[str, bool]]:
    checks: list[tuple[str, bool]] = []
    model_path = Path(model_dir)
    checks.append(("model_dir exists", model_path.exists() and model_path.is_dir()))
    checks.append(("model_dir contains fmdata.txt", (model_path / "fmdata.txt").exists()))
    checks.append(("end period is YYYY.Q", _is_valid_end_period(end_period)))
    normalized_sources = [str(item).strip().lower() for item in selected_sources if str(item).strip()]
    checks.append(("At least one source selected", bool(normalized_sources)))
    if not normalized_sources:
        return checks
    for source in normalized_sources:
        if not source:
            continue
        if source not in SOURCE_REQUIREMENTS:
            checks.append((f"source '{source}' is supported", False))
            continue
        env_var, module_name = SOURCE_REQUIREMENTS[source]
        checks.append((f"{env_var} is set", bool(os.environ.get(env_var))))
        checks.append(
            (f"{source} dependencies installed", not bool(module_name) or _module_available(module_name))
        )
    return checks


def _extract_keyboard_patch_targets(payload: dict[str, object]) -> tuple[list[str], list[str]]:
    patch = payload.get("fminput_keyboard_patch")
    if not isinstance(patch, dict):
        return [], []
    added = _normalize_text_list(patch.get("added"))
    already_present = _normalize_text_list(patch.get("already_present"))
    return added, already_present


def _extract_top_diff_variables(summary: dict[str, object], *, max_items: int = 8) -> list[str]:
    rows = summary.get("top_first_diffs")
    if not isinstance(rows, list):
        return []
    vars_seen: set[str] = set()
    ordered_vars: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("variable", "")).strip()
        if not name or name in vars_seen:
            continue
        ordered_vars.append(name)
        vars_seen.add(name)
        if len(ordered_vars) >= max(0, int(max_items)):
            break
    return ordered_vars


def _safe_int(value: object, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _should_show_gate_failed_caveat(summary: dict[str, object]) -> bool:
    status = str(summary.get("status", "")).strip().lower()
    hard_fail_cell_count = _safe_int(summary.get("hard_fail_cell_count"), default=0)
    return status == "gate_failed" and hard_fail_cell_count == 0


def _build_update_command(
    *,
    model_dir: str,
    out_dir: str,
    end_period: str,
    cache_dir: str,
    sources: list[str],
    selected_vars: list[str],
    replace_history: bool,
    extend_sample: bool,
    allow_carry_forward: bool,
    use_official_bundle: bool = False,
    official_bundle_url: str = DEFAULT_FAIR_BUNDLE_URL,
    base_dir: str = "",
) -> list[str]:
    cmd = [
        sys.executable,
        "-m",
        "fp_wraptr.cli",
        "data",
        "update-fred",
        "--model-dir",
        model_dir,
        "--out-dir",
        out_dir,
        "--end",
        end_period,
    ]
    for source_name in sources:
        cmd.extend(["--sources", source_name])
    if cache_dir.strip():
        cmd.extend(["--cache-dir", cache_dir.strip()])
    if replace_history:
        cmd.append("--replace-history")
    if extend_sample:
        cmd.append("--extend-sample")
    if allow_carry_forward:
        cmd.append("--allow-carry-forward")
    if use_official_bundle:
        cmd.append("--from-official-bundle")
        cmd.extend(["--official-bundle-url", official_bundle_url.strip() or DEFAULT_FAIR_BUNDLE_URL])
        if base_dir.strip():
            cmd.extend(["--base-dir", base_dir.strip()])
    for var_name in selected_vars:
        cmd.extend(["--variables", var_name])
    return cmd


def _build_parity_smoke_command(*, scenario_yaml: Path, output_dir: Path) -> list[str]:
    return [
        sys.executable,
        "-m",
        "fp_wraptr.cli",
        "parity",
        str(scenario_yaml),
        "--with-drift",
        "--output-dir",
        str(output_dir),
    ]


def _find_latest_parity_report(output_dir: Path) -> Path | None:
    if not output_dir.exists():
        return None
    candidates = sorted(
        output_dir.glob("*/parity_report.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _load_parity_report_summary(report_path: Path) -> dict[str, object]:
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    pabev = payload.get("pabev_detail") if isinstance(payload.get("pabev_detail"), dict) else {}
    return {
        "status": payload.get("status", "unknown"),
        "exit_code": payload.get("exit_code", "n/a"),
        "hard_fail_cell_count": pabev.get("hard_fail_cell_count", 0),
        "max_abs_diff": pabev.get("max_abs_diff", 0.0),
        "top_first_diffs": pabev.get("top_first_diffs", []),
    }


def _fp_exe_path(bundle_dir: Path) -> Path:
    return Path(bundle_dir) / "fp.exe"


def _fp_exe_check(bundle_dir: Path) -> tuple[bool, str]:
    fp_exe = _fp_exe_path(bundle_dir)
    return fp_exe.is_file(), str(fp_exe)


def _render_report_summary(payload: dict) -> None:
    merge = payload.get("fmdata_merge") if isinstance(payload.get("fmdata_merge"), dict) else {}

    metric_cols = st.columns(4)
    metric_cols[0].metric("Selected vars", int(payload.get("selected_variable_count", 0)))
    metric_cols[1].metric("Normalized vars", int(payload.get("normalized_variable_count", 0)))
    metric_cols[2].metric("Updated cells", int(merge.get("updated_cells", 0)))
    metric_cols[3].metric("Missing cells", int(merge.get("missing_cells", 0)))

    st.caption(
        "Sample range: "
        f"{payload.get('sample_start', 'n/a')} -> {payload.get('sample_end_after', 'n/a')} "
        f"(before: {payload.get('sample_end_before', 'n/a')})"
    )
    sample_end_after = str(payload.get("sample_end_after", "")).strip()
    fminput_load_end = str(payload.get("fminput_fmdata_load_end", "")).strip()
    if sample_end_after or fminput_load_end:
        st.caption(
            "fminput LOADDATA end: "
            f"{fminput_load_end or 'n/a'}"
        )
    if (
        sample_end_after
        and fminput_load_end
        and sample_end_after != fminput_load_end
    ):
        st.warning(
            "fmdata sample end and fminput LOADDATA end differ. fp.exe may ignore the extra "
            "history quarter until fminput is updated."
        )

    rec_start = str(payload.get("recommended_forecast_start", "")).strip()
    rec_end = str(payload.get("recommended_forecast_end", "")).strip()
    if rec_start or rec_end:
        st.info(
            "Recommended forecast window: "
            f"{rec_start or 'n/a'}..{rec_end or 'n/a'}"
        )

    keyboard_patch = payload.get("fminput_keyboard_patch")
    if isinstance(keyboard_patch, dict):
        added, already_present = _extract_keyboard_patch_targets(payload)
        added_text = ", ".join(f"`{name}`" for name in added) if added else "none"
        present_text = ", ".join(f"`{name}`" for name in already_present) if already_present else "none"
        st.markdown("**fminput KEYBOARD augmentation**")
        st.caption(f"Added targets: {added_text}")
        st.caption(f"Already present: {present_text}")

    templates = payload.get("scenario_templates")
    if isinstance(templates, dict):
        template_rows: list[tuple[str, str]] = []
        for key in ("baseline_yaml", "baseline_smoke_yaml"):
            raw = str(templates.get(key, "")).strip()
            if raw:
                template_rows.append((key, _local_markdown_link(Path(raw))))
        if template_rows:
            st.markdown("**Scenario templates**")
            for label, link in template_rows:
                st.markdown(f"- `{label}`: {link}")

    st.warning(
        "Repo `examples/*.yaml` are pinned to the baseline FM sample window. "
        "Use the recommended window above (or the generated scenario templates) when running against an updated bundle."
    )
    st.json(payload, expanded=False)


def main() -> None:
    st.set_page_config(page_title="fp-wraptr Data Update", page_icon=page_favicon(), layout="wide")
    common.render_sidebar_logo_toggle(width=56, height=56)
    common.render_page_title("Data Update", caption="Pull latest FRED data and rebuild fmdata.txt for the model.")

    model_dir = st.sidebar.text_input("model_dir", value="FM")
    out_dir = st.sidebar.text_input("out_dir", value=_default_out_dir())
    end_period = st.sidebar.text_input("end (YYYY.Q)", value="2025.4")
    cache_dir = st.sidebar.text_input("cache_dir (optional)", value="")
    sources = st.sidebar.multiselect(
        "sources",
        options=list(SUPPORTED_SOURCES),
        default=["fred"],
        help="Data sources to enable for this update.",
    )

    replace_history = st.sidebar.checkbox("replace_history", value=False)
    extend_sample = st.sidebar.checkbox("extend_sample", value=False)
    allow_carry_forward = st.sidebar.checkbox("allow_carry_forward", value=False)
    use_official_bundle = st.sidebar.checkbox("Use official Fair bundle as base", value=False)
    if use_official_bundle:
        official_bundle_url = st.sidebar.text_input(
            "official_bundle_url",
            value=DEFAULT_FAIR_BUNDLE_URL,
        )
        base_dir = st.sidebar.text_input("base_dir (optional)", value="")
    else:
        official_bundle_url = DEFAULT_FAIR_BUNDLE_URL
        base_dir = ""

    timeout_seconds = st.sidebar.number_input(
        "timeout_seconds",
        min_value=10,
        max_value=600,
        value=30,
        step=10,
        help="Subprocess timeout for the update command.",
    )
    parity_timeout_seconds = st.sidebar.number_input(
        "parity_timeout_seconds",
        min_value=30,
        max_value=3600,
        value=900,
        step=30,
        help="Subprocess timeout for one-click parity smoke.",
    )

    mapped_vars, picker_error = _load_mapped_variables(Path(model_dir), sources)
    variable_scope = st.sidebar.radio(
        "Variable scope",
        options=("All mapped vars (default)", "Choose subset"),
        index=0,
    )
    selected_vars: list[str] = []
    if picker_error:
        st.sidebar.warning(f"Variable picker unavailable: {picker_error}")
    elif mapped_vars:
        st.sidebar.caption(f"Mapped variables found for selected sources: {len(mapped_vars)}")
    else:
        st.sidebar.info("No mapped variables discovered for the selected sources.")

    if variable_scope == "Choose subset":
        selected_vars = st.sidebar.multiselect(
            "variables",
            options=mapped_vars,
            default=[],
            help="Choose one or more mapped variables to update.",
        )
        if not selected_vars:
            st.sidebar.info("Select at least one variable to run subset mode.")
    else:
        st.sidebar.caption("Default mode: update all variables with eligible mappings for selected sources.")

    preflight_checks = _build_preflight_checks(
        model_dir=model_dir,
        end_period=end_period,
        selected_sources=sources,
    )
    with st.sidebar.expander("Preflight checklist", expanded=True):
        for label, ok in preflight_checks:
            if ok:
                st.success(label)
            else:
                st.error(label)

    preflight_ok = all(ok for _, ok in preflight_checks)
    cmd = _build_update_command(
        model_dir=model_dir,
        out_dir=out_dir,
        end_period=end_period,
        cache_dir=cache_dir,
        sources=sources,
        selected_vars=selected_vars,
        replace_history=replace_history,
        extend_sample=extend_sample,
        allow_carry_forward=allow_carry_forward,
        use_official_bundle=use_official_bundle,
        official_bundle_url=official_bundle_url,
        base_dir=base_dir,
    )

    st.code(" ".join(cmd), language="bash")

    report_path = Path(out_dir) / "data_update_report.json"
    bundle_path = Path(out_dir) / "FM"
    parsed_report: dict | None = None

    run_disabled = (variable_scope == "Choose subset" and not selected_vars) or not preflight_ok
    if st.button("Run Update", disabled=run_disabled, key="run_update_button"):
        env = os.environ.copy()
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        env.setdefault("PYTHONPYCACHEPREFIX", "/tmp/fp-wraptr-pycache")
        try:
            completed = subprocess.run(
                cmd,
                check=False,
                text=True,
                capture_output=True,
                timeout=int(timeout_seconds),
                env=env,
            )
        except subprocess.TimeoutExpired:
            st.error(f"Timed out after {int(timeout_seconds)} seconds.")
            return

        if completed.returncode == 0:
            st.success("Data update completed.")
            fp_exe_present, fp_exe_path = _fp_exe_check(bundle_path)
            if fp_exe_present:
                st.success(f"fp.exe available: {_local_markdown_link(Path(fp_exe_path))}")
            else:
                st.warning(
                    "fp.exe missing: copy `fp.exe` into the output `FM/` before running `--backend fpexe` or parity."
                )
                st.caption(
                    "Recommended action: copy `fp.exe` into the output `FM/` directory, "
                    "or rerun `fp data fetch-fair-bundle` with `--fp-exe-from <fp.exe|fp_home>`."
                )
        else:
            st.error(f"Data update failed (exit code {completed.returncode}).")
            for hint in _failure_hints(completed.stdout, completed.stderr):
                st.warning(hint)

            with st.expander("Subprocess logs (stdout/stderr)", expanded=completed.returncode != 0):
                st.write(f"exit_code: {completed.returncode}")
                if completed.stdout:
                    stdout_text, stdout_trimmed = _truncate_log(completed.stdout)
                    st.caption("stdout")
                    st.code(stdout_text)
                    if stdout_trimmed:
                        st.caption(f"stdout truncated ({stdout_trimmed} chars omitted).")
                if completed.stderr:
                    stderr_text, stderr_trimmed = _truncate_log(completed.stderr)
                    st.caption("stderr")
                    st.code(stderr_text)
                    if stderr_trimmed:
                        st.caption(f"stderr truncated ({stderr_trimmed} chars omitted).")
                if not completed.stdout and not completed.stderr:
                    st.caption("No subprocess output captured.")

    if report_path.exists():
        try:
            payload = json.loads(report_path.read_text(encoding="utf-8"))
        except Exception as exc:
            st.warning(f"Failed to parse report JSON: {exc}")
        else:
            parsed_report = payload
            st.subheader("Report Summary")
            _render_report_summary(payload)
            bundle_dir = payload.get("bundle_dir")
            if isinstance(bundle_dir, str) and bundle_dir:
                bundle_path = Path(bundle_dir)
    else:
        st.info("No report file found yet.")

    st.subheader("Output Artifacts")
    if bundle_path.exists():
        st.markdown(f"Bundle (`FM`): {_local_markdown_link(bundle_path)}")
    else:
        st.caption(f"Bundle (`FM`) path: {bundle_path}")
    if report_path.exists():
        st.markdown(f"Report JSON: {_local_markdown_link(report_path)}")
    else:
        st.caption(f"Report JSON path: {report_path}")

    scenario_path = Path(out_dir) / "scenarios" / "baseline_smoke.yaml"
    if isinstance(parsed_report, dict):
        templates = parsed_report.get("scenario_templates")
        if isinstance(templates, dict):
            baseline_smoke = str(templates.get("baseline_smoke_yaml", "")).strip()
            if baseline_smoke:
                scenario_path = Path(baseline_smoke)

    parity_output_dir = Path(out_dir) / "parity_smoke"
    st.subheader("Parity Smoke")
    if scenario_path.exists():
        parity_cmd = _build_parity_smoke_command(
            scenario_yaml=scenario_path,
            output_dir=parity_output_dir,
        )
        st.code(" ".join(parity_cmd), language="bash")
        if st.button("Run Parity Smoke", key="run_parity_smoke_button"):
            env = os.environ.copy()
            env["PYTHONDONTWRITEBYTECODE"] = "1"
            env.setdefault("PYTHONPYCACHEPREFIX", "/tmp/fp-wraptr-pycache")
            try:
                parity_completed = subprocess.run(
                    parity_cmd,
                    check=False,
                    text=True,
                    capture_output=True,
                    timeout=int(parity_timeout_seconds),
                    env=env,
                )
            except subprocess.TimeoutExpired:
                st.error(f"Parity smoke timed out after {int(parity_timeout_seconds)} seconds.")
            else:
                if parity_completed.returncode == 0:
                    st.success("Parity smoke completed.")
                else:
                    st.error(f"Parity smoke failed (exit code {parity_completed.returncode}).")
                with st.expander(
                    "Parity smoke logs (stdout/stderr)",
                    expanded=parity_completed.returncode != 0,
                ):
                    st.write(f"exit_code: {parity_completed.returncode}")
                    if parity_completed.stdout:
                        stdout_text, stdout_trimmed = _truncate_log(parity_completed.stdout)
                        st.caption("stdout")
                        st.code(stdout_text)
                        if stdout_trimmed:
                            st.caption(f"stdout truncated ({stdout_trimmed} chars omitted).")
                    if parity_completed.stderr:
                        stderr_text, stderr_trimmed = _truncate_log(parity_completed.stderr)
                        st.caption("stderr")
                        st.code(stderr_text)
                        if stderr_trimmed:
                            st.caption(f"stderr truncated ({stderr_trimmed} chars omitted).")
                    if not parity_completed.stdout and not parity_completed.stderr:
                        st.caption("No subprocess output captured.")
    else:
        st.info(
            "No baseline_smoke scenario template found yet. Run a data update first to generate it."
        )

    latest_parity_report = _find_latest_parity_report(parity_output_dir)
    if latest_parity_report is not None:
        try:
            parity_summary = _load_parity_report_summary(latest_parity_report)
        except Exception as exc:
            st.warning(f"Failed to parse parity report: {exc}")
        else:
            st.markdown("**Latest parity smoke summary**")
            cols = st.columns(4)
            cols[0].metric("status", str(parity_summary.get("status", "unknown")))
            cols[1].metric("exit_code", str(parity_summary.get("exit_code", "n/a")))
            cols[2].metric(
                "hard_fail_cell_count",
                str(parity_summary.get("hard_fail_cell_count", 0)),
            )
            cols[3].metric("max_abs_diff", str(parity_summary.get("max_abs_diff", 0.0)))
            if _should_show_gate_failed_caveat(parity_summary):
                top_diff_vars = _extract_top_diff_variables(parity_summary)
                if top_diff_vars:
                    rendered = ", ".join(f"`{name}`" for name in top_diff_vars)
                    st.caption(f"Top first-diff variables: {rendered}")
                st.info(
                    "Known extend-sample drift may concentrate in rate-chain variables; "
                    "run fp.exe-only for research if strict parity is required."
                )
            st.markdown(f"Latest parity report: {_local_markdown_link(latest_parity_report)}")


if __name__ == "__main__":
    main()
