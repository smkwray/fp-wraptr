"""Scenario execution pipeline.

Orchestrates the full run flow:
1. Load scenario config (YAML)
2. Prepare working directory (copy data files + apply overrides)
3. Invoke fp.exe via subprocess wrapper
4. Parse output
5. Generate charts
"""

from __future__ import annotations

import datetime as _dt
import json
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from fp_wraptr.io.parser import FPOutputData, parse_fp_output
from fp_wraptr.io.writer import patch_input_file, write_exogenous_override_file
from fp_wraptr.runtime.backend import ModelBackend, RunResult
from fp_wraptr.runtime.fairpy import FairPyBackend
from fp_wraptr.runtime.fp_exe import FPExecutable
from fp_wraptr.runtime.semantics import get_backend_semantics_profile
from fp_wraptr.scenarios.config import ScenarioConfig
from fp_wraptr.scenarios.input_tree import (
    InputTreeManifest,
    prepare_work_dir_for_fp_run,
    select_primary_loadformat_output,
)


def load_scenario_config(path: Path | str) -> ScenarioConfig:
    """Load a scenario YAML file with a clear path-focused error message."""
    scenario_path = Path(path)
    if not scenario_path.exists():
        raise FileNotFoundError(
            f"Scenario YAML not found: {scenario_path} (attempted path: {scenario_path.resolve()})"
        )
    raw_payload: object = None
    try:
        raw_payload = yaml.safe_load(scenario_path.read_text(encoding="utf-8"))
    except Exception:
        raw_payload = None
    explicit_fp_home = isinstance(raw_payload, dict) and raw_payload.get("fp_home") not in (
        None,
        "",
    )
    explicit_overlay = isinstance(raw_payload, dict) and raw_payload.get(
        "input_overlay_dir"
    ) not in (
        None,
        "",
    )

    config = ScenarioConfig.from_yaml(scenario_path)

    # Interpret relative paths in the YAML as relative to the YAML file location,
    # not the current working directory.
    fp_home = Path(getattr(config, "fp_home", Path("FM")))
    if explicit_fp_home and not fp_home.is_absolute():
        config.fp_home = (scenario_path.parent / fp_home).resolve()

    overlay_dir = getattr(config, "input_overlay_dir", None)
    if explicit_overlay and overlay_dir:
        overlay_path = Path(overlay_dir)
        if not overlay_path.is_absolute():
            config.input_overlay_dir = (scenario_path.parent / overlay_path).resolve()

    fmout_override = config.fppy.fmout_coefs_override
    if fmout_override not in (None, ""):
        override_path = Path(fmout_override)
        if not override_path.is_absolute():
            config.fppy = config.fppy.model_copy(
                update={
                    "fmout_coefs_override": str((scenario_path.parent / override_path).resolve())
                }
            )

    fpr_settings = getattr(config, "fpr", {}) or {}
    bundle_path = fpr_settings.get("bundle_path")
    if bundle_path not in (None, ""):
        candidate = Path(str(bundle_path))
        if not candidate.is_absolute():
            fpr_settings = dict(fpr_settings)
            fpr_settings["bundle_path"] = str((scenario_path.parent / candidate).resolve())
            config.fpr = fpr_settings
    rscript_path = fpr_settings.get("rscript_path")
    if rscript_path not in (None, ""):
        candidate = Path(str(rscript_path))
        if not candidate.is_absolute():
            fpr_settings = dict(fpr_settings)
            fpr_settings["rscript_path"] = str((scenario_path.parent / candidate).resolve())
            config.fpr = fpr_settings
    expected_csv = fpr_settings.get("expected_csv")
    if expected_csv not in (None, ""):
        candidate = Path(str(expected_csv))
        if not candidate.is_absolute():
            fpr_settings = dict(fpr_settings)
            fpr_settings["expected_csv"] = str((scenario_path.parent / candidate).resolve())
            config.fpr = fpr_settings

    return config


_REQUIRED_MODEL_FILES = ("fminput.txt", "fmdata.txt", "fmage.txt", "fmexog.txt")


def backend_requires_fp_home(backend_name: str | None) -> bool:
    """Return whether a backend uses the staged raw-input model surface."""
    name = str(backend_name or "fpexe").strip().lower()
    return name in {
        "fpexe",
        "fp.exe",
        "fp_exe",
        "fppy",
        "fairpy",
        "fair-py",
        "fp-py",
        "fpr",
        "fp-r",
        "fp_r",
        "both",
    }


def validate_fp_home(fp_home: Path) -> None:
    """Validate that fp_home exists and contains required model files."""
    fp_home = Path(fp_home)
    if not fp_home.exists():
        raise FileNotFoundError(
            f"fp_home path not found: {fp_home}. "
            "Check the fp_home path in your scenario YAML or --fp-home option."
        )
    missing = [f for f in _REQUIRED_MODEL_FILES if not (fp_home / f).exists()]
    if missing:
        raise FileNotFoundError(
            f"fp_home ({fp_home}) is missing required model files: {', '.join(missing)}. "
            "Ensure all FP data files are present before running a scenario."
        )


def _copy_file_with_overlay_precedence(
    *,
    fp_home: Path,
    dest: Path,
    name: str,
    overlay_dir: Path | None,
) -> None:
    """Stage a file into ``dest`` with overlay contents taking precedence."""
    source = fp_home / name
    if source.exists():
        shutil.copy2(source, dest / name)

    if overlay_dir is None:
        return

    overlay_source = overlay_dir / name
    if overlay_source.exists():
        shutil.copy2(overlay_source, dest / name)


@dataclass
class ScenarioResult:
    """Result of a scenario execution."""

    config: ScenarioConfig
    output_dir: Path
    run_result: RunResult | None = None
    parsed_output: FPOutputData | None = None
    chart_path: Path | None = None
    golden_comparison: dict | None = None
    backend_diagnostics: dict[str, Any] | None = None
    timestamp: str = field(
        default_factory=lambda: _dt.datetime.now(_dt.UTC).strftime("%Y%m%d_%H%M%S")
    )

    @property
    def success(self) -> bool:
        return self.run_result is not None and self.run_result.success


def run_scenario(
    config: ScenarioConfig,
    output_dir: Path | None = None,
    backend: ModelBackend | None = None,
    *,
    allow_stale_output: bool = False,
) -> ScenarioResult:
    """Execute a scenario end-to-end.

    Args:
        config: Scenario configuration.
        output_dir: Base directory for run artifacts.
        backend: Optional execution backend (defaults to FPExecutable).
        allow_stale_output: When True and the backend is unavailable, fall back
            to parsing an existing fmout.txt from fp_home instead of failing.

    Returns:
        ScenarioResult with run outcome and parsed data.
    """
    timestamp = _dt.datetime.now(_dt.UTC).strftime("%Y%m%d_%H%M%S")
    if output_dir is None:
        output_dir = Path("artifacts")
    run_dir = output_dir / f"{config.name}_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)

    result = ScenarioResult(config=config, output_dir=run_dir, timestamp=timestamp)

    # Save config for reproducibility. Use an absolute fp_home in the artifact
    # so the saved scenario can be re-run from the run directory.
    config_for_artifact = config.model_copy(deep=True)
    try:
        config_for_artifact.fp_home = Path(config_for_artifact.fp_home).expanduser().resolve()
    except Exception:
        config_for_artifact.fp_home = Path(config_for_artifact.fp_home)
    try:
        overlay_dir = getattr(config_for_artifact, "input_overlay_dir", None)
        if overlay_dir:
            config_for_artifact.input_overlay_dir = Path(overlay_dir).expanduser().resolve()
    except Exception:
        pass
    config_for_artifact.to_yaml(run_dir / "scenario.yaml")

    # Prepare working directory
    work_dir = run_dir / "work"
    work_dir.mkdir(exist_ok=True)

    selected_backend: ModelBackend
    semantics_profile = get_backend_semantics_profile(getattr(config, "semantics_profile", None))
    if backend is not None:
        selected_backend = backend
    else:
        backend_name = str(getattr(config, "backend", "fpexe") or "fpexe").strip().lower()
        if backend_name in {"fpexe", "fp.exe", "fp_exe"}:
            selected_backend = FPExecutable(fp_home=config.fp_home)
        elif backend_name in {"fppy", "fairpy", "fair-py", "fp-py"}:
            fps = config.fppy
            # Default to FP-style solve semantics for direct fppy runs; callers can
            # opt out via `fppy.eq_flags_preset: default`.
            eq_flags_preset = str(
                fps.eq_flags_preset if fps.eq_flags_preset is not None else "parity"
            ).strip()
            default_timeout = 2400 if eq_flags_preset.lower() == "parity" else 600
            timeout_seconds = (
                int(fps.timeout_seconds) if fps.timeout_seconds is not None else default_timeout
            )
            num_threads = (
                int(fps.num_threads)
                if fps.num_threads is not None and int(fps.num_threads) > 0
                else None
            )
            eq_structural_read_cache = str(fps.eq_structural_read_cache or "off").strip()
            selected_backend = FairPyBackend(
                fp_home=config.fp_home,
                timeout_seconds=timeout_seconds,
                eq_flags_preset=eq_flags_preset,
                eq_structural_read_cache=eq_structural_read_cache,
                num_threads=num_threads,
                semantics_profile=semantics_profile.name,
                fmout_coefs_override=(
                    Path(str(fps.fmout_coefs_override)).expanduser().resolve()
                    if fps.fmout_coefs_override not in (None, "")
                    else None
                ),
            )
        elif backend_name in {"fpr", "fp-r", "fp_r"}:
            from fp_wraptr.runtime.fpr import FpRBackend

            fpr_settings = getattr(config, "fpr", {}) or {}
            bundle_path_raw = fpr_settings.get("bundle_path")
            rscript_path_raw = fpr_settings.get("rscript_path")
            timeout_seconds = int(fpr_settings.get("timeout_seconds", 1200))
            selected_backend = FpRBackend(
                bundle_path=(
                    Path(str(bundle_path_raw)).expanduser().resolve()
                    if bundle_path_raw not in (None, "")
                    else None
                ),
                fp_home=config.fp_home,
                rscript_path=(
                    Path(str(rscript_path_raw)).expanduser().resolve()
                    if rscript_path_raw not in (None, "")
                    else None
                ),
                timeout_seconds=timeout_seconds,
                semantics_profile=semantics_profile.name,
            )
        elif backend_name == "both":
            from fp_wraptr.analysis.parity import run_parity
            from fp_wraptr.runtime.backend import RunResult

            parity = run_parity(
                config,
                output_dir=run_dir / "parity",
                fp_home_override=config.fp_home,
            )
            parity_dir = Path(parity.run_dir)
            parity_report = parity_dir / "parity_report.json"
            if parity_report.exists():
                shutil.copy2(parity_report, run_dir / "parity_report.json")
            parity_outputs = [
                parity_dir / "work_fppy" / "PABEV.TXT",
                parity_dir / "work_fppy" / "PACEV.TXT",
                parity_dir / "work_fpexe" / "PABEV.TXT",
                parity_dir / "work_fpexe" / "PACEV.TXT",
            ]
            pabev_selected = next((path for path in parity_outputs if path.exists()), None)
            if pabev_selected is not None:
                preserved = run_dir / pabev_selected.name
                shutil.copy2(pabev_selected, preserved)
                shutil.copy2(preserved, run_dir / "LOADFORMAT.DAT")
            result.run_result = RunResult(
                return_code=int(parity.exit_code),
                stdout=json.dumps(parity.to_dict(), sort_keys=True),
                stderr="",
                working_dir=parity_dir,
                input_file=parity_dir / "bundle" / config.input_file,
                output_file=pabev_selected if pabev_selected is not None else None,
                duration_seconds=0.0,
            )
            result.backend_diagnostics = {"mode": "both", "parity": parity.to_dict()}
            return result
        else:
            raise ValueError(f"Unknown backend: {backend_name!r} (expected fpexe|fppy|fp-r|both)")

    # Copy data files to work dir
    overlay_dir = getattr(config, "input_overlay_dir", None)
    overlay_path = Path(overlay_dir) if overlay_dir is not None else None

    if isinstance(selected_backend, FPExecutable):
        selected_backend._copy_data_files(work_dir)
        for fname in _REQUIRED_MODEL_FILES:
            _copy_file_with_overlay_precedence(
                fp_home=config.fp_home,
                dest=work_dir,
                name=fname,
                overlay_dir=overlay_path,
            )
    else:
        _copy_model_files(config.fp_home, work_dir, overlay_dir=overlay_path)

    # Apply input patches if any
    input_file = work_dir / config.input_file
    overlay_input = overlay_path / config.input_file if overlay_path is not None else None
    base_input = config.fp_home / config.input_file
    if overlay_input is not None and overlay_input.exists():
        shutil.copy2(overlay_input, input_file)
    elif not input_file.exists() and base_input.exists():
        shutil.copy2(base_input, input_file)

    if not input_file.exists():
        searched: list[str] = [str(config.fp_home / config.input_file)]
        if overlay_path is not None:
            searched.append(str(overlay_path / config.input_file))
        raise FileNotFoundError(
            "Scenario input file not found. "
            f"input_file={config.input_file!r} searched: {', '.join(searched)}"
        )

    if config.input_patches:
        patch_input_file(input_file, config.input_patches, input_file)

    # Write exogenous overrides.
    #
    # Keep fp.exe-compatible behavior by writing merged baseline+scenario exogenous
    # adjustments back to `fmexog.txt` (the canonical template filename).
    if config.overrides:
        overrides_dict = {
            name: {"method": ov.method, "value": ov.value} for name, ov in config.overrides.items()
        }
        exogenous_path = work_dir / "fmexog.txt"
        write_exogenous_override_file(
            base_fmexog=config.fp_home / "fmexog.txt",
            variables=overrides_dict,
            sample_start=config.forecast_start,
            sample_end=config.forecast_end,
            output_path=exogenous_path,
        )
        # Keep an inspection copy with an explicit name for operator debugging.
        shutil.copy2(exogenous_path, work_dir / "fmexog_override.txt")

    input_manifest: InputTreeManifest | None = None
    try:
        input_manifest = prepare_work_dir_for_fp_run(
            entry_input=input_file,
            work_dir=work_dir,
            overlay_dir=getattr(config, "input_overlay_dir", None),
            fp_home=config.fp_home,
        )
    except Exception as exc:
        # For missing includes, fail with a path-focused error message early.
        raise RuntimeError(f"Failed to prepare FP input tree: {exc}") from exc
    else:
        if config.input_patches and input_manifest is not None:
            for name in (input_manifest.entry_input_file, *input_manifest.include_files):
                staged = work_dir / name
                if staged.exists():
                    patch_input_file(staged, config.input_patches, staged)

    # Run fp.exe
    if not selected_backend.check_available():
        # fp.exe not available -- parse existing output for development fallback.
        # For custom backends, keep behavior strict (unavailable backend => no run result).
        result.run_result = None
        if isinstance(selected_backend, FPExecutable):
            diagnostics = selected_backend.preflight_report(
                input_file=input_file, work_dir=work_dir
            )
            result.backend_diagnostics = diagnostics
            (run_dir / "backend_preflight.json").write_text(
                json.dumps(diagnostics, indent=2, default=str) + "\n",
                encoding="utf-8",
            )
        if backend is None and allow_stale_output:
            _try_parse_existing_output(result, config)
        elif backend is None:
            import logging

            logging.getLogger(__name__).warning(
                "Backend unavailable and stale output reuse is disabled. "
                "Pass --allow-stale-output to fall back to existing fmout.txt."
            )
        return result

    run_result = selected_backend.run(input_file=input_file, work_dir=work_dir)
    result.run_result = run_result

    # Copy key outputs to artifacts, if present.
    fmout_path = work_dir / "fmout.txt"
    if fmout_path.exists():
        shutil.copy2(fmout_path, run_dir / "fmout.txt")
    pabev_path = work_dir / "PABEV.TXT"
    if pabev_path.exists():
        shutil.copy2(pabev_path, run_dir / "PABEV.TXT")
    pacev_path = work_dir / "PACEV.TXT"
    if pacev_path.exists():
        shutil.copy2(pacev_path, run_dir / "PACEV.TXT")
    for extra_name in (
        "fp_r_series.csv",
        "fp_r_diagnostics.csv",
        "fp_r_report.txt",
        "fp_r.runtime.json",
    ):
        candidate = work_dir / extra_name
        if candidate.exists():
            shutil.copy2(candidate, run_dir / extra_name)

    copied_outputs: list[Path] = []
    if input_manifest is not None and input_manifest.expected_output_files:
        for name in input_manifest.expected_output_files:
            candidate = work_dir / name
            if not candidate.exists():
                continue
            target = run_dir / name
            shutil.copy2(candidate, target)
            copied_outputs.append(target)

    primary = select_primary_loadformat_output(run_dir, copied_outputs)
    if primary is not None:
        shutil.copy2(primary, run_dir / "LOADFORMAT.DAT")

    # Parse output
    output_file = run_dir / "fmout.txt"
    if output_file.exists():
        result.parsed_output = parse_fp_output(output_file)

    # Generate chart
    if result.parsed_output and result.parsed_output.variables:
        try:
            from fp_wraptr.viz.plots import plot_forecast

            chart_path = run_dir / "forecast.png"
            plot_forecast(
                result.parsed_output,
                variables=config.track_variables or None,
                output_path=chart_path,
            )
            result.chart_path = chart_path
        except (ImportError, ValueError):
            pass  # matplotlib not installed

    return result


def _copy_model_files(fp_home: Path, dest: Path, *, overlay_dir: Path | None = None) -> None:
    """Copy expected model files into a working directory."""
    for fname in ("fmdata.txt", "fmage.txt", "fmexog.txt", "fminput.txt", "fmout.txt"):
        _copy_file_with_overlay_precedence(
            fp_home=fp_home,
            dest=dest,
            name=fname,
            overlay_dir=overlay_dir,
        )


def _try_parse_existing_output(result: ScenarioResult, config: ScenarioConfig) -> None:
    """Try to parse an existing fmout.txt from fp_home when fp.exe isn't available.

    This enables development/testing without needing to run fp.exe.
    """
    existing_output = config.fp_home / "fmout.txt"
    if existing_output.exists():
        shutil.copy2(existing_output, result.output_dir / "fmout.txt")
        result.parsed_output = parse_fp_output(existing_output)

        if result.parsed_output and result.parsed_output.variables:
            try:
                from fp_wraptr.viz.plots import plot_forecast

                chart_path = result.output_dir / "forecast.png"
                plot_forecast(
                    result.parsed_output,
                    variables=config.track_variables or None,
                    output_path=chart_path,
                )
                result.chart_path = chart_path
            except (ImportError, ValueError):
                pass
