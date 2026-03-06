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
    explicit_fp_home = isinstance(raw_payload, dict) and raw_payload.get("fp_home") not in (None, "")
    explicit_overlay = isinstance(raw_payload, dict) and raw_payload.get("input_overlay_dir") not in (
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

    return config


def validate_fp_home(fp_home: Path) -> None:
    """Validate that fp_home exists and provide actionable remediation."""
    if not Path(fp_home).exists():
        raise FileNotFoundError(
            f"fp_home path not found: {fp_home}. "
            "Check the fp_home path in your scenario YAML or --fp-home option."
        )


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
) -> ScenarioResult:
    """Execute a scenario end-to-end.

    Args:
        config: Scenario configuration.
        output_dir: Base directory for run artifacts.
        backend: Optional execution backend (defaults to FPExecutable).

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
    if backend is not None:
        selected_backend = backend
    else:
        backend_name = str(getattr(config, "backend", "fpexe") or "fpexe").strip().lower()
        if backend_name in {"fpexe", "fp.exe", "fp_exe"}:
            selected_backend = FPExecutable(fp_home=config.fp_home)
        elif backend_name in {"fppy", "fairpy", "fair-py", "fp-py"}:
            fppy_settings = getattr(config, "fppy", {}) or {}
            # Default to FP-style solve semantics for direct fppy runs; callers can
            # opt out via `fppy.eq_flags_preset: default`.
            eq_flags_preset = str(fppy_settings.get("eq_flags_preset", "parity")).strip()
            default_timeout = 2400 if eq_flags_preset.lower() == "parity" else 600
            timeout_seconds = int(fppy_settings.get("timeout_seconds", default_timeout))
            num_threads_raw = fppy_settings.get("num_threads")
            num_threads = (
                int(num_threads_raw)
                if num_threads_raw is not None and int(num_threads_raw) > 0
                else None
            )
            selected_backend = FairPyBackend(
                fp_home=config.fp_home,
                timeout_seconds=timeout_seconds,
                eq_flags_preset=eq_flags_preset,
                num_threads=num_threads,
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
            raise ValueError(f"Unknown backend: {backend_name!r} (expected fpexe|fppy|both)")

    # Copy data files to work dir
    if isinstance(selected_backend, FPExecutable):
        selected_backend._copy_data_files(work_dir)
    else:
        _copy_model_files(config.fp_home, work_dir)

    # Apply input patches if any
    input_file = work_dir / config.input_file
    if not input_file.exists():
        src = config.fp_home / config.input_file
        if src.exists():
            shutil.copy2(src, input_file)
        else:
            overlay_dir = getattr(config, "input_overlay_dir", None)
            if overlay_dir is not None:
                overlay_src = Path(overlay_dir) / config.input_file
                if overlay_src.exists():
                    shutil.copy2(overlay_src, input_file)

    if not input_file.exists():
        searched: list[str] = [str(config.fp_home / config.input_file)]
        overlay_dir = getattr(config, "input_overlay_dir", None)
        if overlay_dir is not None:
            searched.append(str(Path(overlay_dir) / config.input_file))
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
        if backend is None:
            _try_parse_existing_output(result, config)
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
        except ImportError:
            pass  # matplotlib not installed

    return result


def _copy_model_files(fp_home: Path, dest: Path) -> None:
    """Copy expected model files into a working directory."""
    for fname in ("fmdata.txt", "fmage.txt", "fmexog.txt", "fminput.txt"):
        src = fp_home / fname
        if src.exists():
            target = dest / fname
            if not target.exists():
                shutil.copy2(src, target)


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
            except ImportError:
                pass
