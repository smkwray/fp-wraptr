"""Parity runner for comparing fp.exe vs fp-py (fppy) on scenarios.

fp-wraptr's parity contract is PABEV.TXT (PRINTVAR LOADFORMAT output). This
module runs a scenario against both engines, compares PABEV numerically, and
enforces hard-fail invariants (missing sentinel, discrete, sign flips).
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import shutil
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from fp_wraptr.runtime.fairpy import FairPyBackend
from fp_wraptr.runtime.fp_exe import FPExecutable
from fp_wraptr.runtime.solve_errors import scan_solution_errors
from fp_wraptr.scenarios.config import ScenarioConfig


def _utc_stamp() -> str:
    return _dt.datetime.now(_dt.UTC).strftime("%Y%m%d_%H%M%S")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def compute_input_fingerprint(fp_home: Path) -> dict[str, Any]:
    names = ("fminput.txt", "fmdata.txt", "fmage.txt", "fmexog.txt", "fmout.txt")
    files: dict[str, str | None] = {}
    for name in names:
        path = fp_home / name
        files[name] = _sha256(path) if path.exists() else None
    return {"algo": "sha256", "files": files}


def _load_fingerprint_lock(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("fingerprint lockfile must be a JSON object")
    return payload


def _fingerprint_matches(lock: dict[str, Any], observed: dict[str, Any]) -> tuple[bool, str]:
    if lock.get("algo") != "sha256":
        return False, "fingerprint lockfile algo must be sha256"
    lock_files = lock.get("files")
    observed_files = observed.get("files")
    if not isinstance(lock_files, dict) or not isinstance(observed_files, dict):
        return False, "fingerprint schema invalid (expected {algo, files})"
    for name, expected in lock_files.items():
        if name not in observed_files:
            return False, f"fingerprint missing file key: {name}"
        if observed_files.get(name) != expected:
            return False, f"inputs changed; fingerprint mismatch for {name}"
    return True, "ok"


@dataclass(frozen=True)
class DriftConfig:
    enabled: bool = False
    max_abs: float = 1e-2
    growth_factor: float = 30.0
    quantile: float = 0.99
    ref_periods: int = 1


@dataclass(frozen=True)
class GateConfig:
    pabev_start: str = "2025.4"
    pabev_end: str | None = None
    # PABEV is a floating-point export; across engines we occasionally see
    # ~1e-3 scale rounding noise in otherwise parity-correct scenarios.
    atol: float = 1.1e-3
    rtol: float = 1e-6
    missing_sentinels: tuple[float, ...] = (-99.0,)
    discrete_eps: float = 1e-12
    signflip_eps: float = 1e-3
    drift: DriftConfig = field(default_factory=DriftConfig)


@dataclass(frozen=True)
class EngineRunSummary:
    name: str
    ok: bool
    return_code: int | None = None
    work_dir: str = ""
    pabev_path: str = ""
    stdout_path: str = ""
    stderr_path: str = ""
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ParityResult:
    status: str
    run_dir: str
    scenario_name: str
    input_fingerprint: dict[str, Any]
    fingerprint_ok: bool = True
    fingerprint_message: str = "not_checked"
    engine_runs: dict[str, EngineRunSummary] = field(default_factory=dict)
    pabev_compare_ok: bool = False
    pabev_detail: dict[str, Any] = field(default_factory=dict)
    drift_check: dict[str, Any] | None = None
    exit_code: int = 4

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["engine_runs"] = {k: asdict(v) for k, v in self.engine_runs.items()}
        # Report schema versioning: additive changes do not require bumping.
        # Missing schema_version should be treated as legacy v0 by consumers.
        payload.setdefault("schema_version", 1)
        try:
            from fp_wraptr import __version__ as producer_version
        except Exception:  # pragma: no cover - extremely defensive
            producer_version = "unknown"
        payload.setdefault("producer_version", str(producer_version))
        return payload


def _copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)


def _parity_output_candidates(work_dir: Path) -> tuple[Path, ...]:
    # Fair bundles have historically used PABEV.TXT for PRINTVAR LOADFORMAT output,
    # but some official bundles emit the same output under PACEV.TXT.
    return (
        work_dir / "PABEV.TXT",
        work_dir / "PACEV.TXT",
    )


def _resolve_parity_output_path(
    work_dir: Path, *, expected_outputs: tuple[str, ...] | None = None
) -> Path:
    from fp_wraptr.scenarios.input_tree import select_primary_loadformat_output

    for candidate in _parity_output_candidates(work_dir):
        if candidate.exists():
            return candidate
    if expected_outputs:
        candidates = [work_dir / name for name in expected_outputs]
        primary = select_primary_loadformat_output(work_dir, copied_outputs=candidates)
        if primary is not None and primary.exists():
            return primary
    return _parity_output_candidates(work_dir)[0]


def _scan_fpexe_solution_errors(work_dir: Path) -> list[dict[str, Any]]:
    """Return solution error matches as JSON-friendly dicts."""
    return [m.to_dict() for m in scan_solution_errors(work_dir)]


def _prepare_bundle(config: ScenarioConfig, bundle_dir: Path) -> tuple[Path, InputTreeManifest]:
    from fp_wraptr.io.writer import (
        patch_input_file,
        write_exogenous_override_file,
    )
    from fp_wraptr.scenarios.input_tree import InputTreeManifest, prepare_work_dir_for_fp_run

    bundle_dir.mkdir(parents=True, exist_ok=True)

    # Copy base model inputs
    for fname in ("fmdata.txt", "fmage.txt", "fmexog.txt", "fminput.txt"):
        src = config.fp_home / fname
        if src.exists():
            shutil.copy2(src, bundle_dir / fname)

    # Copy the scenario input file (may be non-default).
    src_input = config.fp_home / config.input_file
    if src_input.exists():
        shutil.copy2(src_input, bundle_dir / config.input_file)
    else:
        overlay_dir = getattr(config, "input_overlay_dir", None)
        if overlay_dir is not None:
            overlay_src = Path(overlay_dir) / config.input_file
            if overlay_src.exists():
                shutil.copy2(overlay_src, bundle_dir / config.input_file)

    input_path = bundle_dir / config.input_file
    if not input_path.exists():
        # Do not hard-fail bundle prep on missing inputs: parity unit tests and
        # certain diagnostic flows stub the engine and do not require a real
        # input script on disk. Engines that need the file will fail later with
        # their own error messages.
        return (
            bundle_dir,
            InputTreeManifest(
                entry_input_file=str(config.input_file),
                include_files=(),
                load_data_files=(),
                expected_output_files=(),
            ),
        )

    if config.input_patches and input_path.exists():
        patch_input_file(input_path, config.input_patches, input_path)

    # Write exogenous overrides.
    #
    # fp.exe appears to treat `fmexog.txt` as a special template input (the stock FM
    # model uses that filename). In practice, pointing fminput's `INPUT FILE=...;`
    # at a differently named file (even with identical content) results in the
    # exogenous adjustments not being applied. To keep fp.exe behavior stable,
    # we write the merged baseline+scenario exogenous script back to `fmexog.txt`
    # in the bundle/work directories.
    if config.overrides:
        overrides_dict = {
            name: {"method": ov.method, "value": ov.value} for name, ov in config.overrides.items()
        }
        exogenous_path = bundle_dir / "fmexog.txt"
        write_exogenous_override_file(
            base_fmexog=config.fp_home / "fmexog.txt",
            variables=overrides_dict,
            sample_start=config.forecast_start,
            sample_end=config.forecast_end,
            output_path=exogenous_path,
        )
        # Keep an inspection copy with an explicit name for operator debugging.
        shutil.copy2(exogenous_path, bundle_dir / "fmexog_override.txt")

    # Ensure nested `INPUT FILE=...;` dependencies are present in the bundle dir.
    manifest = prepare_work_dir_for_fp_run(
        entry_input=input_path,
        work_dir=bundle_dir,
        overlay_dir=getattr(config, "input_overlay_dir", None),
        fp_home=config.fp_home,
    )

    if config.input_patches:
        for name in (manifest.entry_input_file, *manifest.include_files):
            path = bundle_dir / name
            if path.exists():
                patch_input_file(path, config.input_patches, path)

    return bundle_dir, manifest


def _drift_check_from_period_stats(
    per_period_stats: list[dict[str, Any]],
    *,
    drift: DriftConfig,
) -> dict[str, Any]:
    if not per_period_stats:
        return {
            "enabled": True,
            "status": "failed",
            "fail_reasons": ["missing_per_period_stats"],
            "max_abs_observed": 0.0,
            "quantile_growth_factor": 0.0,
            "quantile_name": "",
            "quantile_definition": "Per-period quantile of absolute diffs.",
        }

    quantile_pct = round(float(drift.quantile) * 100)
    quantile_name = "median_abs_diff" if quantile_pct == 50 else f"p{quantile_pct:02d}_abs_diff"

    max_abs_observed = max(float(row.get("max_abs_diff", 0.0)) for row in per_period_stats)
    quantile_values = [float(row.get(quantile_name, 0.0)) for row in per_period_stats]
    ref_n = max(1, int(drift.ref_periods))
    ref_value = max(quantile_values[:ref_n]) if quantile_values else 0.0
    # Avoid false-positive "growth" failures when the reference quantile is
    # extremely small (often due to lots of exact zeros early in the horizon).
    # Anchor the ratio to a fraction of the absolute cap so growth detection
    # focuses on meaningfully large diffs.
    ref_floor = max(float(drift.max_abs) / 20.0, 1e-12)
    ref_value = max(ref_value, ref_floor)
    growth_factor = (max(quantile_values) / ref_value) if quantile_values else 0.0

    fail_reasons: list[str] = []
    if max_abs_observed > float(drift.max_abs):
        fail_reasons.append("max_abs_exceeds_cap")
    if growth_factor > float(drift.growth_factor):
        fail_reasons.append("quantile_growth_exceeds_factor")

    return {
        "enabled": True,
        "status": "failed" if fail_reasons else "ok",
        "fail_reasons": fail_reasons,
        "max_abs_observed": float(max_abs_observed),
        "quantile_growth_factor": float(growth_factor),
        "quantile_name": quantile_name,
        "quantile_definition": "Per-period quantile of absolute diffs.",
    }


def run_parity(
    config: ScenarioConfig,
    *,
    output_dir: Path,
    fp_home_override: Path | None = None,
    gate: GateConfig | None = None,
    fingerprint_lock: Path | None = None,
) -> ParityResult:
    import fppy.pabev_parity as pabev_parity

    gate = gate or GateConfig()
    config_run = config.model_copy(deep=True)
    if fp_home_override is not None:
        config_run.fp_home = Path(fp_home_override)

    # `--quick` (gate.pabev_end == forecast_start) is intended to be a fast smoke
    # check. Keep the engine runtime aligned by also shrinking the deck's solve
    # window (and the final PRINTVAR SMPL window when present).
    if gate.pabev_end:
        gate_end = str(gate.pabev_end).strip()
        original_end = str(config_run.forecast_end).strip()
        if gate_end and gate_end != original_end:
            config_run.forecast_end = gate_end
            patches = dict(getattr(config_run, "input_patches", {}) or {})
            patches.setdefault(
                f"SMPL {config_run.forecast_start} {original_end};",
                f"SMPL {config_run.forecast_start} {gate_end};",
            )
            # Many decks export LOADFORMAT over the full horizon for printing; keep
            # the output file smaller and runtime lighter for quick parity checks.
            patches.setdefault(
                f"SMPL 1952.1 {original_end};",
                f"SMPL 1952.1 {gate_end};",
            )
            config_run.input_patches = patches

    # Make artifact scenario.yaml replayable from the run directory by storing
    # an absolute fp_home path (relative fp_home values can become invalid when
    # the scenario is copied under artifacts/).
    try:
        config_run.fp_home = Path(config_run.fp_home).expanduser().resolve()
    except Exception:  # pragma: no cover - extremely defensive
        config_run.fp_home = Path(config_run.fp_home)

    fp_home = Path(config_run.fp_home)

    run_dir = Path(output_dir) / f"{config_run.name}_{_utc_stamp()}"
    run_dir.mkdir(parents=True, exist_ok=True)
    config_run.to_yaml(run_dir / "scenario.yaml")

    fingerprint = compute_input_fingerprint(fp_home)
    fingerprint_ok = True
    fingerprint_message = "not_checked"
    if fingerprint_lock is not None:
        lock = _load_fingerprint_lock(Path(fingerprint_lock))
        fingerprint_ok, fingerprint_message = _fingerprint_matches(lock, fingerprint)
        if not fingerprint_ok:
            result = ParityResult(
                status="fingerprint_mismatch",
                run_dir=str(run_dir),
                scenario_name=config_run.name,
                input_fingerprint=fingerprint,
                fingerprint_ok=False,
                fingerprint_message=fingerprint_message,
                exit_code=5,
            )
            (run_dir / "parity_report.json").write_text(
                json.dumps(result.to_dict(), indent=2) + "\n", encoding="utf-8"
            )
            return result

    bundle_dir, manifest = _prepare_bundle(config_run, run_dir / "bundle")
    work_fpexe = run_dir / "work_fpexe"
    work_fppy = run_dir / "work_fppy"
    _copy_tree(bundle_dir, work_fpexe)
    _copy_tree(bundle_dir, work_fppy)
    expected_outputs = tuple(getattr(manifest, "expected_output_files", ()) or ())

    # Build backends
    fpexe = FPExecutable(fp_home=fp_home, timeout_seconds=600)
    fppy_settings = getattr(config_run, "fppy", {}) or {}
    # Full-horizon parity runs can be slow (especially with updated fmdata).
    # Keep a conservative default, but allow scenarios to override.
    fppy_timeout = int(fppy_settings.get("timeout_seconds", 2400))
    # Parity is primarily about fp.exe vs fppy solve fidelity; default to a
    # preset that enables EQ backfill + SETUPSOLVE semantics. Scenarios can
    # override via `fppy.eq_flags_preset`.
    fppy_preset = str(fppy_settings.get("eq_flags_preset", "parity"))
    fppy_eq_iter_trace = bool(fppy_settings.get("eq_iter_trace", False))
    fppy_eq_iter_trace_period_raw = fppy_settings.get("eq_iter_trace_period")
    fppy_eq_iter_trace_period = (
        str(fppy_eq_iter_trace_period_raw) if fppy_eq_iter_trace_period_raw is not None else None
    )
    fppy_eq_iter_trace_targets_raw = fppy_settings.get("eq_iter_trace_targets")
    fppy_eq_iter_trace_targets = (
        str(fppy_eq_iter_trace_targets_raw)
        if fppy_eq_iter_trace_targets_raw is not None
        else None
    )
    fppy_eq_iter_trace_max_events_raw = fppy_settings.get("eq_iter_trace_max_events")
    fppy_eq_iter_trace_max_events = (
        int(fppy_eq_iter_trace_max_events_raw)
        if fppy_eq_iter_trace_max_events_raw is not None
        else None
    )
    fppy_num_threads_raw = fppy_settings.get("num_threads")
    fppy_num_threads = (
        int(fppy_num_threads_raw)
        if fppy_num_threads_raw is not None and int(fppy_num_threads_raw) > 0
        else None
    )
    fppy = FairPyBackend(
        fp_home=fp_home,
        timeout_seconds=fppy_timeout,
        eq_flags_preset=fppy_preset,
        eq_iter_trace=fppy_eq_iter_trace,
        eq_iter_trace_period=fppy_eq_iter_trace_period,
        eq_iter_trace_targets=fppy_eq_iter_trace_targets,
        eq_iter_trace_max_events=fppy_eq_iter_trace_max_events,
        num_threads=fppy_num_threads,
    )

    engine_runs: dict[str, EngineRunSummary] = {}
    fpexe_stdout_path = work_fpexe / "fp-exe.stdout.txt"
    fpexe_stderr_path = work_fpexe / "fp-exe.stderr.txt"
    fpexe_retry_template: dict[str, Any] = {
        "attempted": False,
        "trigger": "",
        "first_return_code": None,
        "second_return_code": None,
    }
    fpexe_preflight_report: dict[str, Any] = fpexe.preflight_report(
        input_file=work_fpexe / config_run.input_file,
        work_dir=work_fpexe,
    )

    def _run_fpexe_engine() -> dict[str, Any]:
        fpexe_retry = dict(fpexe_retry_template)
        fpexe_solution_errors: list[dict[str, Any]] = []
        try:
            rr_a = fpexe.run(input_file=work_fpexe / config_run.input_file, work_dir=work_fpexe)
            if int(rr_a.return_code) != 0 and not _resolve_parity_output_path(
                work_fpexe, expected_outputs=expected_outputs
            ).exists():
                # Single retry for intermittent fp.exe failures that return
                # non-zero and fail to emit parity output.
                fpexe_retry["attempted"] = True
                fpexe_retry["trigger"] = "missing_parity_output_and_nonzero_return_code"
                fpexe_retry["first_return_code"] = int(rr_a.return_code)
                rr_retry = fpexe.run(
                    input_file=work_fpexe / config_run.input_file, work_dir=work_fpexe
                )
                fpexe_retry["second_return_code"] = int(rr_retry.return_code)
                rr_a = rr_retry
            fpexe_details: dict[str, Any] = {}
            fpexe_output = _resolve_parity_output_path(work_fpexe, expected_outputs=expected_outputs)
            fpexe_details["parity_output_file"] = fpexe_output.name
            fpexe_details["preflight_report"] = fpexe_preflight_report
            solution_errors = _scan_fpexe_solution_errors(work_fpexe)
            if solution_errors:
                fpexe_solution_errors = list(solution_errors)
                fpexe_details["solution_errors"] = solution_errors
            return {
                "summary": EngineRunSummary(
                    name="fpexe",
                    ok=bool(rr_a.success),
                    return_code=int(rr_a.return_code),
                    work_dir=str(work_fpexe),
                    pabev_path=str(fpexe_output),
                    stdout_path=str(fpexe_stdout_path),
                    stderr_path=str(fpexe_stderr_path),
                    details=fpexe_details,
                ),
                "error": None,
                "fpexe_retry": fpexe_retry,
                "fpexe_solution_errors": fpexe_solution_errors,
            }
        except Exception as exc:  # pragma: no cover (integration failure path)
            fpexe_details = dict(getattr(exc, "details", {}) or {})
            fpexe_details.setdefault("parity_output_file", _parity_output_candidates(work_fpexe)[0].name)
            fpexe_details.setdefault("preflight_report", fpexe_preflight_report)
            return {
                "summary": EngineRunSummary(
                    name="fpexe",
                    ok=False,
                    return_code=None,
                    work_dir=str(work_fpexe),
                    pabev_path=str(_parity_output_candidates(work_fpexe)[0]),
                    stdout_path=str(fpexe_stdout_path),
                    stderr_path=str(fpexe_stderr_path),
                    details=fpexe_details,
                ),
                "error": f"fp.exe run failed: {type(exc).__name__}: {exc}",
                "fpexe_details": fpexe_details,
                "fpexe_retry": fpexe_retry,
                "fpexe_solution_errors": fpexe_solution_errors,
            }

    def _run_fppy_engine() -> dict[str, Any]:
        try:
            rr_b = fppy.run(input_file=work_fppy / config_run.input_file, work_dir=work_fppy)
            fppy_output = _resolve_parity_output_path(work_fppy, expected_outputs=expected_outputs)
            fppy_eq_iter_trace_path = work_fppy / "eq_iter_trace.json"
            fppy_details: dict[str, Any] = {
                "eq_flags_preset": str(fppy_preset),
                "num_threads": fppy_num_threads,
                "parity_output_file": fppy_output.name,
                "eq_iter_trace": bool(fppy_eq_iter_trace),
                "eq_iter_trace_period": fppy_eq_iter_trace_period,
                "eq_iter_trace_targets": fppy_eq_iter_trace_targets,
                "eq_iter_trace_max_events": fppy_eq_iter_trace_max_events,
            }
            if fppy_eq_iter_trace_path.exists():
                fppy_details["eq_iter_trace_path"] = str(fppy_eq_iter_trace_path)
            return {
                "summary": EngineRunSummary(
                    name="fppy",
                    ok=bool(rr_b.success),
                    return_code=int(rr_b.return_code),
                    work_dir=str(work_fppy),
                    pabev_path=str(fppy_output),
                    stdout_path=str(work_fppy / "fppy.stdout.txt"),
                    stderr_path=str(work_fppy / "fppy.stderr.txt"),
                    details=fppy_details,
                ),
                "error": None,
            }
        except Exception as exc:  # pragma: no cover (integration failure path)
            fppy_details = {
                "eq_flags_preset": str(fppy_preset),
                "num_threads": fppy_num_threads,
                "eq_iter_trace": bool(fppy_eq_iter_trace),
                "eq_iter_trace_period": fppy_eq_iter_trace_period,
                "eq_iter_trace_targets": fppy_eq_iter_trace_targets,
                "eq_iter_trace_max_events": fppy_eq_iter_trace_max_events,
            }
            return {
                "summary": EngineRunSummary(
                    name="fppy",
                    ok=False,
                    return_code=None,
                    work_dir=str(work_fppy),
                    pabev_path=str(_parity_output_candidates(work_fppy)[0]),
                    stdout_path=str(work_fppy / "fppy.stdout.txt"),
                    stderr_path=str(work_fppy / "fppy.stderr.txt"),
                    details=fppy_details,
                ),
                "error": f"fp-py run failed: {type(exc).__name__}: {exc}",
                "fppy_details": fppy_details,
            }

    # Phase 2: run both engines concurrently for one parity invocation.
    with ThreadPoolExecutor(max_workers=2) as executor:
        fpexe_future = executor.submit(_run_fpexe_engine)
        fppy_future = executor.submit(_run_fppy_engine)
        # Consume in a stable order for deterministic report payloads.
        fpexe_outcome = fpexe_future.result()
        fppy_outcome = fppy_future.result()

    engine_runs["fpexe"] = fpexe_outcome["summary"]
    engine_runs["fppy"] = fppy_outcome["summary"]
    fpexe_solution_errors = list(fpexe_outcome.get("fpexe_solution_errors") or [])
    fpexe_retry = dict(fpexe_outcome.get("fpexe_retry") or fpexe_retry_template)

    fpexe_error = fpexe_outcome.get("error")
    fppy_error = fppy_outcome.get("error")

    if fpexe_error and not fppy_error:
        result = ParityResult(
            status="engine_failure",
            run_dir=str(run_dir),
            scenario_name=config_run.name,
            input_fingerprint=fingerprint,
            fingerprint_ok=fingerprint_ok,
            fingerprint_message=fingerprint_message,
            engine_runs=engine_runs,
            exit_code=4,
            pabev_detail={
                "error": str(fpexe_error),
                "fpexe_details": dict(fpexe_outcome.get("fpexe_details") or {}),
                "fpexe_stdout_path": str(fpexe_stdout_path),
                "fpexe_stderr_path": str(fpexe_stderr_path),
                "fpexe_retry": fpexe_retry,
            },
        )
        (run_dir / "parity_report.json").write_text(
            json.dumps(result.to_dict(), indent=2) + "\n", encoding="utf-8"
        )
        return result

    if fppy_error and not fpexe_error:
        result = ParityResult(
            status="engine_failure",
            run_dir=str(run_dir),
            scenario_name=config_run.name,
            input_fingerprint=fingerprint,
            fingerprint_ok=fingerprint_ok,
            fingerprint_message=fingerprint_message,
            engine_runs=engine_runs,
            exit_code=4,
            pabev_detail={
                "error": str(fppy_error),
                "fppy_stdout_path": str(work_fppy / "fppy.stdout.txt"),
                "fppy_stderr_path": str(work_fppy / "fppy.stderr.txt"),
                "fppy_pabev_path": str(work_fppy / "PABEV.TXT"),
                "eq_flags_preset": str(fppy_preset),
                "eq_iter_trace": bool(fppy_eq_iter_trace),
                "eq_iter_trace_period": fppy_eq_iter_trace_period,
                "eq_iter_trace_targets": fppy_eq_iter_trace_targets,
                "eq_iter_trace_max_events": fppy_eq_iter_trace_max_events,
                "fpexe_solution_errors_present": bool(fpexe_solution_errors),
                "warnings": (
                    ["fp.exe solution errors present; treat diffs as unreliable."]
                    if fpexe_solution_errors
                    else []
                ),
            },
        )
        (run_dir / "parity_report.json").write_text(
            json.dumps(result.to_dict(), indent=2) + "\n", encoding="utf-8"
        )
        return result

    if fpexe_error and fppy_error:
        result = ParityResult(
            status="engine_failure",
            run_dir=str(run_dir),
            scenario_name=config_run.name,
            input_fingerprint=fingerprint,
            fingerprint_ok=fingerprint_ok,
            fingerprint_message=fingerprint_message,
            engine_runs=engine_runs,
            exit_code=4,
            pabev_detail={
                "error": f"{fpexe_error} | {fppy_error}",
                "fpexe_details": dict(fpexe_outcome.get("fpexe_details") or {}),
                "fpexe_stdout_path": str(fpexe_stdout_path),
                "fpexe_stderr_path": str(fpexe_stderr_path),
                "fpexe_retry": fpexe_retry,
                "fppy_stdout_path": str(work_fppy / "fppy.stdout.txt"),
                "fppy_stderr_path": str(work_fppy / "fppy.stderr.txt"),
                "fppy_pabev_path": str(work_fppy / "PABEV.TXT"),
                "eq_flags_preset": str(fppy_preset),
                "eq_iter_trace": bool(fppy_eq_iter_trace),
                "eq_iter_trace_period": fppy_eq_iter_trace_period,
                "eq_iter_trace_targets": fppy_eq_iter_trace_targets,
                "eq_iter_trace_max_events": fppy_eq_iter_trace_max_events,
                "fpexe_solution_errors_present": bool(fpexe_solution_errors),
                "warnings": (
                    ["fp.exe solution errors present; treat diffs as unreliable."]
                    if fpexe_solution_errors
                    else []
                ),
            },
        )
        (run_dir / "parity_report.json").write_text(
            json.dumps(result.to_dict(), indent=2) + "\n", encoding="utf-8"
        )
        return result

    left = _resolve_parity_output_path(work_fpexe, expected_outputs=expected_outputs)
    right = _resolve_parity_output_path(work_fppy, expected_outputs=expected_outputs)
    if not left.exists() or not right.exists():
        missing = []
        if not left.exists():
            missing.extend(str(path) for path in _parity_output_candidates(work_fpexe) if not path.exists())
        if not right.exists():
            missing.extend(str(path) for path in _parity_output_candidates(work_fppy) if not path.exists())
        result = ParityResult(
            status="missing_output",
            run_dir=str(run_dir),
            scenario_name=config_run.name,
            input_fingerprint=fingerprint,
            fingerprint_ok=fingerprint_ok,
            fingerprint_message=fingerprint_message,
            engine_runs=engine_runs,
            exit_code=4,
            pabev_detail={
                "error": (
                    "Missing parity outputs after engine execution (expected PABEV.TXT or PACEV.TXT). "
                    "Check fp.exe/fp-py stdout and stderr artifacts for diagnostics."
                ),
                "missing_paths": missing,
                "fpexe_return_code": engine_runs.get("fpexe").return_code
                if engine_runs.get("fpexe")
                else None,
                "fppy_return_code": engine_runs.get("fppy").return_code
                if engine_runs.get("fppy")
                else None,
                "fpexe_stdout_path": str(fpexe_stdout_path),
                "fpexe_stderr_path": str(fpexe_stderr_path),
                "fppy_stdout_path": str(work_fppy / "fppy.stdout.txt"),
                "fppy_stderr_path": str(work_fppy / "fppy.stderr.txt"),
                "fpexe_retry": fpexe_retry,
                "fpexe_solution_errors_present": bool(fpexe_solution_errors),
                "warnings": (
                    ["fp.exe solution errors present; treat diffs as unreliable."]
                    if fpexe_solution_errors
                    else []
                ),
            },
        )
        (run_dir / "parity_report.json").write_text(
            json.dumps(result.to_dict(), indent=2) + "\n", encoding="utf-8"
        )
        return result

    expected_compare_end = str(gate.pabev_end or config_run.forecast_end).strip()
    truncated_outputs: list[dict[str, Any]] = []
    parse_errors: list[dict[str, Any]] = []
    try:
        expected_end_period = pabev_parity.PabevPeriod.parse(expected_compare_end)
    except Exception:  # pragma: no cover - defensive for malformed config
        expected_end_period = None
    if expected_end_period is not None:
        for engine_name, pabev_path in (("fpexe", left), ("fppy", right)):
            try:
                periods, _series = pabev_parity.parse_pabev(pabev_path)
            except Exception as exc:
                parse_errors.append({
                    "engine": engine_name,
                    "pabev_path": str(pabev_path),
                    "error": str(exc),
                })
                continue
            if expected_end_period not in periods:
                parsed_end = str(periods[-1]) if periods else None
                truncated_outputs.append({
                    "engine": engine_name,
                    "pabev_path": str(pabev_path),
                    "parsed_end_period": parsed_end,
                    "expected_compare_end": expected_compare_end,
                    "work_dir": engine_runs.get(engine_name).work_dir
                    if engine_runs.get(engine_name)
                    else "",
                    "stdout_path": engine_runs.get(engine_name).stdout_path
                    if engine_runs.get(engine_name)
                    else "",
                    "stderr_path": engine_runs.get(engine_name).stderr_path
                    if engine_runs.get(engine_name)
                    else "",
                })

    if parse_errors or truncated_outputs:
        detail_parts: list[str] = []
        if parse_errors:
            engines = ", ".join(item.get("engine", "?") for item in parse_errors)
            detail_parts.append(f"PABEV parse failed for engine(s): {engines}.")
        if truncated_outputs:
            engines = ", ".join(item.get("engine", "?") for item in truncated_outputs)
            detail_parts.append(
                "PABEV truncated before expected compare end "
                f"{expected_compare_end} for engine(s): {engines}."
            )
        detail_message = " ".join(detail_parts)
        result = ParityResult(
            status="missing_output",
            run_dir=str(run_dir),
            scenario_name=config_run.name,
            input_fingerprint=fingerprint,
            fingerprint_ok=fingerprint_ok,
            fingerprint_message=fingerprint_message,
            engine_runs=engine_runs,
            exit_code=4,
            pabev_detail={
                "error": detail_message,
                "expected_compare_end": expected_compare_end,
                "truncated_outputs": truncated_outputs,
                "parse_errors": parse_errors,
                "fpexe_stdout_path": str(fpexe_stdout_path),
                "fpexe_stderr_path": str(fpexe_stderr_path),
                "fppy_stdout_path": str(work_fppy / "fppy.stdout.txt"),
                "fppy_stderr_path": str(work_fppy / "fppy.stderr.txt"),
                "fpexe_retry": fpexe_retry,
                "fpexe_solution_errors_present": bool(fpexe_solution_errors),
                "warnings": (
                    ["fp.exe solution errors present; treat diffs as unreliable."]
                    if fpexe_solution_errors
                    else []
                ),
            },
        )
        (run_dir / "parity_report.json").write_text(
            json.dumps(result.to_dict(), indent=2) + "\n", encoding="utf-8"
        )
        return result

    compare_ok, detail = pabev_parity.toleranced_compare(
        left,
        right,
        start=str(gate.pabev_start),
        end=str(gate.pabev_end) if gate.pabev_end is not None else None,
        variables=(
            frozenset(str(v).upper() for v in (config_run.track_variables or []))
            if Path(left).name.upper() not in {"PABEV.TXT", "PACEV.TXT"}
            else None
        ),
        atol=float(gate.atol),
        rtol=float(gate.rtol),
        top=20,
        missing_sentinels=frozenset(float(x) for x in gate.missing_sentinels),
        discrete_eps=float(gate.discrete_eps),
        signflip_eps=float(gate.signflip_eps),
        collect_period_stats=bool(gate.drift.enabled),
    )

    drift_check = None
    if gate.drift.enabled:
        drift_check = _drift_check_from_period_stats(
            detail.get("per_period_stats") or [],
            drift=gate.drift,
        )

    hard_fail_cells = int(detail.get("hard_fail_cell_count", 0))
    exit_code = 0
    status = "ok"
    if hard_fail_cells > 0:
        exit_code = 3
        status = "hard_fail"
    elif not bool(compare_ok):
        exit_code = 2
        status = "gate_failed"
    if drift_check is not None and drift_check.get("status") == "failed" and exit_code == 0:
        # Drift failures are treated as gate failures (not hard-fails).
        exit_code = 2
        status = "drift_failed"

    result = ParityResult(
        status=status,
        run_dir=str(run_dir),
        scenario_name=config_run.name,
        input_fingerprint=fingerprint,
        fingerprint_ok=fingerprint_ok,
        fingerprint_message=fingerprint_message,
        engine_runs=engine_runs,
        pabev_compare_ok=bool(compare_ok),
        pabev_detail={
            **dict(detail),
            "fpexe_retry": fpexe_retry,
            "fpexe_solution_errors_present": bool(fpexe_solution_errors),
            "warnings": (
                ["fp.exe solution errors present; treat diffs as unreliable."]
                if fpexe_solution_errors
                else []
            ),
        },
        drift_check=drift_check,
        exit_code=int(exit_code),
    )
    (run_dir / "parity_report.json").write_text(
        json.dumps(result.to_dict(), indent=2) + "\n", encoding="utf-8"
    )
    # Dashboard convenience: keep a primary LOADFORMAT copy at run_dir root so
    # run discovery (`scan_artifacts`) can treat parity runs as having output.
    # Prefer fp.exe output as the canonical reference when present.
    try:
        primary = left if left.exists() else right
        if primary.exists():
            preserved = run_dir / primary.name
            shutil.copy2(primary, preserved)
            shutil.copy2(preserved, run_dir / "LOADFORMAT.DAT")
    except Exception:
        pass
    return result
