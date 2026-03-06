"""Parity golden save/compare helpers based on PABEV artifacts."""

from __future__ import annotations

import json
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fppy.pabev_parity import (
    toleranced_compare,
)

_SCHEMA_VERSION = 1


def _producer_version() -> str:
    try:
        from fp_wraptr import __version__

        return str(__version__)
    except Exception:  # pragma: no cover - extremely defensive
        return "unknown"


def _with_schema(payload: dict[str, Any]) -> dict[str, Any]:
    payload = dict(payload)
    payload.setdefault("schema_version", _SCHEMA_VERSION)
    payload.setdefault("producer_version", _producer_version())
    return payload


_RUN_DIR_RE = re.compile(r"^(?P<scenario_name>.+)_(?P<timestamp>\d{8}_\d{6})$")


@dataclass(frozen=True)
class RegressionGate:
    """Tolerance and invariant settings used for parity regression comparison."""

    start: str | None = "2025.4"
    atol: float = 1e-3
    rtol: float = 1e-6
    missing_sentinels: tuple[float, ...] = (-99.0,)
    discrete_eps: float = 1e-12
    signflip_eps: float = 1e-3

    def to_dict(self) -> dict[str, Any]:
        return {
            "start": self.start,
            "atol": float(self.atol),
            "rtol": float(self.rtol),
            "missing_sentinels": [float(x) for x in self.missing_sentinels],
            "discrete_eps": float(self.discrete_eps),
            "signflip_eps": float(self.signflip_eps),
        }


@dataclass(frozen=True)
class RegressionSignals:
    """Comparable parity regression signals extracted from PABEV pairs."""

    missing_left: frozenset[str]
    missing_right: frozenset[str]
    hard_fail_keys: frozenset[tuple[str, str, str]]
    diff_variables: frozenset[str]
    compare_status: str = "ok"
    compare_reason: str = ""


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must be a JSON object")
    return payload


def _scenario_name_from(run_dir: Path, report: dict[str, Any], override: str | None) -> str:
    if override and override.strip():
        return override.strip()
    scenario = report.get("scenario_name")
    if isinstance(scenario, str) and scenario.strip():
        return scenario.strip()
    match = _RUN_DIR_RE.match(run_dir.name)
    if match:
        return str(match.group("scenario_name"))
    return run_dir.name


def _load_report(run_dir: Path) -> dict[str, Any]:
    report_path = Path(run_dir) / "parity_report.json"
    if not report_path.exists():
        raise FileNotFoundError(f"Missing parity report: {report_path}")
    return _read_json(report_path)


def _gate_from_report(report: dict[str, Any]) -> RegressionGate:
    detail = report.get("pabev_detail")
    if not isinstance(detail, dict):
        return RegressionGate()

    raw_missing = detail.get("missing_sentinels", (-99.0,))
    missing_values: list[float] = []
    if isinstance(raw_missing, (list, tuple)):
        for item in raw_missing:
            try:
                missing_values.append(float(item))
            except (TypeError, ValueError):
                continue
    if not missing_values:
        missing_values = [-99.0]

    start = detail.get("start")
    if not isinstance(start, str):
        start = None

    return RegressionGate(
        start=start,
        atol=float(detail.get("atol", 1e-3)),
        rtol=float(detail.get("rtol", 1e-6)),
        missing_sentinels=tuple(sorted(set(missing_values))),
        discrete_eps=float(detail.get("discrete_eps", 1e-12)),
        signflip_eps=float(detail.get("signflip_eps", 1e-3)),
    )


def _load_gate(golden_scenario_dir: Path, fallback_report: dict[str, Any]) -> RegressionGate:
    gate_path = golden_scenario_dir / "gate.json"
    if gate_path.exists():
        payload = _read_json(gate_path)
        return RegressionGate(
            start=payload.get("start"),
            atol=float(payload.get("atol", 1e-3)),
            rtol=float(payload.get("rtol", 1e-6)),
            missing_sentinels=tuple(float(x) for x in payload.get("missing_sentinels", (-99.0,))),
            discrete_eps=float(payload.get("discrete_eps", 1e-12)),
            signflip_eps=float(payload.get("signflip_eps", 1e-3)),
        )
    return _gate_from_report(fallback_report)


def _pabev_paths(run_dir: Path) -> tuple[Path, Path]:
    run_dir = Path(run_dir)
    left_default = run_dir / "work_fpexe" / "PABEV.TXT"
    right_default = run_dir / "work_fppy" / "PABEV.TXT"
    if left_default.exists() and right_default.exists():
        return left_default, right_default

    report_path = run_dir / "parity_report.json"
    report: dict[str, Any] | None = None
    if report_path.exists():
        try:
            report = _read_json(report_path)
        except Exception:
            report = None

    engine_runs = report.get("engine_runs") if isinstance(report, dict) else None
    if not isinstance(engine_runs, dict):
        engine_runs = {}

    def _resolve_candidate(value: object) -> Path | None:
        if not isinstance(value, str):
            return None
        raw = value.strip()
        if not raw:
            return None
        candidate = Path(raw)
        candidates: list[Path] = []
        if candidate.is_absolute():
            candidates.append(candidate)
        else:
            candidates.append(run_dir / candidate)
            candidates.append(Path.cwd() / candidate)
            candidates.append(candidate)
        for path in candidates:
            if path.exists():
                return path
        return None

    left_report = _resolve_candidate(
        (engine_runs.get("fpexe") or {}).get("pabev_path") if isinstance(engine_runs.get("fpexe"), dict) else None
    )
    right_report = _resolve_candidate(
        (engine_runs.get("fppy") or {}).get("pabev_path") if isinstance(engine_runs.get("fppy"), dict) else None
    )
    if left_report is not None and right_report is not None:
        return left_report, right_report

    if not left_default.exists():
        raise FileNotFoundError(f"Missing fp.exe PABEV artifact: {left_default}")
    if not right_default.exists():
        raise FileNotFoundError(f"Missing fp-py PABEV artifact: {right_default}")
    return left_default, right_default


def _compute_signals(left_path: Path, right_path: Path, gate: RegressionGate) -> RegressionSignals:
    compare_ok, detail = toleranced_compare(
        left_path,
        right_path,
        start=gate.start,
        atol=float(gate.atol),
        rtol=float(gate.rtol),
        # Request a very large slice so all diff variables are represented in detail.
        top=1_000_000,
        # Regression gating must consider all hard-fail cells, not just the report sample.
        hard_fail_top=None,
        missing_sentinels=frozenset(float(x) for x in gate.missing_sentinels),
        discrete_eps=float(gate.discrete_eps),
        signflip_eps=float(gate.signflip_eps),
    )
    if not isinstance(detail, dict):
        raise ValueError("Unexpected toleranced_compare payload: expected detail dictionary")

    hard_fail_keys: set[tuple[str, str, str]] = set()
    for row in detail.get("hard_fail_cells", []) or []:
        if not isinstance(row, dict):
            continue
        variable = str(row.get("variable", "")).strip()
        period = str(row.get("period", "")).strip()
        reason = str(row.get("reason", "")).strip()
        if variable and period and reason:
            hard_fail_keys.add((variable, period, reason))

    diff_vars: set[str] = set()
    for row in detail.get("top_first_diffs", []) or []:
        if not isinstance(row, dict):
            continue
        variable = str(row.get("variable", "")).strip()
        if variable:
            diff_vars.add(variable)

    # If diff count claims more than listed names, keep behavior explicit.
    claimed_diff_count = int(detail.get("diff_variable_count", len(diff_vars)) or 0)
    if claimed_diff_count > len(diff_vars):
        raise ValueError(
            "toleranced_compare detail truncates diff variable names; increase top to cover full set"
        )

    return RegressionSignals(
        missing_left=frozenset(str(x) for x in detail.get("missing_left", []) or []),
        missing_right=frozenset(str(x) for x in detail.get("missing_right", []) or []),
        hard_fail_keys=frozenset(hard_fail_keys),
        diff_variables=frozenset(diff_vars),
        compare_status=str(detail.get("status", "ok" if compare_ok else "failed")),
        compare_reason=str(detail.get("reason", "")),
    )


def _format_hard_fail_set(keys: frozenset[tuple[str, str, str]]) -> list[dict[str, str]]:
    return [
        {"variable": var, "period": period, "reason": reason}
        for var, period, reason in sorted(keys)
    ]


def save_parity_golden(
    run_dir: Path,
    golden_dir: Path,
    *,
    scenario_name: str | None = None,
) -> Path:
    """Save parity report + PABEV artifacts as a golden baseline."""

    run_dir = Path(run_dir)
    golden_dir = Path(golden_dir)
    report = _load_report(run_dir)
    scenario = _scenario_name_from(run_dir, report, scenario_name)
    target_dir = golden_dir / scenario
    target_dir.mkdir(parents=True, exist_ok=True)

    shutil.copy2(run_dir / "parity_report.json", target_dir / "parity_report.json")

    left, right = _pabev_paths(run_dir)
    (target_dir / "work_fpexe").mkdir(parents=True, exist_ok=True)
    (target_dir / "work_fppy").mkdir(parents=True, exist_ok=True)
    shutil.copy2(left, target_dir / "work_fpexe" / "PABEV.TXT")
    shutil.copy2(right, target_dir / "work_fppy" / "PABEV.TXT")

    gate = _gate_from_report(report)
    (target_dir / "gate.json").write_text(
        json.dumps(gate.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return target_dir


def compare_parity_to_golden(
    run_dir: Path,
    golden_dir: Path,
    *,
    scenario_name: str | None = None,
) -> dict[str, Any]:
    """Compare current parity run against stored golden artifacts."""

    run_dir = Path(run_dir)
    golden_dir = Path(golden_dir)
    current_report = _load_report(run_dir)
    scenario = _scenario_name_from(run_dir, current_report, scenario_name)

    golden_scenario_dir = golden_dir / scenario
    golden_report_path = golden_scenario_dir / "parity_report.json"
    if not golden_report_path.exists():
        raise FileNotFoundError(f"Missing golden parity report: {golden_report_path}")
    golden_report = _read_json(golden_report_path)

    gate = _load_gate(golden_scenario_dir, golden_report)

    current_left, current_right = _pabev_paths(run_dir)
    golden_left, golden_right = _pabev_paths(golden_scenario_dir)

    try:
        current = _compute_signals(current_left, current_right, gate)
        golden = _compute_signals(golden_left, golden_right, gate)
    except (TypeError, ValueError) as exc:
        return _with_schema({
            "status": "failed",
            "reason": "compare_exception",
            "error": f"{type(exc).__name__}: {exc}",
            "scenario_name": scenario,
            "gate": gate.to_dict(),
            "golden_dir": str(golden_scenario_dir),
            "run_dir": str(run_dir),
            "new_findings": {
                "missing_left": [],
                "missing_right": [],
                "hard_fail_cells": [],
                "diff_variables": [],
            },
            "resolved_findings": {
                "missing_left": [],
                "missing_right": [],
                "hard_fail_cells": [],
                "diff_variables": [],
            },
            "counts": {
                "new_missing_left": 0,
                "new_missing_right": 0,
                "new_hard_fail_cells": 0,
                "new_diff_variables": 0,
                "resolved_missing_left": 0,
                "resolved_missing_right": 0,
                "resolved_hard_fail_cells": 0,
                "resolved_diff_variables": 0,
            },
        })

    if current.compare_reason == "periods_mismatch" or golden.compare_reason == "periods_mismatch":
        return _with_schema({
            "status": "failed",
            "reason": "periods_mismatch",
            "error": "PABEV period ranges are not aligned between compared artifacts",
            "scenario_name": scenario,
            "gate": gate.to_dict(),
            "golden_dir": str(golden_scenario_dir),
            "run_dir": str(run_dir),
            "compare_status": {
                "current": {
                    "status": current.compare_status,
                    "reason": current.compare_reason,
                },
                "golden": {
                    "status": golden.compare_status,
                    "reason": golden.compare_reason,
                },
            },
            "new_findings": {
                "missing_left": [],
                "missing_right": [],
                "hard_fail_cells": [],
                "diff_variables": [],
            },
            "resolved_findings": {
                "missing_left": [],
                "missing_right": [],
                "hard_fail_cells": [],
                "diff_variables": [],
            },
            "counts": {
                "new_missing_left": 0,
                "new_missing_right": 0,
                "new_hard_fail_cells": 0,
                "new_diff_variables": 0,
                "resolved_missing_left": 0,
                "resolved_missing_right": 0,
                "resolved_hard_fail_cells": 0,
                "resolved_diff_variables": 0,
            },
        })

    new_missing_left = frozenset(sorted(current.missing_left - golden.missing_left))
    new_missing_right = frozenset(sorted(current.missing_right - golden.missing_right))
    new_hard_fail_keys = frozenset(sorted(current.hard_fail_keys - golden.hard_fail_keys))
    new_diff_variables = frozenset(sorted(current.diff_variables - golden.diff_variables))

    resolved_missing_left = frozenset(sorted(golden.missing_left - current.missing_left))
    resolved_missing_right = frozenset(sorted(golden.missing_right - current.missing_right))
    resolved_hard_fail_keys = frozenset(sorted(golden.hard_fail_keys - current.hard_fail_keys))
    resolved_diff_variables = frozenset(sorted(golden.diff_variables - current.diff_variables))

    has_new = bool(
        new_missing_left or new_missing_right or new_hard_fail_keys or new_diff_variables
    )

    return _with_schema({
        "status": "failed" if has_new else "ok",
        "reason": "new_findings" if has_new else "ok",
        "scenario_name": scenario,
        "gate": gate.to_dict(),
        "golden_dir": str(golden_scenario_dir),
        "run_dir": str(run_dir),
        "new_findings": {
            "missing_left": sorted(new_missing_left),
            "missing_right": sorted(new_missing_right),
            "hard_fail_cells": _format_hard_fail_set(new_hard_fail_keys),
            "diff_variables": sorted(new_diff_variables),
        },
        "resolved_findings": {
            "missing_left": sorted(resolved_missing_left),
            "missing_right": sorted(resolved_missing_right),
            "hard_fail_cells": _format_hard_fail_set(resolved_hard_fail_keys),
            "diff_variables": sorted(resolved_diff_variables),
        },
        "counts": {
            "new_missing_left": len(new_missing_left),
            "new_missing_right": len(new_missing_right),
            "new_hard_fail_cells": len(new_hard_fail_keys),
            "new_diff_variables": len(new_diff_variables),
            "resolved_missing_left": len(resolved_missing_left),
            "resolved_missing_right": len(resolved_missing_right),
            "resolved_hard_fail_cells": len(resolved_hard_fail_keys),
            "resolved_diff_variables": len(resolved_diff_variables),
        },
    })


def write_regression_report(payload: dict[str, Any], run_dir: Path) -> Path:
    """Persist parity regression compare output under a run directory."""

    run_dir = Path(run_dir)
    report_path = run_dir / "parity_regression.json"
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report_path
