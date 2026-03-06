#!/usr/bin/env python3
"""Run a fast parity sweep across scenario YAMLs using a single-quarter horizon.

This is intended as a "ready to run real scenarios" validation pass:
- exercises overrides + bundle writing + fp.exe + fppy + PABEV compare
- avoids long runtimes by shrinking SMPL horizon to one period

Run with:
  PYTHONDONTWRITEBYTECODE=1 python -B scripts/parity_sweep_examples_smoke.py
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from fp_wraptr.analysis.parity import run_parity
from fp_wraptr.scenarios.config import ScenarioConfig


def _discover_yaml(paths: list[str]) -> list[Path]:
    out: list[Path] = []
    for raw in paths:
        p = Path(raw)
        if any(ch in raw for ch in ["*", "?", "["]):
            out.extend(sorted(Path().glob(raw)))
        elif p.is_dir():
            out.extend(sorted(p.glob("*.yaml")))
        else:
            out.append(p)
    # Deduplicate while preserving order.
    seen: set[Path] = set()
    deduped: list[Path] = []
    for p in out:
        p = p.resolve()
        if p not in seen:
            seen.add(p)
            deduped.append(p)
    return deduped


def _default_sweep_workers() -> int:
    cores = os.cpu_count()
    if isinstance(cores, int) and cores > 0:
        return max(1, cores // 2)
    return 4


def _resolve_sweep_workers(cli_value: int | None) -> int:
    if cli_value is not None:
        if cli_value < 1:
            raise ValueError("--max-workers must be >= 1")
        return int(cli_value)

    env_raw = os.getenv("FP_PARITY_MAX_WORKERS", "").strip()
    if env_raw:
        try:
            env_value = int(env_raw)
        except ValueError as exc:
            raise ValueError("FP_PARITY_MAX_WORKERS must be an integer >= 1") from exc
        if env_value < 1:
            raise ValueError("FP_PARITY_MAX_WORKERS must be >= 1")
        return env_value

    return _default_sweep_workers()


def _run_smoke_parity_case(ypath: Path, cfg: ScenarioConfig, *, period: str, output_dir: Path) -> dict:
    # Build a short-horizon copy without touching the original YAML.
    patches = dict(cfg.input_patches or {})
    # This is the stock FM solve horizon line we shrink for smoke runs.
    patches.setdefault("SMPL 2025.4 2029.4;", f"SMPL {period} {period};")

    cfg_smoke = cfg.model_copy(
        deep=True,
        update={
            "name": f"{cfg.name}_smoke",
            "description": f"{cfg.description} (smoke: {period} only)".strip(),
            "forecast_start": str(period),
            "forecast_end": str(period),
            "input_patches": patches,
        },
    )

    try:
        result = run_parity(cfg_smoke, output_dir=output_dir, fp_home_override=None)
        detail = result.pabev_detail or {}
        return {
            "scenario_yaml": str(ypath),
            "scenario_name": cfg.name,
            "run_name": cfg_smoke.name,
            "run_dir": result.run_dir,
            "status": result.status,
            "exit_code": result.exit_code,
            "hard_fail_cell_count": int(detail.get("hard_fail_cell_count", 0)),
            "max_abs_diff": float(detail.get("max_abs_diff", 0.0)),
        }
    except Exception as exc:
        return {
            "scenario_yaml": str(ypath),
            "scenario_name": cfg.name,
            "run_name": cfg_smoke.name,
            "run_dir": "",
            "status": "exception",
            "exit_code": 99,
            "error": f"{type(exc).__name__}: {exc}",
        }


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "inputs",
        nargs="*",
        default=["examples/*.yaml"],
        help="YAML scenario(s) or glob(s) (default: examples/*.yaml).",
    )
    ap.add_argument(
        "--output-dir",
        default="artifacts/examples_smoke_sweep",
        help="Artifacts output directory.",
    )
    ap.add_argument(
        "--period",
        default="2025.4",
        help="Single period to solve/compare (default: 2025.4).",
    )
    ap.add_argument(
        "--exclude",
        action="append",
        default=["baseline_smoke"],
        help="Scenario name(s) to exclude (repeatable). Default excludes baseline_smoke.",
    )
    ap.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable summary JSON to stdout.",
    )
    ap.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help=(
            "Max concurrent scenario workers. "
            "Precedence: --max-workers > FP_PARITY_MAX_WORKERS > default policy."
        ),
    )
    ns = ap.parse_args(argv)

    yaml_paths = _discover_yaml(ns.inputs)
    if not yaml_paths:
        print("No scenario YAMLs found.", file=sys.stderr)
        return 2

    try:
        max_workers = _resolve_sweep_workers(ns.max_workers)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    output_dir = Path(ns.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    excluded = set(ns.exclude)
    jobs: list[tuple[Path, ScenarioConfig]] = []
    for ypath in yaml_paths:
        cfg = ScenarioConfig.from_yaml(ypath)
        if cfg.name in excluded:
            continue
        jobs.append((ypath, cfg))

    results: list[dict] = []
    # Phase 3: run scenario sweeps concurrently with bounded workers.
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(_run_smoke_parity_case, ypath, cfg, period=ns.period, output_dir=output_dir)
            for ypath, cfg in jobs
        ]
        # Collect in submission order for deterministic reporting.
        for future in futures:
            results.append(future.result())

    if ns.json:
        print(json.dumps({"results": results}, indent=2, sort_keys=True))
        return 0

    # Human scan: fixed-width-ish table.
    hdr = f"{'scenario':<20} {'status':<14} {'exit':<4} {'hard_fail':<9} {'max_abs':<10} run_dir"
    print(hdr)
    print("-" * len(hdr))
    for row in results:
        print(
            f"{row.get('scenario_name', ''):<20} {row.get('status', ''):<14} "
            f"{row.get('exit_code', '')!s:<4} {row.get('hard_fail_cell_count', '')!s:<9} "
            f"{row.get('max_abs_diff', '')!s:<10} {row.get('run_dir', '')}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
