# Stock FM Fast Mode

This runbook documents the fastest currently validated stock-FM path with low divergence versus `fp.exe`.

## What this mode is

- Scope: stock FM (`FM/fminput.txt`) only.
- Goal: maximize `fppy` speed while keeping divergence tightly bounded versus `fp.exe`.
- Current validated profile (2026-03-07):
  - runtime: ~99-101s (about 5x faster than the prior ~506.5s baseline)
  - `max % diff` vs `fp.exe`: `0.0376329663%`
  - `p95 % diff` vs `fp.exe`: `0.0000441579%`
  - convergence: `converged`

## Recommended scenario settings

Use explicit settings (do not rely on omitted/default preset resolution):

```yaml
name: stock_fm_fast
description: "Stock FM fast validated path"
fp_home: ../FM
input_file: fminput.txt
forecast_start: "2025.4"
forecast_end: "2029.4"
backend: fppy
fppy:
  eq_flags_preset: parity
  eq_structural_read_cache: numpy_columns
```

Why explicit `eq_flags_preset`:
- In setupsolve/period-scoped contexts, omitted/default preset selection may resolve to parity-like EQ behavior.
- For reproducibility, pin the preset in YAML.

## Run command

```bash
scripts/uvsafe fp run <your-scenario>.yaml \
  --backend fppy \
  --fp-home /path/to/FM \
  --output-dir /private/tmp/fp_wraptr_stock_fast_runs
```

## Validate against `fp.exe` (speed + divergence)

1. Produce an `fp.exe` reference run directory.
2. Evaluate the `fppy` run against that reference with the stock KPI harness:

```bash
scripts/uvsafe python -B scripts/stock_fm_speed_divergence_eval.py \
  --label stock_fast_eval \
  --run-dir <fppy_run_dir> \
  --fpexe-output <fpexe_run_dir>/work_fpexe/PACEV.TXT \
  --window-start 2025.4 \
  --out-json /private/tmp/fp_wraptr_stock_fast_runs/stock_fast_eval.json
```

## Acceptance gates

Use these stock-FM gates:

- `max % diff <= 3.0`
- `p95 % diff <= 1.0`
- stop reason is converged
- convergence classification is converged

Current validated results are substantially tighter than those gates.

## Notes on threading

`num_threads` currently sets BLAS/OpenMP environment variables only. In stock-FM solver hotspots, measured within-run speedup from this setting was not material. Prefer solver-path optimizations and cache-enabled parity settings for practical speed gains.
