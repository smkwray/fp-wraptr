# Parity Mode (`fp.exe` vs `fppy`)

This page is the operator playbook for parity runs and scenario changes.

fp-wraptr parity uses the vendored minimal `fppy` execution/parity core (module name `fp_py`) and compares engine outputs on the `PABEV.TXT` contract.

By default, `fp parity` runs the `fppy` backend with `fppy.eq_flags_preset: parity` (EQ backfill enabled and `SETUPSOLVE` honored). You can override this via the scenario's `fppy.eq_flags_preset`.

## Timeouts (fppy)

Full-horizon parity runs can take a while under the `parity` preset. If you hit `status=engine_failure` with an error like `fppy mini-run timed out`, raise the timeout in your scenario YAML:

```yaml
fppy:
  timeout_seconds: 3600
```

## `eq_flags_preset=parity` default and opt-out

- Default behavior for `fp parity` is `fppy.eq_flags_preset: parity`.
- The parity preset targets setupsolve-style solve semantics (including EQ backfill + iteration controls) so fppy execution matches parity expectations more closely.
- This preset is the main reason baseline parity runs can now reach exact/near-exact alignment.
- To opt out, set the scenario YAML explicitly to `default`:

```yaml
fppy:
  eq_flags_preset: default
```

- Use `default` only when intentionally testing legacy/non-parity behavior; parity artifacts and dashboard views will still surface the active preset so results remain auditable.

## Identity Overlay (Why Parity Can Diverge At A Boundary Quarter)

When `fppy.eq_flags_preset=parity` is active, fp-wraptr writes a wrapper deck
(`work_fppy/fppy_fminput.txt`) that injects a pre-`SOLVE` “identity overlay”
extracted from `FM/fminput.txt` (`work_fppy/fppy_identity_overlay.txt`).

This overlay exists because fp.exe runs with compiled model state; many scenario
decks omit large blocks of baseline `CREATE`/`GENR`/`IDENT`/`LHS` definitions
that still affect stored series and therefore `PRINTVAR ... LOADFORMAT` output.

Important details:

- The overlay now preserves `SMPL ...;` statements from the baseline deck so
  windowed `CREATE`/`GENR` blocks keep their intended scope.
- After inserting the overlay, fp-wraptr restores the scenario deck’s current
  `SMPL` window immediately before `SOLVE`. Without this, a baseline overlay
  `SMPL` can “leak” into the solve window and distort boundary-quarter values
  (for example trend-break helper series like `D2`, `CNST2L2`, `TBL2`), which
  can cascade into sign-flip hard fails.

If parity suddenly diverges at the first history/forecast boundary quarter:

1. Inspect `work_fppy/fppy_fminput.txt` around the inserted overlay block.
2. Confirm `work_fppy/fppy_identity_overlay.txt` includes the expected `SMPL`
   statements from the baseline deck.
3. Confirm the last `SMPL` before the `SOLVE ...;` line in `work_fppy/fppy_fminput.txt`
   matches the intended solve window (often the scenario `forecast_start` quarter
   when running `--quick`).

## `pd.eval` function-surface workaround (why it exists)

`fppy` evaluates FP expressions through `pd.eval(..., engine="python")`. In this path,
direct numpy module-attribute calls such as `np.log(...)` can trigger resolver errors on
some pandas/numpy stacks (for example `TypeError: '>' not supported ...`).

To keep parity runs deterministic, expression rewriting routes `LOG/EXP/ABS` through a plain
function surface (`fp.log/fp.exp/fp.abs`) instead of `np.*` names. This behavior is intentional
and regression-tested (`tests/test_fppy_expressions.py`).

Upstream reference: [pandas-dev/pandas#58041](https://github.com/pandas-dev/pandas/issues/58041)

## One-scenario parity run

```bash
uv run fp parity examples/baseline.yaml
```

For quick smoke checks, run with `--quick` (gates comparisons through the scenario `forecast_start` quarter):

```bash
uv run fp parity examples/baseline_smoke.yaml --with-drift --quick
```

Quick smoke with strict gate behavior:

```bash
uv run fp parity examples/baseline_smoke.yaml --with-drift --quick --strict
```

Note: parity reads either `PABEV.TXT` or `PACEV.TXT` when present. fp-wraptr resolves both filenames automatically, so both are accepted in engine artifacts.

With bounded-drift guardrails:

```bash
uv run fp parity examples/baseline.yaml --with-drift
```

## Gate parity comparisons through an end period

Use `--gate-pabev-end` to compare `PABEV.TXT` only through a specified quarter, which is useful for fast smoke gates or focusing on near-term forecast windows.

```bash
uv run fp parity examples/baseline.yaml --with-drift --gate-pabev-end 2025.4 --strict
```

Equivalent via `run` command:

```bash
uv run fp run examples/baseline.yaml --backend both --with-drift
uv run fp run examples/baseline_smoke.yaml --backend both --with-drift --parity-quick
```

Save a parity run as golden artifacts:

```bash
uv run fp parity examples/baseline.yaml --save-golden artifacts/parity-golden
```

Regression-check current run against a saved golden baseline:

```bash
uv run fp parity examples/baseline.yaml --regression artifacts/parity-golden
```

`--save-golden` and `--regression` are intentionally mutually exclusive in one invocation to avoid trivial self-comparison.

- `--save-golden <dir>` stores:
  - `<dir>/<scenario_name>/parity_report.json`
  - `<dir>/<scenario_name>/work_fpexe/PABEV.TXT`
  - `<dir>/<scenario_name>/work_fppy/PABEV.TXT`
  - `<dir>/<scenario_name>/gate.json`
- `--regression <dir>` fails the CLI when new findings appear versus golden:
  - new `missing_left` variables
  - new `missing_right` variables
  - new hard-fail cells (`variable + period + reason`)
  - new diff variables

## Where artifacts are written

Each parity run writes a timestamped directory under `--output-dir` (default `artifacts`), for example:

- `artifacts/<scenario>_<timestamp>/parity_report.json`
- `artifacts/<scenario>_<timestamp>/work_fpexe/PABEV.TXT`
- `artifacts/<scenario>_<timestamp>/work_fppy/PABEV.TXT`
- `artifacts/<scenario>_<timestamp>/work_fpexe/fp-exe.stdout.txt`
- `artifacts/<scenario>_<timestamp>/work_fpexe/fp-exe.stderr.txt`
- `artifacts/<scenario>_<timestamp>/work_fppy/fppy.stdout.txt`
- `artifacts/<scenario>_<timestamp>/work_fppy/fppy.stderr.txt`

## Hard-fail triage and support-gap mapping

One-command loop (parity run + triage artifacts + optional regression gate):

```bash
uv run fp triage loop examples/baseline_smoke.yaml --with-drift --output-dir artifacts/triage_loop_demo
```

For fast smoke, add `--quick`:

```bash
uv run fp triage loop examples/baseline_smoke.yaml --with-drift --quick --output-dir artifacts/triage_loop_quick
```

With regression gate against a saved golden baseline:

```bash
uv run fp triage loop examples/baseline_smoke.yaml --with-drift --regression artifacts/parity-golden
```

Generate full hard-fail cell triage for an existing run:

```bash
uv run fp triage parity-hardfails artifacts/<scenario>_<timestamp>
```

Generate deterministic issue buckets from fppy’s report (if present):

```bash
uv run fp triage fppy-report artifacts/<scenario>_<timestamp>
```

Generate unsupported-statement impact mapping (ranked by hard-fail impact):

```bash
uv run python scripts/support_gap_map.py --run-dir artifacts/<scenario>_<timestamp>
```

This emits:

- `artifacts/<scenario>_<timestamp>/support_gap_map.csv`
- `artifacts/<scenario>_<timestamp>/support_gap_top.md`

Detect solve-window structural fill issues (`zero_fill` and carry-forward `flatline_fill`):

```bash
uv run python scripts/detect_zero_forecast.py --run-dir artifacts/<scenario>_<timestamp>
```

This emits:

- `artifacts/<scenario>_<timestamp>/zero_forecast_offenders.csv`

## Interpreting `parity_report.json` (difference report)

### Divergence interpretation matrix

| `status` | `exit_code` | Meaning | Operator action |
|---|---:|---|---|
| `ok` | `0` | Hard-fail checks passed and numeric gate passed. | Approve parity gate for this scenario/run fingerprint. |
| `hard_fail` | `3` | Semantic mismatch (`missing/discrete/signflip`). | Stop release; investigate hard-fail rows first. |
| `gate_failed` | `2` | Numeric diffs exceeded tolerance gate. | Review top diffs and period-level tails; tune/fix inputs or model changes. |
| `drift_failed` | `2` | Drift guardrail failed (`--with-drift`) even when basic gate passed. | Treat as gate failure; inspect `drift_check.fail_reasons` and trend metrics. |
| `fingerprint_mismatch` | `5` | Current input fingerprint does not match lockfile. | Re-baseline fingerprint after confirming intentional scenario/input changes. |
| `engine_failure` | `4` | Engine execution failed. | Fix runtime/environment issue; rerun parity. |
| `missing_output` | `4` | Expected `PABEV.TXT` artifact was not produced. | Fix execution/output path issue; rerun parity. |

### Metric checklist (read in order)

1. `status` and `exit_code` decide pass/fail for automation and release gating.
2. Hard-fail severity:
   - `pabev_detail.hard_fail_cell_count` (or alias `hard_fail_cells_count`)
   - `pabev_detail.hard_fail_cells` sample for first triage
3. Missing-variable parity health:
   - `pabev_detail.missing_left` and `pabev_detail.missing_right` (compare lengths and names)
4. Numeric divergence magnitude:
   - `pabev_detail.max_abs_diff`
   - `pabev_detail.median_abs_diff`
   - `pabev_detail.p90_abs_diff`
5. Drift guardrail health (`--with-drift` only):
   - `drift_check.status`
   - `drift_check.fail_reasons`
   - `drift_check.max_abs_observed`
   - `drift_check.quantile_growth_factor`

Hard-fail invariants (`missing/discrete/signflip`) are non-negotiable and are enforced regardless of tolerance or drift settings.

## Scenario-change policy (operator)

If a scenario input set changes (deck edits, exogenous changes, forecast var list changes):

1. Treat prior parity approvals as non-authoritative.
2. Re-baseline fingerprint for the changed scenario.
3. Re-run parity (`--with-drift` for sign-off).
4. Block release on hard-fail mismatches.

## Fingerprint lock commands

Write a new fingerprint lockfile:

```bash
scripts/onedrive_safe_env.sh python3 scripts/iss02_acceptance_gate.py \
  --base-dir <SCENARIO_DIR> \
  --write-fingerprint-lock docs/verification/iss02_baseline_fingerprint_<SCENARIO>.json
```

Run acceptance with lockfile + drift:

```bash
scripts/onedrive_safe_env.sh python3 scripts/iss02_acceptance_gate.py \
  --base-dir <SCENARIO_DIR> \
  --with-drift \
  --fingerprint-lock docs/verification/iss02_baseline_fingerprint_<SCENARIO>.json \
  --out-root /private/tmp/fairpy-iss02-acceptance
```

Interpret an existing run without re-running engines:

```bash
scripts/onedrive_safe_env.sh python3 scripts/iss02_acceptance_gate.py \
  --summary-path /private/tmp/fairpy-iss02-acceptance/<TIMESTAMP>/summary.json
```

## `fp.exe` assets installer/provision script

Script: `scripts/ci/provision_fp_assets.sh`

- Inputs:
  - `FP_ASSETS_SOURCE_DIR` **or** `FP_ASSETS_ARCHIVE_URL`
  - optional `FP_ASSETS_BEARER_TOKEN`
  - optional integrity pin `FP_ASSETS_ARCHIVE_SHA256`
  - optional `FP_ASSETS_ARCHIVE_TYPE` (`zip` or `tar`; default `auto`)
- Destination:
  - writes assets into `${GITHUB_WORKSPACE}/FM` (or `<cwd>/FM` if `GITHUB_WORKSPACE` is unset)
- Validation:
  - verifies required files: `fp.exe`, `fminput.txt`, `fmdata.txt`, `fmage.txt`, `fmexog.txt`
  - fails nonzero on missing files or SHA mismatch
