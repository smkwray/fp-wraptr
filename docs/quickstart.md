# Quickstart

## Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) for dependency management
- FP model files (obtain from [fairmodel.econ.yale.edu](https://fairmodel.econ.yale.edu/))

## Installation

```bash
# Clone the repo
git clone https://github.com/shanewray/fp-wraptr.git
cd fp-wraptr

# Install with all extras
uv sync --all-extras
```

## Set up FP model files

Place the Fair-Parke model files in `FM/`:

```
FM/
  fp.exe        # Fair-Parke executable (Windows PE32)
  fminput.txt   # Model input configuration
  fmdata.txt    # Historical time series data
  fmage.txt     # Age demographic variables
  fmexog.txt    # Exogenous variable assumptions
```

These files are gitignored and not included in the repository.

## Parse an FP output file

```bash
# Parse to JSON
uv run fp io parse-output FM/fmout.txt

# Parse to CSV
uv run fp io parse-output FM/fmout.txt --format csv
```

## Parse an FP input file (parser contract)

Input parser output now uses a normalized key style:
- command keys are stored in lowercase (for example: `space`, `setupect`, `commands_by_type["smpl"]`)
- parameter keys are normalized to lowercase/snake_case (for example: `maxvar`, `file`, `solve.params.filevar`)
- no duplicate alias keys are emitted (for example, `setupecst` was removed; use `setupect` only).
- Migration note: canonical keys are now required by all in-repo callers and tests.

If you consume parser output directly, rely on these canonical keys.

## Generate a forecast chart

```bash
uv run fp viz plot FM/fmout.txt --var PCY --var UR --var GDPR
```

## Export a public run bundle

Once you have curated runs under `artifacts/`, export the static GitHub Pages explorer bundle:

```bash
fp export pages --spec public/model-runs.spec.yaml --artifacts-dir artifacts --out-dir public/model-runs
```

That command rewrites `public/model-runs/` with a portable JSON bundle and static browser app.

## Run a scenario

Create a scenario YAML file (see `examples/baseline.yaml`):

```yaml
name: baseline
description: "Default FP model run"
fp_home: FM
forecast_start: "2025.4"
forecast_end: "2029.4"
track_variables:
  - PCY
  - PCPF
  - UR
  - PIEF
  - GDPR
```

Run it:

```bash
uv run fp run examples/baseline.yaml
```

## Parity Quickstart

Use these commands to go from a clean clone to parity triage artifacts and dashboard views.

```bash
# 1) Run fp.exe only
uv run fp run examples/baseline.yaml --backend fpexe --output-dir artifacts/quickstart_fpexe

# 2) Run fppy only
uv run fp run examples/baseline.yaml --backend fppy --output-dir artifacts/quickstart_fppy

# 3) Run parity compare (default fppy preset is parity)
uv run fp parity examples/baseline.yaml --with-drift --output-dir artifacts/quickstart_parity

# 4) Triage from CLI
uv run fp triage fppy-report artifacts/quickstart_parity/<scenario>_<timestamp>
uv run fp triage parity-hardfails artifacts/quickstart_parity/<scenario>_<timestamp>

# 5) Save golden baseline, then run regression compare
uv run fp parity examples/baseline.yaml --save-golden artifacts/parity-golden --output-dir artifacts/quickstart_golden
uv run fp parity examples/baseline.yaml --regression artifacts/parity-golden --output-dir artifacts/quickstart_regression

# 6) Launch dashboard (reads run artifacts from --artifacts-dir)
uv run fp dashboard --artifacts-dir artifacts/quickstart_parity --port 8501
```

Notes:
- Replace `<scenario>_<timestamp>` with the run directory created by `fp parity`.
- Dashboard parity page discovers runs under the selected artifacts directory and renders `parity_report.json`, PABEV artifacts, and optional triage/regression files.

## Fast Parity Smoke (`backend=both`)

Use `examples/baseline_smoke.yaml` for a fast operator/CI smoke pass that still emits parity artifacts.

```bash
# 1) Run both engines through fp run
uv run fp run examples/baseline_smoke.yaml --backend both --with-drift --output-dir artifacts/quickstart_both_smoke

# 2) Open dashboard on this artifact root
uv run fp dashboard --artifacts-dir artifacts/quickstart_both_smoke --port 8501

# 3) Generate triage artifacts from the run root
uv run fp triage fppy-report artifacts/quickstart_both_smoke/<scenario>_<timestamp>
uv run fp triage parity-hardfails artifacts/quickstart_both_smoke/<scenario>_<timestamp>
```

Expected `fp run --backend both` layout:

```text
artifacts/quickstart_both_smoke/<scenario>_<timestamp>/
  scenario.yaml
  parity_report.json                  # copied convenience report at run root
  PABEV.TXT                           # copied convenience PABEV (selected engine)
  parity/<scenario>_<timestamp>/      # full parity artifact payload
    parity_report.json
    work_fpexe/PABEV.TXT
    work_fppy/PABEV.TXT
    work_fppy/fppy_report.json
```

Notes:
- CLI triage accepts the top-level run dir above and resolves nested paths from `parity_report.json`.
- If you want direct file inspection, the canonical per-engine artifacts are under `parity/<scenario>_<timestamp>/work_*`.

## Ready For Real Scenarios

Use this checklist before running production-style scenario edits.

1. Run smoke first, then full-horizon baseline.
2. Confirm where parity artifacts are stored for `--backend both`.
3. Know which file to inspect first for common failures.
4. Establish a golden parity baseline and enforce regression checks.

### Recommended first run sequence

```bash
# 1) Fast smoke gate (Tandy/CI-friendly)
uv run fp run examples/baseline_smoke.yaml --backend both --with-drift --output-dir artifacts/ready_smoke

# 2) Full baseline run (fp.exe only)
uv run fp run examples/baseline.yaml --backend fpexe --output-dir artifacts/ready_baseline_fpexe
```

### Run a scenario on each backend

```bash
# fp.exe only
uv run fp run examples/demand_shock.yaml --backend fpexe --output-dir artifacts/ready_demand_fpexe

# fppy only
uv run fp run examples/demand_shock.yaml --backend fppy --output-dir artifacts/ready_demand_fppy

# both engines from fp run (nested parity artifacts)
uv run fp run examples/demand_shock.yaml --backend both --with-drift --output-dir artifacts/ready_demand_both

# explicit parity command
uv run fp parity examples/demand_shock.yaml --with-drift --output-dir artifacts/ready_demand_parity
```

### Where artifacts live for `--backend both`

```text
artifacts/ready_demand_both/<scenario>_<timestamp>/
  parity_report.json
  PABEV.TXT
  parity/<scenario>_<timestamp>/
    parity_report.json
    work_fpexe/PABEV.TXT
    work_fpexe/fp-exe.stdout.txt
    work_fpexe/fp-exe.stderr.txt
    work_fppy/PABEV.TXT
    work_fppy/fppy.stdout.txt
    work_fppy/fppy.stderr.txt
    work_fppy/fppy_report.json
```

### Common failure modes (check these first)

- `status=missing_output` in `parity_report.json`:
  - One engine did not produce `PABEV.TXT`.
  - Check `engine_runs.<engine>.work_dir` and confirm `PABEV.TXT` exists there.
- Wine killed / return code `143`:
  - Inspect `work_fpexe/fp-exe.stdout.txt` and `work_fpexe/fp-exe.stderr.txt` in the parity run dir.
  - Re-run after confirming Wine prefix/env configuration from CLI docs.
- Missing `work_fppy/fppy_report.json`:
  - `fp triage fppy-report <run_dir>` can still resolve the report via `parity_report.json -> engine_runs.fppy.work_dir` when available.
  - If resolution fails, inspect `work_fppy/fppy.stdout.txt`/`fppy.stderr.txt` and rerun `--backend fppy` directly.

### Golden/regression loop (recommended)

```bash
# Save once
uv run fp parity examples/baseline.yaml --with-drift --save-golden artifacts/parity-golden --output-dir artifacts/ready_golden_seed

# Gate future changes
uv run fp parity examples/demand_shock.yaml --with-drift --regression artifacts/parity-golden --output-dir artifacts/ready_demand_regression
```

## Data update loop (refresh bundle -> rerun)

Primary operator loop: refresh `FM/` from upstream sources, then rerun smoke or scenarios on the produced bundle.

```bash
# 1) Refresh FM bundle with multi-source update
uv run fp data update-fred \
  --model-dir FM \
  --out-dir artifacts/model_updates/2026-03-03 \
  --end 2025.3 \
  --sources fred \
  --sources bea \
  --sources bls
```

Required env vars (name only):
- `FRED_API_KEY`
- `BEA_API_KEY`
- `BLS_API_KEY`

```bash
# 2) Run a fast parity smoke against the generated bundle
uv run fp run artifacts/model_updates/2026-03-03/scenarios/baseline_smoke.yaml --backend both --with-drift --output-dir artifacts/updated_data_smoke
```

- Need full flags, extend-sample caveats, and `fminput` patch behavior?
  - follow [data-update details](data-update.md).

- Open the dashboard and use the **Data Update** page while pointing at the updated output area:
```bash
uv run fp dashboard --artifacts-dir artifacts/model_updates/2026-03-03
```

## Official Fair bundle workflow (recommended for new quarters)

For new-quarter updates, use the official bundle flow instead of manual `curl/unzip` or only appending `fmdata.txt`. Official bundles replace deck/calibration + data together, avoiding hybrid states that often produce `Solution error in SOL1` and first-quarter drift.

```bash
# 1) Download and stage a runnable official quarterly bundle
uv run fp data fetch-fair-bundle \
  --out-dir /tmp/fair-bundle \
  --fp-exe-from /path/to/FM

# 2) Update from the official bundle (no-extend) into a new output model
uv run fp data update-fred \
  --from-official-bundle \
  --official-bundle-url https://fairmodel.econ.yale.edu/fp/FMFP.ZIP \
  --base-dir /tmp/fair-bundle \
  --out-dir artifacts/model_updates/official_bundle_2026Q1 \
  --end 2025.4

# This command copies `fp.exe` from `--model-dir` into the output bundle when needed.
# If your source model has no `fp.exe`, `update-fred` will warn and the output is not yet runnable.

# 3) Validate the baseline deck on the new bundle
uv run fp run artifacts/model_updates/official_bundle_2026Q1/scenarios/baseline_smoke.yaml --backend both --quick --output-dir artifacts/official_bundle_smoke
```

Recommended order:
1. `fp data fetch-fair-bundle --out-dir ...`
2. `fp data update-fred --from-official-bundle ... --out-dir ... --end ...`
3. `fp run .../baseline_smoke.yaml --backend both --quick ...`

## Updating model data safely

Use the safe lane first: refresh series only up through the existing sample window (no-extend) and keep parity strict.

- Safe default:
  - pass `--end` aligned to your current model sample window (historical `fmdata` horizon),
  - run smoke/parity on the updated bundle without extending the forecast sample,
  - then decide whether to change `forecast_end` in scenario YAMLs if needed.
- Extend-sample warning:
  - extending sample can surface `Solution error in SOL1` and small first-quarter rate-chain drift for `RM/RMA/RMMRSL2/RMACDZ` (for example, `status=gate_failed` with few/zero hard-fails).
  - this is a known behavior; avoid strict parity in this mode until you validate behavior changes.
- Workaround for strict parity checks with extended sample:
  - gate parity to the pre-extend region with `--gate-pabev-end`,
  - or run in non-extend mode for strict parity.

```bash
uv run fp parity examples/baseline_smoke.yaml --with-drift --gate-pabev-end 2025.4
```

- For full flags and caveats, see [data-update docs](data-update.md).

## Compare two runs

```bash
uv run fp diff artifacts/baseline_*/  artifacts/high_growth_*/
```

## Run batch scenarios

```bash
uv run fp batch examples/baseline.yaml examples/higher_growth.yaml
```

## Generate reports

```bash
uv run fp report artifacts/baseline_20260223_120000
uv run fp report artifacts/higher_growth_20260223_120000 --baseline artifacts/baseline_20260223_120000
```

## Build dependency graph

```bash
uv run fp graph FM/fminput.txt
uv run fp graph FM/fminput.txt --variable PCY --export dot --output artifacts/pcy.dot
```

## Launch dashboard

```bash
uv run fp dashboard
uv run fp dashboard --artifacts-dir artifacts --port 8501
```

## Use the MCP server

```bash
# Dev mode with Inspector
uv run fastmcp dev fp-mcp

# Or direct stdio mode
uv run fp-mcp
```
