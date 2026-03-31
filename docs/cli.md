# CLI Reference

Install `fp-wraptr` and use the `fp` command:

```bash
scripts/uvsync --all-extras
```

## No Repo-Local Envs/Prefixes

To avoid creating forbidden runtime artifacts inside this repo:

- Set `UV_PROJECT_ENVIRONMENT` to a project-specific external venv path (for example via repo-local `.env`) so `uv` does not create a local `.venv/`.
- Use a Wine prefix outside the repo, for example `WINEPREFIX=$HOME/.wine-<project-name>` (repo-local `.wine*` directories are forbidden).

Example `.env`:

```dotenv
UV_PROJECT_ENVIRONMENT=$HOME/venvs/<project-name>
WINEPREFIX=$HOME/.wine-<project-name>
PYTHONDONTWRITEBYTECODE=1
PYTHONPYCACHEPREFIX=/tmp/<project-name>-pycache
UV_CACHE_DIR=/tmp/uv-cache-<project-name>
RUFF_CACHE_DIR=/tmp/ruff-cache-<project-name>
```

Supported local workflow in this repo:

```bash
scripts/uvsync --all-extras
scripts/uvsafe fp --help
```

## Global options

- `--version` — print version and exit

## Agent-first authoring commands

These commands mirror the new MCP workspace flow for users or agents that prefer the CLI.

### `fp packs list`

List discovered local/public pack manifests.

### `fp packs describe <pack_id>`

Show a pack manifest with resolved catalog entries, cards, recipes, and visualization presets.

### `fp workspace list`

List managed workspaces.

- Options:
  - `--family` (`str`) — optional family filter

### `fp workspace create-catalog <catalog_entry_id>`

Create a managed workspace from a catalog entry.

- Options:
  - `--slug` (`str`) — optional workspace slug override
  - `--label` (`str`) — optional workspace label override

### `fp workspace show <workspace_id>`

Show a managed workspace payload.

### `fp workspace cards <workspace_id>`

Show the current card/default surface for a workspace.

- Options:
  - `--variant-id` (`str`) — inspect a bundle variant’s local card state instead of the shared bundle cards

### `fp workspace apply-card <workspace_id> <card_id>`

Apply card updates with a JSON mutation payload.

- Options:
  - `--constants` (`str`) — JSON object of constant updates
  - `--variant-id` (`str`) — bundle variant id
  - `--selected-target` (`str`) — optional explicit output target
  - `--input-mode` (`str`) — optional input mode hint
  - `--enabled / --disabled`

### `fp workspace import-series <workspace_id> <card_id>`

Import a quarterly series into a series card.

- Options:
  - `--series-json` (`str`) — JSON object mapping `period -> value`
  - `--csv-path` (`str`) — CSV path with `period,value`
  - `--pasted-text` (`str`) — inline `period,value` text
  - `--variant-id` (`str`) — bundle variant id
  - `--selected-target` (`str`) — optional explicit output target

### `fp workspace add-variant <workspace_id> <variant_id>`

Add a new variant to a bundle workspace.

- Options:
  - `--label` (`str`)
  - `--scenario-name` (`str`) — optional exact scenario/run name for the variant output
  - `--input-file` (`str`)
  - `--clone-from` (`str`)

### `fp workspace update-variant <workspace_id> <variant_id>`

Update metadata for an existing bundle variant.

- Options:
  - `--label` (`str`)
  - `--scenario-name` (`str`) — optional exact scenario/run name for the variant output
  - `--input-file` (`str`)
  - `--enabled / --disabled`

### `fp workspace clone-variant <workspace_id> <variant_id>`

Clone a bundle variant, update its metadata, and optionally seed one card patch in one action.

- Options:
  - `--clone-from` (`str`) — existing variant id to clone from
  - `--label` (`str`)
  - `--scenario-name` (`str`) — optional exact scenario/run name for the variant output
  - `--input-file` (`str`)
  - `--enabled / --disabled`
  - `--card-id` (`str`) — optional card id to seed on the cloned variant
  - `--constants` (`str`) — optional JSON object of constant updates for the seeded card
  - `--selected-target` (`str`)
  - `--input-mode` (`str`)

### `fp workspace compile <workspace_id>`

Compile a managed workspace into runnable scenario/bundle artifacts.

### `fp workspace run <workspace_id>`

Compile and run a managed workspace.

- Options:
  - `--output-dir` (`Path`, default `artifacts/agent_runs`)

### `fp workspace compare <workspace_id>`

Compare two linked runs for a workspace.

- Options:
  - `--run-a` (`str`) — optional explicit baseline run dir
  - `--run-b` (`str`) — optional explicit comparison run dir
  - `--top-n` (`int`, default `10`)

### `fp workspace visualizations <workspace_id>`

List built-in plus pack-defined visualization presets for a workspace.

### `fp workspace build-view <view_id>`

Build a chart-ready visualization payload.

- Options:
  - `--workspace-id` (`str`)
  - `--pack-id` (`str`)
  - `--run-dirs` (`str`) — JSON array of explicit run directories

## `fp export pages`

Export a portable, read-only run bundle for GitHub Pages.

- Options:
  - `--spec` (`Path`, default `public/model-runs.spec.yaml`) — checked-in export spec
  - `--artifacts-dir` (`Path`, default `artifacts`) — source run artifacts
  - `--out-dir` (`Path`, default `public/model-runs`) — generated public bundle directory

Example:

```bash
fp export pages --spec public/model-runs.spec.yaml --artifacts-dir artifacts --out-dir public/model-runs
```

The exporter writes only static assets and JSON payloads. It fails if the generated bundle would contain absolute local paths.

## `fp run <scenario.yaml>`

Run a single scenario config.

- Positional:
  - `scenario` (`Path`) — scenario YAML path
- Options:
  - `--baseline`, `-b` (`Path | None`) — baseline scenario YAML to run and diff
  - `--backend` (`str | None`) — backend override: `fpexe`, `fppy`, `fp-r`, or `both`
  - `--fp-home` (`Path | None`, env: `FP_HOME`) — path containing `fp.exe` and FP model files; not required for bundle-backed `fp-r`
  - `--output-dir`, `-o` (`Path`, default `artifacts`) — run output root
  - `--fingerprint-lock` (`Path | None`) — optional fingerprint lockfile for parity mode
  - `--with-drift` (`bool`) — enable drift guardrails (parity mode only)
  - `--parity-quick` (`bool`) — shortcut parity gate to `forecast_start` for a fast smoke check in `--backend both`

Example:

```bash
fp run examples/baseline.yaml --output-dir artifacts
fp run examples/baseline.yaml --backend fppy
fp run examples/fpr_bundle_demo.yaml --backend fp-r
fp run examples/baseline.yaml --backend both --with-drift
fp run examples/baseline.yaml --backend both --with-drift --parity-quick
```

Notes:
- `--backend fp-r` runs the bundle-backed R backend. The scenario should provide `fpr.bundle_path` and may provide `fpr.rscript_path`.
- `examples/fpr_bundle_demo.yaml` is the tracked self-contained `fp-r` example.
- `--backend both` runs parity mode (`fp.exe` + `fppy`) and writes `parity_report.json` (difference report).
- `--baseline` is not supported with `--backend both`.

## `fp fpr-compare <scenario.yaml> [expected.csv]`

Run a reduced `fp-r` slice and compare its emitted `fp_r_series.csv` against a seeded expected CSV.

- Positional:
  - `scenario` (`Path`) — scenario YAML path for the `fp-r` run
  - `expected` (`Path | None`) — optional expected CSV; may also come from `fpr.expected_csv` in the scenario YAML
- Options:
  - `--output-dir`, `-o` (`Path`, default `artifacts`) — output root
  - `--atol` (`float`, default `1.1e-3`) — absolute comparison tolerance
  - `--rtol` (`float`, default `1e-6`) — relative comparison tolerance

Example:

```bash
fp fpr-compare path/to/fpr.yaml path/to/expected.csv
```

Notes:
- This is a diagnostic/helper command for reduced `fp-r` slices.
- It is not the main public `fp-r` workflow; the main phase-1 public path is `fp run ... --backend fp-r`.

## `fp parity <scenario.yaml>`

Run parity explicitly for an engine pair and emit a machine-readable report.

- Positional:
  - `scenario` (`Path`) — scenario YAML path
- Options:
  - `--left` (`str`, default `fpexe`) — left comparison engine: `fpexe`, `fppy`, or `fp-r`
  - `--right` (`str`, default `fppy`) — right comparison engine: `fpexe`, `fppy`, or `fp-r`
  - `--fp-home` (`Path | None`, env: `FP_HOME`) — path containing `fp.exe`
  - `--output-dir`, `-o` (`Path`, default `artifacts`) — parity output root
  - `--fingerprint-lock` (`Path | None`) — optional scenario/input fingerprint lock
  - `--with-drift` (`bool`) — enable bounded-drift guardrails
  - `--quick` (`bool`) — shortcut parity gate to `forecast_start` for fast smoke checks
  - `--save-golden` (`Path | None`) — save this parity run as a golden baseline (copies both PABEVs + report)
  - `--regression` (`Path | None`) — compare this parity run against an existing golden baseline (fails on new findings)

Example:

```bash
fp parity examples/baseline.yaml --with-drift
fp parity examples/fpr_real_stock_eq_parity.yaml --left fpexe --right fp-r --lenient
fp parity examples/baseline.yaml --with-drift --save-golden artifacts/parity-golden
fp parity examples/baseline.yaml --with-drift --regression artifacts/parity-golden
fp parity examples/baseline.yaml --with-drift --quick
```

Preset note:
- `fp parity` uses `fppy.eq_flags_preset=parity` by default; opt out with `fppy.eq_flags_preset: default` in scenario YAML.
- See [`docs/parity.md`](parity.md#eq_flags_presetparity-default-and-opt-out) for behavior details.
- `fp-r` parity requires a scenario that includes both the usual FP assets (`fp_home`) and `fpr.bundle_path`.
- A tracked `fpexe` vs `fp-r` example is included at `examples/fpr_real_stock_eq_parity.yaml`.
- The default parity compare window now starts at the scenario `forecast_start` unless the caller overrides it.
- `--save-golden` and `--regression` work for explicit pairs too; `fppy`-specific triage artifacts still only exist when `fppy` is one side of the run.

Primary artifact:
- `artifacts/<scenario>_<timestamp>/parity_report.json`

Useful report fields:
- `status` (`ok`, `gate_failed`, `hard_fail`, `drift_failed`, `fingerprint_mismatch`, `engine_failure`, `missing_output`)
- `exit_code` (CLI exits with this code)
- `engine_runs.*.pabev_path` (engine-specific `PABEV.TXT` or `PACEV.TXT` paths)
- `pabev_detail` (hard-fail and tolerance details)
- `drift_check` (present when `--with-drift`)

Parity exit codes:
- `0` — parity checks passed
- `2` — numeric gate failed (includes drift-only failures)
- `3` — hard-fail mismatch (`missing/discrete/signflip`)
- `4` — engine/runtime/output failure
- `5` — fingerprint lock mismatch

Operator interpretation matrix and metric checklist: see [`docs/parity.md`](parity.md#divergence-interpretation-matrix).

Hard-fail invariants are always enforced (missing sentinel mismatch, discrete mismatch, sign-flip mismatch) regardless of tolerance settings.

## `fp triage ...`

Generate deterministic triage artifacts from parity outputs (no re-running engines).

### `fp triage loop <scenario.yaml>`

Runs the repeatable parity loop in one command:
1. parity run
2. hard-fail + fppy triage artifact generation
3. optional golden save or regression gate

Example:

```bash
fp triage loop examples/baseline_smoke.yaml --with-drift --output-dir artifacts/triage_loop_demo
fp triage loop examples/baseline_smoke.yaml --with-drift --regression artifacts/parity-golden-demo
fp triage loop examples/baseline_smoke.yaml --with-drift --quick --output-dir artifacts/triage_loop_quick
```

Notes:
- Supports parity options: `--fp-home`, `--output-dir`, `--fingerprint-lock`, `--with-drift`, `--strict`.
- Supports one gate action per run: `--save-golden` or `--regression` (mutually exclusive).
- Writes the same triage outputs as the subcommands below into the produced run directory.

### `fp triage fppy-report <run_dir>`

Buckets `fppy_report.json` issues into stable categories and writes:
- `triage_summary.json`
- `triage_issues.csv`

`run_dir` can be either:
- the parity run root (`artifacts/<scenario>_<timestamp>/`), or
- the `work_fppy/` directory.

Outputs are written next to the resolved `fppy_report.json` (for parity runs this is typically `artifacts/<scenario>_<timestamp>/work_fppy/`).

### `fp triage parity-hardfails <run_dir>`

Recomputes the full hard-fail cell set from the two `PABEV.TXT` artifacts and writes:
- `triage_hardfails.csv`
- `triage_hardfails_summary.json`

Outputs are written to the parity run root (`artifacts/<scenario>_<timestamp>/`).

## `fp validate <scenario.yaml>`

Validate a scenario file schema without executing.

- Positional:
  - `scenario` (`Path`) — scenario YAML path

## `fp describe <variable>`

Describe a variable from `dictionary.json`.

- Positional:
  - `variable` (`str`) — variable code (for example `GDP`, `UR`)
- Options:
  - `--dictionary` (`Path | None`) — override dictionary JSON path
  - `--format`, `-f` (`str`, default `table`) — `table` or `json`

## `fp dictionary search <query>`

Deterministic dictionary search over equation IDs, variable codes, and text.

- Positional:
  - `query` (`str`) — search text (for example `eq 82`, `GDP`, `consumer`)
- Options:
  - `--dictionary` (`Path | None`) — override dictionary JSON path
  - `--limit`, `-n` (`int`, default `10`) — max matches per section
  - `--format`, `-f` (`str`, default `table`) — `table`, `json`, or `csv`
  - `--output`, `-o` (`Path | None`) — write `json` or `csv` output to file
  - `--intent-diagnostics` (`bool`) — emit only `{query,intent,focus}` JSON for scripted intent parsing
- Query examples:
  - `eq 82`
  - `GDP equation`
  - `UR in equation 30`
  - `variables in equation 82`
  - `meaning of UR in equation 30`
  - `consumer expenditures`

## `fp dictionary equation <id>`

Explain an equation and variables referenced by it.

- Positional:
  - `equation_id` (`int`) — equation number
- Options:
  - `--dictionary` (`Path | None`) — override dictionary JSON path
  - `--format`, `-f` (`str`, default `table`) — `table`, `json`, or `csv`
  - `--output`, `-o` (`Path | None`) — write `json` or `csv` output to file

## `fp dictionary sources <variable>`

Show expanded data-source mapping by merging `source_map.yaml` with dictionary raw-data links.
Source-map entries include an `annual_rate` flag (for example SAAR flows) so downstream logic can
de-annualize only when an equation expects non-annualized flows:
- quarterly annual-rate source: `value / 4`
- monthly annual-rate source: de-annualize monthly (`value / 12`) then aggregate to quarter

- Positional:
  - `variable` (`str`) — variable code (for example `GDP`, `UR`)
- Options:
  - `--dictionary` (`Path | None`) — override dictionary JSON path
  - `--source-map` (`Path | None`) — override source-map YAML path
  - `--format`, `-f` (`str`, default `table`) — `table` or `json`

`json` output also includes a `normalization` block when `annual_rate: true`
with explicit divisor/formulas for deterministic downstream handling.

## `fp dictionary source-coverage`

Report source-map coverage against dictionary variable names.

- Options:
  - `--dictionary` (`Path | None`) — override dictionary JSON path
  - `--source-map` (`Path | None`) — override source-map YAML path
  - `--only-with-raw-data` (`bool`) — scope to variables that have `raw_data_sources`
  - `--limit`, `-n` (`int`, default `25`) — max missing variables shown in table mode
  - `--format`, `-f` (`str`, default `table`) — `table` or `json`

## `fp dictionary source-quality`

Audit source-map quality issues (missing series IDs, missing BEA locators, invalid source/frequency).

- Options:
  - `--dictionary` (`Path | None`) — override dictionary JSON path
  - `--source-map` (`Path | None`) — override source-map YAML path
  - `--only-with-raw-data` (`bool`) — scope to variables that have `raw_data_sources`
  - `--limit`, `-n` (`int`, default `25`) — max findings shown in table mode
  - `--format`, `-f` (`str`, default `table`) — `table` or `json`

## `fp dictionary source-report`

Build a deterministic source-map curation report combining coverage and quality metrics.

- Options:
  - `--dictionary` (`Path | None`) — override dictionary JSON path
  - `--source-map` (`Path | None`) — override source-map YAML path
  - `--format`, `-f` (`str`, default `table`) — `table` or `json`
  - `--output`, `-o` (`Path | None`) — write JSON report to file (`--format json` required)

## `fp dictionary source-window-check`

Validate source-map window assumptions (for entries with `window_start`/`window_end`/`outside_window_value`)
against observed FRED data.

- Options:
  - `--source-map` (`Path | None`) — override source-map YAML path
  - `--start` (`str | None`) — observation start date (`YYYY-MM-DD`)
  - `--end` (`str | None`) — observation end date (`YYYY-MM-DD`)
  - `--tolerance` (`float`, default `0.0`) — absolute tolerance for outside-window deviation checks
  - `--cache-dir` (`Path | None`) — override FRED cache directory
  - `--format`, `-f` (`str`, default `table`) — `table` or `json`
  - `--output`, `-o` (`Path | None`) — write JSON report to file (`--format json` required)
  - `--limit` (`int`, default `20`) — max detailed rows shown in table mode

## `fp batch <scenario1.yaml> [scenario2.yaml ...]`

Run multiple scenarios in sequence.

- Positional:
  - `scenarios` (`list[Path]`) — scenario YAML paths
- Options:
  - `--output-dir`, `-o` (`Path`, default `artifacts`) — batch artifacts directory

## `fp report <run_dir>`

Render a markdown run report.

- Positional:
  - `run_dir` (`Path`) — completed run directory
- Options:
  - `--baseline` (`Path | None`) — optional baseline run for comparison
  - `--output` (`Path | None`) — write report file

## `fp graph <fminput.txt>`

Build dependency graph summaries from FP input.

- Positional:
  - `input_file` (`Path`) — `fminput.txt`
- Options:
  - `--variable`, `-v` (`str | None`) — inspect upstream/downstream for variable
  - `--export` (`str | None`) — only `dot` supported
  - `--output` (`Path | None`) — export path

## `fp history`

List prior runs from artifacts.

- Options:
  - `--artifacts-dir` (`Path`, default `artifacts`) — artifacts root

## `fp dashboard`

Launch the Streamlit dashboard.

- Options:
  - `--artifacts-dir` (`Path`, default `artifacts`)
  - `--port` (`int`, default `8501`)

## `fp dsl compile <scenario.dsl>`

Compile a human-readable scenario DSL file into YAML or JSON scenario config.

- Positional:
  - `path` (`Path`) — scenario DSL file
- Options:
  - `--output`, `-o` (`Path | None`) — write compiled output to file
  - `--format`, `-f` (`str`, default `yaml`) — `yaml` or `json`

## `fp data fetch-fair-bundle`

Download and unpack the official Fair quarterly model bundle.

- Options:
  - `--out-dir` (`Path`, required) — directory to write the downloaded/unpacked bundle
  - `--url` (`str`, default `https://fairmodel.econ.yale.edu/fp/FMFP.ZIP`) — bundle URL
  - `--fp-exe-from` (`Path | None`) — optional `fp.exe` or model directory source to copy into unpacked model directory
  - `--timeout-seconds` (`int`, default `60`) — HTTP download timeout
  - `--zip-path` (`Path | None`) — optional local `FMFP.ZIP` path to skip download

Example:

```bash
fp data fetch-fair-bundle --out-dir /tmp/fmfp_bundle
```

```bash
fp data fetch-fair-bundle --out-dir /tmp/fmfp_bundle --fp-exe-from /path/to/FM
```

## `fp data update-fred`

Generate a new runnable model bundle under `--out-dir/FM` with refreshed `fmdata.txt`
from mapped external sources (FRED/BEA/BLS), plus `data_update_report.json`.

API keys (environment variables):
- FRED: `FRED_API_KEY` (required when `--sources fred` is enabled)
- BEA: `BEA_API_KEY` (required when `--sources bea` is enabled)
- BLS: `BLS_API_KEY` (optional but recommended when `--sources bls` is enabled)

API-key safety rules:
- Never commit keys into repo files (`docs/`, `do/`, scenario YAML, or source-map YAML).
- Prefer local shell profile export or OS keychain-backed injection so keys stay machine-local.
- Use source-specific commands to validate connectivity before full updates: `fp bea fetch-nipa ...` and `fp bls fetch ...`.

- Options:
  - `--model-dir` (`Path`, default `FM`) — base model directory to copy/update
  - `--out-dir` (`Path`, required) — output root for updated model bundle/report
  - `--end` (`str`, required) — update end period (`YYYY.Q`)
  - `--source-map` (`Path | None`) — override source-map YAML path
  - `--cache-dir` (`Path | None`) — override FRED cache directory (BEA/BLS use sibling caches under the same root)
  - `--sources` (`list[str] | None`) — enabled sources (repeatable). Supported: `fred`, `bea`, `bls`. Default: `fred`
  - `--variables` (`list[str] | None`) — optional subset of FP variables to update
  - `--replace-history` (`bool`) — apply updates within the existing history window too
  - `--extend-sample` (`bool`) — extend fmdata `sample_end` to `--end` when needed
  - `--allow-carry-forward` (`bool`) — when extending, fill missing series by carrying forward prior values
  - `--from-official-bundle` (`bool`) — use a fresh official Fair bundle as base before update
    - when using this mode, `fp data update-fred` attempts to copy `fp.exe` from `--model-dir` into the output FM/ if needed
  - `--official-bundle-url` (`str`, default `https://fairmodel.econ.yale.edu/fp/FMFP.ZIP`) — override official bundle source
  - `--base-dir` (`Path | None`) — optional base directory for downloaded official bundle
  - `--start-date` (`str | None`) — override FRED observation start date (`YYYY-MM-DD`)
  - `--end-date` (`str | None`) — override FRED observation end date (`YYYY-MM-DD`)

Example:

```bash
fp data update-fred --model-dir FM --out-dir artifacts/model_updates/demo --end 2025.4
```

Example (multi-source, when mappings exist):

```bash
fp data update-fred --sources fred --sources bea --sources bls --model-dir FM --out-dir artifacts/model_updates/demo --end 2025.4
```

Example (official bundle base, no-extend safe path for new vintages):

```bash
fp data update-fred \
  --from-official-bundle \
  --official-bundle-url https://fairmodel.econ.yale.edu/fp/FMFP.ZIP \
  --base-dir /tmp/fair-official \
  --out-dir artifacts/model_updates/demo-official \
  --end 2025.4
```

When `--from-official-bundle` is enabled, `update-fred` copies `fp.exe` from `--model-dir` into the generated output bundle if possible; if `fp.exe` is missing, a warning is printed and parity should not be run until `fp.exe` is restored.

## `fp data check-fred-mapping`

Compare existing `fmdata.txt` values against normalized FRED values for the last N quarters
to calibrate `scale`, `offset`, and monthly aggregation rules in `source_map.yaml`.

- Options:
  - `--model-dir` (`Path`, default `FM`) — base model directory containing `fmdata.txt`
  - `--source-map` (`Path | None`) — override source-map YAML path
  - `--variables` (`list[str] | None`) — optional subset of FP variables to check
  - `--periods` (`int`, default `40`) — number of most recent quarters to compare
  - `--cache-dir` (`Path | None`) — override FRED cache directory
  - `--format` (`str`, default `table`) — `table` or `json`

## `fp fred fetch <series_id...>`

Fetch one or more FRED series IDs and print the data table.

- Positional:
  - `series_ids` (`list[str]`) — one or more FRED series IDs
- Options:
  - `--start` (`str | None`) — observation start date (`YYYY-MM-DD`)
  - `--end` (`str | None`) — observation end date (`YYYY-MM-DD`)
  - `--cache-dir` (`Path | None`) — cache directory override
  - `--format` (`str`, default `csv`) — `csv` or `json`

## `fp fred clear-cache`

Delete cached FRED JSON payloads.

- Options:
  - `--cache-dir` (`Path | None`) — cache directory override

## `fp bea fetch-nipa <table> <line>`

Fetch one NIPA table line from BEA and print the data table.

- Positional:
  - `table` (`str`) — table name (for example `T10106`)
  - `line` (`int`) — line number
- Options:
  - `--frequency` (`str`, default `Q`) — `Q` or `A`
  - `--year` (`str`, default `ALL`) — year selector (use `ALL` for full table)
  - `--cache-dir` (`Path | None`) — cache directory override
  - `--format` (`str`, default `csv`) — `csv` or `json`

## `fp bea clear-cache`

Delete cached BEA JSON payloads.

- Options:
  - `--cache-dir` (`Path | None`) — cache directory override

## `fp bls fetch <series_id...>`

Fetch one or more BLS series IDs and print the data table.

- Positional:
  - `series_ids` (`list[str]`) — one or more BLS series IDs
- Options:
  - `--start-year` (`int`) — start year (YYYY)
  - `--end-year` (`int`) — end year (YYYY)
  - `--cache-dir` (`Path | None`) — cache directory override
  - `--format` (`str`, default `csv`) — `csv` or `json`

## `fp bls clear-cache`

Delete cached BLS JSON payloads.

- Options:
  - `--cache-dir` (`Path | None`) — cache directory override

## `fp diff <run_a> <run_b>`

Compare two completed runs.

- Positional:
  - `run_a` (`Path`) — baseline run directory
  - `run_b` (`Path`) — scenario run directory
- Options:
  - `--export` (`str | None`) — `csv` or `excel`
  - `--output` (`Path | None`) — output path for export

## `fp version`

Print the installed version string.

## `fp info`

Print diagnostic environment info for bug reports.

## `fp io parse-output <fmout.txt>`

Parse FP output file.

- Positional:
  - `path` (`Path`) — `fmout.txt`
- Options:
  - `--format`, `-f` (`str`, default `json`) — `json` or `csv`

## `fp io parse-input <fminput.txt>`

Parse FP input file into JSON sections.

- Positional:
  - `path` (`Path`) — `fminput.txt`

## `fp viz plot <fmout.txt>`

- Positional:
  - `path` (`Path`) — `fmout.txt`
- Options:
  - `--var`, `-v` (`list[str] | None`) — one or more variables
  - `--output`, `-o` (`Path`, default `artifacts/forecast.png`) — output image file
