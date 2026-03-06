# Model Data Update (FRED/BEA/BLS)

## Goal

Enable `fp-wraptr` to **refresh the Fair-Parke model’s historical dataset** (primarily `fmdata.txt`) using automated upstream sources (FRED, BEA, BLS), producing a **new, runnable model bundle** (a directory like `FM/`) that can be used as `fp_home` in scenario YAMLs without mutating the repo’s baseline `FM/` by default.

This is the missing bridge between:

1. existing FRED ingestion (`src/fp_wraptr/fred/ingest.py`)
2. existing source mapping (`src/fp_wraptr/data/source_map.yaml`, `src/fp_wraptr/data/source_map.py`)
3. existing model runners (`fp run`, `fp parity`) that consume `fmdata.txt`

## Non-Goals (MVP)

- Full Census/Treasury ingestion (plan for later; design should not block it).
- Perfect revision-tracking and vintage reconstruction (capture a snapshot, don’t re-run history unless requested).
- Changing FM model equations, estimation, or parity logic.

## Current State (What Exists)

- Fetch/cache arbitrary FRED series IDs:
  - `fp fred fetch ...`
  - `src/fp_wraptr/fred/ingest.py` writes cache under `~/.fp-wraptr/fred-cache/` (good: outside repo)
- Fetch/cache BEA NIPA table lines:
  - `fp bea fetch-nipa ...`
  - `src/fp_wraptr/bea/ingest.py` writes cache under `~/.fp-wraptr/bea-cache/`
  - Note: the data-update pipeline currently supports only BEA NIPA `TableName` values like `T10106` (table+line). Non-NIPA BEA placeholders in `source_map.yaml` are reported under `skipped_unsupported_bea` and are not updated (carry-forward can fill when extending sample).
- Fetch/cache BLS series IDs:
  - `fp bls fetch ...`
  - `src/fp_wraptr/bls/ingest.py` writes cache under `~/.fp-wraptr/bls-cache/`
- Source-map curation & audits:
  - `fp dictionary source-*` commands
  - `src/fp_wraptr/data/source_map.yaml` includes `source: fred`, `series_id`, `frequency`, `annual_rate`, window rules.
- fmdata parsing (read-only):
  - `src/fp_wraptr/io/input_parser.py::parse_fm_data_text`
- Overlay (charting) alignment:
  - `src/fp_wraptr/fred/overlay.py` aligns FRED actuals to forecast periods for plotting (not a data update pipeline)

## Target Operator Workflow

1. Generate an updated model bundle:

```bash
fp data update-fred \
  --model-dir FM \
  --out-dir artifacts/model_updates/2026-03-02 \
  --end 2025.4 \
  --sources fred --sources bea --sources bls
```

Key runtime knobs:
- `--patch-fminput-smpl-endpoints`: updates the LOADDATA `SMPL` endpoint in `fminput.txt` so fp.exe can read newly extended sample history.
- `--extend-sample`: extends `sample_end_after` in the copied `fmdata.txt` to `--end` (default: off).
- `--keyboard-augment`: repeatable list of extra variables to append to the `KEYBOARD` list in `fminput.txt`. When omitted and `--extend-sample` is set, it defaults to `RM`, `RMA`, `RMMRSL2`, `RMACDZ`.

Known limitation:
- Extended bundles can still show `status=gate_failed` with `hard_fail_cell_count=0` when mismatches concentrate in `RM/RMA/RMMRSL2/RMACDZ` at the first forecast quarter. Use `backend=fpexe` (or a non-strict workflow) for strict parity checks if this appears.

API keys (environment variables):
- `FRED_API_KEY` (required when `--sources fred` is enabled)
- `BEA_API_KEY` (required when `--sources bea` is enabled)
- `BLS_API_KEY` (optional but recommended for `--sources bls`)

### API Key Safety

- Keep keys out of repository files: do not place credentials in `docs/`, `do/`, YAML configs, or committed scripts.
- Prefer local shell profile exports (for example `~/.zshrc`) or OS keychain/secret-manager injection.
- Treat all update/log artifacts as potentially shareable outputs and avoid echoing raw keys in command logs.

2. Run scenarios against the updated bundle:

```yaml
# examples/baseline_updated_data.yaml
name: baseline_updated_data
fp_home: artifacts/model_updates/2026-03-02/FM
forecast_start: "2026.1"  # next quarter after `sample_end_after` in the update report
forecast_end: "2029.4"
backend: both
```

Tip:
- `fp data update-fred` writes ready-to-run scenario templates under `<out_dir>/scenarios/`
  (and prints the `recommended_forecast_start` it computed from the updated `fmdata.txt`).
- If `sample_end_after` differs from `fminput_fmdata_load_end` in `data_update_report.json`,
  fp.exe is still loading an older history endpoint from `fminput.txt`. Expect parity mismatches
  until `fminput` is updated to load the extra history quarter.
- If parity remains `gate_failed` with small first-forecast diffs in `RM/RMA/RMMRSL2/RMACDZ`,
  this is the known extend-sample caveat. Prefer a `backend=fpexe` rerun (or non-strict workflow)
  before concluding this is a hard model divergence.

3. (Optional) Promote the updated model bundle to become the new baseline `FM/`:
   - This should be an explicit operator action (a flag like `--promote`), never implicit.

## Design Principles

- **Deterministic artifacts**: every update writes a `data_update_report.json` capturing:
  - update timestamp and tool version
  - source-map path/version used
  - series IDs fetched + last observation dates
  - transform rules applied (frequency aggregation, etc.)
  - update range (start/end)
- **Don’t rewrite history by default**:
  - default is “extend and fill only”: update periods strictly after current `fmdata.txt` end.
  - add `--replace-history` to allow overwriting existing periods (for revisions).
- **No repo mutation by default**:
  - output is a new model directory under `artifacts/…`, intended to be passed via `ScenarioConfig.fp_home`.
- **Fast incremental updates**:
  - use FRED cache; fetch only missing series and only requested date range.

## Implementation Plan

### Milestone 1: Build a Minimal Update Pipeline (few variables)

**Objective:** Update `FM/fmdata.txt` for a small, high-confidence set of variables:
- GDP (`GDPR` -> `GDPC1`)
- unemployment (`UR` -> `UNRATE`)
- CPI (`PCY` -> `CPIAUCSL`)

**Deliverables:**
- A new CLI command: `fp data update-fred`
- A new model bundle directory with updated `fmdata.txt`
- A report file: `data_update_report.json`
- Unit tests covering conversion + fmdata writing

**Code modules (proposed):**
- `src/fp_wraptr/data/update_fred.py`
  - orchestrates: parse base fmdata, fetch series, normalize, merge, write outputs
- `src/fp_wraptr/io/fmdata_writer.py` (or extend `src/fp_wraptr/io/writer.py`)
  - writes `fmdata.txt` in FP `SMPL/LOAD/END` format
- `src/fp_wraptr/fred/normalize_for_fmdata.py`
  - converts raw FRED series (monthly/quarterly) into model-quarterly series keyed by FP periods (`YYYY.Q`)
  - applies window rules + annual-rate conversion + scaling

**Normalization rules (MVP):**
- **Frequency**
  - `Q`: expect FRED series already quarterly (quarter-start timestamps); align to quarter start and keep value.
  - `M`: aggregate to quarterly using **quarterly mean** (typical for rates/indexes like CPI, UNRATE).
  - `A`: reject with a clear error (until a disaggregation rule is implemented).
- **Annual-rate handling**
  - For fmdata updates, **de-annualize annual-rate flows** when `annual_rate: true` in `source_map.yaml`:
    - `frequency: Q` annual-rate (SAAR) flows: `quarterly_value = value / 4`
    - `frequency: M` annual-rate flows: `quarterly_value = sum(monthly_value / 12) over the quarter`
  - This matches the stock `FM/fmdata.txt` convention (per-quarter flows, not SAAR) and is required for parity.
- **Scale/offset**
  - Apply `scale` then `offset` after aggregation and annual-rate conversion.
  - Example: `UR` uses `scale: 0.01` to convert FRED percent to model fraction.
- **Transform**
  - `level`, `index`: direct mapping.
  - `growth_rate`, `cumulative`: not in Milestone 1 (error or skip).

**fmdata merge rules (MVP):**
- Parse base `fmdata.txt` into:
  - sample start/end
  - per-variable value arrays
- Determine requested update end:
  - `--end YYYY.Q` required initially (avoids ambiguous “latest”).
  - later: add `--end latest` that resolves from data availability.
- For each variable being updated:
  - only fill periods `> base_end` unless `--replace-history`
  - if a value is missing for a period, keep existing value (or leave missing if extending past base end)

**Validation:**
- Ensure the output `fmdata.txt` parses via `parse_fm_data_text`.
- Run a baseline scenario using `fp_home` pointing at the updated model dir:
  - fpexe should produce `PABEV.TXT` and `fmout.txt` successfully.

### Milestone 2: Expand Coverage Using `source_map.yaml`

**Objective:** Update all variables in `source_map.yaml` where `source: fred` and `series_id` is present.

**Additions:**
- `--variables` (optional) to limit update scope (useful for debugging).
- `--start`/`--end` date bounds passed through to FRED for performance.
- Write a snapshot of fetched source data:
  - `fred_observations.csv` and/or `fred_observations.parquet` under the update artifact directory.

**Quality gates:**
- `source_map` quality report must be clean for selected variables (series_id present).
- Emit a coverage summary in `data_update_report.json`:
  - mapped vars attempted
  - vars skipped (unmapped / unsupported frequency / missing data)

### Milestone 3: Make It Usable For Research Scenarios

**Objective:** Turn the update bundle into a first-class input for scenario workflows.

**Additions:**
- `fp run` ergonomics:
  - allow `--fp-home <dir>` override at CLI (without needing to edit YAML)
- Dashboard:
  - show `data_update_report.json` if present (as metadata on runs)
- MCP server:
  - add a tool like `update_model_from_fred(model_dir, out_dir, end_period, variables=...)`

### Milestone 4: Add Revision/Vintage Controls (Optional)

**Objective:** Support reproducible research with explicit data vintages.

**Additions:**
- `--replace-history` becomes a structured mode:
  - `extend_only` (default)
  - `revise_last_n_quarters`
  - `rewrite_full_history` (rare, slow)
- Store a stable “as-of” snapshot:
  - `asof_date`, `fred_cache_ttl`, and the exact observations used in the artifact directory.

## CLI Sketch

Add a new CLI group:

- `fp data update-fred`
  - `--model-dir` (default `FM`)
  - `--out-dir` (required)
  - `--end` (required in MVP, `YYYY.Q`)
  - `--variables` (optional list)
  - `--replace-history` (bool, default false)
  - `--extend-sample` (bool, default false)
  - `--allow-carry-forward` (bool, default false)
  - `--patch-fminput-smpl-endpoints` (bool, default false)
  - `--keyboard-augment` (list[str], optional, repeatable)
  - `--cache-dir` (optional override; default `~/.fp-wraptr/fred-cache`)
  - `--start-date`/`--end-date` passthrough (optional)

- `fp data check-fred-mapping`
  - quickly compares last N quarters of `fmdata.txt` vs normalized FRED to find obvious unit mismatches
  - suggests scale factors (0.001/0.01/0.1/10/100/1000) when patterns are clear

Output layout:

```
artifacts/model_updates/2026-03-02/
  FM/
    fmdata.txt   # updated
    fmage.txt    # copied from base (MVP)
    fmexog.txt   # copied from base
    fminput.txt  # copied from base
  data_update_report.json
  fred_observations.csv   # optional (Milestone 2)
```

## Tests (Minimum)

- `tests/test_data_update_fred_normalize.py`
  - monthly -> quarterly mean
  - quarterly -> quarterly identity
  - FP period string mapping correctness
- `tests/test_fmdata_writer_roundtrip.py`
  - writer output parses with `parse_fm_data_text`
  - preserves value counts for a small synthetic sample
- `tests/test_cli_data_update_fred.py`
  - dry-run mode (optional) validates arguments
  - missing API key fails with clear message

## Risks / Open Questions

- **Units and conventions**: some FP variables may not be stored in the same units as a naive FRED mapping implies.
  Source-map must be treated as authoritative, and any per-variable transform must be explicit.
- **Annual series**: must define disaggregation rules before supporting `frequency: A`.
- **Revisions**: FRED series revise historical values; default should avoid rewriting history unless requested.
