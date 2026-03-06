# Scenario Configuration Reference

`ScenarioConfig` is the YAML schema used by `fp run`, `fp batch`, and `fp-mcp` scenario tools.

## YAML fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | required | Scenario name, used for output directory naming. |
| `description` | `str` | `""` | Human-readable scenario description. |
| `fp_home` | `Path` | `FM` | Path to the FP installation directory (contains `fp.exe`, `fminput.txt`, etc.). |
| `input_file` | `str` | `fminput.txt` | FP input filename to copy/patch in each run work directory. |
| `forecast_start` | `str` | `"2025.4"` | Forecast start period (`YYYY.Q`). |
| `forecast_end` | `str` | `"2029.4"` | Forecast end period (`YYYY.Q`). |
| `overrides` | `dict[str, VariableOverride]` | `{}` | Exogenous overrides keyed by variable name. |
| `track_variables` | `list[str]` | `[PCY, PCPF, UR, PIEF, GDPR]` | Variables included in output summaries/charts. |
| `input_patches` | `dict[str, str]` | `{}` | Input patches (literal replacement and command-aware `cmd:` updates). |
| `extra` | `dict` | `{}` | Additional metadata passed through for local tooling. |

## Override methods

Each entry in `overrides` has:

- `method`: one of
  - `CHGSAMEPCT`: change by the same percentage each period
  - `SAMEVALUE`: hold the variable at a constant value
  - `CHGSAMEABS`: change by the same absolute amount each period
- `value`: numeric value used with the method

Example:

```yaml
overrides:
  YS:
    method: CHGSAMEPCT
    value: 0.008
```

## `input_patches`

`input_patches` supports two forms:

1. Literal search/replace (legacy):

```yaml
input_patches:
  "SETUPSOLVE MAXIT=8;": "SETUPSOLVE MAXIT=20;"
```

2. Command-aware parameter patching:

```yaml
input_patches:
  "cmd:SETUPSOLVE.MAXIT": "20"
  "cmd:SETUPSOLVE[1].MAXCHECK": "80"
```

Command-aware patches edit command parameters without requiring exact whole-line text matches.

## `track_variables`

`track_variables` controls which forecast series are shown in run summaries and charts (CLI `fp run`, `fp report`, and dashboard views). Defaults to:

```yaml
track_variables:
  - PCY
  - PCPF
  - UR
  - PIEF
  - GDPR
```

## Full examples

### `examples/baseline.yaml`

```yaml
# Baseline scenario: default FP model run with no overrides.
#
# This runs the model as-is using the standard fminput.txt and fmexog.txt
# configuration. Use this as the reference point for comparing scenario variants.

name: baseline
description: "Default FP model run with standard assumptions"
fp_home: FM
input_file: fminput.txt
forecast_start: "2025.4"
forecast_end: "2029.4"
track_variables:
  - PCY     # Real GDP growth rate (annualized %)
  - PCPF    # GDP deflator inflation rate (annualized %)
  - UR      # Unemployment rate
  - PIEF    # Before-tax corporate profits
  - GDPR    # Real GDP (billions, 2012 dollars)
```

### `examples/higher_growth.yaml`

```yaml
# Higher growth scenario: potential GDP grows faster than baseline.
#
# This overrides the YS (potential GDP) exogenous variable to grow
# at a higher rate, simulating a more optimistic supply-side outlook.
# Compare against baseline.yaml to see impacts on inflation, unemployment, etc.

name: higher_growth
description: "Higher potential GDP growth scenario"
fp_home: FM
input_file: fminput.txt
forecast_start: "2025.4"
forecast_end: "2029.4"
overrides:
  YS:
    method: CHGSAMEPCT
    value: 0.008    # ~3.2% annualized vs baseline ~2.1%
track_variables:
  - PCY
  - PCPF
  - UR
  - PIEF
  - GDPR
```

### New ship-readiness examples

- `examples/demand_shock.yaml` — demand-led expansion via `TBGQ` override.
- `examples/inflation_shock.yaml` — cost-push inflation shock via `PIM` override.
- `examples/rate_path_shock.yaml` — tighter policy-rate path via `RS` override.

## Walkthrough: from override to parity triage

For fast smoke validation (for example Tandy/CI runs), use `examples/baseline_smoke.yaml` with `--backend both` to produce parity artifacts quickly before running larger policy scenarios.
If you want to refresh model history before scenario runs, generate an updated bundle with
`fp data update-fred` and set scenario `fp_home` to the new `.../FM` directory
(see [`docs/data-update.md`](data-update.md)).

### How overrides work (baseline + scenario layers)

- Baseline exogenous settings from `FM/fmexog.txt` remain in effect by default.
- Scenario `overrides` are merged/appended into the run-local `fmexog.txt` used for execution.
- `fminput.txt` is not patched to point at another exogenous filename for this flow.
- A copy of the merged exogenous script is also written as `fmexog_override.txt` for operator inspection.
- First artifact to inspect when results surprise you: `<run>/work_*/fmexog.txt` (inspection copy: `<run>/work_*/fmexog_override.txt`).

### 1. How overrides map to run exogenous files

- Every `overrides` entry in scenario YAML is translated into deterministic override commands in the run work directory.
- The merged exogenous script is written to `work_*/fmexog.txt` and consumed directly by the run pipeline.
- `work_*/fmexog_override.txt` is emitted as an explicit debugging copy of that merged script.
- `method` controls transformation style:
  - `CHGSAMEPCT` — same percentage change path
  - `CHGSAMEABS` — same absolute change path
  - `SAMEVALUE` — fixed level path

### 2. Run the same scenario on each backend

```bash
# fp.exe
uv run fp run examples/demand_shock.yaml --backend fpexe --output-dir artifacts/scenario_walkthrough_fpexe

# fppy
uv run fp run examples/demand_shock.yaml --backend fppy --output-dir artifacts/scenario_walkthrough_fppy
```

### 3. Run parity compare and inspect artifacts

```bash
uv run fp parity examples/demand_shock.yaml --with-drift --output-dir artifacts/scenario_walkthrough_parity
```

- Parity writes `parity_report.json` plus per-engine `PABEV.TXT` under the run directory.
- For interactive triage, launch dashboard against that artifacts root:

```bash
uv run fp dashboard --artifacts-dir artifacts/scenario_walkthrough_parity --port 8501
```

### 4. Triage from CLI when parity fails

```bash
uv run fp triage fppy-report artifacts/scenario_walkthrough_parity/<scenario>_<timestamp>
uv run fp triage parity-hardfails artifacts/scenario_walkthrough_parity/<scenario>_<timestamp>
```

For smoke runs using `fp run ... --backend both`, the same triage commands can target the top-level run dir:

```bash
uv run fp run examples/baseline_smoke.yaml --backend both --with-drift --output-dir artifacts/scenario_smoke_both
uv run fp triage fppy-report artifacts/scenario_smoke_both/<scenario>_<timestamp>
uv run fp triage parity-hardfails artifacts/scenario_smoke_both/<scenario>_<timestamp>
```

### 5. Save golden baseline and run regression checks

```bash
uv run fp parity examples/demand_shock.yaml --save-golden artifacts/parity-golden --output-dir artifacts/scenario_walkthrough_golden
uv run fp parity examples/demand_shock.yaml --regression artifacts/parity-golden --output-dir artifacts/scenario_walkthrough_regression
```
