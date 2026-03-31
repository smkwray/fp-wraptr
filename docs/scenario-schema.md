# Scenario YAML Schema

`ScenarioConfig` is defined in `src/fp_wraptr/scenarios/config.py`.

## Required fields

### `name` (string)
Scenario name. Used for output directory naming and display.

- **Type:** `str`
- **Default:** none (required)

## Optional fields

### `description` (string)
Human-readable scenario description.

- **Type:** `str`
- **Default:** `""`

### `fp_home` (path)
Path to the FP model directory containing `fp.exe`, input files, etc.

- **Type:** `Path`
- **Default:** `FM`
- **Note:** required for `fpexe`, `fppy`, and `both`; not required for bundle-backed `fp-r`

### `input_file` (string)
Input file name copied/processed in each run.

- **Type:** `str`
- **Default:** `fminput.txt`

### `forecast_start` (string)
Forecast start period in `YYYY.Q` format.

- **Type:** `str`
- **Default:** `"2025.4"`

### `forecast_end` (string)
Forecast end period in `YYYY.Q` format.

- **Type:** `str`
- **Default:** `"2029.4"`

### `backend` (string)
Execution backend.

- **Type:** `str`
- **Default:** `"fpexe"`
- **Allowed:** `fpexe`, `fppy`, `fp-r`, `both`
- Behavior:
  - `fpexe`: run legacy `fp.exe` only.
  - `fppy`: run the vendored `fppy` backend only (module name `fp_py`).
  - `fp-r`: run the bundle-backed R backend using `fpr.bundle_path`.
  - `both`: run both engines and emit parity artifacts (`parity_report.json` difference report + per-engine `PABEV.TXT` paths).

### `fppy` (dict)
Optional `fppy` backend settings (used when `backend: fppy` or `backend: both`; module name `fp_py`).

- **Type:** `dict`
- **Default:** `{}`
- Supported keys:
  - `timeout_seconds` (int, default `600`)
  - `eq_flags_preset` (string, default `"parity"`)
    - `"parity"`: enable EQ backfill and honor `SETUPSOLVE` for FP-style solve semantics.
    - `"default"` / `"off"`: run `fppy` without EQ backfill flags (faster, less FP-faithful).
    - `"iss02_baseline"`: legacy preset (kept for compatibility).
  - Any extra keys are preserved in scenario config but ignored by current runner implementation.

### `fpr` (dict)
Optional `fp-r` backend settings (used when `backend: fp-r`).

- **Type:** `dict`
- **Default:** `{}`
- Supported keys:
  - `bundle_path` (string, required for `backend: fp-r`) — path to a prebuilt `fp-r` bundle
  - `rscript_path` (string, optional) — explicit `Rscript` path; otherwise the backend searches `PATH`
  - `timeout_seconds` (int, default `120`) — subprocess timeout for the bundle-backed run
  - `expected_csv` (string, optional) — reference CSV used by `fp fpr-compare`
- Notes:
  - The phase-1 public `fp-r` surface is bundle-backed.
  - `fp-r` can participate in `fp parity` when the scenario provides both `fp_home` and `fpr.bundle_path`.

### `overrides` (dict)
Exogenous variable overrides keyed by variable name.

- **Type:** `dict[str, VariableOverride]`
- **Default:** `{}`

### `alerts` (dict)
Variable watch list used to flag forecast breaches.

- **Type:** `dict[str, dict[str, float]]`
- **Default:** `{}`
- Example:
  - `{"UR": {"max": 6.0}, "PCY": {"min": -2.0}}`

### `track_variables` (list)
Variables included in output summaries and charts.

- **Type:** `list[str]`
- **Default:** `[PCY, PCPF, UR, PIEF, GDPR]`

### `input_patches` (dict)
Input-file patches applied to the copied FP input file.

- **Type:** `dict[str, str]`
- **Default:** `{}`
- Supported key styles:
  - Literal replacement (legacy): `"<search text>": "<replace text>"`
  - Command-aware patch: `"cmd:COMMAND.PARAM": "<value>"`
  - Indexed command patch: `"cmd:COMMAND[index].PARAM": "<value>"`

### `extra` (dict)
Pass-through metadata for local tooling.

- **Type:** `dict`
- **Default:** `{}`

## Full YAML example

```yaml
name: alerts_example
description: "Run with stronger growth and alert checks"
fp_home: FM
input_file: fminput.txt
forecast_start: "2025.4"
forecast_end: "2029.4"
backend: fpexe
overrides:
  YS:
    method: CHGSAMEPCT
    value: 0.008
alerts:
  UR:
    max: 6.0
  PCY:
    min: -2.0
track_variables:
  - PCY
  - PCPF
  - UR
  - PIEF
  - GDPR
input_patches:
  "SETUPSOLVE MAXIT=8;": "SETUPSOLVE MAXIT=20;"
  "cmd:SETUPSOLVE.MAXCHECK": "80"
extra:
  owner: macro-team
  scenario_type: alerts-demo
```

## Minimal parity-focused example (`backend: both`)

```yaml
name: parity_baseline
fp_home: FM
backend: both
fppy:
  timeout_seconds: 900
  eq_flags_preset: iss02_baseline
forecast_start: "2025.4"
forecast_end: "2029.4"
track_variables: [AA, CS, UR, PIEF]
```

## Minimal `fp-r` example (`backend: fp-r`)

```yaml
name: r_bundle_demo
backend: fp-r
fpr:
  bundle_path: fpr_bundle_demo_bundle.R
  rscript_path: /usr/local/bin/Rscript
```

Tracked self-contained example:
- `examples/fpr_bundle_demo.yaml`

## Vendored engine scope

fp-wraptr ships a minimal vendored `fppy` execution/parity core (module name `fp_py`) for scenario runs and `PABEV.TXT` parity checks. Broader fair-py dictionary/release tooling is outside fp-wraptr runtime scope.
