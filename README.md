# fp-wraptr

<p align="center">
  <img src="logo/fp-wraptr-logo.png" alt="fp-wraptr logo" width="200">
</p>

[Documentation](https://smkwray.github.io/fp-wraptr/) · [Model Runs Explorer](https://smkwray.github.io/fp-wraptr/model-runs/)

fp-wraptr is a Python toolkit for running Ray Fair's [US Macroeconometric Model](https://fairmodel.econ.yale.edu/) and its Fair-Parke (FP) program from the command line or from Python. It reads the standard model files (`fminput.txt`, `fmdata.txt`, `fmexog.txt`, `fmout.txt`) as they are, so you can keep your existing model data. Instead of editing `fminput.txt` and `fmexog.txt` by hand for each experiment, you describe a scenario in a short YAML file, run it, and compare it with a baseline.

It can solve a scenario three ways: with Yale's original `fp.exe`, with `fppy` (a pure-Python solver included in this package), or with `fp-r` (an R solver that runs prebuilt bundles). A parity check compares the engines cell by cell. fp-wraptr is alpha software. It does not include the model files, which you download from Yale.

## Example

`examples/fiscal_expansion.yaml` raises the government spending share and tracks a few headline variables:

```yaml
name: fiscal_expansion
description: "Fiscal expansion via higher government spending share"
fp_home: ../FM
input_file: fminput.txt
forecast_start: "2025.4"
forecast_end: "2029.4"
overrides:
  TBGQ:
    method: CHGSAMEPCT
    value: 0.010
track_variables: [PCY, PCPF, UR, PIEF, GDPR, TBG]
```

```bash
uv run fp run examples/fiscal_expansion.yaml --baseline examples/baseline.yaml
```

This runs the scenario and the baseline with `fp.exe` (the default backend), writes both runs under `artifacts/`, and prints a table of each tracked variable's last forecast value under both runs, with the difference and percent change.

## Features

- YAML scenario files that compile to the native FP input format, plus a compact scenario DSL.
- Parsers that turn FP inputs and outputs into Python objects and pandas DataFrames.
- Batch runs, run-to-run diffs, and regression checks against saved baselines.
- An equation dependency graph for tracing why a variable changed.
- A variable and equation dictionary with links to source data.
- FRED, BEA and BLS data update pipelines.
- A static Model Runs Explorer for sharing results in a browser.
- An optional MCP (Model Context Protocol) server for driving the model from an AI assistant.

## Prerequisites

- Python 3.11 or later and [uv](https://docs.astral.sh/uv/).
- The model files (`fmdata.txt`, `fmage.txt`, `fmexog.txt`, `fminput.txt`) from [Yale](https://fairmodel.econ.yale.edu/fp/fp.htm), placed in `FM/` at the repository root (where the examples look) or in a directory named by `FP_HOME`.
- Optional: `fp.exe` from the same page, in the same directory, to use the original engine or run parity checks. It is a Windows program; on macOS and Linux it runs under Wine.
- Optional: R, with `Rscript` on the path, for the `fp-r` backend.

## Quick start

```bash
git clone https://github.com/smkwray/fp-wraptr.git
cd fp-wraptr
uv sync --all-extras        # creates .venv with all optional features
uv run pytest               # tests that need FM/, fp.exe or Rscript are skipped without them
uv run fp --help

uv run fp run examples/fpr_bundle_demo.yaml --backend fp-r   # needs only Rscript

# These need the model files in FM/
uv run fp run examples/baseline.yaml --backend fppy
uv run fp io parse-output FM/fmout.txt
uv run fp viz plot FM/fmout.txt --var PCY
```

The [CLI reference](https://smkwray.github.io/fp-wraptr/cli/) covers every command, and the [scenario guide](https://smkwray.github.io/fp-wraptr/scenarios/) covers the YAML format and DSL.

## Solver backends

Choose a backend with `--backend` or a scenario's `backend:` field.

- `fpexe` runs Yale's `fp.exe`.
- `fppy` is a pure-Python implementation of the FP solve loop, vendored in `src/fppy/`. It needs no Wine or binary and runs on macOS, Linux and Windows.
- `fp-r` runs a prebuilt R bundle named by the scenario's `fpr.bundle_path`. It is not a full replacement for `fminput.txt`. `examples/fpr_bundle_demo.yaml` is a self-contained demo that needs no `FM/` or `fp.exe`.

All three write `PABEV.TXT`-style output, which the parity check compares:

```bash
uv run fp parity examples/baseline.yaml --with-drift                      # fp.exe vs fppy
uv run fp parity my_scenario.yaml --left fpexe --right fp-r --lenient     # any pair
```

Parity fails on missing values, sign flips and discrete jumps, and writes per-variable differences to `parity_report.json`. On a full stock-model run, `fppy` had zero hard fails, an average relative difference of 0.000047%, and a max relative gap under 0.04% against `fp.exe`; `fp-r` stayed within about 0.0045 max absolute difference and about 0.04% max relative difference. See the [parity guide](https://smkwray.github.io/fp-wraptr/parity/).

## Model Runs Explorer

Export completed runs as a static site that anyone can browse without Python or the model installed ([live example](https://smkwray.github.io/fp-wraptr/model-runs/)):

```bash
uv run fp export pages --spec public/model-runs.spec.yaml --artifacts-dir artifacts --out-dir public/model-runs
```

## File compatibility

| File | Contents |
|------|----------|
| `fminput.txt` | Model control and equation definitions |
| `fmdata.txt` | Historical time-series data |
| `fmexog.txt` | Exogenous variable assumptions (`CHANGEVAR` blocks) |
| `fmout.txt` | Estimation results and forecast tables |
| `fmage.txt` | Age-demographic variables |
| `PABEV.TXT` | Forecast output used for parity checks |

## MCP server

```bash
uv run fp-mcp
```

The server offers 43 tools, 9 resources and 6 prompts for running scenarios, managing workspaces, looking up variables and updating data. See the [MCP guide](https://smkwray.github.io/fp-wraptr/mcp/) and [tool reference](https://smkwray.github.io/fp-wraptr/mcp-tools/).

## Project structure

```
src/fp_wraptr/    Python package: CLI, file I/O, backends, scenarios, analysis, data, MCP server
src/fppy/         Vendored pure-Python FP solver
fp-r/             R runtime for the fp-r backend
examples/         Example scenarios
docs/             MkDocs documentation
public/           Published Model Runs Explorer bundle
FM/               Your local model files (not distributed)
```

## Development

```bash
uv run ruff check src/ tests/
uv run ruff format src/ tests/
uv run pytest
uv run mkdocs serve                     # docs at localhost:8000
FP_HOME=/path/to/FM uv run pytest -m requires_fp tests/test_fp_integration.py -q   # real fp.exe tests
```

## License

MIT; see [LICENSE](LICENSE).

## Acknowledgments

Built on Ray Fair's [US Macroeconometric Model](https://fairmodel.econ.yale.edu/) and the Fair-Parke program.
