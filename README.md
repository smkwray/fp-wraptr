# fp-wraptr

<p align="center">
  <img src="logo/fp-wraptr-logo.png" alt="fp-wraptr mascots — Rex the velociraptor and Raptr the eagle" width="220">
</p>

<p align="center"><i>(/ˌɛf ˈpiː ˈræptər/)</i></p>

<p align="center">
  <b>Python utilities to modernize the Fair-Parke (FP) macroeconomic model workflow.</b>
</p>

<p align="center">
  <a href="https://smkwray.github.io/fp-wraptr/">
    <img src="https://img.shields.io/badge/docs-GitHub_Pages-blue?style=for-the-badge&logo=github" alt="Documentation">
  </a>
  &nbsp;
  <a href="https://smkwray.github.io/fp-wraptr/model-runs/">
    <img src="https://img.shields.io/badge/Model_Runs-Explorer-amber?style=for-the-badge&logo=github" alt="Model Runs Explorer">
  </a>
  &nbsp;
  <a href="https://fairmodel.econ.yale.edu/fp/fp.htm">
    <img src="https://img.shields.io/badge/%E2%AC%87%EF%B8%8F_Download-fp.exe_from_Yale-005eb8?style=for-the-badge&logo=data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIyNCIgaGVpZ2h0PSIyNCIgdmlld0JveD0iMCAwIDI0IDI0IiBmaWxsPSJ3aGl0ZSI+PHBhdGggZD0iTTEyIDJhMTAgMTAgMCAxIDAgMCAyMCAxMCAxMCAwIDAgMCAwLTIwem0xIDEwLjU5bDMuMy0zLjNhMSAxIDAgMCAxIDEuNCAxLjQybC00IDRhMSAxIDAgMCAxLTEuNDIgMGwtNC00YTEgMSAwIDAgMSAxLjQyLTEuNDJsMy4zIDMuM1Y3YTEgMSAwIDAgMSAyIDB2NS41OXoiLz48L3N2Zz4=" alt="Download fp.exe from Yale">
  </a>
</p>

---

fp-wraptr wraps Ray Fair's [US Macroeconometric Model](https://fairmodel.econ.yale.edu/), making it easier to run scenarios, inspect results, compare forecasts, and build on top of decades of economic modeling work — all from Python.

It reads the standard Fair Model files (`fminput.txt`, `fmdata.txt`, `fmexog.txt`, `fmout.txt`) directly, so you can use your existing model data as-is. On top of that, fp-wraptr adds YAML scenario configs, a compact DSL, and an MCP server for LLM-assisted authoring — pick variables, tweak assumptions, run, and compare results from a chat interface or the Streamlit dashboard.

## Meet the mascots

| Mascot | Represents |
|--------|-----------|
| **Rex** the Velociraptor | `fp.exe` — the original FORTRAN model |
| **Archie** the Archaeopteryx | `fppy` — the pure-Python solver |
| **Raptr** the Eagle | Agentic features — MCP server, packs, and workspace authoring |

<p align="center">
  <img src="logo/fp-py-logo.png" alt="Archie the archaeopteryx — fppy mascot" width="180">
</p>

## Features

- **YAML scenario configs** — human-readable definitions instead of raw `fminput.txt`
- **Structured I/O** — parse FP inputs and outputs into Python objects and DataFrames
- **Dual engines** — run the original FORTRAN binary or the pure-Python solver (fppy), or both for parity validation
- **Batch runner** — execute multiple scenarios with diff and regression testing
- **Dependency graph** — trace "why did variable X change?" through 130+ equations
- **Dashboard** — 12-page Streamlit app with Plotly charts for run exploration and comparison
- **Data pipelines** — FRED, BEA, and BLS ingestion with safe-lane update workflows
- **Scenario DSL** — human-readable DSL compiler for compact scenario definitions
- **Dictionary** — variable and equation lookup with source coverage and quality reports
- **MCP server** — 44 tools for LLM-assisted exploration and scenario authoring
- **Managed workspaces** — reusable scenario packs and templates for LLM-driven or manual authoring

## Quick start

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone and set up
git clone https://github.com/smkwray/fp-wraptr.git
cd fp-wraptr
uv sync --all-extras

# Run smoke tests
uv run pytest

# Try the CLI
uv run fp --help
uv run fp io parse-output FM/fmout.txt
uv run fp viz plot FM/fmout.txt --var PCY
uv run fp run examples/baseline.yaml --backend both
uv run fp parity examples/baseline.yaml --with-drift
```

## Dual engines: Rex vs Archie

fp-wraptr supports two solver backends:

- **`fpexe`** — the original Fair-Parke Windows binary (`fp.exe`). Battle-hardened FORTRAN. Runs via Wine on macOS/Linux.
- **`fppy`** — a pure-Python re-implementation of the FP solver core. No Wine, no binary blobs. Archie is learning to fly.

Run them head-to-head with **parity mode**:

```bash
# Rex and Archie solve the same scenario, then we compare PABEV.TXT outputs
fp parity examples/baseline.yaml --with-drift

# Or run both backends in a single command
fp run examples/baseline.yaml --backend both
```

Parity validation enforces hard-fail invariants (missing values, sign flips, discrete jumps) and produces `parity_report.json` with per-variable diff metrics. The latest stock-model parity run shows zero hard fails, an average relative difference of 0.000047%, and a max relative gap under 0.04%.

For the full parity operator playbook: [Parity docs](https://smkwray.github.io/fp-wraptr/parity/)

## Parity quickstart

Step-by-step guide covering both engines, parity comparison, triage, and dashboard launch: [Parity quickstart](https://smkwray.github.io/fp-wraptr/quickstart/#parity-quickstart)

## Dashboard

The Streamlit dashboard gives you 12 interactive pages for exploring runs, comparing scenarios, and diagnosing parity:

```bash
fp dashboard --artifacts-dir artifacts --port 8501
```

Pages include: Run Panels, Compare Runs, New Run, Equation Graph, Equations, Tweak Scenario, Sensitivity, Historical Fit, Dictionary, Data Update, and Parity.

See the [Dashboard guide](https://smkwray.github.io/fp-wraptr/dashboard/) for the full walkthrough.

## Model Runs Explorer

fp-wraptr can export completed runs as a static site you can share with your team — no server, no Python, just a browser.

Browse forecasts, compare scenarios side-by-side, and inspect variable-level results from any device. Useful for sharing results with collaborators who don't have the model installed.

```bash
fp export pages --spec public/model-runs.spec.yaml --artifacts-dir artifacts --out-dir public/model-runs
```

**[Live example →](https://smkwray.github.io/fp-wraptr/model-runs/)**

## File compatibility

fp-wraptr reads and writes the standard Fair Model file formats:

| File | Description |
|------|-------------|
| `fminput.txt` | Model control and equation definitions (custom DSL) |
| `fmdata.txt` | Historical time-series data (FORTRAN scientific notation) |
| `fmexog.txt` | Exogenous variable assumptions (`CHANGEVAR` blocks) |
| `fmout.txt` | Model output — estimation results and forecast tables |
| `fmage.txt` | Age-demographic variables |
| `PABEV.TXT` | Forecast output used for parity validation |

You can also define scenarios in **YAML** or a compact **DSL** — fp-wraptr compiles them into the native format before running.

### Input parser

The parser converts `fminput.txt` into Python dicts with all keys normalized to lowercase.

```python
from fp_wraptr.io.input_parser import parse_fp_input_text

result = parse_fp_input_text("SPACE MAXVAR=100 MAXS=10;\nSETUPEST MAXIT=30;")
assert result["space"]["maxvar"] == "100"
assert result["setupest"][0]["maxit"] == "30"
```

## Prerequisites

- **Python 3.11+**
- **FM/ folder**: Model data files (`fmdata.txt`, `fmage.txt`, `fmexog.txt`, `fminput.txt`). Not included in this repo — obtain from [fairmodel.econ.yale.edu](https://fairmodel.econ.yale.edu/fp/fp.htm).
- **fp.exe** *(optional)*: The original Fair-Parke FORTRAN binary. fp-wraptr includes `fppy`, a pure-Python solver, so `fp.exe` is not required. If you want to run the original engine or use parity mode, [download from Yale](https://fairmodel.econ.yale.edu/fp/fp.htm) and place in `FM/`, or set `FP_HOME`. (Windows binary; use Wine on macOS/Linux.)

## Project structure

```
fp-wraptr/
  src/fp_wraptr/        # Main Python package (CLI, IO, runtime, scenarios, analysis)
    cli.py              # Typer CLI (fp run, fp diff, fp io, fp viz, fp parity, ...)
    io/                 # Parse/write FP file formats
    runtime/            # Subprocess wrappers (fp.exe + fppy backends)
    scenarios/          # Scenario config, runner, batch, bundles, DSL
    analysis/           # Run comparison, diff, dependency graph, parity, triage
    dashboard/          # Streamlit dashboard helpers (artifacts, charts)
    data/               # FRED/BEA/BLS data update pipelines
    viz/                # Charts and plots
    mcp_server.py       # FastMCP server (44 tools, 9 resources, 6 prompts)
  src/fppy/             # Vendored pure-Python FP solver core
    cli.py              # Solver CLI
    eq_solver.py        # Equation system solver
    parser.py           # FP input DSL parser
    expressions.py      # Expression evaluator
    mini_run.py         # Mini-run execution
    parity.py           # Parity output formatting
    ...                 # + config, dependency, io/, etc.
  apps/dashboard/       # Streamlit dashboard (12 pages)
  tests/                # Pytest suite (81 files, 500+ tests)
  docs/                 # MkDocs documentation
  examples/             # Example scenario configs (YAML)
  bundles/              # Bundle configurations (multi-variant runs)
  logo/                 # Mascot logos (Rex, Raptr, Archie)
  FM/                   # Local FP runtime assets (gitignored)
  fortran_sc/           # FORTRAN source for fp.exe (reference)
```

## CLI reference

```bash
fp run scenario.yaml                           # Run a scenario
fp run scenario.yaml --backend fpexe|fppy|both # Choose engine backend
fp parity scenario.yaml                        # fp.exe vs fppy parity compare
fp run scenario.yaml --baseline baseline.yaml  # Run + diff vs baseline
fp validate scenario.yaml                      # Validate a scenario file
fp batch scenario.yaml ...                     # Run multiple scenarios
fp report run_dir                              # Render a run report
fp graph fminput.txt                           # Inspect dependency graph
fp history                                     # List historic runs
fp dashboard                                   # Start Streamlit dashboard
fp diff run_a/ run_b/                          # Compare two completed runs
fp io parse-output FM/fmout.txt                # Parse FP output to JSON
fp io parse-input FM/fminput.txt               # Parse FP input
fp viz plot FM/fmout.txt                       # Generate forecast charts
fp dsl compile scenario.dsl                    # Compile DSL to YAML/JSON
fp describe GDP                                # Describe one model variable
fp dictionary search "eq 82"                   # Search dictionary
fp dictionary equation 82                      # Explain one equation
fp dictionary sources GDP                      # Source-map + raw-data links
fp dictionary source-coverage                  # Source-map coverage
fp dictionary source-quality                   # Source-map quality audit
fp dictionary source-report                    # Combined coverage+quality
fp fred fetch GDP UNRATE                       # Fetch FRED series
fp version                                     # Print version
```

### The `fppy` pure-Python solver

fp-wraptr includes a vendored copy of **fppy**, a pure-Python re-implementation of the FP model's solve loop. It lives in `src/fppy/` and provides:

- **Scenario execution** without Wine or fp.exe — runs natively on macOS/Linux/Windows.
- **Parity validation** — run both engines side-by-side (`fp run --backend both`) and compare `PABEV.TXT` output cell-by-cell.
- **Equation solver** — the same behavioral equations and identities, solved iteratively in Python.

Only the minimal execution core is vendored (run, solve, parity). Broader tooling from the upstream fair-py project (dictionary generation, release scripts) is not included. Hard-fail invariants (`missing`, `discrete`, `signflip`) are always enforced regardless of numeric tolerances.

See the [Parity docs](https://smkwray.github.io/fp-wraptr/parity/) for interpretation, scenario-change policy, and asset provisioning.

## MCP server

fp-wraptr includes an optional [MCP](https://modelcontextprotocol.io/) server for LLM integration:

```bash
# Run with FastMCP dev mode (includes Inspector)
uv run fastmcp dev fp-mcp

# Or run directly
uv run fp-mcp
```

44 tools covering scenario runs, workspace management, dictionary lookup, data updates, and more. See the [MCP Tools reference](https://smkwray.github.io/fp-wraptr/mcp-tools/) for the full list.

9 resources are registered:

- `fp://output/variables`, `fp://output/equations` — parsed model catalogs
- `fp://packs`, `fp://pack/{pack_id}/cards`, `fp://pack/{pack_id}/recipes` — pack discovery
- `fp://workspace/{id}`, `fp://workspace/{id}/compile-report` — workspace state
- `fp://runs/latest`, `fp://runs/{run_id}/summary` — run history

6 prompts are registered for common agent tasks: variant creation, coefficient edits, series imports, bundle assembly, run comparison, and visualization prep.

Config file for MCP discoverability: `.mcp.json` (Claude Code).

## Development

```bash
uv sync --extra dev
uv run ruff check src/ tests/     # Lint
uv run ruff format src/ tests/    # Format
uv run pytest                     # Test
uv run mkdocs serve               # Docs (localhost:8000)
```

### Optional fp.exe integration tests

Real fp.exe integration tests are in `tests/test_fp_integration.py` and use the
`requires_fp` marker.

```bash
FP_HOME=/path/to/FM uv run pytest -m requires_fp tests/test_fp_integration.py -q
```

In CI, the optional `fp-integration` job is gated by repo variable
`FP_INTEGRATION_ENABLED=true`.

Provisioning options for `FM/` assets on runner:
- Runner-local directory:
  - Set repository variable `FP_ASSETS_SOURCE_DIR` to an absolute path on the runner.
- Archive download:
  - Set secret `FP_ASSETS_ARCHIVE_URL` (HTTPS URL to zip/tar archive containing FM files).
  - Optional secret `FP_ASSETS_BEARER_TOKEN` for authenticated downloads.
  - Optional variable `FP_ASSETS_ARCHIVE_SHA256` for integrity verification.
  - Optional variable `FP_ASSETS_ARCHIVE_TYPE` as `zip` or `tar` (`auto` by default).

The CI provisioning script is:
- `scripts/ci/provision_fp_assets.sh`

## License

MIT

## Acknowledgments

Built on top of Ray Fair's [US Macroeconometric Model](https://fairmodel.econ.yale.edu/) and the Fair-Parke program.

---

<p align="center"><sub>Rex has been solving equations since before Python was born. Archie is catching up.</sub></p>
