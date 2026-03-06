# Architecture

## Module layout

```
src/fp_wraptr/
  __init__.py           # Package root, version
  cli.py                # Typer CLI entry point
  io/
    parser.py           # FP output parser (fmout.txt -> FPOutputData)
    input_parser.py     # FP input parser (fminput.txt -> dict)
    writer.py           # Write FP-format files from Python objects
  runtime/
    fp_exe.py           # Subprocess wrapper for fp.exe
    fairpy.py           # Subprocess wrapper for fppy (pure-Python backend)
  scenarios/
    config.py           # Pydantic scenario config models
    runner.py           # Scenario execution pipeline
    batch.py            # Multi-scenario batch execution
  analysis/
    diff.py             # Run comparison and summary deltas
    parity.py           # Run both engines and compute PABEV-based parity report
    parity_regression.py# Golden baseline save/compare (new findings gate)
    triage_fppy.py      # Deterministic bucketing of fppy_report.json issues
    triage_parity_hardfails.py  # Recompute full hard-fail set from PABEV artifacts
    report.py           # Markdown report generation
    graph.py            # Dependency graph extraction and traversal
  viz/
    plots.py            # Matplotlib forecast charts
  dashboard/
    artifacts.py        # Artifact scanning and metadata helpers
    charts.py           # Plotly dashboard charts
  mcp_server.py         # FastMCP tool and resource definitions
```

## Data flow

```
Scenario YAML
    -> ScenarioConfig (Pydantic model)
    -> runner.py: prepare work dir, apply overrides
    -> fp_exe.py: invoke fp.exe subprocess (fpexe backend)
    -> fairpy.py: invoke fppy subprocess (fppy backend)
    -> parser.py: parse fmout.txt into FPOutputData
    -> report.py: generate markdown reports
    -> viz/plots.py: generate charts
    -> analysis/diff.py: compare vs baseline
    -> batch.py: run multiple scenarios

Parity (fp.exe vs fppy)
    -> analysis/parity.py: run both engines into `work_fpexe/` and `work_fppy/`
    -> PABEV.TXT contract: both engines emit PRINTVAR LOADFORMAT output
    -> fppy_report.json: fppy emits a structured execution/solve report
    -> toleranced_compare(): compute hard-fails + toleranced numeric diffs
    -> parity_report.json: machine-readable comparison payload (dashboard input)
    -> analysis/parity_regression.py: compare run vs golden baseline (new findings gate)
    -> analysis/triage_*.py: emit deterministic CSV/JSON triage artifacts for debugging

fminput.txt
    -> input_parser.py: structured input model
    -> graph.py: build dependency graph
    -> viz/analysis pages via dashboard

Artifacts + Dashboard
    -> scan_artifacts() from dashboard/artifacts.py
    -> charts.py: forecast/comparison/delta charts
    -> cli.py dashboard command: launch Streamlit pages
```

## Key design decisions

### Subprocess-first for fp.exe
We call `fp.exe` as a subprocess rather than compiling Fortran into a Python extension. This is simpler, more portable (any platform that can run the binary), and avoids complex build tooling. The tradeoff is requiring Wine on macOS/Linux.

### Pydantic for config
Scenario configs use Pydantic v2 models for validation, serialization, and type safety. YAML is the user-facing format.

### Copy-and-patch for input overrides
The initial approach to scenario overrides is simple text-level patching of `fminput.txt`. A future AST-level manipulation layer will replace this once the input parser is complete.

### Canonical parser contract
`parse_fp_input_text` now emits normalized, lower-case dictionary keys (for commands and parameters) and removes ambiguous alias duplication so consumers can rely on one canonical shape.

### Optional extras
Heavy dependencies (matplotlib, streamlit, fastmcp, networkx) are optional extras to keep the core package lightweight.

## FP file format reference

See `llms.txt` at the repo root for a concise format summary.

### fminput.txt
Custom DSL with commands: `SPACE`, `SETUPSOLVE`, `LOADDATA`, `SMPL`, `CREATE`, `GENR`, `EQ`, `IDENT`, `SOLVE`, `CHANGEVAR`, `PRINTVAR`, etc. Comments start with `@`.

### fmdata.txt
FORTRAN-format time series: `SMPL <start> <end>; LOAD <varname>;` followed by scientific notation floats in fixed-width columns.

### fmout.txt
Program output log containing:
1. Echo of input commands with program responses
2. Estimation results (coefficients, R-squared, etc.)
3. Solve iteration log
4. Forecast table: variable ID, name, level/change/pct-change rows

### fmexog.txt
Exogenous variable assumptions: `CHANGEVAR;` blocks with `<VARNAME> <METHOD>` and values. Methods: `CHGSAMEPCT`, `SAMEVALUE`, `CHGSAMEABS`.
