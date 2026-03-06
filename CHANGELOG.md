# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0] - Unreleased

### Added
- Scenario YAML configuration with Pydantic validation (`fp run`, `fp validate`).
- FP output parser: forecast tables, estimation results (31 equations), solve iterations.
- FP input file parser: full DSL support with normalized canonical keys.
- Exogenous override generation: single-value and multi-period series.
- Run comparison with ranked variable deltas (`fp diff`).
- Equation dependency graph analysis with networkx (`fp graph`).
- Batch scenario runner with golden-output regression comparison (`fp batch`).
- Diff export to CSV and Excel (`fp diff --export csv|excel`).
- Matplotlib forecast and comparison charts (`fp viz plot`).
- Interactive Streamlit dashboard with Plotly charts (Explore, Compare, New Run pages).
- Run artifact discovery from filesystem (`fp history`).
- MCP server expanded to 8 tools: `run_fp_scenario`, `parse_fp_output`, `diff_runs`,
  `list_output_variables`, `list_output_equations`, `validate_scenario`, `list_scenarios`,
  `run_batch_scenarios`.
- Dashboard: equation graph visualization page.
- Dashboard: report download on Explore page, diff CSV download on Compare page.
- Markdown run report generation (`fp report`).
- 96 tests covering parsers, CLI, dashboard, writer, batch, graph modules.
- CLI with subcommands: run, validate, diff, graph, batch, report, history, dashboard, io, viz.
- Example scenarios: baseline, higher_growth, tight_monetary, recession, supply_shock, fiscal_expansion.

### Known limitations
- `fp.exe` requires Wine on macOS/Linux (Windows PE32 binary).
- FM/ model data files are not distributable.
- Dashboard caching may serve stale data on scenario re-run to same directory.
- Equation graph requires optional `networkx` dependency (`pip install fp-wraptr[graph]`).
- Excel export requires optional `openpyxl` dependency.

### Requirements
- Python 3.11+
- Wine (for `fp.exe` execution on non-Windows)
