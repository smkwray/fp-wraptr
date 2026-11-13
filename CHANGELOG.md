# Changelog

All notable changes to this project will be documented in this file.

## [0.2.0] - Unreleased

### Added
- Pure-Python solver backend (`fppy`) with dual-engine parity validation.
- Bundle-backed R solver backend (`fp-r`, `--backend fp-r`), including explicit parity pairs such as `fp parity --left fpexe --right fp-r`.
- MCP server with 43 tools, 9 resources, and 6 structured prompts.
- Agent-first workspace authoring: packs, cards, recipes, managed workspaces.
- FRED, BEA, and BLS data ingestion pipelines with a safe-lane update workflow (`fp data update-fred`).
- Official Fair bundle fetch and verification (`fp data fetch-fair-bundle`).
- Scenario DSL compiler (`fp dsl compile`).
- Parity regression testing with golden baselines (`fp parity --regression`).
- Triage tools for fppy reports and parity hard-fails.
- Variable dictionary with source coverage and quality reports.
- Sensitivity sweeps with response tables (`fp_wraptr.analysis.sensitivity`).
- Static Model Runs Explorer export for GitHub Pages (`fp export pages`).

### Changed
- Input parser now uses canonical snake_case keys throughout.
- Scenario runner supports backend selection (`fpexe`/`fppy`/`fp-r`/`both`).
- Development uses the standard project-local `.venv` (`uv sync --all-extras`, `uv run ...`); the CLI no longer refuses to run when a repo-local `.venv/` exists.
- Bundle-backed `fp-r` scenarios (`fpr.bundle_path`) no longer require the `FM/` model files.
- `fp triage backend-release-corpus --manifest` is now a required option.

### Fixed
- Scenario `overrides` are written to the run's `fmexog.txt` after input staging, so the staged base file no longer replaces them.
- `fp triage scenario-delta-compare` no longer fails while writing its report.

### Removed
- The Streamlit dashboard (`fp dashboard`, the `dashboard` extra, and `fp_wraptr.dashboard`). Use the Model Runs Explorer export, `fp report`, `fp diff`, or the MCP visualization tools instead. Run-directory discovery moved to `fp_wraptr.run_artifacts`.

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
- MCP server with 8 tools: `run_fp_scenario`, `parse_fp_output`, `diff_runs`,
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
