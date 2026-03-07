# Dashboard Guide

## Prerequisites

```bash
pip install fp-wraptr[dashboard]
# or with uv:
uv sync --extra dashboard
```

Requires Streamlit + Plotly.

## Product posture

!!! info "Dashboard = inspect, MCP = author"
    The dashboard is optimized for **inspection, comparison, and visualization**. Use MCP-managed workspaces and agent prompts for scenario authoring, then use the dashboard to explore and compare results.

- Use the dashboard to browse runs, compare variants, inspect parity, and review visualizations.
- `New Run` keeps advanced manual authoring available, but hides it behind an explicit toggle by default.

## Launch

```bash
fp dashboard
fp dashboard --artifacts-dir artifacts --port 8501
```

### Fast smoke flow (`fp run --backend both`)

```bash
fp run examples/baseline_smoke.yaml --backend both --with-drift --output-dir artifacts/dashboard_smoke
fp dashboard --artifacts-dir artifacts/dashboard_smoke --port 8501
fp triage fppy-report artifacts/dashboard_smoke/<scenario>_<timestamp>
fp triage parity-hardfails artifacts/dashboard_smoke/<scenario>_<timestamp>
```

`fp run --backend both` creates a top-level run directory with convenience copies plus a nested parity run that contains canonical per-engine artifacts:

```text
artifacts/dashboard_smoke/<scenario>_<timestamp>/
  parity_report.json
  PABEV.TXT
  parity/<scenario>_<timestamp>/work_fpexe/PABEV.TXT
  parity/<scenario>_<timestamp>/work_fppy/PABEV.TXT
```

## Pages

### Parity
- Input artifacts:
  - required: `parity_report.json`, `work_fpexe/PABEV.TXT`, `work_fppy/PABEV.TXT`
  - optional: `triage_hardfails.csv` (or `work_fppy/triage_hardfails.csv`), `triage_hardfails_summary.json`, `work_fppy/support_gap_map.csv`, `work_fppy/support_gap_top.md`, `work_fppy/fppy_report.json`, `work_fppy/triage_summary.json`, `work_fppy/triage_issues.csv`, `parity_regression.json`
- Shows top-level parity status (`status`, `exit_code`, `hard_fail_cells`, max/median/p90 abs diff).
- Reads report metadata fields `schema_version` and `producer_version` when present in `parity_report.json`.
  - Backward compatibility: legacy reports missing these fields are still loaded; dashboard fallbacks remain `unknown`/`n/a` where metadata is unavailable.
- Renders variable x period abs-diff heatmap with start-period control.
- Surfaces hard-fail samples with filter + jump-to-series workflow.
- Compares fp.exe vs fppy series for selected variable with abs-diff overlay.
- Displays unsupported-command impact and example statements from `fppy_report.json` when present.
- Highlights solve controls and mode signals (`eq_flags_preset`, source, `eq_use_setupsolve`, iteration stats).
- Detects zero-filled forecast-window variables and flags likely 1-iteration trap conditions.
- Provides direct downloads for triage/support-gap artifacts and regression report when present.
- Includes an agent handoff panel for parity follow-up.

### Home
- Run listing and artifact discovery
- Quick counts for total runs and runs with outputs
- Entry point for navigation to other pages
- Good landing surface after agent-created runs complete

### Compare Runs
- Select two completed runs
- Compute run comparison metrics and top movers
- View delta bar chart and side-by-side forecast comparison
- Download comparison CSV
- Includes an agent handoff panel for explanation or visualization follow-up

### New Run
- Defaults to an agent-first handoff flow
- Keeps advanced manual authoring behind an explicit toggle
- Managed workspaces remain available when direct editing is needed

### Data Update
- Build a new runnable `FM/` bundle under `artifacts/` by running `fp data update-fred`
- Shows `data_update_report.json` for quick verification

### Equation Graph
- Upload or point to `fminput.txt`
- Build dependency graph summaries
- Inspect upstream/downstream nodes and export adjacency view

### Equations
- Browse all behavioral equations, identities, and generated variables
- Filter by section (equations, identities, generated vars) or search by variable name
- View equation specifications in readable format with expandable details

### Tweak Scenario
- Pick a completed run as a starting point
- Adjust variable overrides (add, remove, or change values)
- See a diff summary of what changed before running
- Re-run the tweaked scenario and immediately compare against the baseline
- Save tweaked config as a new YAML file

### Sensitivity Analysis
- Pick a completed run as the base case
- Sweep one override variable across a min/max range with configurable step count
- Set sweep method (`CHGSAMEPCT`, `SAMEVALUE`, `CHGSAMEABS`)
- Run sensitivity and see fan chart plus response table for tracked variables
