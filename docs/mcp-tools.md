# MCP Tool Reference

Source of truth: `src/fp_wraptr/mcp_server.py`.

## Tools (44)

## Capability matrix

| Tool | Class |
|---|---|
| `get_project_info` | read-only (local) |
| `validate_scenario` | read-only (local) |
| `list_scenarios` | read-only (local) |
| `get_run_history` | read-only (local) |
| `list_packs` | read-only (local) |
| `describe_pack` | read-only (local) |
| `list_workspaces` | read-only (local) |
| `get_workspace` | read-only (local) |
| `list_workspace_cards` | read-only (local) |
| `list_visualizations` | read-only (local) |
| `build_visualization_view` | read-only (local) |
| `get_latest_run` | read-only (local) |
| `get_parity_report` | read-only (local) |
| `parse_fp_output` | read-only (local) |
| `list_output_variables` | read-only (local) |
| `list_output_equations` | read-only (local) |
| `describe_variable` | read-only (local) |
| `search_dictionary` | read-only (local) |
| `explain_equation` | read-only (local) |
| `describe_variable_sources` | read-only (local) |
| `source_map_coverage` | read-only (local) |
| `source_map_quality` | read-only (local) |
| `source_map_report` | read-only (local) |
| `diff_runs` | read-only (local) |
| `source_map_window_check` | read-only (network/FRED) |
| `run_fp_scenario` | mutating/execution |
| `run_bundle` | mutating/execution |
| `run_pse2025` | mutating/execution |
| `update_model_from_fred` | mutating/execution |
| `run_batch_scenarios` | mutating/execution |
| `create_scenario` | mutating (writes files) |
| `update_scenario` | mutating (writes files) |
| `create_workspace_from_catalog` | mutating (writes files) |
| `create_workspace_from_bundle` | mutating (writes files) |
| `update_workspace_metadata` | mutating (writes files) |
| `apply_workspace_card` | mutating (writes files) |
| `import_workspace_series` | mutating (writes files) |
| `add_bundle_variant` | mutating (writes files) |
| `update_bundle_variant` | mutating (writes files) |
| `clone_bundle_variant_recipe` | mutating (writes files) |
| `remove_bundle_variant` | mutating (writes files) |
| `compile_workspace` | mutating (writes files) |
| `run_workspace` | mutating/execution |
| `compare_workspace_runs` | mutating/execution |

### `get_project_info`
- **Parameters**
  - None.
- **Returns**
  - JSON payload with project name, version, description, mascot roster, available backends, and tool count.

### `validate_scenario`
- **Parameters**
  - `yaml_content: str` — Raw scenario YAML text.
- **Returns**
  - JSON payload with `valid` and key scenario metadata, or an error payload.

### `list_scenarios`
- **Parameters**
  - `examples_dir: str = "examples"` — Directory containing scenario YAML files.
- **Returns**
  - JSON array of scenario metadata (path, parsed name/description, parse errors if any).

### `get_run_history`
- **Parameters**
  - `artifacts_dir: str = "artifacts"` — Root artifacts directory.
- **Returns**
  - JSON object with run count and discovered run metadata.

### Agent-first workspace tools

- `list_packs`
  - Returns discovered local/public pack manifests with family, visibility, cards scope, recipe count, and visualization count.
- `describe_pack`
  - Returns one pack manifest plus resolved catalog entries, exposed cards, named recipes, and visualization presets.
- `list_workspaces`
  - Returns managed workspace summaries with stable `workspace_id`, family, forecast window, linked runs, and recent recipe history.
- `create_workspace_from_catalog`
  - Creates a managed scenario or bundle workspace from a catalog entry and returns the workspace payload.
- `create_workspace_from_bundle`
  - Creates a managed bundle workspace directly from a bundle YAML path.
- `get_workspace`
  - Returns one workspace payload by `workspace_id`.
- `update_workspace_metadata`
  - Updates label, description, forecast window, backend, or tracked variables.
- `list_workspace_cards`
  - Returns the current cards/defaults for a workspace so weaker agents can discover editable surfaces.
  - Optional `variant_id` inspects bundle-variant-local card state instead of shared bundle cards.
- `apply_workspace_card`
  - Applies constant/target changes to one card via `constants_json`.
- `import_workspace_series`
  - Imports quarterly points into a series card via `series_json`, `pasted_text`, or `csv_path`.
- `add_bundle_variant` / `remove_bundle_variant`
  - Mutate managed bundle variants without editing raw bundle YAML.
  - `add_bundle_variant` accepts an optional exact `scenario_name` so run/artifact names can stay deliberate instead of inheriting a bundle prefix.
- `update_bundle_variant`
  - Updates metadata for an existing bundle variant without editing raw draft YAML.
  - Supports `label`, exact `scenario_name`, `input_file`, and `enabled`.
- `clone_bundle_variant_recipe`
  - Clones an existing bundle variant, applies metadata updates, and can seed one constant-card patch in the same call.
  - Intended as the high-level managed recipe for research-variant setup.
- `compile_workspace`
  - Compiles a managed workspace and returns generated files, compile report path, and visualization suggestions.
- `run_workspace`
  - Compiles and runs a managed workspace and stores linked run metadata back on the workspace.
- `compare_workspace_runs`
  - Diffs two linked runs for a workspace and returns the standard diff payload.
- `list_visualizations`
  - Returns default plus pack-provided visualization views for a workspace or pack.
- `build_visualization_view`
  - Builds a chart-ready payload from recent or explicit run directories.

### `get_latest_run`
- **Parameters**
  - `artifacts_dir: str = "artifacts"` — Root artifacts directory.
  - `scenario_filter: str = ""` — Optional scenario name substring to filter by.
  - `limit: int = 1` — Number of recent runs to return.
- **Returns**
  - JSON object with `count` and `runs` array, each entry including `run_dir`, `fmout_path`, `has_parity_report`, and `backend_hint`.

### `get_parity_report`
- **Parameters**
  - `run_dir: str = ""` — Explicit run directory. If empty, searches for the latest run with a parity report.
  - `artifacts_dir: str = "artifacts"` — Root artifacts directory (used when `run_dir` is empty).
- **Returns**
  - Raw JSON content of `parity_report.json`, or error payload if not found.

### `run_fp_scenario`
- **Parameters**
  - `scenario_yaml: str` — Path to scenario YAML file.
  - `output_dir: str = "artifacts"` — Output directory for run artifacts.
  - `backend: str = ""` — Solver backend: `"fpexe"`, `"fppy"`, or `"both"` (parity mode). Empty uses scenario default.
- **Returns**
  - JSON summary with scenario name, success, backend used, `fmout_path`, output paths, `backend_diagnostics` (when backend=both), and tracked forecast series.

### `run_bundle`
- **Parameters**
  - `bundle_yaml: str` — Path to bundle YAML file.
  - `output_dir: str = "artifacts/bundles"` — Output directory root for bundle artifacts.
- **Returns**
  - JSON summary of bundle execution plus `run_root` and `report_path`.

### `run_pse2025`
- **Parameters**
  - `output_dir: str = "artifacts/pse2025"` — Output root for generated PSE2025 runs.
  - `fp_home: str = "FM"` — Base model directory.
  - `overlay_dir: str = "projects_local/pse2025"` — PSE overlay directory.
- **Returns**
  - JSON summary of base/low/high PSE2025 bundle run plus `run_root`, `report_path`, resolved paths.

### `update_model_from_fred`
- **Parameters**
  - `model_dir: str = "FM"` — Base model directory (contains `fmdata.txt`).
  - `out_dir: str = "artifacts/model_updates/latest"` — Output directory for updated bundle + report.
  - `end_period: str = "2025.4"` — Update end period (`YYYY.Q`).
  - `extend_sample: bool = False` — Extend `fmdata` sample to `end_period` when needed.
  - `allow_carry_forward: bool = False` — When extending, carry forward values for variables without new observations.
  - `replace_history: bool = False` — Apply updates within the existing history window too.
  - `variables: str = ""` — Optional comma/space-separated variable names to update.
  - `sources: str = "fred"` — Optional comma/space-separated source list (`fred`, `bea`, `bls`).
  - `source_map_path: str = ""` — Optional source-map YAML path override.
  - `cache_dir: str = ""` — Optional FRED cache directory override.
  - `patch_fminput_smpl_endpoints: bool = False` — Optionally patch `fminput.txt` sample endpoints to updated window.
- **Returns**
  - JSON payload with `success`, output paths, and the full `data_update_report` content.

### `run_batch_scenarios`
- **Parameters**
  - `scenario_names: list[str]` — Scenario stems from `examples/` (without `.yaml`).
  - `output_dir: str = "artifacts/batch"` — Output directory for batch runs.
- **Returns**
  - JSON payload with per-scenario results and aggregate totals (`total`, `succeeded`, `failed`).

### `create_scenario`
- **Parameters**
  - `yaml_content: str` — Raw scenario YAML text.
  - `filename: str` — Destination filename (extension optional; defaults to `.yaml`).
  - `examples_dir: str = "examples"` — Target examples directory.
- **Returns**
  - JSON payload with `created`, `path`, and scenario `name` (or error).

### `update_scenario`
- **Parameters**
  - `scenario_path: str` — Existing scenario YAML file path.
  - `yaml_content: str` — Updated scenario YAML text.
- **Returns**
  - JSON payload with `updated`, `path`, and scenario `name` (or error).

### `parse_fp_output`
- **Parameters**
  - `path: str = "FM/fmout.txt"` — FP output file path.
  - `format: str = "json"` — `json` for structured payload, `csv` for tabular CSV text.
- **Returns**
  - Parsed output as JSON string or CSV text.

### `list_output_variables`
- **Parameters**
  - `path: str = "FM/fmout.txt"` — Input forecast output path.
- **Returns**
  - JSON object with variable metadata (ID and series lengths).

### `list_output_equations`
- **Parameters**
  - `path: str = "FM/fmout.txt"` — Input forecast output path.
- **Returns**
  - JSON object with parsed estimation/equation metadata.

### `describe_variable`
- **Parameters**
  - `code: str` — Variable code (for example `GDP` or `UR`).
  - `dictionary_path: str = ""` — Optional dictionary JSON path override.
- **Returns**
  - JSON variable record or error payload if not found.

### `search_dictionary`
- **Parameters**
  - `query: str` — Search text, variable code, or equation id.
  - `limit: int = 10` — Maximum matches per section.
  - `dictionary_path: str = ""` — Optional dictionary JSON path override.
- **Returns**
  - JSON payload with ranked `equation_matches` and `variable_matches`.
  - Includes `intent` + `focus` for compact query forms.
  - Includes `links` metadata on matches (`lhs_variables`, `rhs_variables`, defining/usage equation links).
- **Query examples**
  - `eq 82`
  - `GDP equation`
  - `UR in equation 30`
  - `variables in equation 82`
  - `meaning of UR in equation 30`
  - `consumer expenditures`

### `explain_equation`
- **Parameters**
  - `eq_id: int` — Equation number.
  - `dictionary_path: str = ""` — Optional dictionary JSON path override.
- **Returns**
  - JSON payload with equation metadata and per-variable explanation data.
  - Includes `cross_links` summary for downstream UI drill-down.

### `describe_variable_sources`
- **Parameters**
  - `variable: str` — Variable code (for example `GDP`).
  - `dictionary_path: str = ""` — Optional dictionary JSON path override.
  - `source_map_path: str = ""` — Optional source-map YAML path override.
- **Returns**
  - JSON payload that merges source-map metadata and dictionary raw-data links for one variable.
  - Includes `normalization` guidance when `annual_rate: true` (for example divisor `4` for quarterly AR and `12` for monthly AR).

### `source_map_coverage`
- **Parameters**
  - `dictionary_path: str = ""` — Optional dictionary JSON path override.
  - `source_map_path: str = ""` — Optional source-map YAML path override.
  - `only_with_raw_data: bool = False` — If true, scope coverage to variables that have `raw_data_sources`.
- **Returns**
  - JSON coverage summary: mapped/missing counts, coverage percent, source breakdown, and missing variable list.

### `source_map_quality`
- **Parameters**
  - `dictionary_path: str = ""` — Optional dictionary JSON path override.
  - `source_map_path: str = ""` — Optional source-map YAML path override.
  - `only_with_raw_data: bool = False` — If true, scope quality checks to variables with `raw_data_sources`.
- **Returns**
  - JSON quality audit summary with issue counts and per-variable findings (`missing_series_id`, `missing_bea_locator`, `invalid_source`, `invalid_frequency`, `missing_description`).

### `source_map_report`
- **Parameters**
  - `dictionary_path: str = ""` — Optional dictionary JSON path override.
  - `source_map_path: str = ""` — Optional source-map YAML path override.
- **Returns**
  - Combined deterministic report including dictionary/source-map counts, coverage metrics, and quality audit summaries for all variables and raw-data-linked subsets.

### `source_map_window_check`
- **Parameters**
  - `source_map_path: str = ""` — Optional source-map YAML path override.
  - `start: str = ""` — Optional observation start date (`YYYY-MM-DD`).
  - `end: str = ""` — Optional observation end date (`YYYY-MM-DD`).
  - `tolerance: float = 0.0` — Absolute tolerance for outside-window deviation checks.
  - `cache_dir: str = ""` — Optional FRED cache directory override.
- **Returns**
  - JSON report for windowed FRED mappings, including per-series status (`ok`, `violation`, `series_missing`, `no_observations`) and violation diagnostics.

### `diff_runs`
- **Parameters**
  - `run_a: str` — Baseline run directory.
  - `run_b: str` — Comparison run directory.
  - `top_n: int = 10` — Number of top deltas to include.
- **Returns**
  - JSON diff summary from `analysis.diff.diff_run_dirs`.

## Resources (9)

### `fp://output/variables`
- No parameters.
- Returns `list_output_variables("FM/fmout.txt")`.

### `fp://output/equations`
- No parameters.
- Returns `list_output_equations("FM/fmout.txt")`.

### `fp://packs`
- No parameters.
- Returns `list_packs()`.

### `fp://pack/{pack_id}/cards`
- Parameter: `pack_id`.
- Returns the cards surfaced by one pack.

### `fp://pack/{pack_id}/recipes`
- Parameter: `pack_id`.
- Returns the named recipes surfaced by one pack.

### `fp://workspace/{workspace_id}`
- Parameter: `workspace_id`.
- Returns `get_workspace(workspace_id)`.

### `fp://workspace/{workspace_id}/compile-report`
- Parameter: `workspace_id`.
- Returns the latest compile report for the workspace when present.

### `fp://runs/latest`
- No parameters.
- Returns `get_latest_run(limit=5)`.

### `fp://runs/{run_id}/summary`
- Parameter: `run_id`.
- Returns a compact parsed summary for one discovered run.

## Prompts

- Create a variant from base/high/low
- Change coefficients safely
- Attach a new series override
- Build a bundle of policy variants
- Compare latest family runs
- Prepare dashboard visualization set
