# MCP Server Reference

`fp-wraptr` ships a FastMCP server for LLM-driven workflows.

The recommended use is now workspace-first authoring: agents discover packs, create managed workspaces, mutate cards/series/variants, compile, run, compare, and hand visualization payloads back to the dashboard.

Source of truth: `src/fp_wraptr/mcp_server.py`.

## Install + start

```bash
uv sync --extra mcp
uv run fp-mcp
```

For Inspector-driven development:

```bash
uv run fastmcp dev fp-mcp
```

## Client config

### Claude Code (`.mcp.json`)

```json
{
  "mcpServers": {
    "fp-wraptr": {
      "command": "uv",
      "args": ["run", "fp-mcp"],
      "env": {}
    }
  }
}
```

### Codex (`.codex/config.toml`)

```toml
[mcp_servers.fp-wraptr]
command = "uv"
args = ["run", "fp-mcp"]
```

## Capability classes (safe exposure guide)

Use this split when deciding what to expose to users/agents.

| Class | Tools | Notes |
|---|---|---|
| Read-only (local files) | `validate_scenario`, `list_scenarios`, `get_run_history`, `get_latest_run`, `get_parity_report`, `parse_fp_output`, `list_output_variables`, `list_output_equations`, `describe_variable`, `search_dictionary`, `explain_equation`, `describe_variable_sources`, `source_map_coverage`, `source_map_quality`, `source_map_report`, `diff_runs`, `list_packs`, `describe_pack`, `list_workspaces`, `get_workspace`, `list_workspace_cards`, `list_visualizations`, `build_visualization_view` | No writes expected. |
| Read-only (network) | `source_map_window_check` | Reads FRED data; needs `fredapi` and `FRED_API_KEY`. |
| Mutating / execution | `run_fp_scenario`, `run_bundle`, `run_pse2025`, `update_model_from_fred`, `run_batch_scenarios`, `create_scenario`, `update_scenario`, `create_workspace_from_catalog`, `create_workspace_from_bundle`, `update_workspace_metadata`, `apply_workspace_card`, `import_workspace_series`, `add_bundle_variant`, `remove_bundle_variant`, `compile_workspace`, `run_workspace`, `compare_workspace_runs` | Can write workspace files or run model jobs under `artifacts/`, `examples/`, or authoring directories. |

## Tool inventory (41)

- Scenario + run orchestration: `run_fp_scenario`, `run_bundle`, `run_pse2025`, `run_batch_scenarios`, `get_run_history`
- Scenario authoring/validation: `validate_scenario`, `list_scenarios`, `create_scenario`, `update_scenario`
- Agent-first workspace authoring: `list_packs`, `describe_pack`, `list_workspaces`, `create_workspace_from_catalog`, `create_workspace_from_bundle`, `get_workspace`, `update_workspace_metadata`, `list_workspace_cards`, `apply_workspace_card`, `import_workspace_series`, `add_bundle_variant`, `remove_bundle_variant`, `compile_workspace`, `run_workspace`, `compare_workspace_runs`, `list_visualizations`, `build_visualization_view`
- Output parsing + comparison: `parse_fp_output`, `list_output_variables`, `list_output_equations`, `diff_runs`
- Dictionary + source-map introspection: `describe_variable`, `search_dictionary`, `explain_equation`, `describe_variable_sources`, `source_map_coverage`, `source_map_quality`, `source_map_report`, `source_map_window_check`
- Data update workflow: `update_model_from_fred`

Detailed parameters and return payloads are documented in [`mcp-tools.md`](mcp-tools.md).

## Resources (9)

- `fp://output/variables` — variable metadata from `FM/fmout.txt` via `list_output_variables`
- `fp://output/equations` — equation metadata from `FM/fmout.txt` via `list_output_equations`
- `fp://packs` — discovered pack manifests
- `fp://pack/{pack_id}/cards` — cards exposed by one pack
- `fp://pack/{pack_id}/recipes` — named recipes exposed by one pack
- `fp://workspace/{workspace_id}` — one managed workspace payload
- `fp://workspace/{workspace_id}/compile-report` — latest compile report for a workspace
- `fp://runs/latest` and `fp://runs/{run_id}/summary` — recent run summaries

## Prompts

FastMCP prompts are registered for the common workflow tasks:

- Create a variant from base/high/low
- Change coefficients safely
- Attach a new series override
- Build a bundle of policy variants
- Compare latest family runs
- Prepare dashboard visualization set
