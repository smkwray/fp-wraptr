# Agent Workflows

`fp-wraptr` is designed so that agents can author scenarios through the MCP server.

## Recommended flow

1. Discover a pack with `list_packs` or `fp://packs`.
2. Create or open a managed workspace from a catalog entry or bundle.
3. Mutate the workspace through cards and recipes:
   - coefficient edits
   - quarterly series imports
   - bundle variant add/remove
4. Compile the workspace and inspect the compile report.
5. Run the compiled scenario or bundle.
6. Compare runs and build chart-ready visualization payloads.

## Why workspaces

Workspaces keep agents out of raw FP files for common tasks. They provide:

- stable workspace IDs
- operation history
- linked run history
- compile outputs and reports
- a reversible, structured mutation surface

Low-level file tools are still available for advanced use.

## Common MCP tasks

- Create a variant from base/high/low
- Change coefficients safely
- Attach a new series override
- Build a bundle of policy variants
- Compare latest family runs
- Prepare visualization set

These are available as FastMCP prompts that describe which tools to chain, and in what order.
