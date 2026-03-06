# fp-wraptr

**Python utilities to modernize the Fair-Parke (FP) macroeconomic model workflow.**

fp-wraptr wraps Ray Fair's US Macroeconometric Model, making it easier to run scenarios, inspect results, compare forecasts, and build on top of decades of economic modeling work -- all from Python.

The recommended workflow is now agent-first for authoring and dashboard-first for inspection. Use MCP-managed workspaces and local pack manifests to make changes safely, then use the dashboard to explore and compare the resulting runs.

## Features

- **Scenario configs**: Define runs in YAML instead of editing raw `fminput.txt`
- **IO parsing**: Read FP outputs into pandas DataFrames
- **Parser contract**: Input command parsing uses canonical snake_case keys (`commands_by_type`, `setupect`, etc.)
- **Batch runner**: Execute multiple scenarios and compare outputs
- **Dependency graph**: Trace upstream and downstream variable dependencies
- **Report generation**: Build markdown run reports and summaries
- **Visualization**: Quick matplotlib charts and dashboard Plotly views
- **MCP server**: LLM-assisted exploration plus workspace-first authoring via Model Context Protocol
- **Local packs**: Agent-readable pack manifests, recipes, and visualization presets for scenario families

## Documentation

- [Architecture overview](architecture.md)
- [Agent Workflows](agent-workflows.md)
- [Pack Authoring](packs.md)
- [Scenario configuration reference](scenarios.md)
- [Scenario DSL](dsl.md)
- [Scenario YAML schema](scenario-schema.md)
- [Model runs export](model-runs.md)
- [CLI reference](cli.md)
- [MCP server reference](mcp.md)
- [MCP tool and resource reference](mcp-tools.md)
- [Dashboard guide](dashboard.md)
- [Quickstart](quickstart.md)

## Getting started

See the [Quickstart guide](quickstart.md) to set up your environment and run your first scenario.
