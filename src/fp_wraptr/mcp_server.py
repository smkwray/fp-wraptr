"""FastMCP server for fp-wraptr.

Exposes FP model tools via the Model Context Protocol (MCP).
LLM-platform agnostic and runnable through stdio transport by default.

Tools:
    run_fp_scenario        - Run a scenario from a YAML config and return summary.
    run_bundle             - Run a bundle YAML (base + variants) and return summary.
    run_pse2025            - Convenience runner for PSE2025 base/low/high bundle.
    parse_fp_output        - Parse an FP output file into structured JSON.
    diff_runs              - Compare two FP runs and return top deltas.
    list_output_variables  - List variable metadata from a parsed FM output file.
    list_output_equations  - List estimated equations from a parsed FM output file.
    describe_variable      - Describe one model variable from dictionary.json.
    search_dictionary      - Search variables/equations from dictionary.json.
    explain_equation       - Explain one equation with variable descriptions.
    describe_variable_sources - Resolve one variable to source-map + raw-data metadata.
    source_map_coverage    - Coverage summary of source_map against dictionary variables.
    source_map_quality     - Mapping quality audit over source_map entries.
    source_map_report      - Combined deterministic coverage + quality report.
    source_map_window_check - Validate windowed source-map assumptions against FRED data.
    update_model_from_fred - Update model bundle data (fmdata.txt) from FRED mappings.
    run_batch_scenarios    - Run named scenarios from `examples/*.yaml`.
    create_scenario        - Create a scenario YAML file in examples/.
    update_scenario        - Update an existing scenario YAML file.
    list_packs             - List agent-facing local/public pack manifests.
    describe_pack          - Describe one pack manifest and exposed cards/recipes.
    list_workspaces        - List managed authoring workspaces.
    create_workspace_from_catalog - Create a managed workspace from a catalog entry.
    create_workspace_from_bundle  - Create a managed bundle workspace from a bundle path.
    get_workspace          - Load one managed workspace payload.
    update_workspace_metadata - Update workspace metadata fields.
    list_workspace_cards   - List current cards/defaults for a workspace.
    apply_workspace_card   - Mutate one card in a workspace.
    import_workspace_series - Import a quarterly series into a workspace card.
    add_bundle_variant     - Add a variant to a bundle workspace.
    update_bundle_variant  - Update metadata for a bundle variant.
    clone_bundle_variant_recipe - Clone/update/seed a bundle variant in one action.
    remove_bundle_variant  - Remove a variant from a bundle workspace.
    compile_workspace      - Compile a managed workspace to runnable artifacts.
    run_workspace          - Compile and run a managed workspace.
    compare_workspace_runs - Compare two runs associated with a workspace.
    list_visualizations    - List saved/default visualization views.
    build_visualization_view - Build a visualization payload from recent runs.
    validate_scenario      - Validate scenario YAML payload.
    list_scenarios         - List scenario YAML files in a directory.
    get_run_history        - List recent run artifacts.
    get_project_info       - Return project identity, version, and mascot roster.
    get_latest_run         - Return the most recent run's metadata and output path.
    get_parity_report      - Read a parity_report.json from a run directory.
"""

from __future__ import annotations

import datetime as _dt
import importlib.util
import json
import logging
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml

try:
    from fastmcp import FastMCP
except ModuleNotFoundError:  # pragma: no cover - exercised in wheel smoke environments
    FastMCP = None  # type: ignore[assignment]

# Configure logging to stderr only (required for MCP stdio transport).
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("fp-wraptr-mcp")


class _MissingFastMCP:
    """No-op FastMCP shim so module import still works without optional deps."""

    def tool(self) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def _decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            return func

        return _decorator

    def resource(
        self, *_args: Any, **_kwargs: Any
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def _decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            return func

        return _decorator

    def prompt(self, *_args: Any, **_kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def _decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            return func

        return _decorator

    def run(self) -> None:
        raise RuntimeError(
            "fastmcp is required to run fp-mcp. Install with `pip install fastmcp` "
            "or `pip install 'fp-wraptr[mcp]'`."
        )


mcp = FastMCP("fp-wraptr") if FastMCP is not None else _MissingFastMCP()


def _load_parsed_output(path: str) -> dict[str, object]:
    """Load parsed output data and return JSON-safe payload."""
    from fp_wraptr.io.parser import parse_fp_output as _parse

    output = _parse(Path(path))
    return output.to_dict()


def _error_payload(message: str, path: str) -> str:
    """Build a small JSON error payload."""
    return json.dumps({"error": message, "path": path}, indent=2)


def _load_model_dictionary_payload(dictionary_path: str = ""):
    from fp_wraptr.data import ModelDictionary

    path = Path(dictionary_path) if dictionary_path else None
    return ModelDictionary.load(path)


def _load_source_map_payload(source_map_path: str = ""):
    from fp_wraptr.data import load_source_map

    path = Path(source_map_path) if source_map_path else None
    return load_source_map(path)


def _repo_root() -> Path:
    from fp_wraptr.hygiene import find_project_root

    root = find_project_root(Path.cwd())
    return root.resolve() if root is not None else Path.cwd().resolve()


def _run_summary_payload(*, run_id: str, artifacts_dir: Path | None = None) -> dict[str, object]:
    from fp_wraptr.dashboard.artifacts import scan_artifacts

    root = artifacts_dir or (_repo_root() / "artifacts")
    for run in scan_artifacts(root):
        if run.run_dir.name != str(run_id).strip():
            continue
        fmout = run.run_dir / "fmout.txt"
        payload: dict[str, object] = {
            "run_id": run.run_dir.name,
            "scenario_name": run.config.name if run.config else run.scenario_name,
            "run_dir": str(run.run_dir),
            "timestamp": run.timestamp,
            "backend_hint": run.backend_hint,
            "has_output": run.has_output,
            "has_chart": run.has_chart,
            "description": run.config.description if run.config else "",
        }
        if fmout.exists():
            parsed = _load_parsed_output(str(fmout))
            payload["periods"] = parsed.get("periods", [])
            payload["variables"] = sorted(parsed.get("variables", {}).keys())
        return payload
    raise FileNotFoundError(f"Run not found: {run_id}")


@mcp.tool()
def get_project_info() -> str:
    """Return fp-wraptr project identity, version, mascot roster, and capabilities."""
    from fp_wraptr import __version__

    return json.dumps(
        {
            "project": "fp-wraptr",
            "version": __version__,
            "description": (
                "Python utilities to modernize the Fair-Parke (FP) macroeconomic model workflow. "
                "Run scenarios, inspect results, compare forecasts, and explore equations — all from Python."
            ),
            "mascots": [
                {"name": "Rex", "animal": "Velociraptor", "represents": "fp.exe (FORTRAN solver)"},
                {"name": "Raptr", "animal": "Eagle", "represents": "fp-wraptr (Python wrapper)"},
                {"name": "Archie", "animal": "Archaeopteryx", "represents": "fppy (pure-Python solver)"},
            ],
            "backends": ["fpexe", "fppy"],
            "tool_count": 41,
        },
        indent=2,
        sort_keys=True,
    )


@mcp.tool()
def validate_scenario(yaml_content: str) -> str:
    """Validate a scenario YAML configuration string.

    Args:
        yaml_content: Raw YAML content to validate.
    """
    from fp_wraptr.scenarios.config import ScenarioConfig

    try:
        payload = yaml.safe_load(yaml_content)
    except Exception as exc:
        return _error_payload(f"Invalid YAML: {exc}", path="<inline>")

    if not isinstance(payload, dict):
        return _error_payload("Scenario payload must be a YAML mapping.", path="<inline>")

    try:
        config = ScenarioConfig(**payload)
    except Exception as exc:  # pragma: no cover - validation-only defensive path
        return json.dumps({"error": str(exc), "valid": False}, indent=2)

    return json.dumps(
        {
            "valid": True,
            "name": config.name,
            "description": config.description,
            "forecast_start": config.forecast_start,
            "forecast_end": config.forecast_end,
            "override_count": len(config.overrides),
        },
        indent=2,
    )


@mcp.tool()
def list_scenarios(examples_dir: str = "examples") -> str:
    """List scenario YAML files and metadata from a directory.

    Args:
        examples_dir: Directory containing scenario YAML files.
    """
    from fp_wraptr.scenarios.config import ScenarioConfig

    root = Path(examples_dir)
    if not root.exists():
        return _error_payload(f"Examples directory not found: {root}", path=str(root))

    entries = []
    for path in sorted(root.glob("*.yaml")):
        entry = {
            "path": str(path),
        }
        try:
            config = ScenarioConfig.from_yaml(path)
            entry.update({
                "name": config.name,
                "description": config.description,
            })
        except Exception as exc:
            entry["error"] = str(exc)
        entries.append(entry)

    return json.dumps(entries, indent=2, sort_keys=True)


@mcp.tool()
def get_run_history(artifacts_dir: str = "artifacts") -> str:
    """List runs discovered under an artifacts root directory."""
    from fp_wraptr.dashboard.artifacts import scan_artifacts

    runs = scan_artifacts(Path(artifacts_dir))
    payload = {
        "count": len(runs),
        "runs": [
            {
                "scenario_name": run.config.name if run.config is not None else run.scenario_name,
                "timestamp": run.timestamp,
                "run_dir": str(run.run_dir),
                "has_output": run.has_output,
                "has_chart": run.has_chart,
                "description": run.config.description if run.config else "",
            }
            for run in runs
        ],
    }

    return json.dumps(payload, indent=2, sort_keys=True)


@mcp.tool()
def list_packs() -> str:
    """List local/public agent-facing pack manifests."""
    from fp_wraptr.scenarios.authoring import list_packs as _list_packs

    return json.dumps({"packs": _list_packs(repo_root=_repo_root())}, indent=2, sort_keys=True)


@mcp.tool()
def describe_pack(pack_id: str) -> str:
    """Describe one pack manifest, including cards, recipes, and visualization presets."""
    from fp_wraptr.scenarios.packs import describe_pack_manifest

    try:
        payload = describe_pack_manifest(pack_id, repo_root=_repo_root())
    except FileNotFoundError:
        return _error_payload("Pack not found", pack_id)
    return json.dumps(payload, indent=2, sort_keys=True)


@mcp.tool()
def list_workspaces(family: str = "") -> str:
    """List managed authoring workspaces."""
    from fp_wraptr.scenarios.authoring import list_workspaces_payload as _list_workspaces_payload

    payload = _list_workspaces_payload(repo_root=_repo_root(), family=family)
    return json.dumps({"count": len(payload), "workspaces": payload}, indent=2, sort_keys=True)


@mcp.tool()
def create_workspace_from_catalog(
    catalog_entry_id: str,
    workspace_slug: str = "",
    label: str = "",
) -> str:
    """Create a managed scenario/bundle workspace from a catalog entry."""
    from fp_wraptr.scenarios.authoring import create_workspace_from_catalog as _create

    try:
        payload = _create(
            repo_root=_repo_root(),
            catalog_entry_id=catalog_entry_id,
            workspace_slug=workspace_slug,
            label=label,
        )
    except FileNotFoundError:
        return _error_payload("Catalog entry not found", catalog_entry_id)
    return json.dumps(payload, indent=2, sort_keys=True)


@mcp.tool()
def create_workspace_from_bundle(
    bundle_yaml: str,
    workspace_slug: str = "",
    label: str = "",
) -> str:
    """Create a managed bundle workspace from a bundle YAML path."""
    from fp_wraptr.scenarios.authoring import create_workspace_from_bundle as _create

    payload = _create(
        repo_root=_repo_root(),
        bundle_yaml=bundle_yaml,
        workspace_slug=workspace_slug,
        label=label,
    )
    return json.dumps(payload, indent=2, sort_keys=True)


@mcp.tool()
def get_workspace(workspace_id: str) -> str:
    """Load one managed workspace payload."""
    from fp_wraptr.scenarios.authoring import get_workspace as _get_workspace

    try:
        payload = _get_workspace(repo_root=_repo_root(), workspace_id=workspace_id)
    except FileNotFoundError:
        return _error_payload("Workspace not found", workspace_id)
    return json.dumps(payload, indent=2, sort_keys=True)


@mcp.tool()
def update_workspace_metadata(
    workspace_id: str,
    label: str = "",
    description: str = "",
    forecast_start: str = "",
    forecast_end: str = "",
    backend: str = "",
    track_variables: str = "",
) -> str:
    """Update label, forecast window, backend, or tracked variables for a workspace."""
    from fp_wraptr.scenarios.authoring import (
        update_workspace_metadata as _update_workspace_metadata,
    )

    payload = _update_workspace_metadata(
        repo_root=_repo_root(),
        workspace_id=workspace_id,
        label=label,
        description=description,
        forecast_start=forecast_start,
        forecast_end=forecast_end,
        backend=backend,
        track_variables=[item.strip() for item in track_variables.replace(",", " ").split() if item.strip()]
        if track_variables.strip()
        else None,
    )
    return json.dumps(payload, indent=2, sort_keys=True)


@mcp.tool()
def list_workspace_cards(workspace_id: str, variant_id: str = "") -> str:
    """List available cards and current values/defaults for a workspace."""
    from fp_wraptr.scenarios.authoring import list_workspace_cards as _list_workspace_cards

    payload = _list_workspace_cards(repo_root=_repo_root(), workspace_id=workspace_id, variant_id=variant_id)
    return json.dumps(payload, indent=2, sort_keys=True)


@mcp.tool()
def apply_workspace_card(
    workspace_id: str,
    card_id: str,
    constants_json: str = "{}",
    enabled: bool | None = None,
    selected_target: str = "",
    input_mode: str = "",
    variant_id: str = "",
) -> str:
    """Apply constants/target changes to one workspace card."""
    from fp_wraptr.scenarios.authoring import apply_workspace_card as _apply_workspace_card

    try:
        constants_payload = json.loads(constants_json) if constants_json.strip() else {}
    except json.JSONDecodeError as exc:
        return _error_payload(f"Invalid constants_json: {exc}", constants_json)
    if not isinstance(constants_payload, dict):
        return _error_payload("constants_json must decode to an object", constants_json)
    payload = _apply_workspace_card(
        repo_root=_repo_root(),
        workspace_id=workspace_id,
        card_id=card_id,
        constants={str(key): float(value) for key, value in constants_payload.items()},
        enabled=enabled,
        selected_target=selected_target,
        input_mode=input_mode,
        variant_id=variant_id,
    )
    return json.dumps(payload, indent=2, sort_keys=True)


@mcp.tool()
def import_workspace_series(
    workspace_id: str,
    card_id: str,
    series_json: str = "{}",
    pasted_text: str = "",
    csv_path: str = "",
    variant_id: str = "",
    selected_target: str = "",
) -> str:
    """Import a quarterly series into a workspace card."""
    from fp_wraptr.scenarios.authoring import import_workspace_series as _import_workspace_series

    try:
        series_payload = json.loads(series_json) if series_json.strip() else {}
    except json.JSONDecodeError as exc:
        return _error_payload(f"Invalid series_json: {exc}", series_json)
    if not isinstance(series_payload, dict):
        return _error_payload("series_json must decode to an object", series_json)
    payload = _import_workspace_series(
        repo_root=_repo_root(),
        workspace_id=workspace_id,
        card_id=card_id,
        series_points={str(key): float(value) for key, value in series_payload.items()},
        pasted_text=pasted_text,
        csv_path=csv_path,
        variant_id=variant_id,
        selected_target=selected_target,
    )
    return json.dumps(payload, indent=2, sort_keys=True)


@mcp.tool()
def add_bundle_variant(
    workspace_id: str,
    variant_id: str,
    label: str = "",
    scenario_name: str = "",
    input_file: str = "",
    clone_from: str = "",
) -> str:
    """Add a new variant to a bundle workspace."""
    from fp_wraptr.scenarios.authoring import add_bundle_variant as _add_bundle_variant

    payload = _add_bundle_variant(
        repo_root=_repo_root(),
        workspace_id=workspace_id,
        variant_id=variant_id,
        label=label,
        scenario_name=scenario_name,
        input_file=input_file,
        clone_from=clone_from,
    )
    return json.dumps(payload, indent=2, sort_keys=True)


@mcp.tool()
def remove_bundle_variant(workspace_id: str, variant_id: str) -> str:
    """Remove a variant from a bundle workspace."""
    from fp_wraptr.scenarios.authoring import remove_bundle_variant as _remove_bundle_variant

    payload = _remove_bundle_variant(
        repo_root=_repo_root(),
        workspace_id=workspace_id,
        variant_id=variant_id,
    )
    return json.dumps(payload, indent=2, sort_keys=True)


@mcp.tool()
def update_bundle_variant(
    workspace_id: str,
    variant_id: str,
    label: str = "",
    scenario_name: str = "",
    input_file: str = "",
    enabled: bool | None = None,
) -> str:
    """Update metadata for an existing bundle variant."""
    from fp_wraptr.scenarios.authoring import update_bundle_variant as _update_bundle_variant

    payload = _update_bundle_variant(
        repo_root=_repo_root(),
        workspace_id=workspace_id,
        variant_id=variant_id,
        label=label,
        scenario_name=scenario_name,
        input_file=input_file,
        enabled=enabled,
    )
    return json.dumps(payload, indent=2, sort_keys=True)


@mcp.tool()
def clone_bundle_variant_recipe(
    workspace_id: str,
    variant_id: str,
    clone_from: str,
    label: str = "",
    scenario_name: str = "",
    input_file: str = "",
    enabled: bool | None = None,
    card_id: str = "",
    constants_json: str = "{}",
    selected_target: str = "",
    input_mode: str = "",
) -> str:
    """Clone a bundle variant, update its metadata, and optionally seed one card patch."""
    from fp_wraptr.scenarios.authoring import clone_bundle_variant_recipe as _clone_bundle_variant_recipe

    try:
        constants_payload = json.loads(constants_json) if constants_json.strip() else {}
    except json.JSONDecodeError as exc:
        return _error_payload(f"Invalid constants_json: {exc}", constants_json)
    if not isinstance(constants_payload, dict):
        return _error_payload("constants_json must decode to an object", constants_json)
    payload = _clone_bundle_variant_recipe(
        repo_root=_repo_root(),
        workspace_id=workspace_id,
        variant_id=variant_id,
        clone_from=clone_from,
        label=label,
        scenario_name=scenario_name,
        input_file=input_file,
        enabled=enabled,
        card_id=card_id,
        constants={str(key): float(value) for key, value in constants_payload.items()},
        selected_target=selected_target,
        input_mode=input_mode,
    )
    return json.dumps(payload, indent=2, sort_keys=True)


@mcp.tool()
def compile_workspace(workspace_id: str) -> str:
    """Compile a managed workspace into runnable scenario/bundle artifacts."""
    from fp_wraptr.scenarios.authoring import compile_workspace as _compile_workspace

    payload = _compile_workspace(repo_root=_repo_root(), workspace_id=workspace_id)
    return json.dumps(payload, indent=2, sort_keys=True)


@mcp.tool()
def run_workspace(workspace_id: str, output_dir: str = "artifacts/agent_runs") -> str:
    """Compile and run a managed workspace."""
    from fp_wraptr.scenarios.authoring import run_workspace as _run_workspace

    payload = _run_workspace(repo_root=_repo_root(), workspace_id=workspace_id, output_dir=output_dir)
    return json.dumps(payload, indent=2, sort_keys=True)


@mcp.tool()
def compare_workspace_runs(
    workspace_id: str,
    run_a: str = "",
    run_b: str = "",
    top_n: int = 10,
) -> str:
    """Compare two linked runs for a managed workspace."""
    from fp_wraptr.scenarios.authoring import compare_workspace_runs as _compare_workspace_runs

    payload = _compare_workspace_runs(
        repo_root=_repo_root(),
        workspace_id=workspace_id,
        run_a=run_a,
        run_b=run_b,
        top_n=top_n,
    )
    return json.dumps(payload, indent=2, sort_keys=True, default=str)


@mcp.tool()
def list_visualizations(workspace_id: str = "", pack_id: str = "") -> str:
    """List saved/default visualization views for a workspace or pack."""
    from fp_wraptr.scenarios.authoring import list_visualizations as _list_visualizations

    payload = _list_visualizations(repo_root=_repo_root(), workspace_id=workspace_id, pack_id=pack_id)
    return json.dumps({"visualizations": payload}, indent=2, sort_keys=True)


@mcp.tool()
def build_visualization_view(
    view_id: str,
    workspace_id: str = "",
    pack_id: str = "",
    run_dirs_json: str = "[]",
) -> str:
    """Build a visualization payload from recent or explicit run directories."""
    from fp_wraptr.scenarios.authoring import build_visualization_view as _build_visualization_view

    try:
        run_dirs_payload = json.loads(run_dirs_json) if run_dirs_json.strip() else []
    except json.JSONDecodeError as exc:
        return _error_payload(f"Invalid run_dirs_json: {exc}", run_dirs_json)
    if not isinstance(run_dirs_payload, list):
        return _error_payload("run_dirs_json must decode to an array", run_dirs_json)
    payload = _build_visualization_view(
        repo_root=_repo_root(),
        view_id=view_id,
        workspace_id=workspace_id,
        pack_id=pack_id,
        run_dirs=[str(item) for item in run_dirs_payload],
    )
    return json.dumps(payload, indent=2, sort_keys=True, default=str)


@mcp.tool()
def run_fp_scenario(
    scenario_yaml: str,
    output_dir: str = "artifacts",
    backend: str = "",
) -> str:
    """Run an FP scenario from a YAML config file.

    Args:
        scenario_yaml: Path to the scenario YAML configuration file.
        output_dir: Directory for output artifacts.
        backend: Solver backend — "fpexe", "fppy", or "both" (parity mode). Empty uses scenario default.
    """
    from fp_wraptr.scenarios.config import ScenarioConfig
    from fp_wraptr.scenarios.runner import run_scenario

    logger.info("Running scenario from %s (backend=%s)", scenario_yaml, backend or "default")

    config = ScenarioConfig.from_yaml(Path(scenario_yaml))
    if backend.strip():
        config = config.model_copy(update={"backend": backend.strip().lower()})
    result = run_scenario(config, output_dir=Path(output_dir))

    out_dir = result.output_dir
    fmout_path = out_dir / "fmout.txt" if out_dir else None

    summary = {
        "name": config.name,
        "success": result.success,
        "backend": backend.strip().lower() or str(getattr(config, "backend", "fpexe") or "fpexe"),
        "output_dir": str(out_dir),
        "fmout_path": str(fmout_path) if fmout_path and fmout_path.exists() else None,
        "chart_path": str(result.chart_path) if result.chart_path else None,
        "forecast": {},
    }

    if result.backend_diagnostics:
        summary["backend_diagnostics"] = result.backend_diagnostics

    if result.parsed_output:
        summary["forecast_start"] = result.parsed_output.forecast_start
        summary["forecast_end"] = result.parsed_output.forecast_end
        summary["periods"] = result.parsed_output.periods

        for var_name in config.track_variables:
            if var_name in result.parsed_output.variables:
                var = result.parsed_output.variables[var_name]
                summary["forecast"][var_name] = {
                    "levels": var.levels,
                    "changes": var.changes,
                    "pct_changes": var.pct_changes,
                }

    return json.dumps(summary, indent=2, default=str)


@mcp.tool()
def run_bundle(
    bundle_yaml: str,
    output_dir: str = "artifacts/bundles",
) -> str:
    """Run a bundle YAML (base + variants) and return a JSON summary.

    Args:
        bundle_yaml: Path to a bundle YAML file.
        output_dir: Output directory root for bundle artifacts.
    """
    from fp_wraptr.scenarios.bundle import BundleConfig
    from fp_wraptr.scenarios.bundle import run_bundle as _run_bundle

    bundle_path = Path(bundle_yaml)
    if not bundle_path.exists():
        return _error_payload(f"Bundle YAML not found: {bundle_path}", path=str(bundle_path))

    try:
        bundle_config = BundleConfig.from_yaml(bundle_path)
    except Exception as exc:
        return _error_payload(f"Failed to load bundle YAML: {exc}", path=str(bundle_path))

    bundle_name = str(bundle_config.base.get("name", "bundle"))
    timestamp = _dt.datetime.now(_dt.UTC).strftime("%Y%m%d_%H%M%S")
    run_root = Path(output_dir) / f"{bundle_name}_{timestamp}"
    run_root.mkdir(parents=True, exist_ok=True)

    result = _run_bundle(bundle_config, output_dir=run_root)
    report_path = run_root / "bundle_report.json"
    report_path.write_text(
        json.dumps(result.to_dict(), indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )

    payload = result.to_dict()
    payload["run_root"] = str(run_root)
    payload["report_path"] = str(report_path)
    return json.dumps(payload, indent=2, sort_keys=True)


@mcp.tool()
def run_pse2025(
    output_dir: str = "artifacts/pse2025",
    fp_home: str = "FM",
    overlay_dir: str = "projects_local/pse2025",
) -> str:
    """Convenience tool: run the PSE2025 base/low/high bundle without a YAML path."""
    from fp_wraptr.scenarios.bundle import BundleConfig
    from fp_wraptr.scenarios.bundle import run_bundle as _run_bundle

    cwd = Path.cwd()
    fp_home_path = Path(fp_home).expanduser()
    overlay_path = Path(overlay_dir).expanduser()
    if not fp_home_path.is_absolute():
        fp_home_path = (cwd / fp_home_path).resolve()
    else:
        fp_home_path = fp_home_path.resolve()
    if not overlay_path.is_absolute():
        overlay_path = (cwd / overlay_path).resolve()
    else:
        overlay_path = overlay_path.resolve()

    bundle_config = BundleConfig(
        base={
            "name": "pse2025",
            "description": "PSE2025 (Scott 2017 JG layer) — run base/low/high",
            "fp_home": fp_home_path,
            "input_overlay_dir": overlay_path,
            "backend": "fpexe",
            "forecast_start": "2025.4",
            "forecast_end": "2029.4",
            "input_file": "psebase.txt",
            "track_variables": [
                "GDPR",
                "GDP",
                "PCPF",
                "PIEF",
                "SG",
                "RS",
                "UR",
                "E",
                "JGJ",
                "JF",
                "WF",
                "PF",
            ],
        },
        variants=[
            {"name": "base", "patch": {"input_file": "psebase.txt"}},
            {"name": "low", "patch": {"input_file": "pselow.txt"}},
            {"name": "high", "patch": {"input_file": "psehigh.txt"}},
        ],
        focus_variables=["GDPR", "UR", "PCPF", "SG", "RS"],
    )

    timestamp = _dt.datetime.now(_dt.UTC).strftime("%Y%m%d_%H%M%S")
    run_root = Path(output_dir) / f"pse2025_{timestamp}"
    run_root.mkdir(parents=True, exist_ok=True)

    result = _run_bundle(bundle_config, output_dir=run_root)
    report_path = run_root / "bundle_report.json"
    report_path.write_text(
        json.dumps(result.to_dict(), indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )

    payload = result.to_dict()
    payload["run_root"] = str(run_root)
    payload["report_path"] = str(report_path)
    payload["fp_home"] = str(fp_home_path)
    payload["input_overlay_dir"] = str(overlay_path)
    return json.dumps(payload, indent=2, sort_keys=True)


@mcp.tool()
def update_model_from_fred(
    model_dir: str = "FM",
    out_dir: str = "artifacts/model_updates/latest",
    end_period: str = "2025.4",
    extend_sample: bool = False,
    allow_carry_forward: bool = False,
    replace_history: bool = False,
    variables: str = "",
    sources: str = "fred",
    source_map_path: str = "",
    cache_dir: str = "",
    patch_fminput_smpl_endpoints: bool = False,
) -> str:
    """Update a model bundle's `fmdata.txt` using external source mappings (FRED/BEA/BLS).

    Args:
        model_dir: Base model directory (contains fmdata.txt).
        out_dir: Output directory where an `FM/` bundle + report will be written.
        end_period: Update end period ("YYYY.Q").
        extend_sample: Extend fmdata sample_end to end_period when needed.
        allow_carry_forward: Carry forward prior values for variables without new observations when extending.
        replace_history: Apply updates within the existing history window too.
        variables: Optional comma/space-separated FP variable names to update.
        sources: Optional comma/space-separated source list (fred, bea, bls). Default: fred
        source_map_path: Optional override source-map YAML file path.
        cache_dir: Optional override FRED cache directory.
    """
    enabled_sources = [
        item.strip().lower()
        for item in str(sources or "").replace(",", " ").split()
        if item.strip()
    ] or ["fred"]
    if "fred" in enabled_sources and importlib.util.find_spec("fredapi") is None:
        return json.dumps(
            {
                "success": False,
                "error": "fredapi is required for FRED-backed updates. Install fp-wraptr[fred].",
            },
            indent=2,
        )

    from fp_wraptr.data.update_fred import DataUpdateError
    from fp_wraptr.data.update_fred import update_model_from_fred as _update

    selected_vars: list[str] | None = None
    raw = str(variables or "").strip()
    if raw:
        selected_vars = [
            item.strip().upper() for item in raw.replace(",", " ").split() if item.strip()
        ]

    try:
        result = _update(
            model_dir=Path(model_dir),
            out_dir=Path(out_dir),
            end_period=end_period,
            source_map_path=Path(source_map_path) if source_map_path.strip() else None,
            cache_dir=Path(cache_dir) if cache_dir.strip() else None,
            variables=selected_vars,
            sources=enabled_sources,
            replace_history=bool(replace_history),
            extend_sample=bool(extend_sample),
            allow_carry_forward=bool(allow_carry_forward),
            patch_fminput_smpl_endpoints=bool(patch_fminput_smpl_endpoints),
        )
    except (DataUpdateError, ValueError, OSError) as exc:
        return json.dumps({"success": False, "error": str(exc)}, indent=2)

    return json.dumps(
        {
            "success": True,
            "out_dir": str(result.out_dir),
            "bundle_dir": str(result.model_bundle_dir),
            "fmdata_path": str(result.fmdata_path),
            "report_path": str(result.report_path),
            "report": result.report,
        },
        indent=2,
        sort_keys=True,
        default=str,
    )


@mcp.tool()
def run_batch_scenarios(
    scenario_names: list[str],
    output_dir: str = "artifacts/batch",
) -> str:
    """Run named scenarios from the local `examples` directory."""
    from pydantic import ValidationError

    from fp_wraptr.scenarios.batch import run_batch
    from fp_wraptr.scenarios.config import ScenarioConfig

    results: list[dict] = []
    configs: list[ScenarioConfig] = []

    for scenario_name in scenario_names:
        path = Path("examples") / f"{scenario_name}.yaml"
        try:
            config = ScenarioConfig.from_yaml(path)
        except FileNotFoundError as exc:
            results.append({
                "name": scenario_name,
                "success": False,
                "output_dir": "",
                "error": str(exc),
            })
            continue
        except ValidationError as exc:
            results.append({
                "name": scenario_name,
                "success": False,
                "output_dir": "",
                "error": str(exc),
            })
            continue
        except Exception as exc:  # pragma: no cover - defensive
            results.append({
                "name": scenario_name,
                "success": False,
                "output_dir": "",
                "error": str(exc),
            })
            continue

        configs.append(config)

    if not configs:
        return json.dumps(
            {
                "results": results,
                "total": len(results),
                "succeeded": 0,
                "failed": len(results),
            },
            indent=2,
            sort_keys=True,
        )

    for batch_result in run_batch(configs=configs, output_dir=Path(output_dir)):
        results.append({
            "name": batch_result.config.name,
            "success": batch_result.success,
            "output_dir": str(batch_result.output_dir),
        })

    succeeded = len([item for item in results if item.get("success")])
    return json.dumps(
        {
            "results": results,
            "total": len(results),
            "succeeded": succeeded,
            "failed": len(results) - succeeded,
        },
        indent=2,
        sort_keys=True,
    )


@mcp.tool()
def create_scenario(
    yaml_content: str,
    filename: str,
    examples_dir: str = "examples",
) -> str:
    """Create a new scenario YAML file under the examples directory."""
    from fp_wraptr.scenarios.config import ScenarioConfig

    try:
        payload = yaml.safe_load(yaml_content)
    except Exception as exc:
        return _error_payload(f"Invalid YAML: {exc}", path="<inline>")

    if payload is None:
        payload = {}
    if not isinstance(payload, dict):
        return _error_payload("Scenario payload must be a YAML mapping.", path="<inline>")

    try:
        config = ScenarioConfig(**payload)
    except Exception as exc:
        return json.dumps({"error": str(exc), "created": False}, indent=2)

    examples_root = Path(examples_dir)
    target = examples_root / filename
    if target.suffix.lower() not in {".yaml", ".yml"}:
        target = target.with_suffix(".yaml")
    target.parent.mkdir(parents=True, exist_ok=True)

    if target.exists():
        return json.dumps(
            {
                "created": False,
                "error": f"Scenario file already exists: {target}",
                "path": str(target),
            },
            indent=2,
            sort_keys=True,
        )

    config.to_yaml(target)
    return json.dumps(
        {
            "created": True,
            "path": str(target),
            "name": config.name,
        },
        indent=2,
        sort_keys=True,
    )


@mcp.tool()
def update_scenario(
    scenario_path: str,
    yaml_content: str,
) -> str:
    """Update an existing scenario YAML file."""
    from fp_wraptr.scenarios.config import ScenarioConfig

    target = Path(scenario_path)
    if not target.exists():
        return _error_payload("Scenario file not found", path=str(target))

    try:
        payload = yaml.safe_load(yaml_content)
    except Exception as exc:
        return _error_payload(f"Invalid YAML: {exc}", path="<inline>")

    if payload is None:
        payload = {}
    if not isinstance(payload, dict):
        return _error_payload("Scenario payload must be a YAML mapping.", path="<inline>")

    try:
        config = ScenarioConfig(**payload)
    except Exception as exc:
        return json.dumps({"error": str(exc), "updated": False}, indent=2)

    config.to_yaml(target)
    return json.dumps(
        {
            "updated": True,
            "path": str(target),
            "name": config.name,
        },
        indent=2,
        sort_keys=True,
    )


@mcp.tool()
def parse_fp_output(path: str = "FM/fmout.txt", format: str = "json") -> str:
    """Parse an FP output file (fmout.txt) into structured data.

    Args:
        path: Path to the FP output file.
        format: Output format -- `json` for full structure, `csv` for tabular.
    """
    from fp_wraptr.io.parser import parse_fp_output as _parse

    logger.info("Parsing FP output from %s", path)
    result = _parse(Path(path))

    if format == "csv":
        return result.to_dataframe().to_csv(index=True)

    return json.dumps(result.to_dict(), indent=2, default=str)


@mcp.tool()
def list_output_variables(path: str = "FM/fmout.txt") -> str:
    """List variable metadata from a parsed output file.

    Args:
        path: Path to FM output text file.
    """
    logger.info("Loading forecast variable metadata from %s", path)
    try:
        payload = _load_parsed_output(path)
    except FileNotFoundError:
        return _error_payload("Output file not found", path)
    except Exception as exc:  # pragma: no cover - defensive for parser/runtime surprises
        logger.exception("Failed to parse output for variable catalog: %s", exc)
        return _error_payload(str(exc), path)

    return json.dumps(
        {
            "path": path,
            "variables": [
                {
                    "name": name,
                    "var_id": metadata["var_id"],
                    "level_count": len(metadata["levels"]),
                    "change_count": len(metadata["changes"]),
                    "pct_change_count": len(metadata["pct_changes"]),
                }
                for name, metadata in payload.get("variables", {}).items()
            ],
        },
        indent=2,
        sort_keys=True,
    )


@mcp.tool()
def list_output_equations(path: str = "FM/fmout.txt") -> str:
    """List estimated equation metadata from a parsed output file.

    Args:
        path: Path to FM output text file.
    """
    logger.info("Loading equation metadata from %s", path)
    try:
        payload = _load_parsed_output(path)
    except FileNotFoundError:
        return _error_payload("Output file not found", path)
    except Exception as exc:  # pragma: no cover - defensive for parser/runtime surprises
        logger.exception("Failed to parse output for equation catalog: %s", exc)
        return _error_payload(str(exc), path)

    return json.dumps(
        {
            "path": path,
            "equations": payload.get("estimations", []),
        },
        indent=2,
        sort_keys=True,
        default=str,
    )


@mcp.tool()
def describe_variable(code: str, dictionary_path: str = "") -> str:
    """Describe one variable from dictionary.json.

    Args:
        code: Variable code (for example `GDP` or `UR`).
        dictionary_path: Optional override path to dictionary JSON.
    """
    try:
        dictionary = _load_model_dictionary_payload(dictionary_path)
    except FileNotFoundError:
        return _error_payload("Dictionary file not found", dictionary_path or "default")
    except RuntimeError as exc:
        return _error_payload(str(exc), dictionary_path or "default")

    payload = dictionary.describe(code.upper())
    if payload is None:
        return json.dumps(
            {"error": f"Variable not found: {code}", "code": code.upper()},
            indent=2,
            sort_keys=True,
        )
    return json.dumps(payload, indent=2, sort_keys=True, default=str)


@mcp.tool()
def search_dictionary(query: str, limit: int = 10, dictionary_path: str = "") -> str:
    """Search equations and variables from dictionary.json.

    Args:
        query: Search text, variable code, or equation id.
        limit: Maximum matches in each section.
        dictionary_path: Optional override path to dictionary JSON.
    """
    try:
        dictionary = _load_model_dictionary_payload(dictionary_path)
    except FileNotFoundError:
        return _error_payload("Dictionary file not found", dictionary_path or "default")
    except RuntimeError as exc:
        return _error_payload(str(exc), dictionary_path or "default")

    payload = dictionary.query(query, limit=max(1, limit))
    return json.dumps(payload, indent=2, sort_keys=True, default=str)


@mcp.tool()
def explain_equation(eq_id: int, dictionary_path: str = "") -> str:
    """Explain one equation and list variable-level descriptions.

    Args:
        eq_id: Equation id.
        dictionary_path: Optional override path to dictionary JSON.
    """
    try:
        dictionary = _load_model_dictionary_payload(dictionary_path)
    except FileNotFoundError:
        return _error_payload("Dictionary file not found", dictionary_path or "default")
    except RuntimeError as exc:
        return _error_payload(str(exc), dictionary_path or "default")

    payload = dictionary.explain_equation(eq_id)
    if payload is None:
        return json.dumps(
            {"error": f"Equation not found: {eq_id}", "eq_id": eq_id},
            indent=2,
            sort_keys=True,
        )
    return json.dumps(payload, indent=2, sort_keys=True, default=str)


@mcp.tool()
def describe_variable_sources(
    variable: str,
    dictionary_path: str = "",
    source_map_path: str = "",
) -> str:
    """Resolve one variable to source-map and dictionary raw-data metadata."""
    try:
        dictionary = _load_model_dictionary_payload(dictionary_path)
    except FileNotFoundError:
        return _error_payload("Dictionary file not found", dictionary_path or "default")
    except RuntimeError as exc:
        return _error_payload(str(exc), dictionary_path or "default")

    try:
        source_map = _load_source_map_payload(source_map_path)
    except FileNotFoundError:
        return _error_payload("Source map file not found", source_map_path or "default")
    except RuntimeError as exc:
        return _error_payload(str(exc), source_map_path or "default")

    payload = source_map.resolve_variable_sources(variable, dictionary=dictionary)
    return json.dumps(payload, indent=2, sort_keys=True, default=str)


@mcp.tool()
def source_map_coverage(
    dictionary_path: str = "",
    source_map_path: str = "",
    only_with_raw_data: bool = False,
) -> str:
    """Summarize source-map coverage for dictionary variables."""
    try:
        dictionary = _load_model_dictionary_payload(dictionary_path)
    except FileNotFoundError:
        return _error_payload("Dictionary file not found", dictionary_path or "default")
    except RuntimeError as exc:
        return _error_payload(str(exc), dictionary_path or "default")

    try:
        source_map = _load_source_map_payload(source_map_path)
    except FileNotFoundError:
        return _error_payload("Source map file not found", source_map_path or "default")
    except RuntimeError as exc:
        return _error_payload(str(exc), source_map_path or "default")

    if only_with_raw_data:
        variable_names = [
            record.name for record in dictionary.variables.values() if record.raw_data_sources
        ]
        scope = "variables_with_raw_data"
    else:
        variable_names = list(dictionary.variables.keys())
        scope = "all_dictionary_variables"

    payload = source_map.coverage_report(variable_names)
    payload["scope"] = scope
    return json.dumps(payload, indent=2, sort_keys=True, default=str)


@mcp.tool()
def source_map_quality(
    dictionary_path: str = "",
    source_map_path: str = "",
    only_with_raw_data: bool = False,
) -> str:
    """Audit source-map quality over dictionary variable scopes."""
    try:
        dictionary = _load_model_dictionary_payload(dictionary_path)
    except FileNotFoundError:
        return _error_payload("Dictionary file not found", dictionary_path or "default")
    except RuntimeError as exc:
        return _error_payload(str(exc), dictionary_path or "default")

    try:
        source_map = _load_source_map_payload(source_map_path)
    except FileNotFoundError:
        return _error_payload("Source map file not found", source_map_path or "default")
    except RuntimeError as exc:
        return _error_payload(str(exc), source_map_path or "default")

    if only_with_raw_data:
        variable_names = [
            record.name for record in dictionary.variables.values() if record.raw_data_sources
        ]
        scope = "variables_with_raw_data"
    else:
        variable_names = list(dictionary.variables.keys())
        scope = "all_dictionary_variables"

    payload = source_map.quality_report(variable_names)
    payload["scope"] = scope
    return json.dumps(payload, indent=2, sort_keys=True, default=str)


@mcp.tool()
def source_map_report(
    dictionary_path: str = "",
    source_map_path: str = "",
) -> str:
    """Build a combined deterministic source-map coverage/quality report."""
    try:
        dictionary = _load_model_dictionary_payload(dictionary_path)
    except FileNotFoundError:
        return _error_payload("Dictionary file not found", dictionary_path or "default")
    except RuntimeError as exc:
        return _error_payload(str(exc), dictionary_path or "default")

    try:
        source_map = _load_source_map_payload(source_map_path)
    except FileNotFoundError:
        return _error_payload("Source map file not found", source_map_path or "default")
    except RuntimeError as exc:
        return _error_payload(str(exc), source_map_path or "default")

    all_variables = list(dictionary.variables.keys())
    raw_variables = [
        record.name for record in dictionary.variables.values() if record.raw_data_sources
    ]

    payload = {
        "model_version": dictionary.model_version,
        "dictionary_variable_count": len(all_variables),
        "source_map_variable_count": len(source_map.list_variables()),
        "coverage_all": source_map.coverage_report(all_variables),
        "coverage_with_raw_data": source_map.coverage_report(raw_variables),
        "quality_all": source_map.quality_report(all_variables),
        "quality_with_raw_data": source_map.quality_report(raw_variables),
    }
    return json.dumps(payload, indent=2, sort_keys=True, default=str)


@mcp.tool()
def source_map_window_check(
    source_map_path: str = "",
    start: str = "",
    end: str = "",
    tolerance: float = 0.0,
    cache_dir: str = "",
) -> str:
    """Check windowed source-map assumptions against observed FRED data."""
    try:
        source_map = _load_source_map_payload(source_map_path)
    except FileNotFoundError:
        return _error_payload("Source map file not found", source_map_path or "default")
    except RuntimeError as exc:
        return _error_payload(str(exc), source_map_path or "default")

    entries = source_map.windowed_fred_entries()
    series_ids = sorted({entry.series_id for _, entry in entries if entry.series_id})
    if not series_ids:
        payload = {
            "series_checked": 0,
            "violation_count": 0,
            "status_breakdown": {},
            "tolerance": max(tolerance, 0.0),
            "checks": [],
            "requested_start": start,
            "requested_end": end,
        }
        return json.dumps(payload, indent=2, sort_keys=True, default=str)

    from fp_wraptr.fred.ingest import fetch_series

    try:
        frame = fetch_series(
            series_ids,
            start=start or None,
            end=end or None,
            cache_dir=Path(cache_dir) if cache_dir else None,
        )
    except ModuleNotFoundError:
        return _error_payload("fredapi is required", "fp_wraptr.fred.ingest")
    except ValueError as exc:
        return _error_payload(str(exc), "FRED_API_KEY")

    payload = source_map.window_assumption_report(frame, tolerance=tolerance)
    payload["requested_start"] = start
    payload["requested_end"] = end
    return json.dumps(payload, indent=2, sort_keys=True, default=str)


if hasattr(mcp, "resource"):

    @mcp.resource("fp://output/variables")
    def output_variables_resource() -> str:
        """Read-only resource: variable catalog from default output path."""
        return list_output_variables("FM/fmout.txt")

    @mcp.resource("fp://output/equations")
    def output_equations_resource() -> str:
        """Read-only resource: equation catalog from default output path."""
        return list_output_equations("FM/fmout.txt")

    @mcp.resource("fp://packs")
    def packs_resource() -> str:
        """Read-only resource: discovered pack manifests."""
        return list_packs()

    @mcp.resource("fp://pack/{pack_id}/cards")
    def pack_cards_resource(pack_id: str) -> str:
        """Read-only resource: one pack's exposed cards."""
        from fp_wraptr.scenarios.packs import describe_pack_manifest

        try:
            payload = describe_pack_manifest(pack_id, repo_root=_repo_root())
        except FileNotFoundError:
            return _error_payload("Pack not found", pack_id)
        return json.dumps(
            {
                "pack_id": pack_id,
                "cards": payload.get("cards", []),
            },
            indent=2,
            sort_keys=True,
        )

    @mcp.resource("fp://pack/{pack_id}/recipes")
    def pack_recipes_resource(pack_id: str) -> str:
        """Read-only resource: one pack's named recipes."""
        from fp_wraptr.scenarios.packs import describe_pack_manifest

        try:
            payload = describe_pack_manifest(pack_id, repo_root=_repo_root())
        except FileNotFoundError:
            return _error_payload("Pack not found", pack_id)
        return json.dumps(
            {
                "pack_id": pack_id,
                "recipes": payload.get("recipes", []),
            },
            indent=2,
            sort_keys=True,
        )

    @mcp.resource("fp://workspace/{workspace_id}")
    def workspace_resource(workspace_id: str) -> str:
        """Read-only resource: one managed workspace."""
        return get_workspace(workspace_id)

    @mcp.resource("fp://workspace/{workspace_id}/compile-report")
    def workspace_compile_report_resource(workspace_id: str) -> str:
        """Read-only resource: the latest compile report for one workspace."""
        payload = json.loads(get_workspace(workspace_id))
        report_path = str(payload.get("extra", {}).get("compile_report_path", "")).strip()
        if not report_path:
            return _error_payload("Workspace has no compile report yet", workspace_id)
        target = Path(report_path)
        if not target.exists():
            return _error_payload("Compile report not found", report_path)
        return target.read_text(encoding="utf-8")

    @mcp.resource("fp://runs/latest")
    def latest_runs_resource() -> str:
        """Read-only resource: latest run metadata."""
        return get_latest_run(limit=5)

    @mcp.resource("fp://runs/{run_id}/summary")
    def run_summary_resource(run_id: str) -> str:
        """Read-only resource: one run's summary."""
        try:
            payload = _run_summary_payload(run_id=run_id)
        except FileNotFoundError:
            return _error_payload("Run not found", run_id)
        return json.dumps(payload, indent=2, sort_keys=True)


if hasattr(mcp, "prompt"):

    @mcp.prompt(name="Create a variant from base/high/low")
    def prompt_create_variant() -> str:
        return (
            "Use `list_packs`, `describe_pack`, `list_workspaces`, `create_workspace_from_catalog`, "
            "`clone_bundle_variant_recipe` for the common clone+metadata+starter-patch flow, or "
            "`add_bundle_variant`, `update_bundle_variant`, `apply_workspace_card`, `compile_workspace`, and "
            "`run_workspace` for lower-level control. Prefer a workspace-first flow; do not edit raw YAML "
            "unless the workspace tools cannot express the change."
        )

    @mcp.prompt(name="Change coefficients safely")
    def prompt_change_coefficients() -> str:
        return (
            "Use `list_workspace_cards` to discover deck-constant cards, then `apply_workspace_card` with "
            "a JSON object of constant updates. After mutation, call `compile_workspace` and inspect "
            "`fp://workspace/{workspace_id}/compile-report` before running."
        )

    @mcp.prompt(name="Attach a new series override")
    def prompt_attach_series() -> str:
        return (
            "Use `list_workspace_cards` to find a series card, then `import_workspace_series` with "
            "quarterly `series_json` or `pasted_text`. Prefer the card's default target unless the user "
            "asks for a specific include or fmexog layering strategy."
        )

    @mcp.prompt(name="Build a bundle of policy variants")
    def prompt_build_bundle() -> str:
        return (
            "Create or load a bundle workspace, use `add_bundle_variant` for new variants, apply shared "
            "or variant-specific card changes, then `compile_workspace` and `run_workspace`. Treat the "
            "workspace as the source of truth instead of hand-editing bundle YAML."
        )

    @mcp.prompt(name="Compare latest family runs")
    def prompt_compare_runs() -> str:
        return (
            "Use `get_workspace` to inspect linked runs, `compare_workspace_runs` to diff the latest two "
            "scenario or bundle runs, and `build_visualization_view` for a chart-ready payload over tracked variables."
        )

    @mcp.prompt(name="Prepare dashboard visualization set")
    def prompt_prepare_visualizations() -> str:
        return (
            "Use `list_visualizations` and `build_visualization_view` to prepare forecast overlays, delta "
            "tables, or tracked-variable views that can be handed off to the dashboard pages."
        )


@mcp.tool()
def get_latest_run(
    artifacts_dir: str = "artifacts",
    scenario_filter: str = "",
    limit: int = 1,
) -> str:
    """Return the most recent run(s) with metadata and output paths.

    Args:
        artifacts_dir: Root artifacts directory.
        scenario_filter: Optional scenario name substring to filter by.
        limit: Number of recent runs to return (default 1).
    """
    from fp_wraptr.dashboard.artifacts import latest_runs, scan_artifacts

    runs = scan_artifacts(Path(artifacts_dir))
    if scenario_filter:
        filt = scenario_filter.strip().lower()
        runs = [r for r in runs if filt in r.scenario_name.lower()]

    top = latest_runs(runs, limit=max(1, limit), has_output=False)
    entries = []
    for run in top:
        fmout = run.run_dir / "fmout.txt"
        parity = run.run_dir / "parity_report.json"
        entries.append({
            "scenario_name": run.config.name if run.config else run.scenario_name,
            "timestamp": run.timestamp,
            "run_dir": str(run.run_dir),
            "has_output": run.has_output,
            "fmout_path": str(fmout) if fmout.exists() else None,
            "has_parity_report": parity.exists(),
            "backend_hint": run.backend_hint,
            "description": run.config.description if run.config else "",
        })

    return json.dumps({"count": len(entries), "runs": entries}, indent=2, sort_keys=True)


@mcp.tool()
def get_parity_report(run_dir: str = "", artifacts_dir: str = "artifacts") -> str:
    """Read a parity_report.json from a run directory (or the latest run with one).

    Args:
        run_dir: Explicit run directory containing parity_report.json. If empty, searches for the latest.
        artifacts_dir: Root artifacts directory (used when run_dir is empty).
    """
    if run_dir.strip():
        report_path = Path(run_dir.strip()) / "parity_report.json"
        if not report_path.exists():
            return _error_payload("parity_report.json not found in run directory", str(report_path))
        return report_path.read_text(encoding="utf-8")

    from fp_wraptr.dashboard.artifacts import scan_artifacts

    runs = scan_artifacts(Path(artifacts_dir))
    for run in runs:
        candidate = run.run_dir / "parity_report.json"
        if candidate.exists():
            return candidate.read_text(encoding="utf-8")

    return _error_payload("No parity_report.json found in any run", artifacts_dir)


@mcp.tool()
def diff_runs(run_a: str, run_b: str, top_n: int = 10) -> str:
    """Compare two FP run directories and return top deltas.

    Args:
        run_a: Path to first run directory (baseline).
        run_b: Path to second run directory (scenario).
        top_n: Number of top deltas to include.
    """
    from fp_wraptr.analysis.diff import diff_run_dirs

    logger.info("Diffing runs: %s vs %s", run_a, run_b)
    summary = diff_run_dirs(Path(run_a), Path(run_b), top_n=top_n)
    return json.dumps(summary, indent=2, default=str)


def main() -> None:
    """Run the MCP server with stdio transport."""
    logger.info("Starting fp-wraptr MCP server (stdio transport)")
    try:
        mcp.run()
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    main()
