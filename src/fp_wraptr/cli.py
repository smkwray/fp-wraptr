"""CLI entry point for fp-wraptr.

Usage:
    fp run scenario.yaml [--baseline baseline.yaml]
    fp diff run_a/ run_b/
    fp io parse-output FM/fmout.txt
    fp io parse-input FM/fminput.txt
"""

from __future__ import annotations

import csv
import datetime as _dt
import importlib.util
import io
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Annotated, Any

import typer
import yaml
from pydantic import ValidationError
from rich.console import Console
from rich.table import Table

from fp_wraptr.hygiene import assert_no_forbidden_dirs, find_project_root

app = typer.Typer(
    name="fp",
    help="fp-wraptr: Python utilities for the Fair-Parke macroeconomic model.",
    no_args_is_help=True,
)


def _parity_cli_exit_code(parity_exit_code: int, *, lenient: bool) -> int:
    """Map parity engine/gate exit codes to CLI process exit behavior."""
    code = int(parity_exit_code)
    if not lenient:
        return code
    # Lenient mode: engine/fingerprint failures stay fatal,
    # but gate/hard-fail mismatches are surfaced in the report and return 0.
    if code in {4, 5}:
        return code
    return 0


def _warn_if_parity_suppressed(
    *, parity_exit_code: int, cli_exit_code: int, lenient: bool
) -> None:
    """Warn when lenient CLI mode suppresses parity mismatches."""
    if not lenient:
        return
    if int(cli_exit_code) != 0:
        return
    raw = int(parity_exit_code)
    if raw in {0, 4, 5}:
        return
    console.print(
        "[yellow]WARNING:[/yellow] parity reported a mismatch "
        f"(exit_code={raw}) but --lenient is active, so the process is exiting 0. "
        "Remove --lenient to fail on mismatches."
    )


def _load_parity_seed_diagnostics(run_dir: str | Path) -> dict[str, Any]:
    path = Path(run_dir) / "work_fppy" / "fppy_report.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        return {}
    fields = (
        "solve_outside_seeded_cells",
        "solve_outside_seed_inspected_cells",
        "solve_outside_seed_candidate_cells",
        "eq_backfill_outside_post_seed_cells",
        "eq_backfill_outside_post_seed_inspected_cells",
        "eq_backfill_outside_post_seed_candidate_cells",
    )
    diagnostics = {name: summary.get(name) for name in fields if name in summary}
    return diagnostics


def _format_parity_seed_diagnostics(run_dir: str | Path) -> str:
    diagnostics = _load_parity_seed_diagnostics(run_dir)
    if not diagnostics:
        return ""
    solve_triplet = (
        f"{diagnostics.get('solve_outside_seeded_cells')}/"
        f"{diagnostics.get('solve_outside_seed_inspected_cells')}/"
        f"{diagnostics.get('solve_outside_seed_candidate_cells')}"
    )
    post_triplet = (
        f"{diagnostics.get('eq_backfill_outside_post_seed_cells')}/"
        f"{diagnostics.get('eq_backfill_outside_post_seed_inspected_cells')}/"
        f"{diagnostics.get('eq_backfill_outside_post_seed_candidate_cells')}"
    )
    return (
        " "
        f"solve_seeded/inspected/candidate={solve_triplet} "
        f"post_seeded/inspected/candidate={post_triplet}"
    )


def _format_parity_runtime_profile(run_dir: str | Path) -> str:
    run_path = Path(run_dir)
    parity_report_path = run_path / "parity_report.json"
    fppy_report_path = run_path / "work_fppy" / "fppy_report.json"
    if not parity_report_path.exists() or not fppy_report_path.exists():
        return ""
    try:
        parity_payload = json.loads(parity_report_path.read_text(encoding="utf-8"))
        fppy_payload = json.loads(fppy_report_path.read_text(encoding="utf-8"))
    except Exception:
        return ""

    preset = ""
    engine_runs = parity_payload.get("engine_runs")
    if isinstance(engine_runs, dict):
        fppy_engine = engine_runs.get("fppy")
        if isinstance(fppy_engine, dict):
            details = fppy_engine.get("details")
            if isinstance(details, dict):
                raw = details.get("eq_flags_preset")
                if isinstance(raw, str):
                    preset = raw.strip()

    summary = fppy_payload.get("summary")
    if not isinstance(summary, dict):
        return ""

    min_iters = summary.get("eq_backfill_min_iters")
    max_iters = summary.get("eq_backfill_max_iters")
    iterations = summary.get("eq_backfill_iterations")
    if min_iters is None and max_iters is None and iterations is None and not preset:
        return ""
    return (
        " "
        f"preset={preset or 'unknown'} "
        f"eq_iters={iterations if iterations is not None else '?'} "
        f"eq_minmax={min_iters if min_iters is not None else '?'}/"
        f"{max_iters if max_iters is not None else '?'}"
    )


def _format_parity_hard_fail_reasons(pabev_detail: dict[str, Any] | None) -> str:
    if not pabev_detail:
        return ""
    hard_fail_cells = pabev_detail.get("hard_fail_cells")
    if not isinstance(hard_fail_cells, list) or not hard_fail_cells:
        return ""
    reasons: list[str] = []
    for cell in hard_fail_cells:
        if not isinstance(cell, dict):
            continue
        reason = cell.get("reason")
        if isinstance(reason, str) and reason:
            reasons.append(reason)
    if not reasons:
        return ""
    from collections import Counter

    counts = Counter(reasons)
    top_reason, top_count = counts.most_common(1)[0]
    return f" hard_fail_top_reason={top_reason} hard_fail_top_reason_count={top_count}"


def _print_run_dir(run_dir: str | Path | None) -> None:
    if run_dir is None:
        return
    text = str(run_dir).strip()
    if not text:
        return
    console.print(f"run dir \u2192 {text}")


def _extract_fpexe_solution_errors(parity_result: Any) -> list[dict[str, Any]]:
    engine_runs = getattr(parity_result, "engine_runs", {}) or {}
    if not isinstance(engine_runs, dict):
        return []
    fpexe = engine_runs.get("fpexe")
    if fpexe is None:
        return []
    details = getattr(fpexe, "details", None)
    if details is None and isinstance(fpexe, dict):
        details = fpexe.get("details")
    if not isinstance(details, dict):
        return []
    rows = details.get("solution_errors")
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _print_fpexe_solution_error_warning(parity_result: Any) -> None:
    rows = _extract_fpexe_solution_errors(parity_result)
    if not rows:
        return
    console.print(
        "[yellow]Warning:[/yellow] "
        f"fp.exe reported solution errors ({len(rows)}); treat diffs as unreliable."
    )


def _extract_fpexe_preflight_report(parity_result: Any) -> dict[str, Any]:
    engine_runs = getattr(parity_result, "engine_runs", {}) or {}
    if not isinstance(engine_runs, dict):
        return {}
    fpexe = engine_runs.get("fpexe")
    if fpexe is None:
        return {}
    details = getattr(fpexe, "details", None)
    if details is None and isinstance(fpexe, dict):
        details = fpexe.get("details")
    if not isinstance(details, dict):
        return {}
    preflight = details.get("preflight_report")
    if not isinstance(preflight, dict):
        return {}
    return preflight


def _print_fpexe_preflight_warning(parity_result: Any) -> None:
    preflight = _extract_fpexe_preflight_report(parity_result)
    if not preflight:
        return
    if bool(preflight.get("wine_required")) and not bool(preflight.get("wine_available")):
        console.print(
            "[yellow]Warning:[/yellow] Wine not found; install with "
            "`brew install --cask wine-stable` and rerun parity."
        )


io_app = typer.Typer(name="io", help="Parse and write FP file formats.")
viz_app = typer.Typer(name="viz", help="Generate charts and plots.")
fred_app = typer.Typer(name="fred", help="Fetch and manage FRED time-series overlays.")
bea_app = typer.Typer(name="bea", help="Fetch BEA/NIPA time-series observations.")
bls_app = typer.Typer(name="bls", help="Fetch BLS time-series observations.")
data_app = typer.Typer(name="data", help="Build updated model bundles from external sources.")
dsl_app = typer.Typer(name="dsl", help="Compile human-readable scenario DSL into YAML/JSON.")
bundle_app = typer.Typer(name="bundle", help="Run scenario bundles with variants/grids.")
dict_app = typer.Typer(name="dictionary", help="Search variable/equation dictionary data.")
triage_app = typer.Typer(name="triage", help="Generate triage artifacts from parity outputs.")
packs_app = typer.Typer(name="packs", help="Discover local/public pack manifests.")
workspace_app = typer.Typer(
    name="workspace",
    help="Managed workspaces for agent-first scenario and bundle authoring.",
)
export_app = typer.Typer(name="export", help="Export portable public bundles and reports.")
gender_app = typer.Typer(
    name="gender", help="Bootstrap and manage the local gender scenario family."
)
app.add_typer(io_app, name="io")
app.add_typer(viz_app, name="viz")
app.add_typer(fred_app, name="fred")
app.add_typer(bea_app, name="bea")
app.add_typer(bls_app, name="bls")
app.add_typer(data_app, name="data")
app.add_typer(dsl_app, name="dsl")
app.add_typer(bundle_app, name="bundle")
app.add_typer(dict_app, name="dictionary")
app.add_typer(triage_app, name="triage")
app.add_typer(packs_app, name="packs")
app.add_typer(workspace_app, name="workspace")
app.add_typer(export_app, name="export")
app.add_typer(gender_app, name="gender")

console = Console()


def _emit_json(payload: object) -> None:
    typer.echo(json.dumps(payload, indent=2, sort_keys=True, default=str))


def _cli_repo_root() -> Path:
    root = find_project_root(Path.cwd())
    return root.resolve() if root is not None else Path.cwd().resolve()


def _parse_json_object_option(raw: str, *, option_name: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError as exc:
        console.print(f"[red]{option_name} must be valid JSON:[/red] {exc}")
        raise typer.Exit(code=1) from None
    if not isinstance(payload, dict):
        console.print(f"[red]{option_name} must decode to a JSON object.[/red]")
        raise typer.Exit(code=1)
    return payload


def _parse_json_array_option(raw: str, *, option_name: str) -> list[Any]:
    try:
        payload = json.loads(raw) if raw.strip() else []
    except json.JSONDecodeError as exc:
        console.print(f"[red]{option_name} must be valid JSON:[/red] {exc}")
        raise typer.Exit(code=1) from None
    if not isinstance(payload, list):
        console.print(f"[red]{option_name} must decode to a JSON array.[/red]")
        raise typer.Exit(code=1)
    return payload


def _version_callback(value: bool | None) -> None:
    if not value:
        return
    from fp_wraptr import __version__

    typer.echo(f"fp-wraptr {__version__}")
    raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool | None,
        typer.Option(
            "--version",
            callback=_version_callback,
            is_eager=True,
        ),
    ] = None,
) -> None:
    """fp-wraptr: Python utilities for the Fair-Parke macroeconomic model."""
    root = find_project_root(Path(__file__).resolve())
    if root is None:
        return
    try:
        assert_no_forbidden_dirs(root)
    except RuntimeError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=2) from exc


@app.command()
def version() -> None:
    """Print fp-wraptr version."""
    from fp_wraptr import __version__

    console.print(f"fp-wraptr {__version__}")


@packs_app.command("list")
def packs_list() -> None:
    """List discovered local/public pack manifests."""
    from fp_wraptr.scenarios.authoring import list_packs

    _emit_json({"packs": list_packs(repo_root=_cli_repo_root())})


@packs_app.command("describe")
def packs_describe(
    pack_id: Annotated[str, typer.Argument(help="Pack identifier")],
) -> None:
    """Describe one pack, including cards, recipes, and visualization presets."""
    from fp_wraptr.scenarios.packs import describe_pack_manifest

    try:
        payload = describe_pack_manifest(pack_id, repo_root=_cli_repo_root())
    except FileNotFoundError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from None
    _emit_json(payload)


@workspace_app.command("list")
def workspace_list(
    family: Annotated[
        str,
        typer.Option("--family", help="Optional family filter"),
    ] = "",
) -> None:
    """List managed workspaces."""
    from fp_wraptr.scenarios.authoring import list_workspaces_payload

    payload = list_workspaces_payload(repo_root=_cli_repo_root(), family=family)
    _emit_json({"count": len(payload), "workspaces": payload})


@workspace_app.command("create-catalog")
def workspace_create_catalog(
    catalog_entry_id: Annotated[str, typer.Argument(help="Catalog entry id")],
    slug: Annotated[str, typer.Option("--slug", help="Optional workspace slug override")] = "",
    label: Annotated[str, typer.Option("--label", help="Optional workspace label override")] = "",
) -> None:
    """Create a managed workspace from a catalog entry."""
    from fp_wraptr.scenarios.authoring import create_workspace_from_catalog

    try:
        payload = create_workspace_from_catalog(
            repo_root=_cli_repo_root(),
            catalog_entry_id=catalog_entry_id,
            workspace_slug=slug,
            label=label,
        )
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from None
    _emit_json(payload)


@workspace_app.command("create-bundle")
def workspace_create_bundle(
    bundle_yaml: Annotated[str, typer.Argument(help="Bundle YAML path")],
    slug: Annotated[str, typer.Option("--slug", help="Optional workspace slug override")] = "",
    label: Annotated[str, typer.Option("--label", help="Optional workspace label override")] = "",
) -> None:
    """Create a managed bundle workspace from a bundle YAML path."""
    from fp_wraptr.scenarios.authoring import create_workspace_from_bundle

    try:
        payload = create_workspace_from_bundle(
            repo_root=_cli_repo_root(),
            bundle_yaml=bundle_yaml,
            workspace_slug=slug,
            label=label,
        )
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from None
    _emit_json(payload)


@workspace_app.command("show")
def workspace_show(
    workspace_id: Annotated[str, typer.Argument(help="Workspace id")],
) -> None:
    """Show one managed workspace payload."""
    from fp_wraptr.scenarios.authoring import get_workspace

    try:
        payload = get_workspace(repo_root=_cli_repo_root(), workspace_id=workspace_id)
    except FileNotFoundError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from None
    _emit_json(payload)


@workspace_app.command("cards")
def workspace_cards(
    workspace_id: Annotated[str, typer.Argument(help="Workspace id")],
    variant_id: Annotated[
        str, typer.Option("--variant-id", help="Optional bundle variant id")
    ] = "",
) -> None:
    """List current workspace cards/defaults."""
    from fp_wraptr.scenarios.authoring import list_workspace_cards

    try:
        payload = list_workspace_cards(
            repo_root=_cli_repo_root(),
            workspace_id=workspace_id,
            variant_id=variant_id,
        )
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from None
    _emit_json(payload)


@workspace_app.command("set-meta")
def workspace_set_meta(
    workspace_id: Annotated[str, typer.Argument(help="Workspace id")],
    label: Annotated[str, typer.Option("--label", help="Workspace label")] = "",
    description: Annotated[str, typer.Option("--description", help="Workspace description")] = "",
    forecast_start: Annotated[
        str, typer.Option("--forecast-start", help="Forecast start period")
    ] = "",
    forecast_end: Annotated[str, typer.Option("--forecast-end", help="Forecast end period")] = "",
    backend: Annotated[str, typer.Option("--backend", help="Execution backend")] = "",
    track_variables: Annotated[
        str,
        typer.Option("--track-variables", help="Comma-separated tracked variable list"),
    ] = "",
) -> None:
    """Update workspace metadata."""
    from fp_wraptr.scenarios.authoring import update_workspace_metadata

    try:
        payload = update_workspace_metadata(
            repo_root=_cli_repo_root(),
            workspace_id=workspace_id,
            label=label,
            description=description,
            forecast_start=forecast_start,
            forecast_end=forecast_end,
            backend=backend,
            track_variables=[
                token.strip() for token in track_variables.split(",") if token.strip()
            ]
            if track_variables.strip()
            else None,
        )
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from None
    _emit_json(payload)


@workspace_app.command("apply-card")
def workspace_apply_card(
    workspace_id: Annotated[str, typer.Argument(help="Workspace id")],
    card_id: Annotated[str, typer.Argument(help="Card identifier")],
    constants: Annotated[
        str,
        typer.Option("--constants", help="JSON object of constant updates"),
    ] = "{}",
    variant_id: Annotated[str, typer.Option("--variant-id", help="Bundle variant id")] = "",
    selected_target: Annotated[
        str,
        typer.Option("--selected-target", help="Optional explicit series output target"),
    ] = "",
    input_mode: Annotated[str, typer.Option("--input-mode", help="Input mode hint")] = "",
    enabled: Annotated[
        bool | None,
        typer.Option("--enabled/--disabled", help="Explicit enabled state"),
    ] = None,
) -> None:
    """Apply constant or target changes to a workspace card."""
    from fp_wraptr.scenarios.authoring import apply_workspace_card

    constants_payload = _parse_json_object_option(constants, option_name="--constants")
    try:
        payload = apply_workspace_card(
            repo_root=_cli_repo_root(),
            workspace_id=workspace_id,
            card_id=card_id,
            constants={str(key): float(value) for key, value in constants_payload.items()},
            enabled=enabled,
            selected_target=selected_target,
            input_mode=input_mode,
            variant_id=variant_id,
        )
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from None
    _emit_json(payload)


@workspace_app.command("import-series")
def workspace_import_series(
    workspace_id: Annotated[str, typer.Argument(help="Workspace id")],
    card_id: Annotated[str, typer.Argument(help="Series card identifier")],
    series_json: Annotated[
        str,
        typer.Option("--series-json", help="JSON object mapping period -> value"),
    ] = "{}",
    csv_path: Annotated[
        str,
        typer.Option("--csv-path", help="CSV file with period/value columns"),
    ] = "",
    pasted_text: Annotated[
        str,
        typer.Option("--pasted-text", help="Inline period,value content"),
    ] = "",
    variant_id: Annotated[str, typer.Option("--variant-id", help="Bundle variant id")] = "",
    selected_target: Annotated[
        str,
        typer.Option("--selected-target", help="Optional explicit series output target"),
    ] = "",
) -> None:
    """Import a quarterly series into a workspace card."""
    from fp_wraptr.scenarios.authoring import import_workspace_series

    series_payload = _parse_json_object_option(series_json, option_name="--series-json")
    try:
        payload = import_workspace_series(
            repo_root=_cli_repo_root(),
            workspace_id=workspace_id,
            card_id=card_id,
            series_points={str(key): float(value) for key, value in series_payload.items()},
            pasted_text=pasted_text,
            csv_path=csv_path,
            variant_id=variant_id,
            selected_target=selected_target,
        )
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from None
    _emit_json(payload)


@workspace_app.command("add-variant")
def workspace_add_variant(
    workspace_id: Annotated[str, typer.Argument(help="Workspace id")],
    variant_id: Annotated[str, typer.Argument(help="New variant id")],
    label: Annotated[str, typer.Option("--label", help="Variant label")] = "",
    scenario_name: Annotated[
        str,
        typer.Option("--scenario-name", help="Optional exact scenario/run name for this variant"),
    ] = "",
    input_file: Annotated[str, typer.Option("--input-file", help="Override input file")] = "",
    clone_from: Annotated[
        str,
        typer.Option("--clone-from", help="Existing variant id to clone card state from"),
    ] = "",
) -> None:
    """Add a variant to a bundle workspace."""
    from fp_wraptr.scenarios.authoring import add_bundle_variant

    try:
        payload = add_bundle_variant(
            repo_root=_cli_repo_root(),
            workspace_id=workspace_id,
            variant_id=variant_id,
            label=label,
            scenario_name=scenario_name,
            input_file=input_file,
            clone_from=clone_from,
        )
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from None
    _emit_json(payload)


@workspace_app.command("remove-variant")
def workspace_remove_variant(
    workspace_id: Annotated[str, typer.Argument(help="Workspace id")],
    variant_id: Annotated[str, typer.Argument(help="Variant id to remove")],
) -> None:
    """Remove a variant from a bundle workspace."""
    from fp_wraptr.scenarios.authoring import remove_bundle_variant

    try:
        payload = remove_bundle_variant(
            repo_root=_cli_repo_root(),
            workspace_id=workspace_id,
            variant_id=variant_id,
        )
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from None
    _emit_json(payload)


@workspace_app.command("update-variant")
def workspace_update_variant(
    workspace_id: Annotated[str, typer.Argument(help="Workspace id")],
    variant_id: Annotated[str, typer.Argument(help="Variant id to update")],
    label: Annotated[str, typer.Option("--label", help="Variant label")] = "",
    scenario_name: Annotated[
        str,
        typer.Option(
            "--scenario-name", help="Optional exact scenario/run name for the variant output"
        ),
    ] = "",
    input_file: Annotated[str, typer.Option("--input-file", help="Override input file")] = "",
    enabled: Annotated[
        bool | None,
        typer.Option("--enabled/--disabled", help="Explicit enabled state"),
    ] = None,
) -> None:
    """Update metadata for an existing bundle variant."""
    from fp_wraptr.scenarios.authoring import update_bundle_variant

    try:
        payload = update_bundle_variant(
            repo_root=_cli_repo_root(),
            workspace_id=workspace_id,
            variant_id=variant_id,
            label=label,
            scenario_name=scenario_name,
            input_file=input_file,
            enabled=enabled,
        )
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from None
    _emit_json(payload)


@workspace_app.command("clone-variant")
def workspace_clone_variant(
    workspace_id: Annotated[str, typer.Argument(help="Workspace id")],
    variant_id: Annotated[str, typer.Argument(help="New variant id")],
    clone_from: Annotated[
        str, typer.Option("--clone-from", help="Existing variant id to clone from")
    ],
    label: Annotated[str, typer.Option("--label", help="Variant label")] = "",
    scenario_name: Annotated[
        str,
        typer.Option(
            "--scenario-name", help="Optional exact scenario/run name for the variant output"
        ),
    ] = "",
    input_file: Annotated[str, typer.Option("--input-file", help="Override input file")] = "",
    enabled: Annotated[
        bool | None,
        typer.Option("--enabled/--disabled", help="Explicit enabled state"),
    ] = None,
    card_id: Annotated[
        str, typer.Option("--card-id", help="Optional card id to seed on the new variant")
    ] = "",
    constants: Annotated[
        str,
        typer.Option(
            "--constants", help="Optional JSON object of constant updates for the seeded card"
        ),
    ] = "{}",
    selected_target: Annotated[
        str,
        typer.Option("--selected-target", help="Optional explicit target for the seeded card"),
    ] = "",
    input_mode: Annotated[
        str, typer.Option("--input-mode", help="Optional input mode hint for the seeded card")
    ] = "",
) -> None:
    """Clone a bundle variant, update its metadata, and optionally seed one card patch."""
    from fp_wraptr.scenarios.authoring import clone_bundle_variant_recipe

    constants_payload = _parse_json_object_option(constants, option_name="--constants")
    try:
        payload = clone_bundle_variant_recipe(
            repo_root=_cli_repo_root(),
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
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from None
    _emit_json(payload)


@workspace_app.command("compile")
def workspace_compile(
    workspace_id: Annotated[str, typer.Argument(help="Workspace id")],
) -> None:
    """Compile a managed workspace."""
    from fp_wraptr.scenarios.authoring import compile_workspace

    try:
        payload = compile_workspace(repo_root=_cli_repo_root(), workspace_id=workspace_id)
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from None
    _emit_json(payload)


@workspace_app.command("run")
def workspace_run(
    workspace_id: Annotated[str, typer.Argument(help="Workspace id")],
    output_dir: Annotated[
        Path | None,
        typer.Option(
            "--output-dir",
            help="Artifacts output directory (defaults to workspace artifacts_root)",
        ),
    ] = None,
) -> None:
    """Compile and run a managed workspace."""
    from fp_wraptr.scenarios.authoring import run_workspace

    try:
        payload = run_workspace(
            repo_root=_cli_repo_root(),
            workspace_id=workspace_id,
            output_dir=str(output_dir) if output_dir is not None else "",
        )
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from None
    _emit_json(payload)


@workspace_app.command("compare")
def workspace_compare(
    workspace_id: Annotated[str, typer.Argument(help="Workspace id")],
    run_a: Annotated[str, typer.Option("--run-a", help="Baseline run directory")] = "",
    run_b: Annotated[str, typer.Option("--run-b", help="Comparison run directory")] = "",
    top_n: Annotated[int, typer.Option("--top-n", help="Top delta count")] = 10,
) -> None:
    """Compare two linked runs for a workspace."""
    from fp_wraptr.scenarios.authoring import compare_workspace_runs

    try:
        payload = compare_workspace_runs(
            repo_root=_cli_repo_root(),
            workspace_id=workspace_id,
            run_a=run_a,
            run_b=run_b,
            top_n=top_n,
        )
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from None
    _emit_json(payload)


@workspace_app.command("visualizations")
def workspace_visualizations(
    workspace_id: Annotated[str, typer.Argument(help="Workspace id")],
) -> None:
    """List visualization presets for a workspace."""
    from fp_wraptr.scenarios.authoring import list_visualizations

    try:
        payload = list_visualizations(repo_root=_cli_repo_root(), workspace_id=workspace_id)
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from None
    _emit_json({"visualizations": payload})


@workspace_app.command("build-view")
def workspace_build_view(
    view_id: Annotated[str, typer.Argument(help="Visualization view id")],
    workspace_id: Annotated[str, typer.Option("--workspace-id", help="Workspace id")] = "",
    pack_id: Annotated[str, typer.Option("--pack-id", help="Pack id")] = "",
    run_dirs: Annotated[
        str,
        typer.Option("--run-dirs", help="JSON array of explicit run directories"),
    ] = "[]",
) -> None:
    """Build a visualization payload from recent or explicit runs."""
    from fp_wraptr.scenarios.authoring import build_visualization_view

    run_dirs_payload = _parse_json_array_option(run_dirs, option_name="--run-dirs")
    try:
        payload = build_visualization_view(
            repo_root=_cli_repo_root(),
            view_id=view_id,
            workspace_id=workspace_id,
            pack_id=pack_id,
            run_dirs=[str(item) for item in run_dirs_payload],
        )
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from None
    _emit_json(payload)


@gender_app.command("init")
def gender_init(
    fp_home: Annotated[
        Path,
        typer.Option("--fp-home", help="Local FM/fair bundle directory to bootstrap from"),
    ] = Path("FM"),
    force: Annotated[
        bool,
        typer.Option("--force", help="Overwrite previously generated family files"),
    ] = False,
) -> None:
    """Bootstrap the local/private gender family from a local Fair model directory."""
    from fp_wraptr.gender_family import initialize_gender_family

    try:
        payload = initialize_gender_family(
            repo_root=_cli_repo_root(),
            fp_home=fp_home,
            force=force,
        )
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from None
    _emit_json(payload)


@gender_app.command("refresh-data")
def gender_refresh_data(
    dataset: Annotated[
        str,
        typer.Option(
            "--dataset",
            help=(
                "Dataset to refresh: childcare, paid-leave, caregiver-leave, "
                "mother-share, tax-wedge, or all"
            ),
        ),
    ] = "childcare",
    force: Annotated[
        bool,
        typer.Option("--force", help="Re-download the selected official source when supported"),
    ] = False,
    source_url: Annotated[
        str,
        typer.Option("--source-url", help="Optional exact official source URL override"),
    ] = "",
) -> None:
    """Fetch or materialize exact-source gender helper-series data into the family data area."""
    from fp_wraptr.gender_family import refresh_gender_data

    try:
        payload = refresh_gender_data(
            repo_root=_cli_repo_root(),
            force=force,
            source_url=source_url,
            dataset=dataset,
        )
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from None
    _emit_json(payload)


@gender_app.command("status")
def gender_status_command() -> None:
    """Report gender family bootstrap/data readiness and runnable assets."""
    from fp_wraptr.gender_family import gender_status

    _emit_json(gender_status(repo_root=_cli_repo_root()))


@gender_app.command("refresh-mothers-override")
def gender_refresh_mothers_override(
    fmout_path: Annotated[
        Path | None,
        typer.Option(
            "--fmout",
            help="Optional fmout.txt path; defaults to the latest mothers-paid-leave base artifact.",
        ),
    ] = None,
) -> None:
    """Refresh the default mothers fppy override from a canonical fp.exe fmout.txt."""
    from fp_wraptr.gender_family import refresh_mothers_override

    try:
        payload = refresh_mothers_override(
            repo_root=_cli_repo_root(),
            fmout_path=fmout_path,
        )
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from None
    _emit_json(payload)


@gender_app.command("analyze-mothers-fit")
def gender_analyze_mothers_fit(
    fmout_path: Annotated[
        Path | None,
        typer.Option(
            "--fmout",
            help="Optional fmout.txt path; defaults to the latest mothers-paid-leave base artifact.",
        ),
    ] = None,
) -> None:
    """Compare mothers/non-mothers OLS research coefficients against fp.exe-estimated equations."""
    from fp_wraptr.gender_family import analyze_mothers_fit

    try:
        payload = analyze_mothers_fit(
            repo_root=_cli_repo_root(),
            fmout_path=fmout_path,
        )
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from None
    _emit_json(payload)


@bundle_app.command("run")
def bundle_run(
    bundle_yaml: Annotated[Path, typer.Argument(help="Path to bundle YAML config")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", "-o", help="Output artifacts directory")
    ] = Path("artifacts/bundles"),
    group: Annotated[
        bool,
        typer.Option(
            "--group/--no-group",
            help="Create a bundle run directory (bundle_name_timestamp) under output_dir",
        ),
    ] = True,
    strict: Annotated[
        bool,
        typer.Option("--strict", help="Exit non-zero if any variant fails"),
    ] = False,
) -> None:
    """Run all variants in a bundle YAML and write a bundle_report.json."""
    from fp_wraptr.scenarios.bundle import BundleConfig, run_bundle
    from fp_wraptr.scenarios.runner import validate_fp_home

    try:
        bundle_config = BundleConfig.from_yaml(bundle_yaml)
    except (FileNotFoundError, yaml.YAMLError, ValueError) as exc:
        console.print(f"[red]Failed to load bundle YAML:[/red] {exc}")
        raise typer.Exit(code=1) from None

    variants = bundle_config.resolve_variants()
    fp_homes = sorted({Path(v.fp_home) for v in variants})
    for fp_home in fp_homes:
        validate_fp_home(fp_home)

    bundle_name = str(bundle_config.base.get("name", "bundle"))
    timestamp = _dt.datetime.now(_dt.UTC).strftime("%Y%m%d_%H%M%S")
    run_root = Path(output_dir) / f"{bundle_name}_{timestamp}" if group else Path(output_dir)
    run_root.mkdir(parents=True, exist_ok=True)

    console.print(f"[bold]Running bundle:[/bold] {bundle_name} ({len(variants)} variant(s))")
    result = run_bundle(bundle_config, output_dir=run_root)

    report_path = run_root / "bundle_report.json"
    report_path.write_text(
        json.dumps(result.to_dict(), indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )

    table = Table(title=f"Bundle results: {bundle_name}")
    table.add_column("Variant", style="cyan")
    table.add_column("Success")
    table.add_column("Output dir")
    table.add_column("Error")
    for entry in result.entries:
        table.add_row(
            entry.variant_name,
            "yes" if entry.success else "no",
            str(entry.output_dir) if entry.output_dir else "",
            entry.error or "",
        )
    console.print(table)
    console.print(f"bundle dir \u2192 {run_root}")
    console.print(f"report \u2192 {report_path}")

    if strict and result.n_failed:
        raise typer.Exit(code=1)


@triage_app.command("fppy-report")
def triage_fppy_report(
    run_dir: Annotated[
        Path,
        typer.Argument(help="Parity run directory (or work_fppy dir) containing fppy_report.json"),
    ],
    out_dir: Annotated[
        Path | None,
        typer.Option(
            "--out-dir", help="Optional output directory (defaults next to fppy_report.json)"
        ),
    ] = None,
) -> None:
    """Bucket fppy_report.json issues into stable categories (JSON + CSV)."""

    from fp_wraptr.analysis.triage_fppy import triage_fppy_report as _triage

    try:
        summary_path, csv_path = _triage(run_dir, out_dir=out_dir)
    except FileNotFoundError as exc:
        console.print(f"[red]Not found:[/red] {exc}")
        raise typer.Exit(code=1) from None
    except (RuntimeError, ValueError) as exc:
        console.print(f"[red]Triage failed:[/red] {exc}")
        raise typer.Exit(code=1) from None
    console.print(f"[green]summary:[/green] {summary_path}")
    console.print(f"[green]csv:[/green] {csv_path}")


@triage_app.command("parity-hardfails")
def triage_parity_hardfails(
    run_dir: Annotated[
        Path, typer.Argument(help="Parity run directory containing parity_report.json")
    ],
) -> None:
    """Recompute and export the full hard-fail cell set (CSV + JSON summary)."""

    from fp_wraptr.analysis.triage_parity_hardfails import triage_parity_hardfails as _triage

    try:
        csv_path, summary_path = _triage(run_dir)
    except FileNotFoundError as exc:
        console.print(f"[red]Not found:[/red] {exc}")
        raise typer.Exit(code=1) from None
    except (RuntimeError, ValueError) as exc:
        console.print(f"[red]Triage failed:[/red] {exc}")
        raise typer.Exit(code=1) from None
    console.print(f"[green]csv:[/green] {csv_path}")
    console.print(f"[green]summary:[/green] {summary_path}")


@triage_app.command("anchor-acceptance")
def triage_anchor_acceptance(
    fpexe: Annotated[
        Path, typer.Option("--fpexe", help="LOADFORMAT path for fp.exe output")
    ],
    fppy: Annotated[
        Path, typer.Option("--fppy", help="LOADFORMAT path for fppy output")
    ],
    fpr: Annotated[
        Path, typer.Option("--fpr", help="LOADFORMAT path for fp-r output")
    ],
    preset: Annotated[
        str,
        typer.Option(
            "--preset",
            help="Optional named anchor-acceptance preset (for example: pse_rs_frontier)",
        ),
    ] = "",
    anchors: Annotated[
        str,
        typer.Option(
            "--anchors",
            help="Comma-separated shared-semantic anchor variables",
        ),
    ] = "",
    methodology: Annotated[
        str,
        typer.Option(
            "--methodology",
            help="Comma-separated methodology/explanation variables",
        ),
    ] = "",
    start: Annotated[
        str | None,
        typer.Option("--start", help="Optional start period (YYYY.Q)"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", help="Optional end period (YYYY.Q)"),
    ] = None,
    out_dir: Annotated[
        Path,
        typer.Option("--out-dir", help="Output directory for JSON + CSV acceptance artifacts"),
    ] = Path("artifacts/anchor_acceptance"),
) -> None:
    """Write an anchor-based backend acceptance report for one shared-semantic branch."""

    from fp_wraptr.analysis.anchor_acceptance import (
        build_anchor_acceptance_report,
        resolve_anchor_acceptance_preset,
        write_anchor_acceptance_report,
    )

    try:
        preset_payload = resolve_anchor_acceptance_preset(preset)
        anchor_vars = [item.strip() for item in str(anchors).split(",") if item.strip()]
        methodology_vars = [item.strip() for item in str(methodology).split(",") if item.strip()]
        if not anchor_vars and preset_payload is not None:
            anchor_vars = list(preset_payload.get("anchors", []) or [])
        if not methodology_vars and preset_payload is not None:
            methodology_vars = list(preset_payload.get("methodology", []) or [])
        effective_start = start if start is not None else (preset_payload.get("start") if preset_payload else None)
        effective_end = end if end is not None else (preset_payload.get("end") if preset_payload else None)
        if not anchor_vars:
            raise ValueError("Provide --anchors or a known --preset")
        report = build_anchor_acceptance_report(
            {"fpexe": fpexe, "fppy": fppy, "fp-r": fpr},
            anchor_variables=anchor_vars,
            methodology_variables=methodology_vars,
            start=effective_start,
            end=effective_end,
        )
        json_path, csv_path = write_anchor_acceptance_report(report, output_dir=out_dir)
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]Anchor acceptance failed:[/red] {exc}")
        raise typer.Exit(code=1) from None

    counts = report.get("counts", {}) or {}
    console.print(
        "status="
        f"{report.get('status', 'review')} "
        f"anchor_review_count={counts.get('anchor_review_count', 0)} "
        f"methodology_review_count={counts.get('methodology_review_count', 0)}"
    )
    if preset_payload is not None:
        console.print(f"preset={preset_payload.get('name')}")
    console.print(f"[green]json:[/green] {json_path}")
    console.print(f"[green]csv:[/green] {csv_path}")


@triage_app.command("backend-defensibility")
def triage_backend_defensibility(
    fpexe: Annotated[
        Path, typer.Option("--fpexe", help="LOADFORMAT path for fp.exe output")
    ],
    fppy: Annotated[
        Path, typer.Option("--fppy", help="LOADFORMAT path for fppy output")
    ],
    fpr: Annotated[
        Path, typer.Option("--fpr", help="LOADFORMAT path for fp-r output")
    ],
    variables: Annotated[
        str,
        typer.Option(
            "--variables",
            help="Optional comma-separated variable allowlist",
        ),
    ] = "",
    focus: Annotated[
        str,
        typer.Option(
            "--focus",
            help="Optional comma-separated focus variables for the summary payload",
        ),
    ] = "",
    start: Annotated[
        str | None,
        typer.Option("--start", help="Optional start period (YYYY.Q)"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", help="Optional end period (YYYY.Q)"),
    ] = None,
    out_dir: Annotated[
        Path,
        typer.Option("--out-dir", help="Output directory for JSON + CSV defensibility artifacts"),
    ] = Path("artifacts/backend_defensibility"),
) -> None:
    """Write a backend-defensibility report for fp.exe, fppy, and fp-r."""

    from fp_wraptr.analysis.backend_defensibility import (
        build_backend_defensibility_report,
        write_backend_defensibility_report,
    )

    try:
        requested_variables = [item.strip() for item in str(variables).split(",") if item.strip()]
        focus_variables = [item.strip() for item in str(focus).split(",") if item.strip()]
        report = build_backend_defensibility_report(
            {"fpexe": fpexe, "fppy": fppy, "fp-r": fpr},
            start=start,
            end=end,
            variables=requested_variables or None,
            focus_variables=focus_variables or None,
        )
        json_path, csv_path = write_backend_defensibility_report(report, output_dir=out_dir)
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]Backend defensibility failed:[/red] {exc}")
        raise typer.Exit(code=1) from None

    console.print(
        "summary_rows="
        f"{len(report.get('summary_rows', []) or [])} "
        f"focus_rows={len(report.get('focus_rows', []) or [])}"
    )
    console.print(f"[green]json:[/green] {json_path}")
    console.print(f"[green]csv:[/green] {csv_path}")


@triage_app.command("focused-series")
def triage_focused_series(
    fpexe: Annotated[
        Path, typer.Option("--fpexe", help="LOADFORMAT path for fp.exe output")
    ],
    fppy: Annotated[
        Path, typer.Option("--fppy", help="LOADFORMAT path for fppy output")
    ],
    fpr: Annotated[
        Path, typer.Option("--fpr", help="LOADFORMAT path for fp-r output")
    ],
    variables: Annotated[
        str,
        typer.Option(
            "--variables",
            help="Comma-separated variables to compare",
        ),
    ],
    start: Annotated[
        str | None,
        typer.Option("--start", help="Optional start period (YYYY.Q)"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", help="Optional end period (YYYY.Q)"),
    ] = None,
    out_dir: Annotated[
        Path,
        typer.Option("--out-dir", help="Output directory for JSON + CSV focused-series artifacts"),
    ] = Path("artifacts/focused_series_compare"),
) -> None:
    """Write a focused per-period series comparison for selected variables."""

    from fp_wraptr.analysis.focused_series_compare import (
        build_focused_series_compare_report,
        write_focused_series_compare_report,
    )

    try:
        requested_variables = [item.strip() for item in str(variables).split(",") if item.strip()]
        report = build_focused_series_compare_report(
            {"fpexe": fpexe, "fppy": fppy, "fp-r": fpr},
            variables=requested_variables,
            start=start,
            end=end,
        )
        json_path, csv_path = write_focused_series_compare_report(report, output_dir=out_dir)
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]Focused series compare failed:[/red] {exc}")
        raise typer.Exit(code=1) from None

    console.print(
        "row_count="
        f"{report.get('row_count', 0)} "
        f"common_period_count={report.get('common_period_count', 0)}"
    )
    console.print(f"[green]json:[/green] {json_path}")
    console.print(f"[green]csv:[/green] {csv_path}")


@triage_app.command("identity-decomposition")
def triage_identity_decomposition(
    fpexe: Annotated[
        Path, typer.Option("--fpexe", help="LOADFORMAT path for fp.exe output")
    ],
    fppy: Annotated[
        Path, typer.Option("--fppy", help="LOADFORMAT path for fppy output")
    ],
    fpr: Annotated[
        Path, typer.Option("--fpr", help="LOADFORMAT path for fp-r output")
    ],
    identity: Annotated[
        str,
        typer.Option(
            "--identity",
            help="Additive identity to decompose, for example 'PIEF=XX+...-CCH+CDH'",
        ),
    ],
    period: Annotated[
        str,
        typer.Option("--period", help="Single period to evaluate (YYYY.Q)"),
    ],
    out_dir: Annotated[
        Path,
        typer.Option("--out-dir", help="Output directory for JSON + CSV identity artifacts"),
    ] = Path("artifacts/identity_decomposition"),
) -> None:
    """Write a three-engine identity decomposition for one target and period."""

    from fp_wraptr.analysis.identity_decomposition import (
        build_identity_decomposition_report,
        write_identity_decomposition_report,
    )

    try:
        report = build_identity_decomposition_report(
            {"fpexe": fpexe, "fppy": fppy, "fp-r": fpr},
            identity=identity,
            period=period,
        )
        json_path, csv_path = write_identity_decomposition_report(report, output_dir=out_dir)
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]Identity decomposition failed:[/red] {exc}")
        raise typer.Exit(code=1) from None

    console.print(
        "target="
        f"{report.get('target')} "
        f"term_count={report.get('term_count', 0)} "
        f"period={report.get('period')}"
    )
    console.print(f"[green]json:[/green] {json_path}")
    console.print(f"[green]csv:[/green] {csv_path}")


@triage_app.command("backend-release-shape")
def triage_backend_release_shape(
    anchor_report: Annotated[
        Path,
        typer.Option(
            "--anchor-report",
            help="Path to an existing anchor_acceptance_report.json artifact",
        ),
    ],
    stock_baseline_ok: Annotated[
        bool,
        typer.Option("--stock-baseline-ok/--stock-baseline-fail", help="Whether stock baseline acceptance is green"),
    ] = True,
    raw_input_public_ok: Annotated[
        bool,
        typer.Option("--raw-input-public-ok/--raw-input-public-fail", help="Whether the raw-input public path is real"),
    ] = True,
    modified_decks_run: Annotated[
        bool,
        typer.Option("--modified-decks-run/--modified-decks-fail", help="Whether the modified-deck surface runs"),
    ] = True,
    docs_honest: Annotated[
        bool,
        typer.Option("--docs-honest/--docs-not-honest", help="Whether docs match the supported surface"),
    ] = True,
    corpus_green: Annotated[
        bool,
        typer.Option("--corpus-green/--corpus-not-green", help="Whether the broader release corpus is green"),
    ] = False,
    out_dir: Annotated[
        Path,
        typer.Option("--out-dir", help="Output directory for JSON release-shape artifact"),
    ] = Path("artifacts/backend_release_shape"),
) -> None:
    """Write a preview-vs-peer release-shape decision from one anchor report."""

    from fp_wraptr.analysis.backend_release_shape import (
        build_backend_release_shape_report,
        write_backend_release_shape_report,
    )

    try:
        report = build_backend_release_shape_report(
            anchor_report,
            stock_baseline_ok=stock_baseline_ok,
            raw_input_public_ok=raw_input_public_ok,
            modified_decks_run=modified_decks_run,
            docs_honest=docs_honest,
            corpus_green=corpus_green,
        )
        json_path = write_backend_release_shape_report(report, output_dir=out_dir)
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        console.print(f"[red]Backend release shape failed:[/red] {exc}")
        raise typer.Exit(code=1) from None

    decision = report.get("decision", {}) or {}
    console.print(
        "recommended_label="
        f"{decision.get('recommended_label', '')} "
        f"preview_ready={decision.get('preview_ready', False)} "
        f"peer_backend_ready={decision.get('peer_backend_ready', False)}"
    )
    console.print(f"[green]json:[/green] {json_path}")


@triage_app.command("backend-release-corpus")
def triage_backend_release_corpus(
    manifest: Annotated[
        Path,
        typer.Option(
            "--manifest",
            help="Path to a backend release corpus manifest JSON file",
        ),
    ] = Path("docs/backend-release-corpus.json"),
    out_dir: Annotated[
        Path,
        typer.Option("--out-dir", help="Output directory for JSON + CSV corpus artifact"),
    ] = Path("artifacts/backend_release_corpus"),
) -> None:
    """Summarize which corpus decks already have release packets and which still block corpus readiness."""

    from fp_wraptr.analysis.backend_release_corpus import (
        build_backend_release_corpus_report,
        write_backend_release_corpus_report,
    )

    try:
        report = build_backend_release_corpus_report(manifest)
        json_path, csv_path = write_backend_release_corpus_report(report, output_dir=out_dir)
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        console.print(f"[red]Backend release corpus failed:[/red] {exc}")
        raise typer.Exit(code=1) from None

    console.print(
        "entry_count="
        f"{report.get('entry_count', 0)} "
        f"required_preview_ready={report.get('required_preview_ready_count', 0)}/{report.get('required_entry_count', 0)} "
        f"required_peer_ready={report.get('required_peer_ready_count', 0)}/{report.get('required_entry_count', 0)}"
    )
    console.print(
        "corpus_green_for_preview="
        f"{report.get('corpus_green_for_preview', False)} "
        f"corpus_green_for_peer={report.get('corpus_green_for_peer', False)}"
    )
    console.print(f"[green]json:[/green] {json_path}")
    console.print(f"[green]csv:[/green] {csv_path}")


@triage_app.command("scenario-delta-compare")
def triage_scenario_delta_compare(
    baseline_left: Annotated[
        Path,
        typer.Option("--baseline-left", help="Baseline LOADFORMAT path for the left engine"),
    ],
    scenario_left: Annotated[
        Path,
        typer.Option("--scenario-left", help="Scenario LOADFORMAT path for the left engine"),
    ],
    baseline_right: Annotated[
        Path,
        typer.Option("--baseline-right", help="Baseline LOADFORMAT path for the right engine"),
    ],
    scenario_right: Annotated[
        Path,
        typer.Option("--scenario-right", help="Scenario LOADFORMAT path for the right engine"),
    ],
    left_label: Annotated[
        str,
        typer.Option("--left-label", help="Label for the left engine in the report"),
    ] = "left",
    right_label: Annotated[
        str,
        typer.Option("--right-label", help="Label for the right engine in the report"),
    ] = "right",
    variables: Annotated[
        str,
        typer.Option("--variables", help="Optional comma-separated variable allowlist"),
    ] = "",
    start: Annotated[
        str | None,
        typer.Option("--start", help="Optional start period (YYYY.Q)"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", help="Optional end period (YYYY.Q)"),
    ] = None,
    out_dir: Annotated[
        Path,
        typer.Option("--out-dir", help="Output directory for JSON + CSV scenario-delta artifacts"),
    ] = Path("artifacts/scenario_delta_compare"),
) -> None:
    """Compare how two engines reproduce one scenario's delta from baseline."""

    from fp_wraptr.analysis.scenario_delta_compare import (
        build_scenario_delta_compare_report,
        write_scenario_delta_compare_report,
    )

    try:
        requested_variables = [item.strip() for item in str(variables).split(",") if item.strip()]
        report = build_scenario_delta_compare_report(
            baseline_left=baseline_left,
            scenario_left=scenario_left,
            baseline_right=baseline_right,
            scenario_right=scenario_right,
            left_label=left_label,
            right_label=right_label,
            variables=requested_variables or None,
            start=start,
            end=end,
        )
        json_path, summary_csv_path, detail_csv_path = write_scenario_delta_compare_report(
            report, output_dir=out_dir
        )
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[red]Scenario delta compare failed:[/red] {exc}")
        raise typer.Exit(code=1) from None

    summary_rows = list(report.get("summary_rows", []) or [])
    review_rows = [row for row in summary_rows if str(row.get("classification")) == "review"]
    console.print(
        "summary_rows="
        f"{len(summary_rows)} "
        f"review_rows={len(review_rows)} "
        f"left={left_label} "
        f"right={right_label}"
    )
    console.print(f"[green]json:[/green] {json_path}")
    console.print(f"[green]summary csv:[/green] {summary_csv_path}")
    console.print(f"[green]rows csv:[/green] {detail_csv_path}")


@triage_app.command("fp-ineq-publication")
def triage_fp_ineq_publication(
    manifest: Annotated[
        str,
        typer.Option("--manifest", help="Manifest path or URL for the fp-ineq run bundle"),
    ] = "https://smkwray.github.io/fp-ineq/manifest.json",
    matrix: Annotated[
        Path,
        typer.Option("--matrix", help="Path to the current fp-ineq fp-r matrix JSON"),
    ] = Path("artifacts/fp_ineq_fpr_matrix_20260405/fp_ineq_fpr_matrix.json"),
    contract: Annotated[
        Path,
        typer.Option("--contract", help="Path to fp-ineq publication guardrails JSON"),
    ] = Path("docs/fp-ineq-publication-guardrails.json"),
    out_dir: Annotated[
        Path,
        typer.Option("--out-dir", help="Output directory for JSON + CSV publication artifacts"),
    ] = Path("artifacts/fp_ineq_publication_validation"),
) -> None:
    """Validate an fp-ineq publication bundle against the current fp-r defensibility gate."""

    from fp_wraptr.analysis.fp_ineq_publication_validation import (
        validate_fp_ineq_publication,
        write_fp_ineq_publication_report,
    )

    try:
        report = validate_fp_ineq_publication(
            manifest_source=manifest,
            matrix_path=matrix,
            contract_path=contract,
        )
        json_path, csv_path = write_fp_ineq_publication_report(report, output_dir=out_dir)
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        console.print(f"[red]fp-ineq publication validation failed:[/red] {exc}")
        raise typer.Exit(code=1) from None

    summary = report.get("summary", {}) or {}
    console.print(
        "published_runs="
        f"{summary.get('published_run_count', 0)} "
        f"modern_ok={summary.get('modern_branch_ok_count', 0)} "
        f"legacy_split={summary.get('modern_branch_ok_but_legacy_split_count', 0)} "
        f"default_safe={summary.get('default_runs_public_default_safe_count', 0)}/{summary.get('default_run_count', 0)}"
    )
    console.print(
        "status="
        f"{'pass' if report.get('ok') else 'fail'} "
        f"failure_count={len(report.get('failures', []) or [])}"
    )
    console.print(f"[green]json:[/green] {json_path}")
    console.print(f"[green]csv:[/green] {csv_path}")
    if not report.get("ok"):
        raise typer.Exit(code=1)


@triage_app.command("loop")
def triage_loop(
    scenario: Annotated[Path, typer.Argument(help="Path to scenario YAML config")],
    fp_home: Annotated[
        Path | None,
        typer.Option("--fp-home", envvar="FP_HOME", help="Directory containing fp.exe"),
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", "-o", help="Output artifacts directory")
    ] = Path("artifacts"),
    fingerprint_lock: Annotated[
        Path | None,
        typer.Option("--fingerprint-lock", help="Optional input fingerprint lockfile"),
    ] = None,
    with_drift: Annotated[
        bool,
        typer.Option("--with-drift", help="Enable bounded-drift guardrails"),
    ] = False,
    gate_pabev_end: Annotated[
        str | None,
        typer.Option("--gate-pabev-end", help="Optional parity gate end quarter (YYYY.Q)"),
    ] = None,
    quick: Annotated[
        bool,
        typer.Option(
            "--quick",
            help="Shortcut for gating parity to the scenario forecast_start quarter (fast smoke check)",
        ),
    ] = False,
    lenient: Annotated[
        bool,
        typer.Option("--lenient", help="Exit 0 even on parity gate/hard-fail mismatches"),
    ] = False,
    save_golden: Annotated[
        Path | None,
        typer.Option(
            "--save-golden",
            help="Directory root where parity golden artifacts are written",
        ),
    ] = None,
    regression: Annotated[
        Path | None,
        typer.Option(
            "--regression",
            help="Directory root of saved parity golden artifacts for regression check",
        ),
    ] = None,
) -> None:
    """Run a full parity triage loop (parity -> triage artifacts -> optional regression)."""

    from fp_wraptr.analysis.parity import DriftConfig, GateConfig, run_parity
    from fp_wraptr.analysis.parity_regression import (
        compare_parity_to_golden,
        save_parity_golden,
        write_regression_report,
    )
    from fp_wraptr.analysis.triage_fppy import triage_fppy_report as _triage_fppy
    from fp_wraptr.analysis.triage_parity_hardfails import (
        triage_parity_hardfails as _triage_hardfails,
    )
    from fp_wraptr.scenarios.runner import load_scenario_config, validate_fp_home

    if save_golden is not None and regression is not None:
        console.print(
            "[red]--save-golden and --regression cannot be used together in one loop run.[/red]"
        )
        raise typer.Exit(code=1)

    try:
        config = load_scenario_config(scenario)
    except FileNotFoundError as exc:
        console.print(f"[red]Scenario file not found:[/red] {exc}")
        raise typer.Exit(code=1) from None
    except (ValidationError, yaml.YAMLError, ValueError) as exc:
        _print_validation_error(exc)
        raise typer.Exit(code=1) from None

    if fp_home:
        config.fp_home = fp_home
    validate_fp_home(config.fp_home)

    effective_gate_end = gate_pabev_end
    if effective_gate_end is None and quick:
        effective_gate_end = str(config.forecast_start)
    gate = GateConfig(pabev_end=effective_gate_end, drift=DriftConfig(enabled=bool(with_drift)))
    console.print(f"[bold]Triage loop parity run:[/bold] {config.name}")
    result = run_parity(
        config,
        output_dir=output_dir,
        fp_home_override=config.fp_home,
        gate=gate,
        fingerprint_lock=fingerprint_lock,
    )
    console.print(f"[green]Parity completed.[/green] Output: {result.run_dir}")
    _print_run_dir(result.run_dir)
    seed_diag_suffix = _format_parity_seed_diagnostics(result.run_dir)
    runtime_profile_suffix = _format_parity_runtime_profile(result.run_dir)
    hard_fail_reason_suffix = _format_parity_hard_fail_reasons(result.pabev_detail)
    _print_fpexe_preflight_warning(result)
    _print_fpexe_solution_error_warning(result)
    console.print(
        f"status={result.status} exit_code={result.exit_code} "
        f"hard_fail_cell_count={result.pabev_detail.get('hard_fail_cell_count', 0)} "
        f"max_abs_diff={result.pabev_detail.get('max_abs_diff', 0.0)}"
        f"{hard_fail_reason_suffix}"
        f"{seed_diag_suffix}"
        f"{runtime_profile_suffix}"
    )

    loop_exit_code = _parity_cli_exit_code(int(result.exit_code), lenient=bool(lenient))
    _warn_if_parity_suppressed(
        parity_exit_code=int(result.exit_code),
        cli_exit_code=loop_exit_code,
        lenient=bool(lenient),
    )

    run_dir = Path(result.run_dir)
    try:
        hardfails_csv, hardfails_summary = _triage_hardfails(run_dir)
        console.print(f"[green]triage hardfails csv:[/green] {hardfails_csv}")
        console.print(f"[green]triage hardfails summary:[/green] {hardfails_summary}")
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[yellow]Skipping hard-fail triage:[/yellow] {exc}")

    try:
        fppy_summary, fppy_csv = _triage_fppy(run_dir)
        console.print(f"[green]triage fppy summary:[/green] {fppy_summary}")
        console.print(f"[green]triage fppy csv:[/green] {fppy_csv}")
    except (FileNotFoundError, ValueError) as exc:
        console.print(f"[yellow]Skipping fppy triage:[/yellow] {exc}")

    if save_golden is not None:
        try:
            saved_dir = save_parity_golden(run_dir, Path(save_golden))
        except (FileNotFoundError, ValueError) as exc:
            console.print(f"[red]Failed to save parity golden:[/red] {exc}")
            _print_run_dir(run_dir)
            raise typer.Exit(code=1) from None
        console.print(f"[green]Saved parity golden:[/green] {saved_dir}")

    if regression is not None:
        try:
            regression_payload = compare_parity_to_golden(run_dir, Path(regression))
            regression_path = write_regression_report(regression_payload, run_dir)
        except (FileNotFoundError, ValueError) as exc:
            console.print(f"[red]Parity regression compare failed:[/red] {exc}")
            _print_run_dir(run_dir)
            raise typer.Exit(code=1) from None
        counts = regression_payload.get("counts", {})
        console.print(
            "regression_status="
            f"{regression_payload.get('status', 'failed')} "
            f"new_missing_left={counts.get('new_missing_left', 0)} "
            f"new_missing_right={counts.get('new_missing_right', 0)} "
            f"new_hard_fail_cells={counts.get('new_hard_fail_cells', 0)} "
            f"new_diff_variables={counts.get('new_diff_variables', 0)}"
        )
        console.print(f"[green]Regression report:[/green] {regression_path}")
        if regression_payload.get("status") != "ok" and loop_exit_code == 0:
            loop_exit_code = 6

    console.print(f"[green]Loop completed.[/green] run_dir={run_dir}")
    raise typer.Exit(code=loop_exit_code)


def _render_report(run_dir: Path, baseline: Path | None = None) -> str:
    """Render a run report using shared report generator."""
    from fp_wraptr.analysis.report import build_run_report

    return build_run_report(run_dir, baseline_dir=baseline)


def _format_nodes(nodes: set[str] | list[str]) -> str:
    return ", ".join(sorted(nodes)) if nodes else "<none>"


def _load_model_dictionary(dictionary: Path | None = None):
    from fp_wraptr.data import ModelDictionary

    try:
        return ModelDictionary.load(dictionary)
    except FileNotFoundError as exc:
        console.print(f"[red]Dictionary file not found:[/red] {exc}")
        raise typer.Exit(code=1) from None
    except RuntimeError as exc:
        console.print(f"[red]Dictionary load failed:[/red] {exc}")
        raise typer.Exit(code=1) from None


def _load_source_map(source_map: Path | None = None):
    from fp_wraptr.data import load_source_map

    try:
        return load_source_map(source_map)
    except FileNotFoundError as exc:
        console.print(f"[red]Source map file not found:[/red] {exc}")
        raise typer.Exit(code=1) from None
    except RuntimeError as exc:
        console.print(f"[red]Source map load failed:[/red] {exc}")
        raise typer.Exit(code=1) from None


def _render_dictionary_search_csv(payload: dict) -> str:
    query = str(payload.get("query", ""))
    intent = payload.get("intent", {}) or {}
    focus = payload.get("focus", {}) or {}
    intent_kind = str(intent.get("kind", "generic"))
    present_in_equation = ""
    if intent_kind == "variable_in_equation":
        present = (
            focus.get("variable_in_equation", {}).get("present_in_equation")
            if isinstance(focus.get("variable_in_equation"), dict)
            else None
        )
        if isinstance(present, bool):
            present_in_equation = "true" if present else "false"

    rows: list[dict[str, str]] = []
    for match in payload.get("equation_matches", []):
        equation = match.get("equation", {}) or {}
        links = match.get("links", {}) or {}
        rows.append({
            "section": "equation",
            "query": query,
            "intent_kind": intent_kind,
            "score": str(match.get("score", "")),
            "reason": str(match.get("reason", "")),
            "equation_id": str(equation.get("id", "")),
            "variable_name": "",
            "label": str(equation.get("label", "")),
            "lhs_expr": str(equation.get("lhs_expr", "")),
            "description": "",
            "description_source": "",
            "defined_by_equation": "",
            "related_variables": ";".join(links.get("related_variables", [])),
            "present_in_equation": present_in_equation,
        })

    for match in payload.get("variable_matches", []):
        variable = match.get("variable", {}) or {}
        links = variable.get("links", {}) or {}
        rows.append({
            "section": "variable",
            "query": query,
            "intent_kind": intent_kind,
            "score": str(match.get("score", "")),
            "reason": str(match.get("reason", "")),
            "equation_id": "",
            "variable_name": str(variable.get("name", "")),
            "label": "",
            "lhs_expr": "",
            "description": str(variable.get("description", "")),
            "description_source": str(variable.get("description_source", "")),
            "defined_by_equation": str(variable.get("defined_by_equation", "")),
            "related_variables": ";".join(str(x) for x in links.get("used_in_equations", [])),
            "present_in_equation": present_in_equation,
        })

    headers = [
        "section",
        "query",
        "intent_kind",
        "score",
        "reason",
        "equation_id",
        "variable_name",
        "label",
        "lhs_expr",
        "description",
        "description_source",
        "defined_by_equation",
        "related_variables",
        "present_in_equation",
    ]
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=headers)
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue()


def _render_dictionary_equation_csv(payload: dict[str, Any]) -> str:
    equation = payload.get("equation", {}) or {}
    cross_links = payload.get("cross_links", {}) or {}
    variable_to_equations = cross_links.get("variable_to_equations", {}) or {}

    rows: list[dict[str, str]] = []
    for variable in payload.get("variables", []):
        name = str(variable.get("name", ""))
        variable_link = variable_to_equations.get(name, {}) or {}
        used_in = variable_link.get("used_in_equations")
        rows.append({
            "equation_id": str(equation.get("id", "")),
            "equation_type": str(equation.get("type", "")),
            "equation_label": str(equation.get("label", "")),
            "lhs_expr": str(equation.get("lhs_expr", "")),
            "formula": str(equation.get("formula", "")),
            "variable_name": name,
            "role": str(variable.get("role", "")),
            "description": str(variable.get("description", "")),
            "description_source": str(variable.get("description_source", "")),
            "category": str(variable.get("category", "")),
            "defined_by_equation": str(variable_link.get("defined_by_equation", "")),
            "used_in_equations": ";".join(str(x) for x in (used_in or [])),
        })

    headers = [
        "equation_id",
        "equation_type",
        "equation_label",
        "lhs_expr",
        "formula",
        "variable_name",
        "role",
        "description",
        "description_source",
        "category",
        "defined_by_equation",
        "used_in_equations",
    ]
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=headers)
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue()


@app.command("describe")
def describe(
    variable: Annotated[str, typer.Argument(help="Variable code (for example: GDP, UR, CS)")],
    dictionary: Annotated[
        Path | None,
        typer.Option("--dictionary", help="Override dictionary JSON path"),
    ] = None,
    format: Annotated[
        str,
        typer.Option("--format", "-f", help="Output format: table or json"),
    ] = "table",
) -> None:
    """Describe one dictionary variable."""
    model_dictionary = _load_model_dictionary(dictionary)
    record = model_dictionary.describe(variable.upper())
    if record is None:
        console.print(f"[red]Variable not found:[/red] {variable}")
        raise typer.Exit(code=1)

    selected_format = format.lower()
    if selected_format == "json":
        console.print_json(json.dumps(record, indent=2, default=str))
        return
    if selected_format != "table":
        console.print("[red]Unknown format. Use --format table or --format json.[/red]")
        raise typer.Exit(code=1)

    table = Table(title=f"Variable: {record['name']}")
    table.add_column("Field", style="cyan")
    table.add_column("Value")
    table.add_row("Description", str(record.get("description", "")))
    table.add_row("Units", str(record.get("units", "")))
    table.add_row("Sector", str(record.get("sector", "")))
    table.add_row("Category", str(record.get("category", "")))
    table.add_row("Defined by equation", str(record.get("defined_by_equation", "")))
    table.add_row(
        "Used in equations",
        ", ".join(str(x) for x in record.get("used_in_equations", [])) or "<none>",
    )
    table.add_row(
        "Raw data sources",
        ", ".join(record.get("raw_data_sources", [])) or "<none>",
    )
    table.add_row("Construction", str(record.get("construction", "") or "<none>"))
    console.print(table)


@dict_app.command("search")
def dictionary_search(
    query: Annotated[str, typer.Argument(help="Search text, variable code, or equation id")],
    dictionary: Annotated[
        Path | None,
        typer.Option("--dictionary", help="Override dictionary JSON path"),
    ] = None,
    limit: Annotated[
        int,
        typer.Option("--limit", "-n", help="Maximum matches per section"),
    ] = 10,
    format: Annotated[
        str,
        typer.Option("--format", "-f", help="Output format: table, json, or csv"),
    ] = "table",
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Write json/csv output to file"),
    ] = None,
    intent_diagnostics: Annotated[
        bool,
        typer.Option(
            "--intent-diagnostics",
            help="Emit only intent/focus diagnostics as JSON for scripted workflows",
        ),
    ] = False,
) -> None:
    """Search dictionary entries with deterministic ranking."""
    model_dictionary = _load_model_dictionary(dictionary)
    payload = model_dictionary.query(query, limit=limit)
    if intent_diagnostics:
        diagnostics = {
            "query": payload.get("query", ""),
            "intent": payload.get("intent", {}),
            "focus": payload.get("focus", {}),
        }
        rendered = json.dumps(diagnostics, indent=2, default=str)
        if output is not None:
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(rendered + "\n", encoding="utf-8")
            console.print(f"[green]Intent diagnostics written to:[/green] {output}")
            return
        console.print_json(rendered)
        return

    selected_format = format.lower()
    if output is not None and selected_format == "table":
        console.print("[red]--output requires --format json or --format csv.[/red]")
        raise typer.Exit(code=1)

    if selected_format == "json":
        rendered = json.dumps(payload, indent=2, default=str)
        if output is not None:
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(rendered + "\n", encoding="utf-8")
            console.print(f"[green]Search results written to:[/green] {output}")
            return
        console.print_json(rendered)
        return
    if selected_format == "csv":
        rendered = _render_dictionary_search_csv(payload)
        if output is not None:
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(rendered, encoding="utf-8")
            console.print(f"[green]Search results written to:[/green] {output}")
            return
        typer.echo(rendered.rstrip() + "\n")
        return
    if selected_format != "table":
        console.print("[red]Unknown format. Use --format table, json, or csv.[/red]")
        raise typer.Exit(code=1)

    eq_table = Table(title="Equation matches")
    eq_table.add_column("Eq", style="cyan")
    eq_table.add_column("Score")
    eq_table.add_column("Reason")
    eq_table.add_column("Label")
    for match in payload["equation_matches"]:
        eq = match["equation"]
        eq_table.add_row(
            str(eq.get("id", "")),
            str(match.get("score", "")),
            str(match.get("reason", "")),
            str(eq.get("label", "")),
        )
    if not payload["equation_matches"]:
        eq_table.add_row("<none>", "", "", "")
    console.print(eq_table)

    var_table = Table(title="Variable matches")
    var_table.add_column("Var", style="cyan")
    var_table.add_column("Score")
    var_table.add_column("Reason")
    var_table.add_column("Description")
    for match in payload["variable_matches"]:
        var = match["variable"]
        var_table.add_row(
            str(var.get("name", "")),
            str(match.get("score", "")),
            str(match.get("reason", "")),
            str(var.get("description", "")),
        )
    if not payload["variable_matches"]:
        var_table.add_row("<none>", "", "", "")
    console.print(var_table)


@dict_app.command("equation")
def dictionary_equation(
    equation_id: Annotated[int, typer.Argument(help="Equation id (for example: 82)")],
    dictionary: Annotated[
        Path | None,
        typer.Option("--dictionary", help="Override dictionary JSON path"),
    ] = None,
    format: Annotated[
        str,
        typer.Option("--format", "-f", help="Output format: table, json, or csv"),
    ] = "table",
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Write json/csv output to file"),
    ] = None,
) -> None:
    """Explain one equation and variables used in it."""
    model_dictionary = _load_model_dictionary(dictionary)
    payload = model_dictionary.explain_equation(equation_id)
    if payload is None:
        console.print(f"[red]Equation not found:[/red] {equation_id}")
        raise typer.Exit(code=1)

    selected_format = format.lower()
    if output is not None and selected_format == "table":
        console.print("[red]--output requires --format json or --format csv.[/red]")
        raise typer.Exit(code=1)

    if selected_format == "json":
        rendered = json.dumps(payload, indent=2, default=str)
        if output is not None:
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(rendered + "\n", encoding="utf-8")
            console.print(f"[green]Equation details written to:[/green] {output}")
            return
        console.print_json(rendered)
        return
    if selected_format == "csv":
        rendered = _render_dictionary_equation_csv(payload)
        if output is not None:
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(rendered, encoding="utf-8")
            console.print(f"[green]Equation details written to:[/green] {output}")
            return
        typer.echo(rendered.rstrip() + "\n")
        return
    if selected_format != "table":
        console.print("[red]Unknown format. Use --format table, json, or csv.[/red]")
        raise typer.Exit(code=1)

    equation = payload["equation"]
    header = Table(title=f"Equation {equation['id']}")
    header.add_column("Field", style="cyan")
    header.add_column("Value")
    header.add_row("Type", str(equation.get("type", "")))
    header.add_row("Label", str(equation.get("label", "")))
    header.add_row("LHS", str(equation.get("lhs_expr", "")))
    header.add_row("Formula", str(equation.get("formula", "")))
    console.print(header)

    var_table = Table(title=f"Equation {equation['id']} variables")
    var_table.add_column("Name", style="cyan")
    var_table.add_column("Role")
    var_table.add_column("Description")
    var_table.add_column("Source")
    for variable in payload["variables"]:
        var_table.add_row(
            str(variable.get("name", "")),
            str(variable.get("role", "")),
            str(variable.get("description", "")),
            str(variable.get("description_source", "")),
        )
    console.print(var_table)


@dict_app.command("sources")
def dictionary_sources(
    variable: Annotated[str, typer.Argument(help="Variable code (for example: GDP, UR, PCY)")],
    dictionary: Annotated[
        Path | None,
        typer.Option("--dictionary", help="Override dictionary JSON path"),
    ] = None,
    source_map: Annotated[
        Path | None,
        typer.Option("--source-map", help="Override source_map YAML path"),
    ] = None,
    format: Annotated[
        str,
        typer.Option("--format", "-f", help="Output format: table or json"),
    ] = "table",
) -> None:
    """Show expanded data-source mapping for one variable."""
    model_dictionary = _load_model_dictionary(dictionary)
    mapping = _load_source_map(source_map)
    payload = mapping.resolve_variable_sources(variable, dictionary=model_dictionary)

    selected_format = format.lower()
    if selected_format == "json":
        console.print_json(json.dumps(payload, indent=2, default=str))
        return
    if selected_format != "table":
        console.print("[red]Unknown format. Use --format table or --format json.[/red]")
        raise typer.Exit(code=1)

    table = Table(title=f"Data sources: {payload['variable']}")
    table.add_column("Field", style="cyan")
    table.add_column("Value")
    table.add_row("Mapping status", str(payload.get("mapping_status", "")))
    table.add_row(
        "Dictionary raw-data sources",
        ", ".join(payload.get("dictionary_raw_data_sources", [])) or "<none>",
    )

    source_entry = payload.get("source_map_entry")
    normalization = payload.get("normalization")
    if isinstance(source_entry, dict):
        table.add_row("Source map source", str(source_entry.get("source", "")))
        table.add_row("Series ID", str(source_entry.get("series_id", "")) or "<none>")
        table.add_row("Frequency", str(source_entry.get("frequency", "")))
        table.add_row("Transform", str(source_entry.get("transform", "")))
        table.add_row(
            "Annual rate",
            "yes" if bool(source_entry.get("annual_rate")) else "no",
        )
        if isinstance(normalization, dict):
            table.add_row(
                "Annual-rate divisor",
                str(normalization.get("annual_rate_divisor", "")) or "<none>",
            )
            table.add_row(
                "Per-period formula",
                str(normalization.get("per_period_formula", "")) or "<none>",
            )
            quarterly_formula = str(normalization.get("quarterly_flow_formula", ""))
            if quarterly_formula:
                table.add_row("Quarterly flow formula", quarterly_formula)
        table.add_row("Units", str(source_entry.get("units", "")))
        table.add_row("BEA table", str(source_entry.get("bea_table", "")) or "<none>")
        table.add_row("BEA line", str(source_entry.get("bea_line", "")) or "<none>")
        table.add_row("FRED fallback", str(source_entry.get("fred_fallback", "")) or "<none>")
    else:
        table.add_row("Source map entry", "<none>")
    console.print(table)

    raw_data_details = payload.get("dictionary_raw_data_details", [])
    raw_table = Table(title=f"Dictionary raw-data details: {payload['variable']}")
    raw_table.add_column("R#", style="cyan")
    raw_table.add_column("Variable")
    raw_table.add_column("Source type")
    raw_table.add_column("Description")
    if isinstance(raw_data_details, list) and raw_data_details:
        for item in raw_data_details:
            if not isinstance(item, dict):
                continue
            raw_table.add_row(
                str(item.get("r_number", "")),
                str(item.get("variable", "")),
                str(item.get("source_type", "")),
                str(item.get("description", "")),
            )
    else:
        raw_table.add_row("<none>", "", "", "")
    console.print(raw_table)


@dict_app.command("source-coverage")
def dictionary_source_coverage(
    dictionary: Annotated[
        Path | None,
        typer.Option("--dictionary", help="Override dictionary JSON path"),
    ] = None,
    source_map: Annotated[
        Path | None,
        typer.Option("--source-map", help="Override source_map YAML path"),
    ] = None,
    only_with_raw_data: Annotated[
        bool,
        typer.Option(
            "--only-with-raw-data",
            help="Scope coverage to dictionary variables that have raw_data_sources",
        ),
    ] = False,
    limit: Annotated[
        int,
        typer.Option(
            "--limit", "-n", help="Limit number of missing variables shown in table mode"
        ),
    ] = 25,
    format: Annotated[
        str,
        typer.Option("--format", "-f", help="Output format: table or json"),
    ] = "table",
) -> None:
    """Summarize source-map coverage against dictionary variables."""
    model_dictionary = _load_model_dictionary(dictionary)
    mapping = _load_source_map(source_map)

    if only_with_raw_data:
        variable_names = [
            rec.name for rec in model_dictionary.variables.values() if rec.raw_data_sources
        ]
        scope = "variables_with_raw_data"
    else:
        variable_names = list(model_dictionary.variables.keys())
        scope = "all_dictionary_variables"

    report = mapping.coverage_report(variable_names)
    report["scope"] = scope

    selected_format = format.lower()
    if selected_format == "json":
        console.print_json(json.dumps(report, indent=2, default=str))
        return
    if selected_format != "table":
        console.print("[red]Unknown format. Use --format table or --format json.[/red]")
        raise typer.Exit(code=1)

    summary = Table(title="Source-map coverage")
    summary.add_column("Metric", style="cyan")
    summary.add_column("Value")
    summary.add_row("Scope", scope)
    summary.add_row("Population", str(report.get("population_count", 0)))
    summary.add_row("Mapped", str(report.get("mapped_count", 0)))
    summary.add_row("Missing", str(report.get("missing_count", 0)))
    summary.add_row("Coverage %", str(report.get("coverage_pct", 0.0)))
    summary.add_row(
        "Mapped by source",
        ", ".join(
            f"{name}:{count}" for name, count in (report.get("mapped_by_source", {}) or {}).items()
        )
        or "<none>",
    )
    console.print(summary)

    missing = report.get("missing_variables", []) or []
    missing_table = Table(title=f"Missing variables (showing up to {max(1, limit)})")
    missing_table.add_column("Variable", style="cyan")
    if missing:
        for name in missing[: max(1, limit)]:
            missing_table.add_row(str(name))
    else:
        missing_table.add_row("<none>")
    console.print(missing_table)


@dict_app.command("source-quality")
def dictionary_source_quality(
    dictionary: Annotated[
        Path | None,
        typer.Option("--dictionary", help="Override dictionary JSON path"),
    ] = None,
    source_map: Annotated[
        Path | None,
        typer.Option("--source-map", help="Override source_map YAML path"),
    ] = None,
    only_with_raw_data: Annotated[
        bool,
        typer.Option(
            "--only-with-raw-data",
            help="Scope quality checks to dictionary variables that have raw_data_sources",
        ),
    ] = False,
    limit: Annotated[
        int,
        typer.Option("--limit", "-n", help="Limit findings shown in table mode"),
    ] = 25,
    format: Annotated[
        str,
        typer.Option("--format", "-f", help="Output format: table or json"),
    ] = "table",
) -> None:
    """Audit source-map entry quality and missing locator fields."""
    model_dictionary = _load_model_dictionary(dictionary)
    mapping = _load_source_map(source_map)

    if only_with_raw_data:
        variable_names = [
            rec.name for rec in model_dictionary.variables.values() if rec.raw_data_sources
        ]
        scope = "variables_with_raw_data"
    else:
        variable_names = list(model_dictionary.variables.keys())
        scope = "all_dictionary_variables"

    report = mapping.quality_report(variable_names)
    report["scope"] = scope

    selected_format = format.lower()
    if selected_format == "json":
        console.print_json(json.dumps(report, indent=2, default=str))
        return
    if selected_format != "table":
        console.print("[red]Unknown format. Use --format table or --format json.[/red]")
        raise typer.Exit(code=1)

    summary = Table(title="Source-map quality audit")
    summary.add_column("Metric", style="cyan")
    summary.add_column("Value")
    summary.add_row("Scope", scope)
    summary.add_row("Population", str(report.get("population_count", 0)))
    summary.add_row("Entries with issues", str(report.get("issue_count", 0)))
    summary.add_row("Entries clean", str(report.get("clean_count", 0)))
    summary.add_row(
        "Issue breakdown",
        ", ".join(
            f"{name}:{count}" for name, count in (report.get("issue_breakdown", {}) or {}).items()
        )
        or "<none>",
    )
    console.print(summary)

    findings = report.get("findings", []) or []
    finding_table = Table(title=f"Quality findings (showing up to {max(1, limit)})")
    finding_table.add_column("Variable", style="cyan")
    finding_table.add_column("Source")
    finding_table.add_column("Frequency")
    finding_table.add_column("Issues")
    if findings:
        for item in findings[: max(1, limit)]:
            if not isinstance(item, dict):
                continue
            finding_table.add_row(
                str(item.get("variable", "")),
                str(item.get("source", "")),
                str(item.get("frequency", "")),
                ", ".join(str(x) for x in item.get("issues", [])),
            )
    else:
        finding_table.add_row("<none>", "", "", "")
    console.print(finding_table)


@dict_app.command("source-report")
def dictionary_source_report(
    dictionary: Annotated[
        Path | None,
        typer.Option("--dictionary", help="Override dictionary JSON path"),
    ] = None,
    source_map: Annotated[
        Path | None,
        typer.Option("--source-map", help="Override source_map YAML path"),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Write JSON report to file"),
    ] = None,
    format: Annotated[
        str,
        typer.Option("--format", "-f", help="Output format: table or json"),
    ] = "table",
) -> None:
    """Build a deterministic source-map curation report."""
    model_dictionary = _load_model_dictionary(dictionary)
    mapping = _load_source_map(source_map)

    all_variables = list(model_dictionary.variables.keys())
    raw_variables = [
        rec.name for rec in model_dictionary.variables.values() if rec.raw_data_sources
    ]

    report = {
        "model_version": model_dictionary.model_version,
        "dictionary_variable_count": len(all_variables),
        "source_map_variable_count": len(mapping.list_variables()),
        "coverage_all": mapping.coverage_report(all_variables),
        "coverage_with_raw_data": mapping.coverage_report(raw_variables),
        "quality_all": mapping.quality_report(all_variables),
        "quality_with_raw_data": mapping.quality_report(raw_variables),
    }

    selected_format = format.lower()
    if selected_format == "json":
        rendered = json.dumps(report, indent=2, default=str)
        if output is not None:
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(rendered + "\n", encoding="utf-8")
            console.print(f"[green]Source-map report written to:[/green] {output}")
            return
        console.print_json(rendered)
        return
    if selected_format != "table":
        console.print("[red]Unknown format. Use --format table or --format json.[/red]")
        raise typer.Exit(code=1)
    if output is not None:
        console.print("[red]--output requires --format json.[/red]")
        raise typer.Exit(code=1)

    coverage_all = report["coverage_all"]
    coverage_raw = report["coverage_with_raw_data"]
    quality_all = report["quality_all"]
    quality_raw = report["quality_with_raw_data"]

    table = Table(title="Source-map report summary")
    table.add_column("Metric", style="cyan")
    table.add_column("All variables")
    table.add_column("Raw-data-linked vars")
    table.add_row(
        "Population", str(coverage_all["population_count"]), str(coverage_raw["population_count"])
    )
    table.add_row("Mapped", str(coverage_all["mapped_count"]), str(coverage_raw["mapped_count"]))
    table.add_row(
        "Missing", str(coverage_all["missing_count"]), str(coverage_raw["missing_count"])
    )
    table.add_row(
        "Coverage %", str(coverage_all["coverage_pct"]), str(coverage_raw["coverage_pct"])
    )
    table.add_row(
        "Quality issues", str(quality_all["issue_count"]), str(quality_raw["issue_count"])
    )
    console.print(table)


@dict_app.command("source-window-check")
def dictionary_source_window_check(
    source_map: Annotated[
        Path | None,
        typer.Option("--source-map", help="Override source_map YAML path"),
    ] = None,
    start: Annotated[
        str | None,
        typer.Option("--start", help="Observation start date (YYYY-MM-DD)"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", help="Observation end date (YYYY-MM-DD)"),
    ] = None,
    tolerance: Annotated[
        float,
        typer.Option("--tolerance", help="Absolute tolerance for outside-window deviation checks"),
    ] = 0.0,
    cache_dir: Annotated[
        Path | None,
        typer.Option("--cache-dir", help="Override FRED cache directory"),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Write JSON report to file"),
    ] = None,
    format: Annotated[
        str,
        typer.Option("--format", "-f", help="Output format: table or json"),
    ] = "table",
    limit: Annotated[
        int,
        typer.Option("--limit", help="Max number of detailed rows to print in table mode"),
    ] = 20,
) -> None:
    """Check source-map window assumptions against observed FRED data."""
    if importlib.util.find_spec("fredapi") is None:
        console.print("[red]fredapi is required for this command.[/red]")
        console.print("[yellow]Install with:[/yellow] `uv pip install fp-wraptr[fred]`")
        raise typer.Exit(code=1)

    mapping = _load_source_map(source_map)
    entries = mapping.windowed_fred_entries()
    series_ids = sorted({entry.series_id for _, entry in entries if entry.series_id})
    if not series_ids:
        report = {
            "series_checked": 0,
            "violation_count": 0,
            "status_breakdown": {},
            "tolerance": max(tolerance, 0.0),
            "checks": [],
            "requested_start": start or "",
            "requested_end": end or "",
        }
    else:
        from fp_wraptr.fred.ingest import fetch_series

        try:
            frame = fetch_series(series_ids, start=start, end=end, cache_dir=cache_dir)
        except ValueError as exc:
            console.print(f"[red]FRED configuration error:[/red] {exc}")
            raise typer.Exit(code=1) from None
        report = mapping.window_assumption_report(frame, tolerance=tolerance)
        report["requested_start"] = start or ""
        report["requested_end"] = end or ""

    selected_format = format.lower()
    if selected_format == "json":
        rendered = json.dumps(report, indent=2, default=str)
        if output is not None:
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(rendered + "\n", encoding="utf-8")
            console.print(f"[green]Source-window report written to:[/green] {output}")
            return
        console.print_json(rendered)
        return
    if selected_format != "table":
        console.print("[red]Unknown format. Use --format table or --format json.[/red]")
        raise typer.Exit(code=1)
    if output is not None:
        console.print("[red]--output requires --format json.[/red]")
        raise typer.Exit(code=1)

    summary = Table(title="Source-window assumption check")
    summary.add_column("Metric", style="cyan")
    summary.add_column("Value")
    summary.add_row("Series checked", str(report.get("series_checked", 0)))
    summary.add_row("Violations", str(report.get("violation_count", 0)))
    summary.add_row(
        "Status breakdown",
        ", ".join(
            f"{name}:{count}" for name, count in (report.get("status_breakdown", {}) or {}).items()
        )
        or "<none>",
    )
    summary.add_row("Tolerance", str(report.get("tolerance", max(tolerance, 0.0))))
    summary.add_row("Requested start", str(report.get("requested_start", "") or "<none>"))
    summary.add_row("Requested end", str(report.get("requested_end", "") or "<none>"))
    console.print(summary)

    checks = report.get("checks", []) or []
    detail = Table(title=f"Window checks (showing up to {max(1, limit)})")
    detail.add_column("Variable", style="cyan")
    detail.add_column("Series")
    detail.add_column("Status")
    detail.add_column("Outside pts")
    detail.add_column("Violations")
    detail.add_column("Max abs dev")
    detail.add_column("First violation")
    if checks:
        for item in checks[: max(1, limit)]:
            if not isinstance(item, dict):
                continue
            detail.add_row(
                str(item.get("variable", "")),
                str(item.get("series_id", "")),
                str(item.get("status", "")),
                str(item.get("outside_points", 0)),
                str(item.get("outside_violations", 0)),
                str(item.get("max_abs_deviation", 0.0)),
                str(item.get("first_violation_date", "") or "<none>"),
            )
    else:
        detail.add_row("<none>", "", "", "", "", "", "")
    console.print(detail)


@app.command()
def run(
    scenario: Annotated[Path, typer.Argument(help="Path to scenario YAML config")],
    baseline: Annotated[
        Path | None,
        typer.Option("--baseline", "-b", help="Baseline scenario for diff"),
    ] = None,
    backend: Annotated[
        str | None,
        typer.Option(
            "--backend",
            help="Execution backend override: fpexe, fppy, fp-r, or both",
        ),
    ] = None,
    fp_home: Annotated[
        Path | None,
        typer.Option("--fp-home", envvar="FP_HOME", help="Directory containing fp.exe"),
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", "-o", help="Output artifacts directory")
    ] = Path("artifacts"),
    fingerprint_lock: Annotated[
        Path | None,
        typer.Option("--fingerprint-lock", help="Optional input fingerprint lockfile for parity"),
    ] = None,
    with_drift: Annotated[
        bool,
        typer.Option("--with-drift", help="Enable bounded-drift guardrails (parity only)"),
    ] = False,
    gate_pabev_end: Annotated[
        str | None,
        typer.Option(
            "--gate-pabev-end",
            help="Optional parity gate end quarter (YYYY.Q) for --backend both",
        ),
    ] = None,
    parity_quick: Annotated[
        bool,
        typer.Option(
            "--parity-quick",
            help="Shortcut for gating parity to the scenario forecast_start quarter (fast smoke check)",
        ),
    ] = False,
    parity_lenient: Annotated[
        bool,
        typer.Option(
            "--parity-lenient", help="Exit 0 even on parity mismatches in --backend both mode"
        ),
    ] = False,
    allow_stale_output: Annotated[
        bool,
        typer.Option(
            "--allow-stale-output",
            help="Fall back to existing fmout.txt when the backend is unavailable",
        ),
    ] = False,
) -> None:
    """Run an FP scenario and optionally diff against a baseline."""
    from fp_wraptr.scenarios.runner import (
        backend_requires_fp_home,
        load_scenario_config,
        run_scenario,
        validate_fp_home,
    )

    try:
        config = load_scenario_config(scenario)
    except FileNotFoundError as exc:
        console.print(f"[red]Scenario file not found:[/red] {exc}")
        raise typer.Exit(code=1) from None
    except (ValidationError, yaml.YAMLError, ValueError) as exc:
        _print_validation_error(exc)
        raise typer.Exit(code=1) from None

    backend_choice = str(backend or getattr(config, "backend", "fpexe") or "fpexe").strip().lower()
    if fp_home:
        config.fp_home = fp_home
    if backend_requires_fp_home(backend_choice):
        validate_fp_home(config.fp_home)

    if backend_choice == "both":
        if baseline is not None:
            console.print("[red]--baseline is not supported with --backend both[/red]")
            raise typer.Exit(code=1)
        from fp_wraptr.analysis.parity import DriftConfig, GateConfig, run_parity

        effective_gate_end = gate_pabev_end
        if effective_gate_end is None and parity_quick:
            effective_gate_end = str(config.forecast_start)
        gate = GateConfig(
            pabev_end=effective_gate_end, drift=DriftConfig(enabled=bool(with_drift))
        )
        console.print(f"[bold]Running parity (fp.exe vs fp-py):[/bold] {config.name}")
        parity = run_parity(
            config,
            output_dir=output_dir,
            fp_home_override=config.fp_home,
            gate=gate,
            fingerprint_lock=fingerprint_lock,
        )
        console.print(f"[green]Parity completed.[/green] Output: {parity.run_dir}")
        _print_run_dir(parity.run_dir)
        seed_diag_suffix = _format_parity_seed_diagnostics(parity.run_dir)
        runtime_profile_suffix = _format_parity_runtime_profile(parity.run_dir)
        _print_fpexe_preflight_warning(parity)
        _print_fpexe_solution_error_warning(parity)
        console.print(
            f"status={parity.status} exit_code={parity.exit_code} "
            f"hard_fail_cell_count={parity.pabev_detail.get('hard_fail_cell_count', 0)} "
            f"max_abs_diff={parity.pabev_detail.get('max_abs_diff', 0.0)}"
            f"{seed_diag_suffix}"
            f"{runtime_profile_suffix}"
        )
        raw_exit_code = int(parity.exit_code)
        exit_code = _parity_cli_exit_code(raw_exit_code, lenient=bool(parity_lenient))
        _warn_if_parity_suppressed(
            parity_exit_code=raw_exit_code,
            cli_exit_code=exit_code,
            lenient=bool(parity_lenient),
        )
        raise typer.Exit(code=exit_code)

    config.backend = backend_choice
    console.print(f"[bold]Running scenario:[/bold] {config.name} (backend={backend_choice})")
    result = run_scenario(config, output_dir=output_dir, allow_stale_output=allow_stale_output)

    console.print(f"[green]Run completed.[/green] Output: {result.output_dir}")

    if baseline:
        from fp_wraptr.analysis.diff import diff_runs

        try:
            baseline_config = load_scenario_config(baseline)
        except FileNotFoundError as exc:
            console.print(f"[red]Baseline file not found:[/red] {exc}")
            raise typer.Exit(code=1) from None
        except (ValidationError, yaml.YAMLError, ValueError) as exc:
            _print_validation_error(exc)
            raise typer.Exit(code=1) from None

        if backend_requires_fp_home(getattr(baseline_config, "backend", "fpexe")):
            validate_fp_home(baseline_config.fp_home)
        baseline_result = run_scenario(
            baseline_config,
            output_dir=output_dir / "baseline",
            allow_stale_output=allow_stale_output,
        )
        summary = diff_runs(baseline_result, result)
        _print_diff_summary(summary)


@app.command("fpr-compare")
def fpr_compare(
    scenario: Annotated[Path, typer.Argument(help="Path to fp-r scenario YAML config")],
    expected: Annotated[
        Path | None,
        typer.Argument(help="Optional expected reduced-slice CSV, typically seeded from fppy"),
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", "-o", help="Output artifacts directory")
    ] = Path("artifacts"),
    atol: Annotated[
        float,
        typer.Option("--atol", help="Absolute comparison tolerance"),
    ] = 1.1e-3,
    rtol: Annotated[
        float,
        typer.Option("--rtol", help="Relative comparison tolerance"),
    ] = 1e-6,
) -> None:
    """Run a reduced fp-r slice and compare its emitted series to a seeded expectation."""
    from fp_wraptr.analysis.fpr_compare import (
        compare_fp_r_series_csv,
        write_fp_r_comparison_report,
    )
    from fp_wraptr.scenarios.runner import (
        backend_requires_fp_home,
        load_scenario_config,
        run_scenario,
        validate_fp_home,
    )

    try:
        config = load_scenario_config(scenario)
    except FileNotFoundError as exc:
        console.print(f"[red]Scenario file not found:[/red] {exc}")
        raise typer.Exit(code=1) from None
    except (ValidationError, yaml.YAMLError, ValueError) as exc:
        _print_validation_error(exc)
        raise typer.Exit(code=1) from None

    config.backend = "fp-r"
    if backend_requires_fp_home(config.backend):
        validate_fp_home(config.fp_home)

    expected_path = expected
    if expected_path is None:
        expected_raw = str((getattr(config, "fpr", {}) or {}).get("expected_csv", "")).strip()
        if expected_raw:
            expected_path = Path(expected_raw)
    if expected_path is None:
        console.print(
            "[red]Expected CSV is required via argument or fpr.expected_csv in the scenario YAML.[/red]"
        )
        raise typer.Exit(code=1)
    if not expected_path.is_absolute():
        expected_path = (scenario.parent / expected_path).resolve()
    if not expected_path.exists():
        console.print(f"[red]Expected CSV not found:[/red] {expected_path}")
        raise typer.Exit(code=1)

    console.print(f"[bold]Running fp-r shared-slice compare:[/bold] {config.name}")
    result = run_scenario(config, output_dir=output_dir)
    run_result = result.run_result
    if run_result is None or not run_result.success:
        console.print("[red]fp-r run did not complete successfully.[/red]")
        raise typer.Exit(code=1)

    actual_path = result.output_dir / "fp_r_series.csv"
    if not actual_path.exists():
        console.print(f"[red]fp-r series output not found:[/red] {actual_path}")
        raise typer.Exit(code=1)

    comparison = compare_fp_r_series_csv(actual_path, expected_path, atol=atol, rtol=rtol)
    report_path = write_fp_r_comparison_report(
        comparison,
        result.output_dir / "fp_r_compare_report.json",
    )
    _print_run_dir(result.output_dir)
    console.print(
        f"status={comparison.status} "
        f"mismatch_count={comparison.mismatch_count} "
        f"max_abs_diff={comparison.max_abs_diff} "
        f"compared_periods={comparison.compared_periods} "
        f"variables={','.join(comparison.compared_variables)}"
    )
    console.print(f"[green]Comparison report:[/green] {report_path}")
    raise typer.Exit(code=0 if comparison.status == "ok" else 2)


@app.command("parity")
def parity(
    scenario: Annotated[Path, typer.Argument(help="Path to scenario YAML config")],
    left: Annotated[
        str,
        typer.Option("--left", help="Left comparison engine: fpexe, fppy, or fp-r"),
    ] = "fpexe",
    right: Annotated[
        str,
        typer.Option("--right", help="Right comparison engine: fpexe, fppy, or fp-r"),
    ] = "fppy",
    fp_home: Annotated[
        Path | None,
        typer.Option("--fp-home", envvar="FP_HOME", help="Directory containing fp.exe"),
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", "-o", help="Output artifacts directory")
    ] = Path("artifacts"),
    fingerprint_lock: Annotated[
        Path | None,
        typer.Option("--fingerprint-lock", help="Optional input fingerprint lockfile"),
    ] = None,
    with_drift: Annotated[
        bool,
        typer.Option("--with-drift", help="Enable bounded-drift guardrails"),
    ] = False,
    gate_pabev_end: Annotated[
        str | None,
        typer.Option("--gate-pabev-end", help="Optional parity gate end quarter (YYYY.Q)"),
    ] = None,
    quick: Annotated[
        bool,
        typer.Option(
            "--quick",
            help="Shortcut for gating parity to the scenario forecast_start quarter (fast smoke check)",
        ),
    ] = False,
    lenient: Annotated[
        bool,
        typer.Option("--lenient", help="Exit 0 even on parity gate/hard-fail mismatches"),
    ] = False,
    save_golden: Annotated[
        Path | None,
        typer.Option(
            "--save-golden",
            help="Directory root where parity golden artifacts are written",
        ),
    ] = None,
    regression: Annotated[
        Path | None,
        typer.Option(
            "--regression",
            help="Directory root of saved parity golden artifacts for regression check",
        ),
    ] = None,
) -> None:
    """Run two engines and write a parity report."""
    from fp_wraptr.analysis.parity import (
        DriftConfig,
        GateConfig,
        format_parity_engine_pair,
        normalize_parity_engine_name,
        run_parity,
    )
    from fp_wraptr.analysis.parity_regression import (
        compare_parity_to_golden,
        save_parity_golden,
        write_regression_report,
    )
    from fp_wraptr.scenarios.runner import (
        backend_requires_fp_home,
        load_scenario_config,
        validate_fp_home,
    )

    if save_golden is not None and regression is not None:
        console.print(
            "[red]--save-golden and --regression cannot be used together in one run.[/red]"
        )
        raise typer.Exit(code=1)

    try:
        config = load_scenario_config(scenario)
    except FileNotFoundError as exc:
        console.print(f"[red]Scenario file not found:[/red] {exc}")
        raise typer.Exit(code=1) from None
    except (ValidationError, yaml.YAMLError, ValueError) as exc:
        _print_validation_error(exc)
        raise typer.Exit(code=1) from None

    try:
        left_engine = normalize_parity_engine_name(left)
        right_engine = normalize_parity_engine_name(right)
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from None
    if left_engine == right_engine:
        console.print("[red]Parity requires two distinct engines.[/red]")
        raise typer.Exit(code=1)
    default_pair = (left_engine, right_engine) == ("fpexe", "fppy")

    if fp_home:
        config.fp_home = fp_home
    if any(backend_requires_fp_home(name) for name in (left_engine, right_engine)):
        validate_fp_home(config.fp_home)

    effective_gate_end = gate_pabev_end
    if effective_gate_end is None and quick:
        effective_gate_end = str(config.forecast_start)
    gate = GateConfig(pabev_end=effective_gate_end, drift=DriftConfig(enabled=bool(with_drift)))
    console.print(
        f"[bold]Running parity ({format_parity_engine_pair(left_engine, right_engine)}):[/bold] "
        f"{config.name}"
    )
    run_parity_kwargs: dict[str, Any] = {
        "config": config,
        "output_dir": output_dir,
        "fp_home_override": config.fp_home,
        "gate": gate,
        "fingerprint_lock": fingerprint_lock,
    }
    if not default_pair:
        run_parity_kwargs["left_engine"] = left_engine
        run_parity_kwargs["right_engine"] = right_engine
    result = run_parity(
        **run_parity_kwargs,
    )
    console.print(f"[green]Parity completed.[/green] Output: {result.run_dir}")
    _print_run_dir(result.run_dir)
    seed_diag_suffix = _format_parity_seed_diagnostics(result.run_dir)
    runtime_profile_suffix = _format_parity_runtime_profile(result.run_dir)
    _print_fpexe_preflight_warning(result)
    _print_fpexe_solution_error_warning(result)
    console.print(
        f"status={result.status} exit_code={result.exit_code} "
        f"hard_fail_cell_count={result.pabev_detail.get('hard_fail_cell_count', 0)} "
        f"max_abs_diff={result.pabev_detail.get('max_abs_diff', 0.0)}"
        f"{seed_diag_suffix}"
        f"{runtime_profile_suffix}"
    )
    raw_exit_code = int(result.exit_code)
    exit_code = _parity_cli_exit_code(raw_exit_code, lenient=bool(lenient))
    _warn_if_parity_suppressed(
        parity_exit_code=raw_exit_code,
        cli_exit_code=exit_code,
        lenient=bool(lenient),
    )

    if save_golden is not None:
        try:
            saved_dir = save_parity_golden(Path(result.run_dir), Path(save_golden))
        except (FileNotFoundError, ValueError) as exc:
            console.print(f"[red]Failed to save parity golden:[/red] {exc}")
            _print_run_dir(result.run_dir)
            raise typer.Exit(code=1) from None
        console.print(f"[green]Saved parity golden:[/green] {saved_dir}")

    if regression is not None:
        try:
            regression_payload = compare_parity_to_golden(Path(result.run_dir), Path(regression))
            regression_path = write_regression_report(regression_payload, Path(result.run_dir))
        except (FileNotFoundError, ValueError) as exc:
            console.print(f"[red]Parity regression compare failed:[/red] {exc}")
            _print_run_dir(result.run_dir)
            raise typer.Exit(code=1) from None

        counts = regression_payload.get("counts", {})
        console.print(
            "regression_status="
            f"{regression_payload.get('status', 'failed')} "
            f"new_missing_left={counts.get('new_missing_left', 0)} "
            f"new_missing_right={counts.get('new_missing_right', 0)} "
            f"new_hard_fail_cells={counts.get('new_hard_fail_cells', 0)} "
            f"new_diff_variables={counts.get('new_diff_variables', 0)}"
        )
        console.print(f"[green]Regression report:[/green] {regression_path}")
        if regression_payload.get("status") != "ok":
            # Reserve code 6 for parity regression failures against golden baselines.
            if exit_code == 0:
                exit_code = 6

    raise typer.Exit(code=exit_code)


@app.command()
def validate(
    scenario: Annotated[Path, typer.Argument(help="Path to scenario YAML config")],
) -> None:
    """Validate a scenario YAML file without running fp.exe."""
    from fp_wraptr.scenarios.runner import load_scenario_config

    try:
        config = load_scenario_config(scenario)
    except FileNotFoundError as exc:
        console.print(f"[red]Scenario file not found:[/red] {exc}")
        raise typer.Exit(code=1) from None
    except (ValidationError, yaml.YAMLError, ValueError) as exc:
        _print_validation_error(exc)
        raise typer.Exit(code=1) from None

    table = Table(title="Scenario Summary")
    table.add_column("Field", style="cyan")
    table.add_column("Value")
    table.add_row("Name", config.name)
    table.add_row("Description", config.description or "<none>")
    table.add_row("Override count", str(len(config.overrides)))
    table.add_row("Track variables", ", ".join(config.track_variables) or "<none>")
    table.add_row("Forecast range", f"{config.forecast_start} to {config.forecast_end}")
    console.print(table)


@app.command("graph")
def graph(
    input_file: Annotated[
        Path,
        typer.Argument(help="Path to FP input file (fminput.txt)"),
    ],
    variable: Annotated[
        str | None,
        typer.Option("--variable", "-v", help="Show upstream and downstream for a variable"),
    ] = None,
    export: Annotated[
        str | None,
        typer.Option("--export", help="Export format for dependencies (dot)"),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option("--output", help="Export path for graph output"),
    ] = None,
) -> None:
    """Build a dependency graph from FP equations/identities/GENR commands."""
    if importlib.util.find_spec("networkx") is None:
        console.print("[red]NetworkX is required for graph features.[/red]")
        console.print("[yellow]Install with:[/yellow] `uv pip install fp-wraptr[graph]`")
        raise typer.Exit(code=1)

    from fp_wraptr.analysis.graph import (
        build_dependency_graph,
        get_downstream,
        get_upstream,
        summarize_graph,
    )
    from fp_wraptr.io.input_parser import parse_fp_input

    parsed = parse_fp_input(input_file)
    dependency_graph = build_dependency_graph(parsed)
    summary = summarize_graph(dependency_graph)

    table = Table(title="Dependency graph summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", justify="left")
    table.add_row("Nodes", str(summary["nodes"]))
    table.add_row("Edges", str(summary["edges"]))
    table.add_row("Roots", ", ".join(summary["roots"]) or "<none>")
    table.add_row("Leaves", ", ".join(summary["leaves"]) or "<none>")
    table.add_row("Most connected", ", ".join(summary["most_connected"]) or "<none>")
    console.print(table)

    if variable:
        variable = variable.upper()
        if variable not in dependency_graph:
            console.print(f"[red]Variable not found: {variable}[/red]")
            raise typer.Exit(code=1)

        detail = Table(title=f"{variable} dependencies")
        detail.add_column("Direction", style="cyan")
        detail.add_column("Variables", justify="left")
        detail.add_row("Upstream", _format_nodes(get_upstream(dependency_graph, variable)))
        detail.add_row(
            "Downstream",
            _format_nodes(get_downstream(dependency_graph, variable)),
        )
        console.print(detail)

    if export:
        if export.lower() != "dot":
            console.print("[red]Unsupported format. Use --export dot.[/red]")
            raise typer.Exit(code=1)

        output = output or input_file.with_name(f"{input_file.stem}_dependency_graph.dot")
        _export_dot(dependency_graph, output)
        console.print(f"[green]Dependency graph exported:[/green] {output}")


@app.command()
def batch(
    scenarios: Annotated[
        list[Path],
        typer.Argument(help="Scenario YAML files or wildcard path"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", "-o", help="Output artifacts directory"),
    ] = Path("artifacts"),
) -> None:
    """Run multiple scenarios in sequence."""
    from fp_wraptr.scenarios.batch import run_batch
    from fp_wraptr.scenarios.runner import load_scenario_config

    paths = _expand_scenario_paths(scenarios)
    if not paths:
        console.print("[red]No scenario files found for batch run.[/red]")
        raise typer.Exit(code=1)

    configs = []
    for path in paths:
        try:
            config = load_scenario_config(path)
        except Exception as exc:
            console.print(f"[red]Unable to read scenario {path}:[/red] {exc}")
            raise typer.Exit(code=1) from exc
        configs.append(config)

    results = run_batch(configs, output_dir=output_dir)

    table = Table(title="Batch run results")
    table.add_column("Scenario")
    table.add_column("Output")
    table.add_column("Success")
    for result in results:
        table.add_row(
            result.config.name,
            str(result.output_dir),
            "yes" if result.success else "no",
        )
    console.print(table)


@app.command()
def report(
    run_dir: Annotated[
        Path,
        typer.Argument(help="Completed run directory"),
    ],
    baseline: Annotated[
        Path | None,
        typer.Option("--baseline", help="Optional baseline run directory"),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option("--output", help="Write markdown report to file"),
    ] = None,
) -> None:
    """Render a markdown report for a completed run."""
    from rich.markdown import Markdown

    if not run_dir.exists():
        console.print(f"[red]Run directory not found:[/red] {run_dir}")
        raise typer.Exit(code=1)

    report_markdown = _render_report(run_dir, baseline)
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(report_markdown, encoding="utf-8")
        console.print(f"[green]Report written to:[/green] {output}")
        return

    console.print(Markdown(report_markdown))


@app.command()
def dashboard(
    artifacts_dir: Annotated[
        Path,
        typer.Option("--artifacts-dir", help="Artifacts directory containing fp-wraptr runs"),
    ] = Path("artifacts"),
    port: Annotated[
        int,
        typer.Option("--port", help="Streamlit server port"),
    ] = 8501,
) -> None:
    """Launch the fp-wraptr Streamlit dashboard."""
    if importlib.util.find_spec("streamlit") is None:
        console.print("[red]streamlit is not installed.[/red]")
        console.print("[yellow]Install with:[/yellow] `uv pip install fp-wraptr[dashboard]`")
        raise typer.Exit(code=1)

    cli_dir = Path(__file__).resolve().parent
    app_path = cli_dir.parents[1] / "apps" / "dashboard" / "Run_Manager.py"
    if not app_path.exists():
        app_path = Path.cwd() / "apps" / "dashboard" / "Run_Manager.py"
    if not app_path.exists():
        app_path = cli_dir.parents[1] / "apps" / "dashboard" / "app.py"
    if not app_path.exists():
        app_path = Path.cwd() / "apps" / "dashboard" / "app.py"

    if not app_path.exists():
        console.print(f"[red]Dashboard app not found at: {app_path}[/red]")
        raise typer.Exit(code=1)

    console.print(
        f"[bold blue]Launching dashboard:[/bold blue] {app_path} "
        f"(artifacts_dir={artifacts_dir}, port={port})"
    )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            str(app_path),
            "--server.port",
            str(port),
            "--",
            "--artifacts-dir",
            str(artifacts_dir),
        ],
        check=False,
    )
    if result.returncode != 0:
        raise typer.Exit(code=result.returncode)


@data_app.command("fetch-fair-bundle")
def data_fetch_fair_bundle(
    out_dir: Annotated[
        Path,
        typer.Option("--out-dir", help="Target directory for downloaded/unpacked bundle"),
    ] = ...,
    url: Annotated[
        str,
        typer.Option("--url", help="Fair bundle URL"),
    ] = "https://fairmodel.econ.yale.edu/fp/FMFP.ZIP",
    timeout_seconds: Annotated[
        int,
        typer.Option("--timeout-seconds", help="Download timeout seconds"),
    ] = 60,
    zip_path: Annotated[
        Path | None,
        typer.Option("--zip-path", help="Optional local FMFP.ZIP path (skip download)"),
    ] = None,
    fp_exe_from: Annotated[
        Path | None,
        typer.Option(
            "--fp-exe-from",
            help="Optional file/dir source for fp.exe to copy into fetched model dir",
        ),
    ] = None,
) -> None:
    """Download and unpack the official Fair FP quarterly bundle."""
    from fp_wraptr.data.fair_bundle import (
        FairBundleError,
        ensure_fp_exe_in_model_dir,
        fetch_and_unpack_fair_bundle,
    )

    try:
        payload = fetch_and_unpack_fair_bundle(
            out_dir=out_dir,
            url=url,
            timeout_seconds=int(timeout_seconds),
            zip_path=zip_path,
        )
    except FairBundleError as exc:
        console.print(f"[red]Fair bundle fetch failed:[/red] {exc}")
        raise typer.Exit(code=1) from None

    console.print(f"[green]Bundle output:[/green] {payload.get('output_dir')}")
    console.print(f"[green]Model dir:[/green] {payload.get('model_dir')}")
    console.print(f"[green]Manifest:[/green] {payload.get('manifest_path')}")
    model_dir = Path(str(payload.get("model_dir", "")).strip())
    try:
        fp_exe_status = ensure_fp_exe_in_model_dir(model_dir=model_dir, fp_exe_from=fp_exe_from)
    except FairBundleError as exc:
        console.print(f"[red]Fair bundle fetch failed:[/red] {exc}")
        raise typer.Exit(code=1) from None
    if bool(fp_exe_status.get("present")) and bool(fp_exe_status.get("copied")):
        console.print(
            f"[green]Copied fp.exe:[/green] {fp_exe_status.get('source')} -> {fp_exe_status.get('target')}"
        )
    elif bool(fp_exe_status.get("present")):
        console.print(f"[green]fp.exe available:[/green] {fp_exe_status.get('target')}")
    else:
        console.print(
            "[yellow]Warning:[/yellow] fetched bundle does not include `fp.exe`; "
            "fp.exe/parity runs will fail until you provide it."
        )
        console.print(
            "[yellow]Operator action:[/yellow] rerun with "
            "`--fp-exe-from <path-to-existing-fp.exe-or-fp-home>`."
        )
    _print_run_dir(payload.get("model_dir"))


@data_app.command("update-fred")
def data_update_fred(
    model_dir: Annotated[
        Path,
        typer.Option(
            "--model-dir", help="Base model directory (contains fmdata.txt, fmexog.txt, etc.)"
        ),
    ] = Path("FM"),
    out_dir: Annotated[
        Path,
        typer.Option("--out-dir", help="Output directory for updated bundle/report"),
    ] = ...,
    end: Annotated[
        str | None,
        typer.Option("--end", help="Update end period in YYYY.Q format (for example 2025.4)"),
    ] = None,
    source_map: Annotated[
        Path | None,
        typer.Option("--source-map", help="Override source-map YAML path"),
    ] = None,
    cache_dir: Annotated[
        Path | None,
        typer.Option(
            "--cache-dir",
            help="Optional FRED cache directory override (BEA/BLS use sibling caches under the same root).",
        ),
    ] = None,
    sources: Annotated[
        list[str] | None,
        typer.Option(
            "--sources",
            help="Enabled sources (repeatable). Supported: fred, bea, bls. Default: fred",
        ),
    ] = None,
    variables: Annotated[
        list[str] | None,
        typer.Option(
            "--variables",
            help="Optional FP variables to update (repeat option: --variables UR --variables GDPR)",
        ),
    ] = None,
    replace_history: Annotated[
        bool,
        typer.Option(
            "--replace-history/--no-replace-history", help="Replace historical values too"
        ),
    ] = False,
    extend_sample: Annotated[
        bool,
        typer.Option(
            "--extend-sample/--no-extend-sample", help="Extend fmdata sample_end to --end"
        ),
    ] = False,
    allow_carry_forward: Annotated[
        bool,
        typer.Option(
            "--allow-carry-forward/--no-allow-carry-forward",
            help="When extending sample, fill missing series by carrying forward prior values",
        ),
    ] = False,
    patch_fminput_smpl_endpoints: Annotated[
        bool,
        typer.Option(
            "--patch-fminput-smpl-endpoints/--no-patch-fminput-smpl-endpoints",
            help="(Experimental) When extending sample, patch the single SMPL endpoint in effect for `LOADDATA FILE=FMDATA.TXT;` in fminput.txt so fp.exe actually loads the new history quarter",
        ),
    ] = False,
    keyboard_augment: Annotated[
        list[str] | None,
        typer.Option(
            "--keyboard-augment",
            help="Extra targets to append to the KEYBOARD list in fminput.txt (repeat option). If omitted and --extend-sample is set, defaults to RM/RMA/RMMRSL2/RMACDZ to avoid known rate-chain drift.",
        ),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="Optional FRED observation start date (YYYY-MM-DD)"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="Optional FRED observation end date (YYYY-MM-DD)"),
    ] = None,
    from_official_bundle: Annotated[
        bool,
        typer.Option(
            "--from-official-bundle/--no-from-official-bundle",
            help="Use freshly downloaded official Fair bundle as base model dir before update",
        ),
    ] = False,
    base_dir: Annotated[
        Path | None,
        typer.Option("--base-dir", help="Optional directory for official bundle base"),
    ] = None,
    official_bundle_url: Annotated[
        str,
        typer.Option("--official-bundle-url", help="Official Fair bundle URL"),
    ] = "https://fairmodel.econ.yale.edu/fp/FMFP.ZIP",
    official_bundle_zip_path: Annotated[
        Path | None,
        typer.Option(
            "--official-bundle-zip-path",
            help="Optional local FMFP.ZIP path for --from-official-bundle (skip download)",
        ),
    ] = None,
) -> None:
    """Build a new `FM/` bundle with updated `fmdata.txt` from external source mappings."""

    enabled_sources = [str(s).strip().lower() for s in (sources or ["fred"]) if str(s).strip()]
    enabled_sources = list(dict.fromkeys(enabled_sources)) or ["fred"]
    if "fred" in enabled_sources and importlib.util.find_spec("fredapi") is None:
        console.print("[red]fredapi is required for FRED-backed updates.[/red]")
        console.print("[yellow]Install with:[/yellow] `uv pip install fp-wraptr[fred]`")
        raise typer.Exit(code=1)

    from fp_wraptr.data.fair_bundle import FairBundleError, fetch_and_unpack_fair_bundle
    from fp_wraptr.data.update_fred import DataUpdateError, update_model_from_fred
    from fp_wraptr.io.input_parser import parse_fm_data

    effective_model_dir = Path(model_dir)
    if from_official_bundle:
        bundle_out_dir = (
            Path(base_dir) if base_dir is not None else (Path(out_dir) / "_official_base")
        )
        try:
            bundle_payload = fetch_and_unpack_fair_bundle(
                out_dir=bundle_out_dir,
                url=official_bundle_url,
                zip_path=official_bundle_zip_path,
            )
        except FairBundleError as exc:
            console.print(f"[red]Data update failed:[/red] Could not fetch official bundle: {exc}")
            raise typer.Exit(code=1) from None
        model_dir_token = str(bundle_payload.get("model_dir", "")).strip()
        if not model_dir_token:
            console.print(
                "[red]Data update failed:[/red] Official bundle fetch returned no model_dir."
            )
            raise typer.Exit(code=1)
        effective_model_dir = Path(model_dir_token)
        console.print(
            f"[blue]Info:[/blue] using official bundle base model_dir={effective_model_dir}"
        )

    selected_end = str(end).strip() if isinstance(end, str) and str(end).strip() else None
    if selected_end is None:
        fmdata_path = effective_model_dir / "fmdata.txt"
        try:
            parsed_fmdata = parse_fm_data(fmdata_path)
        except Exception as exc:
            console.print(f"[red]Data update failed:[/red] Unable to parse {fmdata_path}: {exc}")
            raise typer.Exit(code=1) from None
        inferred_end = str(parsed_fmdata.get("sample_end", "")).strip()
        if not inferred_end:
            console.print(
                f"[red]Data update failed:[/red] Could not infer --end from {fmdata_path} sample_end."
            )
            raise typer.Exit(code=1)
        selected_end = inferred_end
        console.print(f"[blue]Info:[/blue] defaulting end={selected_end}")

    keyboard_augment_targets: tuple[str, ...] = tuple()
    if keyboard_augment:
        keyboard_augment_targets = tuple(
            str(name).strip().upper() for name in keyboard_augment if str(name).strip()
        )
    elif extend_sample:
        keyboard_augment_targets = ("RM", "RMA", "RMMRSL2", "RMACDZ")

    try:
        result = update_model_from_fred(
            model_dir=effective_model_dir,
            out_dir=out_dir,
            end_period=selected_end,
            source_map_path=source_map,
            cache_dir=cache_dir,
            variables=variables,
            sources=enabled_sources,
            replace_history=replace_history,
            extend_sample=extend_sample,
            allow_carry_forward=allow_carry_forward,
            patch_fminput_smpl_endpoints=patch_fminput_smpl_endpoints,
            keyboard_augment_targets=keyboard_augment_targets,
            start_date=start_date,
            end_date=end_date,
        )
    except DataUpdateError as exc:
        console.print(f"[red]Data update failed:[/red] {exc}")
        raise typer.Exit(code=1) from None
    except ValueError as exc:
        console.print(f"[red]Data update failed:[/red] {exc}")
        raise typer.Exit(code=1) from None

    report = result.report
    console.print(f"[green]Updated bundle:[/green] {result.model_bundle_dir}")
    console.print(f"[green]Updated fmdata:[/green] {result.fmdata_path}")
    console.print(f"[green]Report:[/green] {result.report_path}")
    merge = report.get("fmdata_merge")
    merge_cells = "n/a"
    if isinstance(merge, dict):
        merge_cells = (
            f"{merge.get('updated_cells', 0)} updated, {merge.get('carried_cells', 0)} carried"
        )
    console.print(
        "[blue]Summary:[/blue] "
        f"variables_selected={report.get('selected_variable_count', 0)} "
        f"variables_normalized={report.get('normalized_variable_count', 0)} "
        f"cells={merge_cells}"
    )
    if from_official_bundle:
        target_fp_exe = Path(result.model_bundle_dir) / "fp.exe"
        if not target_fp_exe.exists():
            source_fp_exe = Path(model_dir) / "fp.exe"
            if source_fp_exe.is_file():
                try:
                    shutil.copy2(source_fp_exe, target_fp_exe)
                    console.print(
                        f"[green]Copied fp.exe:[/green] {source_fp_exe} -> {target_fp_exe}"
                    )
                except Exception as exc:
                    console.print(
                        "[yellow]Warning:[/yellow] official-base output bundle is missing `fp.exe` "
                        f"and copy from `{source_fp_exe}` failed: {exc}"
                    )
            else:
                console.print(
                    "[yellow]Warning:[/yellow] official-base output bundle is missing `fp.exe` "
                    f"and source `{source_fp_exe}` was not found."
                )
                console.print(
                    "[yellow]Operator action:[/yellow] copy `fp.exe` into the output FM directory "
                    "before running `--backend fpexe` or parity."
                )
    console.print(
        "[blue]Recommended forecast window:[/blue] "
        f"{report.get('recommended_forecast_start', 'n/a')}..{report.get('recommended_forecast_end', 'n/a')}"
    )
    sample_end_after = report.get("sample_end_after")
    fminput_load_end = report.get("fminput_fmdata_load_end")
    if (
        isinstance(sample_end_after, str)
        and isinstance(fminput_load_end, str)
        and sample_end_after.strip()
        and fminput_load_end.strip()
        and sample_end_after.strip() != fminput_load_end.strip()
    ):
        console.print(
            "[yellow]Warning:[/yellow] fmdata sample end and fminput LOADDATA end differ "
            f"(`sample_end_after={sample_end_after}`, `fminput_fmdata_load_end={fminput_load_end}`)."
        )
        console.print(
            "[yellow]Operator action:[/yellow] Expect parity mismatches until the model's fminput "
            "is updated to load the extra history quarter."
        )
    templates = report.get("scenario_templates")
    if isinstance(templates, dict) and templates:
        console.print("[blue]Scenario templates:[/blue]")
        for key in ("baseline_yaml", "baseline_smoke_yaml", "readme"):
            if key in templates:
                console.print(f"  - {key}: {templates.get(key)}")


@data_app.command("diff-fmdata")
def data_diff_fmdata(
    base_fmdata: Annotated[
        Path,
        typer.Option("--base-fmdata", help="Baseline fmdata.txt path"),
    ] = ...,
    updated_fmdata: Annotated[
        Path,
        typer.Option("--updated-fmdata", help="Updated fmdata.txt path"),
    ] = ...,
    data_update_report: Annotated[
        Path | None,
        typer.Option(
            "--data-update-report",
            help="Optional data_update_report.json for updated-vs-carried classification",
        ),
    ] = None,
    scope: Annotated[
        str,
        typer.Option(
            "--scope",
            help="Comparison scope: sample_end, update_window, or all",
        ),
    ] = "sample_end",
    start_period: Annotated[
        str | None,
        typer.Option("--start-period", help="Optional explicit start period (YYYY.Q)"),
    ] = None,
    end_period: Annotated[
        str | None,
        typer.Option("--end-period", help="Optional explicit end period (YYYY.Q)"),
    ] = None,
    format: Annotated[
        str,
        typer.Option("--format", help="Output format: table or json"),
    ] = "table",
    limit: Annotated[
        int,
        typer.Option("--limit", help="Max rows in table output (<=0 means show all)"),
    ] = 50,
) -> None:
    """Compare two fmdata files and report changed variables in a selected window."""
    normalized_format = str(format).strip().lower()
    if normalized_format not in {"table", "json"}:
        console.print("[red]--format must be one of: table, json[/red]")
        raise typer.Exit(code=1)

    from fp_wraptr.io.fmdata_diff import FmdataDiffError, diff_fmdata_files

    try:
        payload = diff_fmdata_files(
            base_fmdata=base_fmdata,
            updated_fmdata=updated_fmdata,
            scope=scope,
            start_period=start_period,
            end_period=end_period,
            data_update_report=data_update_report,
        )
    except FmdataDiffError as exc:
        console.print(f"[red]fmdata diff failed:[/red] {exc}")
        raise typer.Exit(code=1) from None

    if normalized_format == "json":
        typer.echo(json.dumps(payload, indent=2, sort_keys=True))
        return

    records = payload.get("records")
    if not isinstance(records, list):
        records = []

    def _fmt_delta(value: Any) -> str:
        if value is None:
            return "n/a"
        try:
            number = float(value)
        except Exception:
            return str(value)
        return f"{number:.6g}"

    window_start = payload.get("window_start", "n/a")
    window_end = payload.get("window_end", "n/a")
    table = Table(title=f"fmdata diff ({window_start}..{window_end})")
    table.add_column("Variable")
    table.add_column("Change type")
    table.add_column("Sample-end |delta|", justify="right")
    table.add_column("Max |delta|", justify="right")
    table.add_column("Changed periods", justify="right")
    table.add_column("Range")

    display_records = records if limit <= 0 else records[:limit]
    for item in display_records:
        variable = str(item.get("variable", ""))
        change_type = str(item.get("change_type", "unknown"))
        changed_count = int(item.get("changed_period_count", 0) or 0)
        first_period = str(item.get("first_changed_period", "") or "")
        last_period = str(item.get("last_changed_period", "") or "")
        changed_range = f"{first_period}..{last_period}" if first_period and last_period else ""
        table.add_row(
            variable,
            change_type,
            _fmt_delta(item.get("sample_end_abs_delta")),
            _fmt_delta(item.get("max_abs_delta")),
            str(changed_count),
            changed_range,
        )

    console.print(table)
    total = int(payload.get("changed_variable_count", 0) or 0)
    if limit > 0 and len(records) > limit:
        console.print(f"[yellow]Showing {limit} of {len(records)} changed variables.[/yellow]")
    console.print(
        "[blue]Summary:[/blue] "
        f"changed_variables={total} "
        f"scope={payload.get('scope', 'n/a')} "
        f"window={window_start}..{window_end}"
    )


@data_app.command("check-fred-mapping")
def data_check_fred_mapping(
    model_dir: Annotated[
        Path,
        typer.Option("--model-dir", help="Base model directory containing fmdata.txt"),
    ] = Path("FM"),
    source_map: Annotated[
        Path | None,
        typer.Option("--source-map", help="Override source-map YAML path"),
    ] = None,
    variables: Annotated[
        list[str] | None,
        typer.Option("--variables", help="Optional variables to check (repeatable)"),
    ] = None,
    periods: Annotated[
        int,
        typer.Option("--periods", help="Number of most recent quarters to check"),
    ] = 40,
    cache_dir: Annotated[
        Path | None,
        typer.Option("--cache-dir", help="Optional FRED cache directory override"),
    ] = None,
    format: Annotated[
        str,
        typer.Option("--format", help="Output format: table or json"),
    ] = "table",
) -> None:
    """Compare fmdata historical values against normalized FRED values (mapping calibration)."""

    if importlib.util.find_spec("fredapi") is None:
        console.print("[red]fredapi is required for this command.[/red]")
        console.print("[yellow]Install with:[/yellow] `uv pip install fp-wraptr[fred]`")
        raise typer.Exit(code=1)

    from fp_wraptr.data.check_mapping import MappingCheckError, check_mapping_against_fmdata

    normalized_format = str(format).strip().lower()
    if normalized_format not in {"table", "json"}:
        console.print("[red]Unknown --format. Use table or json.[/red]")
        raise typer.Exit(code=1)

    try:
        payload = check_mapping_against_fmdata(
            model_dir=model_dir,
            source_map_path=source_map,
            variables=variables,
            periods=periods,
            sources=["fred"],
            cache_dir=cache_dir,
        )
    except MappingCheckError as exc:
        console.print(f"[red]mapping check failed:[/red] {exc}")
        raise typer.Exit(code=1) from None

    rows = payload.get("rows", [])
    skipped_names = [
        str(item.get("variable", ""))
        for item in payload.get("skipped", [])
        if str(item.get("variable", "")).strip()
    ]

    if normalized_format == "json":
        console.print_json(
            json.dumps({"rows": rows, "skipped": sorted(set(skipped_names))}, indent=2)
        )
        return

    table = Table(title=f"FRED Mapping Check (last {periods} quarters)")
    for col in (
        "variable",
        "series_id",
        "frequency",
        "annual_rate",
        "aggregation",
        "scale",
        "offset",
        "overlap_count",
        "correlation",
        "median_abs_error",
        "suggested_scale",
    ):
        table.add_column(col)
    for row in rows:
        table.add_row(
            str(row.get("variable", "")),
            str(row.get("series_id", "")),
            str(row.get("frequency", "")),
            str(row.get("annual_rate", "")),
            str(row.get("aggregation", "")),
            f"{row.get('scale', 1.0):.6g}",
            f"{row.get('offset', 0.0):.6g}",
            str(row.get("overlap_count", 0)),
            f"{row.get('correlation'):.4f}"
            if isinstance(row.get("correlation"), float)
            else "n/a",
            f"{row.get('median_abs_error'):.6g}"
            if isinstance(row.get("median_abs_error"), float)
            else "n/a",
            f"{row.get('suggested_scale'):.6g}"
            if isinstance(row.get("suggested_scale"), float)
            else "n/a",
        )
    console.print(table)


@data_app.command("check-mapping")
def data_check_mapping(
    model_dir: Annotated[
        Path,
        typer.Option("--model-dir", help="Base model directory containing fmdata.txt"),
    ] = Path("FM"),
    source_map: Annotated[
        Path | None,
        typer.Option("--source-map", help="Override source-map YAML path"),
    ] = None,
    variables: Annotated[
        list[str] | None,
        typer.Option("--variables", help="Optional variables to check (repeatable)"),
    ] = None,
    sources: Annotated[
        list[str] | None,
        typer.Option("--sources", help="Sources to audit (repeatable): fred, bea, bls"),
    ] = None,
    periods: Annotated[
        int,
        typer.Option("--periods", help="Number of most recent quarters to check"),
    ] = 40,
    cache_dir: Annotated[
        Path | None,
        typer.Option("--cache-dir", help="Optional cache directory override"),
    ] = None,
    format: Annotated[
        str,
        typer.Option("--format", help="Output format: table or json"),
    ] = "table",
) -> None:
    """Compare fmdata history against normalized source mappings for fred/bea/bls."""
    enabled_sources = [
        str(item).strip().lower() for item in (sources or ["fred"]) if str(item).strip()
    ]
    enabled_sources = list(dict.fromkeys(enabled_sources)) or ["fred"]
    if "fred" in enabled_sources and importlib.util.find_spec("fredapi") is None:
        console.print("[red]fredapi is required when --sources includes fred.[/red]")
        console.print("[yellow]Install with:[/yellow] `uv pip install fp-wraptr[fred]`")
        raise typer.Exit(code=1)

    from fp_wraptr.data.check_mapping import MappingCheckError, check_mapping_against_fmdata

    normalized_format = str(format).strip().lower()
    if normalized_format not in {"table", "json"}:
        console.print("[red]Unknown --format. Use table or json.[/red]")
        raise typer.Exit(code=1)

    try:
        payload = check_mapping_against_fmdata(
            model_dir=model_dir,
            source_map_path=source_map,
            variables=variables,
            periods=periods,
            sources=enabled_sources,
            cache_dir=cache_dir,
        )
    except MappingCheckError as exc:
        console.print(f"[red]mapping check failed:[/red] {exc}")
        raise typer.Exit(code=1) from None

    if normalized_format == "json":
        console.print_json(json.dumps(payload, indent=2))
        return

    table = Table(
        title=(
            "Mapping Check "
            f"(sources={','.join(payload.get('sources', enabled_sources))}, last {periods} quarters)"
        )
    )
    for col in (
        "variable",
        "source",
        "series_id",
        "frequency",
        "annual_rate",
        "aggregation",
        "scale",
        "offset",
        "overlap_count",
        "correlation",
        "median_abs_error",
        "suggested_scale",
    ):
        table.add_column(col)

    for row in payload.get("rows", []):
        table.add_row(
            str(row.get("variable", "")),
            str(row.get("source", "")),
            str(row.get("series_id", "")),
            str(row.get("frequency", "")),
            str(row.get("annual_rate", "")),
            str(row.get("aggregation", "")),
            f"{row.get('scale', 1.0):.6g}",
            f"{row.get('offset', 0.0):.6g}",
            str(row.get("overlap_count", 0)),
            f"{row.get('correlation'):.4f}"
            if isinstance(row.get("correlation"), float)
            else "n/a",
            f"{row.get('median_abs_error'):.6g}"
            if isinstance(row.get("median_abs_error"), float)
            else "n/a",
            f"{row.get('suggested_scale'):.6g}"
            if isinstance(row.get("suggested_scale"), float)
            else "n/a",
        )
    console.print(table)

    skipped = payload.get("skipped", [])
    if skipped:
        skipped_table = Table(title="Skipped")
        for col in ("variable", "reason", "source", "frequency", "bea_table"):
            skipped_table.add_column(col)
        for item in skipped:
            skipped_table.add_row(
                str(item.get("variable", "")),
                str(item.get("reason", "")),
                str(item.get("source", "")),
                str(item.get("frequency", "")),
                str(item.get("bea_table", "")),
            )
        console.print(skipped_table)


@data_app.command("run")
def data_run_pipeline(
    pipeline: Annotated[
        Path,
        typer.Argument(help="Series pipeline YAML path"),
    ],
    report: Annotated[
        Path | None,
        typer.Option("--report", help="Write pipeline report JSON to this path"),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Compute results but do not write any outputs"),
    ] = False,
) -> None:
    """Run a project-agnostic series pipeline and write FP artifacts."""
    from datetime import date

    from fp_wraptr.data.series_pipeline.runner import PipelineRunError, run_pipeline

    if not pipeline.exists():
        console.print(f"[red]Pipeline YAML not found:[/red] {pipeline}")
        raise typer.Exit(code=1)

    if report is None and not dry_run:
        out_dir = (
            Path("artifacts") / "model_updates" / f"{date.today().isoformat()}_{pipeline.stem}"
        )
        report = out_dir / "pipeline_report.json"

    try:
        result = run_pipeline(pipeline_path=pipeline, output_report=report, dry_run=bool(dry_run))
    except PipelineRunError as exc:
        console.print(f"[red]Pipeline failed:[/red] {exc}")
        raise typer.Exit(code=1) from None

    table = Table(title=f"Pipeline: {result.config.name}")
    table.add_column("step")
    table.add_column("target")
    table.add_column("written")
    for step in result.report.get("steps", []):
        if not isinstance(step, dict):
            continue
        step_id = str(step.get("id", ""))
        target = step.get("target") or {}
        target_kind = str(target.get("kind", ""))
        written = step.get("written") or step.get("dry_run_write_to") or []
        if isinstance(written, list):
            written_text = "\n".join(str(p) for p in written)
        else:
            written_text = str(written)
        table.add_row(step_id, target_kind, written_text)
    console.print(table)
    if report:
        console.print(f"[green]Report:[/green] {report}")


@data_app.command("preview")
def data_preview_pipeline(
    pipeline: Annotated[
        Path,
        typer.Argument(help="Series pipeline YAML path"),
    ],
) -> None:
    """Preview a series pipeline without writing outputs."""
    from fp_wraptr.data.series_pipeline.runner import PipelineRunError, run_pipeline

    if not pipeline.exists():
        console.print(f"[red]Pipeline YAML not found:[/red] {pipeline}")
        raise typer.Exit(code=1)

    try:
        result = run_pipeline(pipeline_path=pipeline, output_report=None, dry_run=True)
    except PipelineRunError as exc:
        console.print(f"[red]Pipeline preview failed:[/red] {exc}")
        raise typer.Exit(code=1) from None

    table = Table(title=f"Pipeline preview: {result.config.name}")
    for col in ("step", "last_observed", "target", "extrapolation"):
        table.add_column(col)
    for step in result.report.get("steps", []):
        if not isinstance(step, dict):
            continue
        step_id = str(step.get("id", ""))
        last_obs = step.get("last_observed") or {}
        last_text = ""
        if isinstance(last_obs, dict):
            last_text = f"{last_obs.get('period')}  {last_obs.get('value')}"
        target = step.get("target") or {}
        target_text = str(target.get("kind", ""))
        extrap = step.get("extrapolation") or {}
        extrap_text = ""
        if isinstance(extrap, dict):
            extrap_text = f"{extrap.get('method')} {extrap.get('start')}..{extrap.get('end')}"
        table.add_row(step_id, last_text, target_text, extrap_text)
    console.print(table)


@fred_app.command("fetch")
def fred_fetch(
    series_ids: Annotated[
        list[str],
        typer.Argument(help="One or more FRED series IDs"),
    ],
    start: Annotated[
        str | None,
        typer.Option("--start", help="Observation start date (YYYY-MM-DD)"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", help="Observation end date (YYYY-MM-DD)"),
    ] = None,
    cache_dir: Annotated[
        Path | None,
        typer.Option("--cache-dir", help="Override FRED cache directory"),
    ] = None,
    format: Annotated[
        str,
        typer.Option("--format", help="Output format: csv or json"),
    ] = "csv",
) -> None:
    """Fetch FRED series and print the result table."""
    if importlib.util.find_spec("fredapi") is None:
        console.print("[red]fredapi is required for this command.[/red]")
        console.print("[yellow]Install with:[/yellow] `uv pip install fp-wraptr[fred]`")
        raise typer.Exit(code=1)

    if not series_ids:
        console.print("[red]At least one FRED series ID is required.[/red]")
        raise typer.Exit(code=1)

    from fp_wraptr.fred.ingest import fetch_series

    frame = fetch_series(series_ids, start=start, end=end, cache_dir=cache_dir)
    selected_format = format.lower()
    if selected_format == "csv":
        typer.echo(frame.to_csv(index=True))
        return
    if selected_format == "json":
        console.print_json(frame.reset_index().to_json(orient="records", date_format="iso"))
        return

    console.print("[red]Unknown format. Use --format csv or --format json.[/red]")
    raise typer.Exit(code=1)


@fred_app.command("clear-cache")
def fred_clear_cache(
    cache_dir: Annotated[
        Path | None,
        typer.Option("--cache-dir", help="Override FRED cache directory"),
    ] = None,
) -> None:
    """Delete cached FRED series files."""
    from fp_wraptr.fred.ingest import clear_cache

    deleted = clear_cache(cache_dir=cache_dir)
    console.print(f"[green]Deleted {deleted} cached FRED files.[/green]")


@bea_app.command("fetch-nipa")
def bea_fetch_nipa(
    table: Annotated[
        str,
        typer.Argument(help="BEA NIPA table name (for example T10106)"),
    ],
    line: Annotated[
        int,
        typer.Argument(help="BEA NIPA line number (integer)"),
    ],
    frequency: Annotated[
        str,
        typer.Option("--frequency", help="Frequency: Q or A"),
    ] = "Q",
    year: Annotated[
        str,
        typer.Option("--year", help="Year selector (use ALL for full table)"),
    ] = "ALL",
    cache_dir: Annotated[
        Path | None,
        typer.Option("--cache-dir", help="Override BEA cache directory"),
    ] = None,
    format: Annotated[
        str,
        typer.Option("--format", help="Output format: csv or json"),
    ] = "csv",
) -> None:
    """Fetch a BEA NIPA table line and print observations."""
    from fp_wraptr.bea.ingest import fetch_nipa_lines

    series = fetch_nipa_lines(
        table_name=table,
        line_numbers=[int(line)],
        frequency=frequency,
        year=year,
        cache_dir=cache_dir,
    ).get(int(line))
    if series is None or series.empty:
        console.print("[red]No observations returned.[/red]")
        raise typer.Exit(code=1)

    frame = series.to_frame()
    selected_format = format.lower()
    if selected_format == "csv":
        typer.echo(frame.to_csv(index=True))
        return
    if selected_format == "json":
        console.print_json(frame.reset_index().to_json(orient="records", date_format="iso"))
        return

    console.print("[red]Unknown format. Use --format csv or --format json.[/red]")
    raise typer.Exit(code=1)


@bea_app.command("clear-cache")
def bea_clear_cache(
    cache_dir: Annotated[
        Path | None,
        typer.Option("--cache-dir", help="Override BEA cache directory"),
    ] = None,
) -> None:
    """Delete cached BEA payload files."""
    from fp_wraptr.bea.ingest import clear_cache

    deleted = clear_cache(cache_dir=cache_dir)
    console.print(f"[green]Deleted {deleted} cached BEA files.[/green]")


@bls_app.command("fetch")
def bls_fetch(
    series_ids: Annotated[
        list[str],
        typer.Argument(help="One or more BLS series IDs"),
    ],
    start_year: Annotated[
        int,
        typer.Option("--start-year", help="Start year (YYYY)"),
    ],
    end_year: Annotated[
        int,
        typer.Option("--end-year", help="End year (YYYY)"),
    ],
    cache_dir: Annotated[
        Path | None,
        typer.Option("--cache-dir", help="Override BLS cache directory"),
    ] = None,
    format: Annotated[
        str,
        typer.Option("--format", help="Output format: csv or json"),
    ] = "csv",
) -> None:
    """Fetch BLS series and print the result table."""
    if not series_ids:
        console.print("[red]At least one BLS series ID is required.[/red]")
        raise typer.Exit(code=1)

    from fp_wraptr.bls.ingest import BlsSeriesRequest, fetch_series

    frame = fetch_series(
        BlsSeriesRequest(
            series_ids=series_ids, start_year=int(start_year), end_year=int(end_year)
        ),
        cache_dir=cache_dir,
    )
    selected_format = format.lower()
    if selected_format == "csv":
        typer.echo(frame.to_csv(index=True))
        return
    if selected_format == "json":
        console.print_json(frame.reset_index().to_json(orient="records", date_format="iso"))
        return

    console.print("[red]Unknown format. Use --format csv or --format json.[/red]")
    raise typer.Exit(code=1)


@bls_app.command("clear-cache")
def bls_clear_cache(
    cache_dir: Annotated[
        Path | None,
        typer.Option("--cache-dir", help="Override BLS cache directory"),
    ] = None,
) -> None:
    """Delete cached BLS payload files."""
    from fp_wraptr.bls.ingest import clear_cache

    deleted = clear_cache(cache_dir=cache_dir)
    console.print(f"[green]Deleted {deleted} cached BLS files.[/green]")


@dsl_app.command("compile")
def dsl_compile(
    path: Annotated[
        Path,
        typer.Argument(help="Path to scenario DSL file"),
    ],
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Write compiled scenario to file"),
    ] = None,
    format: Annotated[
        str,
        typer.Option("--format", "-f", help="Output format: yaml or json"),
    ] = "yaml",
) -> None:
    """Compile scenario DSL into ScenarioConfig YAML/JSON."""
    from fp_wraptr.scenarios.dsl import DSLCompileError, compile_scenario_dsl_file

    try:
        config = compile_scenario_dsl_file(path)
    except FileNotFoundError as exc:
        console.print(f"[red]DSL file not found:[/red] {exc}")
        raise typer.Exit(code=1) from None
    except DSLCompileError as exc:
        console.print(f"[red]DSL compile failed:[/red] {exc}")
        raise typer.Exit(code=1) from None

    payload = config.model_dump(mode="json")
    selected_format = format.lower()
    if selected_format == "yaml":
        rendered = yaml.safe_dump(payload, sort_keys=False)
    elif selected_format == "json":
        rendered = json.dumps(payload, indent=2)
    else:
        console.print("[red]Unknown format. Use --format yaml or --format json.[/red]")
        raise typer.Exit(code=1)

    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            rendered + ("\n" if not rendered.endswith("\n") else ""), encoding="utf-8"
        )
        console.print(f"[green]Compiled scenario written to:[/green] {output}")
        return

    typer.echo(rendered.rstrip() + "\n")


@app.command()
def info() -> None:
    """Print environment info for debugging and bug reports."""
    from fp_wraptr import __version__
    from fp_wraptr.runtime.fp_exe import FPExecutable

    table = Table(title="fp-wraptr environment info")
    table.add_column("Field", style="cyan")
    table.add_column("Value", overflow="fold")

    table.add_row("Version", f"fp-wraptr {__version__}")
    table.add_row("Python", sys.version)
    table.add_row("Platform", sys.platform)

    exe = FPExecutable()
    preflight = exe.preflight_report()
    table.add_row("fp.exe available", "yes" if preflight.get("available") else "no")
    wine = shutil.which("wine")
    table.add_row("Wine", wine or "not found")
    table.add_row("WINEPREFIX", str(preflight.get("wineprefix", "") or "<unknown>"))
    table.add_row("WINEPREFIX exists", "yes" if preflight.get("wineprefix_exists") else "no")
    table.add_row(
        "WINEPREFIX initialized",
        "yes" if preflight.get("wineprefix_initialized") else "no",
    )
    table.add_row("FM directory", "yes" if Path("FM").exists() else "no")
    table.add_row(
        "fp.exe path",
        f"{preflight.get('exe_path', '')} ({'found' if preflight.get('exe_exists') else 'missing'})",
    )
    missing_data_files = preflight.get("missing_data_files", []) or []
    table.add_row(
        "Missing FP data files",
        ", ".join(str(name) for name in missing_data_files) if missing_data_files else "<none>",
    )
    table.add_row(
        "Input file in work dir",
        "yes" if preflight.get("input_file_exists") else "no",
    )
    if preflight.get("wine_required") and not preflight.get("wineprefix_initialized"):
        table.add_row(
            "Wine hint",
            "Prefix appears uninitialized; run `WINEPREFIX=<path> wineboot`.",
        )

    optional_extras = [
        "networkx",
        "matplotlib",
        "streamlit",
        "plotly",
        "fastmcp",
        "openpyxl",
        "fredapi",
    ]
    for module in optional_extras:
        table.add_row(
            f"Optional dependency: {module}",
            "installed" if importlib.util.find_spec(module) else "missing",
        )

    console.print(table)


@app.command()
def diff(
    run_a: Annotated[Path, typer.Argument(help="First run output directory")],
    run_b: Annotated[Path, typer.Argument(help="Second run output directory")],
    export: Annotated[
        str | None,
        typer.Option(
            "--export",
            help="Export format for deltas: csv or excel",
        ),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option("--output", help="Export output path"),
    ] = None,
) -> None:
    """Compare two completed FP runs."""
    from fp_wraptr.analysis.diff import (
        diff_run_dirs,
        export_diff_csv,
        export_diff_excel,
    )

    summary = diff_run_dirs(run_a, run_b)
    if summary.get("error"):
        console.print(f"[red]Diff failed:[/red] {summary['error']}")
        raise typer.Exit(code=1)

    if export:
        export = export.lower()
        if export not in {"csv", "excel"}:
            console.print("[red]Export format must be 'csv' or 'excel'.[/red]")
            raise typer.Exit(code=1)

        if output is None:
            output = Path(f"diff.{'xlsx' if export == 'excel' else 'csv'}")

        try:
            if export == "csv":
                export_diff_csv(summary, output)
            else:
                export_diff_excel(summary, output)
        except ModuleNotFoundError as exc:
            console.print("[red]Excel export requires optional dependency 'openpyxl'.[/red]")
            raise typer.Exit(code=1) from exc
        except RuntimeError as exc:
            console.print(f"[red]{exc}[/red]")
            raise typer.Exit(code=1) from exc

        console.print(f"[green]Diff export written to:[/green] {output}")
    _print_diff_summary(summary)


@app.command()
def history(
    artifacts_dir: Annotated[
        Path,
        typer.Option("--artifacts-dir", help="Root directory containing run artifacts"),
    ] = Path("artifacts"),
    latest: Annotated[
        bool,
        typer.Option("--latest", help="Show only the most recent run"),
    ] = False,
) -> None:
    """List historical runs from the artifacts directory."""
    from fp_wraptr.dashboard.artifacts import scan_artifacts

    runs = scan_artifacts(artifacts_dir)
    if not runs:
        console.print(f"[yellow]No runs found in {artifacts_dir}.[/yellow]")
        return
    if latest:
        runs = runs[:1]

    table = Table(title="Run History")
    table.add_column("Scenario", style="cyan")
    table.add_column("Timestamp", style="blue")
    table.add_column("Has Output", justify="center")
    table.add_column("Description")
    table.add_column("Directory")
    for run in runs:
        description = run.config.description if run.config else ""
        table.add_row(
            run.scenario_name,
            run.timestamp,
            "yes" if run.has_output else "no",
            description,
            str(run.run_dir),
        )

    console.print(table)
    if latest and runs:
        _print_run_dir(runs[0].run_dir)


@export_app.command("pages")
def export_pages(
    spec: Annotated[
        Path,
        typer.Option(
            "--spec",
            help="Pages export spec YAML path",
        ),
    ] = Path("public/model-runs.spec.yaml"),
    artifacts_dir: Annotated[
        Path,
        typer.Option("--artifacts-dir", help="Artifacts directory containing exported runs"),
    ] = Path("artifacts"),
    out_dir: Annotated[
        Path,
        typer.Option("--out-dir", help="Target output directory for the public bundle"),
    ] = Path("public/model-runs"),
) -> None:
    """Export a read-only run bundle for GitHub Pages."""
    from fp_wraptr.pages_export import PagesExportError, export_pages_bundle

    try:
        result = export_pages_bundle(
            spec_path=spec,
            artifacts_dir=artifacts_dir,
            out_dir=out_dir,
        )
    except PagesExportError as exc:
        console.print(f"[red]Pages export failed:[/red] {exc}")
        raise typer.Exit(code=1) from None

    console.print(f"[green]Public bundle:[/green] {result.out_dir}")
    console.print(f"[green]Manifest:[/green] {result.manifest_path}")
    console.print(
        "[blue]Summary:[/blue] "
        f"runs={result.run_count} "
        f"variables={result.variable_count} "
        f"generated_at={result.generated_at}"
    )


@io_app.command("parse-output")
def parse_output(
    path: Annotated[Path, typer.Argument(help="Path to FP output file (fmout.txt)")],
    format: Annotated[str, typer.Option("--format", "-f", help="Output format")] = "json",
) -> None:
    """Parse an FP output file into structured data."""
    from fp_wraptr.io.parser import parse_fp_output

    result = parse_fp_output(path)
    if not result.variables:
        console.print(
            "[red]No forecast variables found in output file. "
            "The file may be missing forecast sections or be malformed.[/red]"
        )
        raise typer.Exit(1)

    if format == "json":
        console.print_json(json.dumps(result.to_dict(), indent=2, default=str))
    elif format == "csv":
        df = result.to_dataframe()
        typer.echo(df.to_csv(index=True))
    else:
        console.print(f"[red]Unknown format:[/red] {format}")
        raise typer.Exit(1)


@io_app.command("parse-input")
def parse_input(
    path: Annotated[Path, typer.Argument(help="Path to FP input file (fminput.txt)")],
) -> None:
    """Parse an FP input file and emit structured JSON sections."""
    from fp_wraptr.io.input_parser import parse_fp_input

    result = parse_fp_input(path)
    console.print_json(json.dumps(result, indent=2, default=str))


@viz_app.command("plot")
def plot(
    path: Annotated[Path, typer.Argument(help="Path to FP output file")],
    var: Annotated[
        list[str] | None,
        typer.Option("--var", "-v", help="Variable(s) to plot (default: all forecast vars)"),
    ] = None,
    output: Annotated[Path, typer.Option("--output", "-o", help="Output image file")] = Path(
        "artifacts/forecast.png"
    ),
) -> None:
    """Generate forecast plots from FP output."""
    if not path.exists():
        console.print(f"[red]Output file not found:[/red] {path}")
        raise typer.Exit(1)
    from fp_wraptr.io.parser import parse_fp_output
    from fp_wraptr.viz.plots import plot_forecast

    result = parse_fp_output(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    plot_forecast(result, variables=var, output_path=output)
    console.print(f"[green]Chart saved:[/green] {output}")


def _print_validation_error(exc: ValidationError | Exception) -> None:
    """Render a concise validation error summary."""
    console.print("[red]Validation failed:[/red]")
    if isinstance(exc, ValidationError):
        for issue in exc.errors():
            location = ".".join(str(part) for part in issue.get("loc", ()))
            message = issue.get("msg", "Invalid value")
            console.print(f"  • {location}: {message}")
        return

    console.print(f"  • {exc}")


def _print_diff_summary(summary: dict) -> None:
    """Pretty-print a diff summary table."""

    def _format_optional_float(value: object, precision: int) -> str:
        if isinstance(value, (int, float)):
            return f"{value:.{precision}f}"
        return "N/A"

    table = Table(title="Run Comparison")
    table.add_column("Variable", style="cyan")
    table.add_column("Baseline (last)", justify="right")
    table.add_column("Scenario (last)", justify="right")
    table.add_column("Delta", justify="right", style="yellow")
    table.add_column("% Change", justify="right", style="magenta")

    for var_name, delta in summary.get("deltas", {}).items():
        table.add_row(
            var_name,
            _format_optional_float(delta.get("baseline"), 4),
            _format_optional_float(delta.get("scenario"), 4),
            _format_optional_float(delta.get("abs_delta", 0), 4),
            f"{_format_optional_float(delta.get('pct_delta'), 2)}%"
            if isinstance(delta.get("pct_delta"), (int, float))
            else "N/A",
        )

    console.print(table)


def _export_dot(dependency_graph: object, output: Path) -> None:
    """Write a DOT representation for the dependency graph."""
    from fp_wraptr.analysis.graph import _load_networkx

    nx = _load_networkx()
    if not isinstance(dependency_graph, nx.DiGraph):
        return

    lines = ["digraph fp_dependencies {"]
    for node in dependency_graph.nodes:
        lines.append(f'  "{node}";')
    for source, target in dependency_graph.edges:
        lines.append(f'  "{source}" -> "{target}";')
    lines.append("}")

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _expand_scenario_paths(paths: list[Path]) -> list[Path]:
    """Resolve scenario paths, directories, and wildcard patterns."""
    resolved: list[Path] = []
    seen: set[str] = set()

    for path in paths:
        path_text = str(path)
        is_pattern = any(char in path_text for char in "*?[")
        if is_pattern:
            for match in sorted(Path().glob(path_text)):
                resolved_path = match.resolve()
                if str(resolved_path) in seen:
                    continue
                resolved.append(resolved_path)
                seen.add(str(resolved_path))
            continue

        candidate = Path(path)
        if candidate.is_dir():
            for match in sorted(candidate.glob("*.yaml")):
                resolved_path = match.resolve()
                if str(resolved_path) in seen:
                    continue
                resolved.append(resolved_path)
                seen.add(str(resolved_path))
            continue

        resolved_path = candidate.resolve()
        if str(resolved_path) in seen:
            continue
        resolved.append(resolved_path)
        seen.add(str(resolved_path))

    return resolved


if __name__ == "__main__":
    app()
