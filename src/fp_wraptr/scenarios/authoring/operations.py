"""Agent-facing operations on managed scenario and bundle workspaces."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from fp_wraptr.analysis.diff import diff_run_dirs
from fp_wraptr.dashboard.artifacts import latest_runs, scan_artifacts
from fp_wraptr.io.parser import parse_fp_output
from fp_wraptr.scenarios.authoring.compiler import (
    compile_bundle_workspace,
    compile_scenario_workspace,
    initialize_card_instances,
    load_card_specs,
    load_series_points_from_csv,
    load_series_points_from_text,
    normalize_series_points,
    resolve_card_defaults,
)
from fp_wraptr.scenarios.authoring.models import (
    BundleDraft,
    BundleVariantDraft,
    CardInstance,
    DraftSourceRef,
    ScenarioDraft,
    WorkspaceOperation,
    WorkspaceRunLink,
)
from fp_wraptr.scenarios.authoring.workspace import (
    create_bundle_draft_from_source,
    create_scenario_draft_from_source,
    list_workspaces,
    load_workspace_draft_by_id,
    save_workspace_draft,
    workspace_dir,
    workspace_id_for,
)
from fp_wraptr.scenarios.bundle import run_bundle
from fp_wraptr.scenarios.catalog import load_scenario_catalog
# NOTE: fp_wraptr.scenarios.packs is imported lazily inside functions that need
# it to avoid a circular import (packs → authoring.compiler → authoring.__init__
# → operations → packs).
from fp_wraptr.scenarios.runner import run_scenario

__all__ = [
    "add_bundle_variant",
    "apply_workspace_card",
    "build_visualization_view",
    "clone_bundle_variant_recipe",
    "compare_workspace_runs",
    "compile_workspace",
    "create_workspace_from_bundle",
    "create_workspace_from_catalog",
    "get_workspace",
    "import_workspace_series",
    "list_packs",
    "list_visualizations",
    "list_workspace_cards",
    "list_workspaces_payload",
    "remove_bundle_variant",
    "run_workspace",
    "update_bundle_variant",
    "update_workspace_metadata",
]


def list_packs(*, repo_root: Path | str) -> list[dict[str, object]]:
    from fp_wraptr.scenarios.packs import load_pack_manifests

    out: list[dict[str, object]] = []
    for manifest, path in load_pack_manifests(repo_root=repo_root):
        out.append({
            "pack_id": manifest.pack_id,
            "label": manifest.label,
            "family": manifest.family,
            "description": manifest.description,
            "visibility": manifest.visibility,
            "manifest_path": str(path),
            "cards_family": manifest.cards_scope,
            "catalog_entry_ids": list(manifest.catalog_entry_ids),
            "recipe_count": len(manifest.recipes),
            "visualization_count": len(manifest.visualizations),
        })
    return out


def list_workspaces_payload(
    *,
    repo_root: Path | str,
    family: str = "",
) -> list[dict[str, object]]:
    wanted_family = str(family or "").strip().lower()
    out: list[dict[str, object]] = []
    for info in list_workspaces(repo_root):
        if wanted_family and info.family.lower() != wanted_family:
            continue
        draft, _path = load_workspace_draft_by_id(info.workspace_id, repo_root=repo_root)
        out.append(_workspace_payload(draft, workspace_path=info.draft_path))
    return out


def create_workspace_from_catalog(
    *,
    repo_root: Path | str,
    catalog_entry_id: str,
    workspace_slug: str = "",
    label: str = "",
) -> dict[str, object]:
    repo_root = Path(repo_root).resolve()
    catalog = load_scenario_catalog(repo_root=repo_root)
    entry = catalog.find(catalog_entry_id)
    if entry is None:
        raise FileNotFoundError(f"Catalog entry not found: {catalog_entry_id}")
    if entry.kind == "scenario":
        draft = create_scenario_draft_from_source(
            source=DraftSourceRef(kind="catalog", value=catalog_entry_id),
            repo_root=repo_root,
        )
    else:
        draft = create_bundle_draft_from_source(
            source=DraftSourceRef(kind="catalog", value=catalog_entry_id),
            repo_root=repo_root,
        )
    draft = _retitle_workspace(draft, workspace_slug=workspace_slug, label=label)
    draft.extra["catalog_entry_id"] = catalog_entry_id
    draft.recipe_history.append(
        WorkspaceOperation(
            operation="create_workspace",
            summary=f"Created from catalog entry {catalog_entry_id}",
            details={"catalog_entry_id": catalog_entry_id, "kind": entry.kind},
        )
    )
    draft_path = save_workspace_draft(draft, repo_root=repo_root)
    return _workspace_payload(draft, workspace_path=draft_path)


def create_workspace_from_bundle(
    *,
    repo_root: Path | str,
    bundle_yaml: str,
    workspace_slug: str = "",
    label: str = "",
) -> dict[str, object]:
    repo_root = Path(repo_root).resolve()
    draft = create_bundle_draft_from_source(
        source=DraftSourceRef(kind="path", value=bundle_yaml),
        repo_root=repo_root,
    )
    draft = _retitle_workspace(draft, workspace_slug=workspace_slug, label=label)
    draft.recipe_history.append(
        WorkspaceOperation(
            operation="create_workspace",
            summary="Created from bundle path",
            details={"bundle_yaml": str(bundle_yaml)},
        )
    )
    draft_path = save_workspace_draft(draft, repo_root=repo_root)
    return _workspace_payload(draft, workspace_path=draft_path)


def get_workspace(*, repo_root: Path | str, workspace_id: str) -> dict[str, object]:
    draft, path = load_workspace_draft_by_id(workspace_id, repo_root=repo_root)
    return _workspace_payload(draft, workspace_path=path)


def update_workspace_metadata(
    *,
    repo_root: Path | str,
    workspace_id: str,
    label: str = "",
    description: str = "",
    forecast_start: str = "",
    forecast_end: str = "",
    backend: str = "",
    track_variables: list[str] | None = None,
) -> dict[str, object]:
    draft, path = load_workspace_draft_by_id(workspace_id, repo_root=repo_root)
    updates: dict[str, object] = {}
    if label.strip():
        updates["label"] = label.strip()
    if description.strip():
        updates["description"] = description.strip()
    if forecast_start.strip():
        updates["forecast_start"] = forecast_start.strip()
    if forecast_end.strip():
        updates["forecast_end"] = forecast_end.strip()
    if backend.strip():
        updates["backend"] = backend.strip().lower()
    if track_variables is not None:
        updates["track_variables"] = [
            str(item).strip().upper() for item in track_variables if str(item).strip()
        ]
    draft = draft.model_copy(update=updates)
    draft.recipe_history.append(
        WorkspaceOperation(
            operation="update_workspace_metadata",
            summary="Updated workspace metadata",
            details={key: value for key, value in updates.items()},
        )
    )
    save_workspace_draft(draft, repo_root=repo_root, workspace=path.parent)
    return _workspace_payload(draft, workspace_path=path)


def list_workspace_cards(
    *,
    repo_root: Path | str,
    workspace_id: str,
    variant_id: str = "",
) -> dict[str, object]:
    repo_root = Path(repo_root).resolve()
    draft, path = load_workspace_draft_by_id(workspace_id, repo_root=repo_root)
    specs = load_card_specs(repo_root=repo_root, family=draft.family)
    scope_cards = _target_cards(draft, variant_id=variant_id)
    instances = initialize_card_instances(specs, scope_cards)
    defaults = resolve_card_defaults(draft, repo_root=repo_root)
    return {
        "workspace_id": draft.workspace_id or workspace_id,
        "workspace_path": str(path),
        "scope": "variant" if variant_id.strip() else "shared",
        "variant_id": variant_id.strip() or None,
        "available_variants": (
            [
                {
                    "variant_id": item.variant_id,
                    "label": item.label,
                    "scenario_name": item.scenario_name or item.variant_id,
                    "enabled": item.enabled,
                }
                for item in draft.variants
            ]
            if isinstance(draft, BundleDraft)
            else []
        ),
        "cards": [
            {
                "card_id": spec.card_id,
                "kind": spec.kind,
                "label": spec.label,
                "description": getattr(spec, "description", ""),
                "enabled": instance.enabled,
                "constants": dict(instance.constants),
                "selected_target": instance.selected_target,
                "input_mode": instance.input_mode,
                "defaults": defaults.get(spec.card_id, {}),
            }
            for spec, instance in zip(specs, instances, strict=False)
        ],
    }


def apply_workspace_card(
    *,
    repo_root: Path | str,
    workspace_id: str,
    card_id: str,
    constants: dict[str, float] | None = None,
    enabled: bool | None = None,
    selected_target: str = "",
    input_mode: str = "",
    variant_id: str = "",
) -> dict[str, object]:
    draft, path = load_workspace_draft_by_id(workspace_id, repo_root=repo_root)
    cards = _target_cards(draft, variant_id=variant_id)
    card = _find_or_create_card(cards, card_id)
    if enabled is not None:
        card.enabled = bool(enabled)
    if constants:
        normalized = {str(key).strip().upper(): float(value) for key, value in constants.items()}
        card.constants.update(normalized)
        card.enabled = True if enabled is None else card.enabled
    if selected_target.strip():
        card.selected_target = selected_target.strip()
        card.enabled = True if enabled is None else card.enabled
    if input_mode.strip():
        card.input_mode = input_mode.strip()
    draft.recipe_history.append(
        WorkspaceOperation(
            operation="apply_workspace_card",
            summary=f"Updated card {card_id}",
            details={
                "card_id": card_id,
                "variant_id": variant_id,
                "constants": constants or {},
                "enabled": enabled,
                "selected_target": selected_target,
            },
        )
    )
    save_workspace_draft(draft, repo_root=repo_root, workspace=path.parent)
    return _workspace_payload(draft, workspace_path=path)


def import_workspace_series(
    *,
    repo_root: Path | str,
    workspace_id: str,
    card_id: str,
    series_points: dict[str, float] | None = None,
    pasted_text: str = "",
    csv_path: str = "",
    variant_id: str = "",
    selected_target: str = "",
) -> dict[str, object]:
    draft, path = load_workspace_draft_by_id(workspace_id, repo_root=repo_root)
    cards = _target_cards(draft, variant_id=variant_id)
    card = _find_or_create_card(cards, card_id)
    points = dict(series_points or {})
    if csv_path.strip():
        points = load_series_points_from_csv(Path(csv_path.strip()))
        card.import_path = str(csv_path).strip()
        card.input_mode = "csv"
    elif pasted_text.strip():
        points = load_series_points_from_text(pasted_text)
        card.pasted_text = pasted_text
        card.input_mode = "paste"
    normalized = normalize_series_points({str(key): float(value) for key, value in points.items()})
    card.series_points = normalized
    card.enabled = True
    if selected_target.strip():
        card.selected_target = selected_target.strip()
    draft.recipe_history.append(
        WorkspaceOperation(
            operation="import_workspace_series",
            summary=f"Imported series into {card_id}",
            details={
                "card_id": card_id,
                "variant_id": variant_id,
                "point_count": len(normalized),
                "selected_target": card.selected_target,
            },
        )
    )
    save_workspace_draft(draft, repo_root=repo_root, workspace=path.parent)
    return _workspace_payload(draft, workspace_path=path)


def add_bundle_variant(
    *,
    repo_root: Path | str,
    workspace_id: str,
    variant_id: str,
    label: str = "",
    scenario_name: str = "",
    input_file: str = "",
    clone_from: str = "",
) -> dict[str, object]:
    draft, path = load_workspace_draft_by_id(workspace_id, repo_root=repo_root)
    if not isinstance(draft, BundleDraft):
        raise ValueError("Bundle variants are only supported for bundle workspaces")
    token = str(variant_id).strip()
    if not token:
        raise ValueError("variant_id must be non-empty")
    if any(item.variant_id == token for item in draft.variants):
        raise ValueError(f"Variant already exists: {token}")
    new_variant = BundleVariantDraft(
        variant_id=token,
        label=label.strip() or token.replace("_", " ").title(),
        scenario_name=scenario_name.strip() or token,
        input_file=input_file.strip() or None,
    )
    if clone_from.strip():
        source = next(
            (item for item in draft.variants if item.variant_id == clone_from.strip()), None
        )
        if source is None:
            raise ValueError(f"Clone source variant not found: {clone_from}")
        new_variant.cards = [card.model_copy(deep=True) for card in source.cards]
        if new_variant.input_file is None:
            new_variant.input_file = source.input_file
    draft.variants.append(new_variant)
    draft.recipe_history.append(
        WorkspaceOperation(
            operation="add_bundle_variant",
            summary=f"Added bundle variant {token}",
            details={
                "variant_id": token,
                "clone_from": clone_from,
                "scenario_name": new_variant.scenario_name,
                "input_file": new_variant.input_file,
            },
        )
    )
    save_workspace_draft(draft, repo_root=repo_root, workspace=path.parent)
    return _workspace_payload(draft, workspace_path=path)


def remove_bundle_variant(
    *,
    repo_root: Path | str,
    workspace_id: str,
    variant_id: str,
) -> dict[str, object]:
    draft, path = load_workspace_draft_by_id(workspace_id, repo_root=repo_root)
    if not isinstance(draft, BundleDraft):
        raise ValueError("Bundle variants are only supported for bundle workspaces")
    before = len(draft.variants)
    draft.variants = [
        item for item in draft.variants if item.variant_id != str(variant_id).strip()
    ]
    if len(draft.variants) == before:
        raise ValueError(f"Variant not found: {variant_id}")
    draft.recipe_history.append(
        WorkspaceOperation(
            operation="remove_bundle_variant",
            summary=f"Removed bundle variant {variant_id}",
            details={"variant_id": variant_id},
        )
    )
    save_workspace_draft(draft, repo_root=repo_root, workspace=path.parent)
    return _workspace_payload(draft, workspace_path=path)


def update_bundle_variant(
    *,
    repo_root: Path | str,
    workspace_id: str,
    variant_id: str,
    label: str = "",
    scenario_name: str = "",
    input_file: str = "",
    enabled: bool | None = None,
) -> dict[str, object]:
    draft, path = load_workspace_draft_by_id(workspace_id, repo_root=repo_root)
    if not isinstance(draft, BundleDraft):
        raise ValueError("Bundle variants are only supported for bundle workspaces")
    token = str(variant_id).strip()
    if not token:
        raise ValueError("variant_id must be non-empty")
    variant = next((item for item in draft.variants if item.variant_id == token), None)
    if variant is None:
        raise ValueError(f"Variant not found: {variant_id}")

    updates: dict[str, object] = {}
    if label.strip():
        variant.label = label.strip()
        updates["label"] = variant.label
    if scenario_name.strip():
        variant.scenario_name = scenario_name.strip()
        updates["scenario_name"] = variant.scenario_name
    if input_file.strip():
        variant.input_file = input_file.strip()
        updates["input_file"] = variant.input_file
    if enabled is not None:
        variant.enabled = bool(enabled)
        updates["enabled"] = variant.enabled
    if not updates:
        raise ValueError("Provide at least one variant field to update")

    draft.recipe_history.append(
        WorkspaceOperation(
            operation="update_bundle_variant",
            summary=f"Updated bundle variant {token}",
            details={"variant_id": token, **updates},
        )
    )
    save_workspace_draft(draft, repo_root=repo_root, workspace=path.parent)
    return _workspace_payload(draft, workspace_path=path)


def clone_bundle_variant_recipe(
    *,
    repo_root: Path | str,
    workspace_id: str,
    variant_id: str,
    clone_from: str,
    label: str = "",
    scenario_name: str = "",
    input_file: str = "",
    enabled: bool | None = None,
    card_id: str = "",
    constants: dict[str, float] | None = None,
    selected_target: str = "",
    input_mode: str = "",
) -> dict[str, object]:
    draft, path = load_workspace_draft_by_id(workspace_id, repo_root=repo_root)
    if not isinstance(draft, BundleDraft):
        raise ValueError("Bundle variants are only supported for bundle workspaces")

    token = str(variant_id).strip()
    if not token:
        raise ValueError("variant_id must be non-empty")
    source_id = str(clone_from).strip()
    if not source_id:
        raise ValueError("clone_from must be non-empty")
    if any(item.variant_id == token for item in draft.variants):
        raise ValueError(f"Variant already exists: {token}")
    source = next((item for item in draft.variants if item.variant_id == source_id), None)
    if source is None:
        raise ValueError(f"Clone source variant not found: {clone_from}")

    new_variant = BundleVariantDraft(
        variant_id=token,
        label=label.strip() or token.replace("_", " ").title(),
        scenario_name=scenario_name.strip() or token,
        input_file=input_file.strip() or source.input_file,
        enabled=source.enabled if enabled is None else bool(enabled),
        cards=[card.model_copy(deep=True) for card in source.cards],
    )

    seeded_patch: dict[str, object] | None = None
    if card_id.strip() or constants or selected_target.strip() or input_mode.strip():
        if not card_id.strip():
            raise ValueError("card_id is required when seeding a card patch")
        card = _find_or_create_card(new_variant.cards, card_id)
        if constants:
            normalized = {
                str(key).strip().upper(): float(value) for key, value in constants.items()
            }
            card.constants.update(normalized)
            card.enabled = True
        if selected_target.strip():
            card.selected_target = selected_target.strip()
            card.enabled = True
        if input_mode.strip():
            card.input_mode = input_mode.strip()
        seeded_patch = {
            "card_id": card_id.strip(),
            "constants": {
                str(key).strip().upper(): float(value) for key, value in (constants or {}).items()
            },
            "selected_target": selected_target.strip() or None,
            "input_mode": input_mode.strip() or None,
        }

    draft.variants.append(new_variant)
    details: dict[str, object] = {
        "variant_id": token,
        "clone_from": source_id,
        "label": new_variant.label,
        "scenario_name": new_variant.scenario_name,
        "input_file": new_variant.input_file,
        "enabled": new_variant.enabled,
    }
    if seeded_patch is not None:
        details["seeded_patch"] = seeded_patch
    draft.recipe_history.append(
        WorkspaceOperation(
            operation="clone_bundle_variant_recipe",
            summary=f"Cloned and initialized bundle variant {token}",
            details=details,
        )
    )
    save_workspace_draft(draft, repo_root=repo_root, workspace=path.parent)
    return _workspace_payload(draft, workspace_path=path)


def compile_workspace(
    *,
    repo_root: Path | str,
    workspace_id: str,
) -> dict[str, object]:
    repo_root = Path(repo_root).resolve()
    draft, path = load_workspace_draft_by_id(workspace_id, repo_root=repo_root)
    current_id = draft.workspace_id or workspace_id
    target_dir = workspace_dir(repo_root, family=draft.family, slug=draft.slug)
    if isinstance(draft, ScenarioDraft):
        result = compile_scenario_workspace(draft, repo_root=repo_root, workspace_dir=target_dir)
    else:
        result = compile_bundle_workspace(draft, repo_root=repo_root, workspace_dir=target_dir)
    draft.extra.update({
        "compiled_path": str(result.compiled_path),
        "compile_report_path": str(result.report_path),
        "last_compiled_at": _now(),
    })
    draft.recipe_history.append(
        WorkspaceOperation(
            operation="compile_workspace",
            summary=f"Compiled workspace {current_id}",
            details={"compiled_path": str(result.compiled_path), "errors": list(result.errors)},
        )
    )
    save_workspace_draft(draft, repo_root=repo_root, workspace=path.parent)
    return {
        "workspace_id": current_id,
        "draft_kind": result.draft_kind,
        "workspace_dir": str(result.workspace_dir),
        "compiled_path": str(result.compiled_path),
        "report_path": str(result.report_path),
        "generated_files": [str(item) for item in result.generated_files],
        "errors": list(result.errors),
        "ok": result.ok,
        "visualizations": list_visualizations(repo_root=repo_root, workspace_id=current_id),
    }


def run_workspace(
    *,
    repo_root: Path | str,
    workspace_id: str,
    output_dir: str = "artifacts/agent_runs",
) -> dict[str, object]:
    repo_root = Path(repo_root).resolve()
    compile_payload = compile_workspace(repo_root=repo_root, workspace_id=workspace_id)
    draft, path = load_workspace_draft_by_id(workspace_id, repo_root=repo_root)
    compiled_path = Path(str(compile_payload["compiled_path"]))
    if isinstance(draft, ScenarioDraft):
        from fp_wraptr.scenarios.runner import load_scenario_config

        config = load_scenario_config(compiled_path)
        result = run_scenario(config, output_dir=Path(output_dir))
        run_dir = result.output_dir
        link = WorkspaceRunLink(
            run_kind="scenario",
            label=f"{draft.label} run",
            run_dir=str(run_dir),
            details={"compiled_path": str(compiled_path), "success": result.success},
        )
        summary = {
            "workspace_id": draft.workspace_id or workspace_id,
            "run_kind": "scenario",
            "run_dir": str(run_dir),
            "success": result.success,
            "chart_path": str(result.chart_path) if result.chart_path else None,
        }
    else:
        from fp_wraptr.scenarios.bundle import BundleConfig

        bundle = BundleConfig.from_yaml(compiled_path)
        result = run_bundle(bundle, output_dir=Path(output_dir))
        run_entries = [
            {
                "variant_name": entry.variant_name,
                "success": entry.success,
                "output_dir": str(entry.output_dir) if entry.output_dir else None,
                "error": entry.error,
            }
            for entry in result.entries
        ]
        run_dir = Path(output_dir).resolve()
        link = WorkspaceRunLink(
            run_kind="bundle",
            label=f"{draft.label} bundle run",
            run_dir=str(run_dir),
            details={"compiled_path": str(compiled_path), "variant_count": len(result.entries)},
        )
        summary = {
            "workspace_id": draft.workspace_id or workspace_id,
            "run_kind": "bundle",
            "run_dir": str(run_dir),
            "variant_count": len(result.entries),
            "bundle_name": result.bundle_name,
            "entries": run_entries,
        }
    draft.linked_runs.insert(0, link)
    draft.linked_runs = draft.linked_runs[:20]
    draft.recipe_history.append(
        WorkspaceOperation(
            operation="run_workspace",
            summary=f"Ran workspace {draft.workspace_id or workspace_id}",
            details={"run_dir": link.run_dir, "run_kind": link.run_kind},
        )
    )
    save_workspace_draft(draft, repo_root=repo_root, workspace=path.parent)
    summary["linked_runs"] = [item.model_dump(mode="json") for item in draft.linked_runs[:5]]
    return summary


def compare_workspace_runs(
    *,
    repo_root: Path | str,
    workspace_id: str,
    run_a: str = "",
    run_b: str = "",
    top_n: int = 10,
) -> dict[str, object]:
    draft, path = load_workspace_draft_by_id(workspace_id, repo_root=repo_root)
    left, right = _resolve_compare_pair(draft, run_a=run_a, run_b=run_b)
    payload = diff_run_dirs(Path(left), Path(right), top_n=top_n)
    draft.linked_runs.insert(
        0,
        WorkspaceRunLink(
            run_kind="comparison",
            label=f"Compare {Path(left).name} vs {Path(right).name}",
            run_dir=str(Path(right).resolve()),
            details={"run_a": str(left), "run_b": str(right), "top_n": top_n},
        ),
    )
    draft.recipe_history.append(
        WorkspaceOperation(
            operation="compare_workspace_runs",
            summary="Compared linked runs",
            details={"run_a": str(left), "run_b": str(right), "top_n": top_n},
        )
    )
    save_workspace_draft(draft, repo_root=repo_root, workspace=path.parent)
    payload["workspace_id"] = draft.workspace_id or workspace_id
    return payload


def list_visualizations(
    *,
    repo_root: Path | str,
    workspace_id: str = "",
    pack_id: str = "",
) -> list[dict[str, object]]:
    repo_root = Path(repo_root).resolve()
    manifest_payload: dict[str, object] | None = None
    if pack_id.strip():
        from fp_wraptr.scenarios.packs import describe_pack_manifest

        manifest_payload = describe_pack_manifest(pack_id.strip(), repo_root=repo_root)
    elif workspace_id.strip():
        draft, _path = load_workspace_draft_by_id(workspace_id, repo_root=repo_root)
        manifest_payload = _pack_for_family(draft.family, repo_root=repo_root)
        variables = list(getattr(draft, "track_variables", []))
    else:
        variables = []
    if workspace_id.strip():
        draft, _path = load_workspace_draft_by_id(workspace_id, repo_root=repo_root)
        variables = list(getattr(draft, "track_variables", []))
    elif manifest_payload is not None:
        variables = list({
            item
            for view in manifest_payload.get("visualizations", [])
            if isinstance(view, dict)
            for item in view.get("variables", [])
        })
    builtins = [
        {
            "view_id": "forecast-overlay",
            "label": "Forecast overlay",
            "chart_type": "forecast_overlay",
            "description": "Overlay tracked variables across recent related runs.",
            "variables": variables[:6],
        },
        {
            "view_id": "delta-table",
            "label": "Delta table",
            "chart_type": "delta_table",
            "description": "Largest movers between the latest two runs.",
            "variables": variables[:10],
        },
    ]
    if manifest_payload is None:
        return builtins
    manifest_views = manifest_payload.get("visualizations", [])
    if isinstance(manifest_views, list) and manifest_views:
        return builtins + [item for item in manifest_views if isinstance(item, dict)]
    return builtins


def build_visualization_view(
    *,
    repo_root: Path | str,
    view_id: str,
    workspace_id: str = "",
    pack_id: str = "",
    run_dirs: list[str] | None = None,
) -> dict[str, object]:
    repo_root = Path(repo_root).resolve()
    available = list_visualizations(
        repo_root=repo_root, workspace_id=workspace_id, pack_id=pack_id
    )
    selected = next((item for item in available if item.get("view_id") == view_id), None)
    if selected is None:
        raise ValueError(f"Visualization view not found: {view_id}")
    resolved_run_dirs = [Path(item).resolve() for item in (run_dirs or []) if str(item).strip()]
    if not resolved_run_dirs and workspace_id.strip():
        draft, _path = load_workspace_draft_by_id(workspace_id, repo_root=repo_root)
        resolved_run_dirs = [
            Path(item.run_dir).resolve()
            for item in draft.linked_runs
            if item.run_kind in {"scenario", "bundle"}
        ][:4]
    if not resolved_run_dirs:
        latest = latest_runs(scan_artifacts(repo_root / "artifacts"), limit=4, has_output=True)
        resolved_run_dirs = [item.run_dir.resolve() for item in latest]
    variables = [
        str(item).strip().upper() for item in selected.get("variables", []) if str(item).strip()
    ]
    run_payloads: list[dict[str, object]] = []
    for run_dir in resolved_run_dirs:
        fmout = run_dir / "fmout.txt"
        if not fmout.exists():
            continue
        parsed = parse_fp_output(fmout)
        if not variables:
            variables = list(parsed.variables.keys())[:5]
        series_payload: dict[str, object] = {}
        for variable in variables:
            item = parsed.variables.get(variable)
            if item is None:
                continue
            series_payload[variable] = {
                "periods": list(parsed.periods),
                "levels": list(item.levels),
                "changes": list(item.changes),
                "pct_changes": list(item.pct_changes),
            }
        run_payloads.append({
            "run_dir": str(run_dir),
            "label": run_dir.name,
            "variables": series_payload,
        })
    return {
        "view_id": selected["view_id"],
        "label": selected["label"],
        "chart_type": selected.get("chart_type", "forecast_overlay"),
        "description": selected.get("description", ""),
        "workspace_id": workspace_id or None,
        "pack_id": pack_id or None,
        "variables": variables,
        "runs": run_payloads,
        "suggested_dashboard_pages": ["0_Run_Panels", "2_Compare_Runs", "3_New_Run"],
    }


def _retitle_workspace(
    draft: ScenarioDraft | BundleDraft,
    *,
    workspace_slug: str,
    label: str,
) -> ScenarioDraft | BundleDraft:
    if not workspace_slug.strip() and not label.strip():
        return draft
    slug = workspace_slug.strip() or draft.slug
    updates: dict[str, object] = {
        "slug": slug,
        "workspace_id": workspace_id_for(family=draft.family, slug=slug),
    }
    if label.strip():
        updates["label"] = label.strip()
    return draft.model_copy(update=updates)


def _workspace_payload(
    draft: ScenarioDraft | BundleDraft,
    *,
    workspace_path: Path,
) -> dict[str, object]:
    return {
        "workspace_id": draft.workspace_id
        or workspace_id_for(family=draft.family, slug=draft.slug),
        "draft_kind": draft.draft_kind,
        "family": draft.family,
        "slug": draft.slug,
        "label": draft.label,
        "description": draft.description,
        "workspace_path": str(workspace_path),
        "forecast_start": draft.forecast_start,
        "forecast_end": draft.forecast_end,
        "backend": draft.backend,
        "track_variables": list(draft.track_variables),
        "card_count": len(draft.cards),
        "variant_count": len(draft.variants) if isinstance(draft, BundleDraft) else 0,
        "variants": (
            [
                {
                    "variant_id": item.variant_id,
                    "label": item.label,
                    "scenario_name": item.scenario_name or item.variant_id,
                    "input_file": item.input_file,
                    "enabled": item.enabled,
                    "card_count": len(item.cards),
                }
                for item in draft.variants
            ]
            if isinstance(draft, BundleDraft)
            else []
        ),
        "recipe_history": [item.model_dump(mode="json") for item in draft.recipe_history[-10:]],
        "linked_runs": [item.model_dump(mode="json") for item in draft.linked_runs[:10]],
        "extra": dict(draft.extra),
    }


def _target_cards(draft: ScenarioDraft | BundleDraft, *, variant_id: str) -> list[CardInstance]:
    if not variant_id.strip():
        return draft.cards
    if not isinstance(draft, BundleDraft):
        raise ValueError("variant_id is only supported for bundle workspaces")
    variant = next(
        (item for item in draft.variants if item.variant_id == variant_id.strip()), None
    )
    if variant is None:
        raise ValueError(f"Variant not found: {variant_id}")
    return variant.cards


def _find_or_create_card(cards: list[CardInstance], card_id: str) -> CardInstance:
    wanted = str(card_id).strip()
    if not wanted:
        raise ValueError("card_id must be non-empty")
    for card in cards:
        if card.card_id == wanted:
            return card
    card = CardInstance(card_id=wanted)
    cards.append(card)
    return card


def _resolve_compare_pair(
    draft: ScenarioDraft | BundleDraft, *, run_a: str, run_b: str
) -> tuple[Path, Path]:
    if run_a.strip() and run_b.strip():
        return Path(run_a).resolve(), Path(run_b).resolve()
    scenario_runs = [item for item in draft.linked_runs if item.run_kind in {"scenario", "bundle"}]
    if len(scenario_runs) < 2:
        raise ValueError("Need at least two linked scenario/bundle runs to compare")
    return Path(scenario_runs[1].run_dir).resolve(), Path(scenario_runs[0].run_dir).resolve()


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _pack_for_family(family: str, *, repo_root: Path) -> dict[str, object] | None:
    from fp_wraptr.scenarios.packs import describe_pack_manifest, load_pack_manifests

    for manifest, _path in load_pack_manifests(repo_root=repo_root):
        if manifest.family == family:
            return describe_pack_manifest(manifest.pack_id, repo_root=repo_root)
    return None
