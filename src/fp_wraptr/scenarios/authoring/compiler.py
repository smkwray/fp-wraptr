"""Compilation helpers for managed scenario and bundle authoring workspaces."""

from __future__ import annotations

import csv
import io
import json
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pandas as pd
import yaml
from pydantic import ValidationError

from fp_wraptr.data.series_pipeline.fp_targets import write_fmexog_override, write_include_changevar
from fp_wraptr.data.series_pipeline.periods import normalize_period_token
from fp_wraptr.hygiene import find_project_root
from fp_wraptr.scenarios.bundle import BundleConfig, VariantSpec
from fp_wraptr.scenarios.catalog import load_scenario_catalog
from fp_wraptr.scenarios.config import ScenarioConfig
from fp_wraptr.scenarios.input_tree import scan_input_tree_symbols
from fp_wraptr.scenarios.runner import load_scenario_config

from .models import (
    AttachRule,
    BundleDraft,
    CardInstance,
    CardSpec,
    ConstantFieldSpec,
    DeckConstantsCardSpec,
    DraftSourceRef,
    SeriesCardSpec,
    SeriesTargetSpec,
    ScenarioDraft,
)
from .workspace import resolve_source_path, workspace_paths

CREATE_RE = re.compile(
    r"^(?P<indent>\s*)CREATE\s+(?P<symbol>[A-Za-z0-9_]+)\s*=\s*(?P<value>[^;]+)(?P<suffix>\s*;.*)$",
    re.IGNORECASE,
)
INPUT_RE = re.compile(r"(?im)^(?P<prefix>\s*INPUT\s+FILE\s*=\s*)(?P<name>[^;\r\n]+)(?P<suffix>\s*;.*)$")


@dataclass(frozen=True)
class CompileResult:
    """Compilation outcome for one authored workspace."""

    draft_kind: str
    workspace_dir: Path
    overlay_dir: Path
    compiled_path: Path
    report_path: Path
    generated_files: tuple[Path, ...]
    errors: tuple[str, ...]
    scenario_config: ScenarioConfig | None = None
    bundle_config: BundleConfig | None = None

    @property
    def ok(self) -> bool:
        return not self.errors


def default_cards_root(repo_root: Path | str) -> Path:
    return Path(repo_root).resolve() / "projects_local" / "cards"


def load_card_specs(
    *,
    repo_root: Path | str,
    family: str | None = None,
) -> list[CardSpec]:
    repo_root = Path(repo_root).resolve()
    cards_root = default_cards_root(repo_root)
    if not cards_root.exists():
        return []
    payloads: list[CardSpec] = []
    pattern = cards_root.glob("**/*.yaml") if family is None else (cards_root / family).glob("*.yaml")
    for path in sorted(pattern):
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            continue
        kind = raw.get("kind")
        if kind not in {"deck_constants", "series_card"}:
            continue
        if family is not None and str(raw.get("family", "")).strip() != family:
            continue
        if kind == "deck_constants":
            spec = cast(CardSpec, DeckConstantsCardSpec.model_validate(raw))
        else:
            spec = cast(CardSpec, _validate_series_card_spec(raw))
        payloads.append(spec)
    return sorted(payloads, key=lambda item: (item.family.lower(), item.order, item.card_id))


def _validate_series_card_spec(raw: dict[str, object]) -> SeriesCardSpec:
    try:
        return SeriesCardSpec.model_validate(raw)
    except ValidationError as exc:
        # Older long-lived app processes may still hold a pre-ADDDIFABS schema.
        target_rows = raw.get("targets")
        if not isinstance(target_rows, list):
            raise
        if not any(str(item.get("fp_method", "")).strip().upper() == "ADDDIFABS" for item in target_rows if isinstance(item, dict)):
            raise
        targets: list[SeriesTargetSpec] = []
        for item in target_rows:
            if not isinstance(item, dict):
                raise exc
            attach_raw = item.get("attach_rule")
            attach_rule = (
                AttachRule.model_validate(attach_raw)
                if isinstance(attach_raw, dict)
                else AttachRule()
            )
            targets.append(
                SeriesTargetSpec.model_construct(
                    kind=str(item.get("kind", "fmexog_override")),
                    label=str(item.get("label", "")),
                    output_path=str(item.get("output_path", "")),
                    attach_rule=attach_rule,
                    fp_method=str(item.get("fp_method", "SAMEVALUE")).strip().upper(),
                    mode=str(item.get("mode", "series")),
                    layer_on_base=bool(item.get("layer_on_base", True)),
                )
            )
        return SeriesCardSpec.model_construct(
            kind="series_card",
            card_id=str(raw.get("card_id", "")),
            family=str(raw.get("family", "")),
            label=str(raw.get("label", "")),
            description=str(raw.get("description", "")),
            order=int(raw.get("order", 0) or 0),
            variable=str(raw.get("variable", "")).strip().upper(),
            input_modes=list(raw.get("input_modes", ["csv", "paste"])),
            default_target=str(raw.get("default_target", "include_changevar")),
            targets=targets,
        )


def card_specs_by_id(*, repo_root: Path | str, family: str | None = None) -> dict[str, CardSpec]:
    return {spec.card_id: spec for spec in load_card_specs(repo_root=repo_root, family=family)}


def initialize_card_instances(specs: list[CardSpec], existing: list[CardInstance]) -> list[CardInstance]:
    by_id = {item.card_id: item for item in existing}
    merged: list[CardInstance] = []
    for spec in specs:
        merged.append(by_id.get(spec.card_id, CardInstance(card_id=spec.card_id, enabled=False)))
    return merged


def load_series_points_from_text(text: str) -> dict[str, float]:
    content = str(text or "").strip()
    if not content:
        return {}
    reader = csv.reader(io.StringIO(content))
    rows = [row for row in reader if any(str(cell).strip() for cell in row)]
    if not rows:
        return {}
    if len(rows[0]) == 1:
        rows = [re.split(r"[\t ]+", row[0].strip()) for row in rows]
    header = [str(cell).strip().lower() for cell in rows[0]]
    start_idx = 1 if len(header) >= 2 and header[0] in {"period", "date"} else 0
    points: dict[str, float] = {}
    for row in rows[start_idx:]:
        if len(row) < 2:
            continue
        period_text = str(row[0]).strip()
        value_text = str(row[1]).strip()
        if not period_text or not value_text:
            continue
        period = normalize_period_token(period_text)
        points[period] = float(value_text)
    return dict(sorted(points.items()))


def load_series_points_from_csv(path: Path | str) -> dict[str, float]:
    csv_path = Path(path)
    frame = pd.read_csv(csv_path)
    if not {"period", "value"}.issubset(set(frame.columns)):
        raise ValueError("CSV imports must contain `period` and `value` columns")
    points: dict[str, float] = {}
    for _, row in frame.iterrows():
        period_text = str(row["period"]).strip()
        value = row["value"]
        if not period_text or pd.isna(value):
            continue
        points[normalize_period_token(period_text)] = float(value)
    return dict(sorted(points.items()))


def normalize_series_points(points: dict[str, float]) -> dict[str, float]:
    normalized = {normalize_period_token(period): float(value) for period, value in points.items()}
    ordered = dict(sorted(normalized.items()))
    if not ordered:
        return {}
    expected = _period_range(next(iter(ordered)), next(reversed(ordered)))
    if list(ordered.keys()) != expected:
        raise ValueError("series inputs must cover a contiguous quarterly range")
    return ordered


def resolve_card_defaults(
    draft: ScenarioDraft | BundleDraft,
    *,
    repo_root: Path | str,
) -> dict[str, dict[str, float]]:
    base_config = _resolve_base_config_for_draft(draft, repo_root=repo_root)
    search_dirs: list[Path] = []
    if base_config.input_overlay_dir:
        search_dirs.append(Path(base_config.input_overlay_dir))
    search_dirs.append(Path(base_config.fp_home))
    defaults: dict[str, dict[str, float]] = {}
    for spec in load_card_specs(repo_root=repo_root, family=draft.family):
        if isinstance(spec, DeckConstantsCardSpec):
            fields: dict[str, float] = {}
            for file_spec in spec.files:
                source = _find_source_path(file_spec.path, search_dirs)
                if source is None:
                    continue
                parsed = parse_create_assignments(source.read_text(encoding="utf-8", errors="replace"))
                for group in file_spec.groups:
                    for field in group.fields:
                        if field.symbol in parsed:
                            fields[field.symbol] = parsed[field.symbol]
            defaults[spec.card_id] = fields
    return defaults


def parse_create_assignments(text: str) -> dict[str, float]:
    values: dict[str, float] = {}
    for line in text.splitlines():
        match = CREATE_RE.match(line)
        if match is None:
            continue
        symbol = match.group("symbol").strip().upper()
        raw_value = match.group("value").strip()
        try:
            values[symbol] = float(raw_value)
        except ValueError:
            continue
    return values


def rewrite_create_assignments(text: str, updates: dict[str, float]) -> tuple[str, list[str]]:
    wanted = {key.upper(): float(value) for key, value in updates.items()}
    found: set[str] = set()
    lines: list[str] = []
    for line in text.splitlines():
        match = CREATE_RE.match(line)
        if match is None:
            lines.append(line)
            continue
        symbol = match.group("symbol").strip().upper()
        if symbol not in wanted:
            lines.append(line)
            continue
        found.add(symbol)
        indent = match.group("indent")
        suffix = match.group("suffix")
        value_text = f"{wanted[symbol]:.12g}"
        lines.append(f"{indent}CREATE {symbol}={value_text}{suffix}")
    missing = sorted(set(wanted) - found)
    return "\n".join(lines) + ("\n" if text.endswith("\n") else ""), missing


def apply_attach_rule(
    *,
    overlay_dir: Path,
    generated_path: Path,
    rule: AttachRule,
) -> None:
    if rule.kind == "overlay_file":
        return
    target = overlay_dir / str(rule.target_file)
    if not target.exists():
        raise FileNotFoundError(f"Attach target not found in overlay: {target}")
    text = target.read_text(encoding="utf-8", errors="replace")
    statement = rule.statement or f"INPUT FILE={generated_path.name};"
    if rule.kind == "replace_include":
        match = INPUT_RE.search(text)
        if match is None:
            raise ValueError(f"No INPUT FILE directive found in {target}")
        text = INPUT_RE.sub(
            lambda item: f"{item.group('prefix')}{generated_path.name}{item.group('suffix')}",
            text,
            count=1,
        )
    elif rule.kind == "append_include_after_match":
        marker = str(rule.match_text or "")
        if marker not in text:
            raise ValueError(f"Attach marker {marker!r} not found in {target}")
        text = text.replace(marker, f"{marker}\n{statement}", 1)
    elif rule.kind == "append_include_before_return":
        return_match = re.search(r"(?im)^\s*RETURN;\s*$", text)
        if return_match is None:
            raise ValueError(f"RETURN; not found in {target}")
        text = text[: return_match.start()] + f"{statement}\n" + text[return_match.start() :]
    else:  # pragma: no cover - guarded by model validation
        raise ValueError(f"Unsupported attach rule: {rule.kind}")
    target.write_text(text, encoding="utf-8")


def compile_scenario_workspace(
    draft: ScenarioDraft,
    *,
    repo_root: Path | str,
    workspace_dir: Path | str,
) -> CompileResult:
    base_config = _resolve_base_config_for_draft(draft, repo_root=repo_root)
    return _compile_scenario_from_base_config(
        draft,
        base_config=base_config,
        repo_root=repo_root,
        workspace_dir=workspace_dir,
    )


def _compile_scenario_from_base_config(
    draft: ScenarioDraft,
    *,
    base_config: ScenarioConfig,
    repo_root: Path | str,
    workspace_dir: Path | str,
) -> CompileResult:
    repo_root = Path(repo_root).resolve()
    workspace_dir = Path(workspace_dir).resolve()
    paths = workspace_paths(workspace_dir)
    _reset_compile_dirs(paths)
    overlay_dir = paths["overlay"]
    staged = _stage_source_tree_for_compile(
        base_config,
        overlay_dir=overlay_dir,
        entry_name=_compiled_entry_name(draft.slug),
        input_file_override=draft.input_file_override,
    )
    spec_map = card_specs_by_id(repo_root=repo_root, family=draft.family)
    errors: list[str] = []
    generated_files: list[Path] = list(staged["copied"])
    for instance in draft.cards:
        if not instance.enabled:
            continue
        spec = spec_map.get(instance.card_id)
        if spec is None:
            errors.append(f"Unknown card_id: {instance.card_id}")
            continue
        try:
            generated_files.extend(
                _apply_card_instance(
                    instance,
                    spec=spec,
                    overlay_dir=overlay_dir,
                    fp_home=Path(base_config.fp_home),
                    imports_dir=paths["imports"],
                )
            )
        except Exception as exc:
            errors.append(f"{instance.card_id}: {exc}")
    scenario_config = base_config.model_copy(deep=True)
    scenario_config.name = draft.scenario_name
    scenario_config.description = draft.description
    scenario_config.forecast_start = draft.forecast_start
    scenario_config.forecast_end = draft.forecast_end
    scenario_config.backend = draft.backend
    scenario_config.fppy = dict(draft.fppy or {})
    scenario_config.track_variables = list(draft.track_variables)
    scenario_config.input_overlay_dir = overlay_dir.resolve()
    scenario_config.input_file = cast(str, staged["entry_name"])
    scenario_config.input_patches = {}
    compiled_path = paths["compiled"] / "scenario.yaml"
    scenario_config.to_yaml(compiled_path)
    report_path = _write_compile_report(
        paths["report"],
        draft_kind="scenario",
        workspace_dir=workspace_dir,
        compiled_path=compiled_path,
        overlay_dir=overlay_dir,
        errors=errors,
        generated_files=generated_files,
    )
    return CompileResult(
        draft_kind="scenario",
        workspace_dir=workspace_dir,
        overlay_dir=overlay_dir,
        compiled_path=compiled_path,
        report_path=report_path,
        generated_files=tuple(sorted(set(path.resolve() for path in generated_files))),
        errors=tuple(errors),
        scenario_config=scenario_config,
    )


def compile_bundle_workspace(
    draft: BundleDraft,
    *,
    repo_root: Path | str,
    workspace_dir: Path | str,
) -> CompileResult:
    repo_root = Path(repo_root).resolve()
    workspace_dir = Path(workspace_dir).resolve()
    paths = workspace_paths(workspace_dir)
    _reset_compile_dirs(paths)
    bundle_source = BundleConfig.from_yaml(resolve_source_path(draft.source, repo_root=repo_root, expected_kind="bundle"))
    base_config = ScenarioConfig(**bundle_source.base)
    generated_files: list[Path] = []
    errors: list[str] = []
    variant_specs: list[VariantSpec] = []
    first_variant_base: ScenarioConfig | None = None
    variant_dirs_root = paths["compiled"] / "variants"
    variant_dirs_root.mkdir(parents=True, exist_ok=True)
    shared_cards = {card.card_id: card for card in draft.cards}
    for variant in draft.variants:
        if not variant.enabled:
            continue
        derived = ScenarioDraft(
            family=draft.family,
            slug=f"{draft.slug}-{variant.variant_id}",
            label=variant.label,
            description=draft.description,
            source=DraftSourceRef(kind="path", value=str(resolve_source_path(draft.source, repo_root=repo_root, expected_kind="bundle"))),
            scenario_name=str(variant.scenario_name or f"{draft.bundle_name}_{variant.variant_id}"),
            forecast_start=draft.forecast_start,
            forecast_end=draft.forecast_end,
            backend=draft.backend,
            fppy=dict(draft.fppy or {}),
            track_variables=list(draft.track_variables),
            cards=list(shared_cards.values()) + list(variant.cards),
            input_file_override=variant.input_file,
        )
        variant_dir = variant_dirs_root / variant.variant_id
        variant_base = base_config.model_copy(deep=True)
        if variant.input_file:
            variant_base.input_file = variant.input_file
        result = _compile_scenario_from_base_config(
            derived,
            base_config=variant_base,
            repo_root=repo_root,
            workspace_dir=variant_dir,
        )
        generated_files.extend(result.generated_files)
        errors.extend(result.errors)
        if result.scenario_config is None:
            continue
        if first_variant_base is None:
            first_variant_base = result.scenario_config.model_copy(deep=True)
            first_variant_base.name = draft.bundle_name
        variant_specs.append(
            VariantSpec(
                name=variant.variant_id,
                scenario_name=result.scenario_config.name,
                patch={
                    "input_file": result.scenario_config.input_file,
                    "input_overlay_dir": result.scenario_config.input_overlay_dir,
                    "forecast_start": result.scenario_config.forecast_start,
                    "forecast_end": result.scenario_config.forecast_end,
                    "backend": result.scenario_config.backend,
                    "track_variables": result.scenario_config.track_variables,
                },
            )
        )
    if first_variant_base is None:
        errors.append("No enabled bundle variants to compile")
        first_variant_base = base_config
    bundle_config = BundleConfig(
        base=first_variant_base.model_dump(mode="json"),
        variants=variant_specs,
        focus_variables=list(draft.track_variables[:5] or bundle_source.focus_variables),
    )
    compiled_path = paths["compiled"] / "bundle.yaml"
    compiled_path.write_text(
        yaml.safe_dump(bundle_config.model_dump(mode="json"), sort_keys=False),
        encoding="utf-8",
    )
    report_path = _write_compile_report(
        paths["report"],
        draft_kind="bundle",
        workspace_dir=workspace_dir,
        compiled_path=compiled_path,
        overlay_dir=variant_dirs_root,
        errors=errors,
        generated_files=generated_files,
    )
    return CompileResult(
        draft_kind="bundle",
        workspace_dir=workspace_dir,
        overlay_dir=variant_dirs_root,
        compiled_path=compiled_path,
        report_path=report_path,
        generated_files=tuple(sorted(set(path.resolve() for path in generated_files))),
        errors=tuple(errors),
        bundle_config=bundle_config,
    )


def _apply_card_instance(
    instance: CardInstance,
    *,
    spec: CardSpec,
    overlay_dir: Path,
    fp_home: Path,
    imports_dir: Path,
) -> list[Path]:
    if isinstance(spec, DeckConstantsCardSpec):
        return _apply_deck_constants(instance, spec=spec, overlay_dir=overlay_dir)
    return _apply_series_card(instance, spec=spec, overlay_dir=overlay_dir, fp_home=fp_home, imports_dir=imports_dir)


def _apply_deck_constants(
    instance: CardInstance,
    *,
    spec: DeckConstantsCardSpec,
    overlay_dir: Path,
) -> list[Path]:
    written: list[Path] = []
    updates = {key.upper(): float(value) for key, value in instance.constants.items()}
    for file_spec in spec.files:
        target = overlay_dir / file_spec.path
        if not target.exists():
            raise FileNotFoundError(f"Deck constant file not found in overlay: {target}")
        relevant_symbols = {
            field.symbol
            for group in file_spec.groups
            for field in group.fields
        }
        relevant_updates = {symbol: value for symbol, value in updates.items() if symbol in relevant_symbols}
        if not relevant_updates:
            continue
        rewritten, missing = rewrite_create_assignments(
            target.read_text(encoding="utf-8", errors="replace"),
            relevant_updates,
        )
        if missing:
            raise ValueError(f"Missing CREATE assignments in {file_spec.path}: {', '.join(missing)}")
        target.write_text(rewritten, encoding="utf-8")
        written.append(target)
    return written


def _apply_series_card(
    instance: CardInstance,
    *,
    spec: SeriesCardSpec,
    overlay_dir: Path,
    fp_home: Path,
    imports_dir: Path,
) -> list[Path]:
    selected = instance.selected_target or spec.default_target
    target_spec = next((target for target in spec.targets if target.kind == selected), None)
    if target_spec is None:
        raise ValueError(f"Unknown selected target {selected!r} for card {spec.card_id}")
    points = dict(instance.series_points)
    if instance.import_path and not points:
        import_path = imports_dir / instance.import_path
        if not import_path.exists():
            raise FileNotFoundError(f"Import file not found: {import_path}")
        points = load_series_points_from_csv(import_path)
    elif instance.pasted_text and not points:
        points = load_series_points_from_text(instance.pasted_text)
    points = normalize_series_points(points)
    if not points:
        raise ValueError("series card has no normalized quarterly points")
    output_path = overlay_dir / target_spec.output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _write_series_target(output_path, points, target_spec=target_spec, variable=spec.variable, fp_home=fp_home)
    apply_attach_rule(overlay_dir=overlay_dir, generated_path=output_path, rule=target_spec.attach_rule)
    return [output_path]


def _write_series_target(
    output_path: Path,
    points: dict[str, float],
    *,
    target_spec: SeriesTargetSpec,
    variable: str,
    fp_home: Path,
) -> None:
    periods = list(points.keys())
    values = list(points.values())
    start = periods[0]
    end = periods[-1]
    if target_spec.kind == "include_changevar":
        write_include_changevar(
            out_paths=[output_path],
            variable=variable,
            fp_method=target_spec.fp_method,
            smpl_start=start,
            smpl_end=end,
            values=values,
            mode=target_spec.mode,
        )
        return
    write_fmexog_override(
        out_path=output_path,
        variable=variable,
        fp_method=target_spec.fp_method,
        smpl_start=start,
        smpl_end=end,
        values=values,
        base_fmexog=output_path if output_path.exists() else fp_home / "fmexog.txt",
        layer_on_base=target_spec.layer_on_base,
    )


def _resolve_base_config_for_draft(
    draft: ScenarioDraft | BundleDraft,
    *,
    repo_root: Path | str,
) -> ScenarioConfig:
    repo_root = Path(repo_root).resolve()
    if isinstance(draft, ScenarioDraft):
        path = resolve_source_path(draft.source, repo_root=repo_root, expected_kind="scenario")
        return load_scenario_config(path)
    path = resolve_source_path(draft.source, repo_root=repo_root, expected_kind="bundle")
    bundle = BundleConfig.from_yaml(path)
    return ScenarioConfig(**bundle.base)


def _resolve_entry_source(config: ScenarioConfig) -> Path:
    fp_candidate = Path(config.fp_home) / (config.input_file)
    if fp_candidate.exists():
        return fp_candidate
    overlay_dir = Path(config.input_overlay_dir) if config.input_overlay_dir else None
    if overlay_dir is not None:
        overlay_candidate = overlay_dir / config.input_file
        if overlay_candidate.exists():
            return overlay_candidate
    raise FileNotFoundError(f"Scenario input file not found: {config.input_file}")


def _stage_source_tree_for_compile(
    config: ScenarioConfig,
    *,
    overlay_dir: Path | None,
    entry_name: str,
    input_file_override: str | None = None,
) -> dict[str, object]:
    overlay_root = Path(overlay_dir).resolve() if overlay_dir is not None else Path.cwd()
    overlay_root.mkdir(parents=True, exist_ok=True)
    source_config = config.model_copy(deep=True)
    if input_file_override:
        source_config.input_file = str(input_file_override)
    symbols = scan_input_tree_symbols(
        entry_input_file=source_config.input_file,
        overlay_dir=source_config.input_overlay_dir,
        fp_home=Path(source_config.fp_home),
    )
    search_dirs: list[Path] = []
    if source_config.input_overlay_dir:
        search_dirs.append(Path(source_config.input_overlay_dir))
    search_dirs.append(Path(source_config.fp_home))
    copied: list[Path] = []
    files: dict[str, Path] = {}
    entry_source = _resolve_entry_source(source_config)
    entry_target = overlay_root / entry_name
    shutil.copy2(entry_source, entry_target)
    copied.append(entry_target)
    files[source_config.input_file.lower()] = entry_target
    for name in symbols.include_files:
        src = _find_source_path(name, search_dirs)
        if src is None:
            raise FileNotFoundError(f"Missing source include: {name}")
        target = overlay_root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, target)
        copied.append(target)
        files[name.lower()] = target
    return {"entry_name": entry_name, "copied": copied, "files": files}


def _compiled_entry_name(slug: str) -> str:
    token = re.sub(r"[^a-z0-9]", "", slug.lower())[:8] or "draft"
    return f"au{token}.txt"


def _period_range(start: str, end: str) -> list[str]:
    start_year, start_quarter = [int(part) for part in start.split(".")]
    end_year, end_quarter = [int(part) for part in end.split(".")]
    out: list[str] = []
    year = start_year
    quarter = start_quarter
    while (year, quarter) <= (end_year, end_quarter):
        out.append(f"{year}.{quarter}")
        quarter += 1
        if quarter > 4:
            quarter = 1
            year += 1
    return out


def _find_source_path(name: str, search_dirs: list[Path]) -> Path | None:
    for directory in search_dirs:
        candidate = directory / name
        if candidate.exists():
            return candidate
        want = name.lower()
        try:
            for child in directory.iterdir():
                if child.name.lower() == want:
                    return child
        except OSError:
            continue
    return None


def _reset_compile_dirs(paths: dict[str, Path]) -> None:
    paths["imports"].mkdir(parents=True, exist_ok=True)
    for key in ("overlay", "compiled"):
        shutil.rmtree(paths[key], ignore_errors=True)
        paths[key].mkdir(parents=True, exist_ok=True)


def _write_compile_report(
    report_path: Path,
    *,
    draft_kind: str,
    workspace_dir: Path,
    compiled_path: Path,
    overlay_dir: Path,
    errors: list[str],
    generated_files: list[Path],
) -> Path:
    payload = {
        "draft_kind": draft_kind,
        "workspace_dir": str(workspace_dir),
        "compiled_path": str(compiled_path),
        "overlay_dir": str(overlay_dir),
        "errors": list(errors),
        "generated_files": sorted({str(path) for path in generated_files}),
    }
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report_path
