"""Portable GitHub Pages export pipeline for read-only run browsing."""

from __future__ import annotations

import json
import math
import re
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha1
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

from fp_wraptr.dashboard.artifacts import RunArtifact, overlay_paths_for_runs, scan_artifacts
from fp_wraptr.dashboard.mini_dash_helpers import (
    normalize_preset_transforms,
    normalize_preset_variables,
)
from fp_wraptr.data.dictionary_overlays import load_dictionary_with_overlays
from fp_wraptr.hygiene import find_project_root
from fp_wraptr.io.input_parser import parse_fmexog_text, parse_fp_input
from fp_wraptr.io.loadformat import add_derived_series, read_loadformat
from fp_wraptr.pages_compilers import PagesCompilerError, apply_public_run_compilers

SCHEMA_VERSION = 1
STATIC_SITE_SUBPATH = "model-runs"
ALLOWED_SITE_SUBPATHS = {STATIC_SITE_SUBPATH, "gender-runs"}
DEFAULT_SPEC_PATH = Path("public") / "model-runs.spec.yaml"
_RUN_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_WINDOWS_ABSOLUTE_RE = re.compile(r"^[A-Za-z]:[\\/]")
_PERIOD_TOKEN_RE = re.compile(r"^(?P<year>\d{4})\.(?P<sub>\d+)$")
_INPUT_VAR_RE = re.compile(r"\b([A-Z][A-Z0-9_]{1,})\b")
_DIRECT_OVERLAY_CONTROL_VARIABLES = {
    "INTF",
    "INTS",
    "IVA",
    "JGCOLA",
    "MWCOLA",
    "JGHILO",
    "JGPHASE",
    "JGSWITCH",
    "STAT",
    "UBGH",
}
_PUBLIC_MISSING_SENTINELS = frozenset((-99.0,))

__all__ = [
    "DEFAULT_SPEC_PATH",
    "PAGES_EXPORT_DEFAULT_OUT_DIR",
    "PagesExportError",
    "PagesExportResult",
    "PagesExportSpec",
    "default_pages_export_spec_path",
    "export_pages_bundle",
    "load_pages_export_spec",
]

PAGES_EXPORT_DEFAULT_OUT_DIR = Path("public") / STATIC_SITE_SUBPATH


class PagesExportError(RuntimeError):
    """Raised when a public run-export bundle cannot be built safely."""


class PagesRunSpec(BaseModel):
    """One public run selector resolved against latest matching artifacts."""

    run_id: str = Field(description="Stable public run id without timestamps")
    scenario_name: str = Field(description="Exact scenario name to resolve from artifacts")
    label: str = Field(default="", description="Optional display label for the site")
    summary: str = Field(default="", description="Optional one-line scenario summary")
    group: str = Field(default="", description="Optional run grouping label")
    family_id: str = Field(default="", description="Optional logical family id for horizon pairing")
    horizon_id: str = Field(default="", description="Optional horizon id such as 5y or 10y")
    horizon_label: str = Field(default="", description="Optional display horizon label such as 5Y")
    horizon_years: int | None = Field(default=None, description="Optional numeric horizon length")
    details: list[str] = Field(
        default_factory=list, description="Optional scenario detail bullets"
    )
    public_metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Optional extra public metadata copied into both manifest and run payload",
    )

    @field_validator("run_id")
    @classmethod
    def _validate_run_id(cls, value: str) -> str:
        token = str(value or "").strip()
        if not token or not _RUN_ID_RE.fullmatch(token):
            raise ValueError("run_id must match ^[a-z0-9][a-z0-9_-]*$")
        return token

    @field_validator("scenario_name")
    @classmethod
    def _validate_scenario_name(cls, value: str) -> str:
        token = str(value or "").strip()
        if not token:
            raise ValueError("scenario_name is required")
        return token

    @field_validator("label", "summary", "group", "family_id", "horizon_id", "horizon_label")
    @classmethod
    def _normalize_text(cls, value: str) -> str:
        return " ".join(str(value or "").split()).strip()

    @field_validator("family_id", "horizon_id")
    @classmethod
    def _validate_public_ids(cls, value: str) -> str:
        token = " ".join(str(value or "").split()).strip()
        if token and not _RUN_ID_RE.fullmatch(token):
            raise ValueError("family_id and horizon_id must match ^[a-z0-9][a-z0-9_-]*$")
        return token

    @field_validator("horizon_years")
    @classmethod
    def _validate_horizon_years(cls, value: int | None) -> int | None:
        if value is None:
            return None
        if int(value) <= 0:
            raise ValueError("horizon_years must be positive when provided")
        return int(value)

    @field_validator("details")
    @classmethod
    def _normalize_details(cls, value: list[str]) -> list[str]:
        out: list[str] = []
        for item in value or []:
            token = " ".join(str(item or "").split()).strip()
            if token:
                out.append(token)
        return out

    @field_validator("public_metadata")
    @classmethod
    def _normalize_public_metadata(cls, value: dict[str, Any]) -> dict[str, Any]:
        if value in (None, ""):
            return {}
        if not isinstance(value, dict):
            raise ValueError("public_metadata must be a mapping")
        return dict(value)

    @model_validator(mode="after")
    def _validate_metadata_collisions(self) -> PagesRunSpec:
        top_level_keys = {
            key
            for key, value in {
                "family_id": self.family_id,
                "horizon_id": self.horizon_id,
                "horizon_label": self.horizon_label,
                "horizon_years": self.horizon_years,
            }.items()
            if value not in (None, "")
        }
        collisions = sorted(top_level_keys.intersection(self.public_metadata.keys()))
        if collisions:
            formatted = ", ".join(collisions)
            raise ValueError(
                "public_metadata duplicates top-level horizon/family metadata: "
                f"{formatted}"
            )
        return self

    @property
    def resolved_label(self) -> str:
        label = str(self.label or "").strip()
        return label or self.scenario_name

    @property
    def resolved_public_metadata(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.family_id:
            payload["family_id"] = self.family_id
        if self.horizon_id:
            payload["horizon_id"] = self.horizon_id
        if self.horizon_label:
            payload["horizon_label"] = self.horizon_label
        if self.horizon_years is not None:
            payload["horizon_years"] = self.horizon_years
        payload.update(self.public_metadata)
        return payload


class PagesPresetSpec(BaseModel):
    """One public preset surfaced in the static explorer."""

    id: str = Field(description="Stable preset id")
    label: str = Field(description="Human-readable preset label")
    variables: list[str] = Field(default_factory=list)
    transforms: dict[str, dict[str, str]] = Field(default_factory=dict)
    run_comparisons: dict[str, dict[str, str]] = Field(default_factory=dict)

    @field_validator("id")
    @classmethod
    def _validate_id(cls, value: str) -> str:
        token = str(value or "").strip()
        if not token or not _RUN_ID_RE.fullmatch(token):
            raise ValueError("preset id must match ^[a-z0-9][a-z0-9_-]*$")
        return token

    @field_validator("label")
    @classmethod
    def _validate_label(cls, value: str) -> str:
        token = " ".join(str(value or "").split()).strip()
        if not token:
            raise ValueError("preset label is required")
        return token

    @model_validator(mode="after")
    def _normalize_fields(self) -> PagesPresetSpec:
        self.variables = normalize_preset_variables(self.variables)
        if not self.variables:
            raise ValueError("preset must include at least one variable")
        self.transforms = normalize_preset_transforms(
            variables=self.variables,
            transforms=self.transforms,
        )
        self.run_comparisons = _normalize_public_run_comparisons(
            variables=self.variables,
            run_comparisons=self.run_comparisons,
        )
        return self

    def to_public_record(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "id": self.id,
            "label": self.label,
            "variables": list(self.variables),
        }
        if self.transforms:
            payload["transforms"] = self.transforms
        if self.run_comparisons:
            payload["run_comparisons"] = self.run_comparisons
        return payload


class PagesExportSpec(BaseModel):
    """Checked-in export spec for one public model-runs bundle."""

    version: int = Field(default=SCHEMA_VERSION)
    title: str = Field(description="Public site title")
    site_subpath: str = Field(default=STATIC_SITE_SUBPATH)
    runs: list[PagesRunSpec] = Field(default_factory=list)
    default_run_ids: list[str] = Field(default_factory=list)
    presets: list[PagesPresetSpec] = Field(default_factory=list)
    default_preset_ids: list[str] = Field(default_factory=list)

    @field_validator("title", "site_subpath")
    @classmethod
    def _normalize_text(cls, value: str) -> str:
        return " ".join(str(value or "").split()).strip()

    @model_validator(mode="after")
    def _validate_spec(self) -> PagesExportSpec:
        if self.version != SCHEMA_VERSION:
            raise ValueError(f"unsupported pages export spec version: {self.version}")
        if self.site_subpath not in ALLOWED_SITE_SUBPATHS:
            allowed = "', '".join(sorted(ALLOWED_SITE_SUBPATHS))
            raise ValueError(f"site_subpath must be one of '{allowed}'")
        if not self.title:
            raise ValueError("title is required")
        if not self.runs:
            raise ValueError("spec must include at least one run")
        if not self.presets:
            raise ValueError("spec must include at least one preset")

        run_ids = [item.run_id for item in self.runs]
        if len(run_ids) != len(set(run_ids)):
            raise ValueError("run ids must be unique")

        preset_ids = [item.id for item in self.presets]
        if len(preset_ids) != len(set(preset_ids)):
            raise ValueError("preset ids must be unique")

        run_id_set = set(run_ids)
        invalid_reference_ids: set[str] = set()
        for preset in self.presets:
            for raw_cfg in preset.run_comparisons.values():
                reference_run_id = str(raw_cfg.get("reference_run_id", "") or "").strip()
                if reference_run_id and reference_run_id not in run_id_set:
                    invalid_reference_ids.add(reference_run_id)
        if invalid_reference_ids:
            formatted = ", ".join(sorted(invalid_reference_ids))
            raise ValueError(f"preset run comparisons reference unknown run ids: {formatted}")

        missing_default_runs = sorted(set(self.default_run_ids) - set(run_ids))
        if missing_default_runs:
            raise ValueError(
                f"default_run_ids not present in runs: {', '.join(missing_default_runs)}"
            )

        missing_default_presets = sorted(set(self.default_preset_ids) - set(preset_ids))
        if missing_default_presets:
            raise ValueError(
                f"default_preset_ids not present in presets: {', '.join(missing_default_presets)}"
            )
        return self


@dataclass(frozen=True)
class PagesExportResult:
    """Summary returned after a public model-runs bundle is exported."""

    out_dir: Path
    manifest_path: Path
    run_count: int
    variable_count: int
    generated_at: str


def default_pages_export_spec_path(start: Path | None = None) -> Path:
    """Return the default checked-in pages export spec path."""
    anchor = Path.cwd() if start is None else Path(start)
    root = find_project_root(anchor.resolve()) or anchor.resolve()
    return root / DEFAULT_SPEC_PATH


def load_pages_export_spec(path: Path | str) -> PagesExportSpec:
    """Load and validate a checked-in pages export spec."""
    spec_path = Path(path)
    if not spec_path.exists():
        raise PagesExportError(f"Pages export spec not found: {spec_path}")
    try:
        raw = yaml.safe_load(spec_path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise PagesExportError(f"Failed to parse pages export spec: {exc}") from exc
    if not isinstance(raw, dict):
        raise PagesExportError("Pages export spec must decode to a YAML object.")
    try:
        return PagesExportSpec.model_validate(raw)
    except Exception as exc:
        raise PagesExportError(f"Invalid pages export spec: {exc}") from exc


def export_pages_bundle(
    *,
    spec_path: Path | str,
    artifacts_dir: Path | str,
    out_dir: Path | str,
    childcare_regime_compiler_config_path: Path | str | None = None,
    childcare_regime_input_contract_path: Path | str | None = None,
    childcare_regime_output_contract_path: Path | str | None = None,
) -> PagesExportResult:
    """Build a fully static public run bundle."""
    spec = load_pages_export_spec(spec_path)
    artifacts_root = Path(artifacts_dir)
    output_root = Path(out_dir)
    selected_runs = _resolve_selected_runs(spec=spec, artifacts_dir=artifacts_root)
    public_runs = [item.run for item in selected_runs]

    generated_at = datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    run_payloads: dict[str, dict[str, Any]] = {}
    for item in selected_runs:
        payload = _build_run_payload(item)
        run_payloads[item.spec.run_id] = payload

    try:
        apply_public_run_compilers(
            run_payloads=run_payloads,
            childcare_regime_compiler_config_path=childcare_regime_compiler_config_path,
            childcare_regime_input_contract_path=childcare_regime_input_contract_path,
            childcare_regime_output_contract_path=childcare_regime_output_contract_path,
        )
    except PagesCompilerError as exc:
        raise PagesExportError(f"Public run compiler failed: {exc}") from exc

    available_variables: set[str] = set()
    for payload in run_payloads.values():
        available_variables.update(payload["series"].keys())

    available_variable_list = sorted(available_variables)
    dictionary_payload = _build_dictionary_payload(
        runs=public_runs,
        available_variables=available_variable_list,
    )
    presets_payload = _build_presets_payload(spec=spec)

    manifest_payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        "title": spec.title,
        "site_subpath": spec.site_subpath,
        "runs": [
            _manifest_run_record(
                item=item,
                run_payload=run_payloads[item.spec.run_id],
            )
            for item in selected_runs
        ],
        "available_variables": available_variable_list,
        "default_run_ids": list(spec.default_run_ids),
        "default_preset_ids": list(spec.default_preset_ids),
        "dictionary_path": "dictionary.json",
        "presets_path": "presets.json",
    }

    _assert_public_payload_safe(manifest_payload, label="manifest.json")
    _assert_public_payload_safe(dictionary_payload, label="dictionary.json")
    _assert_public_payload_safe(presets_payload, label="presets.json")
    for run_id, payload in run_payloads.items():
        _assert_public_payload_safe(payload, label=f"runs/{run_id}.json")

    if output_root.exists():
        shutil.rmtree(output_root)
    _copy_static_site_template(output_root)
    _write_json(output_root / "manifest.json", manifest_payload)
    _write_json(output_root / "dictionary.json", dictionary_payload)
    _write_json(output_root / "presets.json", presets_payload)
    runs_dir = output_root / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    for run_id, payload in run_payloads.items():
        _write_json(runs_dir / f"{run_id}.json", payload)

    return PagesExportResult(
        out_dir=output_root,
        manifest_path=output_root / "manifest.json",
        run_count=len(selected_runs),
        variable_count=len(available_variable_list),
        generated_at=generated_at,
    )


def _manifest_run_record(*, item: _SelectedRun, run_payload: dict[str, Any]) -> dict[str, Any]:
    record = {
        "run_id": item.spec.run_id,
        "label": item.spec.resolved_label,
        "scenario_name": item.run.scenario_name,
        "timestamp": item.run.timestamp,
        "forecast_start": run_payload["forecast_start"],
        "forecast_end": run_payload["forecast_end"],
        "data_path": f"runs/{item.spec.run_id}.json",
    }
    if item.spec.summary:
        record["summary"] = item.spec.summary
    if item.spec.details:
        record["details"] = list(item.spec.details)
    _merge_run_public_metadata(
        target=record,
        group=item.spec.group,
        public_metadata=item.spec.resolved_public_metadata,
        context=f"manifest run '{item.spec.run_id}'",
    )
    return record


@dataclass(frozen=True)
class _SelectedRun:
    spec: PagesRunSpec
    run: RunArtifact


def _resolve_selected_runs(*, spec: PagesExportSpec, artifacts_dir: Path) -> list[_SelectedRun]:
    runs = scan_artifacts(artifacts_dir)
    if not runs:
        raise PagesExportError(f"No run artifacts found in {artifacts_dir}")

    out: list[_SelectedRun] = []
    for run_spec in spec.runs:
        matches = [
            run for run in runs if run.scenario_name == run_spec.scenario_name and run.has_output
        ]
        if not matches:
            raise PagesExportError(
                f"Could not resolve latest artifact for scenario '{run_spec.scenario_name}'"
            )
        matches.sort(key=lambda item: str(item.timestamp or ""), reverse=True)
        out.append(_SelectedRun(spec=run_spec, run=matches[0]))
    return out


def _build_run_payload(item: _SelectedRun) -> dict[str, Any]:
    run = item.run
    period_tokens, series = _read_public_series(run)
    start = str(getattr(run.config, "forecast_start", "") or "").strip() if run.config else ""
    end = str(getattr(run.config, "forecast_end", "") or "").strip() if run.config else ""
    sliced_periods, sliced_series = _slice_series(
        period_tokens=period_tokens,
        series=series,
        forecast_start=start or None,
        forecast_end=end or None,
    )
    _validate_or_fill_overlay_exogenous_series(
        run=run,
        period_tokens=sliced_periods,
        series=sliced_series,
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "run_id": item.spec.run_id,
        "scenario_name": run.scenario_name,
        "timestamp": run.timestamp,
        "forecast_start": sliced_periods[0] if sliced_periods else (start or ""),
        "forecast_end": sliced_periods[-1] if sliced_periods else (end or ""),
        "periods": list(sliced_periods),
        "series": {
            name: [_json_number(value) for value in values]
            for name, values in sorted(sliced_series.items())
        },
    }
    _merge_run_public_metadata(
        target=payload,
        group=item.spec.group,
        public_metadata=item.spec.resolved_public_metadata,
        context=f"run payload '{item.spec.run_id}'",
    )
    return payload


def _merge_run_public_metadata(
    *,
    target: dict[str, Any],
    group: str,
    public_metadata: dict[str, Any],
    context: str,
) -> None:
    if group:
        if "group" in target:
            raise PagesExportError(f"{context} attempted to overwrite reserved key 'group'")
        target["group"] = group
    for key, value in (public_metadata or {}).items():
        token = str(key or "").strip()
        if not token:
            continue
        if token in target:
            raise PagesExportError(f"{context} attempted to overwrite reserved key '{token}'")
        target[token] = value


def _build_dictionary_payload(
    *,
    runs: list[RunArtifact],
    available_variables: list[str],
) -> dict[str, Any]:
    merged = load_dictionary_with_overlays(overlay_paths=overlay_paths_for_runs(runs))
    variables: dict[str, dict[str, Any]] = {}
    for code in available_variables:
        record = merged.get_variable(code)
        variables[code] = {
            "code": code,
            "short_name": str(getattr(record, "short_name", "") or "").strip() if record else "",
            "description": str(getattr(record, "description", "") or "").strip() if record else "",
            "units": str(getattr(record, "units", "") or "").strip() if record else "",
            "defined_by_equation": getattr(record, "defined_by_equation", None)
            if record
            else None,
            "used_in_equations": list(getattr(record, "used_in_equations", []) or [])
            if record
            else [],
        }
    equations: dict[str, dict[str, Any]] = {}
    for eq_id, eq in sorted(merged.equations.items()):
        equations[str(eq_id)] = {
            "id": int(eq.id),
            "type": str(getattr(eq, "type", "") or "").strip(),
            "sector_block": str(getattr(eq, "sector_block", "") or "").strip(),
            "label": str(getattr(eq, "label", "") or "").strip(),
            "lhs_expr": str(getattr(eq, "lhs_expr", "") or "").strip(),
            "rhs_variables": [
                str(name).strip() for name in list(getattr(eq, "rhs_variables", []) or [])
            ],
            "formula": str(getattr(eq, "formula", "") or "").strip(),
            "display_id": f"Eq {eq_id}",
            "source_runs": [],
        }
    equations.update(
        _build_run_input_equation_payloads(runs=runs, available_variables=available_variables)
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "variables": variables,
        "equations": equations,
    }


def _validate_or_fill_overlay_exogenous_series(
    *,
    run: RunArtifact,
    period_tokens: list[str],
    series: dict[str, list[float]],
) -> None:
    overlay_series = _overlay_exogenous_series_for_run(run)
    if not overlay_series or not period_tokens:
        return
    period_index = {period: idx for idx, period in enumerate(period_tokens)}
    for variable, values_by_period in overlay_series.items():
        if variable not in series:
            series[variable] = [math.nan] * len(period_tokens)
        target = list(series.get(variable) or [])
        if len(target) < len(period_tokens):
            target.extend([math.nan] * (len(period_tokens) - len(target)))
        for period, value in values_by_period.items():
            idx = period_index.get(period)
            if idx is None:
                continue
            solved_value = target[idx]
            overlay_value = float(value)
            # These variables are authored policy/control lanes. The public site
            # is supposed to show the authored overlay path even when the solved
            # loadformat series does not mirror that control directly.
            target[idx] = overlay_value
        series[variable] = target
    _fill_growth_identity_series(series=series, period_tokens=period_tokens)


def _fill_growth_identity_series(
    *,
    series: dict[str, list[float]],
    period_tokens: list[str],
) -> None:
    if not period_tokens:
        return
    growth_pairs = (
        ("JGW", "JGCOLA"),
        ("MINWAGE", "MWCOLA"),
    )
    for level_var, growth_var in growth_pairs:
        levels = list(series.get(level_var) or [])
        growth = list(series.get(growth_var) or [])
        if not levels or not growth:
            continue
        if len(levels) < len(period_tokens):
            levels.extend([math.nan] * (len(period_tokens) - len(levels)))
        if len(growth) < len(period_tokens):
            growth.extend([math.nan] * (len(period_tokens) - len(growth)))
        seed_idx = next((idx for idx, value in enumerate(levels) if math.isfinite(value)), None)
        if seed_idx is None:
            continue
        for idx in range(seed_idx + 1, len(period_tokens)):
            prev_value = levels[idx - 1]
            growth_value = growth[idx]
            if not math.isfinite(prev_value) or not math.isfinite(growth_value):
                continue
            levels[idx] = float(prev_value) * (1.0 + float(growth_value))
        series[level_var] = levels


def _overlay_exogenous_series_for_run(run: RunArtifact) -> dict[str, dict[str, float]]:
    config = getattr(run, "config", None)
    overlay_candidates: list[Path] = []
    if config is not None:
        overlay_dir = getattr(config, "input_overlay_dir", None)
        if overlay_dir not in (None, ""):
            overlay_candidates.append(Path(str(overlay_dir)) / "fmexog.txt")
    overlay_candidates.append(run.run_dir / "work" / "fmexog.txt")
    fmexog_path = next((path for path in overlay_candidates if path.exists()), None)
    if fmexog_path is None:
        return {}
    parsed = parse_fmexog_text(fmexog_path.read_text(encoding="utf-8", errors="replace"))
    out: dict[str, dict[str, float]] = {}
    for change in parsed.get("changes", []) or []:
        if not isinstance(change, dict):
            continue
        method = str(change.get("method", "") or "").strip().upper() or None
        variable = str(change.get("variable", "") or "").strip().upper()
        if variable not in _DIRECT_OVERLAY_CONTROL_VARIABLES:
            continue
        if method in {"CHGSAMEPCT", "CHGSAMEABS"}:
            continue
        values = [float(value) for value in list(change.get("values") or [])]
        start = str(change.get("sample_start", "") or "").strip()
        end = str(change.get("sample_end", "") or "").strip()
        if not variable or not values or not start or not end:
            continue
        periods = _period_range(start, end)
        if not periods:
            continue
        if len(values) == 1:
            expanded = [values[0]] * len(periods)
        elif len(values) == len(periods):
            expanded = values
        else:
            continue
        target = out.setdefault(variable, {})
        for period, value in zip(periods, expanded, strict=False):
            target[period] = float(value)
    return out


def _input_paths_for_run(run: RunArtifact) -> list[Path]:
    candidates: list[Path] = []
    work_dir = run.run_dir / "work"
    if work_dir.exists():
        has_scenario_model_deck = any(path.name.lower().startswith("pse") for path in work_dir.glob("*.txt"))
        for path in sorted(work_dir.glob("*.txt")):
            name = path.name.lower()
            if name in {
                "fmout.txt",
                "fp-exe.stdout.txt",
                "fp-exe.stderr.txt",
                "fmdata.txt",
                "fmage.txt",
                "fmexog.txt",
            }:
                continue
            if has_scenario_model_deck and name == "fminput.txt":
                continue
            if (
                name == "fminput.txt"
                or name == "ptcoef.txt"
                or name == "intgadj.txt"
                or name.startswith("pse")
            ):
                candidates.append(path)
    root_fminput = run.run_dir / "fminput.txt"
    if root_fminput.exists():
        candidates.append(root_fminput)
    seen: set[Path] = set()
    out: list[Path] = []
    for path in candidates:
        try:
            resolved = path.resolve()
        except Exception:
            resolved = path
        if resolved in seen:
            continue
        seen.add(resolved)
        out.append(path)
    return out


def _extract_input_refs(expression: str) -> list[str]:
    refs = [match.group(1).upper().strip() for match in _INPUT_VAR_RE.finditer(expression or "")]
    out: list[str] = []
    seen: set[str] = set()
    for ref in refs:
        if not ref or ref in seen:
            continue
        seen.add(ref)
        out.append(ref)
    return out


def _equation_touches_visible_variables(
    *,
    lhs: str,
    rhs_variables: list[str],
    visible_variables: set[str],
) -> bool:
    lhs_token = str(lhs or "").strip().upper()
    if lhs_token and lhs_token in visible_variables:
        return True
    return bool(visible_variables & {str(name).strip().upper() for name in rhs_variables})


def _merge_rhs_variables(*groups: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for raw_name in group:
            name = str(raw_name or "").strip().upper()
            if not name or name in seen:
                continue
            seen.add(name)
            out.append(name)
    return out


def _make_run_equation_id(command_type: str, lhs: str, expression: str) -> str:
    digest = sha1(f"{command_type}|{lhs}|{expression}".encode()).hexdigest()[:10]
    return f"{command_type}:{lhs}:{digest}"


def _parse_solver_equation_command(body: str) -> dict[str, Any] | None:
    match = re.match(r"^(\d+)\s+(.*)$", str(body or "").strip(), re.DOTALL)
    if not match:
        return None
    eq_id = int(match.group(1))
    raw_formula = match.group(2).strip()
    tokens = raw_formula.split()
    lhs_expr = tokens[0] if tokens else ""
    rhs_variables = _extract_input_refs(" ".join(tokens[1:])) if len(tokens) > 1 else []
    return {
        "id": _make_run_equation_id(f"eq:{eq_id}", lhs_expr, raw_formula),
        "model_eq_id": eq_id,
        "type": "scenario_equation",
        "sector_block": "scenario_input",
        "label": f"Eq {eq_id}",
        "lhs_expr": lhs_expr,
        "rhs_variables": rhs_variables,
        "formula": raw_formula,
        "display_id": f"Eq {eq_id}",
        "source_runs": [],
    }


def _append_solver_equation_payload(
    *,
    payloads: dict[str, dict[str, Any]],
    equation: dict[str, Any] | None,
    visible_variables: set[str],
    run_id: str,
) -> None:
    if equation is None:
        return
    lhs_expr = str(equation.get("lhs_expr", "") or "").strip().upper()
    rhs_variables = [str(name).strip().upper() for name in list(equation.get("rhs_variables") or [])]
    if not _equation_touches_visible_variables(
        lhs=lhs_expr,
        rhs_variables=rhs_variables,
        visible_variables=visible_variables,
    ):
        return
    item = payloads.setdefault(str(equation["id"]), equation)
    source_runs = item.setdefault("source_runs", [])
    if run_id not in source_runs:
        source_runs.append(run_id)


def _build_run_input_equation_payloads(
    *,
    runs: list[RunArtifact],
    available_variables: list[str],
) -> dict[str, dict[str, Any]]:
    visible_variables = {str(name).strip().upper() for name in available_variables}
    payloads: dict[str, dict[str, Any]] = {}
    for run in runs:
        for input_path in _input_paths_for_run(run):
            try:
                parsed = parse_fp_input(input_path)
            except Exception:
                continue
            pending_solver_equation: dict[str, Any] | None = None
            for command in parsed.get("commands", []) or []:
                command_name = str(command.get("name", "") or "").strip().upper()
                command_body = str(command.get("body", "") or "").strip()
                if command_name == "EQ":
                    _append_solver_equation_payload(
                        payloads=payloads,
                        equation=pending_solver_equation,
                        visible_variables=visible_variables,
                        run_id=run.scenario_name,
                    )
                    pending_solver_equation = _parse_solver_equation_command(command_body)
                    continue
                if command_name == "LHS":
                    if pending_solver_equation is not None:
                        _append_solver_equation_payload(
                            payloads=payloads,
                            equation=pending_solver_equation,
                            visible_variables=visible_variables,
                            run_id=run.scenario_name,
                        )
                        lhs_name, _, lhs_formula = command_body.partition("=")
                        lhs_expr = lhs_name.strip().upper()
                        formula = lhs_formula.strip()
                        if formula:
                            eq_id = pending_solver_equation.get("model_eq_id", pending_solver_equation.get("id", ""))
                            _append_solver_equation_payload(
                                payloads=payloads,
                                equation={
                                    "id": _make_run_equation_id(f"lhs:{eq_id}", lhs_expr, formula),
                                    "model_eq_id": eq_id,
                                    "type": "scenario_equation",
                                    "sector_block": "scenario_input",
                                    "label": f"Eq {eq_id} LHS" if eq_id != "" else f"LHS {lhs_expr}".strip(),
                                    "lhs_expr": lhs_expr,
                                    "rhs_variables": _extract_input_refs(formula),
                                    "formula": formula,
                                    "display_id": f"Eq {eq_id} LHS" if eq_id != "" else f"LHS {lhs_expr}".strip(),
                                    "source_runs": [],
                                },
                                visible_variables=visible_variables,
                                run_id=run.scenario_name,
                            )
                        pending_solver_equation = None
                    continue
                _append_solver_equation_payload(
                    payloads=payloads,
                    equation=pending_solver_equation,
                    visible_variables=visible_variables,
                    run_id=run.scenario_name,
                )
                pending_solver_equation = None
            _append_solver_equation_payload(
                payloads=payloads,
                equation=pending_solver_equation,
                visible_variables=visible_variables,
                run_id=run.scenario_name,
            )
            for command_type, records in (
                ("create", parsed.get("creates", [])),
                ("genr", parsed.get("generated_vars", [])),
                ("identity", parsed.get("identities", [])),
            ):
                for raw_record in records or []:
                    lhs = str(raw_record.get("name", "") or "").strip().upper()
                    expression = str(raw_record.get("expression", "") or "").strip()
                    rhs_variables = _extract_input_refs(expression)
                    if not _equation_touches_visible_variables(
                        lhs=lhs,
                        rhs_variables=rhs_variables,
                        visible_variables=visible_variables,
                    ):
                        continue
                    eq_id = _make_run_equation_id(command_type, lhs, expression)
                    command_label = {
                        "create": "CREATE",
                        "genr": "GENR",
                        "identity": "IDENT",
                    }.get(command_type, command_type.upper())
                    item = payloads.setdefault(
                        eq_id,
                        {
                            "id": eq_id,
                            "type": command_type,
                            "sector_block": "scenario_input",
                            "label": f"{command_label} {lhs}".strip(),
                            "lhs_expr": lhs,
                            "rhs_variables": rhs_variables,
                            "formula": expression,
                            "display_id": f"{command_label} {lhs}".strip(),
                            "source_runs": [],
                        },
                    )
                    source_runs = item.setdefault("source_runs", [])
                    run_id = run.scenario_name
                    if run_id not in source_runs:
                        source_runs.append(run_id)
    return payloads


def _build_presets_payload(*, spec: PagesExportSpec) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "presets": [item.to_public_record() for item in spec.presets],
    }


def _read_public_series(run: RunArtifact) -> tuple[list[str], dict[str, list[float]]]:
    candidate_paths = [
        run.run_dir / "LOADFORMAT.DAT",
        run.run_dir / "PABEV.TXT",
        run.run_dir / "PACEV.TXT",
    ]
    loadformat_path = next((path for path in candidate_paths if path.exists()), None)
    if loadformat_path is None:
        raise PagesExportError(f"Run '{run.run_dir}' is missing LOADFORMAT-style output")
    periods, series = read_loadformat(loadformat_path)
    add_derived_series(series)
    return periods, series


def _slice_series(
    *,
    period_tokens: list[str],
    series: dict[str, list[float]],
    forecast_start: str | None,
    forecast_end: str | None,
) -> tuple[list[str], dict[str, list[float]]]:
    start_idx, end_idx = _slice_bounds(
        period_tokens=period_tokens,
        start=forecast_start,
        end=forecast_end,
    )
    sliced_periods = period_tokens[start_idx : end_idx + 1]
    sliced_series = {
        name: [float(value) for value in values[start_idx : end_idx + 1]]
        for name, values in series.items()
    }
    return sliced_periods, sliced_series


def _slice_bounds(
    *,
    period_tokens: list[str],
    start: str | None,
    end: str | None,
) -> tuple[int, int]:
    if not period_tokens:
        return 0, -1

    start_key = _period_index(start) if start else None
    end_key = _period_index(end) if end else None

    start_idx = 0
    if start_key is not None:
        for idx, token in enumerate(period_tokens):
            key = _period_index(token)
            if key is not None and key >= start_key:
                start_idx = idx
                break

    end_idx = len(period_tokens) - 1
    if end_key is not None:
        for idx in range(len(period_tokens) - 1, -1, -1):
            key = _period_index(period_tokens[idx])
            if key is not None and key <= end_key:
                end_idx = idx
                break

    if end_idx < start_idx:
        end_idx = start_idx
    return start_idx, end_idx


def _period_range(start: str, end: str) -> list[str]:
    start_key = _period_parts(start)
    end_key = _period_parts(end)
    if start_key is None or end_key is None:
        return []
    periods: list[str] = []
    year, sub = start_key
    end_year, end_sub = end_key
    while (year, sub) <= (end_year, end_sub):
        periods.append(f"{year:04d}.{sub}")
        year, sub = _next_period(year, sub)
    return periods


def _period_parts(token: str | None) -> tuple[int, int] | None:
    raw = str(token or "").strip()
    match = _PERIOD_TOKEN_RE.match(raw)
    if not match:
        return None
    try:
        return int(match.group("year")), int(match.group("sub"))
    except (TypeError, ValueError):
        return None


def _next_period(year: int, sub: int) -> tuple[int, int]:
    next_sub = int(sub) + 1
    if next_sub <= 4:
        return int(year), next_sub
    return int(year) + 1, 1


def _period_index(token: str | None) -> int | None:
    parts = _period_parts(token)
    if parts is None:
        return None
    year, sub = parts
    return year * 10 + sub


def _json_number(value: float) -> float | None:
    if not isinstance(value, (int, float)):
        return None
    numeric = float(value)
    if not math.isfinite(numeric):
        return None
    if numeric in _PUBLIC_MISSING_SENTINELS:
        return None
    return numeric


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _copy_static_site_template(out_dir: Path) -> None:
    source_dir = Path(__file__).resolve().parent / "model_runs_static"
    if not source_dir.exists():
        raise PagesExportError(f"Static site template missing: {source_dir}")
    shutil.copytree(
        source_dir,
        out_dir,
        ignore=shutil.ignore_patterns("__init__.py", "__pycache__", "*.pyc"),
    )
    (out_dir / ".nojekyll").write_text("", encoding="utf-8")


def _assert_public_payload_safe(payload: Any, *, label: str) -> None:
    for raw in _iter_strings(payload):
        text = str(raw)
        if not text:
            continue
        if text.startswith("/"):
            raise PagesExportError(f"{label} contains an absolute path: {text}")
        if _WINDOWS_ABSOLUTE_RE.match(text):
            raise PagesExportError(f"{label} contains a Windows absolute path: {text}")
        if str(Path.home()) in text:
            raise PagesExportError(f"{label} leaks a home-path token: {text}")
        if "/Users/" in text or "/home/" in text:
            raise PagesExportError(f"{label} leaks a local path token: {text}")


def _iter_strings(value: Any) -> list[str]:
    out: list[str] = []
    if isinstance(value, str):
        out.append(value)
        return out
    if isinstance(value, dict):
        for key, inner in value.items():
            out.extend(_iter_strings(key))
            out.extend(_iter_strings(inner))
        return out
    if isinstance(value, list):
        for inner in value:
            out.extend(_iter_strings(inner))
    return out


def _normalize_public_run_comparisons(
    *,
    variables: list[str],
    run_comparisons: dict[str, Any] | None,
) -> dict[str, dict[str, str]]:
    allowed_vars = {str(token).strip().upper() for token in variables if str(token).strip()}
    if not allowed_vars or not isinstance(run_comparisons, dict):
        return {}

    out: dict[str, dict[str, str]] = {}
    for raw_var, raw_cfg in run_comparisons.items():
        var_name = str(raw_var or "").strip().upper()
        if not var_name or var_name not in allowed_vars or not isinstance(raw_cfg, dict):
            continue
        mode = str(raw_cfg.get("mode", "") or "").strip().lower()
        if mode not in {"diff_vs_run", "pct_diff_vs_run"}:
            continue
        reference_run_id = str(raw_cfg.get("reference_run_id", "") or "").strip()
        if not reference_run_id:
            continue
        out[var_name] = {
            "mode": mode,
            "reference_run_id": reference_run_id,
        }
    return out
