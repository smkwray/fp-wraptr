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
from fp_wraptr.io.input_parser import parse_fp_input
from fp_wraptr.io.loadformat import add_derived_series, read_loadformat

SCHEMA_VERSION = 1
STATIC_SITE_SUBPATH = "model-runs"
DEFAULT_SPEC_PATH = Path("public") / "model-runs.spec.yaml"
_RUN_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_WINDOWS_ABSOLUTE_RE = re.compile(r"^[A-Za-z]:[\\/]")
_PERIOD_TOKEN_RE = re.compile(r"^(?P<year>\d{4})\.(?P<sub>\d+)$")
_INPUT_VAR_RE = re.compile(r"\b([A-Z][A-Z0-9_]{1,})\b")

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
    details: list[str] = Field(
        default_factory=list, description="Optional scenario detail bullets"
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

    @field_validator("label", "summary")
    @classmethod
    def _normalize_text(cls, value: str) -> str:
        return " ".join(str(value or "").split()).strip()

    @field_validator("details")
    @classmethod
    def _normalize_details(cls, value: list[str]) -> list[str]:
        out: list[str] = []
        for item in value or []:
            token = " ".join(str(item or "").split()).strip()
            if token:
                out.append(token)
        return out

    @property
    def resolved_label(self) -> str:
        label = str(self.label or "").strip()
        return label or self.scenario_name


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
        if self.site_subpath != STATIC_SITE_SUBPATH:
            raise ValueError(f"site_subpath must be '{STATIC_SITE_SUBPATH}'")
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
) -> PagesExportResult:
    """Build a fully static public run bundle."""
    spec = load_pages_export_spec(spec_path)
    artifacts_root = Path(artifacts_dir)
    output_root = Path(out_dir)
    selected_runs = _resolve_selected_runs(spec=spec, artifacts_dir=artifacts_root)
    public_runs = [item.run for item in selected_runs]

    generated_at = datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    run_payloads: dict[str, dict[str, Any]] = {}
    available_variables: set[str] = set()
    for item in selected_runs:
        payload = _build_run_payload(item)
        run_payloads[item.spec.run_id] = payload
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
    return payload


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
            "defined_by_equation": getattr(record, "defined_by_equation", None) if record else None,
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
            "rhs_variables": [str(name).strip() for name in list(getattr(eq, "rhs_variables", []) or [])],
            "formula": str(getattr(eq, "formula", "") or "").strip(),
            "display_id": f"Eq {eq_id}",
            "source_runs": [],
        }
    equations.update(_build_run_input_equation_payloads(runs=runs, available_variables=available_variables))
    return {
        "schema_version": SCHEMA_VERSION,
        "variables": variables,
        "equations": equations,
    }


def _input_paths_for_run(run: RunArtifact) -> list[Path]:
    candidates: list[Path] = []
    work_dir = run.run_dir / "work"
    if work_dir.exists():
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
            if name == "fminput.txt" or name == "ptcoef.txt" or name == "intgadj.txt" or name.startswith("pse"):
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


def _make_run_equation_id(command_type: str, lhs: str, expression: str) -> str:
    digest = sha1(f"{command_type}|{lhs}|{expression}".encode()).hexdigest()[:10]
    return f"{command_type}:{lhs}:{digest}"


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
            for command_type, records in (
                ("create", parsed.get("creates", [])),
                ("genr", parsed.get("generated_vars", [])),
                ("identity", parsed.get("identities", [])),
            ):
                for raw_record in records or []:
                    lhs = str(raw_record.get("name", "") or "").strip().upper()
                    expression = str(raw_record.get("expression", "") or "").strip()
                    rhs_variables = _extract_input_refs(expression)
                    if lhs not in visible_variables and not (visible_variables & set(rhs_variables)):
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


def _period_index(token: str | None) -> int | None:
    raw = str(token or "").strip()
    if not raw:
        return None
    match = _PERIOD_TOKEN_RE.match(raw)
    if not match:
        return None
    try:
        year = int(match.group("year"))
        sub = int(match.group("sub"))
    except (TypeError, ValueError):
        return None
    return year * 10 + sub


def _json_number(value: float) -> float | None:
    if not isinstance(value, (int, float)):
        return None
    numeric = float(value)
    if not math.isfinite(numeric):
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
