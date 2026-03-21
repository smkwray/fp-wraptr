"""Scenario authoring models for managed workspaces and curated cards."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


class DraftSourceRef(BaseModel):
    """Reference to a base scenario or bundle."""

    kind: Literal["catalog", "path"] = "catalog"
    value: str

    @field_validator("value")
    @classmethod
    def _non_empty_value(cls, value: str) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("source reference value must be non-empty")
        return text


class AttachRule(BaseModel):
    """Declarative compile-time splice contract for generated files."""

    kind: Literal[
        "overlay_file",
        "replace_include",
        "append_include_after_match",
        "append_include_before_return",
    ] = "overlay_file"
    target_file: str | None = None
    relative_path: str | None = None
    match_text: str | None = None
    statement: str | None = None

    @model_validator(mode="after")
    def _validate_rule(self) -> AttachRule:
        if self.kind == "overlay_file":
            if not self.relative_path:
                raise ValueError("overlay_file attach rules require relative_path")
            return self
        if not self.target_file:
            raise ValueError(f"{self.kind} attach rules require target_file")
        if self.kind == "append_include_after_match" and not self.match_text:
            raise ValueError("append_include_after_match attach rules require match_text")
        return self


class ConstantFieldSpec(BaseModel):
    """One editable constant exposed by a card."""

    symbol: str
    label: str
    help_text: str = ""
    order: int = 0
    number_format: str = "%.6f"
    step: float | None = None
    min_value: float | None = None
    max_value: float | None = None

    @field_validator("symbol")
    @classmethod
    def _upper_symbol(cls, value: str) -> str:
        text = str(value).strip().upper()
        if not text:
            raise ValueError("constant symbol must be non-empty")
        return text


class ConstantGroupSpec(BaseModel):
    """UI grouping for one or more constants from the same file."""

    group_id: str
    label: str
    description: str = ""
    order: int = 0
    fields: list[ConstantFieldSpec]


class DeckConstantsFileSpec(BaseModel):
    """File-level definition for editable constants."""

    path: str
    label: str = ""
    order: int = 0
    groups: list[ConstantGroupSpec]


class DeckConstantsCardSpec(BaseModel):
    """Curated constant editor for one or more overlay-backed files."""

    kind: Literal["deck_constants"] = "deck_constants"
    card_id: str
    family: str
    label: str
    description: str = ""
    order: int = 0
    files: list[DeckConstantsFileSpec]


class SeriesTargetSpec(BaseModel):
    """One supported output target for a series card."""

    kind: Literal["include_changevar", "fmexog_override", "load_data_file"]
    label: str = ""
    output_path: str
    attach_rule: AttachRule = Field(default_factory=AttachRule)
    fp_method: Literal["SAMEVALUE", "CHGSAMEPCT", "CHGSAMEABS", "ADDDIFABS"] = "SAMEVALUE"
    mode: Literal["constant", "series"] = "series"
    layer_on_base: bool = True


class SeriesCardSpec(BaseModel):
    """Curated quarterly series import/editor card."""

    kind: Literal["series_card"] = "series_card"
    card_id: str
    family: str
    label: str
    description: str = ""
    order: int = 0
    variable: str
    input_modes: list[Literal["csv", "paste"]] = Field(default_factory=lambda: ["csv", "paste"])
    default_target: Literal["include_changevar", "fmexog_override", "load_data_file"] = (
        "include_changevar"
    )
    targets: list[SeriesTargetSpec]

    @field_validator("variable")
    @classmethod
    def _upper_variable(cls, value: str) -> str:
        text = str(value).strip().upper()
        if not text:
            raise ValueError("series variable must be non-empty")
        return text

    @model_validator(mode="after")
    def _validate_default_target(self) -> SeriesCardSpec:
        target_kinds = {target.kind for target in self.targets}
        if self.default_target not in target_kinds:
            raise ValueError(
                f"default_target {self.default_target!r} is not present in targets {sorted(target_kinds)}"
            )
        return self


CardSpec = Annotated[DeckConstantsCardSpec | SeriesCardSpec, Field(discriminator="kind")]


class CardInstance(BaseModel):
    """User-entered values for one card inside a workspace draft."""

    card_id: str
    enabled: bool = True
    constants: dict[str, float] = Field(default_factory=dict)
    selected_target: str | None = None
    input_mode: Literal["csv", "paste"] | None = None
    import_path: str | None = None
    pasted_text: str = ""
    series_points: dict[str, float] = Field(default_factory=dict)

    @field_validator("constants", mode="before")
    @classmethod
    def _normalize_constants(cls, value: object) -> object:
        if not isinstance(value, dict):
            return value
        return {str(key).strip().upper(): float(item) for key, item in value.items()}

    @field_validator("series_points", mode="before")
    @classmethod
    def _normalize_series_points(cls, value: object) -> object:
        if not isinstance(value, dict):
            return value
        normalized: dict[str, float] = {}
        for key, item in value.items():
            token = str(key).strip()
            if not token:
                continue
            normalized[token] = float(item)
        return normalized


class WorkspaceOperation(BaseModel):
    """One recorded workspace mutation or workflow step."""

    operation: str
    summary: str = ""
    timestamp: str = Field(
        default_factory=lambda: datetime.now().astimezone().isoformat(timespec="seconds")
    )
    details: dict[str, Any] = Field(default_factory=dict)


class WorkspaceRunLink(BaseModel):
    """One run or comparison artifact associated with a workspace."""

    run_kind: Literal["scenario", "bundle", "comparison", "visualization"] = "scenario"
    label: str
    run_dir: str
    timestamp: str = Field(
        default_factory=lambda: datetime.now().astimezone().isoformat(timespec="seconds")
    )
    details: dict[str, Any] = Field(default_factory=dict)


class ScenarioDraft(BaseModel):
    """Editable source of truth for one managed scenario workspace."""

    draft_kind: Literal["scenario"] = "scenario"
    workspace_id: str | None = None
    family: str
    slug: str
    label: str
    description: str = ""
    source: DraftSourceRef
    scenario_name: str
    forecast_start: str
    forecast_end: str
    backend: str = "fpexe"
    artifacts_root: str = "artifacts"
    fppy: dict[str, Any] = Field(default_factory=dict)
    track_variables: list[str] = Field(default_factory=list)
    cards: list[CardInstance] = Field(default_factory=list)
    extra: dict[str, Any] = Field(default_factory=dict)
    input_file_override: str | None = None
    recipe_history: list[WorkspaceOperation] = Field(default_factory=list)
    linked_runs: list[WorkspaceRunLink] = Field(default_factory=list)

    @field_validator("family", "slug", "label", "scenario_name")
    @classmethod
    def _non_empty_text(cls, value: str) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("field must be non-empty")
        return text

    @field_validator("workspace_id")
    @classmethod
    def _normalize_workspace_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @classmethod
    def from_yaml(cls, path: Path | str) -> ScenarioDraft:
        payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        return cls.model_validate(payload or {})

    def to_yaml(self, path: Path | str) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            yaml.safe_dump(self.model_dump(mode="json"), sort_keys=False),
            encoding="utf-8",
        )
        return path


class BundleVariantDraft(BaseModel):
    """One editable bundle variant."""

    variant_id: str
    label: str
    scenario_name: str | None = None
    input_file: str | None = None
    enabled: bool = True
    cards: list[CardInstance] = Field(default_factory=list)


class BundleDraft(BaseModel):
    """Editable source of truth for one managed bundle workspace."""

    draft_kind: Literal["bundle"] = "bundle"
    workspace_id: str | None = None
    family: str
    slug: str
    label: str
    description: str = ""
    source: DraftSourceRef
    bundle_name: str
    forecast_start: str
    forecast_end: str
    backend: str = "fpexe"
    artifacts_root: str = "artifacts"
    fppy: dict[str, Any] = Field(default_factory=dict)
    track_variables: list[str] = Field(default_factory=list)
    cards: list[CardInstance] = Field(default_factory=list)
    variants: list[BundleVariantDraft] = Field(default_factory=list)
    extra: dict[str, Any] = Field(default_factory=dict)
    recipe_history: list[WorkspaceOperation] = Field(default_factory=list)
    linked_runs: list[WorkspaceRunLink] = Field(default_factory=list)

    @field_validator("family", "slug", "label", "bundle_name")
    @classmethod
    def _bundle_non_empty_text(cls, value: str) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("field must be non-empty")
        return text

    @field_validator("workspace_id")
    @classmethod
    def _normalize_bundle_workspace_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @classmethod
    def from_yaml(cls, path: Path | str) -> BundleDraft:
        payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        return cls.model_validate(payload or {})

    def to_yaml(self, path: Path | str) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            yaml.safe_dump(self.model_dump(mode="json"), sort_keys=False),
            encoding="utf-8",
        )
        return path
