"""Pipeline YAML spec for generating FP artifacts from external series."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator, model_validator


class PipelineContext(BaseModel):
    sample_start: str | None = None
    sample_end: str | None = None
    history_end: str | None = None
    forecast_start: str | None = None
    forecast_end: str | None = None
    periodicity: Literal["quarterly"] = "quarterly"


class CsvSource(BaseModel):
    kind: Literal["csv"] = "csv"
    path: Path
    period_col: str = "period"
    value_col: str | None = None
    variable_col: str | None = None
    variable: str | None = None
    format: Literal["long", "wide"] | None = None
    missing_values: list[str] = Field(default_factory=lambda: ["", "NA", "N/A", "nan", "NaN", "."])

    @model_validator(mode="after")
    def _validate_layout(self) -> "CsvSource":
        if self.format is None:
            # Infer: value_col implies long format.
            self.format = "long" if self.value_col else "wide"
        if self.format == "long":
            if not self.value_col:
                raise ValueError("csv source format=long requires value_col")
            if self.variable_col and not self.variable:
                raise ValueError("csv source with variable_col requires variable")
        return self


class JsonSource(BaseModel):
    kind: Literal["json"] = "json"
    path: Path
    value_path: str
    period_path: str | None = None


class ConstantSource(BaseModel):
    kind: Literal["constant"] = "constant"
    value: float
    period: str | None = None


SourceSpec = Annotated[CsvSource | JsonSource | ConstantSource, Field(discriminator="kind")]


class TransformSpec(BaseModel):
    bounds: tuple[float | None, float | None] | None = None
    pre: Literal["log", "logit"] | None = None
    post: Literal["exp", "inv_logit"] | None = None

    @model_validator(mode="after")
    def _validate_transform(self) -> "TransformSpec":
        if self.pre and not self.post:
            raise ValueError("transform.pre requires transform.post")
        if self.post and not self.pre:
            raise ValueError("transform.post requires transform.pre")
        return self


class ExtrapolationSpec(BaseModel):
    method: Literal["flat", "rolling_mean", "linear_trend", "ets", "arima"] = "flat"
    window: int = 8
    tail: int = 20
    fallback: Literal["flat", "rolling_mean", "linear_trend"] | None = "flat"

    @field_validator("window", "tail")
    @classmethod
    def _positive_int(cls, value: int) -> int:
        if int(value) <= 0:
            raise ValueError("must be > 0")
        return int(value)


class IncludeChangevarTarget(BaseModel):
    kind: Literal["include_changevar"] = "include_changevar"
    variable: str
    fp_method: Literal["SAMEVALUE", "CHGSAMEPCT", "CHGSAMEABS"] = "SAMEVALUE"
    smpl_start: str
    smpl_end: str
    mode: Literal["constant", "series"] = "constant"


class FmexogOverrideTarget(BaseModel):
    kind: Literal["fmexog_override"] = "fmexog_override"
    variable: str
    fp_method: Literal["SAMEVALUE", "CHGSAMEPCT", "CHGSAMEABS"] = "SAMEVALUE"
    smpl_start: str
    smpl_end: str
    base_fmexog: Path | None = None
    layer_on_base: bool = True


class FmdataPatchTarget(BaseModel):
    kind: Literal["fmdata_patch"] = "fmdata_patch"
    variable: str
    fmdata_in: Path
    fmdata_out: Path
    sample_start: str | None = None
    sample_end: str | None = None


class FmagePatchTarget(BaseModel):
    kind: Literal["fmage_patch"] = "fmage_patch"
    variable: str
    fmage_in: Path
    fmage_out: Path
    sample_start: str | None = None
    sample_end: str | None = None


TargetSpec = Annotated[
    IncludeChangevarTarget | FmexogOverrideTarget | FmdataPatchTarget | FmagePatchTarget,
    Field(discriminator="kind"),
]


class PipelineStep(BaseModel):
    id: str
    source: SourceSpec
    transform: TransformSpec | None = None
    extrapolation: ExtrapolationSpec | None = None
    target: TargetSpec
    write_to: list[Path] = Field(default_factory=list)

    @field_validator("id")
    @classmethod
    def _normalize_id(cls, value: str) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("step id must be non-empty")
        return text

    @model_validator(mode="after")
    def _validate_write_targets(self) -> "PipelineStep":
        if isinstance(self.target, IncludeChangevarTarget) and not self.write_to:
            raise ValueError("include_changevar requires write_to paths (at least one)")
        if isinstance(self.target, FmexogOverrideTarget) and len(self.write_to) < 1:
            raise ValueError("fmexog_override requires write_to with one output path")
        return self


class SeriesPipelineConfig(BaseModel):
    name: str
    description: str | None = None
    context: PipelineContext = Field(default_factory=PipelineContext)
    steps: list[PipelineStep]

    @field_validator("name")
    @classmethod
    def _normalize_name(cls, value: str) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("pipeline name must be non-empty")
        return text
