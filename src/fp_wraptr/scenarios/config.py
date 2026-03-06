"""Scenario configuration models.

A scenario config is a YAML file that describes:
- Which FP model directory to use
- What exogenous variable overrides to apply
- Forecast period settings
- What variables to track in output
- Optional input overlay directory for extra include scripts

Example YAML:
    name: higher_growth
    description: "Test scenario with higher potential GDP growth"
    fp_home: FM
    forecast_start: "2025.4"
    forecast_end: "2029.4"
    overrides:
      YS:
        method: CHGSAMEPCT
        value: 0.008
    track_variables:
      - PCY
      - UR
      - GDPR
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator

__all__ = ["ScenarioConfig", "VariableOverride"]


class VariableOverride(BaseModel):
    """Override for a single exogenous variable."""

    method: str = Field(
        default="SAMEVALUE",
        description="FP method: CHGSAMEPCT, SAMEVALUE, or CHGSAMEABS",
    )
    value: float = Field(default=0.0, description="Override value")


class ScenarioConfig(BaseModel):
    """Configuration for an FP scenario run."""

    name: str = Field(description="Scenario name (used for output directory naming)")
    description: str = Field(default="", description="Human-readable description")
    fp_home: Path = Field(default=Path("FM"), description="Path to FP model directory")
    input_overlay_dir: Path | None = Field(
        default=None,
        description=(
            "Optional directory searched for input scripts/includes referenced by input_file "
            "(e.g. nested `INPUT FILE=...;`). When set, files are copied into the working "
            "directory before running fp.exe."
        ),
    )
    input_file: str = Field(default="fminput.txt", description="FP input filename")
    forecast_start: str = Field(default="2025.4", description="Forecast start period (YYYY.Q)")
    forecast_end: str = Field(default="2029.4", description="Forecast end period (YYYY.Q)")
    backend: str = Field(
        default="fpexe",
        description="Execution backend: fpexe, fppy, or both",
    )
    fppy: dict[str, Any] = Field(
        default_factory=dict,
        description="Optional fp-py backend settings (timeout_seconds, eq_flags_preset, etc)",
    )
    overrides: dict[str, VariableOverride] = Field(
        default_factory=dict,
        description="Exogenous variable overrides",
    )
    track_variables: list[str] = Field(
        default_factory=lambda: ["PCY", "PCPF", "UR", "PIEF", "GDPR"],
        description="Variables to include in output summary",
    )
    input_patches: dict[str, str] = Field(
        default_factory=dict,
        description="Raw text patches to apply to input file {search: replace}",
    )
    alerts: dict[str, dict[str, float]] = Field(
        default_factory=dict,
        description="Alert thresholds by variable, e.g. {'UR': {'max': 6.0}}",
    )
    extra: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata",
    )

    @field_validator("forecast_start", "forecast_end", mode="before")
    @classmethod
    def _coerce_forecast_period_to_str(cls, value: Any) -> Any:
        # Operator-authored YAML often uses unquoted values like `forecast_start: 2025.4`,
        # which YAML parses as a float. Coerce these numeric inputs to strings.
        if isinstance(value, (int, float)):
            return str(value)
        return value

    @classmethod
    def from_yaml(cls, path: Path | str) -> ScenarioConfig:
        """Load scenario config from a YAML file."""
        path = Path(path)
        with path.open() as f:
            data = yaml.safe_load(f)
        # Be tolerant of YAML keys explicitly set to null (e.g. `overrides:` with only comments).
        # Treat these as "unset" so operator-authored scenarios don't crash at load time.
        if isinstance(data, dict):
            for key, default in (
                ("fppy", {}),
                ("overrides", {}),
                ("input_patches", {}),
                ("track_variables", []),
                ("alerts", {}),
                ("extra", {}),
            ):
                if data.get(key) is None:
                    data[key] = default
        return cls(**data)

    def to_yaml(self, path: Path | str) -> Path:
        """Write scenario config to a YAML file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            yaml.dump(self.model_dump(mode="json"), f, default_flow_style=False, sort_keys=False)
        return path
