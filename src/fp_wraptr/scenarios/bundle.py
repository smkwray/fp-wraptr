"""Scenario bundle runner — one config, many variants, auto-compare.

A bundle defines a base scenario and a set of variants (either explicit
or as a parameter grid). Running a bundle executes all variants and
collects results for comparison.

Example bundle YAML:
    base:
      name: jg_baseline
      extends: examples/baseline.yaml
      policies:
        - type: job_guarantee
          jobs: 15000000
          wage: 15.0
    variants:
      - name: jg_12m
        patch:
          policies[0].jobs: 12000000
      - name: jg_20m
        patch:
          policies[0].jobs: 20000000
    variant_grid:
      policies[0].wage: [12.0, 15.0, 18.0]
      policies[0].jobs: [10000000, 15000000, 20000000]
"""

from __future__ import annotations

import copy
import itertools
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

from fp_wraptr.hygiene import find_project_root
from fp_wraptr.scenarios.config import ScenarioConfig

__all__ = ["BundleConfig", "BundleResult", "run_bundle"]


class VariantSpec(BaseModel):
    """A single named variant with parameter patches."""

    name: str = Field(description="Variant name (appended to base scenario name)")
    scenario_name: str | None = Field(
        default=None,
        description="Optional exact scenario/run name for this variant",
    )
    patch: dict[str, Any] = Field(
        default_factory=dict,
        description="Parameter patches to apply to the base config",
    )


class BundleConfig(BaseModel):
    """Configuration for a scenario bundle."""

    base: dict[str, Any] = Field(description="Base scenario config dict")
    variants: list[VariantSpec] = Field(
        default_factory=list,
        description="Explicit variant list",
    )
    variant_grid: dict[str, list[Any]] = Field(
        default_factory=dict,
        description="Parameter grid — produces cartesian product of all values",
    )
    focus_variables: list[str] = Field(
        default_factory=lambda: ["GDPR", "UR", "PCY"],
        description="Variables to highlight in comparison",
    )

    @classmethod
    def from_yaml(cls, path: Path | str) -> BundleConfig:
        """Load bundle config from a YAML file.

        Relative filesystem paths (e.g. ``base.fp_home: FM``) are interpreted
        relative to the fp-wraptr project root (the directory containing
        ``pyproject.toml``). This matches how bundles are typically authored
        (paths like ``FM`` and ``projects_local/...`` from repo root).
        """
        path = Path(path)
        with path.open() as f:
            data = yaml.safe_load(f)
        if not isinstance(data, dict):
            raise ValueError(f"Bundle YAML must be a mapping, got: {type(data).__name__}")

        project_root = find_project_root(path) or path.parent
        base = data.get("base")
        if isinstance(base, dict):
            base = dict(base)
            for key in ("fp_home", "input_overlay_dir"):
                raw = base.get(key)
                if raw is None:
                    continue
                try:
                    raw_path = Path(raw)
                except TypeError:
                    continue
                if not raw_path.is_absolute():
                    base[key] = (project_root / raw_path).resolve()
            data["base"] = base

        variants = data.get("variants")
        if isinstance(variants, list):
            resolved_variants: list[dict[str, Any]] = []
            for item in variants:
                if not isinstance(item, dict):
                    resolved_variants.append(item)
                    continue
                patch = item.get("patch")
                if not isinstance(patch, dict):
                    resolved_variants.append(item)
                    continue
                patch = dict(patch)
                for key in ("fp_home", "input_overlay_dir"):
                    raw = patch.get(key)
                    if raw is None:
                        continue
                    try:
                        raw_path = Path(raw)
                    except TypeError:
                        continue
                    if not raw_path.is_absolute():
                        patch[key] = (project_root / raw_path).resolve()
                resolved_variants.append({**item, "patch": patch})
            data["variants"] = resolved_variants
        return cls(**data)

    def resolve_variants(self) -> list[ScenarioConfig]:
        """Expand variants + grid into a list of ScenarioConfigs.

        Returns:
            List of resolved ScenarioConfig objects, one per variant.
        """
        base_config = ScenarioConfig(**self.base)
        configs: list[ScenarioConfig] = []

        # Explicit variants
        for variant in self.variants:
            config_data = copy.deepcopy(self.base)
            _apply_patch(config_data, variant.patch)
            config_data["name"] = str(variant.scenario_name or f"{base_config.name}_{variant.name}")
            configs.append(ScenarioConfig(**config_data))

        # Grid variants (cartesian product)
        if self.variant_grid:
            keys = sorted(self.variant_grid.keys())
            value_lists = [self.variant_grid[k] for k in keys]
            for combo in itertools.product(*value_lists):
                config_data = copy.deepcopy(self.base)
                name_parts = []
                for key, val in zip(keys, combo, strict=True):
                    _apply_patch(config_data, {key: val})
                    # Build a compact name from the last key segment and value
                    short_key = key.rsplit(".", 1)[-1] if "." in key else key
                    name_parts.append(f"{short_key}={val}")
                config_data["name"] = f"{base_config.name}_{'_'.join(name_parts)}"
                configs.append(ScenarioConfig(**config_data))

        # If no variants at all, just return the base
        if not configs:
            configs.append(base_config)

        return configs


@dataclass
class BundleResultEntry:
    """Result for a single variant in a bundle run."""

    variant_name: str
    config: ScenarioConfig
    success: bool = False
    output_dir: Path | None = None
    error: str = ""


@dataclass
class BundleResult:
    """Result of running a full scenario bundle."""

    bundle_name: str
    entries: list[BundleResultEntry] = field(default_factory=list)

    @property
    def n_variants(self) -> int:
        return len(self.entries)

    @property
    def n_succeeded(self) -> int:
        return sum(1 for e in self.entries if e.success)

    @property
    def n_failed(self) -> int:
        return self.n_variants - self.n_succeeded

    def to_dict(self) -> dict[str, Any]:
        """Serialize to JSON-compatible dict."""
        return {
            "bundle_name": self.bundle_name,
            "n_variants": self.n_variants,
            "n_succeeded": self.n_succeeded,
            "n_failed": self.n_failed,
            "entries": [
                {
                    "variant_name": e.variant_name,
                    "success": e.success,
                    "output_dir": str(e.output_dir) if e.output_dir else None,
                    "error": e.error,
                }
                for e in self.entries
            ],
        }


def run_bundle(
    config: BundleConfig,
    output_dir: Path | None = None,
    backend: object | None = None,
) -> BundleResult:
    """Execute all variants in a bundle.

    Args:
        config: Bundle configuration with base + variants.
        output_dir: Base directory for all variant output artifacts.
        backend: Model execution backend (passed to run_scenario).

    Returns:
        BundleResult with an entry per variant.
    """
    from fp_wraptr.scenarios.runner import run_scenario

    if output_dir is None:
        output_dir = Path("artifacts/bundle")

    base_name = config.base.get("name", "bundle")
    result = BundleResult(bundle_name=base_name)

    variants = config.resolve_variants()

    for variant_config in variants:
        entry = BundleResultEntry(
            variant_name=variant_config.name,
            config=variant_config,
        )

        try:
            scenario_result = run_scenario(
                config=variant_config,
                output_dir=output_dir,
                backend=backend,
            )
            entry.success = scenario_result.success
            entry.output_dir = scenario_result.output_dir
        except Exception as exc:
            entry.success = False
            entry.error = str(exc)

        result.entries.append(entry)

    return result


def _apply_patch(data: dict[str, Any], patch: dict[str, Any]) -> None:
    """Apply a flat key patch to a nested dict.

    Supports dotted keys and list indexing:
        "policies[0].jobs" -> data["policies"][0]["jobs"]
        "overrides.TRGHQ.value" -> data["overrides"]["TRGHQ"]["value"]
    """
    import re

    for key_path, value in patch.items():
        parts = re.split(r"\.", key_path)
        target = data
        for part in parts[:-1]:
            # Handle list indexing: "policies[0]"
            match = re.match(r"(\w+)\[(\d+)\]", part)
            if match:
                dict_key, index = match.group(1), int(match.group(2))
                target = target[dict_key][index]
            else:
                if part not in target:
                    target[part] = {}
                target = target[part]

        # Set the final value
        final = parts[-1]
        match = re.match(r"(\w+)\[(\d+)\]", final)
        if match:
            dict_key, index = match.group(1), int(match.group(2))
            target[dict_key][index] = value
        else:
            target[final] = value
