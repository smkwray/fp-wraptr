"""Typed configuration loading for FAIR model and tabular source inputs."""

from __future__ import annotations

import json
import tomllib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fppy.paths import TEMPLATE_DIR


@dataclass(frozen=True)
class LegacyModelFileConfig:
    """Legacy FAIR file layout used by the template inputs."""

    fminput: Path
    fmdata: Path
    fmage: Path
    fmexog: Path
    fmout: Path


@dataclass(frozen=True)
class ExternalTabularSource:
    """Definition for an optional external tabular source."""

    name: str
    path: Path
    format: str = "auto"


@dataclass(frozen=True)
class ModelInputConfig:
    """Overall model/input configuration."""

    legacy: LegacyModelFileConfig
    external_sources: tuple[ExternalTabularSource, ...] = field(default_factory=tuple)


ModelConfig = ModelInputConfig
LegacyModelFiles = LegacyModelFileConfig


__all__ = [
    "ExternalTabularSource",
    "LegacyModelFileConfig",
    "LegacyModelFiles",
    "ModelConfig",
    "ModelInputConfig",
    "default_legacy_config",
    "load_model_config",
]


_DEFAULT_PATHS = {
    "fminput": TEMPLATE_DIR / "fminput.txt",
    "fmdata": TEMPLATE_DIR / "fmdata.txt",
    "fmage": TEMPLATE_DIR / "fmage.txt",
    "fmexog": TEMPLATE_DIR / "fmexog.txt",
    "fmout": TEMPLATE_DIR / "fmout.txt",
}

_LEGACY_KEYS: tuple[str, ...] = ("fminput", "fmdata", "fmage", "fmexog", "fmout")
_SOURCE_FIELD_KEYS: tuple[str, ...] = ("name", "path", "format")
_SOURCE_LIST_KEYS: tuple[str, ...] = ("external_sources", "sources", "tabular_sources")
_SUPPORTED_SOURCE_FORMATS = {"auto", "csv", "json", "parquet"}


def default_legacy_config() -> ModelInputConfig:
    """Return the default model config using template-backed legacy paths."""
    return ModelInputConfig(
        legacy=LegacyModelFileConfig(**_DEFAULT_PATHS),
    )


def load_model_config(path: Path | str | None) -> ModelInputConfig:
    """Load a model config from a TOML or JSON file.

    If `path` is `None`, returns the legacy template-backed defaults.
    """
    if path is None:
        return default_legacy_config()

    config_path = Path(path)
    raw = _load_config_file(config_path)
    base_dir = config_path.parent
    defaults = default_legacy_config()
    legacy = _build_legacy_config(raw, defaults.legacy, base_dir=base_dir)
    external_sources = _build_external_sources(raw, base_dir=base_dir)

    return ModelInputConfig(legacy=legacy, external_sources=external_sources)


def _load_config_file(path: Path) -> Mapping[str, Any]:
    suffix = path.suffix.lower()

    if suffix == ".toml":
        with path.open("rb") as handle:
            payload = tomllib.load(handle)
    elif suffix == ".json":
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    else:
        raise ValueError(f"Unsupported config format: {path.suffix!r}. Use .toml or .json.")

    if not isinstance(payload, Mapping):
        raise TypeError("Config file top-level content must be a mapping/object.")
    return payload


def _coerce_path(value: Any, *, base: Path, field_name: str) -> Path:
    if not isinstance(value, (str, Path)):
        raise TypeError(f"{field_name} must be a string path.")

    source = Path(value).expanduser()
    if source.is_absolute():
        return source
    return (base / source).resolve()


def _build_legacy_config(
    raw: Mapping[str, Any],
    defaults: LegacyModelFileConfig,
    *,
    base_dir: Path,
) -> LegacyModelFileConfig:
    legacy_values: dict[str, Path] = {}
    model_section = raw.get("model")
    nested_sections: list[Mapping[str, Any]] = [raw]

    if model_section is not None:
        if not isinstance(model_section, Mapping):
            raise TypeError("`model` must be a TOML/JSON table/object.")
        nested_sections.append(model_section)

        legacy_nested = model_section.get("legacy")
        if legacy_nested is not None:
            if not isinstance(legacy_nested, Mapping):
                raise TypeError("`model.legacy` must be a table/object.")
            nested_sections.append(legacy_nested)

    for key in _LEGACY_KEYS:
        value = None
        for section in nested_sections:
            if key in section:
                value = section[key]
        if value is None:
            legacy_values[key] = getattr(defaults, key)
            continue
        legacy_values[key] = _coerce_path(value, base=base_dir, field_name=key)

    return LegacyModelFileConfig(**legacy_values)


def _extract_source_spec(raw: Mapping[str, Any], *, base_dir: Path) -> Sequence[Mapping[str, Any]]:
    candidate: Any | None = None

    for key in _SOURCE_LIST_KEYS:
        candidate = raw.get(key, candidate)

    model_section = raw.get("model")
    if isinstance(model_section, Mapping):
        for key in _SOURCE_LIST_KEYS:
            candidate = model_section.get(key, candidate)

    if candidate is None:
        return ()

    if not isinstance(candidate, Sequence) or isinstance(candidate, (str, bytes, bytearray)):
        raise TypeError("`external_sources` must be a list/table of source objects.")
    return tuple(candidate)


def _build_external_sources(
    raw: Mapping[str, Any],
    *,
    base_dir: Path,
) -> tuple[ExternalTabularSource, ...]:
    source_rows = _extract_source_spec(raw, base_dir=base_dir)
    parsed: list[ExternalTabularSource] = []

    for index, item in enumerate(source_rows):
        if not isinstance(item, Mapping):
            raise TypeError(f"external_sources[{index}] must be a mapping/object.")

        name = item.get("name")
        path_value = item.get("path")
        fmt = item.get("format", "auto")

        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"external_sources[{index}].name must be a non-empty string.")
        if not isinstance(fmt, str):
            raise TypeError(f"external_sources[{index}].format must be a string.")

        fmt_normalized = fmt.strip().lower()
        if fmt_normalized not in _SUPPORTED_SOURCE_FORMATS:
            raise ValueError(
                f"external_sources[{index}].format must be one of "
                f"{sorted(_SUPPORTED_SOURCE_FORMATS)!r}."
            )

        resolved_path = _coerce_path(
            path_value,
            base=base_dir,
            field_name=f"external_sources[{index}].path",
        )

        parsed.append(ExternalTabularSource(name=name, path=resolved_path, format=fmt_normalized))

    return tuple(parsed)
