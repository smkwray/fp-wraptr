"""Run Panels helper utilities for presets and exports."""

from __future__ import annotations

import io
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import matplotlib
import plotly.graph_objects as go

matplotlib.use("Agg")
import matplotlib.pyplot as plt

MiniDashPreset = dict[str, object]
MiniDashTransformConfig = dict[str, str]
MiniDashTransformMap = dict[str, MiniDashTransformConfig]
MiniDashRunComparisonConfig = dict[str, str]
MiniDashRunComparisonMap = dict[str, MiniDashRunComparisonConfig]

TRANSFORM_LEVEL = "level"
TRANSFORM_PCT_OF = "pct_of"
TRANSFORM_LVL_CHANGE = "lvl_change"
TRANSFORM_PCT_CHANGE = "pct_change"
DEFAULT_TRANSFORM_DENOMINATOR = "GDP"
_VALID_TRANSFORM_MODES = {
    TRANSFORM_LEVEL,
    TRANSFORM_PCT_OF,
    TRANSFORM_LVL_CHANGE,
    TRANSFORM_PCT_CHANGE,
}

DEFAULT_MINI_DASH_PRESETS: tuple[MiniDashPreset, ...] = (
    {
        "name": "PSE Economy",
        "variables": ["GDPR", "GDP", "PCPF", "PIEF", "SG", "RS"],
    },
    {
        "name": "PSE Employment/Wages",
        "variables": ["UR", "E", "JGJ", "JF", "WF", "WR"],
    },
)


@dataclass(frozen=True)
class PanelExportRun:
    """Data container for a single run in multi-panel PNG export."""

    legend_label: str
    periods: list[str]
    series: Mapping[str, list[float]]


def mini_dash_presets_path(artifacts_dir: Path) -> Path:
    """Return the on-disk JSON path used for Run Panels presets."""
    return Path(artifacts_dir) / ".dashboard" / "mini_dash_presets.json"


def default_mini_dash_presets() -> list[MiniDashPreset]:
    """Return a detached copy of built-in Run Panels presets."""
    return [
        {
            "name": str(item["name"]),
            "variables": [str(token) for token in item["variables"]],
        }
        for item in DEFAULT_MINI_DASH_PRESETS
    ]


def default_selected_preset_names(preset_names: Sequence[str]) -> list[str]:
    """Return the startup-selected preset names, honoring only an explicit Default preset."""
    for raw_name in preset_names:
        name = normalize_preset_name(str(raw_name))
        if name.casefold() == "default":
            return [name]
    return []


def normalize_preset_name(name: str) -> str:
    """Normalize a preset name by trimming and collapsing whitespace."""
    return " ".join(str(name or "").split()).strip()


def normalize_preset_variables(variables: Sequence[str]) -> list[str]:
    """Normalize preset variable tokens to uppercase, unique, non-empty values."""
    normalized: list[str] = []
    seen: set[str] = set()
    for token in variables:
        value = str(token or "").strip().upper()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def normalize_preset_transforms(
    *,
    variables: Sequence[str],
    transforms: Mapping[str, Any] | None,
) -> MiniDashTransformMap:
    """Normalize preset transform map keyed by variable code."""
    allowed_vars = {str(token).strip().upper() for token in variables if str(token).strip()}
    if not allowed_vars:
        return {}
    if transforms is None:
        return {}
    if not isinstance(transforms, Mapping):
        raise ValueError("Preset transforms must be an object keyed by variable code.")

    out: MiniDashTransformMap = {}
    for raw_var, raw_cfg in transforms.items():
        var_name = str(raw_var or "").strip().upper()
        if not var_name or var_name not in allowed_vars:
            continue
        if not isinstance(raw_cfg, Mapping):
            raise ValueError(f"Transform config for '{var_name}' must be an object.")
        mode = str(raw_cfg.get("mode", TRANSFORM_LEVEL) or "").strip().lower() or TRANSFORM_LEVEL
        if mode not in _VALID_TRANSFORM_MODES:
            mode = TRANSFORM_LEVEL
        denominator = (
            str(raw_cfg.get("denominator", DEFAULT_TRANSFORM_DENOMINATOR) or "").strip().upper()
            or DEFAULT_TRANSFORM_DENOMINATOR
        )
        if mode == TRANSFORM_LEVEL:
            continue
        if mode == TRANSFORM_PCT_OF:
            out[var_name] = {"mode": TRANSFORM_PCT_OF, "denominator": denominator}
            continue
        out[var_name] = {"mode": mode}
    return out


def normalize_preset_run_comparisons(
    *,
    variables: Sequence[str],
    run_comparisons: Mapping[str, Any] | None,
) -> MiniDashRunComparisonMap:
    """Normalize preset run-comparison map keyed by variable code."""
    allowed_vars = {str(token).strip().upper() for token in variables if str(token).strip()}
    if not allowed_vars:
        return {}
    if run_comparisons is None:
        return {}
    if not isinstance(run_comparisons, Mapping):
        raise ValueError("Preset run comparisons must be an object keyed by variable code.")

    out: MiniDashRunComparisonMap = {}
    for raw_var, raw_cfg in run_comparisons.items():
        var_name = str(raw_var or "").strip().upper()
        if not var_name or var_name not in allowed_vars:
            continue
        if not isinstance(raw_cfg, Mapping):
            raise ValueError(f"Run comparison config for '{var_name}' must be an object.")
        mode = str(raw_cfg.get("mode", "none") or "").strip().lower() or "none"
        if mode not in {"none", "diff_vs_run", "pct_diff_vs_run"}:
            mode = "none"
        if mode == "none":
            continue
        reference_run_label = normalize_preset_name(str(raw_cfg.get("reference_run_label", "")))
        entry: MiniDashRunComparisonConfig = {"mode": mode}
        if reference_run_label:
            entry["reference_run_label"] = reference_run_label
        out[var_name] = entry
    return out


def _normalize_presets(
    presets: Sequence[Mapping[str, object]],
    *,
    allow_empty: bool = True,
) -> list[MiniDashPreset]:
    out: list[MiniDashPreset] = []
    names_seen: set[str] = set()
    for idx, item in enumerate(presets):
        if not isinstance(item, Mapping):
            raise ValueError(f"Preset at index {idx} must be an object.")
        name = normalize_preset_name(str(item.get("name", "")))
        variables_raw = item.get("variables", [])
        if not isinstance(variables_raw, Sequence) or isinstance(variables_raw, (str, bytes)):
            raise ValueError(f"Preset '{name or idx}' variables must be a list of strings.")
        variables = normalize_preset_variables([str(value) for value in variables_raw])
        if not name:
            raise ValueError("Preset name is required.")
        if not variables:
            raise ValueError(f"Preset '{name}' must include at least one variable.")
        folded = name.casefold()
        if folded in names_seen:
            raise ValueError(f"Duplicate preset name: {name}")
        names_seen.add(folded)
        transforms = normalize_preset_transforms(
            variables=variables,
            transforms=item.get("transforms"),
        )
        run_comparisons = normalize_preset_run_comparisons(
            variables=variables,
            run_comparisons=item.get("run_comparisons"),
        )
        preset: MiniDashPreset = {"name": name, "variables": variables}
        if transforms:
            preset["transforms"] = transforms
        if run_comparisons:
            preset["run_comparisons"] = run_comparisons
        out.append(preset)
    if not allow_empty and not out:
        raise ValueError("At least one preset is required.")
    return out


def save_mini_dash_presets(artifacts_dir: Path, presets: Sequence[Mapping[str, object]]) -> None:
    """Persist Run Panels presets using versioned JSON schema."""
    normalized = _normalize_presets(presets, allow_empty=True)
    payload = {"version": 1, "presets": normalized}
    out_path = mini_dash_presets_path(artifacts_dir)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def load_mini_dash_presets(artifacts_dir: Path) -> tuple[list[MiniDashPreset], str | None]:
    """Load Run Panels presets, seeding defaults or recovering from corruption."""
    path = mini_dash_presets_path(artifacts_dir)
    defaults = default_mini_dash_presets()
    if not path.exists():
        save_mini_dash_presets(artifacts_dir, defaults)
        return defaults, None

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError("Preset payload must be an object.")
        version = int(payload.get("version", 0))
        if version != 1:
            raise ValueError(f"Unsupported preset schema version: {version}")
        raw_presets = payload.get("presets")
        if not isinstance(raw_presets, list):
            raise ValueError("'presets' must be a list.")
        presets = _normalize_presets(raw_presets, allow_empty=True)
        return presets, None
    except Exception as exc:
        save_mini_dash_presets(artifacts_dir, defaults)
        return defaults, f"Preset file was invalid and was reset to defaults: {exc}"


def reset_mini_dash_presets(artifacts_dir: Path) -> list[MiniDashPreset]:
    """Reset presets back to built-in defaults and persist them."""
    defaults = default_mini_dash_presets()
    save_mini_dash_presets(artifacts_dir, defaults)
    return defaults


def upsert_mini_dash_preset(
    presets: Sequence[Mapping[str, object]],
    *,
    new_name: str,
    variables: Sequence[str],
    transforms: Mapping[str, Mapping[str, object]] | None = None,
    run_comparisons: Mapping[str, Mapping[str, object]] | None = None,
    original_name: str | None = None,
) -> list[MiniDashPreset]:
    """Create or edit a preset entry with normalization and validation."""
    current = _normalize_presets(presets, allow_empty=True)
    normalized_name = normalize_preset_name(new_name)
    normalized_variables = normalize_preset_variables(variables)
    if not normalized_name:
        raise ValueError("Preset name is required.")
    if not normalized_variables:
        raise ValueError("Preset must include at least one variable.")

    target_index: int | None = None
    if original_name:
        target_fold = normalize_preset_name(original_name).casefold()
        for idx, item in enumerate(current):
            if item["name"].casefold() == target_fold:
                target_index = idx
                break
        if target_index is None:
            raise ValueError(f"Preset not found: {original_name}")

    for idx, item in enumerate(current):
        if target_index is not None and idx == target_index:
            continue
        if item["name"].casefold() == normalized_name.casefold():
            raise ValueError(f"Preset name already exists: {normalized_name}")

    normalized_transforms = normalize_preset_transforms(
        variables=normalized_variables,
        transforms=transforms,
    )
    normalized_run_comparisons = normalize_preset_run_comparisons(
        variables=normalized_variables,
        run_comparisons=run_comparisons,
    )
    updated: MiniDashPreset = {"name": normalized_name, "variables": normalized_variables}
    if normalized_transforms:
        updated["transforms"] = normalized_transforms
    if normalized_run_comparisons:
        updated["run_comparisons"] = normalized_run_comparisons
    if target_index is None:
        current.append(updated)
    else:
        current[target_index] = updated
    return current


def delete_mini_dash_preset(
    presets: Sequence[Mapping[str, object]],
    *,
    name: str,
) -> list[MiniDashPreset]:
    """Delete one preset by name."""
    current = _normalize_presets(presets, allow_empty=True)
    target = normalize_preset_name(name).casefold()
    filtered = [item for item in current if item["name"].casefold() != target]
    if len(filtered) == len(current):
        raise ValueError(f"Preset not found: {name}")
    return filtered


def build_plotly_export_config(
    *,
    width_px: int,
    height_px: int,
    scale: int,
) -> dict[str, object]:
    """Build Plotly modebar export settings for higher-resolution downloads."""
    width_value = max(200, int(width_px))
    height_value = max(200, int(height_px))
    scale_value = max(1, int(scale))
    return {
        "displaylogo": False,
        "toImageButtonOptions": {
            "format": "png",
            "filename": "mini_dashboard_chart",
            "width": width_value,
            "height": height_value,
            "scale": scale_value,
        },
    }


def apply_mini_chart_layout(
    fig: go.Figure,
    *,
    title: str,
    height: int,
) -> go.Figure:
    """Apply a stable Run Panels layout that avoids title/legend overlap."""
    fig.update_layout(
        title=dict(text=title, x=0.01, xanchor="left", y=0.98, yanchor="top"),
        height=int(height),
        margin=dict(l=10, r=10, t=74, b=94),
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.25,
            xanchor="left",
            x=0,
            title_text="",
            traceorder="normal",
        ),
    )
    return fig


def apply_inversion_safe_hover_style(
    fig: go.Figure,
    *,
    enabled: bool = True,
) -> go.Figure:
    """Apply explicit hover colors so tooltips remain readable with browser inversion."""
    if not enabled:
        return fig
    fig.update_layout(
        hoverlabel=dict(
            bgcolor="#f9fafb",
            bordercolor="#111827",
            font=dict(color="#111827"),
        )
    )
    return fig


def panel_grid_shape(variable_count: int) -> tuple[int, int]:
    """Return rows/cols for multi-panel export layout."""
    if variable_count <= 0:
        raise ValueError("At least one variable is required.")
    if variable_count == 1:
        return (1, 1)
    if variable_count == 2:
        return (1, 2)
    if variable_count <= 4:
        return (2, 2)
    if variable_count <= 6:
        return (2, 3)
    raise ValueError("Panel export supports at most 6 variables.")


def _plot_axis(
    *,
    ax: matplotlib.axes.Axes,
    variables_name: str,
    title_by_var: Mapping[str, str],
    units_by_var: Mapping[str, str],
    runs: Sequence[PanelExportRun],
    period_tokens: Sequence[str],
    x_labels: Sequence[str],
    color_by_run: Mapping[str, str],
    forecast_start_label: str | None,
) -> None:
    x_positions = list(range(len(period_tokens)))
    for run in runs:
        values = run.series.get(variables_name)
        if not isinstance(values, Sequence):
            continue
        value_map: dict[str, float] = {}
        for period, raw in zip(run.periods, values, strict=False):
            try:
                value_map[str(period)] = float(raw)
            except (TypeError, ValueError):
                value_map[str(period)] = float("nan")
        y_values: list[float] = [value_map.get(token, float("nan")) for token in period_tokens]
        if not y_values:
            continue
        ax.plot(
            x_positions,
            y_values,
            label=run.legend_label,
            color=color_by_run.get(run.legend_label),
            linewidth=2.0,
        )

    ax.set_title(title_by_var.get(variables_name, variables_name))
    units = str(units_by_var.get(variables_name, "") or "").strip()
    if units:
        ax.set_ylabel(units)
    ax.grid(True, alpha=0.25)

    if x_positions:
        step = max(1, len(x_positions) // 8)
        ticks = x_positions[::step]
        tick_labels = [x_labels[idx] for idx in ticks]
        ax.set_xticks(ticks)
        ax.set_xticklabels(tick_labels, rotation=45, ha="right")

    if forecast_start_label and forecast_start_label in x_labels:
        ax.axvline(x_labels.index(forecast_start_label), color="black", alpha=0.25, linewidth=1)


def build_multi_panel_png(
    *,
    runs: Sequence[PanelExportRun],
    variables: Sequence[str],
    period_tokens: Sequence[str],
    x_labels: Sequence[str],
    title_by_var: Mapping[str, str],
    units_by_var: Mapping[str, str],
    figure_title: str,
    dpi: int = 300,
    forecast_start_label: str | None = None,
) -> bytes:
    """Build a single PNG containing multiple variable panels across selected runs."""
    if not runs:
        raise ValueError("At least one run is required.")
    if not period_tokens or not x_labels:
        raise ValueError("At least one period is required.")
    if len(period_tokens) != len(x_labels):
        raise ValueError("period_tokens and x_labels must have identical lengths.")

    normalized_vars = normalize_preset_variables(variables)
    if not normalized_vars:
        raise ValueError("Select at least one variable for panel export.")
    if len(normalized_vars) > 6:
        raise ValueError("Panel export supports at most 6 variables.")

    rows, cols = panel_grid_shape(len(normalized_vars))
    fig_width = 6.2 * cols
    fig_height = 4.6 * rows
    fig, axes = plt.subplots(rows, cols, figsize=(fig_width, fig_height))

    axes_list = list(axes.flatten()) if hasattr(axes, "flatten") else [axes]
    palette = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b"]
    color_by_run = {run.legend_label: palette[idx % len(palette)] for idx, run in enumerate(runs)}

    for ax, var_name in zip(axes_list, normalized_vars, strict=False):
        _plot_axis(
            ax=ax,
            variables_name=var_name,
            title_by_var=title_by_var,
            units_by_var=units_by_var,
            runs=runs,
            period_tokens=period_tokens,
            x_labels=x_labels,
            color_by_run=color_by_run,
            forecast_start_label=forecast_start_label,
        )

    for ax in axes_list[len(normalized_vars) :]:
        ax.axis("off")

    handles: list[object] = []
    labels: list[str] = []
    for ax in axes_list:
        h, legend_labels = ax.get_legend_handles_labels()
        if h:
            handles = h
            labels = legend_labels
            break

    fig.suptitle(str(figure_title or "Run Panels Export").strip(), y=0.995, fontsize=16)
    if handles:
        fig.legend(
            handles,
            labels,
            loc="upper center",
            bbox_to_anchor=(0.5, 0.955),
            ncol=max(1, min(3, len(labels))),
            frameon=False,
        )
    fig.tight_layout(rect=(0, 0, 1, 0.90))

    dpi_value = max(72, int(dpi))
    out = io.BytesIO()
    fig.savefig(out, format="png", dpi=dpi_value)
    plt.close(fig)
    out.seek(0)
    return out.getvalue()
