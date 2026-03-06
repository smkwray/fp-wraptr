"""Plotly chart builders for dashboard forecast and comparison views."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from fp_wraptr.dashboard.plotly_theme import FP_COLOR_PALETTE, apply_white_theme
from fp_wraptr.io.parser import ForecastVariable, FPOutputData
from fp_wraptr.viz.period_labels import format_period_label

__all__ = [
    "comparison_figure",
    "delta_bar_chart",
    "forecast_figure",
    "multi_run_forecast_figure",
]

_PERIOD_TOKEN_RE = re.compile(r"^(?P<year>\d{4})\.(?P<sub>\d+)$")


def _period_key(token: str) -> tuple[int, int, str]:
    match = _PERIOD_TOKEN_RE.match(str(token).strip())
    if not match:
        return (9999, 9999, str(token))
    return (int(match.group("year")), int(match.group("sub")), str(token))


def _sorted_period_tokens(tokens: set[str]) -> list[str]:
    return sorted(tokens, key=_period_key)


def _format_axis_label(var_name: str, units: Mapping[str, str] | None) -> str:
    if not units:
        return var_name
    unit = str(units.get(var_name, "") or "").strip()
    if not unit:
        return var_name
    return f"{var_name} ({unit})"


def _resolve_mode_values(variable: ForecastVariable, mode: str) -> list[float]:
    if mode == "levels":
        return variable.levels
    if mode == "changes":
        return variable.changes
    if mode == "pct_changes":
        return variable.pct_changes
    raise ValueError(f"Invalid mode '{mode}'")


def _coerce_numeric(value: object) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def forecast_figure(
    data: FPOutputData,
    variables: list[str] | None = None,
    title: str | None = None,
    mode: str = "levels",
    *,
    units: Mapping[str, str] | None = None,
) -> go.Figure:
    """Build a multi-panel forecast figure for selected variables."""
    selected = list(variables) if variables else list(data.variables.keys())
    selected = [name for name in selected if name in data.variables]

    if not selected:
        fig = go.Figure()
        fig.update_layout(title=title or "Forecast")
        return apply_white_theme(fig)

    axis_tokens = list(data.periods or [])
    axis_labels = [format_period_label(tok) for tok in axis_tokens] if axis_tokens else []

    fig = make_subplots(
        rows=len(selected),
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
    )

    for row_idx, var_name in enumerate(selected, start=1):
        variable = data.variables[var_name]
        y_values = _resolve_mode_values(variable, mode)
        if axis_tokens:
            x_tokens = axis_tokens[: len(y_values)]
            x_values = [format_period_label(tok) for tok in x_tokens]
        else:
            x_values = list(range(len(y_values)))

        color = FP_COLOR_PALETTE[(row_idx - 1) % len(FP_COLOR_PALETTE)]
        fig.add_trace(
            go.Scatter(
                x=x_values,
                y=y_values,
                mode="lines+markers",
                name=var_name,
                line=dict(color=color),
                marker=dict(color=color, size=6),
                hovertemplate="%{x}<br>%{y:.4f}<extra>" + var_name + "</extra>",
            ),
            row=row_idx,
            col=1,
        )
        fig.update_yaxes(title_text=_format_axis_label(var_name, units), row=row_idx, col=1)

    fig.update_layout(
        title=title or "Forecast",
        height=260 * max(1, len(selected)),
        showlegend=False,
    )
    if axis_labels:
        fig.update_xaxes(
            type="category",
            categoryorder="array",
            categoryarray=axis_labels,
        )
    fig.update_xaxes(title_text="Period", row=len(selected), col=1)
    return apply_white_theme(fig)


def comparison_figure(
    baseline: FPOutputData,
    scenario: FPOutputData,
    variables: list[str] | None = None,
    title: str | None = None,
    baseline_label: str = "Baseline",
    scenario_label: str = "Scenario",
    *,
    units: Mapping[str, str] | None = None,
) -> go.Figure:
    """Build baseline-vs-scenario subplots with area shading between series."""
    requested = list(variables) if variables else list(baseline.variables.keys())
    selected = [
        var_name
        for var_name in requested
        if var_name in baseline.variables and var_name in scenario.variables
    ]

    if not selected:
        fig = go.Figure()
        fig.update_layout(title=title or "Forecast comparison")
        return apply_white_theme(fig)

    fig = make_subplots(
        rows=len(selected),
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
    )

    for row_idx, var_name in enumerate(selected, start=1):
        base = baseline.variables[var_name]
        scen = scenario.variables[var_name]
        baseline_has_periods = bool(baseline.periods)
        scenario_has_periods = bool(scenario.periods)
        baseline_periods = (
            baseline.periods if baseline_has_periods else list(range(len(base.levels)))
        )
        scenario_periods = (
            scenario.periods if scenario_has_periods else list(range(len(scen.levels)))
        )
        point_count = min(
            len(base.levels),
            len(scen.levels),
            len(baseline_periods),
            len(scenario_periods),
        )
        if point_count <= 0:
            continue
        x_tokens = (
            baseline_periods[:point_count] if baseline_periods else scenario_periods[:point_count]
        )
        x_values = (
            [format_period_label(tok) for tok in x_tokens]
            if baseline_has_periods
            else list(x_tokens)
        )

        fig.add_trace(
            go.Scatter(
                x=x_values,
                y=base.levels[:point_count],
                mode="lines+markers",
                name=f"{baseline_label}: {var_name}",
                showlegend=row_idx == 1,
                line=dict(color=FP_COLOR_PALETTE[0]),
                marker=dict(size=6),
                hovertemplate="%{x}<br>%{y:.4f}<extra>" + baseline_label + "</extra>",
            ),
            row=row_idx,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=x_values,
                y=scen.levels[:point_count],
                mode="lines+markers",
                name=f"{scenario_label}: {var_name}",
                showlegend=row_idx == 1,
                fill="tonexty",
                fillcolor="rgba(89, 161, 79, 0.15)",
                line=dict(color=FP_COLOR_PALETTE[4]),
                marker=dict(size=6),
                hovertemplate="%{x}<br>%{y:.4f}<extra>" + scenario_label + "</extra>",
            ),
            row=row_idx,
            col=1,
        )
        fig.update_yaxes(title_text=_format_axis_label(var_name, units), row=row_idx, col=1)
        if baseline_has_periods:
            fig.update_xaxes(
                type="category",
                categoryorder="array",
                categoryarray=x_values,
                row=row_idx,
                col=1,
            )

    fig.update_layout(
        title=title or "Scenario comparison",
        height=280 * max(1, len(selected)),
    )
    fig.update_xaxes(title_text="Period", row=len(selected), col=1)
    return apply_white_theme(fig)


def _sort_key(
    value: float,
    sort_by: str,
    descending: bool = True,
) -> float:
    if sort_by == "abs_delta":
        return abs(value)
    if descending:
        return value
    return -value


def delta_bar_chart(
    diff_result: dict,
    top_n: int = 15,
    sort_by: str = "abs_delta",
) -> go.Figure:
    """Build a horizontal bar chart of top-delta forecast changes."""
    if sort_by not in {"abs_delta", "baseline", "scenario", "pct_delta"}:
        raise ValueError(f"Invalid sort_by '{sort_by}'")

    deltas: dict[str, dict[str, float]] = diff_result.get("deltas", {})
    items: Sequence[tuple[str, dict[str, float]]] = sorted(
        deltas.items(),
        key=lambda item: _sort_key(
            _coerce_numeric(item[1].get(sort_by, 0.0)),
            sort_by,
        ),
        reverse=True,
    )

    selected = list(items[:top_n]) if top_n > 0 else list(items)
    variable_names = [name for name, _ in selected]
    raw_values = [delta.get(sort_by, 0.0) for _, delta in selected]
    values = [_coerce_numeric(value) for value in raw_values]
    colors = [
        "#16a34a" if value > 0 else "#dc2626" if value < 0 else "#6b7280" for value in values
    ]
    text_values = [
        f"{value:.4f}" if isinstance(value, (int, float)) else "N/A" for value in raw_values
    ]

    fig = go.Figure(
        data=[
            go.Bar(
                x=values,
                y=variable_names,
                orientation="h",
                marker_color=colors,
                text=text_values,
                textposition="auto",
            )
        ]
    )
    sort_label = sort_by.replace("_", " ").title()
    fig.update_layout(
        title=f"Top {top_n} Variable Deltas ({sort_label})",
        xaxis_title=sort_label,
        yaxis_title="Variable",
        height=max(300, 35 * max(1, len(variable_names))),
    )
    return apply_white_theme(fig)


def multi_run_forecast_figure(
    runs: Sequence[tuple[str, FPOutputData]],
    variables: list[str],
    *,
    title: str | None = None,
    mode: str = "levels",
    units: Mapping[str, str] | None = None,
) -> go.Figure:
    """Build a multi-panel forecast overlay figure for multiple runs.

    Each selected variable is a subplot row; each run becomes a line trace per row.
    Legend entries are shown once (on the first row) to avoid duplicates.
    """
    cleaned_runs = [(label, data) for label, data in runs if label and data is not None]
    if not cleaned_runs or not variables:
        fig = go.Figure()
        fig.update_layout(title=title or "Forecast overlay")
        return apply_white_theme(fig)

    # Prefer a stable, chronological union of period tokens.
    all_tokens: set[str] = set()
    for _, data in cleaned_runs:
        all_tokens.update(str(tok) for tok in (data.periods or []) if str(tok).strip())
    axis_tokens = _sorted_period_tokens(all_tokens) if all_tokens else []
    axis_labels = [format_period_label(tok) for tok in axis_tokens] if axis_tokens else []

    fig = make_subplots(
        rows=len(variables),
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
    )

    stable_labels = sorted({label for label, _ in cleaned_runs})
    color_map = {
        label: FP_COLOR_PALETTE[idx % len(FP_COLOR_PALETTE)]
        for idx, label in enumerate(stable_labels)
    }

    for row_idx, var_name in enumerate(variables, start=1):
        for run_label, data in cleaned_runs:
            variable = data.variables.get(var_name) if getattr(data, "variables", None) else None
            if variable is None:
                continue
            y_values = _resolve_mode_values(variable, mode)
            if axis_tokens and data.periods:
                x_tokens = list(data.periods)[: len(y_values)]
                value_map = {str(p): v for p, v in zip(x_tokens, y_values, strict=False)}
                y = [value_map.get(tok) for tok in axis_tokens]
                x_values = axis_labels
            else:
                # Fallback: plot by index if periods are missing.
                x_values = list(range(len(y_values)))
                y = y_values

            color = color_map.get(run_label)
            fig.add_trace(
                go.Scatter(
                    x=x_values,
                    y=y,
                    mode="lines+markers",
                    name=run_label,
                    showlegend=(row_idx == 1),
                    line=dict(color=color),
                    marker=dict(color=color, size=6),
                    hovertemplate="%{x}<br>%{y:.4f}<extra>" + run_label + "</extra>",
                ),
                row=row_idx,
                col=1,
            )
        fig.update_yaxes(title_text=_format_axis_label(var_name, units), row=row_idx, col=1)

        if axis_labels:
            fig.update_xaxes(
                type="category",
                categoryorder="array",
                categoryarray=axis_labels,
                row=row_idx,
                col=1,
            )

    fig.update_layout(
        title=title or "Forecast overlay",
        height=260 * max(1, len(variables)),
    )
    fig.update_xaxes(title_text="Period", row=len(variables), col=1)
    return apply_white_theme(fig)
