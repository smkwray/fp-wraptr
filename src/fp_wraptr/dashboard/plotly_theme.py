"""Plotly theming helpers for Streamlit.

Streamlit dark mode can make Plotly charts hard to read if figures inherit
transparent backgrounds. fp-wraptr defaults to an explicit white theme for
visibility, while still allowing pages to opt out.
"""

from __future__ import annotations

import plotly.graph_objects as go

__all__ = ["FP_COLOR_PALETTE", "apply_white_theme"]

# Colorblind-friendly palette (Tableau 10 inspired, muted tones).
FP_COLOR_PALETTE: list[str] = [
    "#4e79a7",  # steel blue
    "#f28e2b",  # orange
    "#e15759",  # red
    "#76b7b2",  # teal
    "#59a14f",  # green
    "#edc948",  # gold
    "#b07aa1",  # purple
    "#ff9da7",  # pink
    "#9c755f",  # brown
    "#bab0ac",  # grey
]


def apply_white_theme(fig: go.Figure, *, enabled: bool = True) -> go.Figure:
    """Force a white Plotly theme suitable for Streamlit dark mode."""
    if not enabled:
        return fig

    text_color = "#111827"
    grid_color = "#e5e7eb"

    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="white",
        plot_bgcolor="white",
        font=dict(color=text_color),
        title=dict(font=dict(color=text_color)),
        legend=dict(font=dict(color=text_color), title=dict(font=dict(color=text_color))),
        hoverlabel=dict(font=dict(color=text_color)),
    )
    axis_style = dict(
        gridcolor=grid_color,
        linecolor=text_color,
        tickfont=dict(color=text_color),
        title_font=dict(color=text_color),
        zerolinecolor=grid_color,
    )
    fig.update_xaxes(**axis_style)
    fig.update_yaxes(**axis_style)

    # Plotly templates can leave some subplot axes with grey tick/title fonts,
    # especially when figures mutate axes after initial theme application.
    # Force explicit per-axis overrides for every xaxis*/yaxis* present.
    layout_updates: dict[str, dict] = {}
    for key in fig.layout:
        if not (key.startswith("xaxis") or key.startswith("yaxis")):
            continue
        layout_updates[key] = axis_style
    if layout_updates:
        fig.update_layout(**layout_updates)
    return fig
