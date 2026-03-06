"""Unit tests for Mini Dashboard helper utilities."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

plotly = pytest.importorskip("plotly")
pytest.importorskip("matplotlib")
go = plotly.graph_objects

from fp_wraptr.dashboard.mini_dash_helpers import (  # noqa: E402
    DEFAULT_TRANSFORM_DENOMINATOR,
    TRANSFORM_LVL_CHANGE,
    TRANSFORM_PCT_CHANGE,
    TRANSFORM_PCT_OF,
    PanelExportRun,
    apply_inversion_safe_hover_style,
    apply_mini_chart_layout,
    build_multi_panel_png,
    build_plotly_export_config,
    default_mini_dash_presets,
    default_selected_preset_names,
    delete_mini_dash_preset,
    load_mini_dash_presets,
    mini_dash_presets_path,
    panel_grid_shape,
    save_mini_dash_presets,
    upsert_mini_dash_preset,
)


def test_load_mini_dash_presets_seeds_defaults(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    presets, warning = load_mini_dash_presets(artifacts_dir)

    assert warning is None
    assert [item["name"] for item in presets] == [item["name"] for item in default_mini_dash_presets()]

    payload = json.loads(mini_dash_presets_path(artifacts_dir).read_text(encoding="utf-8"))
    assert payload["version"] == 1
    assert isinstance(payload["presets"], list)


def test_default_selected_preset_names_only_honors_literal_default() -> None:
    assert default_selected_preset_names(["PSE Economy", "PSE Employment/Wages"]) == []
    assert default_selected_preset_names(["Default", "PSE Economy"]) == ["Default"]
    assert default_selected_preset_names([" default "]) == ["default"]


def test_mini_dash_presets_crud_roundtrip(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    presets = default_mini_dash_presets()

    created = upsert_mini_dash_preset(
        presets,
        new_name="  custom   panel  ",
        variables=["gdp", "gdp", " pief ", " "],
    )
    assert created[-1] == {"name": "custom panel", "variables": ["GDP", "PIEF"]}

    edited = upsert_mini_dash_preset(
        created,
        original_name="custom panel",
        new_name="Custom Growth",
        variables=["gdpr", "rs"],
    )
    assert edited[-1] == {"name": "Custom Growth", "variables": ["GDPR", "RS"]}

    pruned = delete_mini_dash_preset(edited, name="Custom Growth")
    assert [item["name"] for item in pruned] == [item["name"] for item in default_mini_dash_presets()]

    save_mini_dash_presets(artifacts_dir, pruned)
    reloaded, warning = load_mini_dash_presets(artifacts_dir)
    assert warning is None
    assert reloaded == pruned


def test_mini_dash_presets_transforms_roundtrip(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    presets = default_mini_dash_presets()

    created = upsert_mini_dash_preset(
        presets,
        new_name="GDP share panel",
        variables=["GDP", "SG", "PIEF"],
        transforms={
            "SG": {"mode": TRANSFORM_PCT_OF, "denominator": "GDP"},
            "PIEF": {"mode": TRANSFORM_LVL_CHANGE},
            "GDP": {"mode": TRANSFORM_PCT_CHANGE},
            "GDPR": {"mode": "invalid-mode", "denominator": "GDP"},
            "MISSING": {"mode": TRANSFORM_PCT_OF, "denominator": "GDP"},
        },
    )
    latest = created[-1]
    assert latest["name"] == "GDP share panel"
    assert latest["variables"] == ["GDP", "SG", "PIEF"]
    assert latest["transforms"] == {
        "SG": {"mode": TRANSFORM_PCT_OF, "denominator": "GDP"},
        "GDP": {"mode": TRANSFORM_PCT_CHANGE},
        "PIEF": {"mode": TRANSFORM_LVL_CHANGE},
    }

    save_mini_dash_presets(artifacts_dir, created)
    reloaded, warning = load_mini_dash_presets(artifacts_dir)
    assert warning is None
    assert reloaded[-1]["name"] == "GDP share panel"
    assert reloaded[-1]["transforms"] == {
        "SG": {"mode": TRANSFORM_PCT_OF, "denominator": "GDP"},
        "GDP": {"mode": TRANSFORM_PCT_CHANGE},
        "PIEF": {"mode": TRANSFORM_LVL_CHANGE},
    }


def test_mini_dash_presets_run_comparisons_roundtrip(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    presets = default_mini_dash_presets()

    created = upsert_mini_dash_preset(
        presets,
        new_name="Compare panel",
        variables=["GDP", "SG", "PIEF"],
        run_comparisons={
            "SG": {"mode": "diff_vs_run", "reference_run_label": "baseline [fpexe]"},
            "PIEF": {"mode": "pct_diff_vs_run", "reference_run_label": "baseline [fppy]"},
            "GDP": {"mode": "none", "reference_run_label": "ignored"},
            "MISSING": {"mode": "diff_vs_run", "reference_run_label": "skip"},
        },
    )
    latest = created[-1]
    assert latest["run_comparisons"] == {
        "SG": {"mode": "diff_vs_run", "reference_run_label": "baseline [fpexe]"},
        "PIEF": {"mode": "pct_diff_vs_run", "reference_run_label": "baseline [fppy]"},
    }

    save_mini_dash_presets(artifacts_dir, created)
    reloaded, warning = load_mini_dash_presets(artifacts_dir)
    assert warning is None
    assert reloaded[-1]["run_comparisons"] == {
        "SG": {"mode": "diff_vs_run", "reference_run_label": "baseline [fpexe]"},
        "PIEF": {"mode": "pct_diff_vs_run", "reference_run_label": "baseline [fppy]"},
    }


def test_load_mini_dash_presets_version1_with_transforms_normalizes(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    path = mini_dash_presets_path(artifacts_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "presets": [
                    {
                        "name": "legacy",
                        "variables": ["gdp", "sg"],
                        "transforms": {
                            "sg": {"mode": "pct_of", "denominator": ""},
                            "GDP": {"mode": TRANSFORM_LVL_CHANGE},
                            "MISSING": {"mode": "level", "denominator": "GDP"},
                        },
                    }
                ],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    presets, warning = load_mini_dash_presets(artifacts_dir)
    assert warning is None
    assert presets == [
        {
            "name": "legacy",
            "variables": ["GDP", "SG"],
            "transforms": {
                "SG": {
                    "mode": TRANSFORM_PCT_OF,
                    "denominator": DEFAULT_TRANSFORM_DENOMINATOR,
                },
                "GDP": {"mode": TRANSFORM_LVL_CHANGE},
            },
        }
    ]


def test_load_mini_dash_presets_version1_with_run_comparisons_normalizes(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    path = mini_dash_presets_path(artifacts_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "presets": [
                    {
                        "name": "compare",
                        "variables": ["gdp", "sg"],
                        "run_comparisons": {
                            "sg": {"mode": "diff_vs_run", "reference_run_label": " baseline "},
                            "GDP": {"mode": "none", "reference_run_label": "ignored"},
                            "MISSING": {"mode": "pct_diff_vs_run", "reference_run_label": "skip"},
                        },
                    }
                ],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    presets, warning = load_mini_dash_presets(artifacts_dir)
    assert warning is None
    assert presets == [
        {
            "name": "compare",
            "variables": ["GDP", "SG"],
            "run_comparisons": {
                "SG": {"mode": "diff_vs_run", "reference_run_label": "baseline"},
            },
        }
    ]


def test_mini_dash_preset_validation_errors() -> None:
    presets = default_mini_dash_presets()

    with pytest.raises(ValueError, match="already exists"):
        upsert_mini_dash_preset(
            presets,
            new_name="pse economy",
            variables=["GDP"],
        )

    with pytest.raises(ValueError, match="at least one variable"):
        upsert_mini_dash_preset(
            presets,
            new_name="My Empty",
            variables=[],
        )


def test_load_mini_dash_presets_recovers_corrupt_json(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    path = mini_dash_presets_path(artifacts_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{bad json", encoding="utf-8")

    presets, warning = load_mini_dash_presets(artifacts_dir)

    assert warning is not None
    assert "reset to defaults" in warning
    assert [item["name"] for item in presets] == [item["name"] for item in default_mini_dash_presets()]


def test_apply_mini_chart_layout_and_hover_style() -> None:
    fig = go.Figure()
    apply_mini_chart_layout(fig, title="Nominal GDP", height=320)
    apply_inversion_safe_hover_style(fig, enabled=True)

    assert fig.layout.title.text == "Nominal GDP"
    assert fig.layout.title.x == 0.01
    assert fig.layout.legend.y == -0.25
    assert fig.layout.margin.b == 94
    assert fig.layout.hoverlabel.bgcolor == "#f9fafb"
    assert fig.layout.hoverlabel.font.color == "#111827"

    fig_default = go.Figure()
    apply_inversion_safe_hover_style(fig_default, enabled=False)
    assert fig_default.layout.hoverlabel.bgcolor is None


def test_build_plotly_export_config() -> None:
    config = build_plotly_export_config(width_px=1600, height_px=900, scale=2)
    options = config["toImageButtonOptions"]

    assert options["format"] == "png"
    assert options["width"] == 1600
    assert options["height"] == 900
    assert options["scale"] == 2


@pytest.mark.parametrize(
    ("var_count", "expected_shape"),
    [
        (4, (2, 2)),
        (6, (2, 3)),
    ],
)
def test_build_multi_panel_png_outputs_png(var_count: int, expected_shape: tuple[int, int]) -> None:
    assert panel_grid_shape(var_count) == expected_shape
    periods = [f"2025.{quarter}" for quarter in (1, 2, 3, 4)]
    variables = ["GDP", "GDPR", "PCPF", "PIEF", "SG", "RS"][:var_count]

    run_a = PanelExportRun(
        legend_label="baseline [fpexe]",
        periods=periods,
        series={name: [float(idx + jdx + 1) for idx in range(len(periods))] for jdx, name in enumerate(variables)},
    )
    run_b = PanelExportRun(
        legend_label="baseline [fppy]",
        periods=periods,
        series={
            name: [float((idx + jdx + 1) * 1.05) for idx in range(len(periods))]
            for jdx, name in enumerate(variables)
        },
    )

    png_bytes = build_multi_panel_png(
        runs=[run_a, run_b],
        variables=variables,
        period_tokens=periods,
        x_labels=periods,
        title_by_var={name: name for name in variables},
        units_by_var={name: "units" for name in variables},
        figure_title="Run Panels Export",
        dpi=220,
        forecast_start_label="2025.2",
    )

    assert png_bytes.startswith(b"\x89PNG\r\n\x1a\n")
    assert len(png_bytes) > 2048
