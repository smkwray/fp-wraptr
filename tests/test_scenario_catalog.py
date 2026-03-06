from __future__ import annotations

from pathlib import Path

from fp_wraptr.scenarios.catalog import (
    ScenarioCatalog,
    load_scenario_catalog,
    resolve_catalog_or_path,
)


def test_scenario_catalog_filters_surface_and_applies_order(tmp_path: Path) -> None:
    catalog_path = tmp_path / "projects_local" / "scenario_catalog.yaml"
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    scenario_path = tmp_path / "examples" / "baseline.yaml"
    scenario_path.parent.mkdir(parents=True, exist_ok=True)
    scenario_path.write_text("name: baseline\n", encoding="utf-8")
    bundle_path = tmp_path / "bundles" / "pse2025.yaml"
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    bundle_path.write_text("base: {}\n", encoding="utf-8")

    catalog_path.write_text(
        "\n".join(
            [
                "entries:",
                "  - id: baseline",
                "    label: Baseline",
                "    kind: scenario",
                "    path: examples/baseline.yaml",
                "    family: stock",
                "    surfaces: [home, new_run]",
                "    order: 20",
                "  - id: pse_bundle",
                "    label: PSE Bundle",
                "    kind: bundle",
                "    path: bundles/pse2025.yaml",
                "    family: pse2025",
                "    surfaces: [home]",
                "    order: 10",
                "  - id: hidden-new-run",
                "    label: Hidden",
                "    kind: scenario",
                "    path: examples/baseline.yaml",
                "    family: stock",
                "    surfaces: [new_run]",
                "    order: 5",
                "    new_run_visible: false",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    catalog = ScenarioCatalog.from_yaml(catalog_path, project_root=tmp_path)

    home_entries = catalog.filtered(surface="home")
    new_run_entries = catalog.filtered(surface="new_run", new_run_visible_only=True)
    home_bundle = catalog.for_surface("home", kind="bundle")

    assert [entry.entry_id for entry in home_entries] == ["pse_bundle", "baseline"]
    assert [entry.entry_id for entry in new_run_entries] == ["baseline"]
    assert [entry.entry_id for entry in home_bundle] == ["pse_bundle"]

    resolved_path, resolved_entry = resolve_catalog_or_path("baseline", catalog=catalog)
    assert resolved_entry is not None
    assert resolved_path == scenario_path.resolve()


def test_load_scenario_catalog_missing_file_returns_empty_catalog(tmp_path: Path) -> None:
    catalog = load_scenario_catalog(tmp_path / "does_not_exist.yaml", project_root=tmp_path)

    assert isinstance(catalog, ScenarioCatalog)
    assert catalog.entries == ()


def test_canonical_catalog_is_curated_stock_and_pse_only() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    catalog = load_scenario_catalog(repo_root=repo_root)

    assert [entry.entry_id for entry in catalog.entries] == [
        "stock-baseline",
        "pse2025-base",
        "pse2025-low",
        "pse2025-high",
        "pse2025-bundle",
        "pse2008-base",
        "pse2008-low",
        "pse2008-high",
        "pse2008-bundle",
    ]
    assert {entry.family for entry in catalog.entries} == {"stock", "pse2008", "pse2025"}
    assert {entry.kind for entry in catalog.entries} == {"scenario", "bundle"}
    assert [entry.entry_id for entry in catalog.filtered(surface="home", public_only=True)] == [
        "stock-baseline"
    ]
