from __future__ import annotations

import asyncio
import json
from pathlib import Path

from fp_wraptr.mcp_server import (
    apply_workspace_card as mcp_apply_workspace_card,
)
from fp_wraptr.mcp_server import (
    clone_bundle_variant_recipe as mcp_clone_bundle_variant_recipe,
)
from fp_wraptr.mcp_server import (
    update_bundle_variant as mcp_update_bundle_variant,
)
from fp_wraptr.mcp_server import (
    build_visualization_view as mcp_build_visualization_view,
)
from fp_wraptr.mcp_server import (
    create_workspace_from_catalog as mcp_create_workspace_from_catalog,
)
from fp_wraptr.mcp_server import (
    describe_pack as mcp_describe_pack,
)
from fp_wraptr.mcp_server import (
    list_packs as mcp_list_packs,
)
from fp_wraptr.mcp_server import (
    list_visualizations as mcp_list_visualizations,
)
from fp_wraptr.mcp_server import (
    list_workspace_cards as mcp_list_workspace_cards,
)
from fp_wraptr.mcp_server import (
    mcp,
)
from fp_wraptr.scenarios.authoring import (
    add_bundle_variant,
    apply_workspace_card,
    build_visualization_view,
    clone_bundle_variant_recipe,
    compile_workspace,
    create_workspace_from_catalog,
    import_workspace_series,
    list_packs,
    list_workspace_cards,
    update_bundle_variant,
)
from fp_wraptr.scenarios.packs import describe_pack_manifest
from tests.test_authoring import _make_repo

_FMOUT_TEXT = """\
SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;
Variable   Periods forecast are  2025.4  TO   2026.1

                   2025.4      2026.1

   1 INTGADJ  P lv   1.0      2.0
              P ch   0.1      0.2
              P %ch  10.0     10.0

   2 JGW      P lv   0.02     0.03
              P ch   0.00     0.01
              P %ch  0.0      50.0
"""


def _write_pack_manifest(repo: Path) -> None:
    target = repo / "projects_local" / "packs" / "pse2025" / "pack.yaml"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        "\n".join(
            [
                "pack_id: pse2025",
                "label: PSE2025 Working Pack",
                "family: pse2025",
                "description: Local PSE pack",
                "visibility: local",
                "source_dir: projects_local/pse2025",
                "cards_family: pse2025",
                "catalog_entry_ids: [pse-base, pse-bundle]",
                "recipes:",
                "  - recipe_id: change-coefficients",
                "    label: Change coefficients",
                "    summary: Update constants in managed cards.",
                "visualizations:",
                "  - view_id: pse-main",
                "    label: Main tracks",
                "    chart_type: forecast_overlay",
                "    variables: [INTGADJ, JGW]",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _write_run(root: Path) -> Path:
    run_dir = root / "artifacts" / "agent-test_20260306_120000"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "fmout.txt").write_text(_FMOUT_TEXT, encoding="utf-8")
    (run_dir / "scenario.yaml").write_text("name: agent-test\n", encoding="utf-8")
    return run_dir


def test_pack_manifest_and_workspace_operations(tmp_path: Path) -> None:
    repo = _make_repo(tmp_path / "repo")
    _write_pack_manifest(repo)

    packs = list_packs(repo_root=repo)
    assert [item["pack_id"] for item in packs] == ["pse2025"]

    pack = describe_pack_manifest("pse2025", repo_root=repo)
    assert pack["cards"][0]["card_id"] == "pse2025.intgadj"
    assert pack["catalog_entries"][0]["entry_id"] == "pse-base"

    workspace = create_workspace_from_catalog(repo_root=repo, catalog_entry_id="pse-base")
    assert workspace["workspace_id"] == "pse2025--pse2025-base"

    updated = apply_workspace_card(
        repo_root=repo,
        workspace_id=str(workspace["workspace_id"]),
        card_id="pse2025.jg_constants",
        constants={"JPTUR": 0.55},
    )
    assert updated["recipe_history"][-1]["operation"] == "apply_workspace_card"

    updated = import_workspace_series(
        repo_root=repo,
        workspace_id=str(workspace["workspace_id"]),
        card_id="pse2025.intgadj",
        series_points={"2025.4": 1.5, "2026.1": 2.5},
    )
    assert updated["recipe_history"][-1]["operation"] == "import_workspace_series"

    compile_payload = compile_workspace(repo_root=repo, workspace_id=str(workspace["workspace_id"]))
    assert compile_payload["ok"] is True
    assert compile_payload["visualizations"][0]["view_id"] == "forecast-overlay"

    run_dir = _write_run(repo)
    viz = build_visualization_view(
        repo_root=repo,
        workspace_id=str(workspace["workspace_id"]),
        view_id="pse-main",
        run_dirs=[str(run_dir)],
    )
    assert viz["view_id"] == "pse-main"
    assert viz["runs"][0]["variables"]["INTGADJ"]["levels"] == [1.0, 2.0]

    bundle_workspace = create_workspace_from_catalog(repo_root=repo, catalog_entry_id="pse-bundle")
    mutated_bundle = add_bundle_variant(
        repo_root=repo,
        workspace_id=str(bundle_workspace["workspace_id"]),
        variant_id="custom",
        scenario_name="custom",
        clone_from="base",
        input_file="psebase.txt",
    )
    assert mutated_bundle["variant_count"] == 4
    assert mutated_bundle["variants"][-1]["scenario_name"] == "custom"

    variant_cards = list_workspace_cards(
        repo_root=repo,
        workspace_id=str(bundle_workspace["workspace_id"]),
        variant_id="custom",
    )
    assert variant_cards["scope"] == "variant"
    assert variant_cards["variant_id"] == "custom"

    updated_bundle = update_bundle_variant(
        repo_root=repo,
        workspace_id=str(bundle_workspace["workspace_id"]),
        variant_id="custom",
        label="Custom Variant",
        scenario_name="custom_exact",
        input_file="pselow.txt",
        enabled=False,
    )
    updated_variant = next(item for item in updated_bundle["variants"] if item["variant_id"] == "custom")
    assert updated_variant["label"] == "Custom Variant"
    assert updated_variant["scenario_name"] == "custom_exact"
    assert updated_variant["input_file"] == "pselow.txt"
    assert updated_variant["enabled"] is False

    recipe_bundle = clone_bundle_variant_recipe(
        repo_root=repo,
        workspace_id=str(bundle_workspace["workspace_id"]),
        variant_id="recipe_variant",
        clone_from="low",
        label="Recipe Variant",
        scenario_name="recipe_variant_exact",
        input_file="pselow.txt",
        card_id="pse2025.jg_constants",
        constants={"JGW": 0.015, "JGCOLA": 0.0},
    )
    recipe_variant = next(item for item in recipe_bundle["variants"] if item["variant_id"] == "recipe_variant")
    assert recipe_variant["scenario_name"] == "recipe_variant_exact"

    compile_payload = compile_workspace(repo_root=repo, workspace_id=str(bundle_workspace["workspace_id"]))
    assert compile_payload["ok"] is True


def test_mcp_pack_and_workspace_tools(tmp_path: Path, monkeypatch) -> None:
    repo = _make_repo(tmp_path / "repo")
    _write_pack_manifest(repo)
    run_dir = _write_run(repo)
    monkeypatch.chdir(repo)

    packs = json.loads(mcp_list_packs())
    assert packs["packs"][0]["pack_id"] == "pse2025"

    described = json.loads(mcp_describe_pack("pse2025"))
    assert described["family"] == "pse2025"

    workspace = json.loads(mcp_create_workspace_from_catalog("pse-base"))
    workspace_id = str(workspace["workspace_id"])
    cards = json.loads(mcp_list_workspace_cards(workspace_id))
    assert {item["card_id"] for item in cards["cards"]} == {"pse2025.intgadj", "pse2025.jg_constants"}

    bundle_workspace = json.loads(mcp_create_workspace_from_catalog("pse-bundle"))
    bundle_workspace_id = str(bundle_workspace["workspace_id"])
    bundle_cards = json.loads(mcp_list_workspace_cards(bundle_workspace_id, variant_id="base"))
    assert bundle_cards["scope"] == "variant"
    assert bundle_cards["variant_id"] == "base"

    updated_bundle = json.loads(
        mcp_update_bundle_variant(
            workspace_id=bundle_workspace_id,
            variant_id="base",
            label="Base Variant",
            scenario_name="pse2025_base_exact",
            input_file="psebase.txt",
            enabled=False,
        )
    )
    updated_variant = next(item for item in updated_bundle["variants"] if item["variant_id"] == "base")
    assert updated_variant["label"] == "Base Variant"
    assert updated_variant["scenario_name"] == "pse2025_base_exact"
    assert updated_variant["enabled"] is False

    cloned_bundle = json.loads(
        mcp_clone_bundle_variant_recipe(
            workspace_id=bundle_workspace_id,
            variant_id="recipe_variant",
            clone_from="base",
            label="Recipe Variant",
            scenario_name="recipe_variant_exact",
            input_file="psebase.txt",
            card_id="pse2025.jg_constants",
            constants_json=json.dumps({"JGW": 0.015}),
        )
    )
    cloned_variant = next(item for item in cloned_bundle["variants"] if item["variant_id"] == "recipe_variant")
    assert cloned_variant["scenario_name"] == "recipe_variant_exact"

    applied = json.loads(
        mcp_apply_workspace_card(
            workspace_id=workspace_id,
            card_id="pse2025.jg_constants",
            constants_json=json.dumps({"JGW": 0.03}),
        )
    )
    assert applied["recipe_history"][-1]["operation"] == "apply_workspace_card"

    views = json.loads(mcp_list_visualizations(workspace_id=workspace_id))
    assert any(item["view_id"] == "pse-main" for item in views["visualizations"])

    viz = json.loads(
        mcp_build_visualization_view(
            view_id="pse-main",
            workspace_id=workspace_id,
            run_dirs_json=json.dumps([str(run_dir)]),
        )
    )
    assert viz["runs"][0]["label"] == run_dir.name

    prompts = asyncio.run(mcp.list_prompts())
    prompt_names = {getattr(item, "name", "") for item in prompts}
    assert "Create a variant from base/high/low" in prompt_names
    assert "Prepare dashboard visualization set" in prompt_names
