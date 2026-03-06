from __future__ import annotations

from pathlib import Path

from pydantic import TypeAdapter

from fp_wraptr.scenarios.authoring import load_card_specs, workspace_dir, workspace_root
from fp_wraptr.scenarios.authoring.models import BundleDraft, CardSpec


def test_models_parse_current_card_union_and_bundle() -> None:
    deck = TypeAdapter(CardSpec).validate_python({
        "kind": "deck_constants",
        "card_id": "pse2025.jg_constants",
        "family": "pse2025",
        "label": "JG Constants",
        "files": [
            {
                "path": "ptcoef.txt",
                "groups": [
                    {
                        "group_id": "takeup",
                        "label": "Take-up",
                        "fields": [{"symbol": "JPTUR", "label": "UR"}],
                    }
                ],
            }
        ],
    })
    assert deck.card_id == "pse2025.jg_constants"

    series = TypeAdapter(CardSpec).validate_python({
        "kind": "series_card",
        "card_id": "pse2025.intgadj",
        "family": "pse2025",
        "label": "INTGADJ",
        "variable": "INTGADJ",
        "default_target": "include_changevar",
        "targets": [
            {
                "kind": "include_changevar",
                "output_path": "intgadj.txt",
                "attach_rule": {"kind": "overlay_file", "relative_path": "intgadj.txt"},
            }
        ],
    })
    assert series.card_id == "pse2025.intgadj"

    bundle = BundleDraft.model_validate({
        "family": "pse2025",
        "slug": "pse-bundle",
        "label": "PSE Bundle",
        "source": {"kind": "catalog", "value": "pse2025-bundle"},
        "bundle_name": "pse2025_bundle",
        "forecast_start": "2025.4",
        "forecast_end": "2029.4",
        "variants": [{"variant_id": "low", "label": "Low", "input_file": "pselow.txt"}],
    })
    assert bundle.variants[0].variant_id == "low"


def test_workspace_root_and_loader_ignore_legacy_specs(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    cards_dir = repo / "projects_local" / "cards" / "pse2025"
    cards_dir.mkdir(parents=True)
    (cards_dir / "legacy.yaml").write_text(
        "card_id: legacy.ptcoef\nlabel: Legacy\ncapability: deck_table\n",
        encoding="utf-8",
    )
    (cards_dir / "jg_constants.yaml").write_text(
        "\n".join([
            "card_id: pse2025.jg_constants",
            "family: pse2025",
            "kind: deck_constants",
            "label: JG Constants",
            "files:",
            "  - path: ptcoef.txt",
            "    groups:",
            "      - group_id: takeup",
            "        label: Take-up",
            "        fields:",
            "          - symbol: JPTUR",
            "            label: UR",
            "",
        ]),
        encoding="utf-8",
    )

    assert workspace_root(repo) == repo / "projects_local" / "authoring"
    assert workspace_dir(repo, family="PSE 2025", slug="Base Draft") == (
        repo / "projects_local" / "authoring" / "pse-2025" / "base-draft"
    )

    specs = load_card_specs(repo_root=repo, family="pse2025")
    assert [spec.card_id for spec in specs] == ["pse2025.jg_constants"]
