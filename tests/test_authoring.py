from __future__ import annotations

from pathlib import Path

import yaml

from fp_wraptr.data.series_pipeline.fp_targets import write_fmexog_override
from fp_wraptr.scenarios.authoring import (
    AttachRule,
    CardInstance,
    DraftSourceRef,
    apply_attach_rule,
    compile_bundle_workspace,
    compile_scenario_workspace,
    create_bundle_draft_from_source,
    create_scenario_draft_from_source,
    list_workspaces,
    load_card_specs,
    save_workspace_draft,
    workspace_dir,
)


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _make_repo(root: Path) -> Path:
    _write(root / "pyproject.toml", "[project]\nname='authoring-test'\nversion='0.0.0'\n")
    _write(
        root / "FM" / "fmexog.txt",
        "SMPL 2025.4 2026.1;\nCHANGEVAR;\nUR SAMEVALUE\n4.0\n;\nRETURN;\n",
    )
    overlay = root / "projects_local" / "pse2025"
    _write(
        overlay / "psebase.txt",
        "INPUT FILE=pse_common.txt;\nINPUT FILE=intgadj.txt;\nRETURN;\n",
    )
    _write(
        overlay / "pselow.txt",
        "INPUT FILE=pse_common.txt;\nINPUT FILE=intgadj.txt;\nRETURN;\n",
    )
    _write(
        overlay / "psehigh.txt",
        "INPUT FILE=pse_common.txt;\nINPUT FILE=intgadj.txt;\nRETURN;\n",
    )
    _write(
        overlay / "pse_common.txt",
        (
            "CREATE JGW=0.02;\n"
            "CREATE JGCOLA=0.02;\n"
            "CREATE MINWAGE=0.01;\n"
            "CREATE JGSWITCH=0;\n"
            "INPUT FILE=ptcoef.txt;\n"
            "INPUT FILE=fmexog.txt;\n"
            "RETURN;\n"
        ),
    )
    _write(
        overlay / "ptcoef.txt",
        "CREATE JPTUR=0.10;\nCREATE JPTYD=-0.20;\n",
    )
    _write(
        overlay / "intgadj.txt",
        "SMPL 2025.4 2026.1;\nCHANGEVAR;\nINTGADJ SAMEVALUE\n0\n0\n;\nRETURN;\n",
    )
    _write(
        root / "examples" / "pse2025_base.yaml",
        "\n".join([
            "name: pse2025_base",
            "description: Base",
            "fp_home: ../FM",
            "input_overlay_dir: ../projects_local/pse2025",
            "input_file: psebase.txt",
            'forecast_start: "2025.4"',
            'forecast_end: "2026.1"',
            "track_variables: [INTGADJ, JGW]",
            "",
        ]),
    )
    _write(
        root / "bundles" / "pse2025.yaml",
        "\n".join([
            "base:",
            "  name: pse2025",
            "  description: PSE bundle",
            "  fp_home: FM",
            "  input_overlay_dir: projects_local/pse2025",
            "  input_file: psebase.txt",
            '  forecast_start: "2025.4"',
            '  forecast_end: "2026.1"',
            "  track_variables: [INTGADJ, JGW]",
            "variants:",
            "  - name: base",
            "    patch:",
            "      input_file: psebase.txt",
            "  - name: low",
            "    patch:",
            "      input_file: pselow.txt",
            "  - name: high",
            "    patch:",
            "      input_file: psehigh.txt",
            "",
        ]),
    )
    _write(
        root / "projects_local" / "scenario_catalog.yaml",
        "\n".join([
            "entries:",
            "  - id: pse-base",
            "    label: PSE base",
            "    kind: scenario",
            "    family: pse2025",
            "    path: examples/pse2025_base.yaml",
            "    surfaces: [new_run]",
            "    public: true",
            "  - id: pse-bundle",
            "    label: PSE bundle",
            "    kind: bundle",
            "    family: pse2025",
            "    path: bundles/pse2025.yaml",
            "    surfaces: [new_run]",
            "    public: true",
            "",
        ]),
    )
    _write(
        root / "projects_local" / "cards" / "pse2025" / "jg_constants.yaml",
        "\n".join([
            "card_id: pse2025.jg_constants",
            "family: pse2025",
            "kind: deck_constants",
            "label: JG constants",
            "files:",
            "  - path: ptcoef.txt",
            "    groups:",
            "      - group_id: a",
            "        label: Takeup",
            "        fields:",
            "          - symbol: JPTUR",
            "            label: UR",
            "          - symbol: JPTYD",
            "            label: Y",
            "  - path: pse_common.txt",
            "    groups:",
            "      - group_id: b",
            "        label: Core",
            "        fields:",
            "          - symbol: JGW",
            "            label: Wage",
            "          - symbol: JGCOLA",
            "            label: Cola",
            "          - symbol: MINWAGE",
            "            label: Min wage",
            "",
        ]),
    )
    _write(
        root / "projects_local" / "cards" / "pse2025" / "intgadj.yaml",
        "\n".join([
            "card_id: pse2025.intgadj",
            "family: pse2025",
            "kind: series_card",
            "label: INTGADJ",
            "variable: INTGADJ",
            "input_modes: [csv, paste]",
            "default_target: include_changevar",
            "targets:",
            "  - kind: include_changevar",
            "    output_path: intgadj.txt",
            "    fp_method: SAMEVALUE",
            "    mode: series",
            "    attach_rule:",
            "      kind: overlay_file",
            "      relative_path: intgadj.txt",
            "  - kind: fmexog_override",
            "    output_path: fmexog.txt",
            "    fp_method: SAMEVALUE",
            "    mode: series",
            "    attach_rule:",
            "      kind: overlay_file",
            "      relative_path: fmexog.txt",
            "",
        ]),
    )
    return root


def test_compile_scenario_workspace_updates_constants_and_series(tmp_path: Path) -> None:
    repo = _make_repo(tmp_path / "repo")
    draft = create_scenario_draft_from_source(
        DraftSourceRef(kind="catalog", value="pse-base"),
        repo_root=repo,
    )
    draft.cards = [
        CardInstance(
            card_id="pse2025.jg_constants",
            enabled=True,
            constants={"JPTUR": 0.55, "JGW": 0.03},
        ),
        CardInstance(
            card_id="pse2025.intgadj",
            enabled=True,
            selected_target="include_changevar",
            input_mode="paste",
            pasted_text="period,value\n2025.4,1.5\n2026.1,2.5\n",
            series_points={"2025.4": 1.5, "2026.1": 2.5},
        ),
    ]
    save_workspace_draft(draft, repo_root=repo)
    result = compile_scenario_workspace(
        draft,
        repo_root=repo,
        workspace_dir=workspace_dir(repo, family=draft.family, slug=draft.slug),
    )

    assert result.ok is True
    assert result.scenario_config is not None
    assert result.scenario_config.input_file.startswith("au")
    overlay = result.overlay_dir
    assert "CREATE JPTUR=0.55;" in (overlay / "ptcoef.txt").read_text(encoding="utf-8")
    assert "CREATE JGW=0.03;" in (overlay / "pse_common.txt").read_text(encoding="utf-8")
    intgadj_text = (overlay / "intgadj.txt").read_text(encoding="utf-8")
    assert "CHANGEVAR;" in intgadj_text
    assert "1.5" in intgadj_text
    assert "2.5" in intgadj_text
    listed = list_workspaces(repo)
    assert any(item.slug == draft.slug for item in listed)


def test_compile_bundle_workspace_emits_bundle_and_variant_outputs(tmp_path: Path) -> None:
    repo = _make_repo(tmp_path / "repo")
    draft = create_bundle_draft_from_source(
        DraftSourceRef(kind="catalog", value="pse-bundle"),
        repo_root=repo,
    )
    draft.cards = [
        CardInstance(
            card_id="pse2025.jg_constants",
            enabled=True,
            constants={"MINWAGE": 0.012},
        )
    ]
    draft.variants.append(
        draft.variants[0].model_copy(
            update={
                "variant_id": "high_15h",
                "label": "High 15h",
                "scenario_name": "high_15h",
                "cards": [
                    CardInstance(
                        card_id="pse2025.jg_constants",
                        enabled=True,
                        constants={"JGW": 0.015, "JGCOLA": 0.0},
                    )
                ],
            },
            deep=True,
        )
    )
    save_workspace_draft(draft, repo_root=repo)
    result = compile_bundle_workspace(
        draft,
        repo_root=repo,
        workspace_dir=workspace_dir(repo, family=draft.family, slug=draft.slug),
    )

    assert result.ok is True
    assert result.bundle_config is not None
    assert result.compiled_path.exists()
    payload = yaml.safe_load(result.compiled_path.read_text(encoding="utf-8"))
    assert payload["base"]["name"] == draft.bundle_name
    assert len(payload["variants"]) == 4
    assert payload["variants"][-1]["scenario_name"] == "high_15h"
    assert (
        result.workspace_dir / "compiled" / "variants" / "base" / "compiled" / "scenario.yaml"
    ).exists()
    custom_variant = next(item for item in draft.variants if item.variant_id == "high_15h")
    custom_compiled = result.workspace_dir / "compiled" / "variants" / "high_15h" / "compiled"
    compiled_scenario = yaml.safe_load(
        (custom_compiled / "scenario.yaml").read_text(encoding="utf-8")
    )
    assert compiled_scenario["name"] == custom_variant.scenario_name
    custom_overlay = (
        result.workspace_dir / "compiled" / "variants" / "high_15h" / "overlay" / "pse_common.txt"
    )
    custom_text = custom_overlay.read_text(encoding="utf-8")
    assert "CREATE MINWAGE=0.012;" in custom_text
    assert "CREATE JGW=0.015;" in custom_text
    assert "CREATE JGCOLA=0;" in custom_text


def test_apply_attach_rule_and_write_fmexog_override_support_series(tmp_path: Path) -> None:
    overlay = tmp_path / "overlay"
    overlay.mkdir()
    target = overlay / "entry.txt"
    target.write_text("INPUT FILE=old.txt;\nRETURN;\n", encoding="utf-8")
    generated = overlay / "generated.txt"
    generated.write_text("RETURN;\n", encoding="utf-8")

    apply_attach_rule(
        overlay_dir=overlay,
        generated_path=generated,
        rule=AttachRule(kind="replace_include", target_file="entry.txt"),
    )
    assert "generated.txt" in target.read_text(encoding="utf-8")

    base = tmp_path / "fmexog.txt"
    base.write_text(
        "SMPL 2025.4 2026.1;\nCHANGEVAR;\nUR SAMEVALUE\n4\n;\nRETURN;\n", encoding="utf-8"
    )
    out = tmp_path / "fmexog_override.txt"
    write_fmexog_override(
        out_path=out,
        variable="INTGADJ",
        fp_method="SAMEVALUE",
        smpl_start="2025.4",
        smpl_end="2026.1",
        values=[1.0, 2.0],
        base_fmexog=base,
        layer_on_base=True,
    )
    text = out.read_text(encoding="utf-8")
    assert "SMPL 2025.4 2025.4;" in text
    assert "SMPL 2026.1 2026.1;" in text
    assert "INTGADJ SAMEVALUE" in text


def test_load_card_specs_skips_legacy_yaml_without_current_schema(tmp_path: Path) -> None:
    repo = _make_repo(tmp_path / "repo")
    legacy = repo / "projects_local" / "cards" / "pse2025" / "legacy.yaml"
    _write(
        legacy,
        "\n".join([
            "card_id: legacy.ptcoef",
            "label: Legacy",
            "capability: deck_table",
            "",
        ]),
    )

    specs = load_card_specs(repo_root=repo, family="pse2025")

    assert [spec.card_id for spec in specs] == ["pse2025.intgadj", "pse2025.jg_constants"]


def test_compile_scenario_workspace_layers_multiple_fmexog_cards(tmp_path: Path) -> None:
    repo = _make_repo(tmp_path / "repo")
    cards_dir = repo / "projects_local" / "cards" / "pse2025"
    _write(
        cards_dir / "jgswitch.yaml",
        "\n".join([
            "card_id: pse2025.jgswitch",
            "family: pse2025",
            "kind: series_card",
            "label: JGSWITCH",
            "variable: JGSWITCH",
            "default_target: fmexog_override",
            "targets:",
            "  - kind: fmexog_override",
            "    output_path: fmexog.txt",
            "    fp_method: SAMEVALUE",
            "    mode: series",
            "    attach_rule:",
            "      kind: overlay_file",
            "      relative_path: fmexog.txt",
            "",
        ]),
    )
    _write(
        cards_dir / "jgphase.yaml",
        "\n".join([
            "card_id: pse2025.jgphase",
            "family: pse2025",
            "kind: series_card",
            "label: JGPHASE",
            "variable: JGPHASE",
            "default_target: fmexog_override",
            "targets:",
            "  - kind: fmexog_override",
            "    output_path: fmexog.txt",
            "    fp_method: ADDDIFABS",
            "    mode: series",
            "    attach_rule:",
            "      kind: overlay_file",
            "      relative_path: fmexog.txt",
            "",
        ]),
    )

    draft = create_scenario_draft_from_source(
        DraftSourceRef(kind="catalog", value="pse-base"),
        repo_root=repo,
    )
    draft.cards = [
        CardInstance(
            card_id="pse2025.jgswitch",
            enabled=True,
            selected_target="fmexog_override",
            input_mode="paste",
            pasted_text="period,value\n2025.4,1\n2026.1,1\n",
            series_points={"2025.4": 1.0, "2026.1": 1.0},
        ),
        CardInstance(
            card_id="pse2025.jgphase",
            enabled=True,
            selected_target="fmexog_override",
            input_mode="paste",
            pasted_text="period,value\n2025.4,0.2\n2026.1,0.4\n",
            series_points={"2025.4": 0.2, "2026.1": 0.4},
        ),
    ]

    result = compile_scenario_workspace(
        draft,
        repo_root=repo,
        workspace_dir=workspace_dir(repo, family=draft.family, slug=f"{draft.slug}-policy"),
    )

    assert result.ok is True
    fmexog_text = (result.overlay_dir / "fmexog.txt").read_text(encoding="utf-8")
    assert "JGSWITCH SAMEVALUE" in fmexog_text
    assert "JGPHASE ADDDIFABS" in fmexog_text
