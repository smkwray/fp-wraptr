from __future__ import annotations

from pathlib import Path

from fp_wraptr.dashboard.authoring_workspace import (
    compile_workspace,
    create_workspace_from_scenario_config,
    load_workspace,
    save_workspace,
)
from fp_wraptr.scenarios.config import ScenarioConfig


def test_workspace_save_load_and_compile_generates_overlay(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    fp_home = repo_root / "FM"
    fp_home.mkdir()
    (fp_home / "fminput.txt").write_text("INPUT FILE=ptcoef.txt;\n", encoding="utf-8")
    (fp_home / "ptcoef.txt").write_text("CREATE JPTUR = -0.35;\n", encoding="utf-8")

    config = ScenarioConfig(
        name="pse_base",
        fp_home=fp_home,
        input_file="fminput.txt",
        input_overlay_dir=None,
    )
    workspace = create_workspace_from_scenario_config(
        workspace_id="pse-test",
        label="PSE Test",
        config=config,
        source_yaml_path=repo_root / "examples" / "pse_base.yaml",
        source_catalog_id="pse_base",
    ).model_copy(update={"card_values": {"deck.ptcoef": {"JPTUR": -0.5}}})

    path = save_workspace(workspace, repo_root=repo_root)
    loaded = load_workspace(path)
    compiled = compile_workspace(loaded, repo_root=repo_root)

    assert compiled.generated_overlay_dir is not None
    generated_ptcoef = compiled.generated_overlay_dir / "ptcoef.txt"
    assert generated_ptcoef.exists()
    assert "CREATE JPTUR = -0.5;" in generated_ptcoef.read_text(encoding="utf-8")
    assert compiled.config.input_overlay_dir == compiled.generated_overlay_dir
