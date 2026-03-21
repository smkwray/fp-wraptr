from __future__ import annotations

from pathlib import Path

from fp_wraptr.dashboard.scenario_tools import build_tweaked_config, preflight_scenario_input
from fp_wraptr.scenarios.config import ScenarioConfig, VariableOverride


def test_preflight_scenario_input_collects_overlay_backed_manifest(tmp_path: Path) -> None:
    fp_home = tmp_path / "FM"
    fp_home.mkdir()
    overlay_dir = tmp_path / "overlay"
    overlay_dir.mkdir()

    (fp_home / "fminput.txt").write_text(
        "\n".join([
            "INPUT FILE=pse_common.txt;",
            "PRINTVAR FILEOUT=PABEV.TXT PCY;",
            "",
        ]),
        encoding="utf-8",
    )
    (overlay_dir / "pse_common.txt").write_text(
        "\n".join([
            "INPUT FILE=ptcoef.txt;",
            "",
        ]),
        encoding="utf-8",
    )
    (overlay_dir / "ptcoef.txt").write_text("JPTUR = -0.35;\n", encoding="utf-8")

    config = ScenarioConfig(
        name="pse_preview",
        fp_home=fp_home,
        input_overlay_dir=overlay_dir,
    )

    preflight = preflight_scenario_input(config)

    assert preflight.ok is True
    assert preflight.entry_source_kind == "fp_home"
    assert preflight.entry_source_path == fp_home / "fminput.txt"
    assert preflight.include_files == ("pse_common.txt", "ptcoef.txt")
    assert preflight.expected_output_files == ("PABEV.TXT",)


def test_preflight_scenario_input_reports_missing_entry_file(tmp_path: Path) -> None:
    fp_home = tmp_path / "FM"
    fp_home.mkdir()

    config = ScenarioConfig(
        name="missing_entry",
        fp_home=fp_home,
        input_file="does_not_exist.txt",
    )

    preflight = preflight_scenario_input(config)

    assert preflight.ok is False
    assert preflight.entry_source_path is None
    assert preflight.error is not None
    assert "does_not_exist.txt" in preflight.error


def test_build_tweaked_config_preserves_overlay_and_patches(tmp_path: Path) -> None:
    fp_home = tmp_path / "FM"
    fp_home.mkdir()
    overlay_dir = tmp_path / "overlay"
    overlay_dir.mkdir()

    base = ScenarioConfig(
        name="base",
        description="baseline",
        fp_home=fp_home,
        input_overlay_dir=overlay_dir,
        input_file="psebase.txt",
        backend="fpexe",
        fppy={"timeout_seconds": 2400},
        overrides={"UR": VariableOverride(method="CHGSAMEABS", value=0.2)},
        input_patches={"old": "new"},
        track_variables=["PCY", "UR"],
        alerts={"UR": {"max": 6.0}},
        extra={"family": "pse2025"},
    )

    tweaked = build_tweaked_config(
        base_config=base,
        new_name="base_tweaked",
        description="Tweaked from base",
        backend="both",
        fppy_num_threads=8,
        overrides={"PCY": VariableOverride(method="CHGSAMEPCT", value=0.5)},
    )

    assert tweaked.name == "base_tweaked"
    assert tweaked.description == "Tweaked from base"
    assert tweaked.backend == "both"
    assert tweaked.input_overlay_dir == overlay_dir
    assert tweaked.input_file == "psebase.txt"
    assert tweaked.input_patches == {"old": "new"}
    assert tweaked.alerts == {"UR": {"max": 6.0}}
    assert tweaked.extra == {"family": "pse2025"}
    assert tweaked.fppy.timeout_seconds == 2400
    assert tweaked.fppy.num_threads == 8
    assert set(tweaked.overrides) == {"PCY"}
