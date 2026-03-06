from __future__ import annotations

from pathlib import Path

import pytest

from fp_wraptr.dashboard.ptcoef_tools import load_ptcoef_deck, parse_ptcoef_text, write_ptcoef_overlay


def test_parse_ptcoef_text_extracts_sections_and_entries() -> None:
    text = "\n".join([
        "@ JGPTJ1 update",
        "CREATE JPTUR=0.438306;",
        "CREATE JPTYD=-0.004794;",
        "",
        "@ Unemployment-share state updates:",
        "CREATE JULUR=0.856919;",
        "",
    ])

    entries = parse_ptcoef_text(text)

    assert [entry.name for entry in entries] == ["JPTUR", "JPTYD", "JULUR"]
    assert entries[0].section == "JGPTJ1 update"
    assert entries[-1].section == "Unemployment-share state updates:"


def test_load_ptcoef_deck_prefers_overlay_copy(tmp_path: Path) -> None:
    fp_home = tmp_path / "FM"
    overlay_dir = tmp_path / "overlay"
    fp_home.mkdir()
    overlay_dir.mkdir()
    (fp_home / "ptcoef.txt").write_text("CREATE JPTUR=0.1;\n", encoding="utf-8")
    (overlay_dir / "ptcoef.txt").write_text("CREATE JPTUR=0.2;\n", encoding="utf-8")

    deck = load_ptcoef_deck(overlay_dir=overlay_dir, fp_home=fp_home)

    assert deck.source_kind == "overlay"
    assert deck.source_path == (overlay_dir / "ptcoef.txt").resolve()
    assert deck.entries[0].value == pytest.approx(0.2)


def test_write_ptcoef_overlay_creates_overlay_copy_and_backup(tmp_path: Path) -> None:
    fp_home = tmp_path / "FM"
    overlay_dir = tmp_path / "overlay"
    fp_home.mkdir()
    overlay_dir.mkdir()
    (fp_home / "ptcoef.txt").write_text(
        "\n".join([
            "@ JGPTJ1 update",
            "CREATE JPTUR=0.438306;",
            "CREATE JPTYD=-0.004794;",
            "",
        ]),
        encoding="utf-8",
    )

    deck = load_ptcoef_deck(overlay_dir=overlay_dir, fp_home=fp_home)
    first = write_ptcoef_overlay(deck, {"JPTUR": 0.5, "JPTYD": -0.004794})

    target = overlay_dir / "ptcoef.txt"
    assert first.created_overlay_copy is True
    assert first.backup_path is None
    assert first.changed_names == ("JPTUR",)
    assert "CREATE JPTUR=0.5;" in target.read_text(encoding="utf-8")

    updated_deck = load_ptcoef_deck(overlay_dir=overlay_dir, fp_home=fp_home)
    second = write_ptcoef_overlay(updated_deck, {"JPTUR": 0.6})

    assert second.created_overlay_copy is False
    assert second.backup_path is not None
    assert second.backup_path.exists()
    assert "CREATE JPTUR=0.5;" in second.backup_path.read_text(encoding="utf-8")
    assert "CREATE JPTUR=0.6;" in target.read_text(encoding="utf-8")
