from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from fp_wraptr.dashboard.ptcoef_editor import (
    parse_ptcoef_text,
    resolve_ptcoef_overlay_path,
    rewrite_ptcoef_text,
    write_ptcoef_overlay_text,
)


def test_parse_ptcoef_text_collects_create_entries() -> None:
    text = "\n".join([
        "@ heading",
        "CREATE JPTUR=0.438306;",
        "  CREATE jchur = -1.1706049 ;",
        "GENR X=1;",
        "",
    ])
    doc = parse_ptcoef_text(text)

    assert [entry.symbol for entry in doc.entries] == ["JPTUR", "JCHUR"]
    assert doc.entries[0].value_text == "0.438306"
    assert doc.entries[1].value == pytest.approx(-1.1706049)


def test_rewrite_ptcoef_text_updates_symbols_and_preserves_comments() -> None:
    original = "\n".join([
        "@ section",
        "CREATE JPTUR=0.438306;",
        "CREATE JPTYD=-0.004794;",
        "@ leave me",
        "",
    ])

    rewritten, missing = rewrite_ptcoef_text(original, {"jptur": "0.5", "missing": 1.2})

    assert "@ section" in rewritten
    assert "@ leave me" in rewritten
    assert "CREATE JPTUR=0.5;" in rewritten
    assert "CREATE JPTYD=-0.004794;" in rewritten
    assert missing == ("MISSING",)


def test_resolve_ptcoef_overlay_path_blocks_escape(tmp_path: Path) -> None:
    overlay = tmp_path / "overlay"
    overlay.mkdir()

    with pytest.raises(ValueError, match="outside overlay dir"):
        resolve_ptcoef_overlay_path(overlay_dir=overlay, relative_path="../ptcoef.txt")


def test_write_ptcoef_overlay_text_creates_backup(tmp_path: Path) -> None:
    overlay = tmp_path / "overlay"
    overlay.mkdir()
    target = overlay / "ptcoef.txt"
    target.write_text("CREATE JPTUR=0.1;\n", encoding="utf-8")

    result = write_ptcoef_overlay_text(
        overlay_dir=overlay,
        text="CREATE JPTUR=0.2;\n",
        backup_timestamp=datetime(2026, 3, 5, 12, 34, 56, tzinfo=UTC),
    )

    assert result.target_path == target.resolve()
    assert result.backup_path is not None
    assert result.backup_path.name == "ptcoef.txt.bak.20260305_123456"
    assert result.backup_path.read_text(encoding="utf-8") == "CREATE JPTUR=0.1;\n"
    assert target.read_text(encoding="utf-8") == "CREATE JPTUR=0.2;\n"


def test_write_ptcoef_overlay_text_rejects_non_ptcoef_filename(tmp_path: Path) -> None:
    overlay = tmp_path / "overlay"
    overlay.mkdir()

    with pytest.raises(ValueError, match="ptcoef editor"):
        write_ptcoef_overlay_text(
            overlay_dir=overlay,
            text="CREATE JPTUR=0.2;\n",
            relative_path="coeffs.txt",
        )
