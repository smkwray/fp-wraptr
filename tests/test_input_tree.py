from __future__ import annotations

from pathlib import Path

import pytest

from fp_wraptr.scenarios.input_tree import prepare_work_dir_for_fp_run


def test_prepare_work_dir_for_fp_run_copies_transitive_includes(tmp_path: Path) -> None:
    fp_home = tmp_path / "FM"
    fp_home.mkdir()
    overlay = tmp_path / "overlay"
    overlay.mkdir()
    work_dir = tmp_path / "work"
    work_dir.mkdir()

    # Entry input is already staged into work_dir (runner responsibility).
    (work_dir / "psebase.txt").write_text(
        "\n".join([
            "INPUT FILE=pse_common.txt;",
            "PRINTVAR FILEOUT=OUT_PSEBASE.DAT GDPR UR;",
        ])
        + "\n",
        encoding="utf-8",
    )

    # Includes live in overlay_dir (PSE-style).
    (overlay / "pse_common.txt").write_text(
        "\n".join([
            "INPUT FILE=ptcoef.txt;",
            "PRINTVAR FILEOUT=OUT_PSECOMMON.DAT GDPR;",
        ])
        + "\n",
        encoding="utf-8",
    )
    (overlay / "ptcoef.txt").write_text(
        "PRINTVAR FILEOUT=OUT_PTCOEF.DAT UR;\n",
        encoding="utf-8",
    )

    manifest = prepare_work_dir_for_fp_run(
        entry_input=work_dir / "psebase.txt",
        work_dir=work_dir,
        overlay_dir=overlay,
        fp_home=fp_home,
    )

    assert (work_dir / "pse_common.txt").exists()
    assert (work_dir / "ptcoef.txt").exists()
    assert set(manifest.include_files) >= {"pse_common.txt", "ptcoef.txt"}
    assert set(manifest.expected_output_files) >= {
        "OUT_PSEBASE.DAT",
        "OUT_PSECOMMON.DAT",
        "OUT_PTCOEF.DAT",
    }


def test_prepare_work_dir_for_fp_run_errors_on_missing_includes(tmp_path: Path) -> None:
    fp_home = tmp_path / "FM"
    fp_home.mkdir()
    overlay = tmp_path / "overlay"
    overlay.mkdir()
    work_dir = tmp_path / "work"
    work_dir.mkdir()

    (work_dir / "psebase.txt").write_text("INPUT FILE=missing.txt;\n", encoding="utf-8")

    with pytest.raises(FileNotFoundError) as excinfo:
        prepare_work_dir_for_fp_run(
            entry_input=work_dir / "psebase.txt",
            work_dir=work_dir,
            overlay_dir=overlay,
            fp_home=fp_home,
        )

    message = str(excinfo.value)
    assert "missing.txt" in message
    assert "searched:" in message.lower()


def test_repo_pse2008_input_tree_copies_expected_family_includes(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    overlay = repo_root / "projects_local" / "pse2008"
    fp_home = tmp_path / "FM"
    fp_home.mkdir()
    for name in ("fmdata.txt", "fmage.txt", "fmexog.txt"):
        (fp_home / name).write_text(f"@ stub {name}\n", encoding="utf-8")

    work_dir = tmp_path / "work"
    work_dir.mkdir()
    (work_dir / "psebase.txt").write_text(
        (overlay / "psebase.txt").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    manifest = prepare_work_dir_for_fp_run(
        entry_input=work_dir / "psebase.txt",
        work_dir=work_dir,
        overlay_dir=overlay,
        fp_home=fp_home,
    )

    assert (work_dir / "pse_common.txt").exists()
    assert (work_dir / "ptcoef.txt").exists()
    assert (work_dir / "intgadj.txt").exists()
    assert set(manifest.include_files) >= {"pse_common.txt", "ptcoef.txt", "intgadj.txt"}
    assert set(manifest.load_data_files) >= {"fmage.txt", "fmdata.txt"}
    assert set(manifest.expected_output_files) >= {"OUT_PSEBASE.DAT"}
