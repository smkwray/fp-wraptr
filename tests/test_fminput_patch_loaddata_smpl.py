from __future__ import annotations

from pathlib import Path

from fp_wraptr.data.update_fred import _patch_fminput_smpl_endpoints


def test_patch_fminput_smpl_endpoints_patches_only_loaddata_smpl(tmp_path: Path) -> None:
    # Two SMPL blocks share the same endpoint, but only the SMPL immediately in effect
    # for LOADDATA should be rewritten.
    text = (
        "@ comment ignored\n"
        "SMPL 1952.1 2025.3;\n"
        "LOADDATA FILE=FMDATA.TXT;\n"
        "SMPL 1952.1 2025.3;\n"
        "GENR X=1;\n"
    )
    path = tmp_path / "fminput.txt"
    path.write_text(text, encoding="utf-8")

    payload = _patch_fminput_smpl_endpoints(fminput_path=path, old_end="2025.3", new_end="2025.4")
    assert payload["patched_count"] == 1
    assert payload["patched_lines_sample"][0]["line"] == 2

    out = path.read_text(encoding="utf-8")
    # Only the first SMPL (active at LOADDATA) is patched.
    assert out.splitlines()[1] == "SMPL 1952.1 2025.4;"
    assert out.splitlines()[3] == "SMPL 1952.1 2025.3;"


def test_patch_fminput_smpl_endpoints_noop_if_loaddata_smpl_end_mismatch(tmp_path: Path) -> None:
    text = "SMPL 1952.1 2025.2;\nLOADDATA FILE=FMDATA.TXT;\nSMPL 1952.1 2025.3;\n"
    path = tmp_path / "fminput.txt"
    path.write_text(text, encoding="utf-8")

    payload = _patch_fminput_smpl_endpoints(fminput_path=path, old_end="2025.3", new_end="2025.4")
    assert payload["patched_count"] == 0
