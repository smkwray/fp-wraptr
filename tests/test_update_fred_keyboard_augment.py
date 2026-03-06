from __future__ import annotations

from pathlib import Path

from fp_wraptr.data.update_fred import _augment_fminput_keyboard_targets


def test_augment_fminput_keyboard_targets_appends_before_terminator(tmp_path: Path) -> None:
    fminput = tmp_path / "fminput.txt"
    # Use CRLF to match stock Fair templates.
    fminput.write_bytes(
        b"SMPL 2025.4 2029.4;\r\n"
        b"SOLVE DYNAMIC OUTSIDE FILEVAR=KEYBOARD NORESET;\r\n"
        b"PCY\r\n"
        b"PCPF\r\n"
        b";\r\n"
    )

    payload = _augment_fminput_keyboard_targets(
        fminput_path=fminput,
        extra_targets=("RM", "RMA", "PCY"),
    )
    assert payload["found"] is True
    assert payload["added"] == ["RM", "RMA"]
    assert "PCY" in payload["already_present"]

    out = fminput.read_bytes()
    assert b"PCY\r\nPCPF\r\nRM\r\nRMA\r\n;\r\n" in out
