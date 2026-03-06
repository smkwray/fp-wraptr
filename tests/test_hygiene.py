from __future__ import annotations

from pathlib import Path

import pytest

from fp_wraptr.hygiene import assert_no_forbidden_dirs, forbidden_dirs_present


def test_forbidden_dirs_present_detects(tmp_path: Path) -> None:
    (tmp_path / ".venv").mkdir()
    (tmp_path / ".wine").mkdir()
    (tmp_path / ".wine-fp-wraptr").mkdir()
    present = forbidden_dirs_present(tmp_path)
    assert tmp_path / ".venv" in present
    assert tmp_path / ".wine" in present
    assert tmp_path / ".wine-fp-wraptr" in present


def test_assert_no_forbidden_dirs_raises(tmp_path: Path) -> None:
    (tmp_path / ".venv").mkdir()
    with pytest.raises(RuntimeError, match="Forbidden repo-local"):
        assert_no_forbidden_dirs(tmp_path)
