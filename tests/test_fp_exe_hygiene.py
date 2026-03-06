from __future__ import annotations

from pathlib import Path

import pytest

from fp_wraptr.runtime.fp_exe import FPExecutable, FPExecutableError


@pytest.mark.parametrize(
    "wineprefix",
    [
        ".wine",
        "./.wine",
        "relative/prefix",
    ],
)
def test_fp_exe_rejects_relative_wineprefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, wineprefix: str
) -> None:
    fp = FPExecutable(fp_home=tmp_path, use_wine=True)
    env = {"WINEPREFIX": wineprefix}
    with pytest.raises(FPExecutableError, match="relative WINEPREFIX"):
        fp._validate_wineprefix(env, work_dir=tmp_path)


def test_fp_exe_rejects_project_tree_wineprefix(tmp_path: Path) -> None:
    # Force project root to tmp_path for deterministic behavior.
    fp = FPExecutable(fp_home=tmp_path, use_wine=True)

    def _root() -> Path:
        return tmp_path

    object.__setattr__(fp, "_project_root", _root)  # type: ignore[misc]
    env = {"WINEPREFIX": str(tmp_path / ".wine")}
    with pytest.raises(FPExecutableError, match="inside the project tree"):
        fp._validate_wineprefix(env, work_dir=tmp_path)


def test_fp_exe_defaults_wineprefix_to_user_home(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fp = FPExecutable(fp_home=tmp_path, use_wine=True)
    env: dict[str, str] = {}
    fp._validate_wineprefix(env, work_dir=tmp_path)
    assert "WINEPREFIX" in env
    assert Path(env["WINEPREFIX"]).is_absolute()
    assert ".wine-fp-wraptr" in env["WINEPREFIX"]
