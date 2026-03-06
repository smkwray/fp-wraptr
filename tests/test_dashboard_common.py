from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("streamlit")

from fp_wraptr.dashboard._common import discover_artifacts_roots, resolve_artifacts_dir_token


def test_discover_artifacts_roots_finds_repo_local_artifacts_dirs(tmp_path: Path) -> None:
    (tmp_path / "artifacts").mkdir()
    (tmp_path / "artifacts-2008").mkdir()
    (tmp_path / "artifacts-scratch").mkdir()
    (tmp_path / "other").mkdir()

    discovered = discover_artifacts_roots(repo_root=tmp_path)

    assert discovered == (
        (tmp_path / "artifacts").resolve(),
        (tmp_path / "artifacts-2008").resolve(),
        (tmp_path / "artifacts-scratch").resolve(),
    )


def test_discover_artifacts_roots_includes_current_custom_path(tmp_path: Path) -> None:
    (tmp_path / "artifacts").mkdir()
    custom = tmp_path / "runs-special"
    custom.mkdir()

    discovered = discover_artifacts_roots(repo_root=tmp_path, include=custom)

    assert custom.resolve() in discovered


def test_resolve_artifacts_dir_token_resolves_relative_to_repo_root(tmp_path: Path) -> None:
    resolved = resolve_artifacts_dir_token("artifacts-2008", repo_root=tmp_path)

    assert resolved == (tmp_path / "artifacts-2008").resolve()
