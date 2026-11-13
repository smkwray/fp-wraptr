"""The fp-r backend must find its R runtime both in a checkout and in an installed wheel."""

from __future__ import annotations

from pathlib import Path

from fp_wraptr.runtime.fpr import _default_fp_r_home


def _touch_runtime(root: Path) -> None:
    script = root / "fp-r" / "scripts" / "run_backend_bundle.R"
    script.parent.mkdir(parents=True)
    script.write_text("# runtime\n", encoding="utf-8")


def test_source_checkout_layout(tmp_path: Path) -> None:
    module = tmp_path / "repo" / "src" / "fp_wraptr" / "runtime" / "fpr.py"
    module.parent.mkdir(parents=True)
    _touch_runtime(tmp_path / "repo")
    assert _default_fp_r_home(module) == tmp_path / "repo" / "fp-r"


def test_installed_wheel_layout(tmp_path: Path) -> None:
    # The wheel force-includes fp-r/ at the top of site-packages, beside fp_wraptr/.
    site = tmp_path / "lib" / "python3.12" / "site-packages"
    module = site / "fp_wraptr" / "runtime" / "fpr.py"
    module.parent.mkdir(parents=True)
    _touch_runtime(site)
    assert _default_fp_r_home(module) == site / "fp-r"
