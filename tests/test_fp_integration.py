"""Optional fp.exe integration tests.

These tests are marked ``requires_fp`` and are intended for environments where
real FP assets are available (fp.exe + FM data files). They are skipped by
default in normal local/CI runs unless explicitly selected.
"""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
from pathlib import Path

import pytest

from fp_wraptr.io.parser import parse_fp_output
from fp_wraptr.runtime.fp_exe import FPExecutable
from fp_wraptr.scenarios.config import ScenarioConfig
from fp_wraptr.scenarios.runner import run_scenario

pytestmark = pytest.mark.requires_fp


def _require_fp_home() -> Path:
    fp_home = Path(os.environ.get("FP_HOME", "FM")).resolve()
    required = ["fp.exe", "fminput.txt", "fmdata.txt", "fmage.txt", "fmexog.txt"]
    missing = [name for name in required if not (fp_home / name).exists()]
    if missing:
        pytest.skip(f"FP assets missing under {fp_home}: {', '.join(missing)}")

    if platform.system() != "Windows" and shutil.which("wine") is None:
        pytest.skip("Wine is required for fp.exe integration tests on non-Windows hosts")

    if platform.system() != "Windows":
        result = subprocess.run(
            ["wine", "cmd", "/c", "echo", "hi"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0 or "Operation not permitted" in (result.stderr or ""):
            pytest.skip("Wine cannot start in this environment")

    return fp_home


def test_fp_executable_run_smoke(tmp_path: Path) -> None:
    """Run fp.exe directly and verify a parseable fmout is produced."""
    fp_home = _require_fp_home()

    exe = FPExecutable(fp_home=fp_home, timeout_seconds=600)
    result = exe.run(input_file=fp_home / "fminput.txt", work_dir=tmp_path / "work")

    assert result.return_code == 0, result.stderr
    assert result.output_file is not None
    assert result.output_file.exists()
    assert result.output_file.stat().st_size > 0

    parsed = parse_fp_output(result.output_file)
    assert parsed.variables


def test_run_scenario_with_real_fp(tmp_path: Path) -> None:
    """Run full scenario pipeline against real fp.exe assets."""
    fp_home = _require_fp_home()

    config = ScenarioConfig(name="fp_integration", fp_home=fp_home)
    result = run_scenario(config=config, output_dir=tmp_path / "artifacts")

    assert result.run_result is not None
    assert result.run_result.success is True
    assert result.parsed_output is not None
    assert result.parsed_output.variables
