"""fp-r bundle runner output must stay readable by fp-wraptr's LOADFORMAT parser."""

from __future__ import annotations

import math
import shutil
import subprocess
from pathlib import Path

import pytest

from fp_wraptr.io.loadformat import read_loadformat

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_bundle_runner_writes_missing_values_as_fp_sentinel(tmp_path: Path) -> None:
    """The public demo bundle has missing first-period values.

    fp.exe writes missing LOADFORMAT values as -99. The bundle runner used to
    write them as ``NA``, so every downstream reader (parity, diff, dashboard)
    raised ``ValueError`` on the demo run's PABEV.TXT / LOADFORMAT.DAT.
    """
    rscript = shutil.which("Rscript")
    if rscript is None:
        pytest.skip("Rscript not available")

    work_dir = tmp_path / "work"
    subprocess.run(
        [
            rscript,
            str(REPO_ROOT / "fp-r" / "scripts" / "run_backend_bundle.R"),
            "--bundle",
            str(REPO_ROOT / "examples" / "fpr_bundle_demo_bundle.R"),
            "--work-dir",
            str(work_dir),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    pabev = work_dir / "PABEV.TXT"
    assert "NA" not in pabev.read_text(encoding="utf-8").split()

    periods, series = read_loadformat(pabev)
    assert periods
    assert series["CONS"][0] == -99.0
    assert all(math.isfinite(value) for value in series["CONS"][1:])


def _run_bundle(rscript: str, bundle: Path, work_dir: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            rscript,
            str(REPO_ROOT / "fp-r" / "scripts" / "run_backend_bundle.R"),
            "--bundle",
            str(bundle),
            "--work-dir",
            str(work_dir),
        ],
        check=True,
        capture_output=True,
        text=True,
    )


def test_bundle_runner_reports_iteration_limit_non_convergence(tmp_path: Path) -> None:
    """A solve that stops at the iteration cap must not look like a clean run.

    fp.exe prints "Solution error. Iteration limit ... reached"; fp-r used to
    record non-convergence only in fp_r_diagnostics.csv, with nothing in the
    report or on stderr, so an unconverged solution was indistinguishable
    from a converged one.
    """
    rscript = shutil.which("Rscript")
    if rscript is None:
        pytest.skip("Rscript not available")

    demo = REPO_ROOT / "examples" / "fpr_bundle_demo_bundle.R"
    capped = tmp_path / "capped_bundle.R"
    capped.write_text(
        f'sys.source("{demo.as_posix()}", envir = environment())\n'
        "bundle$control <- utils::modifyList(\n"
        "  if (is.null(bundle$control)) list() else bundle$control,\n"
        "  list(max_iter = 1L, tolerance = 0)\n"
        ")\n",
        encoding="utf-8",
    )

    capped_run = _run_bundle(rscript, capped, tmp_path / "capped")
    report = (tmp_path / "capped" / "fp_r_report.txt").read_text(encoding="utf-8")
    line = next(item for item in report.splitlines() if item.startswith("unconverged_periods="))
    assert line != "unconverged_periods="
    assert "did not converge" in capped_run.stderr

    clean_run = _run_bundle(rscript, demo, tmp_path / "clean")
    clean_report = (tmp_path / "clean" / "fp_r_report.txt").read_text(encoding="utf-8")
    assert "unconverged_periods=" in clean_report.splitlines()
    assert "did not converge" not in clean_run.stderr
