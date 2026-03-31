"""fp-r backend.

Thin subprocess wrapper around the ignored `fp-r/` R prototype. This is
intentionally narrow: it executes a prebuilt bundle via `Rscript`, writes a
PABEV-style output, and returns a backend-agnostic `RunResult`.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

from fp_wraptr.runtime.backend import BackendInfo, RunResult


class FpRBackendError(Exception):
    """Raised when the fp-r backend cannot execute successfully."""


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _select_primary_output(work_dir: Path) -> Path | None:
    for candidate in ("PABEV.TXT", "PACEV.TXT"):
        path = work_dir / candidate
        if path.exists():
            return path
    return None


@dataclass
class FpRBackend:
    """Bundle-oriented backend for the ignored fp-r prototype."""

    bundle_path: Path | None = None
    fp_r_home: Path = field(default_factory=lambda: _repo_root() / "fp-r")
    rscript_path: Path | None = None
    timeout_seconds: int = 120

    def _resolve_bundle_path(self) -> Path | None:
        bundle = self.bundle_path
        if bundle is None:
            return None
        return Path(bundle).expanduser().resolve()

    def _resolve_runner_script(self) -> Path:
        script = self.fp_r_home / "scripts" / "run_backend_bundle.R"
        return script.resolve()

    def _resolve_rscript_path(self) -> Path | None:
        explicit = self.rscript_path
        if explicit is not None:
            candidate = Path(explicit).expanduser().resolve()
            return candidate if candidate.exists() else None

        from_path = shutil.which("Rscript")
        if from_path:
            candidate = Path(from_path).expanduser().resolve()
            if candidate.exists():
                return candidate

        user_local_root = Path.home() / "AppData" / "Local" / "Programs" / "R"
        globs = [
            "R-*\\{app}\\bin\\x64\\Rscript.exe",
            "R-*\\bin\\x64\\Rscript.exe",
            "R-*\\bin\\Rscript.exe",
        ]
        for pattern in globs:
            matches = sorted(user_local_root.glob(pattern), reverse=True)
            for match in matches:
                if match.exists():
                    return match.resolve()
        return None

    def check_available(self) -> bool:
        return (
            self._resolve_bundle_path() is not None
            and self._resolve_bundle_path().exists()
            and self._resolve_runner_script().exists()
            and self._resolve_rscript_path() is not None
        )

    def info(self) -> BackendInfo:
        rscript = self._resolve_rscript_path()
        bundle = self._resolve_bundle_path()
        return BackendInfo(
            name="fp-r",
            available=self.check_available(),
            details={
                "fp_r_home": str(self.fp_r_home),
                "bundle_path": str(bundle) if bundle is not None else "",
                "rscript_path": str(rscript) if rscript is not None else "",
            },
        )

    def run(
        self,
        input_file: Path | None = None,
        work_dir: Path | None = None,
        extra_env: dict[str, str] | None = None,
    ) -> RunResult:
        if work_dir is None:
            raise FpRBackendError("fp-r backend requires a working directory")
        work_dir = Path(work_dir).resolve()
        work_dir.mkdir(parents=True, exist_ok=True)
        bundle_path = self._resolve_bundle_path()
        if bundle_path is None or not bundle_path.exists():
            raise FpRBackendError("fp-r backend requires an existing bundle_path")
        runner_script = self._resolve_runner_script()
        if not runner_script.exists():
            raise FpRBackendError(f"Missing fp-r runner script: {runner_script}")
        rscript = self._resolve_rscript_path()
        if rscript is None:
            raise FpRBackendError("Rscript is not available for fp-r backend")

        command = [
            str(rscript),
            str(runner_script),
            "--bundle",
            str(bundle_path),
            "--work-dir",
            str(work_dir),
        ]
        env = os.environ.copy()
        if extra_env:
            env.update({str(k): str(v) for k, v in extra_env.items()})

        runtime_payload = {
            "backend": "fp-r",
            "bundle_path": str(bundle_path),
            "runner_script": str(runner_script),
            "rscript_path": str(rscript),
            "input_file": str(Path(input_file).resolve()) if input_file is not None else None,
            "work_dir": str(work_dir),
            "command": command,
        }
        runtime_path = work_dir / "fp_r.runtime.json"
        runtime_path.write_text(json.dumps(runtime_payload, indent=2) + "\n", encoding="utf-8")

        start = time.perf_counter()
        completed = subprocess.run(
            command,
            cwd=str(work_dir),
            env=env,
            capture_output=True,
            text=True,
            timeout=self.timeout_seconds,
            check=False,
        )
        duration = time.perf_counter() - start

        output_file = _select_primary_output(work_dir)
        if int(completed.returncode) == 0:
            missing: list[str] = []
            if output_file is None:
                missing.append("PABEV.TXT or PACEV.TXT")
            if not (work_dir / "fp_r_series.csv").exists():
                missing.append("fp_r_series.csv")
            if not (work_dir / "fp_r_report.txt").exists():
                missing.append("fp_r_report.txt")
            if missing:
                raise FpRBackendError(
                    "fp-r backend completed without required artifacts: "
                    + ", ".join(missing)
                )

        return RunResult(
            return_code=int(completed.returncode),
            stdout=str(completed.stdout or ""),
            stderr=str(completed.stderr or ""),
            working_dir=work_dir,
            input_file=(Path(input_file).resolve() if input_file is not None else bundle_path),
            output_file=output_file,
            duration_seconds=float(duration),
        )
