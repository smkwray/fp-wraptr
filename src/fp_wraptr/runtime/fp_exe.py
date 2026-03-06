"""Subprocess wrapper for the Fair-Parke executable (fp.exe).

fp.exe is a Windows PE32 binary. On macOS/Linux it requires Wine.
The executable reads from stdin (via INPUT FILE= directive) and writes
to stdout. Key files it expects in its working directory:
  - fminput.txt (or specified input file)
  - fmdata.txt, fmage.txt, fmexog.txt (data files referenced by input)

Environment variable FP_HOME (or config) points to the directory
containing fp.exe and data files.
"""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fp_wraptr.runtime.backend import RunResult


class FPExecutableError(Exception):
    """Raised when fp.exe execution fails."""

    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.details: dict[str, Any] = dict(details or {})


@dataclass
class FPRunResult(RunResult):
    """Result of an fp.exe invocation."""


@dataclass
class FPExecutable:
    """Wrapper around the fp.exe subprocess.

    Usage:
        fp = FPExecutable(fp_home=Path("FM"))
        result = fp.run(input_file=Path("fminput.txt"), work_dir=Path("runs/test1"))
    """

    fp_home: Path = field(default_factory=lambda: Path(os.environ.get("FP_HOME", "FM")))
    exe_name: str = "fp.exe"
    timeout_seconds: int = 300
    use_wine: bool | None = None  # None = auto-detect
    required_data_files: tuple[str, ...] = ("fmdata.txt", "fmage.txt", "fmexog.txt")

    def __post_init__(self) -> None:
        if self.use_wine is None:
            self.use_wine = platform.system() != "Windows"

    def _project_root(self) -> Path | None:
        """Best-effort project root discovery (for hygiene guardrails).

        We use this to ensure WINEPREFIX is not set inside the repo, which can create
        `.wine/` in the project tree.
        """
        cur = Path(__file__).resolve()
        for parent in (cur, *tuple(cur.parents)):
            if (parent / "pyproject.toml").exists():
                return parent
        return None

    def _validate_wineprefix(self, env: dict[str, str], *, work_dir: Path) -> None:
        if not self.use_wine:
            return

        project_root = self._project_root()
        raw = str(env.get("WINEPREFIX") or "").strip()
        if not raw:
            # Default to a user-home prefix rather than letting Wine choose a cwd-relative prefix.
            raw = str(Path.home() / ".wine-fp-wraptr")
            env["WINEPREFIX"] = raw

        prefix = Path(raw).expanduser()
        if not prefix.is_absolute():
            raise FPExecutableError(
                "Refusing to run Wine with a relative WINEPREFIX (would create `.wine/` "
                f"under the current directory). Set WINEPREFIX to an absolute path outside the repo. "
                f"Got: {raw!r}"
            )

        # Resolve relative symlinks, etc.
        try:
            resolved = prefix.resolve()
        except Exception:
            resolved = prefix

        if project_root is not None:
            try:
                resolved.relative_to(project_root)
            except ValueError:
                pass
            else:
                raise FPExecutableError(
                    "Refusing to run Wine with WINEPREFIX inside the project tree. "
                    f"Got: {resolved}. Set WINEPREFIX to an absolute path outside the repo."
                )

        # Ensure the prefix directory exists (and is user-writable) before launching Wine.
        resolved.mkdir(parents=True, exist_ok=True)
        if not os.access(str(resolved), os.W_OK):
            raise FPExecutableError(f"WINEPREFIX not writable: {resolved}")

        env["WINEPREFIX"] = str(resolved)

    @property
    def exe_path(self) -> Path:
        return self.fp_home / self.exe_name

    def check_available(self) -> bool:
        """Check if fp.exe (and Wine if needed) are available."""
        if not self.exe_path.exists():
            return False
        return not (self.use_wine and not shutil.which("wine"))

    def preflight_report(
        self,
        input_file: Path | None = None,
        work_dir: Path | None = None,
    ) -> dict[str, Any]:
        """Return diagnostic details about fp.exe run prerequisites."""
        resolved_work_dir = work_dir or self.fp_home
        input_name = (input_file or self.fp_home / "fminput.txt").name
        expected_input_path = resolved_work_dir / input_name
        missing_data_files = self._missing_files(resolved_work_dir, self.required_data_files)
        wine_path = shutil.which("wine")
        exe_exists = self.exe_path.exists()
        wine_available = bool(wine_path)
        wine_required = bool(self.use_wine)
        raw_wineprefix = str(os.environ.get("WINEPREFIX") or "").strip()
        if raw_wineprefix:
            wineprefix_path = Path(raw_wineprefix).expanduser()
        else:
            wineprefix_path = Path.home() / ".wine-fp-wraptr"
        try:
            resolved_wineprefix = wineprefix_path.resolve()
        except Exception:
            resolved_wineprefix = wineprefix_path
        wineprefix_exists = resolved_wineprefix.exists()
        wineprefix_initialized = bool(
            wineprefix_exists
            and (resolved_wineprefix / "drive_c").exists()
            and (resolved_wineprefix / "system.reg").exists()
            and (resolved_wineprefix / "user.reg").exists()
        )

        available = (
            exe_exists
            and (not wine_required or wine_available)
            and expected_input_path.exists()
            and not missing_data_files
        )
        return {
            "available": available,
            "fp_home": str(self.fp_home),
            "work_dir": str(resolved_work_dir),
            "exe_path": str(self.exe_path),
            "exe_exists": exe_exists,
            "wine_required": wine_required,
            "wine_path": wine_path or "",
            "wine_available": wine_available,
            "wineprefix": str(resolved_wineprefix),
            "wineprefix_exists": bool(wineprefix_exists),
            "wineprefix_initialized": bool(wineprefix_initialized),
            "input_file_name": input_name,
            "expected_input_path": str(expected_input_path),
            "input_file_exists": expected_input_path.exists(),
            "required_data_files": list(self.required_data_files),
            "missing_data_files": missing_data_files,
        }

    def run(
        self,
        input_file: Path | None = None,
        work_dir: Path | None = None,
        extra_env: dict[str, str] | None = None,
    ) -> FPRunResult:
        """Run fp.exe with the given input file.

        Args:
            input_file: Path to the FP input file. If None, uses fminput.txt in fp_home.
            work_dir: Working directory for the run. If None, uses fp_home.
                      Data files are copied here if work_dir != fp_home.
            extra_env: Additional environment variables.

        Returns:
            FPRunResult with stdout/stderr capture.

        Raises:
            FPExecutableError: If fp.exe is not found or Wine is unavailable.
        """
        import time

        resolved_input = input_file or (self.fp_home / "fminput.txt")
        resolved_work_dir = work_dir or self.fp_home
        preflight = self.preflight_report(input_file=resolved_input, work_dir=resolved_work_dir)

        if not self.exe_path.exists():
            raise FPExecutableError(
                f"fp.exe not found at {self.exe_path}. "
                "Check your fp_home path and that fp.exe exists there.",
                details={"preflight_report": preflight},
            )

        if self.use_wine and not shutil.which("wine"):
            raise FPExecutableError(
                "Wine is required on non-Windows systems to run fp.exe. "
                "Install with `brew install --cask wine-stable`.",
                details={"preflight_report": preflight},
            )

        if not self.check_available():
            raise FPExecutableError(
                f"fp.exe not available at {self.exe_path} "
                f"(Wine needed: {self.use_wine}, Wine found: {bool(shutil.which('wine'))})",
                details={"preflight_report": preflight},
            )

        if input_file is None:
            input_file = self.fp_home / "fminput.txt"

        if work_dir is None:
            work_dir = self.fp_home
        else:
            work_dir.mkdir(parents=True, exist_ok=True)
            self._copy_data_files(work_dir)

        expected_input_path = work_dir / input_file.name
        if not expected_input_path.exists():
            raise FPExecutableError(
                "Input file for fp.exe is missing in work directory: "
                f"{expected_input_path}. Ensure scenario input was copied/patched before run."
            )
        missing_data_files = self._missing_files(work_dir, self.required_data_files)
        if missing_data_files:
            raise FPExecutableError(
                "Missing required FP data files in work directory "
                f"{work_dir}: {', '.join(missing_data_files)}"
            )

        # Build command
        cmd = self._build_command()

        # Prepare stdin: fp.exe expects "INPUT FILE=<filename> ;" on stdin
        stdin_text = f"INPUT FILE={input_file.name} ;\n"

        env = os.environ.copy()
        if extra_env:
            env.update(extra_env)
        if self.use_wine:
            self._validate_wineprefix(env, work_dir=work_dir)

        start = time.monotonic()
        stdout_file = work_dir / "fp-exe.stdout.txt"
        stderr_file = work_dir / "fp-exe.stderr.txt"
        try:
            proc = subprocess.run(
                cmd,
                input=stdin_text,
                capture_output=True,
                text=True,
                cwd=str(work_dir),
                timeout=self.timeout_seconds,
                env=env,
            )
        except subprocess.TimeoutExpired as e:
            duration = time.monotonic() - start
            timed_out_stdout = e.stdout or ""
            timed_out_stderr = e.stderr or ""
            if isinstance(timed_out_stdout, bytes):
                timed_out_stdout = timed_out_stdout.decode("utf-8", errors="replace")
            if isinstance(timed_out_stderr, bytes):
                timed_out_stderr = timed_out_stderr.decode("utf-8", errors="replace")
            stdout_file.write_text(str(timed_out_stdout), encoding="utf-8")
            stderr_file.write_text(str(timed_out_stderr), encoding="utf-8")
            raise FPExecutableError(
                f"fp.exe timed out after {self.timeout_seconds}s",
                details={
                    "timeout_seconds": int(self.timeout_seconds),
                    "termination_reason": "timeout_expired",
                    "work_dir": str(work_dir),
                    "command": cmd,
                    "duration_seconds": float(duration),
                    "stdout_path": str(stdout_file),
                    "stderr_path": str(stderr_file),
                },
            ) from e
        except FileNotFoundError as e:
            raise FPExecutableError(f"Failed to execute: {e}") from e

        duration = time.monotonic() - start

        # Write captured output to file
        output_file = work_dir / "fmout.txt"
        stdout_text = proc.stdout or ""
        stderr_text = proc.stderr or ""
        stdout_file.write_text(stdout_text, encoding="utf-8")
        stderr_file.write_text(stderr_text, encoding="utf-8")
        if stdout_text:
            output_file.write_text(stdout_text, encoding="utf-8")

        return FPRunResult(
            return_code=proc.returncode,
            stdout=stdout_text,
            stderr=stderr_text,
            working_dir=work_dir,
            input_file=input_file,
            output_file=output_file if stdout_text else None,
            duration_seconds=duration,
        )

    def _build_command(self) -> list[str]:
        """Build the subprocess command list."""
        exe = str(self.exe_path.resolve())
        if self.use_wine:
            return ["wine", exe]
        return [exe]

    def _copy_data_files(self, dest: Path) -> None:
        """Copy required data files from fp_home to a working directory."""
        data_files = ["fmdata.txt", "fmage.txt", "fmexog.txt", "fminput.txt"]
        for fname in data_files:
            src = self.fp_home / fname
            if src.exists():
                dst = dest / fname
                if not dst.exists():
                    shutil.copy2(src, dst)

    def _missing_files(self, root: Path, names: Iterable[str]) -> list[str]:
        """Return required filenames that do not exist under root."""
        return [name for name in names if not (root / name).exists()]
