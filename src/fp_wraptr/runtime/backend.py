"""Runtime backend protocol for model execution.

Defines the interface that any model backend (fp.exe subprocess, fair-py
pure-Python engine, or future alternatives) must satisfy in order to be
used by the scenario runner.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, runtime_checkable


@dataclass
class RunResult:
    """Backend-agnostic result of a model run."""

    return_code: int
    stdout: str
    stderr: str
    working_dir: Path
    input_file: Path
    output_file: Path | None = None
    duration_seconds: float = 0.0

    @property
    def success(self) -> bool:
        return self.return_code == 0


@runtime_checkable
class ModelBackend(Protocol):
    """Protocol that every model execution backend must implement.

    Implementations:
      - ``FPExecutable``  — subprocess wrapper around fp.exe / Wine
      - ``FairPyBackend`` — pure-Python engine (wraps fair-py / fp-py)
    """

    def check_available(self) -> bool:
        """Return True if this backend can execute runs right now."""
        ...

    def run(
        self,
        input_file: Path | None = None,
        work_dir: Path | None = None,
        extra_env: dict[str, str] | None = None,
    ) -> RunResult:
        """Execute a model run and return the result.

        Args:
            input_file: Path to the input file.
            work_dir: Working directory for the run.
            extra_env: Additional environment variables.

        Returns:
            RunResult with captured output.
        """
        ...


@dataclass
class BackendInfo:
    """Metadata about a backend for display/debugging."""

    name: str
    version: str = ""
    available: bool = False
    details: dict[str, str] = field(default_factory=dict)
