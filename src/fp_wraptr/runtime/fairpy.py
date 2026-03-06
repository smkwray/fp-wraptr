"""fppy (fp-py) pure-Python backend.

fp-wraptr vendors the ``fppy`` package and runs it as a subprocess using:

    python -m fppy.cli mini-run ...

This keeps the integration surface stable while fppy continues to evolve.
The parity contract for fp-wraptr is PABEV.TXT (PRINTVAR LOADFORMAT output).
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from fp_wraptr.runtime.backend import BackendInfo, RunResult

_INPUT_FILE_RE = re.compile(r"\bFILE\s*=\s*(?P<name>[^\s]+)", re.IGNORECASE)


class FairPyBackendError(Exception):
    """Raised when the fair-py backend cannot execute successfully."""


@dataclass
class FairPyBackend:
    """Pure-Python model backend wrapping vendored fppy via subprocess."""

    fp_home: Path = field(default_factory=lambda: Path("FM"))
    timeout_seconds: int = 600
    heartbeat_seconds: int = 5
    # Default is intentionally "default" here; wraptr's parity runner layers
    # override to "parity" when doing cross-engine comparisons.
    eq_flags_preset: str = "default"
    eq_iter_trace: bool = False
    eq_iter_trace_period: str | None = None
    eq_iter_trace_targets: str | None = None
    eq_iter_trace_max_events: int | None = None
    num_threads: int | None = None

    def check_available(self) -> bool:
        try:
            import fppy  # noqa: F401
        except ImportError:
            return False
        return bool(sys.executable)

    def _require_file(self, path: Path, *, label: str) -> None:
        if not path.exists():
            raise FairPyBackendError(f"Missing required {label}: {path}")

    def _write_model_config(
        self,
        work_dir: Path,
        *,
        fminput_path: Path,
        fmexog_path: Path,
        fmout_path: Path,
    ) -> Path:
        config_path = work_dir / "model-config.toml"
        legacy = {
            "fminput": fminput_path,
            "fmdata": work_dir / "fmdata.txt",
            "fmage": work_dir / "fmage.txt",
            # fppy reads exogenous CHANGEVAR instructions from this path. For scenario
            # runs, wraptr may generate a `fmexog_override.txt`; honor it when present.
            "fmexog": fmexog_path,
            "fmout": fmout_path,
        }
        lines = ["[model]", "", "[model.legacy]"]
        for key, value in legacy.items():
            lines.append(f'{key} = "{Path(value).resolve()!s}"')
        lines.append("")
        config_path.write_text("\n".join(lines), encoding="utf-8")
        return config_path

    def _eq_args(self, *, fmout_coefs: Path) -> list[str]:
        preset = str(self.eq_flags_preset or "").strip().lower()
        if preset in {"", "default", "none", "off"}:
            return []
        if preset not in {"iss02_baseline", "parity"}:
            raise FairPyBackendError(f"Unknown eq_flags_preset: {self.eq_flags_preset!r}")
        if preset == "parity":
            # Parity runs want FP-style solve semantics:
            # - enable EQ backfill
            # - honor SETUPSOLVE iteration controls (MINITERS/MAXITERS/MAXCHECK)
            # - solve per-period (Gauss-Seidel), period-scoped
            #
            # Intentionally do NOT enable "context assignments first iter only";
            # it can hide non-closure in GENR/IDENT chains during solve windows.
            return [
                "--enable-eq",
                "--eq-use-setupsolve",
                "--eq-flags-preset-label",
                "parity",
                "--eq-period-sequential",
                "--eq-period-scoped",
                "on",
                "--eq-coefs-fmout",
                str(fmout_coefs),
            ]
        return [
            "--enable-eq",
            "--eq-use-setupsolve",
            "--eq-flags-preset-label",
            "iss02_baseline",
            "--eq-period-sequential",
            "--eq-period-scoped",
            "on",
            "--eq-period-sequential-context-assignments-first-iter-only",
            "--eq-coefs-fmout",
            str(fmout_coefs),
        ]

    def _eq_iter_trace_args(self) -> list[str]:
        if not bool(self.eq_iter_trace):
            return []
        args = ["--eq-iter-trace"]
        if self.eq_iter_trace_period is not None:
            args.extend(["--eq-iter-trace-period", str(self.eq_iter_trace_period)])
        if self.eq_iter_trace_targets is not None:
            args.extend(["--eq-iter-trace-targets", str(self.eq_iter_trace_targets)])
        if self.eq_iter_trace_max_events is not None:
            args.extend(["--eq-iter-trace-max-events", str(int(self.eq_iter_trace_max_events))])
        return args

    def _thread_env_overrides(self) -> dict[str, str]:
        raw = self.num_threads
        if raw is None:
            return {}
        threads = int(raw)
        if threads < 1:
            return {}
        value = str(threads)
        return {
            "OMP_NUM_THREADS": value,
            "OPENBLAS_NUM_THREADS": value,
            "MKL_NUM_THREADS": value,
            "NUMEXPR_NUM_THREADS": value,
            "VECLIB_MAXIMUM_THREADS": value,
        }

    def _write_eq_overlay_from_fmout(self, *, fmout_path: Path, out_path: Path) -> Path:
        """Write a standalone FP include file listing all equation numbers from fmout.

        fppy uses EQ records as the *execution deck* that decides which equation
        specs to apply during EQ backfill. fp.exe scenarios can omit EQ statements
        because the model is compiled, but fppy still needs an explicit EQ target
        list to approximate `SOLVE`.
        """
        from fppy.eq_solver import load_eq_specs_from_fmout

        specs = load_eq_specs_from_fmout(fmout_path)
        numbers: list[int] = []
        for spec in specs.values():
            number = getattr(spec, "equation_number", None)
            if isinstance(number, int):
                numbers.append(int(number))
        numbers = sorted(set(numbers))

        header = [
            "@ fp-wraptr: autogenerated EQ target overlay for fppy",
            f"@ source: {fmout_path.resolve()}",
            f"@ equation_count: {len(numbers)}",
            "",
        ]
        lines = header + [f"EQ {num} ;" for num in numbers] + [""]
        out_path.write_text("\n".join(lines), encoding="utf-8")
        return out_path

    def _write_fppy_wrapper_input(
        self,
        *,
        work_dir: Path,
        base_input: Path,
        eq_overlay: Path,
        identity_overlay: Path | None = None,
    ) -> Path:
        wrapper_path = work_dir / "fppy_fminput.txt"
        # Important: fppy post-command replay (PRINTVAR, SETYYTOY, etc.) uses the
        # parsed record list. If we `INPUT FILE=...` here, those statements live
        # in nested decks and are not reliably replayed. Inline both the EQ
        # overlay and the base input deck instead.
        eq_text = eq_overlay.read_text(encoding="utf-8", errors="replace").splitlines()
        base_text = base_input.read_text(encoding="utf-8", errors="replace").splitlines()
        identity_name = identity_overlay.name if identity_overlay is not None else None
        identity_text: list[str] = []
        if identity_overlay is not None:
            identity_text = identity_overlay.read_text(
                encoding="utf-8", errors="replace"
            ).splitlines()

        if identity_name:
            inserted = False
            out_lines: list[str] = []
            last_smpl_line: str | None = None
            for raw in base_text:
                stripped = raw.lstrip()
                if stripped.upper().startswith("SMPL"):
                    last_smpl_line = raw.rstrip("\n")
                if not inserted and raw.lstrip().upper().startswith("SOLVE"):
                    out_lines.extend(identity_text)
                    # The identity overlay may contain its own SMPL statements.
                    # Restore the scenario deck's current SMPL window so the SOLVE
                    # statement executes over the intended range.
                    if last_smpl_line is not None:
                        out_lines.append(last_smpl_line)
                    inserted = True
                out_lines.append(raw.rstrip("\n"))
            if not inserted:
                # Fall back: insert before QUIT/END, or append at the end.
                fallback: list[str] = []
                inserted = False
                last_smpl_line = None
                for raw in out_lines:
                    stripped = raw.lstrip()
                    if stripped.upper().startswith("SMPL"):
                        last_smpl_line = raw.rstrip("\n")
                    if not inserted and raw.lstrip().upper().startswith(("QUIT", "END")):
                        fallback.extend(identity_text)
                        if last_smpl_line is not None:
                            fallback.append(last_smpl_line)
                        inserted = True
                    fallback.append(raw)
                if not inserted:
                    fallback.extend(identity_text)
                    if last_smpl_line is not None:
                        fallback.append(last_smpl_line)
                base_text = fallback
            else:
                base_text = out_lines

        lines = [
            "@ fp-wraptr: fppy wrapper input",
            "@ - inlines EQ records (from fmout) so fppy can run EQ backfill",
            "@ - optionally injects baseline identity/GENR/IDENT definitions before SOLVE",
            "@ - then inlines the original scenario input deck",
            "",
            *eq_text,
            "",
            "@ fp-wraptr: begin base input",
            *base_text,
            "",
        ]
        wrapper_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return wrapper_path

    def _collect_assignment_lhs_from_input_tree(self, *, work_dir: Path, entry_input: Path) -> set[str]:
        """Collect assignment LHS names from a staged scenario input tree.

        When the parity preset injects baseline identity/GENR statements, we must
        avoid overriding scenario-specific definitions (for example PSE adds JG
        terms to GDP/GDPR). Scan the staged deck(s) in `work_dir` to discover
        which symbols are already defined so we can skip them in the overlay.
        """
        from fp_wraptr.io.input_parser import parse_fp_input_text

        def _clean_filename(token: str) -> str:
            raw = str(token or "").strip().strip("\"'")
            raw = raw.rstrip(";")
            return raw

        def _extract_input_file_from_command(body: str) -> str | None:
            match = _INPUT_FILE_RE.search(body or "")
            if not match:
                return None
            return _clean_filename(match.group("name"))

        def _resolve_case_insensitive(root: Path, relative: str) -> Path:
            rel = Path(relative)
            cur = root
            for part in rel.parts:
                candidate = cur / part
                if candidate.exists():
                    cur = candidate
                    continue
                alt_lower = cur / part.lower()
                if alt_lower.exists():
                    cur = alt_lower
                    continue
                alt_upper = cur / part.upper()
                if alt_upper.exists():
                    cur = alt_upper
                    continue
                want = part.lower()
                found = None
                try:
                    for child in cur.iterdir():
                        if child.name.lower() == want:
                            found = child
                            break
                except OSError:
                    found = None
                if found is None:
                    return root / rel
                cur = found
            return cur

        lhs_names: set[str] = set()
        visited: set[str] = set()
        queue: list[str] = [str(entry_input.name)]

        while queue:
            name = _clean_filename(queue.pop(0))
            if not name:
                continue
            norm = name.lower()
            if norm in visited:
                continue
            visited.add(norm)

            path = _resolve_case_insensitive(work_dir, name)
            if not path.exists():
                # Work dirs should be staged with all includes, but keep this
                # best-effort and avoid hard failures in the backend wrapper.
                continue

            parsed = parse_fp_input_text(path.read_text(encoding="utf-8", errors="replace"))
            for section in ("creates", "generated_vars", "identities", "equation_lhs"):
                for item in parsed.get(section, []) or []:
                    if not isinstance(item, dict):
                        continue
                    lhs = item.get("name")
                    if lhs is None:
                        continue
                    token = str(lhs).strip().upper()
                    if token:
                        lhs_names.add(token)

            for cmd in parsed.get("control_commands", []) or []:
                if not isinstance(cmd, dict):
                    continue
                if str(cmd.get("name", "")).upper() != "INPUT":
                    continue
                include = _extract_input_file_from_command(str(cmd.get("body", "")))
                if include:
                    queue.append(include)

        return lhs_names

    def _write_identity_overlay_from_base_deck(
        self,
        *,
        fp_home: Path,
        out_path: Path,
        exclude_lhs: set[str] | None = None,
    ) -> Path:
        """Extract baseline GENR/IDENT/LHS/CREATE statements from the base fminput deck.

        Many real scenarios (including PSE) rely on fp.exe's compiled identity/equation
        machinery and therefore omit a large number of derived-series GENR/IDENT
        statements from their scenario scripts. fppy needs these statements to exist
        in the input deck to compute the same stored series used by PRINTVAR exports.
        """
        from fppy.expressions import parse_assignment

        src = fp_home / "fminput.txt"
        self._require_file(src, label="fminput.txt in fp_home (identity overlay source)")
        keep_prefixes = ("GENR", "IDENT", "LHS", "CREATE")
        exclude = {str(item).strip().upper() for item in (exclude_lhs or set()) if str(item).strip()}
        out_lines: list[str] = [
            "@ fp-wraptr: autogenerated identity overlay (from base fminput.txt)",
            f"@ source: {src.resolve()}",
            "",
        ]
        statement_lines: list[str] | None = None
        for raw in src.read_text(encoding="utf-8", errors="replace").splitlines():
            stripped = raw.lstrip()
            upper = stripped.upper()
            if statement_lines is not None:
                statement_lines.append(raw.rstrip("\n"))
                if ";" in raw:
                    keep = True
                    if exclude:
                        try:
                            assignment = parse_assignment("\n".join(statement_lines))
                        except Exception:
                            assignment = None
                        if assignment is not None:
                            lhs_key = str(assignment.lhs).strip().upper()
                            if lhs_key and lhs_key in exclude:
                                keep = False
                    if keep:
                        out_lines.extend(statement_lines)
                    statement_lines = None
                continue
            if upper.startswith("SOLVE"):
                break
            if upper.startswith("@") or not stripped:
                continue
            if upper.startswith("SMPL"):
                # Preserve SMPL window context for extracted CREATE/GENR/IDENT/LHS
                # statements. This is especially important for pre-solve trend-break
                # blocks that intentionally end at the last historical quarter.
                out_lines.append(raw.rstrip("\n"))
                continue
            if not upper.startswith(keep_prefixes):
                continue
            statement_lines = [raw.rstrip("\n")]
            if ";" in raw:
                keep = True
                if exclude:
                    try:
                        assignment = parse_assignment("\n".join(statement_lines))
                    except Exception:
                        assignment = None
                    if assignment is not None:
                        lhs_key = str(assignment.lhs).strip().upper()
                        if lhs_key and lhs_key in exclude:
                            keep = False
                if keep:
                    out_lines.extend(statement_lines)
                statement_lines = None
        if statement_lines:
            keep = True
            if exclude:
                try:
                    assignment = parse_assignment("\n".join(statement_lines))
                except Exception:
                    assignment = None
                if assignment is not None:
                    lhs_key = str(assignment.lhs).strip().upper()
                    if lhs_key and lhs_key in exclude:
                        keep = False
            if keep:
                out_lines.extend(statement_lines)
        out_lines.append("")
        out_path.write_text("\n".join(out_lines), encoding="utf-8")
        return out_path

    def run(
        self,
        input_file: Path | None = None,
        work_dir: Path | None = None,
        extra_env: dict[str, str] | None = None,
    ) -> RunResult:
        if not self.check_available():
            raise FairPyBackendError("fppy is not importable (vendoring/packaging issue).")

        if work_dir is None:
            raise FairPyBackendError("work_dir is required for fair-py backend runs")
        work_dir = Path(work_dir).resolve()
        work_dir.mkdir(parents=True, exist_ok=True)

        expected_input = work_dir / (input_file.name if input_file is not None else "fminput.txt")
        self._require_file(expected_input, label="input file")
        self._require_file(work_dir / "fmdata.txt", label="fmdata.txt")
        self._require_file(work_dir / "fmage.txt", label="fmage.txt")
        fmexog_override = work_dir / "fmexog_override.txt"
        fmexog_path = fmexog_override if fmexog_override.exists() else (work_dir / "fmexog.txt")
        self._require_file(fmexog_path, label="fmexog (or fmexog_override).txt")

        # Coefficient baseline for EQ solve
        fmout_src = self.fp_home / "fmout.txt"
        self._require_file(fmout_src, label="fmout.txt (coefficient baseline) in fp_home")
        fmout_coefs = work_dir / "fmout_coefs.txt"
        shutil.copy2(fmout_src, fmout_coefs)

        # fppy needs explicit `EQ ...;` records present in the input deck to build
        # the equation backfill target list. fp.exe scenarios rely on compiled
        # model state, so wrap fppy with a small include that injects these EQ
        # statements extracted from the coefficient fmout.
        eq_overlay = self._write_eq_overlay_from_fmout(
            fmout_path=fmout_coefs, out_path=work_dir / "fppy_eq_overlay.txt"
        )
        identity_overlay: Path | None = None
        if str(self.eq_flags_preset or "").strip().lower() == "parity":
            # For parity preset, include full pre-first-SOLVE CREATE/GENR/IDENT/LHS
            # statements from baseline fminput so prerequisite derived series are
            # available to fppy (including multiline assignments).
            defined_lhs = self._collect_assignment_lhs_from_input_tree(
                work_dir=work_dir,
                entry_input=expected_input,
            )
            identity_overlay = self._write_identity_overlay_from_base_deck(
                fp_home=self.fp_home,
                out_path=work_dir / "fppy_identity_overlay.txt",
                exclude_lhs=defined_lhs,
            )
        wrapper_input = self._write_fppy_wrapper_input(
            work_dir=work_dir,
            base_input=expected_input,
            eq_overlay=eq_overlay,
            identity_overlay=identity_overlay,
        )

        config_path = self._write_model_config(
            work_dir,
            fminput_path=wrapper_input,
            fmexog_path=fmexog_path,
            fmout_path=fmout_coefs,
        )

        cmd = [
            sys.executable,
            "-m",
            "fppy.cli",
            "mini-run",
            "--config",
            str(config_path),
            "--on-error",
            "continue",
            *self._eq_args(fmout_coefs=fmout_coefs),
            *self._eq_iter_trace_args(),
            "--report-json",
            str(work_dir / "fppy_report.json"),
        ]

        thread_env = self._thread_env_overrides()
        env = os.environ.copy()
        if extra_env:
            env.update(extra_env)
        env.update(thread_env)
        # Never write bytecode into the repo (OneDrive path).
        env.setdefault("PYTHONDONTWRITEBYTECODE", "1")
        env.setdefault("PYTHONPYCACHEPREFIX", "/tmp/fp-wraptr-pycache")

        stdout_path = work_dir / "fppy.stdout.txt"
        stderr_path = work_dir / "fppy.stderr.txt"
        runtime_path = work_dir / "fppy.runtime.json"

        def _write_runtime(status: str, **extra: object) -> None:
            payload: dict[str, object] = {
                "status": str(status),
                "pid": int(extra.pop("pid", 0) or 0),
                "elapsed_seconds": float(extra.pop("elapsed_seconds", 0.0) or 0.0),
                "timeout_seconds": int(self.timeout_seconds),
                "heartbeat_seconds": int(max(1, int(self.heartbeat_seconds))),
                "updated_unix": float(time.time()),
                "num_threads_requested": (
                    int(self.num_threads) if self.num_threads is not None and int(self.num_threads) > 0 else None
                ),
                "thread_env_overrides": dict(thread_env),
            }
            payload.update(extra)
            runtime_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

        started = time.monotonic()
        heartbeat_seconds = max(1, int(self.heartbeat_seconds))
        deadline = started + float(self.timeout_seconds)
        with (
            stdout_path.open("w", encoding="utf-8") as stdout_file,
            stderr_path.open("w", encoding="utf-8") as stderr_file,
        ):
            proc = subprocess.Popen(
                cmd,
                cwd=str(work_dir),
                stdout=stdout_file,
                stderr=stderr_file,
                text=True,
                env=env,
            )
            _write_runtime("running", pid=proc.pid, elapsed_seconds=0.0)
            next_heartbeat = started + float(heartbeat_seconds)
            while True:
                return_code = proc.poll()
                now = time.monotonic()
                elapsed = now - started
                if return_code is not None:
                    break
                if now >= deadline:
                    proc.kill()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.terminate()
                    _write_runtime(
                        "timeout",
                        pid=proc.pid,
                        elapsed_seconds=elapsed,
                    )
                    raise FairPyBackendError(
                        f"fppy mini-run timed out after {self.timeout_seconds}s"
                    )
                if now >= next_heartbeat:
                    _write_runtime(
                        "running",
                        pid=proc.pid,
                        elapsed_seconds=elapsed,
                    )
                    next_heartbeat = now + float(heartbeat_seconds)
                time.sleep(0.25)

        duration_seconds = time.monotonic() - started
        stdout_text = stdout_path.read_text(encoding="utf-8", errors="replace")
        stderr_text = stderr_path.read_text(encoding="utf-8", errors="replace")
        _write_runtime(
            "completed",
            pid=proc.pid,
            elapsed_seconds=duration_seconds,
            return_code=int(proc.returncode),
        )

        # Prefer PABEV parity contract output if present; fall back to fmout.
        output_file = None
        for candidate in ("PABEV.TXT", "pabev.txt", "fmout.txt"):
            path = work_dir / candidate
            if path.exists():
                output_file = path
                break

        return RunResult(
            return_code=int(proc.returncode),
            stdout=stdout_text,
            stderr=stderr_text,
            working_dir=work_dir,
            input_file=expected_input,
            output_file=output_file,
            duration_seconds=float(duration_seconds),
        )

    def info(self) -> BackendInfo:
        """Return metadata about this backend."""
        available = self.check_available()
        version = ""
        if available:
            try:
                import fppy

                version = getattr(fppy, "__version__", "unknown")
            except ImportError:
                pass
        return BackendInfo(
            name="fair-py",
            version=version,
            available=available,
            details={"fp_home": str(self.fp_home)},
        )
