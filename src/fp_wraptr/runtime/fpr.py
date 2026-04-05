"""fp-r backend."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

from fp_wraptr.runtime.backend import BackendInfo, RunResult
from fp_wraptr.runtime.semantics import (
    get_backend_semantics_profile,
    write_semantics_manifest,
)


class FpRBackendError(Exception):
    """Raised when the fp-r backend cannot execute successfully."""


_INPUT_FILE_RE = re.compile(r"\bFILE\s*=\s*(?P<name>[^\s]+)", re.IGNORECASE)


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
    """Subprocess wrapper for fp-r runtime entrypoints."""

    bundle_path: Path | None = None
    fp_home: Path = field(default_factory=lambda: Path("FM"))
    fp_r_home: Path = field(default_factory=lambda: _repo_root() / "fp-r")
    rscript_path: Path | None = None
    timeout_seconds: int = 1200
    semantics_profile: str = "compat"

    def _resolve_bundle_path(self) -> Path | None:
        bundle = self.bundle_path
        if bundle is None:
            return None
        return Path(bundle).expanduser().resolve()

    def _clean_filename(self, token: str) -> str:
        raw = str(token or "").strip().strip("\"'")
        return raw.rstrip(";")

    def _extract_input_file_from_command(self, body: str) -> str | None:
        match = _INPUT_FILE_RE.search(body or "")
        if not match:
            return None
        return self._clean_filename(match.group("name"))

    def _resolve_case_insensitive(self, root: Path, relative: str) -> Path:
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

    def _collect_assignment_lhs_from_input_tree(
        self, *, work_dir: Path, entry_input: Path
    ) -> set[str]:
        """Collect assignment targets already defined in the staged scenario tree."""
        from fp_wraptr.io.input_parser import parse_fp_input_text

        lhs_names: set[str] = set()
        visited: set[str] = set()
        queue: list[str] = [str(entry_input.name)]

        while queue:
            name = self._clean_filename(queue.pop(0))
            if not name:
                continue
            norm = name.lower()
            if norm in visited:
                continue
            visited.add(norm)

            path = self._resolve_case_insensitive(work_dir, name)
            if not path.exists():
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
                include = self._extract_input_file_from_command(str(cmd.get("body", "")))
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
        """Extract baseline CREATE/GENR/IDENT/LHS statements from base fminput."""
        src = Path(fp_home).expanduser().resolve() / "fminput.txt"
        if not src.exists():
            raise FpRBackendError(f"Missing required base fminput.txt: {src}")

        keep_prefixes = ("GENR", "IDENT", "LHS", "CREATE")
        exclude = {
            str(item).strip().upper() for item in (exclude_lhs or set()) if str(item).strip()
        }
        out_lines: list[str] = [
            "@ fp-wraptr: autogenerated fp-r identity overlay (from base fminput.txt)",
            f"@ source: {src.resolve()}",
            "",
        ]
        lhs_pattern = re.compile(r"^\s*(CREATE|GENR|IDENT|LHS)\s+([A-Za-z][A-Za-z0-9_]*)\s*=", re.IGNORECASE)

        def _extract_lhs(statement_lines: list[str]) -> str:
            statement_text = "\n".join(statement_lines)
            match = lhs_pattern.match(statement_text)
            if not match:
                return ""
            return str(match.group(2) or "").strip().upper()

        statement_lines: list[str] | None = None
        for raw in src.read_text(encoding="utf-8", errors="replace").splitlines():
            stripped = raw.lstrip()
            upper = stripped.upper()
            if statement_lines is not None:
                statement_lines.append(raw.rstrip("\n"))
                if ";" in raw:
                    keep = True
                    if exclude:
                        lhs_key = _extract_lhs(statement_lines)
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
                out_lines.append(raw.rstrip("\n"))
                continue
            if not upper.startswith(keep_prefixes):
                continue
            statement_lines = [raw.rstrip("\n")]
            if ";" in raw:
                keep = True
                if exclude:
                    lhs_key = _extract_lhs(statement_lines)
                    if lhs_key and lhs_key in exclude:
                        keep = False
                if keep:
                    out_lines.extend(statement_lines)
                statement_lines = None
        if statement_lines:
            keep = True
            if exclude:
                lhs_key = _extract_lhs(statement_lines)
                if lhs_key and lhs_key in exclude:
                    keep = False
            if keep:
                out_lines.extend(statement_lines)
        out_lines.append("")
        out_path.write_text("\n".join(out_lines), encoding="utf-8")
        return out_path

    def _write_fpr_wrapper_input(
        self,
        *,
        work_dir: Path,
        base_input: Path,
        identity_overlay: Path | None = None,
    ) -> Path:
        """Write an fp-r wrapper input that injects base helper definitions."""
        wrapper_path = work_dir / "fp_r_wrapper_input.txt"
        base_text = base_input.read_text(encoding="utf-8", errors="replace").splitlines()
        identity_text = (
            identity_overlay.read_text(encoding="utf-8", errors="replace").splitlines()
            if identity_overlay is not None
            else []
        )
        if identity_text:
            out_lines: list[str] = []
            inserted = False
            last_smpl_line: str | None = None
            for raw in base_text:
                stripped = raw.lstrip()
                if stripped.upper().startswith("SMPL"):
                    last_smpl_line = raw.rstrip("\n")
                if not inserted and stripped.upper().startswith("SOLVE"):
                    out_lines.extend(identity_text)
                    if last_smpl_line is not None:
                        out_lines.append(last_smpl_line)
                    inserted = True
                out_lines.append(raw)
            if not inserted:
                fallback: list[str] = []
                last_smpl_line = None
                for raw in base_text:
                    stripped = raw.lstrip()
                    if stripped.upper().startswith("SMPL"):
                        last_smpl_line = raw.rstrip("\n")
                    if not inserted and stripped.upper().startswith(("QUIT", "END")):
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
            "@ fp-wraptr: fp-r wrapper input",
            "@ - injects baseline CREATE/GENR/IDENT/LHS definitions before SOLVE",
            "@ - then inlines the original scenario input deck",
            "",
            "@ fp-wraptr: begin base input",
            *base_text,
            "",
        ]
        wrapper_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return wrapper_path

    def _resolve_runner_script(self) -> Path:
        script = self.fp_r_home / "scripts" / "run_backend_bundle.R"
        return script.resolve()

    def _resolve_standard_input_runner(self) -> Path:
        script = self.fp_r_home / "scripts" / "run_standard_input.R"
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
        rscript = self._resolve_rscript_path()
        if rscript is None:
            return False
        bundle = self._resolve_bundle_path()
        if bundle is not None and bundle.exists() and self._resolve_runner_script().exists():
            return True
        return self._resolve_standard_input_runner().exists()

    def info(self) -> BackendInfo:
        rscript = self._resolve_rscript_path()
        bundle = self._resolve_bundle_path()
        profile = get_backend_semantics_profile(self.semantics_profile)
        return BackendInfo(
            name="fp-r",
            available=self.check_available(),
            details={
                "fp_r_home": str(self.fp_r_home),
                "bundle_path": str(bundle) if bundle is not None else "",
                "mode": "bundle" if bundle is not None else "raw_input",
                "rscript_path": str(rscript) if rscript is not None else "",
                "semantics_profile": profile.name,
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
        rscript = self._resolve_rscript_path()
        if rscript is None:
            raise FpRBackendError("Rscript is not available for fp-r backend")
        profile = get_backend_semantics_profile(self.semantics_profile)
        command: list[str]
        mode: str
        staged_input: Path | None = None
        if bundle_path is not None and bundle_path.exists():
            runner_script = self._resolve_runner_script()
            if not runner_script.exists():
                raise FpRBackendError(f"Missing fp-r runner script: {runner_script}")
            command = [
                str(rscript),
                str(runner_script),
                "--bundle",
                str(bundle_path),
                "--work-dir",
                str(work_dir),
                "--semantics-profile",
                profile.name,
            ]
            mode = "bundle"
        else:
            if input_file is None:
                raise FpRBackendError("fp-r raw-input mode requires an input_file")
            input_path = Path(input_file).expanduser().resolve()
            if not input_path.exists():
                raise FpRBackendError(f"Missing fp-r raw input file: {input_path}")
            staged_input = input_path
            runner_script = self._resolve_standard_input_runner()
            if not runner_script.exists():
                raise FpRBackendError(f"Missing fp-r raw-input runner script: {runner_script}")
            command = [
                str(rscript),
                str(runner_script),
                "--input",
                str(staged_input),
                "--work-dir",
                str(work_dir),
                "--search-dir",
                str(work_dir),
                "--semantics-profile",
                profile.name,
            ]
            for flag, name in (
                ("--fmdata", "fmdata.txt"),
                ("--fmexog", "fmexog.txt"),
                ("--fmout", "fmout.txt"),
            ):
                candidate = work_dir / name
                if candidate.exists():
                    command.extend([flag, str(candidate.resolve())])
            mode = "raw_input"
        env = os.environ.copy()
        if extra_env:
            env.update({str(k): str(v) for k, v in extra_env.items()})

        manifest_path, manifest, manifest_hash = write_semantics_manifest(
            work_dir=work_dir,
            semantics_profile=profile.name,
            entry_input=staged_input,
        )

        runtime_payload = {
            "backend": "fp-r",
            "mode": mode,
            "bundle_path": str(bundle_path) if bundle_path is not None else None,
            "runner_script": str(runner_script),
            "rscript_path": str(rscript),
            "semantics_profile": profile.name,
            "input_file": str(Path(input_file).resolve()) if input_file is not None else None,
            "effective_input_file": str(staged_input) if staged_input is not None else None,
            "fp_home": str(Path(self.fp_home).expanduser().resolve()),
            "work_dir": str(work_dir),
            "semantics_manifest_path": str(manifest_path),
            "semantics_manifest_hash": manifest_hash,
            "semantics_manifest": manifest,
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
