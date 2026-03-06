"""Map unsupported fppy statements to parity hard-fail impact."""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict, deque
from pathlib import Path
from typing import Any

try:
    from scripts.triage_parity_hardfails import triage_parity_hardfails
except ModuleNotFoundError:  # pragma: no cover - direct script execution fallback
    from triage_parity_hardfails import triage_parity_hardfails

_LHS_ASSIGN_RE = re.compile(r"^\s*LHS\s+(?P<name>[A-Za-z][A-Za-z0-9_]*)\s*=", re.IGNORECASE)
_BLOCK_START_RE = re.compile(r"^\s*(EQ|MODEQ|FSR)\b", re.IGNORECASE)
_EQ_LHS_RE = re.compile(r"^\s*EQ\s+\d+\s+(?P<name>[A-Za-z][A-Za-z0-9_]*)\b", re.IGNORECASE)
_EQ_NO_NUM_LHS_RE = re.compile(r"^\s*EQ\s+(?P<name>[A-Za-z][A-Za-z0-9_]*)\b", re.IGNORECASE)
_MODEQ_LHS_RE = re.compile(
    r"^\s*MODEQ\s+\d+\s+(?P<name>[A-Za-z][A-Za-z0-9_]*)\b",
    re.IGNORECASE,
)
_MODEQ_NO_NUM_LHS_RE = re.compile(
    r"^\s*MODEQ\s+(?P<name>[A-Za-z][A-Za-z0-9_]*)\b",
    re.IGNORECASE,
)
_FSR_LHS_RE = re.compile(r"^\s*FSR\s+(?P<name>[A-Za-z][A-Za-z0-9_]*)\b", re.IGNORECASE)
_ASSIGNMENT_LHS_RE = re.compile(r"(?P<name>[A-Za-z][A-Za-z0-9_]*)\s*=")
_EQ_NUM_RE = re.compile(r"^\s*EQ\s+(?P<number>\d+)\b", re.IGNORECASE)
_MODEQ_NUM_RE = re.compile(r"^\s*MODEQ\s+(?P<number>\d+)\b", re.IGNORECASE)
_ASSIGNMENT_RE = re.compile(
    r"^\s*(?P<kind>GENR|IDENT|LHS)\s+(?P<lhs>[A-Za-z][A-Za-z0-9_]*)\s*=(?P<rhs>.*)$",
    re.IGNORECASE,
)
_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9_]*")
_RESERVED_TOKENS = frozenset({
    "GENR",
    "IDENT",
    "LHS",
    "EQ",
    "MODEQ",
    "FSR",
    "C",
    "T",
    "RHO",
    "ABS",
    "EXP",
    "LOG",
    "MIN",
    "MAX",
    "SQRT",
    "SIN",
    "COS",
    "TAN",
    "INT",
})


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Rank unsupported fppy statements by parity hard-fail impact and emit "
            "support-gap mapping artifacts."
        ),
    )
    parser.add_argument("--run-dir", required=True, help="Parity run directory")
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Optional output directory (defaults to the parity run directory).",
    )
    return parser.parse_args(argv)


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must be a JSON object")
    return payload


def _resolve_paths(run_dir: Path) -> tuple[Path, Path, Path, Path | None]:
    run_dir = Path(run_dir)
    nested_report = run_dir / "work_fppy" / "fppy_report.json"
    direct_report = run_dir / "fppy_report.json"

    if nested_report.exists():
        parity_run_dir = run_dir
        work_fppy_dir = run_dir / "work_fppy"
        report_path = nested_report
    elif direct_report.exists():
        parity_run_dir = run_dir.parent
        work_fppy_dir = run_dir
        report_path = direct_report
    else:
        raise FileNotFoundError(
            f"Could not find fppy_report.json under {run_dir} "
            "(expected <run-dir>/work_fppy/fppy_report.json or <run-dir>/fppy_report.json)"
        )

    fminput_candidates = (
        work_fppy_dir / "fminput.txt",
        parity_run_dir / "bundle" / "fminput.txt",
        parity_run_dir / "fminput.txt",
    )
    fminput_path = next((path for path in fminput_candidates if path.exists()), None)
    return parity_run_dir, work_fppy_dir, report_path, fminput_path


def _normalize_statement(value: str) -> str:
    return " ".join(str(value).replace("\n", " ").split())


def _eq_number_from_statement(statement: str) -> int | None:
    eq_match = _EQ_NUM_RE.match(statement)
    if eq_match:
        return int(eq_match.group("number"))
    modeq_match = _MODEQ_NUM_RE.match(statement)
    if modeq_match:
        return int(modeq_match.group("number"))
    return None


def _guess_lhs_from_statement(command: str, statement: str) -> str:
    text = statement.strip()
    upper_command = command.strip().upper()
    regexes: tuple[re.Pattern[str], ...]
    if upper_command == "EQ":
        regexes = (_EQ_LHS_RE, _EQ_NO_NUM_LHS_RE)
    elif upper_command == "MODEQ":
        regexes = (_MODEQ_LHS_RE, _MODEQ_NO_NUM_LHS_RE)
    elif upper_command == "FSR":
        regexes = (_FSR_LHS_RE,)
    else:
        regexes = ()

    for regex in regexes:
        match = regex.match(text)
        if match:
            return str(match.group("name"))

    assignment_match = _ASSIGNMENT_LHS_RE.search(text)
    if assignment_match:
        return str(assignment_match.group("name"))
    return ""


def _guess_lhs_from_context(line_no: int, fminput_lines: list[str]) -> str:
    if line_no <= 0 or not fminput_lines:
        return ""

    start_idx = max(0, line_no - 1)
    end_idx = min(len(fminput_lines), start_idx + 10)

    for idx in range(start_idx, end_idx):
        line = fminput_lines[idx]
        if idx > start_idx and _BLOCK_START_RE.match(line):
            break
        match = _LHS_ASSIGN_RE.match(line)
        if match:
            return str(match.group("name"))
    return ""


def _load_hard_fail_counts(hardfails_csv: Path) -> tuple[dict[str, int], dict[str, Counter[str]]]:
    var_totals: Counter[str] = Counter()
    var_reasons: dict[str, Counter[str]] = defaultdict(Counter)
    with hardfails_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            variable = str(row.get("variable", "")).strip()
            reason = str(row.get("reason", "")).strip()
            if not variable:
                continue
            var_totals[variable] += 1
            if reason:
                var_reasons[variable][reason] += 1
    return dict(var_totals), var_reasons


def _format_reason_counts(counter: Counter[str]) -> str:
    if not counter:
        return ""
    parts = [f"{reason}={int(count)}" for reason, count in counter.most_common()]
    return "; ".join(parts)


def _pick_lhs_guess(
    context_lhs: str,
    preferred_statement_lhs: str,
    hard_fail_totals: dict[str, int],
) -> str:
    if context_lhs and context_lhs in hard_fail_totals:
        return context_lhs
    if preferred_statement_lhs and preferred_statement_lhs in hard_fail_totals:
        return preferred_statement_lhs
    if context_lhs:
        return context_lhs
    return preferred_statement_lhs


def _ensure_hardfails_csv(parity_run_dir: Path) -> Path:
    hardfails_csv = parity_run_dir / "triage_hardfails.csv"
    if hardfails_csv.exists():
        return hardfails_csv
    triage_parity_hardfails(parity_run_dir)
    if not hardfails_csv.exists():
        raise FileNotFoundError(f"Failed to generate hard-fail triage CSV at {hardfails_csv}")
    return hardfails_csv


def _parse_fminput_statements(lines: list[str]) -> list[tuple[int, str]]:
    statements: list[tuple[int, str]] = []
    current: list[str] = []
    start_line = 0

    for line_no, raw_line in enumerate(lines, start=1):
        line = raw_line.rstrip("\n")
        stripped = line.strip()
        if not current and (not stripped or stripped.startswith("@")):
            continue
        if not current:
            start_line = line_no
        current.append(line)
        if ";" in line:
            statements.append((start_line, "\n".join(current)))
            current = []
            start_line = 0

    if current:
        statements.append((start_line if start_line > 0 else len(lines), "\n".join(current)))
    return statements


def _classify_unsupported_command(statement: str) -> tuple[str | None, int | None]:
    text = statement.strip()
    if not text:
        return None, None

    modeq_match = _MODEQ_NUM_RE.match(text)
    if modeq_match:
        return "MODEQ", int(modeq_match.group("number"))

    eq_match = _EQ_NUM_RE.match(text)
    if eq_match:
        eq_number = int(eq_match.group("number"))
        rest = text[eq_match.end() :].strip()
        if rest.startswith("FSR") or re.search(r"\bFSR\b", rest):
            return "FSR", eq_number
        return "EQ", eq_number

    if re.match(r"^\s*FSR\b", text, re.IGNORECASE):
        return "FSR", None

    return None, None


def _lhs_from_lhs_statement(statement: str) -> str:
    match = _LHS_ASSIGN_RE.match(statement)
    if not match:
        return ""
    return str(match.group("name"))


def _extract_assignment(statement: str) -> tuple[str, set[str]] | None:
    match = _ASSIGNMENT_RE.match(statement)
    if not match:
        return None
    lhs = str(match.group("lhs")).upper()
    rhs = str(match.group("rhs"))
    rhs_vars: set[str] = set()
    for token in _TOKEN_RE.findall(rhs):
        name = token.upper()
        if name == lhs:
            continue
        if name in _RESERVED_TOKENS:
            continue
        rhs_vars.add(name)
    return lhs, rhs_vars


def _build_dependency_maps(
    statements: list[tuple[int, str]],
) -> tuple[dict[str, set[str]], dict[int, str]]:
    normalized = [(line_no, _normalize_statement(text)) for line_no, text in statements]
    reverse_deps: dict[str, set[str]] = defaultdict(set)
    eq_lhs_by_number: dict[int, str] = {}

    for index, (_line, statement) in enumerate(normalized):
        command, eq_number = _classify_unsupported_command(statement)
        if command == "EQ" and eq_number is not None and index + 1 < len(normalized):
            lhs_name = _lhs_from_lhs_statement(normalized[index + 1][1])
            if lhs_name:
                eq_lhs_by_number[int(eq_number)] = lhs_name.upper()

        assignment = _extract_assignment(statement)
        if assignment is None:
            continue
        lhs, rhs_vars = assignment
        for rhs_var in rhs_vars:
            reverse_deps[rhs_var].add(lhs)
    return dict(reverse_deps), eq_lhs_by_number


def _derive_unsupported_entries_from_fminput(
    fminput_lines: list[str],
    unsupported_counts: dict[str, int],
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    taken: Counter[str] = Counter()
    for index, raw_line in enumerate(fminput_lines):
        stripped = raw_line.strip()
        if not stripped:
            continue
        command: str | None = None
        if _EQ_NUM_RE.match(stripped):
            command = "EQ"
        elif _MODEQ_NUM_RE.match(stripped):
            command = "MODEQ"
        elif re.match(r"^FSR\b", stripped, re.IGNORECASE):
            command = "FSR"
        if command is None:
            continue

        limit = int(unsupported_counts.get(command, 0))
        if limit <= 0 or taken[command] >= limit:
            continue

        chunk: list[str] = [raw_line]
        if ";" not in raw_line:
            for follow in fminput_lines[index + 1 :]:
                chunk.append(follow)
                if ";" in follow:
                    break
        statement = _normalize_statement("\n".join(chunk))
        eq_number = _eq_number_from_statement(statement)
        taken[command] += 1
        selected.append({
            "line": index + 1,
            "command": command,
            "statement": statement,
            "eq_number": int(eq_number) if eq_number is not None else None,
        })
    return selected


def _derive_unsupported_entries_from_examples(raw_examples: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_examples, list):
        return []
    out: list[dict[str, Any]] = []
    for item in raw_examples:
        if not isinstance(item, dict):
            continue
        line_raw = item.get("line")
        line_no = int(line_raw) if isinstance(line_raw, int) else 0
        command = str(item.get("command", "")).strip().upper()
        statement = _normalize_statement(str(item.get("statement", "")).strip())
        out.append({
            "line": line_no,
            "command": command,
            "statement": statement,
            "eq_number": _eq_number_from_statement(statement),
        })
    return out


def _merge_unsupported_entries(
    derived: list[dict[str, Any]],
    examples: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[tuple[int, str, str]] = set()
    for item in [*derived, *examples]:
        line_no = int(item.get("line", 0) or 0)
        command = str(item.get("command", "")).strip().upper()
        statement = _normalize_statement(str(item.get("statement", "")).strip())
        key = (line_no, command, statement)
        if key in seen:
            continue
        seen.add(key)
        merged.append({
            "line": line_no,
            "command": command,
            "statement": statement,
            "eq_number": item.get("eq_number"),
        })
    return sorted(
        merged,
        key=lambda row: (
            int(row.get("line", 0) or 0),
            str(row.get("command", "")),
            str(row.get("statement", "")),
        ),
    )


def _aggregate_reason_counts(
    variables: list[str],
    hard_fail_reasons: dict[str, Counter[str]],
) -> Counter[str]:
    out: Counter[str] = Counter()
    for variable in variables:
        out.update(hard_fail_reasons.get(variable, Counter()))
    return out


def _estimate_impact(
    seed_variables: list[str],
    reverse_deps: dict[str, set[str]],
    hard_fail_totals: dict[str, int],
    hard_fail_reasons: dict[str, Counter[str]],
) -> tuple[int, list[str], str]:
    if not seed_variables:
        return 0, [], ""

    queue: deque[tuple[str, int]] = deque()
    visited: set[str] = set()
    for seed in seed_variables:
        key = seed.upper().strip()
        if not key:
            continue
        if key in visited:
            continue
        visited.add(key)
        queue.append((key, 0))

    impacted: set[str] = set()
    max_depth = 12
    while queue:
        node, depth = queue.popleft()
        if node in hard_fail_totals:
            impacted.add(node)
        if depth >= max_depth:
            continue
        for child in sorted(reverse_deps.get(node, ())):
            if child in visited:
                continue
            visited.add(child)
            queue.append((child, depth + 1))

    impacted_sorted = sorted(
        impacted,
        key=lambda name: (-int(hard_fail_totals.get(name, 0)), name),
    )
    impact_total = sum(int(hard_fail_totals.get(name, 0)) for name in impacted_sorted)
    impact_reasons = _format_reason_counts(
        _aggregate_reason_counts(impacted_sorted, hard_fail_reasons)
    )
    return impact_total, impacted_sorted, impact_reasons


def _render_top_markdown(
    rows: list[dict[str, str]],
    *,
    run_dir: Path,
    unsupported_total: int,
    unsupported_examples_count: int,
    hard_fail_totals: dict[str, int],
    seed_diagnostics: dict[str, object] | None = None,
) -> str:
    ranked = sorted(
        rows,
        key=lambda row: (
            -int(row.get("estimated_hard_fail_cell_count", "0") or 0),
            -int(row.get("hard_fail_cell_count", "0") or 0),
            str(row.get("unsupported_command", "")),
            int(row.get("unsupported_line", "0") or 0),
            str(row.get("lhs_variable_guess", "")),
            str(row.get("unsupported_statement", "")),
        ),
    )[:20]

    top_hard_fail_vars = sorted(
        hard_fail_totals.items(),
        key=lambda item: (-int(item[1]), str(item[0])),
    )[:6]
    top_var_names = [name for name, _count in top_hard_fail_vars]

    lines = [
        "# Support Gap Top 20 (Hard-Fail Impact)",
        "",
        f"- run_dir: `{run_dir}`",
        f"- unsupported statements (summary): {unsupported_total}",
        f"- unsupported statements analyzed: {unsupported_examples_count}",
        f"- leading hard-fail vars: {', '.join(top_var_names) if top_var_names else 'none'}",
        "",
    ]
    if isinstance(seed_diagnostics, dict) and seed_diagnostics:
        lines.extend([
            "## OUTSIDE Seed Diagnostics",
            "",
            (
                "- solve_outside_seeded/inspected/candidate: "
                f"{seed_diagnostics.get('solve_outside_seeded_cells')}/"
                f"{seed_diagnostics.get('solve_outside_seed_inspected_cells')}/"
                f"{seed_diagnostics.get('solve_outside_seed_candidate_cells')}"
            ),
            (
                "- eq_backfill_outside_post_seed seeded/inspected/candidate: "
                f"{seed_diagnostics.get('eq_backfill_outside_post_seed_cells')}/"
                f"{seed_diagnostics.get('eq_backfill_outside_post_seed_inspected_cells')}/"
                f"{seed_diagnostics.get('eq_backfill_outside_post_seed_candidate_cells')}"
            ),
            "",
        ])
    if not ranked:
        lines.append("No unsupported statements found for analysis.")
        return "\n".join(lines) + "\n"

    for idx, row in enumerate(ranked, start=1):
        direct_reasons = row.get("hard_fail_reason_counts", "") or "none"
        estimated_reasons = row.get("estimated_hard_fail_reason_counts", "") or "none"
        lhs = row.get("lhs_variable_guess", "") or "<unknown>"
        line = row.get("unsupported_line", "") or "?"
        lines.append(
            f"{idx}. est_impact={row.get('estimated_hard_fail_cell_count', '0')} | "
            f"direct_impact={row.get('hard_fail_cell_count', '0')} | "
            f"{row.get('unsupported_command', '')} line {line} | lhs={lhs}"
        )
        lines.append(f"   direct_reasons={direct_reasons}")
        lines.append(f"   estimated_reasons={estimated_reasons}")
        touched = row.get("estimated_hard_fail_vars", "")
        if touched:
            lines.append(f"   touched={touched}")
        lines.append(f"   `{row.get('unsupported_statement', '')}`")

    spotlight = [
        row
        for row in ranked
        if any(var and var in (row.get("estimated_hard_fail_vars", "")) for var in top_var_names)
    ][:5]
    if spotlight:
        lines.append("")
        lines.append("## Top 5 Statements Touching Leading Hard-Fail Vars")
        lines.append("")
        for idx, row in enumerate(spotlight, start=1):
            lines.append(
                f"{idx}. est_impact={row.get('estimated_hard_fail_cell_count', '0')} | "
                f"line {row.get('unsupported_line', '?')} | "
                f"{row.get('unsupported_command', '')} | "
                f"touched={row.get('estimated_hard_fail_vars', '')}"
            )
    lines.append("")
    return "\n".join(lines)


def build_support_gap_map(run_dir: Path, *, out_dir: Path | None = None) -> tuple[Path, Path]:
    parity_run_dir, _work_fppy_dir, report_path, fminput_path = _resolve_paths(Path(run_dir))
    out_root = Path(out_dir) if out_dir else parity_run_dir
    out_root.mkdir(parents=True, exist_ok=True)

    hardfails_csv = _ensure_hardfails_csv(parity_run_dir)
    hard_fail_totals, hard_fail_reasons = _load_hard_fail_counts(hardfails_csv)

    report = _load_json(report_path)
    summary = report.get("summary")
    unsupported_total = 0
    unsupported_counts: dict[str, int] = {}
    seed_diagnostics: dict[str, object] = {}
    if isinstance(summary, dict):
        unsupported_raw = summary.get("unsupported")
        if isinstance(unsupported_raw, int):
            unsupported_total = int(unsupported_raw)
        raw_counts = summary.get("unsupported_counts")
        if isinstance(raw_counts, dict):
            unsupported_counts = {
                str(key).upper(): int(value)
                for key, value in raw_counts.items()
                if isinstance(value, int) and int(value) > 0
            }
        for key in (
            "solve_outside_seeded_cells",
            "solve_outside_seed_inspected_cells",
            "solve_outside_seed_candidate_cells",
            "eq_backfill_outside_post_seed_cells",
            "eq_backfill_outside_post_seed_inspected_cells",
            "eq_backfill_outside_post_seed_candidate_cells",
        ):
            if key in summary:
                seed_diagnostics[key] = summary.get(key)

    fminput_lines: list[str] = []
    fminput_statements: list[tuple[int, str]] = []
    if fminput_path and fminput_path.exists():
        fminput_lines = fminput_path.read_text(encoding="utf-8", errors="replace").splitlines()
        fminput_statements = _parse_fminput_statements(fminput_lines)

    reverse_deps, eq_lhs_by_number = _build_dependency_maps(fminput_statements)
    derived = _derive_unsupported_entries_from_fminput(fminput_lines, unsupported_counts)
    examples = _derive_unsupported_entries_from_examples(report.get("unsupported_examples"))
    unsupported_rows = _merge_unsupported_entries(derived, examples)

    rows: list[dict[str, str]] = []
    for item in unsupported_rows:
        line_no = int(item.get("line", 0) or 0)
        command = str(item.get("command", "")).strip().upper()
        statement = _normalize_statement(str(item.get("statement", "")).strip())
        eq_number = item.get("eq_number")
        eq_number_i = (
            int(eq_number) if isinstance(eq_number, int) else _eq_number_from_statement(statement)
        )

        statement_lhs = _guess_lhs_from_statement(command, statement)
        context_lhs = _guess_lhs_from_context(line_no, fminput_lines)
        eq_lhs = eq_lhs_by_number.get(eq_number_i) if eq_number_i is not None else ""
        preferred_lhs = eq_lhs or statement_lhs
        lhs_guess = _pick_lhs_guess(context_lhs, preferred_lhs, hard_fail_totals).upper()

        direct_count = int(hard_fail_totals.get(lhs_guess, 0)) if lhs_guess else 0
        direct_reasons = (
            _format_reason_counts(hard_fail_reasons.get(lhs_guess, Counter()))
            if direct_count > 0
            else ""
        )

        seed_vars: list[str] = []
        for candidate in (lhs_guess, statement_lhs.upper(), str(eq_lhs).upper()):
            token = candidate.strip()
            if not token or token in seed_vars:
                continue
            seed_vars.append(token)
        estimated_total, estimated_vars, estimated_reasons = _estimate_impact(
            seed_vars,
            reverse_deps,
            hard_fail_totals,
            hard_fail_reasons,
        )

        rows.append({
            "unsupported_line": str(line_no if line_no > 0 else ""),
            "unsupported_command": command,
            "unsupported_statement": statement,
            "lhs_variable_guess": lhs_guess,
            "hard_fail_var_hit": "true" if direct_count > 0 else "false",
            "hard_fail_reason_counts": direct_reasons,
            "hard_fail_cell_count": str(direct_count),
            "eq_number": str(eq_number_i if eq_number_i is not None else ""),
            "estimated_hard_fail_cell_count": str(int(estimated_total)),
            "estimated_hard_fail_vars": ",".join(estimated_vars),
            "estimated_hard_fail_reason_counts": estimated_reasons,
        })

    map_csv_path = out_root / "support_gap_map.csv"
    with map_csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "unsupported_line",
                "unsupported_command",
                "unsupported_statement",
                "lhs_variable_guess",
                "hard_fail_var_hit",
                "hard_fail_reason_counts",
                "hard_fail_cell_count",
                "eq_number",
                "estimated_hard_fail_cell_count",
                "estimated_hard_fail_vars",
                "estimated_hard_fail_reason_counts",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    top_md_path = out_root / "support_gap_top.md"
    top_md_path.write_text(
        _render_top_markdown(
            rows,
            run_dir=parity_run_dir,
            unsupported_total=unsupported_total,
            unsupported_examples_count=len(rows),
            hard_fail_totals=hard_fail_totals,
            seed_diagnostics=seed_diagnostics,
        ),
        encoding="utf-8",
    )
    return map_csv_path, top_md_path


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    map_csv_path, top_md_path = build_support_gap_map(
        Path(args.run_dir),
        out_dir=Path(args.out_dir) if args.out_dir else None,
    )
    print(f"Wrote {map_csv_path}")
    print(f"Wrote {top_md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
