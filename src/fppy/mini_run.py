"""Mini-run execution helpers with per-step diagnostics."""

from __future__ import annotations

import math
import re
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from fppy.executor import build_execution_plan, execute_plan, parse_smpl_statement
from fppy.expressions import EvalContext
from fppy.io.legacy_data import (
    generate_smpl_period_index,
    parse_fmdata_file,
    parse_fmexog_file,
    parse_fmexog_text,
)
from fppy.parser import FPCommand, FPCommandRecord
from fppy.runtime_commands import (
    ExogenousCommand,
    ExtrapolateCommand,
    InputCommand,
    LoadDataCommand,
    PrintVarCommand,
    SetYyToYCommand,
    parse_runtime_command,
)


@dataclass(frozen=True)
class MiniRunIssue:
    line_number: int
    statement: str
    error: str


@dataclass(frozen=True)
class MiniRunUnsupportedCommand:
    line_number: int
    command: str
    statement: str


@dataclass(frozen=True)
class MiniRunResult:
    frame: pd.DataFrame
    total_records: int
    planned_steps: int
    executed_steps: int
    failed_steps: int
    issues: tuple[MiniRunIssue, ...]
    unsupported_counts: dict[str, int]
    unsupported_examples: tuple[MiniRunUnsupportedCommand, ...] = field(default_factory=tuple)
    executed_line_numbers: tuple[int, ...] = field(default_factory=tuple)


_SUPPORTED_COMMANDS = {
    FPCommand.GENR,
    FPCommand.IDENT,
    FPCommand.LHS,
    FPCommand.CREATE,
    FPCommand.LOADDATA,
    FPCommand.CHANGEVAR,
    FPCommand.INPUT,
    FPCommand.PRINTVAR,
    FPCommand.EXOGENOUS,
    FPCommand.EXTRAPOLATE,
    FPCommand.SETYYTOY,
    # Workflow controls that do not mutate the runtime frame in mini-run mode.
    FPCommand.SPACE,
    FPCommand.SETUPEST,
    FPCommand.EST,
    FPCommand.TWOSLS,
    FPCommand.TEST,
    FPCommand.PRINTMODEL,
    FPCommand.PRINTNAMES,
    FPCommand.END,
    FPCommand.QUIT,
    FPCommand.SETUPSOLVE,
    FPCommand.SOLVE,
    FPCommand.SMPL,
    FPCommand.COMMENT,
}


def _unsupported_command_counts(records: Sequence[FPCommandRecord]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for record in records:
        if record.command not in _SUPPORTED_COMMANDS:
            counter[record.command.value] += 1

    return dict(counter)


def _unsupported_command_examples(
    records: Sequence[FPCommandRecord], *, limit: int = 20
) -> tuple[MiniRunUnsupportedCommand, ...]:
    examples: list[MiniRunUnsupportedCommand] = []
    for record in records:
        if record.command in _SUPPORTED_COMMANDS:
            continue
        examples.append(
            MiniRunUnsupportedCommand(
                line_number=record.line_number,
                command=record.command.value,
                statement=record.statement,
            )
        )
        if len(examples) >= limit:
            break
    return tuple(examples)


def _error_text(exc: Exception) -> str:
    if exc.args:
        return f"{type(exc).__name__}: {exc}"
    return type(exc).__name__


def _find_case_insensitive(directory: Path, filename: str) -> Path | None:
    direct = directory / filename
    if direct.exists():
        return direct
    wanted = filename.lower()
    for candidate in directory.iterdir():
        if candidate.name.lower() == wanted:
            return candidate
    return None


def _resolve_runtime_file(path: str, *, runtime_base_dir: Path | None) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate

    if runtime_base_dir is not None and runtime_base_dir.exists():
        resolved = _find_case_insensitive(runtime_base_dir, path)
        if resolved is not None:
            return resolved
        return runtime_base_dir / path

    cwd = Path.cwd()
    resolved = _find_case_insensitive(cwd, path)
    if resolved is not None:
        return resolved
    return cwd / path


def _merge_loaded_frame(base: pd.DataFrame, loaded: pd.DataFrame) -> pd.DataFrame:
    if loaded.empty:
        return base

    merged = base.reindex(base.index.union(loaded.index)).sort_index()
    for column in loaded.columns:
        merged.loc[loaded.index, column] = loaded[column]
    return merged


def _resolve_column_name(frame: pd.DataFrame, name: str) -> str:
    target = str(name).upper()
    for column in frame.columns:
        if str(column).upper() == target:
            return str(column)
    return str(name)


def _window_periods_in_frame(
    frame: pd.DataFrame,
    *,
    window_start: str,
    window_end: str,
) -> list[str]:
    window = generate_smpl_period_index(window_start, window_end)
    frame_periods = {str(period) for period in frame.index}
    return [str(period) for period in window if str(period) in frame_periods]


def _active_window_periods(
    frame: pd.DataFrame,
    *,
    active_window: tuple[str, str] | None,
) -> list[str]:
    if active_window is None:
        return [str(period) for period in frame.index]
    return _window_periods_in_frame(
        frame,
        window_start=active_window[0],
        window_end=active_window[1],
    )


def _first_finite(values: list[float]) -> float:
    for value in values:
        if math.isfinite(value):
            return float(value)
    return 0.0


def _apply_fmexog_rows(base: pd.DataFrame, rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return base

    working = base.copy(deep=True).sort_index()
    index_positions = {str(period): idx for idx, period in enumerate(working.index)}

    for _, row in rows.iterrows():
        variable = str(row.get("variable", "")).strip()
        if not variable:
            continue

        method_raw = row.get("method")
        method = "" if pd.isna(method_raw) else str(method_raw).strip().upper()
        # Legacy FAIR template CHANGEVAR blocks often omit the method token for
        # scalar updates; empirically (and per template intent) this means
        # CHGSAMEPCT rather than SAMEVALUE.
        if method == "":
            method = "CHGSAMEPCT"
        values_obj = row.get("values", ())
        if isinstance(values_obj, (tuple, list)):
            values = [float(value) for value in values_obj]
        else:
            try:
                values = [float(values_obj)]
            except (TypeError, ValueError):
                values = []
        if not values:
            continue

        periods = _window_periods_in_frame(
            working,
            window_start=str(row.get("window_start")),
            window_end=str(row.get("window_end")),
        )
        if not periods:
            continue

        column = _resolve_column_name(working, variable)
        if column not in working.columns:
            working[column] = pd.Series(index=working.index, dtype="float64")

        is_vector_raw = row.get("is_vector")
        is_vector = False if pd.isna(is_vector_raw) else bool(is_vector_raw)

        if is_vector:
            if len(values) == len(periods):
                working.loc[periods, column] = values
                continue

            if len(values) > 1:
                padded = values[: len(periods)]
                if len(padded) < len(periods):
                    padded.extend([padded[-1]] * (len(periods) - len(padded)))
                working.loc[periods, column] = padded
                continue

            scalar = float(values[0])
            working.loc[periods, column] = scalar
            continue

        # Scalar instructions: always treat the single parsed value as a scalar adjustment
        # according to method, even when the SMPL window is only one period long.
        scalar = float(values[0])
        if method in {"SAMEVALUE"}:
            working.loc[periods, column] = scalar
            continue

        if method in {"CHGSAMEABS", "CHGSAMEPCT"}:
            series = pd.to_numeric(working[column], errors="coerce")
            for period in periods:
                position = index_positions.get(period)
                prev_value = float("nan")
                if position is not None and position > 0:
                    prev_period = str(working.index[position - 1])
                    prev_value = float(series.loc[prev_period])
                current_value = (
                    float(series.loc[period]) if period in series.index else float("nan")
                )
                baseline = _first_finite([prev_value, current_value, 0.0])
                if method == "CHGSAMEABS":
                    updated = baseline + scalar
                else:
                    updated = baseline * (1.0 + scalar)
                working.at[period, column] = updated
                series.at[period] = updated
            continue

        # Unknown method: fall back to scalar fill.
        working.loc[periods, column] = scalar

    return working


def _apply_extrapolate_variables(
    frame: pd.DataFrame,
    *,
    variables: set[str],
    active_window: tuple[str, str] | None,
    include_all_columns: bool = False,
) -> pd.DataFrame:
    if not variables and not include_all_columns:
        return frame
    periods = _active_window_periods(frame, active_window=active_window)
    if not periods:
        return frame

    working = frame.copy(deep=True)
    index_positions = {str(period): idx for idx, period in enumerate(working.index)}
    variable_names: set[str] = set(str(name) for name in variables)
    if include_all_columns:
        variable_names.update(str(column) for column in working.columns)
    for variable in sorted(variable_names):
        column = _resolve_column_name(working, variable)
        if column not in working.columns:
            working[column] = pd.Series(index=working.index, dtype="float64")
        series = pd.to_numeric(working[column], errors="coerce")
        for period in periods:
            position = index_positions.get(period)
            if position is None:
                continue
            current = float(series.iat[position])
            if not math.isnan(current):
                continue
            if position <= 0:
                continue
            prev_value = float(series.iat[position - 1])
            if math.isnan(prev_value):
                continue
            series.iat[position] = prev_value
        working[column] = series

    return working


def _apply_setyytoy(
    frame: pd.DataFrame,
    *,
    active_window: tuple[str, str] | None,
) -> pd.DataFrame:
    source_column = _resolve_column_name(frame, "Y")
    if source_column not in frame.columns:
        return frame

    target_column = _resolve_column_name(frame, "YY")
    if target_column not in frame.columns:
        return frame
    periods = _active_window_periods(frame, active_window=active_window)
    if not periods:
        return frame

    source = pd.to_numeric(frame[source_column], errors="coerce")
    frame.loc[periods, target_column] = source.loc[periods]
    return frame


def _apply_runtime_preloads(
    records: Sequence[FPCommandRecord],
    *,
    data: pd.DataFrame,
    runtime_base_dir: Path | None,
    issues: list[MiniRunIssue],
    on_error: str,
) -> pd.DataFrame:
    output = data.copy(deep=True)
    active_window: tuple[str, str] | None = None
    exogenous_variables: set[str] = set()

    for record in records:
        if record.command == FPCommand.SMPL:
            try:
                window = parse_smpl_statement(record.statement)
            except Exception:
                window = None
            if window is not None:
                active_window = (str(window.start), str(window.end))
            continue

        parsed = parse_runtime_command(record)
        if isinstance(parsed, LoadDataCommand):
            try:
                file_path = _resolve_runtime_file(parsed.file, runtime_base_dir=runtime_base_dir)
                loaded = parse_fmdata_file(file_path)
                output = _merge_loaded_frame(output, loaded)
            except Exception as exc:  # pragma: no cover - explicit diagnostic path
                issues.append(MiniRunIssue(record.line_number, record.statement, _error_text(exc)))
                if on_error == "stop":
                    break
            continue

        if isinstance(parsed, InputCommand):
            try:
                file_path = _resolve_runtime_file(parsed.file, runtime_base_dir=runtime_base_dir)
                fmexog_rows = parse_fmexog_file(file_path)
                output = _apply_fmexog_rows(output, fmexog_rows)
            except Exception as exc:  # pragma: no cover - explicit diagnostic path
                issues.append(MiniRunIssue(record.line_number, record.statement, _error_text(exc)))
                if on_error == "stop":
                    break
            continue

        if isinstance(parsed, ExogenousCommand):
            exogenous_variables.add(parsed.variable.upper())
            continue

        if isinstance(parsed, ExtrapolateCommand):
            try:
                output = _apply_extrapolate_variables(
                    output,
                    variables=exogenous_variables,
                    active_window=active_window,
                    include_all_columns=True,
                )
            except Exception as exc:  # pragma: no cover - explicit diagnostic path
                issues.append(MiniRunIssue(record.line_number, record.statement, _error_text(exc)))
                if on_error == "stop":
                    break

    return output


def _write_printvar_loadformat(
    frame: pd.DataFrame,
    *,
    output_path: Path,
    variables: tuple[str, ...],
    active_window: tuple[str, str] | None = None,
) -> None:
    def _format_load_value(value: float) -> str:
        numeric = float(value)
        if not math.isfinite(numeric):
            return "-99"
        if numeric == 0.0:
            return " 0.00000000000E+00"
        abs_value = abs(numeric)
        exponent = math.floor(math.log10(abs_value)) + 1
        mantissa = numeric / (10.0**exponent)
        if abs(mantissa) >= 1.0:
            mantissa /= 10.0
            exponent += 1
        return f"{mantissa: .11f}E{exponent:+03d}"

    working = frame.sort_index()
    if active_window is not None:
        start, end = active_window
        try:
            window_index = generate_smpl_period_index(start, end)
        except Exception:
            window_index = None
        if window_index is not None and len(window_index) > 0:
            working = working.reindex(window_index)
    selected: list[str]
    if variables:
        selected = []
        for variable in variables:
            resolved = _resolve_column_name(working, variable)
            if resolved in working.columns:
                selected.append(resolved)
    else:
        selected = [str(column) for column in working.columns]

    if not selected:
        output_path.write_text("", encoding="utf-8")
        return

    first_period = str(working.index[0])
    last_period = str(working.index[-1])
    lines: list[str] = [f" SMPL    {first_period}   {last_period} ;"]
    for variable in selected:
        values = pd.to_numeric(working[variable], errors="coerce")
        lines.append(f" LOAD {variable:<8} ;")
        chunk: list[str] = []
        for value in values:
            if pd.isna(value):
                chunk.append("-99")
            else:
                chunk.append(_format_load_value(float(value)))
            if len(chunk) >= 4:
                lines.append(f"  {' '.join(chunk)}")
                chunk = []
        if chunk:
            lines.append(f"  {' '.join(chunk)}")
        lines.append(" 'END' ")
    lines.append(" END;")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _infer_printvar_variable_order(
    frame: pd.DataFrame, *, runtime_base_dir: Path | None
) -> tuple[str, ...]:
    """Infer fp.exe-like implicit PRINTVAR ordering when variable list is omitted."""
    if runtime_base_dir is None:
        return ()

    # Prefer the variable ordering map emitted in fmout*. This best matches fp.exe's
    # implicit PRINTVAR ordering for PABEV.TXT (especially for generated variables).
    fmout_path = _find_case_insensitive(runtime_base_dir, "fmout.txt")
    if fmout_path is None:
        fmout_path = _find_case_insensitive(runtime_base_dir, "fmout_coefs.txt")
    fmout_re = re.compile(r"(?P<var>[A-Za-z][A-Za-z0-9_]*)\s+(?P<idx>\d{1,4})")
    var_to_idx: dict[str, int] = {}
    if fmout_path is not None and fmout_path.exists():
        for raw in fmout_path.read_text(encoding="utf-8", errors="replace").splitlines():
            pairs = [(m.group("var").upper(), int(m.group("idx"))) for m in fmout_re.finditer(raw)]
            # Heuristic: the variable map lines contain multiple (var, idx) pairs.
            if len(pairs) < 2:
                continue
            for var, idx in pairs:
                # Exclude obvious non-variable keys and out-of-range indices.
                if idx <= 0 or idx > 500:
                    continue
                if var in {
                    "MAXITERS",
                    "MINITERS",
                    "MAXCHECK",
                    "MAXVAR",
                    "MAXS",
                    "MAXCOEF",
                    "MAXFSR",
                }:
                    continue
                existing = var_to_idx.get(var)
                if existing is None or idx < existing:
                    var_to_idx[var] = idx

    cols_upper = {str(col).upper(): str(col) for col in frame.columns}
    if var_to_idx:
        ordered_vars = [var for var, _ in sorted(var_to_idx.items(), key=lambda item: item[1])]
        selected = [cols_upper[var] for var in ordered_vars if var in cols_upper]
        if selected:
            for col in frame.columns:
                name = str(col)
                if name not in selected:
                    selected.append(name)
            return tuple(selected)

    # Fallback: legacy LOAD order from the template blocks (fmdata/fmage/fmexog).
    load_re = re.compile(r"^\s*LOAD\s+(?P<name>[A-Za-z0-9_]+)\b", re.IGNORECASE)
    seen: set[str] = set()
    ordered: list[str] = []
    for name in ("fmdata.txt", "fmage.txt", "fmexog.txt"):
        path = runtime_base_dir / name
        if not path.exists():
            continue
        for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
            match = load_re.match(raw)
            if match is None:
                continue
            var = str(match.group("name")).strip().upper()
            if not var or var in seen:
                continue
            seen.add(var)
            ordered.append(var)

    if not ordered:
        return ()

    selected: list[str] = []
    for var in ordered:
        col = cols_upper.get(var)
        if col is not None:
            selected.append(col)

    for col in frame.columns:
        name = str(col)
        if name not in selected:
            selected.append(name)
    return tuple(selected)


def _apply_runtime_post_commands(
    records: Sequence[FPCommandRecord],
    *,
    frame: pd.DataFrame,
    runtime_base_dir: Path | None,
    issues: list[MiniRunIssue],
    on_error: str,
) -> None:
    def _default_printvar_variables() -> tuple[str, ...]:
        return _infer_printvar_variable_order(frame, runtime_base_dir=runtime_base_dir)

    active_window: tuple[str, str] | None = None
    active_window_plan: object | None = None
    assignment_buffer: list[FPCommandRecord] = []

    def _append_issue(record: FPCommandRecord, exc: Exception) -> bool:
        issues.append(MiniRunIssue(record.line_number, record.statement, _error_text(exc)))
        return on_error == "stop"

    def _flush_assignment_buffer() -> bool:
        if not assignment_buffer:
            return False
        try:
            plan = build_execution_plan(
                assignment_buffer,
                initial_window=active_window_plan,
            )
        except Exception as exc:  # pragma: no cover - explicit diagnostic path
            stop = False
            for record in assignment_buffer:
                stop = _append_issue(record, exc)
                if stop:
                    break
            assignment_buffer.clear()
            return stop

        for step in plan:
            try:
                execute_plan((step,), data=frame, inplace=True)
            except Exception as exc:  # pragma: no cover - explicit diagnostic path
                issues.append(MiniRunIssue(step.line_number, step.statement, _error_text(exc)))
                if on_error == "stop":
                    assignment_buffer.clear()
                    return True

        assignment_buffer.clear()
        return False

    for record in records:
        if record.command == FPCommand.SMPL:
            if _flush_assignment_buffer():
                break
            try:
                window = parse_smpl_statement(record.statement)
            except Exception:
                window = None
            if window is not None:
                active_window = (str(window.start), str(window.end))
                active_window_plan = window
            continue

        if record.command in {FPCommand.GENR, FPCommand.IDENT, FPCommand.LHS, FPCommand.CREATE}:
            assignment_buffer.append(record)
            continue

        parsed = parse_runtime_command(record)
        if isinstance(parsed, SetYyToYCommand):
            if _flush_assignment_buffer():
                break
            try:
                _apply_setyytoy(
                    frame,
                    active_window=active_window,
                )
            except Exception as exc:  # pragma: no cover - explicit diagnostic path
                issues.append(MiniRunIssue(record.line_number, record.statement, _error_text(exc)))
                if on_error == "stop":
                    break
            continue

        if not isinstance(parsed, PrintVarCommand):
            continue
        if not parsed.fileout:
            continue
        if _flush_assignment_buffer():
            break
        try:
            output_path = _resolve_runtime_file(parsed.fileout, runtime_base_dir=runtime_base_dir)
            if parsed.loadformat:
                variables = parsed.variables
                if not variables:
                    variables = _default_printvar_variables()
                _write_printvar_loadformat(
                    frame,
                    output_path=output_path,
                    variables=variables,
                    active_window=active_window,
                )
            else:
                frame.to_csv(output_path)
        except Exception as exc:  # pragma: no cover - explicit diagnostic path
            issues.append(MiniRunIssue(record.line_number, record.statement, _error_text(exc)))
            if on_error == "stop":
                break

    _flush_assignment_buffer()


def replay_runtime_post_commands(
    records: Sequence[FPCommandRecord],
    *,
    frame: pd.DataFrame,
    runtime_base_dir: Path | None,
    on_error: str = "continue",
    line_number_min: int | None = None,
) -> tuple[MiniRunIssue, ...]:
    selected_records: Sequence[FPCommandRecord]
    if line_number_min is None:
        selected_records = records
    else:
        selected_records = [
            record for record in records if int(record.line_number) >= int(line_number_min)
        ]
        # Preserve the active SMPL context at the replay entry point so
        # post-SOLVE commands (for example SETYYTOY) operate on the same
        # window that was active at SOLVE time.
        prior_smpl: FPCommandRecord | None = None
        for record in records:
            if int(record.line_number) >= int(line_number_min):
                break
            if record.command == FPCommand.SMPL:
                prior_smpl = record
        if prior_smpl is not None:
            selected_records = [prior_smpl, *selected_records]

    issues: list[MiniRunIssue] = []
    _apply_runtime_post_commands(
        selected_records,
        frame=frame,
        runtime_base_dir=runtime_base_dir,
        issues=issues,
        on_error=on_error,
    )
    return tuple(issues)


def run_mini_run(
    records: Sequence[FPCommandRecord],
    data: pd.DataFrame,
    max_steps: int | None = None,
    on_error: str = "continue",
    eval_context: EvalContext | None = None,
    runtime_base_dir: Path | None = None,
) -> MiniRunResult:
    if on_error not in {"continue", "stop"}:
        raise ValueError("on_error must be 'continue' or 'stop'")

    if max_steps is not None and (not isinstance(max_steps, int) or max_steps <= 0):
        raise ValueError("max_steps must be a positive integer when provided")

    issues: list[MiniRunIssue] = []
    planned_steps = sum(
        1
        for record in records
        if record.command in {FPCommand.GENR, FPCommand.IDENT, FPCommand.LHS, FPCommand.CREATE}
    )

    output = data.copy(deep=True)
    active_window_tuple: tuple[str, str] | None = None
    active_window_plan: object | None = None
    assignment_buffer: list[FPCommandRecord] = []
    exogenous_variables: set[str] = set()

    executed_steps = 0
    executed_line_numbers: list[int] = []

    def _append_issue_for_record(record: FPCommandRecord, exc: Exception) -> bool:
        issues.append(MiniRunIssue(record.line_number, record.statement, _error_text(exc)))
        return on_error == "stop"

    def _max_steps_reached() -> bool:
        if max_steps is None:
            return False
        return executed_steps >= int(max_steps)

    def _flush_assignment_buffer() -> bool:
        nonlocal output
        nonlocal assignment_buffer
        nonlocal executed_steps
        if not assignment_buffer:
            return False
        if _max_steps_reached():
            assignment_buffer = []
            return False

        # Preserve the existing executor behavior: reorder definitions within the
        # current SMPL window, but never reorder across non-assignment runtime commands.
        try:
            plan = build_execution_plan(
                assignment_buffer,
                initial_window=active_window_plan,
            )
        except Exception as exc:  # pragma: no cover - explicit diagnostic path
            for record in assignment_buffer:
                if _append_issue_for_record(record, exc):
                    assignment_buffer = []
                    return True
            assignment_buffer = []
            return False

        for step in plan:
            if _max_steps_reached():
                break
            try:
                output = execute_plan(
                    (step,),
                    data=output,
                    inplace=False,
                    eval_context=eval_context,
                )
                executed_steps += 1
                executed_line_numbers.append(step.line_number)
            except Exception as exc:  # pragma: no cover - explicit broad catch for diagnostics
                executed_steps += 1
                executed_line_numbers.append(step.line_number)
                issues.append(MiniRunIssue(step.line_number, step.statement, _error_text(exc)))
                if on_error == "stop":
                    assignment_buffer = []
                    return True

        assignment_buffer = []
        return False

    for record in records:
        if record.command == FPCommand.SMPL:
            if _flush_assignment_buffer():
                break
            try:
                window = parse_smpl_statement(record.statement)
            except Exception:
                active_window_tuple = None
                active_window_plan = None
            else:
                active_window_tuple = (str(window.start), str(window.end))
                active_window_plan = window
                # FP decks assume SMPL windows are "live" even when data hasn't
                # been loaded for that horizon yet (for example, EXTRAPOLATE
                # before LOADDATA/INPUT). Ensure the runtime frame has rows for
                # the active window so subsequent commands can operate on it.
                window_index = generate_smpl_period_index(window.start, window.end)
                output = output.reindex(output.index.union(window_index)).sort_index()
            continue

        if record.command in {FPCommand.GENR, FPCommand.IDENT, FPCommand.LHS, FPCommand.CREATE}:
            assignment_buffer.append(record)
            continue

        if record.command == FPCommand.CHANGEVAR:
            if _flush_assignment_buffer():
                break
            if active_window_tuple is None:
                if _append_issue_for_record(
                    record,
                    ValueError("CHANGEVAR encountered without an active SMPL window"),
                ):
                    break
                continue
            try:
                smpl_start, smpl_end = active_window_tuple
                fmexog_rows = parse_fmexog_text(
                    f"SMPL {smpl_start} {smpl_end};\n{record.statement}\n"
                )
                output = _apply_fmexog_rows(output, fmexog_rows)
            except Exception as exc:  # pragma: no cover - explicit diagnostic path
                if _append_issue_for_record(record, exc):
                    break
            continue

        # Runtime commands must execute in-order relative to assignments so
        # commands like EXTRAPOLATE see the derived series created earlier.
        parsed = parse_runtime_command(record)
        if parsed is None:
            continue

        if _flush_assignment_buffer():
            break

        if isinstance(parsed, LoadDataCommand):
            try:
                file_path = _resolve_runtime_file(parsed.file, runtime_base_dir=runtime_base_dir)
                loaded = parse_fmdata_file(file_path)
                output = _merge_loaded_frame(output, loaded)
            except Exception as exc:  # pragma: no cover - explicit diagnostic path
                if _append_issue_for_record(record, exc):
                    break
            continue

        if isinstance(parsed, InputCommand):
            try:
                file_path = _resolve_runtime_file(parsed.file, runtime_base_dir=runtime_base_dir)
                fmexog_rows = parse_fmexog_file(file_path)
                output = _apply_fmexog_rows(output, fmexog_rows)
            except Exception as exc:  # pragma: no cover - explicit diagnostic path
                if _append_issue_for_record(record, exc):
                    break
            continue

        if isinstance(parsed, ExogenousCommand):
            exogenous_variables.add(parsed.variable.upper())
            continue

        if isinstance(parsed, ExtrapolateCommand):
            try:
                output = _apply_extrapolate_variables(
                    output,
                    variables=exogenous_variables,
                    active_window=active_window_tuple,
                    include_all_columns=True,
                )
            except Exception as exc:  # pragma: no cover - explicit diagnostic path
                if _append_issue_for_record(record, exc):
                    break
            continue

        if isinstance(parsed, SetYyToYCommand):
            try:
                _apply_setyytoy(
                    output,
                    active_window=active_window_tuple,
                )
            except Exception as exc:  # pragma: no cover - explicit diagnostic path
                if _append_issue_for_record(record, exc):
                    break
            continue

        if isinstance(parsed, PrintVarCommand):
            if not parsed.fileout:
                continue
            try:
                output_path = _resolve_runtime_file(
                    parsed.fileout, runtime_base_dir=runtime_base_dir
                )
                if parsed.loadformat:
                    _write_printvar_loadformat(
                        output,
                        output_path=output_path,
                        variables=parsed.variables,
                        active_window=active_window_tuple,
                    )
                else:
                    output.to_csv(output_path)
            except Exception as exc:  # pragma: no cover - explicit diagnostic path
                if _append_issue_for_record(record, exc):
                    break
            continue

        # Other runtime commands (e.g. SOLVE) are treated as workflow controls in mini-run.

    if on_error != "stop" or not issues:
        _flush_assignment_buffer()

    return MiniRunResult(
        frame=output,
        total_records=len(records),
        planned_steps=planned_steps,
        executed_steps=executed_steps,
        failed_steps=len(issues),
        issues=tuple(issues),
        unsupported_counts=_unsupported_command_counts(records),
        unsupported_examples=_unsupported_command_examples(records),
        executed_line_numbers=tuple(executed_line_numbers),
    )
