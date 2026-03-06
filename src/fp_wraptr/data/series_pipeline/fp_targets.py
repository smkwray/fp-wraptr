"""FP artifact writers for series pipelines."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from fp_wraptr.io.fmdata_writer import write_fmdata
from fp_wraptr.io.input_parser import parse_fm_data
from fp_wraptr.io.writer import write_exogenous_file, write_exogenous_override_file
from fp_wraptr.data.series_pipeline.periods import periods_between


class TargetError(RuntimeError):
    """Raised when a pipeline target cannot be written."""


@dataclass(frozen=True, slots=True)
class TargetWriteResult:
    paths: list[Path]
    kind: str
    variable: str
    notes: list[str]


def render_changevar_include(
    *,
    variable: str,
    fp_method: str,
    smpl_start: str,
    smpl_end: str,
    values: list[float],
    mode: str = "constant",
) -> str:
    var = str(variable).strip().upper()
    if not var:
        raise TargetError("include_changevar variable is required")
    method = str(fp_method).strip().upper()
    if not method:
        raise TargetError("include_changevar fp_method is required")

    lines: list[str] = []
    lines.append(f"SMPL {smpl_start} {smpl_end};")
    lines.append("CHANGEVAR;")
    lines.append(f"{var} {method}")
    if str(mode).strip().lower() == "constant":
        if not values:
            raise TargetError("include_changevar constant mode requires one value")
        lines.append(f"{float(values[-1]):.12g}")
    else:
        for v in values:
            lines.append(f"{float(v):.12g}")
    lines.append(";")
    lines.append("RETURN;")
    return "\n".join(lines) + "\n"


def write_include_changevar(
    *,
    out_paths: list[Path],
    variable: str,
    fp_method: str,
    smpl_start: str,
    smpl_end: str,
    values: list[float],
    mode: str = "constant",
) -> TargetWriteResult:
    text = render_changevar_include(
        variable=variable,
        fp_method=fp_method,
        smpl_start=smpl_start,
        smpl_end=smpl_end,
        values=values,
        mode=mode,
    )
    written: list[Path] = []
    for path in out_paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        written.append(path)
    return TargetWriteResult(paths=written, kind="include_changevar", variable=variable, notes=[])


def write_fmexog_override(
    *,
    out_path: Path,
    variable: str,
    fp_method: str,
    smpl_start: str,
    smpl_end: str,
    values: list[float],
    base_fmexog: Path | None = None,
    layer_on_base: bool = True,
) -> TargetWriteResult:
    spec: dict[str, dict] = {}
    var = str(variable).strip().upper()
    method = str(fp_method).strip().upper()
    if not values:
        raise TargetError("fmexog_override requires at least one value")
    if len(values) == 1:
        spec[var] = {"method": method, "value": float(values[0])}
    else:
        spec[var] = {
            "method": method,
            "value": [
                (period, float(value))
                for period, value in zip(
                    periods_between(smpl_start, smpl_end),
                    values,
                    strict=False,
                )
            ],
        }

    if layer_on_base and base_fmexog:
        write_exogenous_override_file(
            base_fmexog=base_fmexog,
            variables=spec,
            sample_start=smpl_start,
            sample_end=smpl_end,
            output_path=out_path,
        )
    else:
        write_exogenous_file(spec, smpl_start, smpl_end, out_path)
    return TargetWriteResult(paths=[out_path], kind="fmexog_override", variable=var, notes=[])


def _detect_newline(path: Path) -> str:
    data = path.read_bytes()
    return "\r\n" if b"\r\n" in data else "\n"


def patch_fmdata_like_file(
    *,
    in_path: Path,
    out_path: Path,
    variable: str,
    series_values_by_period: dict[str, float],
    sample_start: str | None = None,
    sample_end: str | None = None,
    kind: str = "fmdata_patch",
) -> TargetWriteResult:
    from fp_wraptr.data.series_pipeline.periods import periods_between

    if not in_path.exists():
        raise TargetError(f"Input file not found: {in_path}")
    parsed = parse_fm_data(in_path)
    file_start = str(parsed.get("sample_start", "")).strip()
    file_end = str(parsed.get("sample_end", "")).strip()
    if not file_start or not file_end:
        raise TargetError(f"Failed to determine SMPL window in {in_path}")
    start = str(sample_start).strip() if sample_start else file_start
    end = str(sample_end).strip() if sample_end else file_end
    if start != file_start or end != file_end:
        raise TargetError(
            f"MVP patcher requires sample window to match file ({file_start}..{file_end}); "
            f"requested {start}..{end}"
        )

    blocks = parsed.get("blocks") or []
    series: dict[str, list[float]] = {}
    var_u = str(variable).strip().upper()

    for block in blocks:
        if not isinstance(block, dict):
            continue
        name = str(block.get("name", "")).strip().upper()
        values = block.get("values")
        if not name or not isinstance(values, list):
            continue
        series[name] = [float(v) for v in values]

    if var_u not in series:
        # Allow adding a brand-new LOAD block when the caller provides a complete
        # window of values (one per period in the file sample).
        periods = periods_between(file_start, file_end)
        if set(series_values_by_period.keys()) != set(periods):
            raise TargetError(
                f"Variable '{var_u}' not present in {in_path} and updates do not cover the full "
                f"sample window ({file_start}..{file_end}). To add a new series, provide one value "
                "for every period in the file window."
            )
        series[var_u] = [float(series_values_by_period[p]) for p in periods]
        newline = _detect_newline(in_path)
        write_fmdata(
            sample_start=file_start,
            sample_end=file_end,
            series=series,
            newline=newline,
            path=out_path,
        )
        return TargetWriteResult(paths=[out_path], kind=kind, variable=var_u, notes=["added_series"])

    # Apply updates, requiring all requested periods to exist.
    periods = periods_between(file_start, file_end)
    index = {p: i for i, p in enumerate(periods)}
    target = series[var_u]
    for p, v in series_values_by_period.items():
        if p not in index:
            raise TargetError(f"Period '{p}' not in fmdata sample window ({file_start}..{file_end})")
        target[index[p]] = float(v)
    series[var_u] = target

    newline = _detect_newline(in_path)
    write_fmdata(sample_start=file_start, sample_end=file_end, series=series, newline=newline, path=out_path)
    return TargetWriteResult(paths=[out_path], kind=kind, variable=var_u, notes=[])
