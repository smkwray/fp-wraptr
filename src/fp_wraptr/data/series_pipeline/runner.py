"""Execute series pipeline YAML specs and generate FP artifacts."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from fp_wraptr.data.series_pipeline.extrapolation import ExtrapolationError, extrapolate_quarterly
from fp_wraptr.data.series_pipeline.fp_targets import (
    TargetError,
    TargetWriteResult,
    patch_fmdata_like_file,
    write_fmexog_override,
    write_include_changevar,
)
from fp_wraptr.data.series_pipeline.periods import normalize_period_token
from fp_wraptr.data.series_pipeline.series_io import (
    SeriesIoError,
    read_series_from_constant,
    read_series_from_csv,
    read_series_from_json,
)
from fp_wraptr.data.series_pipeline.spec import (
    ConstantSource,
    CsvSource,
    ExtrapolationSpec,
    FmagePatchTarget,
    FmdataPatchTarget,
    FmexogOverrideTarget,
    IncludeChangevarTarget,
    JsonSource,
    SeriesPipelineConfig,
)


class PipelineRunError(RuntimeError):
    """Raised when a pipeline fails to run."""


@dataclass(frozen=True, slots=True)
class PipelineRunResult:
    config: SeriesPipelineConfig
    report: dict[str, Any]
    written_paths: list[Path]


_TEMPLATE_RE = re.compile(r"\$\{(?P<key>[A-Za-z0-9_.:-]+)\}")


def _resolve_template(value: str, *, context: dict[str, Any]) -> str:
    text = str(value)

    def repl(match: re.Match[str]) -> str:
        key = match.group("key")
        if key == "today":
            return date.today().isoformat()
        if key.startswith("context."):
            _, _, rest = key.partition(".")
            return str(context.get(rest, ""))
        return match.group(0)

    return _TEMPLATE_RE.sub(repl, text)


def load_pipeline_config(path: Path) -> SeriesPipelineConfig:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise PipelineRunError(f"Failed to read pipeline YAML: {path}") from exc
    try:
        config = SeriesPipelineConfig.model_validate(payload)
    except Exception as exc:
        raise PipelineRunError(f"Invalid pipeline config: {path}") from exc
    _resolve_relative_paths(config, base_dir=path.parent)
    return config


def _resolve_relative_paths(config: SeriesPipelineConfig, *, base_dir: Path) -> None:
    """Resolve Path fields relative to the pipeline YAML directory."""
    for step in config.steps:
        if isinstance(step.source, (CsvSource, JsonSource)) and not step.source.path.is_absolute():
            step.source.path = (base_dir / step.source.path).resolve()
        if isinstance(step.target, FmexogOverrideTarget):
            if step.target.base_fmexog and not step.target.base_fmexog.is_absolute():
                step.target.base_fmexog = (base_dir / step.target.base_fmexog).resolve()
        if isinstance(step.target, FmdataPatchTarget):
            if not step.target.fmdata_in.is_absolute():
                step.target.fmdata_in = (base_dir / step.target.fmdata_in).resolve()
            if not step.target.fmdata_out.is_absolute():
                step.target.fmdata_out = (base_dir / step.target.fmdata_out).resolve()
        if isinstance(step.target, FmagePatchTarget):
            if not step.target.fmage_in.is_absolute():
                step.target.fmage_in = (base_dir / step.target.fmage_in).resolve()
            if not step.target.fmage_out.is_absolute():
                step.target.fmage_out = (base_dir / step.target.fmage_out).resolve()
        if step.write_to:
            step.write_to = [
                (base_dir / p).resolve() if not p.is_absolute() else p for p in step.write_to
            ]


def _read_source(step_id: str, source: object) -> tuple[list[str], list[float | None], dict[str, Any]]:
    if isinstance(source, CsvSource):
        frame = read_series_from_csv(source)
        return frame.periods, frame.values, {"kind": "csv", "path": str(source.path)}
    if isinstance(source, JsonSource):
        frame = read_series_from_json(source)
        return frame.periods, frame.values, {"kind": "json", "path": str(source.path)}
    if isinstance(source, ConstantSource):
        frame = read_series_from_constant(source)
        return frame.periods, frame.values, {"kind": "constant"}
    raise PipelineRunError(f"Step '{step_id}' has unsupported source kind")


def run_pipeline(
    *,
    pipeline_path: Path,
    output_report: Path | None = None,
    dry_run: bool = False,
) -> PipelineRunResult:
    config = load_pipeline_config(pipeline_path)
    ctx = config.context.model_dump()

    report: dict[str, Any] = {
        "pipeline": {
            "name": config.name,
            "description": config.description,
            "path": str(pipeline_path),
        },
        "context": ctx,
        "steps": [],
        "written": [],
    }
    written_paths: list[Path] = []

    for step in config.steps:
        step_ctx = {**ctx}
        step_record: dict[str, Any] = {"id": step.id}
        try:
            periods, values, source_meta = _read_source(step.id, step.source)
        except (SeriesIoError, PipelineRunError) as exc:
            raise PipelineRunError(f"Step '{step.id}' source load failed: {exc}") from exc

        step_record["source"] = source_meta
        step_record["history_points"] = len([v for v in values if v is not None])
        # Snapshot last observed point (post source parsing; pre transforms).
        last_period = ""
        last_value: float | None = None
        for p, v in zip(periods, values, strict=False):
            if v is None:
                continue
            last_period = str(p)
            last_value = float(v)
        if last_period:
            step_record["last_observed"] = {"period": last_period, "value": last_value}

        bounds = step.transform.bounds if step.transform else None
        pre = step.transform.pre if step.transform else None
        post = step.transform.post if step.transform else None

        extrap: ExtrapolationSpec | None = step.extrapolation
        if extrap is None:
            # Default: flat carry-forward into forecast window when target requires forecast.
            extrap = ExtrapolationSpec(method="flat")

        # Resolve smpl window using context substitutions.
        def resolve_period(token: str) -> str:
            resolved = _resolve_template(token, context=ctx).strip()
            return normalize_period_token(resolved)

        target = step.target
        target_kind = target.kind
        step_record["target"] = {"kind": target_kind}
        target_result: TargetWriteResult | None = None

        try:
            if isinstance(target, IncludeChangevarTarget):
                smpl_start = resolve_period(target.smpl_start)
                smpl_end = resolve_period(target.smpl_end)
                forecast = extrapolate_quarterly(
                    history_periods=periods,
                    history_values=values,
                    start=smpl_start,
                    end=smpl_end,
                    method=extrap.method,
                    window=extrap.window,
                    tail=extrap.tail,
                    bounds=bounds,
                    pre=pre,
                    post=post,
                    fallback=extrap.fallback,
                )
                mode = str(target.mode).strip().lower()
                out_paths = [
                    Path(_resolve_template(str(p), context=ctx)).expanduser()
                    for p in (step.write_to or [])
                ]
                if not dry_run:
                    target_result = write_include_changevar(
                        out_paths=out_paths,
                        variable=target.variable,
                        fp_method=target.fp_method,
                        smpl_start=smpl_start,
                        smpl_end=smpl_end,
                        values=forecast.values,
                        mode=mode,
                    )
                else:
                    step_record["dry_run_write_to"] = [str(p) for p in out_paths]
                step_record["extrapolation"] = {
                    "method": forecast.method,
                    "notes": forecast.notes,
                    "start": smpl_start,
                    "end": smpl_end,
                }
            elif isinstance(target, FmexogOverrideTarget):
                smpl_start = resolve_period(target.smpl_start)
                smpl_end = resolve_period(target.smpl_end)
                forecast = extrapolate_quarterly(
                    history_periods=periods,
                    history_values=values,
                    start=smpl_start,
                    end=smpl_end,
                    method=extrap.method,
                    window=extrap.window,
                    tail=extrap.tail,
                    bounds=bounds,
                    pre=pre,
                    post=post,
                    fallback=extrap.fallback,
                )
                out_path = Path(_resolve_template(str(step.write_to[0]), context=ctx)).expanduser()
                base = target.base_fmexog
                if base is not None:
                    base = Path(_resolve_template(str(base), context=ctx)).expanduser()
                if not dry_run:
                    target_result = write_fmexog_override(
                        out_path=out_path,
                        variable=target.variable,
                        fp_method=target.fp_method,
                        smpl_start=smpl_start,
                        smpl_end=smpl_end,
                        values=[forecast.values[-1]],
                        base_fmexog=base,
                        layer_on_base=bool(target.layer_on_base),
                    )
                else:
                    step_record["dry_run_write_to"] = [str(out_path)]
                step_record["extrapolation"] = {
                    "method": forecast.method,
                    "notes": forecast.notes,
                    "start": smpl_start,
                    "end": smpl_end,
                }
            elif isinstance(target, FmdataPatchTarget):
                in_path = Path(_resolve_template(str(target.fmdata_in), context=ctx)).expanduser()
                out_path = Path(_resolve_template(str(target.fmdata_out), context=ctx)).expanduser()
                # Patch only the periods present in source.
                updates = {
                    normalize_period_token(p): float(v)  # type: ignore[arg-type]
                    for p, v in zip(periods, values, strict=False)
                    if v is not None
                }
                if not dry_run:
                    target_result = patch_fmdata_like_file(
                        in_path=in_path,
                        out_path=out_path,
                        variable=target.variable,
                        series_values_by_period=updates,
                        sample_start=target.sample_start,
                        sample_end=target.sample_end,
                        kind="fmdata_patch",
                    )
                else:
                    step_record["dry_run_write_to"] = [str(out_path)]
            elif isinstance(target, FmagePatchTarget):
                in_path = Path(_resolve_template(str(target.fmage_in), context=ctx)).expanduser()
                out_path = Path(_resolve_template(str(target.fmage_out), context=ctx)).expanduser()
                updates = {
                    normalize_period_token(p): float(v)  # type: ignore[arg-type]
                    for p, v in zip(periods, values, strict=False)
                    if v is not None
                }
                if not dry_run:
                    target_result = patch_fmdata_like_file(
                        in_path=in_path,
                        out_path=out_path,
                        variable=target.variable,
                        series_values_by_period=updates,
                        sample_start=target.sample_start,
                        sample_end=target.sample_end,
                        kind="fmage_patch",
                    )
                else:
                    step_record["dry_run_write_to"] = [str(out_path)]
            else:
                raise PipelineRunError(f"Step '{step.id}' has unsupported target kind '{target_kind}'")
        except (ExtrapolationError, TargetError, PeriodError, IndexError, ValueError) as exc:
            raise PipelineRunError(f"Step '{step.id}' failed: {exc}") from exc

        if target_result:
            step_record["written"] = [str(p) for p in target_result.paths]
            report["written"].extend([str(p) for p in target_result.paths])
            written_paths.extend(target_result.paths)
        report["steps"].append(step_record)

    if output_report:
        output_report.parent.mkdir(parents=True, exist_ok=True)
        output_report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return PipelineRunResult(config=config, report=report, written_paths=written_paths)
