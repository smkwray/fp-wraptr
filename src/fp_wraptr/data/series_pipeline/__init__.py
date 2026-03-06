"""Project-agnostic series pipelines for generating FP artifacts.

This package implements a small "PSE-style" pipeline system that can ingest
time-series data, optionally extrapolate it into the forecast window, and
emit FP-consumable files (includes, fmexog overrides, fmdata/fmage patches).
"""

from fp_wraptr.data.series_pipeline.runner import PipelineRunError, PipelineRunResult, run_pipeline
from fp_wraptr.data.series_pipeline.spec import SeriesPipelineConfig

__all__ = ["PipelineRunError", "PipelineRunResult", "SeriesPipelineConfig", "run_pipeline"]
