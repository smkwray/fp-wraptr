# API Reference

Auto-generated from source docstrings.

## IO

### Output Parser

::: fp_wraptr.io.parser
    options:
      members:
        - FPOutputData
        - ForecastVariable
        - EstimationResult
        - EstimationCoefficient
        - SolveIteration
        - parse_fp_output

### Input Parser

::: fp_wraptr.io.input_parser
    options:
      members:
        - parse_fp_input
        - parse_fp_input_text

### Writer

::: fp_wraptr.io.writer
    options:
      members:
        - write_exogenous_file
        - patch_fmexog_reference
        - patch_input_file

## Scenarios

### Configuration

::: fp_wraptr.scenarios.config
    options:
      members:
        - ScenarioConfig
        - VariableOverride

### Runner

::: fp_wraptr.scenarios.runner
    options:
      members:
        - run_scenario
        - load_scenario_config
        - validate_fp_home
        - ScenarioResult

### Batch

::: fp_wraptr.scenarios.batch
    options:
      members:
        - run_batch
        - compare_to_golden
        - save_golden

### DSL Compiler

::: fp_wraptr.scenarios.dsl
    options:
      members:
        - DSLCompileError
        - compile_scenario_dsl_text
        - compile_scenario_dsl_file

## Analysis

### Diff

::: fp_wraptr.analysis.diff
    options:
      members:
        - diff_outputs
        - diff_run_dirs
        - export_diff_csv
        - export_diff_excel

### Report

::: fp_wraptr.analysis.report
    options:
      members:
        - build_run_report

### Dependency Graph

::: fp_wraptr.analysis.graph
    options:
      members:
        - build_dependency_graph
        - summarize_graph
        - get_upstream
        - get_downstream

## Visualization

::: fp_wraptr.viz.plots
    options:
      members:
        - plot_forecast
        - plot_comparison

## Dashboard

### Artifacts

::: fp_wraptr.dashboard.artifacts
    options:
      members:
        - RunArtifact
        - scan_artifacts

### Charts

::: fp_wraptr.dashboard.charts
    options:
      members:
        - forecast_figure
        - comparison_figure
        - delta_bar_chart

## FRED

::: fp_wraptr.fred.ingest
    options:
      members:
        - get_fred_client
        - fetch_series
        - clear_cache

## Runtime

### Backend Protocol

::: fp_wraptr.runtime.backend
    options:
      members:
        - ModelBackend
        - RunResult
        - BackendInfo

### fp.exe Backend

::: fp_wraptr.runtime.fp_exe
    options:
      members:
        - FPExecutable
        - FPRunResult

### fair-py Backend (stub)

::: fp_wraptr.runtime.fairpy
    options:
      members:
        - FairPyBackend
