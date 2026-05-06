args <- commandArgs(trailingOnly = TRUE)

read_flag <- function(name, default = NULL) {
  idx <- match(name, args)
  if (is.na(idx)) {
    return(default)
  }
  if (idx >= length(args)) {
    stop(sprintf("Missing value for flag %s", name), call. = FALSE)
  }
  args[[idx + 1L]]
}

work_dir_raw <- read_flag("--work-dir", default = getwd())
work_dir <- normalizePath(work_dir_raw, winslash = "/", mustWork = FALSE)
dir.create(work_dir, recursive = TRUE, showWarnings = FALSE)

resolve_script_path <- function() {
  args_all <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", args_all, value = TRUE)
  if (!length(file_arg)) {
    stop("Unable to resolve check_solver_expression_breakdown.R script path", call. = FALSE)
  }
  normalizePath(sub("^--file=", "", file_arg[[1L]]), winslash = "/", mustWork = TRUE)
}

script_path <- resolve_script_path()
r_dir <- normalizePath(file.path(dirname(script_path), "..", "R"), winslash = "/", mustWork = TRUE)
runtime_files <- c(
  "000_utils.R",
  "bundle.R",
  "dependency.R",
  "standard_input.R",
  "equations.R",
  "expressions.R",
  "legacy_data.R",
  "mini_run.R",
  "parser.R",
  "periods.R",
  "solver.R"
)
for (name in runtime_files) {
  source(file.path(r_dir, name), local = globalenv())
}

assert_true <- function(condition, message) {
  if (!isTRUE(condition)) {
    stop(message, call. = FALSE)
  }
}

make_state <- function() {
  periods <- c("2025.4", "2026.1")
  series <- list(
    SF = c(1.2, NA_real_),
    PIEF = c(0.8, NA_real_),
    YD = c(10.0, NA_real_),
    LIMZ = c(0.6, NA_real_),
    RS = c(3.8, NA_real_),
    SG = c(2.1, NA_real_),
    YT = c(7.5, 7.8),
    PCPD = c(1.9, 1.95),
    UR = c(0.05, 0.052),
    EXPG = c(4.1, 4.2),
    TRGH = c(2.5, 2.6),
    TRSH = c(2.3, 2.4),
    AAG = c(40, 39.5),
    INTGZ = c(0.09, 0.091),
    JF = c(4.4, 4.35),
    HO = c(1.0, 1.01),
    X01 = c(1.0, 1.01),
    X02 = c(1.1, 1.08),
    X03 = c(1.2, 1.18),
    X04 = c(1.3, 1.28),
    X05 = c(1.4, 1.38),
    X06 = c(1.5, 1.48),
    X07 = c(1.6, 1.58),
    X08 = c(1.7, 1.68),
    X09 = c(1.8, 1.78),
    X10 = c(1.9, 1.88),
    X11 = c(2.0, 1.98),
    X12 = c(2.1, 2.08),
    X13 = c(2.2, 2.18),
    X14 = c(2.3, 2.28),
    X15 = c(2.4, 2.38),
    X16 = c(2.5, 2.48),
    X17 = c(2.6, 2.58),
    X18 = c(2.7, 2.68),
    X19 = c(2.8, 2.78),
    X20 = c(2.9, 2.88)
  )
  coef_values <- as_coef_lookup_env(c(
    "1,1" = 0.05,
    "1,2" = 0.04,
    "1,3" = 0.03,
    "1,4" = 0.02,
    "1,5" = 0.01,
    "1,6" = 0.06,
    "1,7" = 0.07,
    "1,8" = 0.08,
    "1,9" = 0.09,
    "1,10" = 0.025,
    "1,11" = 0.035,
    "1,12" = 0.045
  ))
  list(periods = periods, series = series, coef_values = coef_values)
}

make_specs <- function() {
  list(
    list(
      target = "SF",
      equation_number = 101L,
      kind = "eq",
      active_fsr_terms = c("PIEF", "YD", "RS", "LIMZ", "X01", "X02", "X03", "X04"),
      expression = paste(
        "0.15*PIEF + 0.12*YD + 0.08*LIMZ + 0.05*RS + 0.03*SG",
        "+ COEF(1,1)*X01 + COEF(1,2)*X02 + COEF(1,3)*X03 + COEF(1,4)*X04",
        "+ LOG(ABS(X05)+1) + MAX(X06, X07) - MIN(X08, X09) + X10(-1) + X11(-1)"
      )
    ),
    list(
      target = "PIEF",
      equation_number = 102L,
      kind = "eq",
      active_fsr_terms = c("SF", "YD", "RS", "PCPD", "X05", "X06", "X07", "X08"),
      expression = paste(
        "0.11*SF + 0.07*YD + 0.03*RS + 0.04*PCPD + 0.02*SG",
        "+ COEF(1,5)*X05 + COEF(1,6)*X06 + COEF(1,7)*X07 + COEF(1,8)*X08",
        "+ LOG(ABS(X09)+1) + MAX(X10, X11) - MIN(X12, X13) + X14(-1)"
      )
    ),
    list(
      target = "YD",
      equation_number = 103L,
      kind = "eq",
      active_fsr_terms = c("SF", "PIEF", "LIMZ", "RS", "TRGH", "TRSH", "AAG", "INTGZ"),
      expression = paste(
        "0.10*SF + 0.09*PIEF + 0.06*LIMZ - 0.03*RS + 0.07*TRGH + 0.05*TRSH",
        "- 0.02*AAG + 0.04*INTGZ + COEF(1,9)*X09 + COEF(1,10)*X10",
        "+ X12 + X13 + X15(-1) + X16(-1)"
      )
    ),
    list(
      target = "LIMZ",
      equation_number = 104L,
      kind = "eq",
      active_fsr_terms = c("YD", "SF", "PIEF", "UR", "EXPG", "X11", "X12", "X13"),
      expression = paste(
        "0.08*YD + 0.04*SF + 0.05*PIEF - 0.03*UR + 0.02*EXPG",
        "+ COEF(1,11)*X11 + COEF(1,12)*X12 + MAX(X13, X14) - MIN(X15, X16)",
        "+ X17(-1)"
      )
    ),
    list(
      target = "RS",
      equation_number = 105L,
      kind = "eq",
      active_fsr_terms = c("SF", "PIEF", "YD", "LIMZ", "PCPD", "UR", "X17", "X18"),
      expression = paste(
        "0.06*SF + 0.05*PIEF + 0.04*YD + 0.03*LIMZ + 0.02*PCPD + 0.01*UR",
        "+ X17 + X18 + LOG(ABS(X19)+1) - MIN(X01, X02) + X03(-1)"
      )
    ),
    list(
      target = "SG",
      equation_number = 106L,
      kind = "eq",
      active_fsr_terms = c("YD", "RS", "TRGH", "EXPG", "JF", "HO", "X19", "X20"),
      expression = paste(
        "0.07*YD - 0.02*RS + 0.06*TRGH - 0.04*EXPG + 0.05*JF + 0.03*HO",
        "+ X19 + X20 + MAX(X04, X05) - MIN(X06, X07) + X08(-1)"
      )
    )
  )
}

strip_prepared_evaluator <- function(specs) {
  lapply(specs, function(spec) {
    if (!is.null(spec$compiled)) {
      spec$compiled$scalar_eval_context <- NULL
      spec$compiled$scalar_eval_env <- NULL
    }
    spec
  })
}

extract_target_values <- function(result, targets, period_index = 2L) {
  vapply(targets, function(target) {
    as.numeric(result$state$series[[target]][[period_index]])
  }, numeric(1))
}

run_case <- function(expression_profile_path = "", spec_profile_path = "", disable_prepared_evaluator = FALSE) {
  specs <- normalize_specs(make_specs())
  if (isTRUE(disable_prepared_evaluator)) {
    specs <- strip_prepared_evaluator(specs)
  }
  started <- proc.time()[["elapsed"]]
  result <- solve_equations(
    make_state(),
    specs,
    control = list(
      start = "2026.1",
      end = "2026.1",
      min_iter = 4L,
      max_iter = 4L,
      tolerance = 0,
      expression_profile_path = expression_profile_path,
      spec_profile_path = spec_profile_path,
      spec_profile_periods = "2026.1"
    )
  )
  result$wall_elapsed_sec <- as.numeric(proc.time()[["elapsed"]] - started)
  result
}

targets <- c("SF", "PIEF", "YD", "LIMZ", "RS", "SG")
legacy_result <- run_case(disable_prepared_evaluator = TRUE)
baseline_result <- run_case()
expression_profile_path <- file.path(work_dir, "solver_expression_profile.csv")
spec_profile_path <- file.path(work_dir, "solver_spec_profile.csv")
profiled_result <- run_case(
  expression_profile_path = expression_profile_path,
  spec_profile_path = spec_profile_path,
  disable_prepared_evaluator = FALSE
)

legacy_values <- extract_target_values(legacy_result, targets)
baseline_values <- extract_target_values(baseline_result, targets)
profiled_values <- extract_target_values(profiled_result, targets)
legacy_diff <- max(abs(legacy_values - baseline_values))
max_abs_diff <- max(abs(baseline_values - profiled_values))
assert_true(legacy_diff < 1e-12, sprintf("Prepared evaluator changed results vs legacy: %.16f", legacy_diff))
assert_true(max_abs_diff < 1e-12, sprintf("Profiled solve changed results: %.16f", max_abs_diff))

expression_profile <- utils::read.csv(expression_profile_path, stringsAsFactors = FALSE)
spec_profile <- utils::read.csv(spec_profile_path, stringsAsFactors = FALSE)

assert_true(nrow(expression_profile) >= length(targets) * 2L, "Expected profiler rows for each target")
assert_true(
  identical(sort(unique(expression_profile$component)), c("eval", "fp_coef", "fp_value")),
  "Expression profile should include eval/fp_coef/fp_value rows"
)
for (target in targets) {
  target_components <- sort(unique(as.character(expression_profile$component[expression_profile$target == target])))
  assert_true(
    identical(target_components, c("eval", "fp_value")) ||
      identical(target_components, c("eval", "fp_coef", "fp_value")),
    sprintf("Unexpected component set for target %s", target)
  )
}
assert_true(all(expression_profile$call_count > 0L), "Expression profile call counts should be positive")
assert_true(all(expression_profile$total_elapsed_sec >= 0), "Expression profile elapsed times should be non-negative")

eval_calls <- sum(as.integer(expression_profile$call_count[expression_profile$component == "eval"]))
fp_value_calls <- sum(as.integer(expression_profile$call_count[expression_profile$component == "fp_value"]))
fp_coef_calls <- sum(as.integer(expression_profile$call_count[expression_profile$component == "fp_coef"]))
spec_eval_calls <- sum(as.integer(spec_profile$eval_count))

assert_true(eval_calls == spec_eval_calls, "Eval call count should match spec eval count")
assert_true(fp_value_calls > eval_calls, "fp_value calls should exceed eval calls")
assert_true(fp_coef_calls > 0L, "fp_coef calls should be recorded")

component_totals <- stats::aggregate(
  total_elapsed_sec ~ component,
  data = expression_profile,
  FUN = sum
)
component_totals <- component_totals[order(-component_totals$total_elapsed_sec), , drop = FALSE]
top_target_rows <- stats::aggregate(
  total_elapsed_sec ~ target,
  data = expression_profile,
  FUN = sum
)
top_target_rows <- top_target_rows[order(-top_target_rows$total_elapsed_sec), , drop = FALSE]

writeLines(
  c(
    "status=ok",
    sprintf("legacy_wall_elapsed_sec=%.6f", as.numeric(legacy_result$wall_elapsed_sec)),
    sprintf("prepared_wall_elapsed_sec=%.6f", as.numeric(baseline_result$wall_elapsed_sec)),
    sprintf("eval_calls=%d", eval_calls),
    sprintf("fp_value_calls=%d", fp_value_calls),
    sprintf("fp_coef_calls=%d", fp_coef_calls),
    sprintf("legacy_max_abs_diff=%.16f", legacy_diff),
    sprintf("max_abs_diff=%.16f", max_abs_diff),
    sprintf("slowest_component=%s", as.character(component_totals$component[[1L]])),
    sprintf("slowest_target=%s", as.character(top_target_rows$target[[1L]]))
  ),
  file.path(work_dir, "solver_expression_breakdown_report.txt")
)
