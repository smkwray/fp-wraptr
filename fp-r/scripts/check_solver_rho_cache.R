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
    stop("Unable to resolve check_solver_rho_cache.R script path", call. = FALSE)
  }
  normalizePath(sub("^--file=", "", file_arg[[1L]]), winslash = "/", mustWork = TRUE)
}

script_path <- resolve_script_path()
r_dir <- normalizePath(file.path(dirname(script_path), "..", "R"), winslash = "/", mustWork = TRUE)
runtime_files <- c(
  "000_utils.R",
  "bundle.R",
  "dependency.R",
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

orig_evaluate_compiled_expression <- evaluate_compiled_expression
evaluation_count <- 0L
evaluate_compiled_expression <- function(...) {
  evaluation_count <<- evaluation_count + 1L
  orig_evaluate_compiled_expression(...)
}

make_state <- function() {
  list(
    periods = c("2025.4", "2026.1"),
    series = list(
      A = c(10, NA_real_),
      X = c(1, 1)
    )
  )
}

specs <- list(
  list(
    target = "A",
    expression = "X + 1",
    rho_terms = list(list(order = 1L, coefficient = 0.5))
  )
)

run_case <- function(use_cache) {
  evaluation_count <<- 0L
  result <- solve_equations(
    make_state(),
    specs,
    control = list(
      start = "2026.1",
      end = "2026.1",
      min_iter = 3L,
      max_iter = 3L,
      tolerance = 0,
      rho_lagged_structural_cache = use_cache
    )
  )
  list(
    count = as.integer(evaluation_count),
    value = as.numeric(result$state$series$A[[2L]]),
    diagnostics = result$diagnostics
  )
}

without_cache <- run_case(FALSE)
with_cache <- run_case(TRUE)

assert_true(
  identical(without_cache$diagnostics$iterations, with_cache$diagnostics$iterations),
  "Iteration counts should match"
)
assert_true(
  abs(without_cache$value - with_cache$value) < 1e-12,
  sprintf("Solved values differ: %.16f vs %.16f", without_cache$value, with_cache$value)
)
assert_true(
  without_cache$count > with_cache$count,
  sprintf("Expected fewer compiled-expression evaluations with cache: slow=%d fast=%d", without_cache$count, with_cache$count)
)
assert_true(
  without_cache$count == 6L,
  sprintf("Unexpected no-cache evaluation count: %d", without_cache$count)
)
assert_true(
  with_cache$count == 4L,
  sprintf("Unexpected cached evaluation count: %d", with_cache$count)
)

writeLines(
  c(
    "status=ok",
    sprintf("without_cache_eval_count=%d", without_cache$count),
    sprintf("with_cache_eval_count=%d", with_cache$count),
    sprintf("solved_value=%.16f", with_cache$value)
  ),
  file.path(work_dir, "solver_rho_cache_report.txt")
)
