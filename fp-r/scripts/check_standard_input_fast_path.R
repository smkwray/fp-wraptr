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
    stop("Unable to resolve check_standard_input_fast_path.R script path", call. = FALSE)
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
  "solver.R",
  "standard_input.R"
)
for (name in runtime_files) {
  source(file.path(r_dir, name), local = globalenv())
}

assert_true <- function(condition, message) {
  if (!isTRUE(condition)) {
    stop(message, call. = FALSE)
  }
}

extract_series_by_period <- function(bundle, name, periods) {
  state_periods <- as.character(bundle$state$periods %||% character())
  values <- as.numeric(bundle$state$series[[name]] %||% numeric())
  if (!length(state_periods) || !length(values)) {
    return(rep(NA_real_, length(periods)))
  }
  matched <- match(periods, state_periods)
  out <- rep(NA_real_, length(periods))
  keep <- !is.na(matched)
  out[keep] <- values[matched[keep]]
  out
}

series_max_abs_diff <- function(left, right) {
  left_num <- as.numeric(left %||% numeric())
  right_num <- as.numeric(right %||% numeric())
  if (!length(left_num) && !length(right_num)) {
    return(0)
  }
  if (length(left_num) != length(right_num)) {
    stop("Series length mismatch in fast-path contract", call. = FALSE)
  }
  diffs <- abs(left_num - right_num)
  diffs[!is.finite(diffs)] <- 0
  max(diffs)
}

frame <- data.frame(
  period = c("2025.3", "2025.4", "2026.1"),
  AUX = c(10, 14, 20),
  PSI4 = c(1.0, 1.1, 1.2),
  PD = c(2.0, 2.0, 2.0),
  PCD = c(2.0, 2.2, 2.4),
  LPIEFAZ = c(NA_real_, 16.0, 22.2),
  stringsAsFactors = FALSE,
  check.names = FALSE
)
history <- list(
  list(command = "SMPL", raw = "SMPL 1952.1 2025.4;"),
  list(command = "IDENT", kind = "ident", name = "PCD", raw = "IDENT PCD=PSI4*PD;", expression = "PSI4*PD"),
  list(command = "IDENT", kind = "ident", name = "LPIEFAZ", raw = "IDENT LPIEFAZ=PCD(-1)+AUX;", expression = "PCD(-1)+AUX")
)
sources <- list(
  entry_path = "fast-path-demo",
  fmdata = "",
  fmexog = "",
  fmout = "",
  tree = list(files_scanned = character(), statements = history)
)

slow_profile <- file.path(work_dir, "slow_replay_profile.csv")
fast_profile <- file.path(work_dir, "fast_replay_profile.csv")
slow_progress <- file.path(work_dir, "slow_stage_build_progress.csv")
fast_progress <- file.path(work_dir, "fast_stage_build_progress.csv")
unlink(c(slow_profile, fast_profile, slow_progress, fast_progress))

base_metadata <- list(
  options = list(outside = TRUE, noreset = TRUE),
  semantics_profile = "compat",
  solve_stage_index = 1L,
  watch_variables = c("PCD", "LPIEFAZ")
)

slow_bundle <- build_standard_solve_bundle(
  sources,
  frame,
  history,
  solve_index = 3L,
  active_window = c("2026.1", "2026.1"),
  solve_metadata = modifyList(base_metadata, list(
    stage_build_progress_path = slow_progress,
    replay_profile_path = slow_profile,
    frame_is_pre_solve_state = FALSE
  ))
)

fast_bundle <- build_standard_solve_bundle(
  sources,
  frame,
  history,
  solve_index = 3L,
  active_window = c("2026.1", "2026.1"),
  solve_metadata = modifyList(base_metadata, list(
    stage_build_progress_path = fast_progress,
    replay_profile_path = fast_profile,
    frame_is_pre_solve_state = TRUE
  ))
)

slow_profile_rows <- utils::read.csv(slow_profile, stringsAsFactors = FALSE)
fast_profile_rows <- utils::read.csv(fast_profile, stringsAsFactors = FALSE)
assert_true(nrow(slow_profile_rows) >= 1L, "Slow replay profile should record replay rows")
assert_true(nrow(fast_profile_rows) == 0L, "Fast replay profile should be empty")

compare_periods <- as.character(frame$period)
pcd_diff <- series_max_abs_diff(
  extract_series_by_period(slow_bundle, "PCD", compare_periods),
  extract_series_by_period(fast_bundle, "PCD", compare_periods)
)
lpie_diff <- series_max_abs_diff(
  extract_series_by_period(slow_bundle, "LPIEFAZ", compare_periods),
  extract_series_by_period(fast_bundle, "LPIEFAZ", compare_periods)
)
assert_true(pcd_diff < 1e-12, sprintf("PCD state mismatch: %.16f", pcd_diff))
assert_true(lpie_diff < 1e-12, sprintf("LPIEFAZ state mismatch: %.16f", lpie_diff))

slow_progress_rows <- utils::read.csv(slow_progress, stringsAsFactors = FALSE)
fast_progress_rows <- utils::read.csv(fast_progress, stringsAsFactors = FALSE)
slow_replay_plan <- slow_progress_rows[slow_progress_rows$event == "replay_plan_ready", , drop = FALSE]
fast_replay_plan <- fast_progress_rows[fast_progress_rows$event == "replay_plan_ready", , drop = FALSE]
assert_true(nrow(slow_replay_plan) == 1L, "Slow build should record replay_plan_ready once")
assert_true(nrow(fast_replay_plan) == 1L, "Fast build should record replay_plan_ready once")

writeLines(
  c(
    "status=ok",
    sprintf("slow_replay_rows=%d", nrow(slow_profile_rows)),
    sprintf("fast_replay_rows=%d", nrow(fast_profile_rows)),
    sprintf("pcd_max_abs_diff=%.16f", pcd_diff),
    sprintf("lpie_max_abs_diff=%.16f", lpie_diff),
    sprintf("replay_plan_rows=%d", as.integer(slow_replay_plan$row_count[[1L]] %||% 0L))
  ),
  file.path(work_dir, "standard_input_fast_path_report.txt")
)
