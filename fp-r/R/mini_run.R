mini_run <- function(bundle, control = list()) {
  if (is.character(bundle) && length(bundle) == 1L) {
    bundle <- read_model_bundle(bundle)
  }
  run_control <- modifyList(bundle$control %||% list(), control)
  specs <- bundle_specs(bundle)
  result <- solve_equations(bundle$state, specs, control = run_control)
  post_solve_assignments <- bundle$post_solve_assignments %||% list()
  if (length(post_solve_assignments)) {
    active_window <- c(
      as.character(run_control$sample_start %||% run_control$start %||% ""),
      as.character(run_control$sample_end %||% run_control$end %||% "")
    )
    if (!all(nzchar(active_window))) {
      active_window <- NULL
    }
    frame <- result$series
    for (statement in post_solve_assignments) {
      frame <- apply_runtime_assignment_frame(frame, statement, active_window = active_window, allow_lhs = TRUE)
      frame <- sort_frame_by_period(frame)
    }
    result$series <- frame
  }
  result$bundle_name <- bundle$name %||% "<unnamed>"
  result
}
