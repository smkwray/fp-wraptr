ensure_state <- function(state) {
  periods <- state$periods %||% NULL
  series <- state$series %||% NULL
  if (is.null(periods) || is.null(series)) {
    stopf("State must contain periods and series")
  }
  state$periods <- as.character(periods)
  state$series <- as.list(series)
  state
}

clone_state_values <- function(state) {
  state <- ensure_state(state)
  list(
    periods = state$periods,
    series = lapply(state$series, as.numeric)
  )
}

initialize_state_for_specs <- function(state, specs) {
  n_periods <- length(state$periods)
  for (spec in specs) {
    target <- spec$target
    if (is.null(state$series[[target]])) {
      state$series[[target]] <- rep(NA_real_, n_periods)
      next
    }
    values <- as.numeric(state$series[[target]])
    if (length(values) != n_periods) {
      stopf("Series %s does not align with the period vector", target)
    }
    state$series[[target]] <- values
  }
  state
}

seed_period_targets <- function(state, specs, period_pos) {
  for (spec in specs) {
    target <- spec$target
    current <- as.numeric(state$series[[target]][[period_pos]])
    if (is.finite(current)) {
      next
    }
    if (period_pos > 1L && is.finite(state$series[[target]][[period_pos - 1L]])) {
      state$series[[target]][[period_pos]] <- as.numeric(state$series[[target]][[period_pos - 1L]])
    } else {
      state$series[[target]][[period_pos]] <- 0
    }
  }
  state
}

as_series_frame <- function(state) {
  frame <- data.frame(
    period = state$periods,
    stringsAsFactors = FALSE,
    check.names = FALSE
  )
  for (name in names(state$series)) {
    frame[[name]] <- as.numeric(state$series[[name]])
  }
  frame
}

initialize_resid_ar1_states <- function(specs, initial_states = NULL) {
  states <- list()
  for (spec in specs) {
    cfg <- spec$resid_ar1 %||% NULL
    if (is.null(cfg)) {
      next
    }
    prior <- initial_states[[spec$target]] %||% NULL
    states[[spec$target]] <- list(
      rho_lag1 = as.numeric(cfg$rho_lag1),
      residual = prior$residual %||% NULL,
      residual_position = prior$residual_position %||% NULL
    )
  }
  states
}

normalize_solver_trace_value <- function(value) {
  if (is.null(value) || length(value) == 0L) {
    return(NA_real_)
  }
  numeric_value <- suppressWarnings(as.numeric(value))
  if (length(numeric_value) == 0L) {
    return(NA_real_)
  }
  if (!is.finite(numeric_value)) {
    return(NA_real_)
  }
  if (abs(numeric_value + 99.0) <= 1e-12) {
    return(NA_real_)
  }
  numeric_value
}

spec_series_overrides <- function(spec) {
  source_name <- spec$target_lag_source %||% ""
  if (!nzchar(source_name)) {
    return(NULL)
  }
  stats::setNames(source_name, spec$target)
}

spec_lagged_target_series <- function(spec) {
  source_name <- spec$target_lag_source %||% ""
  if (!nzchar(source_name)) {
    return(spec$target)
  }
  source_name
}

resolve_solver_trace_source_name <- function(name, lag = 0L, series_overrides = NULL, lag_only_overrides = FALSE) {
  source_name <- as.character(name)
  if (!is.null(series_overrides) &&
      name %in% names(series_overrides) &&
      (!isTRUE(lag_only_overrides) || as.integer(lag) != 0L)) {
    source_name <- as.character(series_overrides[[name]])
  }
  source_name
}

build_spec_reference_trace_rows <- function(spec, state, period_pos, iteration) {
  compiled <- spec$compiled %||% NULL
  refs <- compiled$references %||% NULL
  build_trace_rows_from_references(
    spec = spec,
    refs = refs,
    state = state,
    period_pos = period_pos,
    iteration = iteration,
    trace_kind = "compiled_reference"
  )
}

build_trace_rows_from_references <- function(spec, refs, state, period_pos, iteration, trace_kind) {
  if (is.null(refs) || !is.data.frame(refs) || !nrow(refs)) {
    return(data.frame(
      period = character(),
      iteration = integer(),
      target = character(),
      trace_kind = character(),
      variable = character(),
      lag = integer(),
      source_name = character(),
      source_period = character(),
      value = numeric(),
      stringsAsFactors = FALSE
    ))
  }
  refs <- unique(data.frame(
    name = as.character(refs$name %||% character()),
    lag = as.integer(refs$lag %||% integer()),
    stringsAsFactors = FALSE
  ))
  refs <- refs[nzchar(refs$name), , drop = FALSE]
  if (!nrow(refs)) {
    return(data.frame(
      period = character(),
      iteration = integer(),
      target = character(),
      trace_kind = character(),
      variable = character(),
      lag = integer(),
      source_name = character(),
      source_period = character(),
      value = numeric(),
      stringsAsFactors = FALSE
    ))
  }
  series_overrides <- spec_series_overrides(spec)
  rows <- lapply(seq_len(nrow(refs)), function(idx) {
    name <- as.character(refs$name[[idx]])
    lag <- as.integer(refs$lag[[idx]])
    source_name <- resolve_solver_trace_source_name(
      name,
      lag = lag,
      series_overrides = series_overrides,
      lag_only_overrides = TRUE
    )
    series <- state$series[[source_name]]
    target_index <- as.integer(period_pos) + lag
    source_period <- if (target_index >= 1L && target_index <= length(state$periods)) {
      as.character(state$periods[[target_index]])
    } else {
      ""
    }
    value <- if (!is.null(series) && target_index >= 1L && target_index <= length(series)) {
      normalize_solver_trace_value(series[[target_index]])
    } else {
      NA_real_
    }
    data.frame(
      period = as.character(state$periods[[period_pos]]),
      iteration = as.integer(iteration),
      target = as.character(spec$target),
      trace_kind = as.character(trace_kind),
      variable = name,
      lag = lag,
      source_name = as.character(source_name),
      source_period = source_period,
      value = as.numeric(value),
      stringsAsFactors = FALSE
    )
  })
  do.call(rbind, rows)
}

build_spec_active_fsr_trace_rows <- function(spec, state, period_pos, iteration) {
  tokens <- as.character(spec$active_fsr_terms %||% character())
  if (!length(tokens)) {
    return(data.frame(
      period = character(),
      iteration = integer(),
      target = character(),
      trace_kind = character(),
      variable = character(),
      lag = integer(),
      source_name = character(),
      source_period = character(),
      value = numeric(),
      stringsAsFactors = FALSE
    ))
  }
  refs <- tokens_to_reference_frame(tokens)
  refs <- refs[nzchar(refs$name %||% character()), , drop = FALSE]
  build_trace_rows_from_references(
    spec = spec,
    refs = refs,
    state = state,
    period_pos = period_pos,
    iteration = iteration,
    trace_kind = "active_fsr_reference"
  )
}

build_spec_result_trace_rows <- function(spec, state, period_pos, iteration, previous, evaluation) {
  data.frame(
    period = as.character(state$periods[[period_pos]]),
    iteration = rep(as.integer(iteration), 3L),
    target = rep(as.character(spec$target), 3L),
    trace_kind = c("previous_value", "evaluated_structural", "evaluated_value"),
    variable = c(as.character(spec$target), as.character(spec$target), as.character(spec$target)),
    lag = c(0L, 0L, 0L),
    source_name = c("", "", ""),
    source_period = c(
      as.character(state$periods[[period_pos]]),
      as.character(state$periods[[period_pos]]),
      as.character(state$periods[[period_pos]])
    ),
    value = c(
      normalize_solver_trace_value(previous),
      normalize_solver_trace_value(evaluation$structural %||% NA_real_),
      normalize_solver_trace_value(evaluation$value %||% NA_real_)
    ),
    stringsAsFactors = FALSE
  )
}

evaluate_resid_ar1_value <- function(spec, structural, period_pos, resid_state) {
  if (is.null(resid_state)) {
    return(structural)
  }
  cfg <- spec$resid_ar1
  ar_term <- 0
  if (!is.null(resid_state$residual) && !is.null(resid_state$residual_position)) {
    step_distance <- as.integer(period_pos) - as.integer(resid_state$residual_position)
    carry_allowed <- cfg$carry_lag <= 0L || step_distance <= cfg$carry_lag
    if (cfg$carry_multipass && cfg$carry_damp_mode %in% c("state", "sol4")) {
      carry_allowed <- step_distance == 1L
    }
    if (step_distance > 0L && carry_allowed) {
      ar_term <- (resid_state$rho_lag1^step_distance) * as.numeric(resid_state$residual)
      if (identical(cfg$carry_damp_mode, "term")) {
        ar_term <- ar_term * cfg$carry_damp
      }
    }
  }
  structural + ar_term
}

resolve_resid_source_value <- function(spec, source_state, period_pos) {
  cfg <- spec$resid_ar1
  if (identical(cfg$update_source, "solved")) {
    return(NA_real_)
  }
  source_name <- cfg$source_series
  if (!nzchar(source_name)) {
    source_name <- spec$target
  }
  series <- source_state$series[[source_name]]
  if (is.null(series)) {
    stopf("Residual source series %s is missing for target %s", source_name, spec$target)
  }
  as.numeric(series[[period_pos]])
}

commit_resid_ar1_state <- function(spec, resid_state, source_state, solved_state, period_pos, structural) {
  if (is.null(resid_state)) {
    return(resid_state)
  }
  cfg <- spec$resid_ar1
  solved_value <- as.numeric(solved_state$series[[spec$target]][[period_pos]])
  source_value <- resolve_resid_source_value(spec, source_state, period_pos)
  residual <- NULL

  if (identical(cfg$update_source, "solved")) {
    residual <- solved_value - structural
  } else if (is.finite(source_value)) {
    if (identical(cfg$update_source, "result")) {
      residual <- source_value - solved_value
    } else {
      residual <- source_value - structural
    }
  }

  if (is.null(residual)) {
    return(resid_state)
  }
  if (!is.finite(residual)) {
    resid_state$residual <- NULL
    resid_state$residual_position <- NULL
    return(resid_state)
  }

  if (identical(cfg$carry_damp_mode, "term")) {
    resid_state$residual <- as.numeric(residual)
  } else if (identical(cfg$carry_damp_mode, "state")) {
    if (is.null(resid_state$residual)) {
      resid_state$residual <- as.numeric(residual)
    } else {
      previous <- as.numeric(resid_state$residual)
      resid_state$residual <- previous + cfg$carry_damp * (as.numeric(residual) - previous)
    }
  } else if (identical(cfg$carry_damp_mode, "sol4")) {
    previous <- if (is.null(resid_state$residual)) 0 else as.numeric(resid_state$residual)
    resid_state$residual <- previous + cfg$carry_damp * (as.numeric(residual) - previous)
  } else {
    stopf("Unsupported resid_ar1 carry_damp_mode: %s", cfg$carry_damp_mode)
  }
  resid_state$residual_position <- as.integer(period_pos)
  resid_state
}

evaluate_spec_at_period <- function(spec, state, period_pos, strict = TRUE, resid_state = NULL) {
  series_overrides <- spec_series_overrides(spec)
  structural <- as.numeric(
    evaluate_compiled_expression(
      spec$compiled,
      state,
      period_pos,
      strict = strict,
      series_overrides = series_overrides,
      lag_only_overrides = TRUE
    )
  )
  if (!is.null(spec$resid_ar1)) {
    return(list(
      value = evaluate_resid_ar1_value(spec, structural, period_pos, resid_state),
      structural = structural
    ))
  }
  rho_terms <- spec$rho_terms %||% NULL
  if (is.null(rho_terms) || nrow(rho_terms) == 0L) {
    return(list(value = structural, structural = structural))
  }

  result <- structural
  lagged_target_series <- state$series[[spec_lagged_target_series(spec)]]
  if (is.null(lagged_target_series)) {
    stopf("Lagged target source is missing for %s", spec$target)
  }
  for (rho_index in seq_len(nrow(rho_terms))) {
    order <- as.integer(rho_terms$order[[rho_index]])
    coefficient <- as.numeric(rho_terms$coefficient[[rho_index]])
    lagged_position <- as.integer(period_pos) - order
    if (lagged_position < 1L || lagged_position > length(state$periods)) {
      return(list(value = NA_real_, structural = structural))
    }
    lagged_lhs_value <- as.numeric(lagged_target_series[[lagged_position]])
    if (!is.finite(lagged_lhs_value)) {
      return(list(value = NA_real_, structural = structural))
    }
    lagged_structural <- tryCatch(
      as.numeric(evaluate_compiled_expression(
        spec$compiled,
        state,
        lagged_position,
        strict = FALSE,
        series_overrides = series_overrides,
        lag_only_overrides = TRUE
      )),
      error = function(...) NA_real_
    )
    if (!is.finite(lagged_structural)) {
      return(list(value = NA_real_, structural = structural))
    }
    result <- result + coefficient * lagged_lhs_value
    result <- result - coefficient * lagged_structural
  }

  list(value = result, structural = structural)
}

solve_equations <- function(state, specs, control = list()) {
  state <- ensure_state(state)
  source_state <- clone_state_values(state)
  normalized_specs <- normalize_specs(specs)
  normalized_specs <- Filter(function(spec) !is.null(spec$compiled), normalized_specs)
  state <- initialize_state_for_specs(state, normalized_specs)
  resid_ar1_states <- initialize_resid_ar1_states(
    normalized_specs,
    initial_states = control$resid_ar1_states %||% list()
  )
  period_window <- resolve_sample_window(
    state$periods,
    sample_start = control$sample_start %||% control$start,
    sample_end = control$sample_end %||% control$end
  )
  max_iter <- as.integer(control$max_iter %||% 100L)
  min_iter <- as.integer(control$min_iter %||% 1L)
  tolerance <- as.numeric(control$tolerance %||% 1e-8)
  damping <- as.numeric(control$damping %||% 1.0)
  strict <- if (is.null(control$strict)) TRUE else isTRUE(control$strict)
  order <- control$order %||% build_dependency_order(normalized_specs)
  ordered_specs <- normalized_specs[match(order, vapply(normalized_specs, `[[`, "", "target"))]
  ordered_specs <- Filter(Negate(is.null), ordered_specs)
  ordered_targets <- vapply(ordered_specs, `[[`, character(1), "target")
  solver_policy <- as.character(control$solver_policy %||% "full_scan")
  active_set_enabled <- isTRUE(control$active_set_enabled) &&
    identical(solver_policy, "active_set_v1") &&
    length(ordered_targets) > 0L
  active_set_start_iteration <- as.integer(control$active_set_start_iteration %||% 0L)
  if (!is.finite(active_set_start_iteration) || active_set_start_iteration < 1L) {
    active_set_start_iteration <- 1L
  }
  active_set_delta_threshold <- as.numeric(control$active_set_delta_threshold %||% tolerance)
  if (!is.finite(active_set_delta_threshold) || active_set_delta_threshold <= 0) {
    active_set_delta_threshold <- max(tolerance, 1e-8)
  }
  active_set_delta_threshold <- max(active_set_delta_threshold, tolerance)
  reverse_dependency_map <- if (active_set_enabled) {
    map <- stats::setNames(vector("list", length(ordered_targets)), ordered_targets)
    for (spec in ordered_specs) {
      refs <- spec$compiled$references %||% NULL
      if (is.null(refs) || !is.data.frame(refs) || !nrow(refs)) {
        next
      }
      deps <- unique(as.character(refs$name[as.integer(refs$lag) == 0L]))
      deps <- deps[deps %in% ordered_targets]
      deps <- deps[deps != spec$target]
      for (dep in deps) {
        map[[dep]] <- unique(c(as.character(map[[dep]] %||% character()), spec$target))
      }
    }
    map
  } else {
    stats::setNames(vector("list", length(ordered_targets)), ordered_targets)
  }
  diagnostics <- vector("list", period_window[["end"]] - period_window[["start"]] + 1L)
  diag_index <- 1L
  fallback_rows <- list()
  equation_input_rows <- list()
  spec_profile_path <- as.character(control$spec_profile_path %||% "")
  spec_profile_periods <- unique(as.character(control$spec_profile_periods %||% character()))
  spec_profile_stage <- as.integer(control$solve_stage_index %||% 0L)
  spec_profile <- new.env(parent = emptyenv())
  iteration_profile_path <- as.character(control$iteration_profile_path %||% "")
  iteration_profile_periods <- unique(as.character(control$iteration_profile_periods %||% character()))
  iteration_profile_rows <- list()
  progress_path <- as.character(control$period_progress_path %||% "")
  progress_stage <- as.integer(control$solve_stage_index %||% 0L)
  progress_has_header <- nzchar(progress_path) && file.exists(progress_path)
  trace_targets <- unique(toupper(as.character(control$equation_input_trace_targets %||% character())))
  trace_periods <- unique(as.character(control$equation_input_trace_periods %||% character()))
  trace_max_iterations <- as.integer(control$equation_input_trace_max_iterations %||% 0L)

  for (period_pos in seq.int(period_window[["start"]], period_window[["end"]])) {
    period_started <- proc.time()[["elapsed"]]
    period_label <- as.character(state$periods[[period_pos]])
    profile_this_period <- nzchar(spec_profile_path) &&
      (!length(spec_profile_periods) || period_label %in% spec_profile_periods)
    state <- seed_period_targets(state, ordered_specs, period_pos)
    period_structural <- list()
    converged <- FALSE
    max_delta <- Inf
    iter_used <- 0L
    active_targets <- ordered_targets
    trace_this_period <- !length(trace_periods) || as.character(state$periods[[period_pos]]) %in% trace_periods
    for (iter in seq_len(max_iter)) {
      iter_used <- iter
      max_delta <- 0
      changed_specs <- 0L
      max_delta_target <- ""
      current_targets <- ordered_targets
      if (active_set_enabled && as.integer(iter) > active_set_start_iteration && length(active_targets)) {
        current_targets <- ordered_targets[ordered_targets %in% active_targets]
      }
      current_specs <- ordered_specs[ordered_targets %in% current_targets]
      materially_changed_targets <- character()
      for (spec in current_specs) {
        target <- spec$target
        previous <- as.numeric(state$series[[target]][[period_pos]])
        spec_started <- if (profile_this_period) proc.time()[["elapsed"]] else NA_real_
        trace_this_target <- trace_this_period &&
          (!length(trace_targets) || toupper(as.character(target)) %in% trace_targets) &&
          (trace_max_iterations <= 0L || as.integer(iter) <= trace_max_iterations)
        if (trace_this_target) {
          equation_input_rows[[length(equation_input_rows) + 1L]] <- build_spec_reference_trace_rows(
            spec,
            state,
            period_pos,
            iter
          )
          equation_input_rows[[length(equation_input_rows) + 1L]] <- build_spec_active_fsr_trace_rows(
            spec,
            state,
            period_pos,
            iter
          )
        }
        evaluation <- suppressWarnings(evaluate_spec_at_period(
          spec,
          state,
          period_pos,
          strict = strict,
          resid_state = resid_ar1_states[[target]] %||% NULL
        ))
        if (trace_this_target) {
          equation_input_rows[[length(equation_input_rows) + 1L]] <- build_spec_result_trace_rows(
            spec,
            state,
            period_pos,
            iter,
            previous,
            evaluation
          )
        }
        value <- as.numeric(evaluation$value)
        period_structural[[target]] <- as.numeric(evaluation$structural)
        fallback_reason <- ""
        if (!is.finite(value)) {
          # Match the reduced fppy rho-aware boundary behavior by preserving
          # the existing value when the rho correction falls off available history.
          if (!is.null(spec$rho_terms) && nrow(spec$rho_terms) > 0L && is.finite(previous)) {
            fallback_reason <- "rho_preserve_previous"
            fallback_rows[[length(fallback_rows) + 1L]] <- data.frame(
              period = as.character(state$periods[[period_pos]]),
              iteration = as.integer(iter),
              target = as.character(target),
              fallback_reason = "rho_preserve_previous",
              previous_value = as.numeric(previous),
              evaluated_value = as.numeric(value),
              stringsAsFactors = FALSE
            )
            value <- previous
          } else if (nzchar(as.character(spec$kind %||% "")) && is.finite(previous)) {
            fallback_reason <- "kind_preserve_previous"
            fallback_rows[[length(fallback_rows) + 1L]] <- data.frame(
              period = as.character(state$periods[[period_pos]]),
              iteration = as.integer(iter),
              target = as.character(target),
              fallback_reason = "kind_preserve_previous",
              previous_value = as.numeric(previous),
              evaluated_value = as.numeric(value),
              stringsAsFactors = FALSE
            )
            value <- previous
          } else {
            stopf("Equation for %s produced a non-finite value", target)
          }
        }
        next_value <- previous + damping * (value - previous)
        state$series[[target]][[period_pos]] <- next_value
        spec_delta <- abs(next_value - previous)
        if (spec_delta > 0) {
          changed_specs <- changed_specs + 1L
        }
        if (active_set_enabled && spec_delta > active_set_delta_threshold) {
          materially_changed_targets <- c(materially_changed_targets, target)
        }
        if (profile_this_period) {
          spec_elapsed <- as.numeric(proc.time()[["elapsed"]] - spec_started)
          profile_key <- paste(period_label, target, sep = "||")
          entry <- if (exists(profile_key, envir = spec_profile, inherits = FALSE)) {
            get(profile_key, envir = spec_profile, inherits = FALSE)
          } else {
            list(
              solve_stage = spec_profile_stage,
              period = period_label,
              target = target,
              eval_count = 0L,
              changed_count = 0L,
              fallback_count = 0L,
              total_elapsed_sec = 0.0,
              max_elapsed_sec = 0.0,
              last_iteration = 0L
            )
          }
          entry$eval_count <- as.integer(entry$eval_count) + 1L
          entry$changed_count <- as.integer(entry$changed_count) + if (abs(next_value - previous) > 0) 1L else 0L
          entry$fallback_count <- as.integer(entry$fallback_count) + if (nzchar(fallback_reason)) 1L else 0L
          entry$total_elapsed_sec <- as.numeric(entry$total_elapsed_sec) + spec_elapsed
          entry$max_elapsed_sec <- max(as.numeric(entry$max_elapsed_sec), spec_elapsed)
          entry$last_iteration <- as.integer(iter)
          assign(profile_key, entry, envir = spec_profile)
        }
        if (spec_delta >= max_delta) {
          max_delta <- spec_delta
          max_delta_target <- target
        }
      }
      next_active_targets <- ordered_targets
      if (active_set_enabled && as.integer(iter) >= active_set_start_iteration) {
        queue <- unique(as.character(materially_changed_targets))
        seen <- character()
        while (length(queue)) {
          node <- queue[[1L]]
          queue <- queue[-1L]
          if (!nzchar(node) || node %in% seen) {
            next
          }
          seen <- c(seen, node)
          queue <- c(queue, setdiff(as.character(reverse_dependency_map[[node]] %||% character()), seen))
        }
        next_active_targets <- unique(seen)
        if (!length(next_active_targets) && is.finite(max_delta) && max_delta > tolerance && nzchar(max_delta_target)) {
          queue <- max_delta_target
          seen <- character()
          while (length(queue)) {
            node <- queue[[1L]]
            queue <- queue[-1L]
            if (!nzchar(node) || node %in% seen) {
              next
            }
            seen <- c(seen, node)
            queue <- c(queue, setdiff(as.character(reverse_dependency_map[[node]] %||% character()), seen))
          }
          next_active_targets <- unique(seen)
        }
      }
      if (nzchar(iteration_profile_path) &&
        (!length(iteration_profile_periods) || period_label %in% iteration_profile_periods)) {
        iteration_profile_rows[[length(iteration_profile_rows) + 1L]] <- data.frame(
          solve_stage = spec_profile_stage,
          solver_policy = as.character(solver_policy),
          period = period_label,
          iteration = as.integer(iter),
          evaluated_specs = as.integer(length(current_specs)),
          changed_specs = as.integer(changed_specs),
          next_active_specs = as.integer(length(next_active_targets)),
          max_delta = as.numeric(max_delta),
          max_delta_target = as.character(max_delta_target),
          stringsAsFactors = FALSE
        )
      }
      if (iter >= min_iter && is.finite(max_delta) && max_delta <= tolerance) {
        converged <- TRUE
        break
      }
      if (active_set_enabled && as.integer(iter) >= active_set_start_iteration) {
        active_targets <- unique(as.character(next_active_targets))
      }
    }
    for (spec in ordered_specs) {
      target <- spec$target
      if (is.null(spec$resid_ar1)) {
        next
      }
      resid_ar1_states[[target]] <- commit_resid_ar1_state(
        spec,
        resid_ar1_states[[target]],
        source_state = source_state,
        solved_state = state,
        period_pos = period_pos,
        structural = as.numeric(period_structural[[target]])
      )
    }
    elapsed_sec <- as.numeric(proc.time()[["elapsed"]] - period_started)
    diagnostics[[diag_index]] <- data.frame(
      period = state$periods[[period_pos]],
      iterations = iter_used,
      converged = converged,
      max_delta = max_delta,
      termination = if (converged) "tolerance" else "max_iter",
      elapsed_sec = elapsed_sec,
      spec_count = length(ordered_specs),
      stringsAsFactors = FALSE
    )
    if (nzchar(progress_path)) {
      progress_row <- data.frame(
        solve_stage = progress_stage,
        period = state$periods[[period_pos]],
        iterations = iter_used,
        converged = converged,
        max_delta = max_delta,
        termination = if (converged) "tolerance" else "max_iter",
        elapsed_sec = elapsed_sec,
        spec_count = length(ordered_specs),
        stringsAsFactors = FALSE
      )
      utils::write.table(
        progress_row,
        file = progress_path,
        sep = ",",
        row.names = FALSE,
        col.names = !progress_has_header,
        append = progress_has_header,
        quote = TRUE
      )
      progress_has_header <- TRUE
    }
    diag_index <- diag_index + 1L
  }
  if (nzchar(spec_profile_path)) {
    keys <- ls(envir = spec_profile, all.names = TRUE)
    rows <- lapply(keys, function(key) {
      entry <- get(key, envir = spec_profile, inherits = FALSE)
      data.frame(
        solve_stage = as.integer(entry$solve_stage),
        period = as.character(entry$period),
        target = as.character(entry$target),
        eval_count = as.integer(entry$eval_count),
        changed_count = as.integer(entry$changed_count),
        fallback_count = as.integer(entry$fallback_count),
        total_elapsed_sec = as.numeric(entry$total_elapsed_sec),
        avg_elapsed_sec = as.numeric(entry$total_elapsed_sec) / max(1L, as.integer(entry$eval_count)),
        max_elapsed_sec = as.numeric(entry$max_elapsed_sec),
        last_iteration = as.integer(entry$last_iteration),
        stringsAsFactors = FALSE
      )
    })
    spec_profile_rows <- if (length(rows)) do.call(rbind, rows) else data.frame(
      solve_stage = integer(),
      period = character(),
      target = character(),
      eval_count = integer(),
      changed_count = integer(),
      fallback_count = integer(),
      total_elapsed_sec = numeric(),
      avg_elapsed_sec = numeric(),
      max_elapsed_sec = numeric(),
      last_iteration = integer(),
      stringsAsFactors = FALSE
    )
    utils::write.csv(spec_profile_rows, spec_profile_path, row.names = FALSE)
  }
  if (nzchar(iteration_profile_path)) {
    iteration_rows <- if (length(iteration_profile_rows)) do.call(rbind, iteration_profile_rows) else data.frame(
      solve_stage = integer(),
      solver_policy = character(),
      period = character(),
      iteration = integer(),
      evaluated_specs = integer(),
      changed_specs = integer(),
      next_active_specs = integer(),
      max_delta = numeric(),
      max_delta_target = character(),
      stringsAsFactors = FALSE
    )
    utils::write.csv(iteration_rows, iteration_profile_path, row.names = FALSE)
  }

  list(
    state = state,
    series = as_series_frame(state),
    diagnostics = do.call(rbind, diagnostics),
    fallback_audit = if (length(fallback_rows)) do.call(rbind, fallback_rows) else data.frame(
      period = character(),
      iteration = integer(),
      target = character(),
      fallback_reason = character(),
      previous_value = numeric(),
      evaluated_value = numeric(),
      stringsAsFactors = FALSE
    ),
    equation_input_trace = if (length(equation_input_rows)) do.call(rbind, equation_input_rows) else data.frame(
      period = character(),
      iteration = integer(),
      target = character(),
      trace_kind = character(),
      variable = character(),
      lag = integer(),
      source_name = character(),
      source_period = character(),
      value = numeric(),
      stringsAsFactors = FALSE
    ),
    specs = ordered_specs,
    control = control,
    order = order,
    resid_ar1_states = resid_ar1_states
  )
}
