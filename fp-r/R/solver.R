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
  diagnostics <- vector("list", period_window[["end"]] - period_window[["start"]] + 1L)
  diag_index <- 1L

  for (period_pos in seq.int(period_window[["start"]], period_window[["end"]])) {
    state <- seed_period_targets(state, ordered_specs, period_pos)
    period_structural <- list()
    converged <- FALSE
    max_delta <- Inf
    iter_used <- 0L
    for (iter in seq_len(max_iter)) {
      iter_used <- iter
      max_delta <- 0
      for (spec in ordered_specs) {
        target <- spec$target
        previous <- as.numeric(state$series[[target]][[period_pos]])
        evaluation <- suppressWarnings(evaluate_spec_at_period(
          spec,
          state,
          period_pos,
          strict = strict,
          resid_state = resid_ar1_states[[target]] %||% NULL
        ))
        value <- as.numeric(evaluation$value)
        period_structural[[target]] <- as.numeric(evaluation$structural)
        if (!is.finite(value)) {
          # Match the reduced fppy rho-aware boundary behavior by preserving
          # the existing value when the rho correction falls off available history.
          if (!is.null(spec$rho_terms) && nrow(spec$rho_terms) > 0L && is.finite(previous)) {
            value <- previous
          } else if (nzchar(as.character(spec$kind %||% "")) && is.finite(previous)) {
            value <- previous
          } else {
            stopf("Equation for %s produced a non-finite value", target)
          }
        }
        next_value <- previous + damping * (value - previous)
        state$series[[target]][[period_pos]] <- next_value
        max_delta <- max(max_delta, abs(next_value - previous))
      }
      if (iter >= min_iter && is.finite(max_delta) && max_delta <= tolerance) {
        converged <- TRUE
        break
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
    diagnostics[[diag_index]] <- data.frame(
      period = state$periods[[period_pos]],
      iterations = iter_used,
      converged = converged,
      max_delta = max_delta,
      termination = if (converged) "tolerance" else "max_iter",
      stringsAsFactors = FALSE
    )
    diag_index <- diag_index + 1L
  }

  list(
    state = state,
    series = as_series_frame(state),
    diagnostics = do.call(rbind, diagnostics),
    specs = ordered_specs,
    control = control,
    order = order,
    resid_ar1_states = resid_ar1_states
  )
}
