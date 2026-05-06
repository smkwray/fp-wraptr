tokenize_expression <- function(text) {
  chars <- strsplit(text, "", fixed = TRUE)[[1]]
  n <- length(chars)
  i <- 1L
  tokens <- list()
  push_token <- function(type, value) {
    tokens[[length(tokens) + 1L]] <<- list(type = type, value = value)
  }
  while (i <= n) {
    ch <- chars[[i]]
    if (grepl("\\s", ch, perl = TRUE)) {
      i <- i + 1L
      next
    }
    if (grepl("[A-Za-z_]", ch, perl = TRUE)) {
      start <- i
      i <- i + 1L
      while (i <= n && grepl("[A-Za-z0-9_]", chars[[i]], perl = TRUE)) {
        i <- i + 1L
      }
      push_token("identifier", paste(chars[start:(i - 1L)], collapse = ""))
      next
    }
    if (grepl("[0-9]", ch, perl = TRUE) ||
      (identical(ch, ".") && i < n && grepl("[0-9]", chars[[i + 1L]], perl = TRUE))) {
      start <- i
      i <- i + 1L
      while (i <= n && grepl("[0-9.]", chars[[i]], perl = TRUE)) {
        i <- i + 1L
      }
      push_token("number", paste(chars[start:(i - 1L)], collapse = ""))
      next
    }
    if (ch %in% c("(", ")", ",", "+", "-", "*", "/", "^")) {
      push_token("symbol", ch)
      i <- i + 1L
      next
    }
    stopf("Unsupported expression character: %s", ch)
  }
  tokens
}

normalize_fp_series_value_scalar <- function(value) {
  if (!is.finite(value)) {
    return(NA_real_)
  }
  if (abs(value + 99.0) <= 1e-12) {
    return(NA_real_)
  }
  value
}

coef_call_width <- function(tokens, index) {
  remaining <- length(tokens) - index + 1L
  if (remaining < 6L) {
    return(0L)
  }
  if (!identical(tokens[[index]]$type, "identifier") || !identical(toupper(tokens[[index]]$value), "COEF")) {
    return(0L)
  }
  if (!identical(tokens[[index + 1L]]$value, "(")) {
    return(0L)
  }

  cursor <- index + 2L
  read_int_arg <- function(position) {
    sign <- 1L
    current <- position
    if (tokens[[current]]$value %in% c("+", "-")) {
      sign <<- if (identical(tokens[[current]]$value, "-")) -1L else 1L
      current <- current + 1L
    }
    if (current > length(tokens) || !identical(tokens[[current]]$type, "number")) {
      return(NULL)
    }
    if (!grepl("^\\d+$", tokens[[current]]$value, perl = TRUE)) {
      return(NULL)
    }
    list(value = sign * as.integer(tokens[[current]]$value), next_pos = current + 1L)
  }

  row_arg <- read_int_arg(cursor)
  if (is.null(row_arg) || row_arg$next_pos > length(tokens) || !identical(tokens[[row_arg$next_pos]]$value, ",")) {
    return(0L)
  }
  col_arg <- read_int_arg(row_arg$next_pos + 1L)
  if (is.null(col_arg) || col_arg$next_pos > length(tokens) || !identical(tokens[[col_arg$next_pos]]$value, ")")) {
    return(0L)
  }
  as.integer(col_arg$next_pos - index + 1L)
}

parse_coef_call <- function(tokens, index) {
  width <- coef_call_width(tokens, index)
  if (width == 0L) {
    return(NULL)
  }
  inner <- tokens[(index + 2L):(index + width - 2L)]
  pieces <- vapply(inner, function(item) item$value, character(1))
  args <- strsplit(paste(pieces, collapse = " "), ",", fixed = TRUE)[[1]]
  if (length(args) != 2L) {
    return(NULL)
  }
  list(
    row = as.integer(trimws(args[[1]])),
    col = as.integer(trimws(args[[2]])),
    width = width
  )
}

lag_reference_width <- function(tokens, index) {
  remaining <- length(tokens) - index + 1L
  if (remaining < 4L) {
    return(0L)
  }
  if (!identical(tokens[[index]]$type, "identifier")) {
    return(0L)
  }
  if (!identical(tokens[[index + 1L]]$value, "(")) {
    return(0L)
  }
  if (remaining >= 5L &&
    tokens[[index + 2L]]$value %in% c("+", "-") &&
    identical(tokens[[index + 3L]]$type, "number") &&
    identical(tokens[[index + 4L]]$value, ")")) {
    return(5L)
  }
  if (identical(tokens[[index + 2L]]$type, "number") &&
    identical(tokens[[index + 3L]]$value, ")")) {
    return(4L)
  }
  0L
}

parse_lag_reference <- function(tokens, index) {
  width <- lag_reference_width(tokens, index)
  if (width == 0L) {
    return(NULL)
  }
  name <- tokens[[index]]$value
  if (width == 5L) {
    sign <- tokens[[index + 2L]]$value
    amount <- as.integer(tokens[[index + 3L]]$value)
    lag <- if (identical(sign, "-")) -amount else amount
  } else {
    lag <- as.integer(tokens[[index + 2L]]$value)
  }
  list(name = name, lag = lag, width = width)
}

compile_expression <- function(text) {
  normalized_text <- gsub("**", "^", as.character(text), fixed = TRUE)
  tokens <- tokenize_expression(normalized_text)
  fn_map <- c(LOG = "log", EXP = "exp", ABS = "abs", MAX = "max", MIN = "min")
  output <- character()
  references <- data.frame(
    name = character(),
    lag = integer(),
    stringsAsFactors = FALSE
  )
  i <- 1L
  while (i <= length(tokens)) {
    token <- tokens[[i]]
    if (identical(token$type, "identifier")) {
      coef_call <- parse_coef_call(tokens, i)
      if (!is.null(coef_call)) {
        output <- c(output, sprintf(".fp_coef(%dL, %dL)", coef_call$row, coef_call$col))
        i <- i + coef_call$width
        next
      }
      lag_ref <- parse_lag_reference(tokens, i)
      if (!is.null(lag_ref)) {
        output <- c(output, sprintf('.fp_value("%s", %dL)', lag_ref$name, lag_ref$lag))
        references <- rbind(
          references,
          data.frame(name = lag_ref$name, lag = lag_ref$lag, stringsAsFactors = FALSE)
        )
        i <- i + lag_ref$width
        next
      }
      next_is_call <- i < length(tokens) && identical(tokens[[i + 1L]]$value, "(")
      token_upper <- toupper(token$value)
      mapped_fn <- if (token_upper %in% names(fn_map)) unname(fn_map[[token_upper]]) else NULL
      if (!is.null(mapped_fn) && next_is_call) {
        output <- c(output, mapped_fn)
      } else {
        output <- c(output, sprintf('.fp_value("%s", 0L)', token$value))
        references <- rbind(
          references,
          data.frame(name = token$value, lag = 0L, stringsAsFactors = FALSE)
        )
      }
      i <- i + 1L
      next
    }
    output <- c(output, token$value)
      i <- i + 1L
    }
  r_expression <- paste(output, collapse = " ")
  scalar_eval_context <- new.env(parent = emptyenv())
  scalar_eval_env <- new.env(parent = baseenv())
  scalar_eval_context$state <- NULL
  scalar_eval_context$series_list <- NULL
  scalar_eval_context$period_index <- 1L
  scalar_eval_context$strict <- TRUE
  scalar_eval_context$series_overrides <- NULL
  scalar_eval_context$series_overrides_env <- NULL
  scalar_eval_context$has_series_overrides <- FALSE
  scalar_eval_context$lag_only_overrides <- FALSE
  scalar_eval_context$coef_values <- list()
  scalar_eval_context$metrics_env <- NULL
  scalar_eval_context$metrics_context <- list()
  scalar_eval_env$.fp_value <- function(name, lag = 0L) {
    metrics_enabled <- is.environment(scalar_eval_context$metrics_env)
    started <- if (metrics_enabled) proc.time()[["elapsed"]] else NA_real_
    source_name <- name
    if (isTRUE(scalar_eval_context$has_series_overrides) &&
      (!isTRUE(scalar_eval_context$lag_only_overrides) || lag != 0L) &&
      exists(name, envir = scalar_eval_context$series_overrides_env, inherits = FALSE)) {
      source_name <- get(name, envir = scalar_eval_context$series_overrides_env, inherits = FALSE)
    }
    series <- scalar_eval_context$series_list[[source_name]]
    if (is.null(series)) {
      if (isTRUE(scalar_eval_context$strict)) {
        stopf("Unknown series referenced in expression: %s", source_name)
      }
      return(NA_real_)
    }
    target_index <- scalar_eval_context$period_index + lag
    if (target_index < 1L || target_index > length(series)) {
      if (isTRUE(scalar_eval_context$strict)) {
        stopf("Reference %s(%d) is outside the available period range", name, lag)
      }
      return(NA_real_)
    }
    value <- normalize_fp_series_value_scalar(series[[target_index]])
    if (!is.finite(value) && isTRUE(scalar_eval_context$strict)) {
      stopf("Series %s is non-finite at period position %d", name, target_index)
    }
    if (metrics_enabled) {
      record_expression_metric(
        scalar_eval_context$metrics_env,
        scalar_eval_context$metrics_context,
        component = "fp_value",
        elapsed_sec = as.numeric(proc.time()[["elapsed"]] - started)
      )
    }
    value
  }
  scalar_eval_env$.fp_coef <- function(row, col) {
    metrics_enabled <- is.environment(scalar_eval_context$metrics_env)
    started <- if (metrics_enabled) proc.time()[["elapsed"]] else NA_real_
    key <- paste0(row, ",", col)
    coef_values <- scalar_eval_context$coef_values
    value <- if (is.list(coef_values) && !is.null(coef_values[[key]])) {
      coef_values[[key]]
    } else if (is.environment(coef_values) && exists(key, envir = coef_values, inherits = FALSE)) {
      get(key, envir = coef_values, inherits = FALSE)
    } else if (is.numeric(coef_values) && !is.null(names(coef_values)) && key %in% names(coef_values)) {
      coef_values[[key]]
    } else {
      0.0
    }
    if (metrics_enabled) {
      record_expression_metric(
        scalar_eval_context$metrics_env,
        scalar_eval_context$metrics_context,
        component = "fp_coef",
        elapsed_sec = as.numeric(proc.time()[["elapsed"]] - started)
      )
    }
    value
  }
  scalar_eval_fn <- eval(
    call("function", as.pairlist(alist()), parse(text = r_expression)[[1]]),
    envir = scalar_eval_env,
    enclos = baseenv()
  )
  scalar_eval_fn <- tryCatch(
    compiler::cmpfun(scalar_eval_fn),
    error = function(...) scalar_eval_fn
  )
  list(
    original_text = as.character(text),
    normalized_text = normalized_text,
    r_expression = r_expression,
    parsed_expression = parse(text = r_expression)[[1]],
    tokens = tokens,
    references = references,
    scalar_eval_context = scalar_eval_context,
    scalar_eval_env = scalar_eval_env,
    scalar_eval_fn = scalar_eval_fn
  )
}

record_expression_metric <- function(metrics_env, context, component, elapsed_sec, count = 1L) {
  if (is.null(metrics_env) || !is.environment(metrics_env)) {
    return(invisible(NULL))
  }
  period <- as.character(context$period %||% "")
  target <- as.character(context$target %||% "")
  key <- paste(period, target, as.character(component), sep = "||")
  entry <- if (exists(key, envir = metrics_env, inherits = FALSE)) {
    get(key, envir = metrics_env, inherits = FALSE)
  } else {
    list(
      period = period,
      target = target,
      component = as.character(component),
      call_count = 0L,
      total_elapsed_sec = 0.0
    )
  }
  entry$call_count <- as.integer(entry$call_count) + as.integer(count)
  entry$total_elapsed_sec <- as.numeric(entry$total_elapsed_sec) + as.numeric(elapsed_sec %||% 0.0)
  assign(key, entry, envir = metrics_env)
  invisible(NULL)
}

expression_metrics_as_frame <- function(metrics_env) {
  if (is.null(metrics_env) || !is.environment(metrics_env)) {
    return(data.frame(
      period = character(),
      target = character(),
      component = character(),
      call_count = integer(),
      total_elapsed_sec = numeric(),
      avg_elapsed_sec = numeric(),
      stringsAsFactors = FALSE
    ))
  }
  keys <- ls(envir = metrics_env, all.names = TRUE)
  if (!length(keys)) {
    return(data.frame(
      period = character(),
      target = character(),
      component = character(),
      call_count = integer(),
      total_elapsed_sec = numeric(),
      avg_elapsed_sec = numeric(),
      stringsAsFactors = FALSE
    ))
  }
  rows <- lapply(keys, function(key) {
    entry <- get(key, envir = metrics_env, inherits = FALSE)
    call_count <- as.integer(entry$call_count %||% 0L)
    total_elapsed_sec <- as.numeric(entry$total_elapsed_sec %||% 0.0)
    data.frame(
      period = as.character(entry$period %||% ""),
      target = as.character(entry$target %||% ""),
      component = as.character(entry$component %||% ""),
      call_count = call_count,
      total_elapsed_sec = total_elapsed_sec,
      avg_elapsed_sec = total_elapsed_sec / max(1L, call_count),
      stringsAsFactors = FALSE
    )
  })
  rows <- do.call(rbind, rows)
  rows[order(rows$period, rows$target, rows$component), , drop = FALSE]
}

evaluate_compiled_expression <- function(
  compiled,
  state,
  period_index,
  strict = TRUE,
  series_overrides = NULL,
  lag_only_overrides = FALSE,
  metrics_env = NULL,
  metrics_context = NULL
) {
  expr <- compiled$parsed_expression %||% parse(text = compiled$r_expression)[[1]]
  coef_values <- state$coef_values %||% state$coefficients %||% list()
  metrics_context <- metrics_context %||% list()
  metrics_enabled <- is.environment(metrics_env)
  prepared_eval_context <- compiled$scalar_eval_context %||% NULL
  prepared_eval_env <- compiled$scalar_eval_env %||% NULL
  prepared_eval_fn <- compiled$scalar_eval_fn %||% NULL
  if (!is.null(prepared_eval_context) && is.environment(prepared_eval_context) &&
      !is.null(prepared_eval_env) && is.environment(prepared_eval_env) &&
      !is.null(prepared_eval_fn) && is.function(prepared_eval_fn)) {
    prepared_eval_context$state <- state
    prepared_eval_context$period_index <- as.integer(period_index)
    prepared_eval_context$strict <- isTRUE(strict)
    prepared_eval_context$series_overrides <- series_overrides
    prepared_eval_context$series_list <- state$series
    prepared_eval_context$has_series_overrides <- !is.null(series_overrides) && length(series_overrides) > 0L
    if (isTRUE(prepared_eval_context$has_series_overrides)) {
      overrides_env <- new.env(parent = emptyenv())
      for (override_name in names(series_overrides)) {
        assign(override_name, as.character(series_overrides[[override_name]]), envir = overrides_env)
      }
      prepared_eval_context$series_overrides_env <- overrides_env
    } else {
      prepared_eval_context$series_overrides_env <- NULL
    }
    prepared_eval_context$lag_only_overrides <- isTRUE(lag_only_overrides)
    prepared_eval_context$coef_values <- coef_values
    prepared_eval_context$metrics_env <- metrics_env
    prepared_eval_context$metrics_context <- metrics_context
    eval_started <- if (metrics_enabled) proc.time()[["elapsed"]] else NA_real_
    value <- prepared_eval_fn()
    if (metrics_enabled) {
      record_expression_metric(
        metrics_env,
        metrics_context,
        component = "eval",
        elapsed_sec = as.numeric(proc.time()[["elapsed"]] - eval_started)
      )
    }
    return(value)
  }
  normalize_fp_series_value <- function(value) {
    normalize_fp_series_value_scalar(value)
  }
  .fp_value <- function(name, lag = 0L) {
    started <- if (metrics_enabled) proc.time()[["elapsed"]] else NA_real_
    source_name <- name
    if (!is.null(series_overrides) &&
      name %in% names(series_overrides) &&
      (!isTRUE(lag_only_overrides) || as.integer(lag) != 0L)) {
      source_name <- as.character(series_overrides[[name]])
    }
    series <- state$series[[source_name]]
    if (is.null(series)) {
      if (strict) {
        stopf("Unknown series referenced in expression: %s", source_name)
      }
      return(NA_real_)
    }
    target_index <- as.integer(period_index) + as.integer(lag)
    if (target_index < 1L || target_index > length(series)) {
      if (strict) {
        stopf("Reference %s(%d) is outside the available period range", name, lag)
      }
      return(NA_real_)
    }
    value <- normalize_fp_series_value(series[[target_index]])
    if (!is.finite(value) && strict) {
      stopf("Series %s is non-finite at period position %d", name, target_index)
    }
    if (metrics_enabled) {
      record_expression_metric(
        metrics_env,
        metrics_context,
        component = "fp_value",
        elapsed_sec = as.numeric(proc.time()[["elapsed"]] - started)
      )
    }
    value
  }
  .fp_coef <- function(row, col) {
    started <- if (metrics_enabled) proc.time()[["elapsed"]] else NA_real_
    key <- sprintf("%d,%d", as.integer(row), as.integer(col))
    value <- if (is.list(coef_values) && !is.null(coef_values[[key]])) {
      as.numeric(coef_values[[key]])
    } else if (is.environment(coef_values) && exists(key, envir = coef_values, inherits = FALSE)) {
      as.numeric(get(key, envir = coef_values, inherits = FALSE))
    } else if (is.numeric(coef_values) && !is.null(names(coef_values)) && key %in% names(coef_values)) {
      as.numeric(coef_values[[key]])
    } else {
      0.0
    }
    if (metrics_enabled) {
      record_expression_metric(
        metrics_env,
        metrics_context,
        component = "fp_coef",
        elapsed_sec = as.numeric(proc.time()[["elapsed"]] - started)
      )
    }
    value
  }
  eval_started <- if (metrics_enabled) proc.time()[["elapsed"]] else NA_real_
  value <- eval(
    expr,
    envir = list(.fp_value = .fp_value, .fp_coef = .fp_coef),
    enclos = baseenv()
  )
  if (metrics_enabled) {
    record_expression_metric(
      metrics_env,
      metrics_context,
      component = "eval",
      elapsed_sec = as.numeric(proc.time()[["elapsed"]] - eval_started)
    )
  }
  value
}

evaluate_compiled_expression_positions <- function(
  compiled,
  state,
  period_indices,
  strict = TRUE,
  series_overrides = NULL,
  lag_only_overrides = FALSE
) {
  period_indices <- as.integer(period_indices %||% integer())
  if (!length(period_indices)) {
    return(numeric())
  }
  expr <- compiled$parsed_expression %||% parse(text = compiled$r_expression)[[1]]
  coef_values <- state$coef_values %||% state$coefficients %||% list()
  normalize_fp_series_values <- function(values) {
    numeric_values <- as.numeric(values)
    numeric_values[!is.finite(numeric_values)] <- NA_real_
    numeric_values[abs(numeric_values + 99.0) <= 1e-12] <- NA_real_
    numeric_values
  }
  .fp_value <- function(name, lag = 0L) {
    source_name <- name
    if (!is.null(series_overrides) &&
      name %in% names(series_overrides) &&
      (!isTRUE(lag_only_overrides) || as.integer(lag) != 0L)) {
      source_name <- as.character(series_overrides[[name]])
    }
    series <- state$series[[source_name]]
    if (is.null(series)) {
      if (strict) {
        stopf("Unknown series referenced in expression: %s", source_name)
      }
      return(rep(NA_real_, length(period_indices)))
    }
    target_indices <- period_indices + as.integer(lag)
    if (strict && any(target_indices < 1L | target_indices > length(series))) {
      stopf("Reference %s(%d) is outside the available period range", name, lag)
    }
    values <- rep(NA_real_, length(target_indices))
    in_range <- target_indices >= 1L & target_indices <= length(series)
    if (any(in_range)) {
      values[in_range] <- as.numeric(series[target_indices[in_range]])
    }
    values <- normalize_fp_series_values(values)
    if (strict && any(!is.finite(values))) {
      bad_index <- which(!is.finite(values))[1L]
      stopf("Series %s is non-finite at period position %d", name, target_indices[[bad_index]])
    }
    values
  }
  .fp_coef <- function(row, col) {
    key <- sprintf("%d,%d", as.integer(row), as.integer(col))
    if (is.list(coef_values) && !is.null(coef_values[[key]])) {
      return(as.numeric(coef_values[[key]]))
    }
    if (is.environment(coef_values) && exists(key, envir = coef_values, inherits = FALSE)) {
      return(as.numeric(get(key, envir = coef_values, inherits = FALSE)))
    }
    if (is.numeric(coef_values) && !is.null(names(coef_values)) && key %in% names(coef_values)) {
      return(as.numeric(coef_values[[key]]))
    }
    0.0
  }
  eval(
    expr,
    envir = list(
      .fp_value = .fp_value,
      .fp_coef = .fp_coef,
      max = pmax,
      min = pmin,
      log = log,
      exp = exp,
      abs = abs
    ),
    enclos = baseenv()
  )
}

evaluate_expression <- function(
  text,
  state,
  period_index,
  strict = TRUE,
  series_overrides = NULL,
  lag_only_overrides = FALSE
) {
  compiled <- compile_expression(text)
  evaluate_compiled_expression(
    compiled,
    state,
    period_index,
    strict = strict,
    series_overrides = series_overrides,
    lag_only_overrides = lag_only_overrides
  )
}
