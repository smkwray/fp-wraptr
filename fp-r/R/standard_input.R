clean_fp_filename <- function(token) {
  raw <- trimws(as.character(token %||% ""))
  raw <- gsub("^['\"]|['\"]$", "", raw)
  raw <- sub(";\\s*$", "", raw)
  raw
}

extract_fp_named_arg <- function(statement, key = "FILE") {
  pattern <- sprintf("\\b%s\\s*=\\s*([^\\s;]+)", toupper(key))
  matches <- regexec(pattern, statement, ignore.case = TRUE, perl = TRUE)
  parts <- regmatches(statement, matches)[[1]]
  if (length(parts) < 2L) {
    return(NULL)
  }
  clean_fp_filename(parts[[2]])
}

extract_fp_file_arg <- function(statement, key = "FILE") {
  extract_fp_named_arg(statement, key = key)
}

parse_smpl_statement <- function(statement) {
  tokens <- strsplit(trimws(gsub(";", " ", statement, fixed = TRUE)), "\\s+", perl = TRUE)[[1]]
  if (length(tokens) < 3L || !identical(toupper(tokens[[1]]), "SMPL")) {
    return(NULL)
  }
  list(
    start = clean_fp_filename(tokens[[2]]),
    end = clean_fp_filename(tokens[[3]])
  )
}

frame_from_state <- function(state) {
  as_series_frame(ensure_state(state))
}

resolve_frame_column_name <- function(frame, name) {
  target <- toupper(as.character(name))
  for (column in names(frame)) {
    if (toupper(as.character(column)) == target) {
      return(as.character(column))
    }
  }
  as.character(name)
}

order_period_values <- function(periods) {
  vapply(periods, function(period) parse_period(period)$index, integer(1))
}

sort_frame_by_period <- function(frame) {
  if (!nrow(frame) || !"period" %in% names(frame)) {
    return(frame)
  }
  ordered <- order(order_period_values(as.character(frame$period)))
  frame[ordered, , drop = FALSE]
}

ensure_frame_periods <- function(frame, periods) {
  if (!length(periods)) {
    return(frame)
  }
  base_periods <- if ("period" %in% names(frame)) as.character(frame$period) else character()
  all_periods <- unique(c(base_periods, as.character(periods)))
  out <- data.frame(
    period = all_periods,
    stringsAsFactors = FALSE,
    check.names = FALSE
  )
  out <- sort_frame_by_period(out)
  for (name in setdiff(names(frame), "period")) {
    values <- rep(NA_real_, nrow(out))
    names(values) <- out$period
    values[base_periods] <- as.numeric(frame[[name]])
    out[[name]] <- as.numeric(values)
  }
  out
}

window_periods_in_frame <- function(frame, window_start, window_end) {
  if (!nrow(frame)) {
    return(character())
  }
  periods <- seq_periods(window_start, window_end)
  periods[periods %in% as.character(frame$period)]
}

apply_extrapolate_frame <- function(frame, window_start, window_end, variables = character(), include_all_columns = TRUE) {
  if (!nrow(frame)) {
    return(frame)
  }
  working <- ensure_frame_periods(frame, seq_periods(window_start, window_end))
  working <- sort_frame_by_period(working)
  periods <- window_periods_in_frame(working, window_start, window_end)
  if (!length(periods)) {
    return(working)
  }

  variable_names <- unique(as.character(variables))
  if (include_all_columns) {
    variable_names <- unique(c(variable_names, setdiff(names(working), "period")))
  }
  for (name in variable_names) {
    if (!(name %in% names(working))) {
      working[[name]] <- NA_real_
    }
    values <- as.numeric(working[[name]])
    for (period in periods) {
      pos <- match(period, working$period)
      if (is.na(pos) || pos <= 1L) {
        next
      }
      current <- values[[pos]]
      if (is.finite(current)) {
        next
      }
      previous <- values[[pos - 1L]]
      if (is.finite(previous)) {
        values[[pos]] <- previous
      }
    }
    working[[name]] <- values
  }

  working
}

apply_setyytoy_frame <- function(frame, window_start, window_end) {
  source_column <- resolve_frame_column_name(frame, "Y")
  target_column <- resolve_frame_column_name(frame, "YY")
  if (!(source_column %in% names(frame))) {
    return(frame)
  }
  periods <- window_periods_in_frame(frame, window_start, window_end)
  if (!length(periods)) {
    return(frame)
  }
  working <- frame
  if (!(target_column %in% names(working))) {
    working[[target_column]] <- NA_real_
  }
  src_values <- as.numeric(working[[source_column]])
  names(src_values) <- working$period
  positions <- match(periods, working$period)
  working[[target_column]][positions] <- src_values[periods]
  working
}

format_printvar_load_value <- function(value) {
  numeric <- as.numeric(value)
  if (!is.finite(numeric)) {
    return("-99")
  }
  if (identical(numeric, 0.0)) {
    return(" 0.00000000000E+00")
  }
  abs_value <- abs(numeric)
  exponent <- floor(log10(abs_value)) + 1
  mantissa <- numeric / (10.0^exponent)
  if (abs(mantissa) >= 1.0) {
    mantissa <- mantissa / 10.0
    exponent <- exponent + 1
  }
  sprintf("% .11fE%+03d", mantissa, exponent)
}

infer_printvar_variable_order <- function(frame, fmout_path = NULL, fallback_paths = character()) {
  columns <- setdiff(names(frame), "period")
  if (!length(columns)) {
    return(character())
  }
  columns_upper <- setNames(columns, toupper(columns))
  excluded_names <- c("MAXITERS", "MINITERS", "MAXCHECK", "MAXVAR", "MAXS", "MAXCOEF", "MAXFSR")

  if (!is.null(fmout_path) && nzchar(fmout_path) && file.exists(fmout_path)) {
    pair_pattern <- "([A-Za-z][A-Za-z0-9_]*)\\s+(\\d{1,4})"
    var_to_idx <- numeric()
    names(var_to_idx) <- character()
    for (raw in readLines(fmout_path, warn = FALSE, encoding = "UTF-8")) {
      pair_hits <- regmatches(raw, gregexpr(pair_pattern, raw, perl = TRUE))[[1]]
      if (length(pair_hits) < 2L) {
        next
      }
      for (pair in pair_hits) {
        parts <- strsplit(trimws(pair), "\\s+", perl = TRUE)[[1]]
        if (length(parts) != 2L) {
          next
        }
        name <- toupper(parts[[1]])
        idx <- suppressWarnings(as.integer(parts[[2]]))
        if (!is.finite(idx) || idx <= 0L || idx > 500L || name %in% excluded_names) {
          next
        }
        existing <- if (name %in% names(var_to_idx)) as.integer(var_to_idx[[name]]) else NA_integer_
        if (!is.finite(existing) || idx < existing) {
          var_to_idx[[name]] <- idx
        }
      }
    }
    if (length(var_to_idx)) {
      ordered <- names(sort(var_to_idx, decreasing = FALSE))
      selected <- unname(columns_upper[ordered[ordered %in% names(columns_upper)]])
      selected <- selected[nzchar(selected)]
      if (length(selected)) {
        return(selected)
      }
    }
  }

  load_pattern <- "^\\s*LOAD\\s+([A-Za-z0-9_]+)\\b"
  ordered <- character()
  for (path in unique(as.character(fallback_paths %||% character()))) {
    if (!nzchar(path) || !file.exists(path)) {
      next
    }
    for (raw in readLines(path, warn = FALSE, encoding = "UTF-8")) {
      match <- regexec(load_pattern, raw, perl = TRUE, ignore.case = TRUE)
      parts <- regmatches(raw, match)[[1]]
      if (length(parts) != 2L) {
        next
      }
      variable <- toupper(trimws(parts[[2]]))
      if (!nzchar(variable) || variable %in% ordered) {
        next
      }
      ordered <- c(ordered, variable)
    }
  }
  if (!length(ordered)) {
    return(character())
  }
  selected <- unname(columns_upper[ordered[ordered %in% names(columns_upper)]])
  selected <- selected[nzchar(selected)]
  c(selected, columns[!columns %in% selected])
}

write_printvar_loadformat <- function(frame, output_path, variables = character(), active_window = NULL, fmout_path = NULL, fallback_paths = character()) {
  working <- sort_frame_by_period(frame)
  if (!is.null(active_window) && length(active_window) >= 2L) {
    periods <- seq_periods(active_window[[1]], active_window[[2]])
    working <- ensure_frame_periods(working, periods)
    working <- working[working$period %in% periods, , drop = FALSE]
    working <- sort_frame_by_period(working)
  }
  selected <- character()
  if (length(variables)) {
    for (variable in variables) {
      resolved <- resolve_frame_column_name(working, variable)
      if (resolved %in% names(working)) {
        selected <- c(selected, resolved)
      }
    }
    selected <- unique(selected)
  } else {
    selected <- infer_printvar_variable_order(
      working,
      fmout_path = fmout_path,
      fallback_paths = fallback_paths
    )
    if (!length(selected)) {
      selected <- setdiff(names(working), "period")
    }
  }
  con <- file(output_path, open = "wt", encoding = "UTF-8")
  on.exit(close(con), add = TRUE)
  if (!nrow(working) || !length(selected)) {
    writeLines("", con)
    return(invisible(output_path))
  }
  writeLines(sprintf(" SMPL    %s   %s ;", working$period[[1]], working$period[[nrow(working)]]), con)
  for (variable in selected) {
    writeLines(sprintf(" LOAD %-8s ;", variable), con)
    values <- as.numeric(working[[variable]])
    chunks <- split(vapply(values, format_printvar_load_value, character(1)), ceiling(seq_along(values) / 4L))
    for (chunk in chunks) {
      writeLines(sprintf("  %s", paste(chunk, collapse = " ")), con)
    }
    writeLines(" 'END' ", con)
  }
  writeLines(" END;", con)
  invisible(output_path)
}

window_frame_for_printvar <- function(frame, active_window = NULL) {
  working <- sort_frame_by_period(frame)
  if (is.null(active_window) || length(active_window) < 2L) {
    return(working)
  }
  periods <- window_periods_in_frame(working, active_window[[1]], active_window[[2]])
  working[working$period %in% periods, , drop = FALSE]
}

resolve_generated_output_path <- function(default_name, work_dir) {
  base_name <- clean_fp_filename(default_name)
  candidate <- resolve_runtime_output_path(base_name, work_dir)
  if (!file.exists(candidate)) {
    return(candidate)
  }
  stem <- sub("\\.[^.]+$", "", base_name, perl = TRUE)
  ext <- sub("^.*?(\\.[^.]+)$", "\\1", base_name, perl = TRUE)
  if (identical(ext, base_name)) {
    ext <- ""
  }
  counter <- 2L
  repeat {
    candidate_name <- sprintf("%s_%02d%s", stem, counter, ext)
    candidate <- resolve_runtime_output_path(candidate_name, work_dir)
    if (!file.exists(candidate)) {
      return(candidate)
    }
    counter <- counter + 1L
  }
}

empty_printvar_stats_frame <- function() {
  data.frame(
    variable = character(),
    sample_start = character(),
    sample_end = character(),
    rows = integer(),
    non_missing = integer(),
    mean = numeric(),
    sd = numeric(),
    min = numeric(),
    max = numeric(),
    first = numeric(),
    last = numeric(),
    stringsAsFactors = FALSE
  )
}

build_printvar_stats_frame <- function(frame, variables = character(), active_window = NULL) {
  working <- window_frame_for_printvar(frame, active_window = active_window)
  if (!nrow(working)) {
    return(empty_printvar_stats_frame())
  }

  selected <- character()
  if (length(variables)) {
    for (variable in variables) {
      resolved <- resolve_frame_column_name(working, variable)
      if (resolved %in% names(working)) {
        selected <- c(selected, resolved)
      }
    }
    selected <- unique(selected)
  } else {
    selected <- setdiff(names(working), "period")
  }
  if (!length(selected)) {
    return(empty_printvar_stats_frame())
  }

  sample_start <- as.character(working$period[[1]])
  sample_end <- as.character(working$period[[nrow(working)]])
  rows <- lapply(selected, function(variable) {
    values <- as.numeric(working[[variable]])
    finite <- is.finite(values)
    finite_values <- values[finite]
    data.frame(
      variable = as.character(variable),
      sample_start = sample_start,
      sample_end = sample_end,
      rows = length(values),
      non_missing = sum(finite),
      mean = if (length(finite_values)) mean(finite_values) else NA_real_,
      sd = if (length(finite_values) >= 2L) stats::sd(finite_values) else NA_real_,
      min = if (length(finite_values)) min(finite_values) else NA_real_,
      max = if (length(finite_values)) max(finite_values) else NA_real_,
      first = if (length(finite_values)) finite_values[[1]] else NA_real_,
      last = if (length(finite_values)) finite_values[[length(finite_values)]] else NA_real_,
      stringsAsFactors = FALSE
    )
  })
  do.call(rbind, rows)
}

write_printvar_stats <- function(frame, output_path, variables = character(), active_window = NULL) {
  stats_frame <- build_printvar_stats_frame(
    frame,
    variables = variables,
    active_window = active_window
  )
  write.csv(stats_frame, output_path, row.names = FALSE)
  invisible(output_path)
}

build_printvar_table_frame <- function(frame, variables = character(), active_window = NULL) {
  working <- window_frame_for_printvar(frame, active_window = active_window)
  if (!nrow(working)) {
    return(data.frame(period = character(), stringsAsFactors = FALSE, check.names = FALSE))
  }
  if (!length(variables)) {
    return(working)
  }

  selected <- character()
  for (variable in variables) {
    resolved <- resolve_frame_column_name(working, variable)
    if (resolved %in% names(working)) {
      selected <- c(selected, resolved)
    }
  }
  selected <- unique(selected)
  working[, c("period", selected), drop = FALSE]
}

write_printvar_table <- function(frame, output_path, variables = character(), active_window = NULL) {
  table_frame <- build_printvar_table_frame(
    frame,
    variables = variables,
    active_window = active_window
  )
  write.csv(table_frame, output_path, row.names = FALSE)
  invisible(output_path)
}

parse_printvar_statement <- function(statement) {
  text <- trimws(as.character(statement %||% ""))
  if (!nzchar(text)) {
    return(NULL)
  }
  tokens <- strsplit(gsub(";", " ", text, fixed = TRUE), "\\s+", perl = TRUE)[[1]]
  tokens <- tokens[nzchar(tokens)]
  if (!length(tokens) || !identical(toupper(tokens[[1]]), "PRINTVAR")) {
    return(NULL)
  }
  fileout <- extract_fp_named_arg(text, key = "FILEOUT")
  if (is.null(fileout)) {
    fileout <- extract_fp_named_arg(text, key = "FILE")
  }
  loadformat <- any(toupper(tokens) == "LOADFORMAT")
  stats <- any(toupper(tokens) == "STATS")
  variables <- character()
  if (length(tokens) > 1L) {
    for (token in tokens[-1L]) {
      upper <- toupper(token)
      if (upper %in% c("LOADFORMAT", "STATS")) {
        next
      }
      if (grepl("=", token, fixed = TRUE)) {
        next
      }
      variables <- c(variables, clean_fp_filename(token))
    }
  }
  list(
    fileout = fileout,
    loadformat = loadformat,
    stats = stats,
    variables = unique(variables)
  )
}

resolve_printvar_fallback_paths <- function(source_info = NULL, search_dirs = NULL) {
  if (is.null(source_info)) {
    return(character())
  }
  resolved <- as.character(Filter(
    function(path) !is.null(path) && nzchar(path) && file.exists(path),
    c(source_info$fmdata %||% NULL, source_info$fmexog %||% NULL)
  ))
  loaddata_names <- unique(as.character(source_info$loaddata %||% character()))
  if (length(loaddata_names)) {
    for (name in loaddata_names) {
      resolved_path <- resolve_fp_source_path(name, search_dirs %||% character())
      if (!is.null(resolved_path) && nzchar(resolved_path) && file.exists(resolved_path)) {
        resolved <- c(resolved, resolved_path)
      }
    }
  }
  unique(normalizePath(resolved, winslash = "/", mustWork = FALSE))
}

parse_solve_statement_runtime <- function(statement) {
  text <- trimws(as.character(statement %||% ""))
  if (!nzchar(text) || !grepl("^SOLVE\\b", text, ignore.case = TRUE, perl = TRUE)) {
    return(NULL)
  }
  text <- sub("^SOLVE\\b", "", text, ignore.case = TRUE, perl = TRUE)
  text <- gsub(";", " ", text, fixed = TRUE)
  tokens <- strsplit(text, "\\s+", perl = TRUE)[[1]]
  tokens <- tokens[nzchar(tokens)]
  if (!length(tokens)) {
    return(list(
      command = "SOLVE",
      kind = "control",
      name = "SOLVE",
      body = "",
      raw = trimws(as.character(statement %||% "")),
      solve_options = list(),
      options = list(),
      watch_variables = character(),
      option_text = "",
      watch_text = ""
    ))
  }

  options <- list(
    dynamic = FALSE,
    outside = FALSE,
    filevar = NULL,
    noreset = FALSE
  )
  watch_variables <- character()
  seen_watch_payload <- FALSE
  for (token in tokens) {
    upper <- toupper(token)
    if (!seen_watch_payload && identical(upper, "DYNAMIC")) {
      options$dynamic <- TRUE
      next
    }
    if (!seen_watch_payload && identical(upper, "OUTSIDE")) {
      options$outside <- TRUE
      next
    }
    if (!seen_watch_payload && identical(upper, "NORESET")) {
      options$noreset <- TRUE
      next
    }
    if (!seen_watch_payload && startsWith(upper, "FILEVAR=")) {
      options$filevar <- clean_fp_filename(sub("^FILEVAR=", "", token, ignore.case = TRUE, perl = TRUE))
      next
    }
    if (!seen_watch_payload && grepl("=", token, fixed = TRUE)) {
      seen_watch_payload <- TRUE
      next
    }
    if (grepl("^[A-Za-z][A-Za-z0-9_]*$", token, perl = TRUE)) {
      seen_watch_payload <- TRUE
      watch_variables <- c(watch_variables, clean_fp_filename(token))
    }
  }

  watch_variables <- unique(watch_variables[nzchar(watch_variables)])
  list(
    command = "SOLVE",
    kind = "control",
    name = "SOLVE",
    body = trimws(sub("^[A-Za-z]+\\s*", "", trimws(as.character(statement %||% "")))),
    raw = trimws(as.character(statement %||% "")),
    solve_options = options,
    options = options,
    watch_variables = watch_variables,
    option_text = paste(
      c(
        if (isTRUE(options$dynamic)) "dynamic=TRUE" else "dynamic=FALSE",
        if (isTRUE(options$outside)) "outside=TRUE" else "outside=FALSE",
        if (nzchar(options$filevar %||% "")) sprintf("filevar=%s", options$filevar) else "filevar=",
        if (isTRUE(options$noreset)) "noreset=TRUE" else "noreset=FALSE"
      ),
      collapse = ";"
    ),
    watch_text = paste(watch_variables, collapse = ",")
  )
}

extract_watch_variables_from_statement <- function(statement) {
  raw <- if (is.list(statement)) {
    as.character(statement$raw %||% statement$name %||% "")
  } else {
    as.character(statement %||% "")
  }
  if (!length(raw)) {
    raw <- ""
  }
  raw <- raw[[1]]
  normalized <- trimws(raw)
  if (!nzchar(normalized)) {
    return(character())
  }
  lines <- strsplit(gsub("\r", "", normalized), "\n", fixed = TRUE)[[1]]
  lines <- trimws(lines)
  lines <- lines[nzchar(lines)]
  if (!length(lines)) {
    return(character())
  }
  if (!all(grepl("^[A-Za-z][A-Za-z0-9_]*$", lines, perl = TRUE))) {
    return(character())
  }
  reserved_commands <- c(
    "CHANGEVAR", "CREATE", "END", "ENDOGENOUS", "EQ", "EST", "EXOGENOUS", "EXTRAPOLATE",
    "GENR", "IDENT", "INPUT", "LHS", "LOADDATA", "MODEQ", "PRINTMODEL",
    "PRINTNAMES", "PRINTVAR", "QUIT", "RETURN", "SETUPEST", "SETUPSOLVE", "SETYYTOY",
    "SMPL", "SOLVE", "SPACE", "TEST", "US"
  )
  if (any(toupper(lines) %in% reserved_commands)) {
    return(character())
  }
  unique(lines)
}

is_bare_watch_statement <- function(statement) {
  length(extract_watch_variables_from_statement(statement)) > 0L
}

solve_statement_metadata <- function(statement, following_statements = list()) {
  parsed <- statement$solve_metadata %||% parse_solve_statement_runtime(statement$raw %||% "")
  options <- parsed$solve_options %||% parsed$options %||% list(dynamic = FALSE, outside = FALSE, filevar = NULL, noreset = FALSE)
  watch_variables <- as.character(statement$watch_variables %||% parsed$watch_variables %||% character())
  if (!length(watch_variables) && length(following_statements)) {
    for (candidate in following_statements) {
      candidate_watch <- extract_watch_variables_from_statement(candidate)
      if (!length(candidate_watch)) {
        break
      }
      watch_variables <- c(watch_variables, candidate_watch)
    }
  }
  watch_variables <- unique(watch_variables[nzchar(watch_variables)])
  list(
    options = options,
    watch_variables = watch_variables,
    option_text = parsed$option_text %||% paste(
      c(
        if (isTRUE(options$dynamic)) "dynamic=TRUE" else "dynamic=FALSE",
        if (isTRUE(options$outside)) "outside=TRUE" else "outside=FALSE",
        if (nzchar(options$filevar %||% "")) sprintf("filevar=%s", options$filevar) else "filevar=",
        if (isTRUE(options$noreset)) "noreset=TRUE" else "noreset=FALSE"
      ),
      collapse = ";"
    ),
    watch_text = paste(watch_variables, collapse = ",")
  )
}

resolve_standard_input_semantics_profile <- function(profile = NULL) {
  normalized <- tolower(trimws(as.character(profile %||% getOption("fp_r.semantics_profile", "compat"))))
  if (!nzchar(normalized) || normalized %in% c("default", "legacy")) {
    normalized <- "compat"
  }
  if (!normalized %in% c("compat", "canonical")) {
    stopf("Unknown standard-input semantics profile: %s", normalized)
  }
  canonical_active_set_start <- as.integer(
    getOption("fp_r.canonical_active_set_start_iteration", 4L) %||% 4L
  )
  canonical_active_set_delta <- as.numeric(
    getOption("fp_r.canonical_active_set_delta_threshold", 1e-6) %||% 1e-6
  )
  exogenous_equation_target_policy <- tolower(trimws(as.character(
    getOption("fp_r.exogenous_equation_target_policy", "exclude_from_solve") %||% "exclude_from_solve"
  )))
  if (!exogenous_equation_target_policy %in% c(
    "exclude_from_solve",
    "retain_equation_targets",
    "retain_reduced_eq_only"
  )) {
    stopf(
      "Unknown exogenous equation-target policy: %s",
      exogenous_equation_target_policy
    )
  }
  list(
    name = normalized,
    apply_base_helper_overlay = TRUE,
    exogenous_equation_target_policy = exogenous_equation_target_policy,
    apply_outside_boundary_carry = identical(normalized, "compat"),
    apply_outside_first_period_carry = TRUE,
    outside_first_period_carry_mode = if (identical(normalized, "compat")) {
      "full"
    } else {
      "boundary_materialized_only"
    },
    solver_policy = if (identical(normalized, "canonical")) "active_set_v1" else "full_scan",
    solver_active_set_enabled = identical(normalized, "canonical"),
    solver_active_set_start_iteration = if (identical(normalized, "canonical")) canonical_active_set_start else 0L,
    solver_active_set_delta_threshold = if (identical(normalized, "canonical")) canonical_active_set_delta else 0.0
  )
}

apply_semantics_profile_to_solve_control <- function(control, semantics_profile) {
  control$semantics_profile <- semantics_profile$name
  control$exogenous_equation_target_policy <- as.character(
    semantics_profile$exogenous_equation_target_policy %||% "exclude_from_solve"
  )
  control$solver_policy <- as.character(semantics_profile$solver_policy %||% "full_scan")
  control$active_set_enabled <- isTRUE(semantics_profile$solver_active_set_enabled)
  control$active_set_start_iteration <- as.integer(
    semantics_profile$solver_active_set_start_iteration %||% 0L
  )
  control$active_set_delta_threshold <- as.numeric(
    semantics_profile$solver_active_set_delta_threshold %||% 0.0
  )
  control
}

resolve_outside_first_period_carry_targets <- function(plan, semantics_profile, protected_targets = character()) {
  mode <- as.character(semantics_profile$outside_first_period_carry_mode %||% "full")
  target_roles <- plan$target_roles %||% NULL
  boundary_materialized_targets <- character()
  if (is.data.frame(target_roles) &&
      nrow(target_roles) &&
      all(c("target", "boundary_materialized") %in% names(target_roles))) {
    boundary_materialized_targets <- toupper(as.character(
      target_roles$target[as.logical(target_roles$boundary_materialized)]
    ))
    boundary_materialized_targets <- boundary_materialized_targets[nzchar(boundary_materialized_targets)]
  }
  if (identical(mode, "boundary_materialized_only")) {
    return(unique(boundary_materialized_targets))
  }
  if (identical(mode, "full")) {
    targets <- unique(c(
      toupper(as.character(plan$first_period_targets %||% character())),
      boundary_materialized_targets,
      toupper(as.character(protected_targets %||% character()))
    ))
    return(targets[nzchar(targets)])
  }
  if (identical(mode, "off")) {
    return(character())
  }
  stopf("Unknown OUTSIDE first-period carry mode: %s", mode)
}

apply_outside_carry_plan_frame <- function(
  frame,
  sample_start,
  plan,
  semantics_profile,
  protected_targets = character()
) {
  working <- frame
  if (isTRUE(semantics_profile$apply_outside_boundary_carry)) {
    working <- apply_outside_boundary_carry_frame(
      working,
      sample_start = sample_start,
      targets = plan$boundary_targets %||% character()
    )
  }
  if (isTRUE(semantics_profile$apply_outside_first_period_carry)) {
    first_period_targets <- resolve_outside_first_period_carry_targets(
      plan,
      semantics_profile,
      protected_targets = protected_targets
    )
    working <- apply_outside_first_period_carry_frame(
      working,
      sample_start = sample_start,
      targets = first_period_targets
    )
  }
  working
}

runtime_assignment_target_name <- function(statement) {
  if (is.null(statement)) {
    return("")
  }
  command <- statement_command_runtime(statement)
  kind <- tolower(as.character(statement$kind %||% ""))
  if (!(command %in% c("CREATE", "GENR", "IDENT", "LHS") || kind %in% c("create", "genr", "ident", "lhs"))) {
    return("")
  }
  toupper(as.character(statement$name %||% statement$target %||% ""))
}

collect_outside_boundary_window_assignment_targets <- function(statements, sample_start) {
  sample_index <- parse_period(sample_start)$index
  active_window <- NULL
  targets <- character()
  for (statement in statements %||% list()) {
    command <- statement_command_runtime(statement)
    if (identical(command, "SMPL")) {
      parsed <- parse_smpl_statement(statement$raw %||% "")
      if (!is.null(parsed)) {
        active_window <- c(parsed$start, parsed$end)
      }
      next
    }
    if (is.null(active_window) || length(active_window) < 2L) {
      next
    }
    if (parse_period(active_window[[2]])$index >= sample_index) {
      next
    }
    target <- runtime_assignment_target_name(statement)
    if (nzchar(target)) {
      targets <- c(targets, target)
    }
  }
  unique(targets[nzchar(targets)])
}

annotate_outside_support_provenance <- function(specs, statements, sample_start) {
  all_specs <- specs %||% list()
  normalized_specs <- normalize_specs(Filter(
    function(item) is.null(item$equation_number %||% NULL),
    all_specs
  ))
  if (!length(normalized_specs)) {
    return(list())
  }
  boundary_targets <- collect_outside_boundary_window_assignment_targets(statements, sample_start)
  target_names <- vapply(normalized_specs, `[[`, character(1), "target")
  spec_map <- stats::setNames(normalized_specs, target_names)

  direct_negative_lag_targets <- unique(vapply(normalized_specs, function(spec) {
    refs <- spec$compiled$references %||% NULL
    if (!is.data.frame(refs) || !nrow(refs) || !any(as.integer(refs$lag) < 0L)) {
      return("")
    }
    spec$target
  }, character(1)))
  direct_negative_lag_targets <- direct_negative_lag_targets[nzchar(direct_negative_lag_targets)]

  boundary_seed_targets <- intersect(boundary_targets, direct_negative_lag_targets)

  classify_zero_lag_support_roles <- function(seed_targets) {
    queue <- unique(seed_targets[nzchar(seed_targets)])
    roles <- stats::setNames(rep("none", length(spec_map)), names(spec_map))
    depths <- stats::setNames(rep(NA_integer_, length(spec_map)), names(spec_map))
    while (length(queue)) {
      target <- queue[[1L]]
      queue <- queue[-1L]
      if (!(target %in% names(spec_map))) {
        next
      }
      if (roles[[target]] == "none") {
        roles[[target]] <- if (target %in% seed_targets) "seed" else "support"
        depths[[target]] <- if (target %in% seed_targets) 0L else 1L
      }
      refs <- spec_map[[target]]$compiled$references %||% NULL
      if (!is.data.frame(refs) || !nrow(refs)) {
        next
      }
      zero_refs <- unique(toupper(as.character(refs$name[as.integer(refs$lag) == 0L])))
      next_targets <- zero_refs[zero_refs %in% names(spec_map)]
      current_depth <- depths[[target]] %||% 0L
      for (next_target in next_targets) {
        next_depth <- as.integer(current_depth) + 1L
        if (identical(roles[[next_target]], "none")) {
          roles[[next_target]] <- "support"
          depths[[next_target]] <- next_depth
          queue <- c(queue, next_target)
          next
        }
        if (is.na(depths[[next_target]]) || next_depth < depths[[next_target]]) {
          depths[[next_target]] <- next_depth
          queue <- c(queue, next_target)
        }
      }
    }
    list(roles = roles, depths = depths)
  }

  boundary_support <- classify_zero_lag_support_roles(boundary_seed_targets)
  boundary_roles <- boundary_support$roles %||% stats::setNames(rep("none", length(spec_map)), names(spec_map))
  boundary_depths <- boundary_support$depths %||% stats::setNames(rep(NA_integer_, length(spec_map)), names(spec_map))
  first_period_seed_targets <- unique(unlist(lapply(normalized_specs, function(spec) {
    refs <- spec$compiled$references %||% NULL
    if (!is.data.frame(refs) || !nrow(refs)) {
      return(character())
    }
    lagged_boundary_refs <- unique(toupper(as.character(refs$name[as.integer(refs$lag) < 0L])))
    lagged_boundary_refs[lagged_boundary_refs %in% boundary_targets]
  })))
  first_period_seed_targets <- first_period_seed_targets[nzchar(first_period_seed_targets)]
  first_period_support <- classify_zero_lag_support_roles(first_period_seed_targets)
  first_period_roles <- first_period_support$roles %||% stats::setNames(rep("none", length(spec_map)), names(spec_map))
  first_period_depths <- first_period_support$depths %||% stats::setNames(rep(NA_integer_, length(spec_map)), names(spec_map))

  lapply(normalized_specs, function(spec) {
    refs <- spec$compiled$references %||% NULL
    if (is.null(refs)) {
      refs <- data.frame(name = character(), lag = integer(), stringsAsFactors = FALSE)
    }
    zero_lag_raw_refs <- unique(toupper(as.character(refs$name[as.integer(refs$lag) == 0L])))
    zero_lag_raw_refs <- zero_lag_raw_refs[!(zero_lag_raw_refs %in% names(spec_map))]
    negative_lag_raw_refs <- unique(toupper(as.character(refs$name[as.integer(refs$lag) < 0L])))
    negative_lag_raw_refs <- negative_lag_raw_refs[!(negative_lag_raw_refs %in% names(spec_map))]
    spec$outside_provenance <- list(
      boundary_materialized = spec$target %in% boundary_targets,
      direct_negative_lag = spec$target %in% direct_negative_lag_targets,
      boundary_role = unname(boundary_roles[[spec$target]] %||% "none"),
      boundary_depth = as.integer(boundary_depths[[spec$target]] %||% NA_integer_),
      boundary_reachable = !identical(boundary_roles[[spec$target]] %||% "none", "none"),
      first_period_role = unname(first_period_roles[[spec$target]] %||% "none"),
      first_period_depth = as.integer(first_period_depths[[spec$target]] %||% NA_integer_),
      first_period_reachable = !identical(first_period_roles[[spec$target]] %||% "none", "none"),
      lagged_boundary_support_targets = unique(toupper(as.character(refs$name[as.integer(refs$lag) < 0L])))[
        unique(toupper(as.character(refs$name[as.integer(refs$lag) < 0L]))) %in% boundary_targets
      ],
      zero_lag_support_targets = unique(toupper(as.character(refs$name[as.integer(refs$lag) == 0L])))[
        unique(toupper(as.character(refs$name[as.integer(refs$lag) == 0L]))) %in% names(spec_map)
      ],
      zero_lag_raw_refs = zero_lag_raw_refs[nzchar(zero_lag_raw_refs)],
      negative_lag_raw_refs = negative_lag_raw_refs[nzchar(negative_lag_raw_refs)]
    )
    spec
  })
}

collect_outside_boundary_materialization_refs <- function(
  specs,
  statements,
  sample_start,
  protected_targets = character(),
  equation_targets = character(),
  equation_support_refs = character(),
  annotated_specs = NULL
) {
  all_specs <- specs %||% list()
  implicit_equation_targets <- unique(toupper(vapply(
    Filter(function(item) !is.null(item$equation_number %||% NULL), all_specs),
    function(item) as.character(item$target %||% item$name %||% ""),
    character(1)
  )))
  normalized_specs <- annotated_specs %||% annotate_outside_support_provenance(all_specs, statements, sample_start)
  if (!length(normalized_specs)) {
    return(character())
  }
  boundary_selected <- vapply(normalized_specs, function(spec) {
    provenance <- spec$outside_provenance %||% list()
    role <- provenance$boundary_role %||% "none"
    depth <- as.integer(provenance$boundary_depth %||% NA_integer_)
    materialized <- isTRUE(provenance$boundary_materialized)
    identical(role, "seed") ||
      (identical(role, "support") &&
        !materialized &&
        isTRUE(!is.na(depth) && depth == 1L))
  }, logical(1))
  if (!any(boundary_selected)) {
    return(character())
  }
  raw_refs <- unique(unlist(lapply(normalized_specs[boundary_selected], function(spec) {
    as.character((spec$outside_provenance %||% list())$zero_lag_raw_refs %||% character())
  })))

  excluded <- unique(c(
    toupper(as.character(protected_targets %||% character())),
    toupper(as.character(equation_targets %||% character())),
    implicit_equation_targets,
    toupper(as.character(equation_support_refs %||% character()))
  ))
  raw_refs <- unique(raw_refs[nzchar(raw_refs)])
  raw_refs[!(raw_refs %in% excluded)]
}

apply_outside_boundary_carry_frame <- function(frame, sample_start, targets = character()) {
  targets <- unique(toupper(as.character(targets %||% character())))
  if (!length(targets)) {
    return(frame)
  }
  boundary_period <- format_period(parse_period(sample_start)$index - 1L)
  source_period <- format_period(parse_period(boundary_period)$index - 1L)
  boundary_pos <- match(boundary_period, as.character(frame$period))
  source_pos <- match(source_period, as.character(frame$period))
  if (is.na(boundary_pos) || is.na(source_pos)) {
    return(frame)
  }
  working <- frame
  for (target in targets) {
    column <- resolve_frame_column_name(working, target)
    if (!(column %in% names(working))) {
      next
    }
    current_value <- as.numeric(working[[column]][[boundary_pos]])
    if (is.finite(current_value) && abs(current_value + 99.0) > 1e-12) {
      next
    }
    source_value <- as.numeric(working[[column]][[source_pos]])
    if (!is.finite(source_value) || abs(source_value + 99.0) <= 1e-12) {
      next
    }
    working[[column]][[boundary_pos]] <- source_value
  }
  working
}

collect_outside_first_period_materialization_input_refs <- function(
  specs,
  statements,
  sample_start,
  protected_targets = character(),
  equation_targets = character(),
  equation_support_refs = character(),
  annotated_specs = NULL
) {
  all_specs <- specs %||% list()
  implicit_equation_targets <- unique(toupper(vapply(
    Filter(function(item) !is.null(item$equation_number %||% NULL), all_specs),
    function(item) as.character(item$target %||% item$name %||% ""),
    character(1)
  )))
  normalized_specs <- annotated_specs %||% annotate_outside_support_provenance(all_specs, statements, sample_start)
  if (!length(normalized_specs)) {
    return(character())
  }
  first_period_selected <- vapply(normalized_specs, function(spec) {
    provenance <- spec$outside_provenance %||% list()
    role <- provenance$first_period_role %||% "none"
    depth <- as.integer(provenance$first_period_depth %||% NA_integer_)
    identical(role, "support") &&
      isTRUE(!is.na(depth) && depth == 1L)
  }, logical(1))
  if (!any(first_period_selected)) {
    first_period_selected <- logical(length(normalized_specs))
  }
  excluded <- unique(c(
    toupper(as.character(protected_targets %||% character())),
    toupper(as.character(equation_targets %||% character())),
    implicit_equation_targets
  ))
  raw_refs <- unique(unlist(lapply(normalized_specs[first_period_selected], function(spec) {
    as.character((spec$outside_provenance %||% list())$zero_lag_raw_refs %||% character())
  })))
  boundary_materialized_targets <- unique(vapply(
    Filter(function(spec) isTRUE((spec$outside_provenance %||% list())$boundary_materialized), normalized_specs),
    function(spec) as.character(spec$target %||% spec$name %||% ""),
    character(1)
  ))
  direct_equation_support_targets <- intersect(
    toupper(as.character(equation_support_refs %||% character())),
    toupper(as.character(boundary_materialized_targets %||% character()))
  )
  refs <- unique(c(raw_refs, direct_equation_support_targets))
  refs <- unique(refs[nzchar(refs)])
  refs[!(refs %in% setdiff(excluded, direct_equation_support_targets))]
}

build_outside_carry_plan <- function(
  specs,
  statements,
  sample_start,
  protected_targets = character(),
  equation_targets = character(),
  equation_support_refs = character()
) {
  annotated_specs <- annotate_outside_support_provenance(specs %||% list(), statements, sample_start)
  boundary_targets <- collect_outside_boundary_materialization_refs(
    specs,
    statements = statements,
    sample_start = sample_start,
    protected_targets = protected_targets,
    equation_targets = equation_targets,
    equation_support_refs = equation_support_refs,
    annotated_specs = annotated_specs
  )
  first_period_targets <- collect_outside_first_period_materialization_input_refs(
    specs,
    statements = statements,
    sample_start = sample_start,
    protected_targets = protected_targets,
    equation_targets = equation_targets,
    equation_support_refs = equation_support_refs,
    annotated_specs = annotated_specs
  )
  role_rows <- if (length(annotated_specs)) {
    do.call(rbind, lapply(annotated_specs, function(spec) {
      provenance <- spec$outside_provenance %||% list()
      data.frame(
        target = as.character(spec$target %||% ""),
        boundary_role = as.character(provenance$boundary_role %||% "none"),
        boundary_depth = as.integer(provenance$boundary_depth %||% NA_integer_),
        first_period_role = as.character(provenance$first_period_role %||% "none"),
        first_period_depth = as.integer(provenance$first_period_depth %||% NA_integer_),
        boundary_materialized = isTRUE(provenance$boundary_materialized),
        direct_negative_lag = isTRUE(provenance$direct_negative_lag),
        stringsAsFactors = FALSE,
        check.names = FALSE
      )
    }))
  } else {
    data.frame(
      target = character(),
      boundary_role = character(),
      boundary_depth = integer(),
      first_period_role = character(),
      first_period_depth = integer(),
      boundary_materialized = logical(),
      direct_negative_lag = logical(),
      stringsAsFactors = FALSE,
      check.names = FALSE
    )
  }
  list(
    boundary_targets = unique(toupper(as.character(boundary_targets %||% character()))),
    first_period_targets = unique(toupper(as.character(first_period_targets %||% character()))),
    target_roles = role_rows
  )
}

resolve_outside_snapshot_variables <- function(plan, watch_variables = character(), spec_targets = character()) {
  role_rows <- plan$target_roles %||% data.frame()
  role_targets <- if (is.data.frame(role_rows) && "target" %in% names(role_rows)) {
    as.character(role_rows$target %||% character())
  } else {
    character()
  }
  vars <- unique(c(
    toupper(as.character(plan$boundary_targets %||% character())),
    toupper(as.character(plan$first_period_targets %||% character())),
    toupper(role_targets),
    toupper(as.character(watch_variables %||% character())),
    toupper(as.character(spec_targets %||% character()))
  ))
  vars[nzchar(vars)]
}

build_frame_snapshot_rows <- function(frame, variables, sample_start, phase) {
  vars <- unique(toupper(as.character(variables %||% character())))
  vars <- vars[nzchar(vars)]
  if (!length(vars) || is.null(frame) || !"period" %in% names(frame) || !nzchar(sample_start %||% "")) {
    return(data.frame(
      phase = character(),
      period = character(),
      variable = character(),
      value = numeric(),
      stringsAsFactors = FALSE
    ))
  }
  sample_index <- parse_period(sample_start)$index
  periods <- unique(c(format_period(sample_index - 1L), as.character(sample_start)))
  rows <- list()
  for (period in periods) {
    period_pos <- match(as.character(period), as.character(frame$period))
    if (is.na(period_pos)) {
      next
    }
    for (variable in vars) {
      column <- resolve_frame_column_name(frame, variable)
      if (!(column %in% names(frame))) {
        next
      }
      value <- suppressWarnings(as.numeric(frame[[column]][[period_pos]]))
      rows[[length(rows) + 1L]] <- data.frame(
        phase = as.character(phase),
        period = as.character(period),
        variable = as.character(variable),
        value = value,
        stringsAsFactors = FALSE
      )
    }
  }
  if (!length(rows)) {
    return(data.frame(
      phase = character(),
      period = character(),
      variable = character(),
      value = numeric(),
      stringsAsFactors = FALSE
    ))
  }
  do.call(rbind, rows)
}

append_exogenous_path_trace_rows <- function(current_rows, frame, active_window = NULL, exogenous_targets = character(), watch_variables = character(), phase = "") {
  if (is.null(active_window) || length(active_window) < 2L || !nzchar(as.character(active_window[[1]] %||% ""))) {
    return(current_rows)
  }
  trace_vars <- resolve_exogenous_path_trace_variables(
    exogenous_targets = exogenous_targets,
    watch_variables = watch_variables
  )
  rows <- build_frame_snapshot_rows(
    frame,
    trace_vars,
    sample_start = as.character(active_window[[1]]),
    phase = as.character(phase)
  )
  if (!is.data.frame(rows) || !nrow(rows)) {
    return(current_rows)
  }
  if (!is.data.frame(current_rows) || !nrow(current_rows)) {
    return(rows)
  }
  unique(rbind(current_rows, rows))
}

resolve_live_frontier_trace_variables <- function(watch_variables = character(), spec_targets = character()) {
  vars <- unique(c(
    "UR", "UR1", "PCPD", "RS1", "PCM1L1A", "PCM1L1B", "RS", "RB", "RM",
    toupper(as.character(watch_variables %||% character())),
    toupper(as.character(spec_targets %||% character()))
  ))
  vars[nzchar(vars)]
}

resolve_exogenous_path_trace_variables <- function(exogenous_targets = character(), watch_variables = character()) {
  vars <- unique(c(
    "LUB", "UB", "UIFAC", "TRGH", "TRSH", "YD", "GDPR", "RS", "RB", "RM", "AH",
    toupper(as.character(exogenous_targets %||% character())),
    toupper(as.character(watch_variables %||% character()))
  ))
  vars[nzchar(vars)]
}

resolve_live_frontier_equation_trace_targets <- function(watch_variables = character(), spec_targets = character()) {
  targets <- unique(c(
    "UR", "UR1", "PCPD", "RS", "RB", "RM"
  ))
  targets[nzchar(targets)]
}

resolve_standard_input_equation_trace_config <- function(sample_start = "") {
  raw_targets <- getOption("fp_r.equation_trace_targets", NULL)
  if (is.null(raw_targets)) {
    return(list(
      enabled = FALSE,
      targets = character(),
      periods = character(),
      max_iterations = 0L
    ))
  }
  targets <- unique(toupper(as.character(raw_targets %||% character())))
  targets <- targets[nzchar(targets)]
  if (!length(targets)) {
    return(list(
      enabled = FALSE,
      targets = character(),
      periods = character(),
      max_iterations = 0L
    ))
  }
  raw_periods <- unique(as.character(getOption("fp_r.equation_trace_periods", character()) %||% character()))
  raw_periods <- raw_periods[nzchar(raw_periods)]
  periods <- if (length(raw_periods)) raw_periods else if (nzchar(sample_start %||% "")) as.character(sample_start) else character()
  list(
    enabled = TRUE,
    targets = targets,
    periods = periods,
    max_iterations = as.integer(getOption("fp_r.equation_trace_max_iterations", 0L) %||% 0L)
  )
}

resolve_standard_input_first_eval_targets <- function() {
  raw_targets <- getOption("fp_r.first_eval_targets", NULL)
  if (is.null(raw_targets)) {
    return(character())
  }
  targets <- unique(toupper(as.character(raw_targets %||% character())))
  targets <- targets[nzchar(targets)]
  targets
}

apply_outside_first_period_carry_frame <- function(frame, sample_start, targets = character()) {
  targets <- unique(toupper(as.character(targets %||% character())))
  if (!length(targets)) {
    return(frame)
  }
  sample_pos <- match(as.character(sample_start), as.character(frame$period))
  if (is.na(sample_pos) || sample_pos <= 1L) {
    return(frame)
  }
  source_pos <- sample_pos - 1L
  working <- frame
  for (target in targets) {
    column <- resolve_frame_column_name(working, target)
    if (!(column %in% names(working))) {
      next
    }
    current_value <- as.numeric(working[[column]][[sample_pos]])
    if (is.finite(current_value) && abs(current_value + 99.0) > 1e-12) {
      next
    }
    source_value <- as.numeric(working[[column]][[source_pos]])
    if (!is.finite(source_value) || abs(source_value + 99.0) <= 1e-12) {
      next
    }
    working[[column]][[sample_pos]] <- source_value
  }
  working
}

resolve_runtime_output_path <- function(name, work_dir) {
  cleaned <- clean_fp_filename(name)
  if (!nzchar(cleaned)) {
    return(NULL)
  }
  if (grepl("^[A-Za-z]:[/\\\\]|^/", cleaned, perl = TRUE)) {
    return(normalizePath(cleaned, winslash = "/", mustWork = FALSE))
  }
  normalizePath(file.path(work_dir, cleaned), winslash = "/", mustWork = FALSE)
}

json_escape_string <- function(value) {
  text <- as.character(value %||% "")
  text <- gsub("\\\\", "\\\\\\\\", text, perl = TRUE)
  text <- gsub("\"", "\\\\\"", text, perl = TRUE)
  text <- gsub("\n", "\\\\n", text, fixed = TRUE)
  text <- gsub("\r", "\\\\r", text, fixed = TRUE)
  text <- gsub("\t", "\\\\t", text, fixed = TRUE)
  paste0("\"", text, "\"")
}

render_json_value <- function(value, indent = 0L) {
  indent_text <- paste(rep("  ", max(0L, as.integer(indent))), collapse = "")
  next_indent_text <- paste(rep("  ", max(0L, as.integer(indent) + 1L)), collapse = "")
  if (is.null(value)) {
    return("null")
  }
  if (is.atomic(value) && length(value) == 0L) {
    return("[]")
  }
  if (is.data.frame(value)) {
    if (!nrow(value)) {
      return("[]")
    }
    return(render_json_value(lapply(seq_len(nrow(value)), function(idx) as.list(value[idx, , drop = FALSE])), indent = indent))
  }
  if (is.list(value)) {
    value_names <- names(value) %||% character(length(value))
    if (!length(value)) {
      return(if (length(value_names[nzchar(value_names)])) "{}" else "[]")
    }
    if (length(value_names) && any(nzchar(value_names))) {
      parts <- character()
      for (idx in seq_along(value)) {
        key <- value_names[[idx]] %||% ""
        if (!nzchar(key)) {
          next
        }
        parts[[length(parts) + 1L]] <- paste0(
          next_indent_text,
          json_escape_string(key),
          ": ",
          render_json_value(value[[idx]], indent = indent + 1L)
        )
      }
      if (!length(parts)) {
        return("{}")
      }
      return(paste0("{\n", paste(parts, collapse = ",\n"), "\n", indent_text, "}"))
    }
    parts <- vapply(value, render_json_value, character(1), indent = indent + 1L)
    return(paste0("[\n", paste0(next_indent_text, parts, collapse = ",\n"), "\n", indent_text, "]"))
  }
  if (is.logical(value)) {
    if (length(value) != 1L || is.na(value)) {
      return("null")
    }
    return(if (isTRUE(value)) "true" else "false")
  }
  if (is.numeric(value)) {
    if (length(value) != 1L || !is.finite(value)) {
      return("null")
    }
    return(as.character(signif(as.numeric(value), digits = 15L)))
  }
  if (length(value) > 1L) {
    return(render_json_value(as.list(value), indent = indent))
  }
  json_escape_string(value)
}

write_json_output <- function(value, output_path) {
  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  write_lines_output(output_path, render_json_value(value))
  output_path
}

annotate_scanned_statement <- function(statement, source_path, source_order, source_depth = 0L) {
  out <- normalize_scanned_statement(statement)
  out$source_path <- as.character(source_path %||% "")
  out$source_order <- as.integer(source_order %||% 0L)
  out$source_depth <- as.integer(source_depth %||% 0L)
  out
}

normalize_scanned_statement <- function(statement) {
  if (is.null(statement)) {
    return(list(command = "", kind = "control", name = "", body = "", raw = ""))
  }
  raw <- as.character(statement$raw %||% "")
  command <- as.character(statement$command %||% statement$name %||% "")
  if (!nzchar(command) && nzchar(raw)) {
    command <- toupper(sub("\\s+.*$", "", raw))
  }
  command <- toupper(command)
  if (!nzchar(command) && nzchar(raw)) {
    command <- toupper(sub("^\\s*([A-Za-z]+).*$", "\\1", raw, perl = TRUE))
  }
  kind <- as.character(statement$kind %||% "")
  if (!nzchar(kind)) {
    kind <- if (command %in% c("CREATE", "GENR", "IDENT", "LHS")) tolower(command) else "control"
  }
  name <- as.character(statement$name %||% command)
  body <- as.character(statement$body %||% "")
  if (!nzchar(body) && nzchar(raw)) {
    body <- trimws(sub("^[A-Za-z]+\\s*", "", raw))
  }
  out <- statement
  out$command <- command
  out$kind <- kind
  out$name <- name
  out$body <- body
  out$raw <- raw
  if (is.null(out$compiled) && nzchar(as.character(out$expression %||% "")) && kind %in% c("create", "genr", "ident", "lhs")) {
    out$compiled <- compile_expression(as.character(out$expression))
  }
  out
}

statement_command_runtime <- function(statement) {
  raw <- if (is.list(statement)) {
    as.character(statement$raw %||% statement$name %||% "")
  } else {
    as.character(statement %||% "")
  }
  raw <- raw[[1]]
  command <- if (is.list(statement)) {
    as.character(statement$command %||% "")
  } else {
    as.character(statement %||% "")
  }
  command <- command[[1]]
  if (nzchar(command)) {
    return(toupper(command))
  }
  if (nzchar(raw)) {
    return(toupper(sub("^\\s*([A-Za-z]+).*$", "\\1", raw, perl = TRUE)))
  }
  ""
}

write_lines_output <- function(path, lines) {
  con <- file(path, open = "wt", encoding = "UTF-8")
  on.exit(close(con), add = TRUE)
  writeLines(as.character(lines), con = con)
  invisible(path)
}

format_stage_watch_value <- function(value) {
  if (is.na(value)) {
    return("")
  }
  format(as.numeric(value), digits = 14, scientific = FALSE, trim = TRUE)
}

emit_solve_watch_output <- function(stage_index, frame, solve_metadata, work_dir, active_window = NULL) {
  if (is.null(work_dir) || !nzchar(work_dir)) {
    return(NULL)
  }
  watch_variables <- unique(as.character(solve_metadata$watch_variables %||% character()))
  output_path <- resolve_runtime_output_path(sprintf("SOLVE_STAGE%d_WATCH.txt", as.integer(stage_index)), work_dir)
  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)

  working <- sort_frame_by_period(frame)
  if (!is.null(active_window) && length(active_window) >= 2L) {
    periods <- seq_periods(active_window[[1]], active_window[[2]])
    working <- ensure_frame_periods(working, periods)
    working <- working[working$period %in% periods, , drop = FALSE]
    working <- sort_frame_by_period(working)
  }

  lines <- c(
    sprintf("SOLVE_STAGE=%d", as.integer(stage_index)),
    sprintf("SMPL=%s %s", active_window[[1]] %||% "", active_window[[2]] %||% ""),
    sprintf("OPTIONS=%s", solve_metadata$option_text %||% ""),
    sprintf("WATCH=%s", paste(watch_variables, collapse = ","))
  )
  if (length(watch_variables)) {
    selected <- unique(vapply(watch_variables, function(variable) resolve_frame_column_name(working, variable), character(1)))
    selected <- selected[selected %in% names(working)]
    if (length(selected)) {
      watch_frame <- working[, c("period", selected), drop = FALSE]
      lines <- c(lines, paste(names(watch_frame), collapse = ","))
      for (row in seq_len(nrow(watch_frame))) {
        values <- vapply(watch_frame[row, , drop = TRUE], format_stage_watch_value, character(1))
        lines <- c(lines, paste(values, collapse = ","))
      }
    }
  }
  write_lines_output(output_path, lines)
}

evaluate_test_statements <- function(frame, statements, active_window = NULL, kind = c("IDENT", "LHS")) {
  kind <- toupper(match.arg(kind))
  if (!length(statements)) {
    return(data.frame(
      period = character(),
      target = character(),
      actual = numeric(),
      expected = numeric(),
      abs_diff = numeric(),
      stringsAsFactors = FALSE
    ))
  }

  working <- sort_frame_by_period(frame)
  periods <- if (is.null(active_window) || length(active_window) < 2L) {
    as.character(working$period)
  } else {
    window_periods_in_frame(working, active_window[[1]], active_window[[2]])
  }
  if (!length(periods)) {
    return(data.frame(
      period = character(),
      target = character(),
      actual = numeric(),
      expected = numeric(),
      abs_diff = numeric(),
      stringsAsFactors = FALSE
    ))
  }

  rows <- list()
  for (statement in statements) {
    if (!identical(statement_command_runtime(statement), kind)) {
      next
    }
    target <- as.character(statement$name %||% "")
    if (!nzchar(target)) {
      next
    }
    compiled <- statement$compiled %||% compile_expression(statement$expression %||% "")
    if (is.null(compiled)) {
      next
    }
    resolved_target <- resolve_frame_column_name(working, target)
    if (!(resolved_target %in% names(working))) {
      working[[resolved_target]] <- NA_real_
    }
    for (period in periods) {
      period_pos <- match(period, working$period)
      if (is.na(period_pos)) {
        next
      }
      state <- state_from_frame(working)
      state$coef_values <- state$coef_values %||% list()
      expected <- as.numeric(evaluate_compiled_expression(
        compiled,
        state,
        period_pos,
        strict = FALSE
      ))
      actual <- as.numeric(working[[resolved_target]][[period_pos]])
      rows[[length(rows) + 1L]] <- data.frame(
        period = period,
        target = resolved_target,
        actual = actual,
        expected = expected,
        abs_diff = if (is.finite(actual) && is.finite(expected)) abs(actual - expected) else NA_real_,
        stringsAsFactors = FALSE
      )
    }
  }

  if (!length(rows)) {
    return(data.frame(
      period = character(),
      target = character(),
      actual = numeric(),
      expected = numeric(),
      abs_diff = numeric(),
      stringsAsFactors = FALSE
    ))
  }
  do.call(rbind, rows)
}

emit_test_output <- function(kind, frame, statements, active_window, work_dir, occurrence = 1L) {
  if (is.null(work_dir) || !nzchar(work_dir)) {
    return(list(path = NULL, data = data.frame(), max_abs_diff = NA_real_))
  }
  kind <- toupper(kind)
  occurrence <- as.integer(occurrence %||% 1L)
  output_name <- if (occurrence <= 1L) {
    sprintf("TEST_%s.csv", kind)
  } else {
    sprintf("TEST_%s_%02d.csv", kind, occurrence)
  }
  output_path <- resolve_runtime_output_path(output_name, work_dir)
  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  test_data <- evaluate_test_statements(frame, statements, active_window = active_window, kind = kind)
  write.csv(test_data, output_path, row.names = FALSE)
  list(
    path = output_path,
    data = test_data,
    max_abs_diff = if (nrow(test_data)) max(test_data$abs_diff, na.rm = TRUE) else NA_real_
  )
}

emit_printnames_output <- function(frame, work_dir) {
  if (is.null(work_dir) || !nzchar(work_dir)) {
    return(NULL)
  }
  output_path <- resolve_runtime_output_path("PRINTNAMES.txt", work_dir)
  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  names_out <- sort(setdiff(names(sort_frame_by_period(frame)), "period"))
  write_lines_output(output_path, names_out)
  output_path
}

emit_printmodel_output <- function(bundle, work_dir) {
  if (is.null(work_dir) || !nzchar(work_dir)) {
    return(NULL)
  }
  output_path <- resolve_runtime_output_path("PRINTMODEL.txt", work_dir)
  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)

  specs <- bundle$specs %||% list()
  eq_specs <- Filter(function(item) !is.null(item$equation_number), specs)
  lines <- c(
    sprintf("bundle_name=%s", bundle$bundle_name %||% "<unnamed>"),
    sprintf("solve_stage_count=%d", length(bundle$solve_stages %||% list())),
    sprintf("equation_targets=%s", paste(vapply(eq_specs, function(item) as.character(item$target %||% item$name), character(1)), collapse = ",")),
    "equations:"
  )
  for (spec in eq_specs) {
    lines <- c(
      lines,
      sprintf(
        "EQ %s %s :: %s",
        as.character(spec$equation_number %||% ""),
        as.character(spec$target %||% spec$name %||% ""),
        as.character(spec$expression %||% "")
      )
    )
    if (!is.null(spec$rho_terms) && nrow(spec$rho_terms)) {
      lines <- c(lines, sprintf("  rho_terms=%s", paste(sprintf("%d:%s", as.integer(spec$rho_terms$order), format(as.numeric(spec$rho_terms$coefficient), digits = 14, scientific = FALSE, trim = TRUE)), collapse = ",")))
    }
    if (!is.null(spec$resid_ar1)) {
      lines <- c(lines, sprintf(
        "  resid_ar1=lag1:%s source:%s update:%s carry_damp:%s carry_mode:%s",
        format(as.numeric(spec$resid_ar1$rho_lag1), digits = 14, scientific = FALSE, trim = TRUE),
        as.character(spec$resid_ar1$source_series %||% ""),
        as.character(spec$resid_ar1$update_source %||% ""),
        format(as.numeric(spec$resid_ar1$carry_damp), digits = 14, scientific = FALSE, trim = TRUE),
        as.character(spec$resid_ar1$carry_damp_mode %||% "")
      ))
    }
    if (!is.null(spec$target_lag_source) && nzchar(spec$target_lag_source %||% "")) {
      lines <- c(lines, sprintf("  target_lag_source=%s", as.character(spec$target_lag_source)))
    }
    if (length(spec$active_fsr_terms %||% character())) {
      lines <- c(lines, sprintf("  active_fsr_terms=%s", paste(as.character(spec$active_fsr_terms), collapse = " ")))
    }
  }

  if (length(bundle$solve_stages %||% list())) {
    lines <- c(lines, "solve_stages:")
    for (stage in bundle$solve_stages) {
      lines <- c(lines, sprintf(
        "stage=%s solve_index=%s options=%s watch=%s",
        as.character(stage$stage %||% ""),
        as.character(stage$solve_index %||% ""),
        as.character(stage$solve_metadata$option_text %||% ""),
        paste(as.character(stage$solve_metadata$watch_variables %||% character()), collapse = ",")
      ))
    }
  }

  estimation_summary <- bundle$estimation_summary %||% data.frame()
  if (nrow(estimation_summary)) {
    lines <- c(lines, "estimation:")
    for (idx in seq_len(nrow(estimation_summary))) {
      lines <- c(lines, sprintf(
        "order=%s command=%s smpl=%s %s method=%s eq=%s flags=%s options=%s",
        as.character(estimation_summary$order[[idx]]),
        as.character(estimation_summary$command[[idx]]),
        as.character(estimation_summary$sample_start[[idx]]),
        as.character(estimation_summary$sample_end[[idx]]),
        as.character(estimation_summary$method[[idx]]),
        as.character(estimation_summary$equation_spec[[idx]]),
        as.character(estimation_summary$flags[[idx]]),
        as.character(estimation_summary$options[[idx]])
      ))
    }
  }

  header_summary <- bundle$header_summary %||% data.frame()
  if (nrow(header_summary)) {
    lines <- c(lines, "header:")
    for (idx in seq_len(nrow(header_summary))) {
      lines <- c(lines, sprintf(
        "order=%s command=%s title=%s options=%s",
        as.character(header_summary$order[[idx]]),
        as.character(header_summary$command[[idx]]),
        as.character(header_summary$title[[idx]]),
        as.character(header_summary$options[[idx]])
      ))
    }
  }

  modeq <- bundle$equations$modeq %||% data.frame()
  modeq_summary <- bundle$equations$modeq_summary %||% data.frame()
  eq_fsr <- bundle$equations$eq_fsr %||% data.frame()
  eq_fsr_summary <- bundle$equations$eq_fsr_summary %||% data.frame()
  if (nrow(eq_fsr)) {
    lines <- c(lines, "eq_fsr:")
    for (idx in seq_len(nrow(eq_fsr))) {
      lines <- c(lines, sprintf(
        "EQ %s FSR tokens=%s",
        as.character(eq_fsr$equation_number[[idx]]),
        as.character(eq_fsr$tokens[[idx]] %||% "")
      ))
    }
  }
  if (nrow(eq_fsr_summary)) {
    lines <- c(
      lines,
      "eq_fsr_summary:",
      sprintf(
        "equations=%s",
        paste(as.character(eq_fsr_summary$equation_number), collapse = ",")
      ),
      sprintf(
        "name_count=%s",
        paste(as.character(eq_fsr_summary$name_count), collapse = ",")
      ),
      sprintf(
        "max_lag=%s",
        paste(as.character(eq_fsr_summary$max_lag), collapse = ",")
      )
    )
  }
  if (nrow(modeq)) {
    lines <- c(lines, "modeq:")
    for (idx in seq_len(nrow(modeq))) {
      lines <- c(lines, sprintf(
        "MODEQ %s tokens=%s sub_tokens=%s fsr_tokens=%s fsr_sub_tokens=%s active_fsr_tokens=%s",
        as.character(modeq$equation_number[[idx]]),
        as.character(modeq$tokens[[idx]] %||% ""),
        as.character(modeq$sub_tokens[[idx]] %||% ""),
        as.character(modeq$fsr_tokens[[idx]] %||% ""),
        as.character(modeq$fsr_sub_tokens[[idx]] %||% ""),
        as.character(modeq$active_fsr_tokens[[idx]] %||% "")
      ))
    }
  }
  if (nrow(modeq_summary)) {
    lines <- c(
      lines,
      "modeq_summary:",
      sprintf(
        "equations=%s",
        paste(as.character(modeq_summary$equation_number), collapse = ",")
      ),
      sprintf(
        "shared_name_count=%s",
        paste(as.character(modeq_summary$shared_name_count), collapse = ",")
      ),
      sprintf(
        "max_fsr_lag=%s",
        paste(as.character(modeq_summary$max_fsr_lag), collapse = ",")
      ),
      sprintf(
        "active_fsr_tokens=%s",
        paste(as.character(modeq_summary$active_fsr_tokens), collapse = ";")
      )
    )
  }

  write_lines_output(output_path, lines)
}

emit_summary_output <- function(summary, output_name, work_dir) {
  if (is.null(work_dir) || !nzchar(work_dir) || !is.data.frame(summary) || !nrow(summary)) {
    return(NULL)
  }
  output_path <- resolve_runtime_output_path(output_name, work_dir)
  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  write.csv(summary, output_path, row.names = FALSE)
  output_path
}

resolve_bundle_outside_carry_plan <- function(bundle) {
  direct_plan <- bundle$control$outside_carry_plan %||% bundle$runtime$outside_carry_plan %||% NULL
  if (!is.null(direct_plan)) {
    return(direct_plan)
  }
  solve_stages <- bundle$solve_stages %||% list()
  if (length(solve_stages)) {
    last_stage <- solve_stages[[length(solve_stages)]]
    return(last_stage$bundle$control$outside_carry_plan %||% last_stage$bundle$runtime$outside_carry_plan %||% NULL)
  }
  NULL
}

resolve_bundle_outside_carry_snapshots <- function(bundle) {
  direct_rows <- bundle$runtime$outside_carry_snapshots %||% NULL
  if (is.data.frame(direct_rows) && nrow(direct_rows)) {
    return(direct_rows)
  }
  solve_stages <- bundle$solve_stages %||% list()
  if (!length(solve_stages)) {
    return(data.frame(
      phase = character(),
      period = character(),
      variable = character(),
      value = numeric(),
      stringsAsFactors = FALSE
    ))
  }
  last_stage <- solve_stages[[length(solve_stages)]]
  runtime_rows <- last_stage$bundle$runtime$outside_carry_snapshots %||% NULL
  if (!is.data.frame(runtime_rows)) {
    runtime_rows <- data.frame(
      phase = character(),
      period = character(),
      variable = character(),
      value = numeric(),
      stringsAsFactors = FALSE
    )
  }
  sample_start <- as.character(last_stage$bundle$control$sample_start %||% "")
  snapshot_vars <- as.character(last_stage$bundle$runtime$outside_snapshot_variables %||% character())
  post_rows <- if (length(snapshot_vars) && nzchar(sample_start)) {
    build_frame_snapshot_rows(last_stage$result$series, snapshot_vars, sample_start, "post_solve")
  } else {
    data.frame(
      phase = character(),
      period = character(),
      variable = character(),
      value = numeric(),
      stringsAsFactors = FALSE
    )
  }
  unique(rbind(runtime_rows, post_rows))
}

resolve_bundle_replay_plan_rows <- function(bundle) {
  direct_rows <- bundle$presolve_replay_plan_rows %||% bundle$runtime$presolve_replay_plan_rows %||% NULL
  if (is.data.frame(direct_rows)) {
    return(direct_rows)
  }
  data.frame(
    replay_order = integer(),
    plan_type = character(),
    command = character(),
    target = character(),
    active_window_start = character(),
    active_window_end = character(),
    preserve_mode = character(),
    changevar_applied = logical(),
    source_path = character(),
    source_order = integer(),
    cycle_revisit = logical(),
    raw = character(),
    stringsAsFactors = FALSE
  )
}

resolve_bundle_replay_plan_meta <- function(bundle) {
  bundle$presolve_replay_plan_meta %||% bundle$runtime$presolve_replay_plan_meta %||% list(
    cyclic_targets = character(),
    revisit_targets = character()
  )
}

resolve_bundle_preserve_mode_audit <- function(bundle) {
  direct_rows <- bundle$preserve_mode_audit %||% bundle$runtime$preserve_mode_audit %||% NULL
  if (is.data.frame(direct_rows)) {
    return(direct_rows)
  }
  data.frame(
    target = character(),
    mode = character(),
    trigger_reason = character(),
    trigger_period = character(),
    protected_value = numeric(),
    candidate_value = numeric(),
    active_window_start = character(),
    active_window_end = character(),
    source_path = character(),
    source_order = integer(),
    command = character(),
    raw = character(),
    stringsAsFactors = FALSE
  )
}

resolve_bundle_solve_input_trace_rows <- function(bundle) {
  direct_rows <- bundle$runtime$solve_input_trace_rows %||% NULL
  if (is.data.frame(direct_rows) && nrow(direct_rows)) {
    return(direct_rows)
  }
  solve_stages <- bundle$solve_stages %||% list()
  if (!length(solve_stages)) {
    return(data.frame(
      phase = character(),
      period = character(),
      variable = character(),
      value = numeric(),
      stringsAsFactors = FALSE
    ))
  }
  rows <- lapply(solve_stages, function(stage) {
    runtime_rows <- stage$bundle$runtime$solve_input_trace_rows %||% data.frame()
    if (!is.data.frame(runtime_rows) || !nrow(runtime_rows)) {
      return(NULL)
    }
    runtime_rows$solve_stage <- as.integer(stage$stage %||% 0L)
    runtime_rows
  })
  rows <- Filter(Negate(is.null), rows)
  if (!length(rows)) {
    return(data.frame(
      phase = character(),
      period = character(),
      variable = character(),
      value = numeric(),
      solve_stage = integer(),
      stringsAsFactors = FALSE
    ))
  }
  do.call(rbind, rows)
}

resolve_bundle_exogenous_path_trace_rows <- function(bundle) {
  rows <- bundle$exogenous_path_trace %||% bundle$runtime$exogenous_path_trace %||% data.frame()
  if (!is.data.frame(rows) || !nrow(rows)) {
    return(data.frame(
      phase = character(),
      period = character(),
      variable = character(),
      value = numeric(),
      stringsAsFactors = FALSE
    ))
  }
  rows
}

resolve_bundle_equation_first_eval_rows <- function(bundle) {
  direct_rows <- bundle$runtime$equation_first_eval_rows %||% NULL
  if (is.data.frame(direct_rows)) {
    return(direct_rows)
  }
  solve_stages <- bundle$solve_stages %||% list()
  if (!length(solve_stages)) {
    return(data.frame(
      solve_stage = integer(),
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
  rows <- lapply(solve_stages, function(stage) {
    stage_rows <- stage$bundle$runtime$equation_first_eval_rows %||% data.frame()
    if (!is.data.frame(stage_rows) || !nrow(stage_rows)) {
      return(NULL)
    }
    stage_rows$solve_stage <- as.integer(stage$stage %||% 0L)
    stage_rows
  })
  rows <- Filter(Negate(is.null), rows)
  if (!length(rows)) {
    return(data.frame(
      solve_stage = integer(),
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
  do.call(rbind, rows)
}

resolve_bundle_equation_input_trace_rows <- function(bundle) {
  solve_stages <- bundle$solve_stages %||% list()
  if (!length(solve_stages)) {
    return(data.frame(
      solve_stage = integer(),
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
  rows <- lapply(solve_stages, function(stage) {
    stage_rows <- stage$result$equation_input_trace %||% data.frame()
    if (!is.data.frame(stage_rows) || !nrow(stage_rows)) {
      return(NULL)
    }
    stage_rows$solve_stage <- as.integer(stage$stage %||% 0L)
    stage_rows
  })
  rows <- Filter(Negate(is.null), rows)
  if (!length(rows)) {
    return(data.frame(
      solve_stage = integer(),
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
  do.call(rbind, rows)
}

resolve_bundle_nonfinite_fallback_rows <- function(bundle) {
  solve_stages <- bundle$solve_stages %||% list()
  if (!length(solve_stages)) {
    return(data.frame(
      solve_stage = integer(),
      period = character(),
      iteration = integer(),
      target = character(),
      fallback_reason = character(),
      previous_value = numeric(),
      evaluated_value = numeric(),
      stringsAsFactors = FALSE
    ))
  }
  rows <- lapply(solve_stages, function(stage) {
    stage_rows <- stage$result$fallback_audit %||% data.frame()
    if (!is.data.frame(stage_rows) || !nrow(stage_rows)) {
      return(NULL)
    }
    stage_rows$solve_stage <- as.integer(stage$stage %||% 0L)
    stage_rows
  })
  rows <- Filter(Negate(is.null), rows)
  if (!length(rows)) {
    return(data.frame(
      solve_stage = integer(),
      period = character(),
      iteration = integer(),
      target = character(),
      fallback_reason = character(),
      previous_value = numeric(),
      evaluated_value = numeric(),
      stringsAsFactors = FALSE
    ))
  }
  do.call(rbind, rows)
}

emit_outside_carry_plan_outputs <- function(bundle, work_dir) {
  if (is.null(work_dir) || !nzchar(work_dir)) {
    return(character())
  }
  plan <- resolve_bundle_outside_carry_plan(bundle)
  if (is.null(plan)) {
    return(character())
  }
  summary_path <- resolve_runtime_output_path("OUTSIDE_CARRY_PLAN.txt", work_dir)
  dir.create(dirname(summary_path), recursive = TRUE, showWarnings = FALSE)
  write_lines_output(
    summary_path,
    c(
      sprintf("boundary_targets=%s", paste(as.character(plan$boundary_targets %||% character()), collapse = ",")),
      sprintf("first_period_targets=%s", paste(as.character(plan$first_period_targets %||% character()), collapse = ","))
    )
  )
  role_path <- NULL
  role_rows <- plan$target_roles %||% data.frame()
  if (is.data.frame(role_rows)) {
    role_path <- resolve_runtime_output_path("OUTSIDE_CARRY_ROLES.csv", work_dir)
    dir.create(dirname(role_path), recursive = TRUE, showWarnings = FALSE)
    write.csv(role_rows, role_path, row.names = FALSE)
  }
  Filter(Negate(is.null), c(summary_path, role_path))
}

emit_outside_carry_snapshot_outputs <- function(bundle, work_dir) {
  if (is.null(work_dir) || !nzchar(work_dir)) {
    return(NULL)
  }
  snapshot_rows <- resolve_bundle_outside_carry_snapshots(bundle)
  if (!is.data.frame(snapshot_rows) || !nrow(snapshot_rows)) {
    return(NULL)
  }
  output_path <- resolve_runtime_output_path("OUTSIDE_CARRY_SNAPSHOTS.csv", work_dir)
  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  write.csv(snapshot_rows, output_path, row.names = FALSE)
  output_path
}

emit_replay_plan_output <- function(bundle, work_dir) {
  if (is.null(work_dir) || !nzchar(work_dir)) {
    return(NULL)
  }
  rows <- resolve_bundle_replay_plan_rows(bundle)
  if (!is.data.frame(rows) || !nrow(rows)) {
    return(NULL)
  }
  output_path <- resolve_runtime_output_path("REPLAY_PLAN.json", work_dir)
  payload <- list(
    replay_plan = if (nrow(rows)) lapply(seq_len(nrow(rows)), function(idx) as.list(rows[idx, , drop = FALSE])) else list(),
    meta = resolve_bundle_replay_plan_meta(bundle)
  )
  write_json_output(payload, output_path)
}

emit_preserve_mode_audit_output <- function(bundle, work_dir) {
  if (is.null(work_dir) || !nzchar(work_dir)) {
    return(NULL)
  }
  rows <- resolve_bundle_preserve_mode_audit(bundle)
  if (!is.data.frame(rows) || !nrow(rows)) {
    return(NULL)
  }
  output_path <- resolve_runtime_output_path("PRESERVE_MODE_AUDIT.csv", work_dir)
  write.csv(rows, output_path, row.names = FALSE)
  output_path
}

emit_solve_input_trace_output <- function(bundle, work_dir) {
  if (is.null(work_dir) || !nzchar(work_dir)) {
    return(NULL)
  }
  rows <- resolve_bundle_solve_input_trace_rows(bundle)
  if (!is.data.frame(rows) || !nrow(rows)) {
    return(NULL)
  }
  output_path <- resolve_runtime_output_path("SOLVE_INPUT_TRACE.csv", work_dir)
  write.csv(rows, output_path, row.names = FALSE)
  output_path
}

emit_exogenous_path_trace_output <- function(bundle, work_dir) {
  if (is.null(work_dir) || !nzchar(work_dir)) {
    return(NULL)
  }
  rows <- resolve_bundle_exogenous_path_trace_rows(bundle)
  if (!is.data.frame(rows) || !nrow(rows)) {
    return(NULL)
  }
  output_path <- resolve_runtime_output_path("EXOGENOUS_PATH_TRACE.csv", work_dir)
  write.csv(rows, output_path, row.names = FALSE)
  output_path
}

emit_equation_first_eval_output <- function(bundle, work_dir) {
  if (is.null(work_dir) || !nzchar(work_dir)) {
    return(NULL)
  }
  rows <- resolve_bundle_equation_first_eval_rows(bundle)
  if (!is.data.frame(rows) || !nrow(rows)) {
    return(NULL)
  }
  output_path <- resolve_runtime_output_path("EQUATION_FIRST_EVAL_SNAPSHOT.csv", work_dir)
  write.csv(rows, output_path, row.names = FALSE)
  output_path
}

emit_equation_input_trace_output <- function(bundle, work_dir) {
  if (is.null(work_dir) || !nzchar(work_dir)) {
    return(NULL)
  }
  rows <- resolve_bundle_equation_input_trace_rows(bundle)
  if (!is.data.frame(rows) || !nrow(rows)) {
    return(NULL)
  }
  output_path <- resolve_runtime_output_path("EQUATION_INPUT_SNAPSHOT.csv", work_dir)
  write.csv(rows, output_path, row.names = FALSE)
  output_path
}

emit_nonfinite_fallback_output <- function(bundle, work_dir) {
  if (is.null(work_dir) || !nzchar(work_dir)) {
    return(NULL)
  }
  rows <- resolve_bundle_nonfinite_fallback_rows(bundle)
  if (!is.data.frame(rows) || !nrow(rows)) {
    return(NULL)
  }
  output_path <- resolve_runtime_output_path("NONFINITE_FALLBACKS.csv", work_dir)
  write.csv(rows, output_path, row.names = FALSE)
  output_path
}

emit_printmodel_support_outputs <- function(bundle, work_dir) {
  Filter(Negate(is.null), c(
    emit_summary_output(bundle$estimation_summary %||% data.frame(), "ESTIMATION_SUMMARY.csv", work_dir),
    emit_summary_output(bundle$header_summary %||% data.frame(), "HEADER_SUMMARY.csv", work_dir),
    emit_replay_plan_output(bundle, work_dir),
    emit_preserve_mode_audit_output(bundle, work_dir),
    emit_outside_carry_plan_outputs(bundle, work_dir),
    emit_outside_carry_snapshot_outputs(bundle, work_dir),
    emit_solve_input_trace_output(bundle, work_dir),
    emit_exogenous_path_trace_output(bundle, work_dir),
    emit_equation_first_eval_output(bundle, work_dir),
    emit_equation_input_trace_output(bundle, work_dir),
    emit_nonfinite_fallback_output(bundle, work_dir)
  ))
}

emit_solve_support_outputs <- function(bundle, work_dir) {
  Filter(Negate(is.null), c(
    emit_replay_plan_output(bundle, work_dir),
    emit_preserve_mode_audit_output(bundle, work_dir),
    emit_outside_carry_plan_outputs(bundle, work_dir),
    emit_outside_carry_snapshot_outputs(bundle, work_dir),
    emit_solve_input_trace_output(bundle, work_dir),
    emit_exogenous_path_trace_output(bundle, work_dir),
    emit_equation_first_eval_output(bundle, work_dir),
    emit_equation_input_trace_output(bundle, work_dir),
    emit_nonfinite_fallback_output(bundle, work_dir)
  ))
}

emit_test_summary_output <- function(summary, work_dir) {
  emit_summary_output(summary, "TEST_SUMMARY.csv", work_dir)
}

emit_solve_outputs <- function(stage_summary, diagnostics, work_dir) {
  Filter(Negate(is.null), c(
    emit_summary_output(stage_summary, "SOLVE_STAGE_SUMMARY.csv", work_dir),
    emit_summary_output(diagnostics, "SOLVE_DIAGNOSTICS.csv", work_dir)
  ))
}

build_source_summary_frame <- function(source) {
  if (!is.list(source)) {
    return(data.frame(
      source_kind = character(),
      path = character(),
      basename = character(),
      exists = logical(),
      stringsAsFactors = FALSE
    ))
  }
  source_keys <- c("entry_input", "fmdata", "fmexog", "fmout")
  data.frame(
    source_kind = source_keys,
    path = vapply(source_keys, function(key) as.character(source[[key]] %||% ""), character(1)),
    basename = vapply(source_keys, function(key) basename(as.character(source[[key]] %||% "")), character(1)),
    exists = vapply(source_keys, function(key) {
      path <- as.character(source[[key]] %||% "")
      nzchar(path) && file.exists(path)
    }, logical(1)),
    stringsAsFactors = FALSE
  )
}

build_scanned_files_frame <- function(files_scanned) {
  files_scanned <- unique(as.character(files_scanned %||% character()))
  data.frame(
    scan_order = seq_along(files_scanned),
    path = files_scanned,
    basename = basename(files_scanned),
    stringsAsFactors = FALSE
  )
}

emit_source_outputs <- function(source, work_dir) {
  Filter(Negate(is.null), c(
    emit_summary_output(build_source_summary_frame(source), "SOURCE_SUMMARY.csv", work_dir),
    emit_summary_output(build_scanned_files_frame(source$files_scanned %||% character()), "SCANNED_FILES.csv", work_dir)
  ))
}

parse_estimation_equation_numbers <- function(equation_spec) {
  spec <- trimws(as.character(equation_spec %||% ""))
  if (!nzchar(spec)) {
    return(integer())
  }
  parts <- trimws(strsplit(spec, ",", fixed = TRUE)[[1]])
  parts <- parts[nzchar(parts)]
  values <- integer()
  for (part in parts) {
    if (grepl("^[0-9]+-[0-9]+$", part, perl = TRUE)) {
      bounds <- as.integer(strsplit(part, "-", fixed = TRUE)[[1]])
      if (length(bounds) == 2L && all(is.finite(bounds))) {
        values <- c(values, seq.int(bounds[[1]], bounds[[2]]))
      }
      next
    }
    if (grepl("^[0-9]+$", part, perl = TRUE)) {
      values <- c(values, as.integer(part))
    }
  }
  unique(values[is.finite(values)])
}

parse_estimation_target_map_from_fmout <- function(fmout_path) {
  if (is.null(fmout_path) || !nzchar(fmout_path) || !file.exists(fmout_path)) {
    return(character())
  }
  lines <- readLines(normalizePath(fmout_path, winslash = "/", mustWork = TRUE), warn = FALSE, encoding = "UTF-8")
  eq_rows <- Filter(function(item) !is.null(item) && !isTRUE(item$is_fsr), lapply(lines, parse_eq_statement))
  if (!length(eq_rows)) {
    return(character())
  }
  eq_numbers <- vapply(eq_rows, function(item) as.integer(item$equation_number %||% NA_integer_), integer(1))
  eq_targets <- vapply(eq_rows, function(item) as.character(item$target %||% ""), character(1))
  valid <- is.finite(eq_numbers) & nzchar(eq_targets)
  eq_numbers <- eq_numbers[valid]
  eq_targets <- eq_targets[valid]
  if (!length(eq_numbers)) {
    return(character())
  }
  keep <- !duplicated(as.character(eq_numbers))
  stats::setNames(eq_targets[keep], as.character(eq_numbers[keep]))
}

build_estimation_fmout_detail_tables <- function(fmout_path) {
  if (is.null(fmout_path) || !nzchar(fmout_path) || !file.exists(fmout_path)) {
    return(list(
      equations = data.frame(
        equation_number = integer(),
        target = character(),
        rho_order = integer(),
        rhs_count = integer(),
        reference_names = character(),
        stringsAsFactors = FALSE
      ),
      eq_fsr_summary = data.frame(
        equation_number = integer(),
        token_count = integer(),
        name_count = integer(),
        max_lag = integer(),
        has_lags = logical(),
        reference_names = character(),
        stringsAsFactors = FALSE
      ),
      modeq_summary = data.frame(
        equation_number = integer(),
        modeq_name_count = integer(),
        fsr_name_count = integer(),
        shared_name_count = integer(),
        max_fsr_lag = integer(),
        fsr_has_lags = logical(),
        active_fsr_token_count = integer(),
        active_fsr_tokens = character(),
        active_fsr_name_count = integer(),
        active_fsr_reference_names = character(),
        active_max_fsr_lag = integer(),
        active_fsr_has_lags = logical(),
        stringsAsFactors = FALSE
      )
    ))
  }

  lines <- readLines(normalizePath(fmout_path, winslash = "/", mustWork = TRUE), warn = FALSE, encoding = "UTF-8")
  eq_rows <- list()
  eq_fsr_rows <- list()
  modeq_rows <- list()
  active_fsr_tokens_by_equation <- list()

  for (line in lines) {
    parsed_eq <- parse_eq_statement(line)
    if (!is.null(parsed_eq)) {
      if (isTRUE(parsed_eq$is_fsr)) {
        eq_fsr_rows[[length(eq_fsr_rows) + 1L]] <- list(
          equation_number = parsed_eq$equation_number,
          token_count = length(parsed_eq$rhs_tokens),
          name_count = length(unique(parsed_eq$references$name %||% character())),
          max_lag = if (nrow(parsed_eq$references)) max(abs(parsed_eq$references$lag)) else 0L,
          has_lags = any((parsed_eq$references$lag %||% integer()) != 0L),
          reference_names = collapse_unique_values(parsed_eq$references$name)
        )
      } else {
        eq_rows[[length(eq_rows) + 1L]] <- list(
          equation_number = parsed_eq$equation_number,
          target = parsed_eq$target,
          rho_order = parsed_eq$rho_order,
          rhs_count = length(parsed_eq$rhs_tokens),
          reference_names = collapse_unique_values(parsed_eq$references$name)
        )
      }
      next
    }

    parsed_modeq <- parse_modeq_statement(line)
    if (is.null(parsed_modeq)) {
      next
    }
    equation_key <- as.character(parsed_modeq$equation_number)
    active_fsr_tokens <- apply_modeq_term_key_update(
      active_fsr_tokens_by_equation[[equation_key]] %||% character(),
      add_terms = parsed_modeq$fsr_add_terms %||% list(),
      sub_terms = parsed_modeq$fsr_sub_terms %||% list()
    )
    active_fsr_tokens_by_equation[[equation_key]] <- active_fsr_tokens
    active_summary <- summarize_reference_tokens(active_fsr_tokens)
    modeq_names <- unique(c(
      parsed_modeq$references$name %||% character(),
      parsed_modeq$sub_references$name %||% character()
    ))
    modeq_names <- modeq_names[nzchar(modeq_names)]
    fsr_names <- unique(c(
      parsed_modeq$fsr_references$name %||% character(),
      parsed_modeq$fsr_sub_references$name %||% character()
    ))
    fsr_names <- fsr_names[nzchar(fsr_names)]
    modeq_rows[[length(modeq_rows) + 1L]] <- list(
      equation_number = parsed_modeq$equation_number,
      modeq_name_count = length(modeq_names),
      fsr_name_count = length(fsr_names),
      shared_name_count = length(intersect(modeq_names, fsr_names)),
      max_fsr_lag = max(
        c(
          abs(parsed_modeq$fsr_references$lag %||% integer()),
          abs(parsed_modeq$fsr_sub_references$lag %||% integer()),
          0L
        )
      ),
      fsr_has_lags = any(c(
        (parsed_modeq$fsr_references$lag %||% integer()) != 0L,
        (parsed_modeq$fsr_sub_references$lag %||% integer()) != 0L
      )),
      active_fsr_token_count = active_summary$token_count,
      active_fsr_tokens = active_summary$token_text,
      active_fsr_name_count = active_summary$name_count,
      active_fsr_reference_names = active_summary$reference_names,
      active_max_fsr_lag = as.integer(active_summary$max_lag),
      active_fsr_has_lags = isTRUE(active_summary$has_lags)
    )
  }

  equations_frame <- if (!length(eq_rows)) {
    data.frame(
      equation_number = integer(),
      target = character(),
      rho_order = integer(),
      rhs_count = integer(),
      reference_names = character(),
      stringsAsFactors = FALSE
    )
  } else {
    data.frame(
      equation_number = vapply(eq_rows, `[[`, integer(1), "equation_number"),
      target = vapply(eq_rows, `[[`, character(1), "target"),
      rho_order = vapply(eq_rows, `[[`, integer(1), "rho_order"),
      rhs_count = vapply(eq_rows, `[[`, integer(1), "rhs_count"),
      reference_names = vapply(eq_rows, `[[`, character(1), "reference_names"),
      stringsAsFactors = FALSE
    )
  }
  eq_fsr_summary <- if (!length(eq_fsr_rows)) {
    data.frame(
      equation_number = integer(),
      token_count = integer(),
      name_count = integer(),
      max_lag = integer(),
      has_lags = logical(),
      reference_names = character(),
      stringsAsFactors = FALSE
    )
  } else {
    data.frame(
      equation_number = vapply(eq_fsr_rows, `[[`, integer(1), "equation_number"),
      token_count = vapply(eq_fsr_rows, `[[`, integer(1), "token_count"),
      name_count = vapply(eq_fsr_rows, `[[`, integer(1), "name_count"),
      max_lag = vapply(eq_fsr_rows, `[[`, integer(1), "max_lag"),
      has_lags = vapply(eq_fsr_rows, `[[`, logical(1), "has_lags"),
      reference_names = vapply(eq_fsr_rows, `[[`, character(1), "reference_names"),
      stringsAsFactors = FALSE
    )
  }
  modeq_summary <- if (!length(modeq_rows)) {
    data.frame(
      equation_number = integer(),
      modeq_name_count = integer(),
      fsr_name_count = integer(),
      shared_name_count = integer(),
      max_fsr_lag = integer(),
      fsr_has_lags = logical(),
      active_fsr_token_count = integer(),
      active_fsr_tokens = character(),
      active_fsr_name_count = integer(),
      active_fsr_reference_names = character(),
      active_max_fsr_lag = integer(),
      active_fsr_has_lags = logical(),
      stringsAsFactors = FALSE
    )
  } else {
    data.frame(
      equation_number = vapply(modeq_rows, `[[`, integer(1), "equation_number"),
      modeq_name_count = vapply(modeq_rows, `[[`, integer(1), "modeq_name_count"),
      fsr_name_count = vapply(modeq_rows, `[[`, integer(1), "fsr_name_count"),
      shared_name_count = vapply(modeq_rows, `[[`, integer(1), "shared_name_count"),
      max_fsr_lag = vapply(modeq_rows, `[[`, integer(1), "max_fsr_lag"),
      fsr_has_lags = vapply(modeq_rows, `[[`, logical(1), "fsr_has_lags"),
      active_fsr_token_count = vapply(modeq_rows, `[[`, integer(1), "active_fsr_token_count"),
      active_fsr_tokens = vapply(modeq_rows, `[[`, character(1), "active_fsr_tokens"),
      active_fsr_name_count = vapply(modeq_rows, `[[`, integer(1), "active_fsr_name_count"),
      active_fsr_reference_names = vapply(modeq_rows, `[[`, character(1), "active_fsr_reference_names"),
      active_max_fsr_lag = vapply(modeq_rows, `[[`, integer(1), "active_max_fsr_lag"),
      active_fsr_has_lags = vapply(modeq_rows, `[[`, logical(1), "active_fsr_has_lags"),
      stringsAsFactors = FALSE
    )
  }

  list(
    equations = equations_frame,
    eq_fsr_summary = eq_fsr_summary,
    modeq_summary = modeq_summary
  )
}

build_estimation_requests_frame <- function(estimation_summary, equations, fmout_path = NULL) {
  if (!is.data.frame(estimation_summary) || !nrow(estimation_summary)) {
    return(data.frame(
      order = integer(),
      command = character(),
      sample_start = character(),
      sample_end = character(),
      method = character(),
      equation_spec = character(),
      equation_numbers = character(),
      equation_targets = character(),
      resolved_count = integer(),
      resolution_status = character(),
      flags = character(),
      options = character(),
      raw = character(),
      stringsAsFactors = FALSE
    ))
  }

  eq_specs <- equations$specs %||% list()
  eq_specs <- Filter(function(item) !is.null(item$equation_number), eq_specs)
  eq_numbers <- vapply(eq_specs, function(item) as.integer(item$equation_number %||% NA_integer_), integer(1))
  eq_targets <- vapply(eq_specs, function(item) as.character(item$target %||% item$name %||% ""), character(1))
  eq_map <- stats::setNames(eq_targets[is.finite(eq_numbers)], as.character(eq_numbers[is.finite(eq_numbers)]))
  fmout_eq_map <- parse_estimation_target_map_from_fmout(fmout_path)
  if (length(fmout_eq_map)) {
    missing_keys <- setdiff(names(fmout_eq_map), names(eq_map))
    if (length(missing_keys)) {
      eq_map[missing_keys] <- fmout_eq_map[missing_keys]
    }
  }

  rows <- lapply(seq_len(nrow(estimation_summary)), function(idx) {
    equation_spec <- as.character(estimation_summary$equation_spec[[idx]] %||% "")
    equation_numbers <- parse_estimation_equation_numbers(equation_spec)
    resolved_targets <- if (length(equation_numbers)) {
      unname(eq_map[as.character(equation_numbers)])
    } else {
      character()
    }
    resolved_targets <- as.character(resolved_targets[!is.na(resolved_targets)])
    resolved_targets <- unique(resolved_targets[nzchar(resolved_targets)])
    resolution_status <- if (!identical(as.character(estimation_summary$command[[idx]]), "EST")) {
      "non_est"
    } else if (!nzchar(equation_spec)) {
      "no_equation_spec"
    } else if (!length(equation_numbers)) {
      "unparsed"
    } else if (!length(eq_map)) {
      "metadata_only"
    } else if (length(resolved_targets) == length(equation_numbers)) {
      if (length(fmout_eq_map) && !length(eq_specs)) "resolved_from_fmout" else "resolved"
    } else if (length(resolved_targets)) {
      if (length(fmout_eq_map) && !length(eq_specs)) "partial_from_fmout" else "partial"
    } else {
      "unresolved"
    }

    data.frame(
      order = as.integer(estimation_summary$order[[idx]]),
      command = as.character(estimation_summary$command[[idx]]),
      sample_start = as.character(estimation_summary$sample_start[[idx]]),
      sample_end = as.character(estimation_summary$sample_end[[idx]]),
      method = as.character(estimation_summary$method[[idx]]),
      equation_spec = equation_spec,
      equation_numbers = if (length(equation_numbers)) paste(as.character(equation_numbers), collapse = ",") else "",
      equation_targets = if (length(resolved_targets)) paste(resolved_targets, collapse = ",") else "",
      resolved_count = as.integer(length(resolved_targets)),
      resolution_status = resolution_status,
      flags = as.character(estimation_summary$flags[[idx]]),
      options = as.character(estimation_summary$options[[idx]]),
      raw = as.character(estimation_summary$raw[[idx]]),
      stringsAsFactors = FALSE
    )
  })

  do.call(rbind, rows)
}

build_estimation_equations_frame <- function(estimation_requests, equations, fmout_path = NULL) {
  if (!is.data.frame(estimation_requests) || !nrow(estimation_requests)) {
    return(data.frame(
      request_order = integer(),
      request_command = character(),
      sample_start = character(),
      sample_end = character(),
      method = character(),
      equation_number = integer(),
      target = character(),
      detail_source = character(),
      rho_order = integer(),
      rhs_count = integer(),
      eq_reference_names = character(),
      fsr_reference_names = character(),
      active_fsr_reference_names = character(),
      fsr_token_count = integer(),
      fsr_name_count = integer(),
      fsr_max_lag = integer(),
      fsr_has_lags = logical(),
      modeq_name_count = integer(),
      modeq_fsr_name_count = integer(),
      modeq_shared_name_count = integer(),
      modeq_active_fsr_tokens = character(),
      stringsAsFactors = FALSE
    ))
  }

  eq_details <- equations$equations %||% data.frame()
  eq_fsr_summary <- equations$eq_fsr_summary %||% data.frame()
  modeq_summary <- equations$modeq_summary %||% data.frame()
  fmout_details <- build_estimation_fmout_detail_tables(fmout_path)
  fmout_equations <- fmout_details$equations
  fmout_eq_fsr_summary <- fmout_details$eq_fsr_summary
  fmout_modeq_summary <- fmout_details$modeq_summary

  eq_rows <- Filter(function(item) identical(as.character(item$command), "EST"), split(estimation_requests, seq_len(nrow(estimation_requests))))
  rows <- list()
  for (request_row in eq_rows) {
    equation_numbers <- parse_estimation_equation_numbers(request_row$equation_spec[[1]])
    if (!length(equation_numbers)) {
      next
    }
    for (equation_number in equation_numbers) {
      local_eq <- if (nrow(eq_details)) eq_details[eq_details$equation_number == equation_number, , drop = FALSE] else data.frame()
      fmout_eq <- if (nrow(fmout_equations)) fmout_equations[fmout_equations$equation_number == equation_number, , drop = FALSE] else data.frame()
      eq_row <- if (nrow(local_eq)) local_eq[1, , drop = FALSE] else fmout_eq[1, , drop = FALSE]
      detail_source <- if (nrow(local_eq)) "equations" else if (nrow(fmout_eq)) "fmout" else "unresolved"
      local_fsr <- if (nrow(eq_fsr_summary)) eq_fsr_summary[eq_fsr_summary$equation_number == equation_number, , drop = FALSE] else data.frame()
      fmout_fsr <- if (nrow(fmout_eq_fsr_summary)) fmout_eq_fsr_summary[fmout_eq_fsr_summary$equation_number == equation_number, , drop = FALSE] else data.frame()
      fsr_row <- if (nrow(local_fsr)) local_fsr[1, , drop = FALSE] else fmout_fsr[1, , drop = FALSE]
      local_modeq <- if (nrow(modeq_summary)) modeq_summary[modeq_summary$equation_number == equation_number, , drop = FALSE] else data.frame()
      fmout_modeq <- if (nrow(fmout_modeq_summary)) fmout_modeq_summary[fmout_modeq_summary$equation_number == equation_number, , drop = FALSE] else data.frame()
      modeq_row <- if (nrow(local_modeq)) local_modeq[1, , drop = FALSE] else fmout_modeq[1, , drop = FALSE]

      rows[[length(rows) + 1L]] <- data.frame(
        request_order = as.integer(request_row$order[[1]]),
        request_command = as.character(request_row$command[[1]]),
        sample_start = as.character(request_row$sample_start[[1]]),
        sample_end = as.character(request_row$sample_end[[1]]),
        method = as.character(request_row$method[[1]]),
        equation_number = as.integer(equation_number),
        target = as.character(eq_row$target[[1]] %||% ""),
        detail_source = detail_source,
        rho_order = as.integer(eq_row$rho_order[[1]] %||% NA_integer_),
        rhs_count = as.integer(eq_row$rhs_count[[1]] %||% NA_integer_),
        eq_reference_names = as.character(eq_row$reference_names[[1]] %||% ""),
        fsr_reference_names = as.character(fsr_row$reference_names[[1]] %||% ""),
        active_fsr_reference_names = as.character(modeq_row$active_fsr_reference_names[[1]] %||% ""),
        fsr_token_count = as.integer(fsr_row$token_count[[1]] %||% NA_integer_),
        fsr_name_count = as.integer(fsr_row$name_count[[1]] %||% NA_integer_),
        fsr_max_lag = as.integer(fsr_row$max_lag[[1]] %||% NA_integer_),
        fsr_has_lags = as.logical(fsr_row$has_lags[[1]] %||% NA),
        modeq_name_count = as.integer(modeq_row$modeq_name_count[[1]] %||% NA_integer_),
        modeq_fsr_name_count = as.integer(modeq_row$fsr_name_count[[1]] %||% NA_integer_),
        modeq_shared_name_count = as.integer(modeq_row$shared_name_count[[1]] %||% NA_integer_),
        modeq_active_fsr_tokens = as.character(modeq_row$active_fsr_tokens[[1]] %||% ""),
        stringsAsFactors = FALSE
      )
    }
  }

  if (!length(rows)) {
    return(data.frame(
      request_order = integer(),
      request_command = character(),
      sample_start = character(),
      sample_end = character(),
      method = character(),
      equation_number = integer(),
      target = character(),
      detail_source = character(),
      rho_order = integer(),
      rhs_count = integer(),
      eq_reference_names = character(),
      fsr_reference_names = character(),
      active_fsr_reference_names = character(),
      fsr_token_count = integer(),
      fsr_name_count = integer(),
      fsr_max_lag = integer(),
      fsr_has_lags = logical(),
      modeq_name_count = integer(),
      modeq_fsr_name_count = integer(),
      modeq_shared_name_count = integer(),
      modeq_active_fsr_tokens = character(),
      stringsAsFactors = FALSE
    ))
  }
  do.call(rbind, rows)
}

build_estimation_coverage_frame <- function(estimation_equations, estimation_records) {
  if (!is.data.frame(estimation_equations) || !nrow(estimation_equations)) {
    return(data.frame(
      request_order = integer(),
      equation_number = integer(),
      target = character(),
      sample_start = character(),
      sample_end = character(),
      requested_periods = integer(),
      target_non_missing = integer(),
      target_missing = integer(),
      reference_name_count = integer(),
      reference_names = character(),
      complete_case_rows = integer(),
      coverage_status = character(),
      stringsAsFactors = FALSE
    ))
  }

  record_map <- stats::setNames(estimation_records %||% list(), vapply(estimation_records %||% list(), function(item) as.character(item$order %||% ""), character(1)))
  rows <- list()
  for (idx in seq_len(nrow(estimation_equations))) {
    order_key <- as.character(estimation_equations$request_order[[idx]] %||% "")
    record <- record_map[[order_key]] %||% NULL
    frame <- record$frame %||% data.frame(period = character(), stringsAsFactors = FALSE, check.names = FALSE)
    sample_start <- as.character(estimation_equations$sample_start[[idx]] %||% "")
    sample_end <- as.character(estimation_equations$sample_end[[idx]] %||% "")
    reference_names_text <- as.character(estimation_equations$active_fsr_reference_names[[idx]] %||% "")
    if (!nzchar(reference_names_text)) {
      reference_names_text <- as.character(estimation_equations$fsr_reference_names[[idx]] %||% "")
    }
    if (!nzchar(reference_names_text)) {
      reference_names_text <- as.character(estimation_equations$eq_reference_names[[idx]] %||% "")
    }
    if (is.na(reference_names_text) || identical(reference_names_text, "NA")) {
      reference_names_text <- ""
    }
    reference_names <- unique(trimws(strsplit(reference_names_text, "\\s+", perl = TRUE)[[1]]))
    reference_names <- reference_names[!is.na(reference_names) & nzchar(reference_names)]

    working <- sort_frame_by_period(frame)
    periods <- if (nzchar(sample_start) && nzchar(sample_end)) {
      window_periods_in_frame(working, sample_start, sample_end)
    } else {
      as.character(working$period %||% character())
    }
    window_frame <- if (length(periods)) working[working$period %in% periods, , drop = FALSE] else working[0, , drop = FALSE]
    target_name <- resolve_frame_column_name(window_frame, estimation_equations$target[[idx]] %||% "")
    target_values <- if (target_name %in% names(window_frame)) as.numeric(window_frame[[target_name]]) else numeric()
    reference_columns <- vapply(reference_names, function(name) resolve_frame_column_name(window_frame, name), character(1))
    reference_columns <- reference_columns[reference_columns %in% names(window_frame)]
    if (length(reference_columns)) {
      reference_matrix <- do.call(cbind, lapply(reference_columns, function(column) as.numeric(window_frame[[column]])))
      complete_ref <- rowSums(!is.finite(reference_matrix)) == 0L
    } else {
      complete_ref <- rep(TRUE, nrow(window_frame))
    }
    complete_case_rows <- if (length(target_values)) {
      sum(is.finite(target_values) & complete_ref)
    } else {
      0L
    }
    coverage_status <- if (!nrow(window_frame)) {
      "no_sample_rows"
    } else if (!(target_name %in% names(window_frame))) {
      "missing_target"
    } else if (!length(reference_names)) {
      "target_only"
    } else if (!length(reference_columns)) {
      "missing_references"
    } else {
      "covered"
    }

    rows[[length(rows) + 1L]] <- data.frame(
      request_order = as.integer(estimation_equations$request_order[[idx]]),
      equation_number = as.integer(estimation_equations$equation_number[[idx]]),
      target = as.character(estimation_equations$target[[idx]]),
      sample_start = sample_start,
      sample_end = sample_end,
      requested_periods = as.integer(length(periods)),
      target_non_missing = as.integer(sum(is.finite(target_values))),
      target_missing = as.integer(length(periods) - sum(is.finite(target_values))),
      reference_name_count = as.integer(length(reference_names)),
      reference_names = if (length(reference_names)) paste(reference_names, collapse = " ") else "",
      complete_case_rows = as.integer(complete_case_rows),
      coverage_status = coverage_status,
      stringsAsFactors = FALSE
    )
  }

  do.call(rbind, rows)
}

emit_estimation_outputs <- function(estimation_summary, equations, estimation_records, work_dir, fmout_path = NULL) {
  estimation_requests <- build_estimation_requests_frame(estimation_summary, equations, fmout_path = fmout_path)
  estimation_equations <- build_estimation_equations_frame(estimation_requests, equations, fmout_path = fmout_path)
  paths <- Filter(Negate(is.null), c(
    emit_summary_output(estimation_requests, "ESTIMATION_REQUESTS.csv", work_dir),
    emit_summary_output(
      estimation_equations,
      "ESTIMATION_EQUATIONS.csv",
      work_dir
    ),
    emit_summary_output(
      build_estimation_coverage_frame(estimation_equations, estimation_records),
      "ESTIMATION_COVERAGE.csv",
      work_dir
    )
  ))
  if (!length(paths)) {
    return(NULL)
  }
  paths
}

is_fmexog_like_text <- function(text) {
  normalized <- gsub("\r", "", as.character(text %||% ""))
  normalized <- trimws(normalized)
  if (!nzchar(normalized)) {
    return(FALSE)
  }

  parsed_rows <- tryCatch(parse_fmexog_text(normalized), error = function(...) NULL)
  if (is.data.frame(parsed_rows) && nrow(parsed_rows) > 0L) {
    return(TRUE)
  }

  parsed <- tryCatch(parse_fp_input(normalized), error = function(...) NULL)
  statements <- parsed$statements %||% list()
  if (!length(statements)) {
    return(FALSE)
  }
  commands <- unique(toupper(vapply(statements, function(item) {
    as.character(item$command %||% "")
  }, character(1))))
  commands <- commands[nzchar(commands)]
  if (!length(commands) || !("CHANGEVAR" %in% commands)) {
    return(FALSE)
  }
  allowed_commands <- c("SMPL", "CHANGEVAR", "RETURN", "QUIT", "END")
  all(commands %in% allowed_commands)
}

inline_changevar_payload_raw <- function(statements, index) {
  if (is.null(statements) || !length(statements)) {
    return("")
  }
  position <- as.integer(index %||% 0L)
  if (!is.finite(position) || position < 1L || position >= length(statements)) {
    return("")
  }
  trimws(as.character(statements[[position + 1L]]$raw %||% ""))
}

apply_inline_changevar_payload_frame <- function(frame, payload_raw, active_window) {
  payload_text <- trimws(as.character(payload_raw %||% ""))
  if (!nzchar(payload_text)) {
    return(frame)
  }
  if (is.null(active_window) || length(active_window) < 2L) {
    stopf("Inline CHANGEVAR payload encountered without an active SMPL window")
  }
  changevar_text <- paste(
    sprintf("SMPL %s %s;", active_window[[1]], active_window[[2]]),
    "CHANGEVAR;",
    payload_text,
    ";",
    sep = "\n"
  )
  apply_fmexog_rows(frame, parse_fmexog_text(changevar_text))
}

collect_runtime_input_targets <- function(statements, search_dirs = NULL) {
  items <- statements %||% list()
  if (!length(items)) {
    return(character())
  }
  targets <- character()
  active_window <- NULL
  fallback_window <- c("2000.1", "2000.1")

  parse_changevar_targets <- function(payload_raw, window) {
    payload_text <- trimws(as.character(payload_raw %||% ""))
    if (!nzchar(payload_text)) {
      return(character())
    }
    current_window <- window
    if (is.null(current_window) || length(current_window) < 2L) {
      current_window <- fallback_window
    }
    changevar_text <- paste(
      sprintf("SMPL %s %s;", current_window[[1]], current_window[[2]]),
      "CHANGEVAR;",
      payload_text,
      ";",
      sep = "\n"
    )
    rows <- tryCatch(parse_fmexog_text(changevar_text), error = function(...) NULL)
    if (!is.data.frame(rows) || !nrow(rows) || !"variable" %in% names(rows)) {
      return(character())
    }
    unique(toupper(as.character(rows$variable)))
  }

  for (idx in seq_along(items)) {
    statement <- items[[idx]]
    raw <- statement$raw %||% ""
    command <- statement_command_runtime(statement)
    if (idx > 1L && identical(statement_command_runtime(items[[idx - 1L]]), "CHANGEVAR")) {
      next
    }
    if (identical(command, "SMPL")) {
      parsed_window <- parse_smpl_statement(raw)
      if (!is.null(parsed_window)) {
        active_window <- c(parsed_window$start, parsed_window$end)
      }
      next
    }
    if (identical(command, "INPUT")) {
      input_name <- extract_fp_file_arg(raw, key = "FILE")
      resolved_input <- resolve_fp_source_path(input_name, search_dirs)
      if (!is.null(resolved_input)) {
        rows <- tryCatch(parse_fmexog_file(resolved_input), error = function(...) NULL)
        if (is.data.frame(rows) && nrow(rows) && "variable" %in% names(rows)) {
          targets <- c(targets, toupper(as.character(rows$variable)))
        }
      }
      next
    }
    if (identical(command, "CHANGEVAR")) {
      targets <- c(targets, parse_changevar_targets(
        inline_changevar_payload_raw(items, idx),
        active_window
      ))
    }
  }

  unique(targets[nzchar(targets)])
}

parse_setupsolve_options <- function(statement) {
  text <- trimws(as.character(statement %||% ""))
  if (!nzchar(text)) {
    return(list())
  }
  text <- sub("^SETUPSOLVE\\b", "", text, ignore.case = TRUE, perl = TRUE)
  tokens <- strsplit(gsub(";", " ", text, fixed = TRUE), "\\s+", perl = TRUE)[[1]]
  tokens <- tokens[nzchar(tokens)]
  if (!length(tokens)) {
    return(list())
  }

  parsed <- list()
  for (token in tokens) {
    if (!grepl("=", token, fixed = TRUE)) {
      next
    }
    parts <- strsplit(token, "=", fixed = TRUE)[[1]]
    if (length(parts) != 2L) {
      next
    }
    key <- toupper(trimws(parts[[1]]))
    value <- clean_fp_filename(parts[[2]])
    if (!nzchar(value)) {
      next
    }
    parsed[[key]] <- value
  }

  control <- list()
  if (!is.null(parsed$MINITERS)) {
    control$min_iter <- as.integer(parsed$MINITERS)
  }
  if (!is.null(parsed$MAXITERS)) {
    control$max_iter <- as.integer(parsed$MAXITERS)
  }
  if (!is.null(parsed$TOLALL)) {
    control$tolerance <- as.numeric(parsed$TOLALL)
  }
  if (!is.null(parsed$DAMPALL)) {
    control$damping <- as.numeric(parsed$DAMPALL)
  }
  if (!is.null(parsed$RHORESIDAR1)) {
    control$eq_rho_resid_ar1 <- parsed$RHORESIDAR1 %in% c("1", "TRUE", "T", "YES", "ON")
  }
  if (!is.null(parsed$RHORESIDUPDATESOURCE)) {
    control$eq_rho_resid_update_source <- tolower(parsed$RHORESIDUPDATESOURCE)
  }
  if (!is.null(parsed$RHORESIDCARRYDAMP)) {
    control$eq_rho_resid_carry_damp <- as.numeric(parsed$RHORESIDCARRYDAMP)
  }
  if (!is.null(parsed$RHORESIDCARRYDAMPMODE)) {
    control$eq_rho_resid_carry_damp_mode <- tolower(parsed$RHORESIDCARRYDAMPMODE)
  }
  if (!is.null(parsed$RHORESIDSOURCESUFFIX)) {
    control$eq_rho_resid_source_suffix <- parsed$RHORESIDSOURCESUFFIX
  }
  if (!is.null(parsed$TARGETLAGSUFFIX)) {
    control$eq_target_lag_suffix <- parsed$TARGETLAGSUFFIX
  }
  control
}

parse_setupest_statement <- function(statement) {
  text <- trimws(as.character(statement %||% ""))
  if (!nzchar(text) || !grepl("^SETUPEST\\b", text, ignore.case = TRUE, perl = TRUE)) {
    return(NULL)
  }
  text <- sub("^SETUPEST\\b", "", text, ignore.case = TRUE, perl = TRUE)
  tokens <- strsplit(gsub(";", " ", text, fixed = TRUE), "\\s+", perl = TRUE)[[1]]
  tokens <- tokens[nzchar(tokens)]
  flags <- character()
  options <- character()
  for (token in tokens) {
    if (grepl("=", token, fixed = TRUE)) {
      parts <- strsplit(token, "=", fixed = TRUE)[[1]]
      if (length(parts) != 2L) {
        next
      }
      key <- toupper(trimws(parts[[1]]))
      value <- clean_fp_filename(parts[[2]])
      if (nzchar(key) && nzchar(value)) {
        options[[key]] <- value
      }
      next
    }
    flags <- c(flags, toupper(clean_fp_filename(token)))
  }
  list(
    flags = unique(flags[nzchar(flags)]),
    options = options,
    option_text = paste(
      c(
        unique(flags[nzchar(flags)]),
        if (length(options)) sprintf("%s=%s", names(options), as.character(options)) else character()
      ),
      collapse = ";"
    )
  )
}

parse_space_statement <- function(statement) {
  text <- trimws(as.character(statement %||% ""))
  if (!nzchar(text) || !grepl("^SPACE\\b", text, ignore.case = TRUE, perl = TRUE)) {
    return(NULL)
  }
  text <- sub("^SPACE\\b", "", text, ignore.case = TRUE, perl = TRUE)
  tokens <- strsplit(gsub(";", " ", text, fixed = TRUE), "\\s+", perl = TRUE)[[1]]
  tokens <- tokens[nzchar(tokens)]
  options <- character()
  for (token in tokens) {
    if (!grepl("=", token, fixed = TRUE)) {
      next
    }
    parts <- strsplit(token, "=", fixed = TRUE)[[1]]
    if (length(parts) != 2L) {
      next
    }
    key <- toupper(trimws(parts[[1]]))
    value <- clean_fp_filename(parts[[2]])
    if (nzchar(key) && nzchar(value)) {
      options[[key]] <- value
    }
  }
  list(
    options = options,
    option_text = if (length(options)) paste(sprintf("%s=%s", names(options), as.character(options)), collapse = ";") else ""
  )
}

parse_us_statement <- function(statement) {
  text <- trimws(as.character(statement %||% ""))
  if (!nzchar(text) || !grepl("^US\\b", text, ignore.case = TRUE, perl = TRUE)) {
    return(NULL)
  }
  lines <- strsplit(gsub("\r", "", text), "\n", fixed = TRUE)[[1]]
  lines <- trimws(lines)
  lines <- lines[nzchar(lines)]
  if (!length(lines)) {
    return(list(title = "", space = NULL))
  }
  first_line <- lines[[1]]
  title <- trimws(sub("^US\\b", "", first_line, ignore.case = TRUE, perl = TRUE))
  embedded_space <- NULL
  if (length(lines) >= 2L) {
    for (line in lines[-1L]) {
      if (grepl("^SPACE\\b", line, ignore.case = TRUE, perl = TRUE)) {
        embedded_space <- parse_space_statement(line)
        break
      }
    }
  }
  list(
    title = title,
    space = embedded_space
  )
}

empty_header_summary <- function() {
  data.frame(
    order = integer(),
    command = character(),
    title = character(),
    options = character(),
    raw = character(),
    stringsAsFactors = FALSE
  )
}

collect_header_summary <- function(statements) {
  if (!length(statements)) {
    return(empty_header_summary())
  }

  rows <- list()
  row_order <- 1L
  for (statement in statements) {
    raw <- as.character(statement$raw %||% "")
    command <- statement_command_runtime(statement)
    if (identical(command, "US")) {
      parsed_us <- parse_us_statement(raw) %||% list(title = "", space = NULL)
      rows[[length(rows) + 1L]] <- data.frame(
        order = row_order,
        command = "US",
        title = as.character(parsed_us$title %||% ""),
        options = "",
        raw = raw,
        stringsAsFactors = FALSE
      )
      row_order <- row_order + 1L
      if (!is.null(parsed_us$space)) {
        rows[[length(rows) + 1L]] <- data.frame(
          order = row_order,
          command = "SPACE",
          title = "",
          options = as.character(parsed_us$space$option_text %||% ""),
          raw = raw,
          stringsAsFactors = FALSE
        )
        row_order <- row_order + 1L
      }
      next
    }
    if (!identical(command, "SPACE")) {
      next
    }
    parsed_space <- parse_space_statement(raw) %||% list(option_text = "")
    rows[[length(rows) + 1L]] <- data.frame(
      order = row_order,
      command = "SPACE",
      title = "",
      options = as.character(parsed_space$option_text %||% ""),
      raw = raw,
      stringsAsFactors = FALSE
    )
    row_order <- row_order + 1L
  }

  if (!length(rows)) {
    return(empty_header_summary())
  }
  do.call(rbind, rows)
}

parse_est_statement <- function(statement) {
  text <- trimws(as.character(statement %||% ""))
  if (!nzchar(text) || !grepl("^EST\\b", text, ignore.case = TRUE, perl = TRUE)) {
    return(NULL)
  }
  text <- sub("^EST\\b", "", text, ignore.case = TRUE, perl = TRUE)
  tokens <- strsplit(gsub(";", " ", text, fixed = TRUE), "\\s+", perl = TRUE)[[1]]
  tokens <- tokens[nzchar(tokens)]
  if (!length(tokens)) {
    return(list(equation_spec = "", options = character(), option_text = ""))
  }
  equation_spec <- ""
  options <- character()
  for (token in tokens) {
    if (!nzchar(equation_spec) && !grepl("=", token, fixed = TRUE)) {
      equation_spec <- clean_fp_filename(token)
      next
    }
    if (!grepl("=", token, fixed = TRUE)) {
      next
    }
    parts <- strsplit(token, "=", fixed = TRUE)[[1]]
    if (length(parts) != 2L) {
      next
    }
    key <- toupper(trimws(parts[[1]]))
    value <- clean_fp_filename(parts[[2]])
    if (nzchar(key) && nzchar(value)) {
      options[[key]] <- value
    }
  }
  list(
    equation_spec = equation_spec,
    options = options,
    option_text = if (length(options)) paste(sprintf("%s=%s", names(options), as.character(options)), collapse = ";") else ""
  )
}

empty_estimation_summary <- function() {
  data.frame(
    order = integer(),
    command = character(),
    sample_start = character(),
    sample_end = character(),
    method = character(),
    equation_spec = character(),
    flags = character(),
    options = character(),
    raw = character(),
    stringsAsFactors = FALSE
  )
}

collect_estimation_summary <- function(statements) {
  if (!length(statements)) {
    return(empty_estimation_summary())
  }

  active_window <- NULL
  setupest_flags <- character()
  active_method <- ""
  rows <- list()

  for (idx in seq_along(statements)) {
    statement <- statements[[idx]]
    raw <- as.character(statement$raw %||% "")
    command <- statement_command_runtime(statement)

    if (identical(command, "SMPL")) {
      parsed_window <- parse_smpl_statement(raw)
      if (!is.null(parsed_window)) {
        active_window <- c(parsed_window$start, parsed_window$end)
      }
      next
    }

    if (!(command %in% c("SETUPEST", "2SLS", "EST", "END"))) {
      next
    }

    equation_spec <- ""
    options_text <- ""
    if (identical(command, "SETUPEST")) {
      parsed_setupest <- parse_setupest_statement(raw) %||% list(flags = character(), options = character(), option_text = "")
      setupest_flags <- as.character(parsed_setupest$flags %||% character())
      options_text <- as.character(parsed_setupest$option_text %||% "")
    } else if (identical(command, "2SLS")) {
      active_method <- "2SLS"
    } else if (identical(command, "EST")) {
      parsed_est <- parse_est_statement(raw) %||% list(equation_spec = "", option_text = "")
      equation_spec <- as.character(parsed_est$equation_spec %||% "")
      options_text <- as.character(parsed_est$option_text %||% "")
    } else if (identical(command, "END")) {
      options_text <- ""
    }

    rows[[length(rows) + 1L]] <- data.frame(
      order = as.integer(idx),
      command = command,
      sample_start = as.character(active_window[[1]] %||% ""),
      sample_end = as.character(active_window[[2]] %||% ""),
      method = as.character(active_method %||% ""),
      equation_spec = equation_spec,
      flags = paste(unique(setupest_flags[nzchar(setupest_flags)]), collapse = " "),
      options = options_text,
      raw = raw,
      stringsAsFactors = FALSE
    )

    if (identical(command, "END")) {
      active_method <- ""
    }
  }

  if (!length(rows)) {
    return(empty_estimation_summary())
  }
  do.call(rbind, rows)
}

resolve_fp_source_path <- function(name, search_dirs) {
  cleaned <- clean_fp_filename(name)
  if (!nzchar(cleaned)) {
    return(NULL)
  }
  direct <- normalizePath(cleaned, winslash = "/", mustWork = FALSE)
  if (file.exists(direct)) {
    return(direct)
  }
  for (directory in search_dirs) {
    candidate <- file.path(directory, cleaned)
    if (file.exists(candidate)) {
      return(normalizePath(candidate, winslash = "/", mustWork = TRUE))
    }
    want <- tolower(cleaned)
    children <- list.files(directory, full.names = TRUE)
    matched <- children[tolower(basename(children)) == want]
    if (length(matched)) {
      return(normalizePath(matched[[1]], winslash = "/", mustWork = TRUE))
    }
  }
  NULL
}

resolve_standard_input_sources <- function(entry_input, fmdata_path = NULL, fmexog_path = NULL, fmout_path = NULL, search_dirs = NULL) {
  entry_path <- normalizePath(entry_input, winslash = "/", mustWork = TRUE)
  tree <- scan_fp_input_tree(entry_path, search_dirs = search_dirs)
  tree$statements <- lapply(tree$statements %||% list(), normalize_scanned_statement)
  parsed <- parse_fp_input(tree$text)
  resolved_search_dirs <- unique(normalizePath(
    Filter(function(path) !is.null(path) && nzchar(path), c(dirname(entry_path), search_dirs)),
    winslash = "/",
    mustWork = FALSE
  ))

  resolved_fmdata <- fmdata_path
  if (is.null(resolved_fmdata) || !nzchar(resolved_fmdata)) {
    resolved_fmdata <- resolve_fp_source_path("fmdata.txt", resolved_search_dirs)
  } else {
    resolved_fmdata <- normalizePath(resolved_fmdata, winslash = "/", mustWork = TRUE)
  }

  resolved_fmexog <- fmexog_path
  if (is.null(resolved_fmexog) || !nzchar(resolved_fmexog)) {
    resolved_fmexog <- resolve_fp_source_path("fmexog.txt", resolved_search_dirs)
  } else {
    resolved_fmexog <- normalizePath(resolved_fmexog, winslash = "/", mustWork = TRUE)
  }

  resolved_fmout <- fmout_path
  if (is.null(resolved_fmout) || !nzchar(resolved_fmout)) {
    resolved_fmout <- resolve_fp_source_path("fmout.txt", resolved_search_dirs)
  } else {
    resolved_fmout <- normalizePath(resolved_fmout, winslash = "/", mustWork = TRUE)
  }

  list(
    entry_path = entry_path,
    tree = tree,
    parsed = parsed,
    search_dirs = resolved_search_dirs,
    fmdata = resolved_fmdata,
    fmexog = resolved_fmexog,
    fmout = resolved_fmout
  )
}

scan_fp_input_tree <- function(entry_input, search_dirs = NULL) {
  entry_path <- normalizePath(entry_input, winslash = "/", mustWork = TRUE)
  normalized_dirs <- unique(normalizePath(
    Filter(function(path) !is.null(path) && nzchar(path), c(dirname(entry_path), search_dirs)),
    winslash = "/",
    mustWork = FALSE
  ))

  visit_file <- function(path, stack = character()) {
    normalized <- normalizePath(path, winslash = "/", mustWork = TRUE)
    if (normalized %in% stack) {
      stopf("Recursive INPUT FILE loop detected at %s", normalized)
    }

    text <- paste(readLines(normalized, warn = FALSE, encoding = "UTF-8"), collapse = "\n")
    parsed <- parse_fp_input(text)
    parsed_statements <- lapply(seq_along(parsed$statements %||% list()), function(idx) {
      annotate_scanned_statement(
        parsed$statements[[idx]],
        source_path = normalized,
        source_order = idx,
        source_depth = length(stack)
      )
    })
    statements <- list()
    include_files <- character()
    loaddata_files <- character()
    files_scanned <- normalized

    for (statement in parsed_statements) {
      raw <- statement$raw %||% ""
      command <- statement_command_runtime(statement)
      if (identical(command, "INPUT")) {
        include_name <- extract_fp_file_arg(raw, key = "FILE")
        if (!nzchar(include_name %||% "")) {
          statements[[length(statements) + 1L]] <- statement
          next
        }
        include_path <- resolve_fp_source_path(include_name, normalized_dirs)
        if (is.null(include_path)) {
          stopf("Missing FP include file %s", include_name)
        }
        include_text <- paste(readLines(include_path, warn = FALSE, encoding = "UTF-8"), collapse = "\n")
        if (is_fmexog_like_text(include_text)) {
          statements[[length(statements) + 1L]] <- statement
          include_files <- c(include_files, include_name)
          files_scanned <- c(files_scanned, include_path)
          next
        }
        nested <- visit_file(include_path, stack = c(stack, normalized))
        nested_statements <- Filter(function(item) {
          !identical(statement_command_runtime(item), "RETURN")
        }, nested$statements)
        statements <- c(statements, nested_statements)
        include_files <- c(include_files, include_name, nested$include_files)
        loaddata_files <- c(loaddata_files, nested$loaddata_files)
        files_scanned <- c(files_scanned, nested$files_scanned)
        next
      }
      if (identical(command, "LOADDATA")) {
        load_name <- extract_fp_file_arg(raw, key = "FILE")
        if (nzchar(load_name %||% "")) {
          loaddata_files <- c(loaddata_files, load_name)
        }
      }
      statements[[length(statements) + 1L]] <- statement
    }

    list(
      statements = statements,
      include_files = unique(include_files),
      loaddata_files = unique(loaddata_files),
      files_scanned = unique(files_scanned),
      text = paste(vapply(statements, function(item) paste0(item$raw, ";"), character(1)), collapse = "\n")
    )
  }

  visit_file(entry_path)
}

standard_assignment_target_name <- function(statement) {
  toupper(as.character(statement$name %||% statement$target %||% ""))
}

collect_standard_assignment_targets <- function(statements = list()) {
  if (!length(statements)) {
    return(character())
  }
  targets <- vapply(
    Filter(function(statement) {
      command <- statement_command_runtime(statement)
      command %in% c("CREATE", "GENR", "IDENT", "LHS", "EQ")
    }, statements),
    standard_assignment_target_name,
    character(1)
  )
  unique(targets[nzchar(targets)])
}

normalize_assignment_expression_text <- function(expression) {
  text <- trimws(as.character(expression %||% ""))
  text <- sub(";\\s*$", "", text)
  gsub("\\s+", "", text, perl = TRUE)
}

assignment_reference_keys <- function(statement) {
  compiled <- statement$compiled %||% NULL
  if (is.null(compiled)) {
    expression <- sub(";\\s*$", "", as.character(statement$expression %||% ""))
    if (nzchar(trimws(expression))) {
      compiled <- compile_expression(expression)
    }
  }
  refs <- compiled$references %||% NULL
  if (!is.data.frame(refs) || !nrow(refs)) {
    return(character())
  }
  unique(sprintf(
    "%s(%d)",
    toupper(as.character(refs$name)),
    as.integer(refs$lag)
  ))
}

build_assignment_statement_map <- function(statements = list(), targets = character()) {
  wanted_targets <- unique(toupper(as.character(targets %||% character())))
  wanted_targets <- wanted_targets[nzchar(wanted_targets)]
  assignments <- Filter(function(item) {
    !identical(item$kind %||% "control", "control") &&
      nzchar(as.character(item$expression %||% ""))
  }, statements %||% list())
  statement_map <- list()
  for (statement in assignments) {
    target <- toupper(as.character(statement$name %||% statement$target %||% ""))
    if (!nzchar(target)) {
      next
    }
    if (length(wanted_targets) && !(target %in% wanted_targets)) {
      next
    }
    statement_map[[target]] <- statement
  }
  statement_map
}

scan_targeted_fmout_assignment_statements <- function(fmout_path, targets = character()) {
  normalized_path <- normalizePath(fmout_path %||% "", winslash = "/", mustWork = FALSE)
  wanted_targets <- unique(toupper(as.character(targets %||% character())))
  wanted_targets <- wanted_targets[nzchar(wanted_targets)]
  if (!nzchar(normalized_path) || !file.exists(normalized_path) || !length(wanted_targets)) {
    return(list())
  }

  lines <- readLines(normalized_path, warn = FALSE, encoding = "UTF-8")
  statement_map <- list()
  current <- character()
  current_target <- ""

  flush_current <- function() {
    if (!length(current) || !nzchar(current_target)) {
      current <<- character()
      current_target <<- ""
      return(NULL)
    }
    raw <- paste(current, collapse = "\n")
    parsed <- parse_assignment_statement(trimws(raw))
    if (!is.null(parsed) && identical(toupper(as.character(parsed$name %||% "")), current_target)) {
      parsed$expression <- sub(";\\s*$", "", as.character(parsed$expression %||% ""))
      statement_map[[current_target]] <<- c(list(command = toupper(parsed$kind)), parsed)
    }
    current <<- character()
    current_target <<- ""
    NULL
  }

  for (line in lines) {
    trimmed <- trimws(line)
    if (!nzchar(trimmed)) {
      next
    }
    if (!length(current)) {
      matches <- regexec(
        "^(GENR|IDENT|LHS|CREATE)\\s+([A-Za-z][A-Za-z0-9_]*)\\b",
        trimmed,
        perl = TRUE
      )
      parts <- regmatches(trimmed, matches)[[1]]
      if (length(parts) < 3L) {
        next
      }
      target <- toupper(parts[[3]])
      if (!(target %in% wanted_targets)) {
        next
      }
      current <- trimmed
      current_target <- target
      if (grepl(";\\s*$", trimmed)) {
        flush_current()
      }
      next
    }
    current <- c(current, trimmed)
    if (grepl(";\\s*$", trimmed)) {
      flush_current()
      if (length(statement_map) >= length(wanted_targets)) {
        break
      }
    }
  }
  flush_current()
  statement_map
}

build_fmout_assignment_statement_map <- function(fmout_path, targets = character()) {
  normalized_path <- normalizePath(fmout_path %||% "", winslash = "/", mustWork = FALSE)
  if (!nzchar(normalized_path) || !file.exists(normalized_path)) {
    return(list())
  }
  wanted_targets <- unique(toupper(as.character(targets %||% character())))
  wanted_targets <- wanted_targets[nzchar(wanted_targets)]
  if (length(wanted_targets)) {
    targeted <- scan_targeted_fmout_assignment_statements(normalized_path, targets = wanted_targets)
    if (length(targeted)) {
      return(targeted)
    }
  }
  parsed <- tryCatch(
    parse_fp_input(paste(readLines(normalized_path, warn = FALSE, encoding = "UTF-8"), collapse = "\n")),
    error = function(...) NULL
  )
  if (is.null(parsed)) {
    return(list())
  }
  build_assignment_statement_map(parsed$statements, targets = targets)
}

compare_assignment_expression_sources <- function(statements = list(), fmout_path = NULL, targets = character()) {
  scenario_map <- build_assignment_statement_map(statements, targets = targets)
  fmout_map <- build_fmout_assignment_statement_map(fmout_path, targets = targets)
  all_targets <- unique(c(names(scenario_map), names(fmout_map)))
  all_targets <- all_targets[nzchar(all_targets)]
  if (!length(all_targets)) {
    return(data.frame(
      target = character(),
      scenario_expression = character(),
      fmout_expression = character(),
      expressions_match = logical(),
      scenario_only_refs = character(),
      fmout_only_refs = character(),
      stringsAsFactors = FALSE,
      check.names = FALSE
    ))
  }

  rows <- lapply(all_targets, function(target) {
    scenario_statement <- scenario_map[[target]] %||% NULL
    fmout_statement <- fmout_map[[target]] %||% NULL
    scenario_expression <- as.character(scenario_statement$expression %||% "")
    fmout_expression <- as.character(fmout_statement$expression %||% "")
    scenario_refs <- assignment_reference_keys(scenario_statement %||% list())
    fmout_refs <- assignment_reference_keys(fmout_statement %||% list())
    data.frame(
      target = target,
      scenario_expression = scenario_expression,
      fmout_expression = fmout_expression,
      expressions_match = identical(
        normalize_assignment_expression_text(scenario_expression),
        normalize_assignment_expression_text(fmout_expression)
      ),
      scenario_only_refs = paste(setdiff(scenario_refs, fmout_refs), collapse = ","),
      fmout_only_refs = paste(setdiff(fmout_refs, scenario_refs), collapse = ","),
      stringsAsFactors = FALSE,
      check.names = FALSE
    )
  })
  do.call(rbind, rows)
}

standard_input_simple_cust_replay_targets <- function() {
  c("PIEF", "SF", "RECG")
}

resolve_simple_cust_compat_replay_statements <- function(
  statements = list(),
  fmout_path = NULL,
  assignment_targets = character(),
  preserve_modes_by_target = NULL,
  semantics_profile = NULL
) {
  profile <- resolve_standard_input_semantics_profile(semantics_profile)
  if (!identical(profile$name, "compat")) {
    return(statements)
  }
  active_targets <- intersect(
    unique(toupper(as.character(assignment_targets %||% character()))),
    standard_input_simple_cust_replay_targets()
  )
  active_targets <- active_targets[nzchar(active_targets)]
  if (!length(active_targets)) {
    return(statements)
  }
  if (!is.null(preserve_modes_by_target) && length(preserve_modes_by_target)) {
    active_targets <- active_targets[vapply(active_targets, function(target) {
      identical(as.character(preserve_modes_by_target[[target]] %||% "skip"), "fallback")
    }, logical(1))]
  }
  if (!length(active_targets)) {
    return(statements)
  }

  comparison <- compare_assignment_expression_sources(
    statements,
    fmout_path = fmout_path,
    targets = active_targets
  )
  if (!nrow(comparison)) {
    return(statements)
  }
  replacement_targets <- comparison$target[
    !comparison$expressions_match &
      !nzchar(comparison$scenario_only_refs) &
      comparison$fmout_only_refs == "CUST(0)"
  ]
  replacement_targets <- unique(toupper(as.character(replacement_targets)))
  replacement_targets <- replacement_targets[nzchar(replacement_targets)]
  if (!length(replacement_targets)) {
    return(statements)
  }

  fmout_map <- build_fmout_assignment_statement_map(
    fmout_path,
    targets = replacement_targets
  )
  if (!length(fmout_map)) {
    return(statements)
  }

  lapply(statements, function(statement) {
    target <- toupper(as.character(statement$name %||% statement$target %||% ""))
    if (!nzchar(target) || !(target %in% replacement_targets) || is.null(fmout_map[[target]])) {
      return(statement)
    }
    fmout_map[[target]]
  })
}

parse_single_fp_statement <- function(raw) {
  text <- trimws(as.character(raw %||% ""))
  if (!nzchar(text)) {
    return(NULL)
  }
  parsed <- parse_fp_input(paste0(text, if (grepl(";\\s*$", text)) "" else ";"))
  statements <- parsed$statements %||% list()
  if (!length(statements)) {
    return(NULL)
  }
  statements[[1L]]
}

extract_base_helper_overlay_statements <- function(base_input_path, exclude_targets = character()) {
  normalized_base <- normalizePath(base_input_path, winslash = "/", mustWork = TRUE)
  text <- paste(readLines(normalized_base, warn = FALSE, encoding = "UTF-8"), collapse = "\n")
  parsed <- parse_fp_input(text)
  statements <- lapply(seq_along(parsed$statements %||% list()), function(idx) {
    annotate_scanned_statement(
      parsed$statements[[idx]],
      source_path = normalized_base,
      source_order = idx,
      source_depth = 0L
    )
  })
  excluded <- unique(toupper(as.character(exclude_targets %||% character())))
  excluded <- excluded[nzchar(excluded)]

  overlay <- list()
  for (statement in statements) {
    command <- statement_command_runtime(statement)
    if (identical(command, "SOLVE")) {
      break
    }
    if (identical(command, "SMPL")) {
      overlay[[length(overlay) + 1L]] <- statement
      next
    }
    if (!(command %in% c("CREATE", "GENR", "IDENT", "LHS"))) {
      next
    }
    target <- standard_assignment_target_name(statement)
    if (nzchar(target) && target %in% excluded) {
      next
    }
    overlay[[length(overlay) + 1L]] <- statement
  }

  list(
    statements = overlay,
    path = normalized_base
  )
}

apply_base_helper_overlay_to_tree <- function(entry_path, tree, search_dirs = NULL, semantics_profile = NULL) {
  profile <- resolve_standard_input_semantics_profile(semantics_profile)
  entry_name <- tolower(basename(entry_path %||% ""))
  if (!isTRUE(profile$apply_base_helper_overlay) || identical(entry_name, "fminput.txt")) {
    return(tree)
  }

  base_input_path <- resolve_fp_source_path("fminput.txt", search_dirs)
  if (is.null(base_input_path)) {
    return(tree)
  }
  normalized_base <- normalizePath(base_input_path, winslash = "/", mustWork = TRUE)
  normalized_entry <- normalizePath(entry_path, winslash = "/", mustWork = TRUE)
  if (identical(normalized_base, normalized_entry)) {
    return(tree)
  }

  statements <- tree$statements %||% list()
  if (!length(statements)) {
    tree$files_scanned <- unique(c(tree$files_scanned %||% character(), normalized_base))
    return(tree)
  }

  exclude_targets <- collect_standard_assignment_targets(statements)
  overlay <- extract_base_helper_overlay_statements(
    normalized_base,
    exclude_targets = exclude_targets
  )
  overlay_statements <- overlay$statements %||% list()
  if (!length(overlay_statements)) {
    tree$files_scanned <- unique(c(tree$files_scanned %||% character(), normalized_base))
    return(tree)
  }

  solve_indices <- which(vapply(statements, function(statement) {
    identical(statement_command_runtime(statement), "SOLVE")
  }, logical(1)))
  insert_at <- if (length(solve_indices)) solve_indices[[1L]] else NA_integer_

  last_smpl_statement <- NULL
  if (is.finite(insert_at)) {
    for (statement in statements[seq_len(max(0L, insert_at - 1L))]) {
      if (identical(statement_command_runtime(statement), "SMPL")) {
        last_smpl_statement <- statement
      }
    }
  }
  restore_smpl <- last_smpl_statement

  if (is.finite(insert_at)) {
    before <- if (insert_at > 1L) statements[seq_len(insert_at - 1L)] else list()
    after <- statements[seq.int(insert_at, length(statements))]
    statements <- c(
      before,
      overlay_statements,
      if (!is.null(restore_smpl)) list(restore_smpl) else list(),
      after
    )
  } else {
    statements <- c(
      statements,
      overlay_statements,
      if (!is.null(restore_smpl)) list(restore_smpl) else list()
    )
  }

  tree$statements <- statements
  tree$files_scanned <- unique(c(tree$files_scanned %||% character(), normalized_base))
  tree
}

prepare_standard_runtime <- function(statements, frame, search_dirs, default_fmexog_path = NULL) {
  working <- sort_frame_by_period(frame)
  active_window <- NULL
  solve_snapshot <- NULL
  setupsolve <- list()
  exogenous_targets <- character()
  saw_runtime_input <- FALSE
  termination_command <- ""
  termination_index <- 0L

  for (idx in seq_along(statements)) {
    statement <- statements[[idx]]
    raw <- statement$raw %||% ""
    command <- statement_command_runtime(statement)
    if (idx > 1L && identical(statement_command_runtime(statements[[idx - 1L]]), "CHANGEVAR")) {
      next
    }
    if (command %in% c("QUIT", "RETURN")) {
      termination_command <- command
      termination_index <- idx
      break
    }

    if (identical(command, "SMPL")) {
      parsed_window <- parse_smpl_statement(raw)
      if (!is.null(parsed_window)) {
        active_window <- c(parsed_window$start, parsed_window$end)
        working <- ensure_frame_periods(working, seq_periods(active_window[[1]], active_window[[2]]))
      }
      next
    }

    if (identical(command, "LOADDATA")) {
      load_name <- extract_fp_file_arg(raw, key = "FILE")
      resolved_load <- resolve_fp_source_path(load_name, search_dirs)
      if (!is.null(resolved_load)) {
        working <- merge_fm_numeric_frames(
          working,
          parse_fm_numeric_file(resolved_load, block_name = basename(resolved_load))
        )
        working <- sort_frame_by_period(working)
      }
      next
    }

    if (identical(command, "INPUT")) {
      input_name <- extract_fp_file_arg(raw, key = "FILE")
      resolved_input <- resolve_fp_source_path(input_name, search_dirs)
      if (!is.null(resolved_input)) {
        saw_runtime_input <- TRUE
        working <- apply_fmexog_rows(working, parse_fmexog_file(resolved_input))
        working <- sort_frame_by_period(working)
      }
      next
    }

    if (identical(command, "CHANGEVAR")) {
      payload_raw <- inline_changevar_payload_raw(statements, idx)
      if (nzchar(payload_raw)) {
        saw_runtime_input <- TRUE
        working <- apply_inline_changevar_payload_frame(working, payload_raw, active_window)
        working <- sort_frame_by_period(working)
      }
      next
    }

    if (identical(command, "EXOGENOUS")) {
      variable <- extract_fp_named_arg(raw, key = "VARIABLE")
      if (nzchar(variable %||% "")) {
        exogenous_targets <- unique(c(exogenous_targets, variable))
      }
      next
    }

    if (identical(command, "ENDOGENOUS")) {
      variable <- extract_fp_named_arg(raw, key = "VARIABLE")
      if (nzchar(variable %||% "")) {
        exogenous_targets <- exogenous_targets[toupper(exogenous_targets) != toupper(variable)]
      }
      next
    }

    if (identical(command, "EXTRAPOLATE")) {
      if (!is.null(active_window)) {
        working <- apply_extrapolate_frame(
          working,
          window_start = active_window[[1]],
          window_end = active_window[[2]],
          variables = exogenous_targets,
          include_all_columns = TRUE
        )
      }
      next
    }

    if (identical(command, "SETUPSOLVE")) {
      setupsolve <- modifyList(setupsolve, parse_setupsolve_options(raw))
      next
    }

    if (identical(command, "SOLVE")) {
      solve_metadata <- solve_statement_metadata(statement, statements[seq.int(idx + 1L, length(statements))])
      solve_snapshot <- list(
        solve_index = idx,
        sample_start = if (is.null(active_window)) NULL else active_window[[1]],
        sample_end = if (is.null(active_window)) NULL else active_window[[2]],
        exogenous_targets = exogenous_targets,
        setupsolve = setupsolve,
        solve_options = solve_metadata$options,
        watch_variables = solve_metadata$watch_variables,
        solve_option_text = solve_metadata$option_text,
        solve_watch_text = solve_metadata$watch_text
      )
    }
  }

  if (!saw_runtime_input && !is.null(default_fmexog_path) && nzchar(default_fmexog_path) && file.exists(default_fmexog_path)) {
    working <- apply_fmexog_rows(working, parse_fmexog_file(default_fmexog_path))
    working <- sort_frame_by_period(working)
  }

  list(
    frame = working,
    solve_snapshot = solve_snapshot,
    exogenous_targets = exogenous_targets,
    saw_runtime_input = saw_runtime_input,
    setupsolve = setupsolve,
    termination_command = termination_command,
    termination_index = as.integer(termination_index)
  )
}

replay_standard_postsolve <- function(bundle, frame, work_dir) {
  statements <- bundle$runtime$statements %||% list()
  if (!length(statements)) {
    return(list(frame = frame, emitted_files = character(), termination_command = ""))
  }
  start_index <- as.integer(bundle$runtime$solve_index %||% 0L) + 1L
  if (start_index > length(statements)) {
    return(list(frame = frame, emitted_files = character(), termination_command = ""))
  }
  working <- sort_frame_by_period(frame)
  active_window <- NULL
  if (nzchar(bundle$runtime$solve_window_start %||% "") && nzchar(bundle$runtime$solve_window_end %||% "")) {
    active_window <- c(bundle$runtime$solve_window_start, bundle$runtime$solve_window_end)
  }
  emitted_files <- character()
  termination_command <- ""

  for (idx in seq.int(start_index, length(statements))) {
    statement <- statements[[idx]]
    raw <- statement$raw %||% ""
    command <- statement_command_runtime(statement)
    if (command %in% c("QUIT", "RETURN")) {
      termination_command <- command
      break
    }
    if (identical(command, "SMPL")) {
      parsed_window <- parse_smpl_statement(raw)
      if (!is.null(parsed_window)) {
        active_window <- c(parsed_window$start, parsed_window$end)
      }
      next
    }
    if (identical(command, "SETYYTOY")) {
      if (!is.null(active_window)) {
        working <- apply_setyytoy_frame(working, active_window[[1]], active_window[[2]])
      }
      next
    }
    if (identical(command, "PRINTNAMES")) {
      emitted_path <- emit_printnames_output(working, work_dir)
      if (!is.null(emitted_path)) {
        emitted_files <- c(emitted_files, emitted_path)
      }
      next
    }
    if (identical(command, "PRINTMODEL")) {
      emitted_paths <- c(
        emit_printmodel_output(bundle, work_dir),
        emit_printmodel_support_outputs(bundle, work_dir)
      )
      if (length(emitted_paths)) {
        emitted_files <- c(emitted_files, emitted_paths)
      }
      next
    }
    if (!identical(command, "PRINTVAR")) {
      next
    }
    parsed_printvar <- parse_printvar_statement(raw)
    if (is.null(parsed_printvar)) {
      next
    }
    output_path <- if (nzchar(parsed_printvar$fileout %||% "")) {
      resolve_runtime_output_path(parsed_printvar$fileout, work_dir)
    } else if (isTRUE(parsed_printvar$stats)) {
      resolve_generated_output_path("PRINTVAR_STATS.csv", work_dir)
    } else if (!isTRUE(parsed_printvar$loadformat)) {
      resolve_generated_output_path("PRINTVAR_TABLE.csv", work_dir)
    } else {
      next
    }
    dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
    if (isTRUE(parsed_printvar$stats)) {
      write_printvar_stats(
        working,
        output_path = output_path,
        variables = parsed_printvar$variables,
        active_window = active_window
      )
    } else if (isTRUE(parsed_printvar$loadformat)) {
      write_printvar_loadformat(
        working,
        output_path = output_path,
        variables = parsed_printvar$variables,
        active_window = active_window,
        fmout_path = bundle$source$fmout %||% NULL,
        fallback_paths = resolve_printvar_fallback_paths(
          bundle$source %||% list(),
          search_dirs = unique(Filter(nzchar, c(
            dirname(bundle$source$entry_input %||% ""),
            dirname(bundle$source$fmdata %||% ""),
            dirname(bundle$source$fmout %||% "")
          )))
        )
      )
    } else {
      write_printvar_table(
        working,
        output_path = output_path,
        variables = parsed_printvar$variables,
        active_window = active_window
      )
    }
    emitted_files <- c(emitted_files, output_path)
  }

  list(frame = working, emitted_files = unique(emitted_files), termination_command = termination_command)
}

parsed_frame_from_standard_data <- function(fmdata_path, loaddata_paths = character(), fmexog_path = NULL) {
  base_frame <- if (is.null(fmdata_path) || !nzchar(fmdata_path)) {
    data.frame(period = character(), stringsAsFactors = FALSE, check.names = FALSE)
  } else {
    parse_fm_numeric_file(fmdata_path, block_name = basename(fmdata_path))
  }
  if (!length(loaddata_paths)) {
    frame <- base_frame
  } else {
    frame <- base_frame
    for (path in loaddata_paths) {
      frame <- merge_fm_numeric_frames(
        frame,
        parse_fm_numeric_file(path, block_name = basename(path))
      )
    }
  }

  if (!is.null(fmexog_path) && nzchar(fmexog_path) && file.exists(fmexog_path)) {
    frame <- apply_fmexog_rows(frame, parse_fmexog_file(fmexog_path))
  }
  frame
}

state_from_frame <- function(frame) {
  series <- list()
  if (nrow(frame)) {
    for (name in setdiff(names(frame), "period")) {
      series[[name]] <- as.numeric(frame[[name]])
    }
    return(list(periods = as.character(frame$period), series = series))
  }
  list(periods = character(), series = series)
}

build_frame_finite_mask <- function(frame) {
  periods <- as.character(frame$period %||% character())
  mask <- lapply(setdiff(names(frame), "period"), function(name) {
    numeric_values <- as.numeric(frame[[name]])
    values <- as.logical(is.finite(numeric_values) & abs(numeric_values + 99.0) > 1e-12)
    names(values) <- periods
    values
  })
  setNames(mask, setdiff(names(frame), "period"))
}

align_frame_finite_mask <- function(mask, frame) {
  periods <- as.character(frame$period %||% character())
  out <- lapply(setdiff(names(frame), "period"), function(name) {
    values <- rep(FALSE, length(periods))
    names(values) <- periods
    current <- mask[[name]] %||% NULL
    if (!is.null(current)) {
      current_names <- names(current)
      current <- as.logical(current)
      names(current) <- current_names
      current_periods <- names(current) %||% character()
      shared <- intersect(periods, current_periods)
      if (length(shared)) {
        values[shared] <- current[shared]
      }
    }
    values
  })
  setNames(out, setdiff(names(frame), "period"))
}

runtime_assignment_references_target <- function(statement, target, compiled = NULL) {
  expression <- statement$expression %||% NULL
  if (is.null(expression) || !nzchar(target)) {
    return(FALSE)
  }
  compiled <- compiled %||% statement$compiled %||% compile_expression(expression)
  refs <- compiled$references %||% NULL
  if (is.null(refs) || !nrow(refs)) {
    return(FALSE)
  }
  any(toupper(as.character(refs$name %||% character())) == toupper(target), na.rm = TRUE)
}

update_pre_solve_protected_target <- function(protected_frame, working, target) {
  target <- toupper(as.character(target %||% ""))
  if (!nzchar(target) || !nrow(working)) {
    return(protected_frame)
  }
  protected <- ensure_frame_periods(
    protected_frame,
    as.character(working$period %||% character())
  )
  protected <- sort_frame_by_period(protected)
  shared_periods <- intersect(as.character(protected$period), as.character(working$period))
  if (!length(shared_periods)) {
    return(protected)
  }
  if (!(target %in% names(working))) {
    return(protected)
  }
  if (!(target %in% names(protected))) {
    protected[[target]] <- NA_real_
  }
  protected_idx <- match(shared_periods, as.character(protected$period))
  working_idx <- match(shared_periods, as.character(working$period))
  protected[[target]][protected_idx] <- as.numeric(working[[target]][working_idx])
  protected
}

restore_historical_boundary_replay_targets <- function(
  frame,
  replayed_frame,
  replay_plan_rows,
  sample_start,
  protected_targets = character()
) {
  if (!nrow(frame) || !nrow(replayed_frame) || !is.data.frame(replay_plan_rows) || !nrow(replay_plan_rows)) {
    return(replayed_frame)
  }
  sample_start <- as.character(sample_start %||% "")
  if (!nzchar(sample_start)) {
    return(replayed_frame)
  }
  sample_index <- parse_period(sample_start)$index
  if (!is.finite(sample_index) || sample_index <= 0L) {
    return(replayed_frame)
  }
  boundary_period <- format_period(sample_index - 1L)
  replay_windows <- replay_plan_rows$active_window_start %||% character()
  replay_window_index <- suppressWarnings(vapply(replay_windows, function(period) {
    if (!nzchar(as.character(period %||% ""))) {
      return(NA_integer_)
    }
    parse_period(period)$index
  }, integer(1)))
  restore_rows <- replay_plan_rows[
    replay_plan_rows$plan_type %in% c("assignment", "changevar_assignment") &
      nzchar(as.character(replay_plan_rows$target %||% "")) &
      as.character(replay_plan_rows$active_window_end %||% "") == boundary_period &
      is.finite(replay_window_index) &
      replay_window_index < sample_index,
    ,
    drop = FALSE
  ]
  restore_targets <- unique(toupper(as.character(restore_rows$target %||% character())))
  restore_targets <- setdiff(restore_targets[nzchar(restore_targets)], unique(toupper(as.character(protected_targets %||% character()))))
  if (!length(restore_targets)) {
    return(replayed_frame)
  }
  source_pos <- match(boundary_period, as.character(frame$period %||% character()))
  target_pos <- match(boundary_period, as.character(replayed_frame$period %||% character()))
  if (is.na(source_pos) || is.na(target_pos)) {
    return(replayed_frame)
  }
  restored <- replayed_frame
  for (target in restore_targets) {
    source_column <- resolve_frame_column_name(frame, target)
    target_column <- resolve_frame_column_name(restored, target)
    if (!(source_column %in% names(frame))) {
      next
    }
    source_value <- as.numeric(frame[[source_column]][[source_pos]])
    if (!is.finite(source_value) || abs(source_value + 99.0) <= 1e-12) {
      next
    }
    if (!(target_column %in% names(restored))) {
      restored[[target_column]] <- NA_real_
    }
    restored[[target_column]][[target_pos]] <- source_value
  }
  restored
}

runtime_assignment_positions <- function(frame, active_window = NULL) {
  if (!nrow(frame)) {
    return(integer())
  }
  if (is.null(active_window) || length(active_window) < 2L) {
    return(seq_len(nrow(frame)))
  }
  positions <- match(seq_periods(active_window[[1]], active_window[[2]]), as.character(frame$period))
  positions[is.finite(positions)]
}

is_runtime_assignment_statement <- function(statement, allow_lhs = FALSE) {
  command <- statement_command_runtime(statement)
  allowed <- c("CREATE", "GENR", "IDENT")
  if (isTRUE(allow_lhs)) {
    allowed <- c(allowed, "LHS")
  }
  command %in% allowed
}

apply_runtime_assignment_frame <- function(frame, statement, active_window = NULL, allow_lhs = FALSE, coef_values = NULL, preserve_existing = FALSE, preserve_mask = NULL, preserve_mode = c("skip", "fallback")) {
  result <- apply_runtime_assignment_state_frame(
    frame,
    statement,
    active_window = active_window,
    allow_lhs = allow_lhs,
    coef_values = coef_values,
    preserve_existing = preserve_existing,
    preserve_mask = preserve_mask,
    preserve_mode = preserve_mode
  )
  result$frame
}

apply_runtime_assignment_state_frame <- function(frame, statement, active_window = NULL, allow_lhs = FALSE, coef_values = NULL, state = NULL, preserve_existing = FALSE, preserve_mask = NULL, preserve_mode = c("skip", "fallback")) {
  numeric_values_equal <- function(lhs, rhs) {
    lhs_numeric <- as.numeric(lhs)
    rhs_numeric <- as.numeric(rhs)
    if (length(lhs_numeric) != 1L || length(rhs_numeric) != 1L) {
      return(FALSE)
    }
    if (is.na(lhs_numeric) && is.na(rhs_numeric)) {
      return(identical(is.nan(lhs_numeric), is.nan(rhs_numeric)))
    }
    identical(lhs_numeric, rhs_numeric)
  }

  preserve_mode <- match.arg(preserve_mode)
  if (!is_runtime_assignment_statement(statement, allow_lhs = allow_lhs)) {
    return(list(frame = frame, state = state, changed = FALSE))
  }

  target <- as.character(statement$name %||% "")
  if (!nzchar(target)) {
    return(list(frame = frame, state = state, changed = FALSE))
  }
  periods <- if (is.null(active_window) || length(active_window) < 2L) {
    if ("period" %in% names(frame)) as.character(frame$period) else character()
  } else {
    seq_periods(active_window[[1]], active_window[[2]])
  }
  working <- ensure_frame_periods(frame, periods)
  target_positions <- runtime_assignment_positions(working, active_window)
  state_periods <- as.character(state$periods %||% character())
  if (is.null(state) || !identical(state_periods, as.character(working$period))) {
    state <- state_from_frame(working)
  }
  state$coef_values <- coef_values %||% state$coef_values %||% list()

  if (!(target %in% names(working))) {
    working[[target]] <- NA_real_
  }
  if (isTRUE(preserve_existing) && is.null(preserve_mask)) {
    preserve_mask <- build_frame_finite_mask(working)
  }
  target_values <- as.numeric(state$series[[target]] %||% working[[target]])
  target_mask <- rep(FALSE, length(target_values))
  target_mask_names <- as.character(working$period)
  names(target_mask) <- target_mask_names
  if (isTRUE(preserve_existing) && !is.null(preserve_mask[[target]])) {
    current_mask_names <- names(preserve_mask[[target]])
    current_mask <- as.logical(preserve_mask[[target]])
    names(current_mask) <- current_mask_names
    current_periods <- names(current_mask) %||% character()
    shared <- intersect(target_mask_names, current_periods)
    if (length(shared)) {
      target_mask[shared] <- current_mask[shared]
    }
  }
  if (isTRUE(preserve_existing) &&
    identical(preserve_mode, "skip") &&
    length(target_positions)) {
    all_protected <- all(vapply(target_positions, function(period_pos) {
      period <- target_mask_names[[period_pos]]
      isTRUE(target_mask[[period]]) && is.finite(as.numeric(target_values[[period_pos]]))
    }, logical(1)))
    if (isTRUE(all_protected)) {
      working[[target]] <- target_values
      state$series[[target]] <- target_values
      return(list(frame = working, state = state, changed = FALSE))
    }
  }

  expression <- statement$expression %||% NULL
  if (is.null(expression)) {
    changed <- FALSE
    if (length(target_positions)) {
      changed <- any(!vapply(target_values[target_positions], function(value) numeric_values_equal(value, NA_real_), logical(1)))
      target_values[target_positions] <- NA_real_
    }
    working[[target]] <- target_values
    state$series[[target]] <- target_values
    return(list(frame = working, state = state, changed = changed))
  }

  compiled <- statement$compiled %||% compile_expression(expression)
  self_referential <- runtime_assignment_references_target(statement, target, compiled = compiled)
  eval_positions <- target_positions
  if (isTRUE(preserve_existing) && identical(preserve_mode, "skip") && length(eval_positions)) {
    protected_positions <- eval_positions[
      vapply(eval_positions, function(period_pos) {
        period <- target_mask_names[[period_pos]]
        isTRUE(target_mask[[period]]) && is.finite(as.numeric(target_values[[period_pos]]))
      }, logical(1))
    ]
    if (length(protected_positions)) {
      eval_positions <- setdiff(eval_positions, protected_positions)
    }
  }
  changed <- FALSE
  if (!isTRUE(self_referential) && length(eval_positions)) {
    values <- as.numeric(evaluate_compiled_expression_positions(
      compiled,
      state,
      eval_positions,
      strict = FALSE
    ))
    if (length(values) != length(eval_positions)) {
      values <- rep_len(values, length(eval_positions))
    }
    if (isTRUE(preserve_existing) && identical(preserve_mode, "fallback")) {
      restore_mask <- vapply(seq_along(eval_positions), function(idx) {
        period_pos <- eval_positions[[idx]]
        period <- target_mask_names[[period_pos]]
        protected_value <- as.numeric(target_values[[period_pos]])
        isTRUE(target_mask[[period]]) && is.finite(protected_value) && !is.finite(values[[idx]])
      }, logical(1))
      if (any(restore_mask)) {
        values[restore_mask] <- target_values[eval_positions[restore_mask]]
      }
    }
    if (length(values)) {
      changed <- any(!mapply(numeric_values_equal, target_values[eval_positions], values))
      target_values[eval_positions] <- values
    }
    working[[target]] <- target_values
    state$series[[target]] <- target_values
    return(list(frame = working, state = state, changed = changed))
  }
  for (period_pos in eval_positions) {
    period <- target_mask_names[[period_pos]]
    protected_value <- as.numeric(target_values[[period_pos]])
    if (isTRUE(preserve_existing) &&
      identical(preserve_mode, "skip") &&
      isTRUE(target_mask[[period]]) &&
      is.finite(protected_value)) {
      next
    }
    value <- as.numeric(evaluate_compiled_expression(
      compiled,
      state,
      period_pos,
      strict = FALSE
    ))
    if (isTRUE(preserve_existing) &&
      identical(preserve_mode, "fallback") &&
      isTRUE(target_mask[[period]]) &&
      is.finite(protected_value) &&
      !is.finite(value)) {
      value <- protected_value
    }
    if (!numeric_values_equal(target_values[[period_pos]], value)) {
      changed <- TRUE
    }
    target_values[[period_pos]] <- value
    if (isTRUE(self_referential)) {
      state$series[[target]] <- target_values
    }
  }
  working[[target]] <- target_values
  state$series[[target]] <- target_values

  list(frame = working, state = state, changed = changed)
}

base_standard_input_frame <- function(fmdata_path) {
  if (is.null(fmdata_path) || !nzchar(fmdata_path)) {
    return(data.frame(period = character(), stringsAsFactors = FALSE, check.names = FALSE))
  }
  parse_fm_numeric_file(fmdata_path, block_name = basename(fmdata_path))
}

filter_standard_specs_for_exogenous <- function(specs, exogenous_targets = character(), retained_targets = character()) {
  if (!length(exogenous_targets)) {
    return(specs)
  }
  normalized_retained_targets <- toupper(as.character(retained_targets %||% character()))
  Filter(
    function(item) {
      target <- toupper(item$name %||% item$target %||% "")
      !(target %in% toupper(exogenous_targets) && !(target %in% normalized_retained_targets))
    },
    specs
  )
}

partition_standard_solve_specs <- function(
  eq_specs = list(),
  candidate_specs = list(),
  exogenous_targets = character(),
  exogenous_equation_target_policy = "exclude_from_solve"
) {
  normalized_policy <- tolower(as.character(
    exogenous_equation_target_policy %||% "exclude_from_solve"
  ))
  if (!normalized_policy %in% c(
    "exclude_from_solve",
    "retain_equation_targets",
    "retain_reduced_eq_only"
  )) {
    stopf("Unknown exogenous equation-target policy: %s", normalized_policy)
  }
  eq_targets <- unique(toupper(vapply(
    eq_specs %||% list(),
    function(item) as.character(item$target %||% item$name %||% ""),
    character(1)
  )))
  eq_targets <- eq_targets[nzchar(eq_targets)]
  candidate_targets <- vapply(
    candidate_specs %||% list(),
    function(item) toupper(as.character(item$name %||% item$target %||% "")),
    character(1)
  )
  candidate_kinds <- vapply(
    candidate_specs %||% list(),
    function(item) tolower(as.character(item$kind %||% "")),
    character(1)
  )
  setup_candidate_mask <- if (length(candidate_specs)) {
    candidate_kinds %in% c("create", "genr", "ident")
  } else {
    logical()
  }
  same_target_lhs <- vapply(
    candidate_specs %||% list(),
    function(item) {
      identical(tolower(as.character(item$kind %||% "")), "lhs") &&
        toupper(as.character(item$name %||% item$target %||% "")) %in% eq_targets
    },
    logical(1)
  )
  same_target_setup <- if (length(candidate_specs)) {
    setup_candidate_mask &
      candidate_targets %in% eq_targets
  } else {
    logical()
  }
  pure_create_setup_mask <- if (length(candidate_specs)) {
    vapply(seq_along(candidate_specs), function(idx) {
      if (!identical(candidate_kinds[[idx]], "create")) {
        return(FALSE)
      }
      target <- candidate_targets[[idx]]
      if (!nzchar(target) || target %in% eq_targets) {
        return(FALSE)
      }
      item <- candidate_specs[[idx]]
      compiled <- item$compiled %||% NULL
      if (is.null(compiled) && !is.null(item$expression)) {
        compiled <- compile_expression(item$expression)
      }
      refs <- compiled$references %||% NULL
      if (is.null(refs) || !nrow(refs)) {
        return(TRUE)
      }
      zero_refs <- unique(toupper(as.character(refs$name[refs$lag == 0L])))
      !length(zero_refs[nzchar(zero_refs)])
    }, logical(1))
  } else {
    logical()
  }
  safe_setup_targets <- character()
  if (length(candidate_specs)) {
    setup_targets <- candidate_targets[setup_candidate_mask]
    setup_targets <- setup_targets[nzchar(setup_targets)]
    duplicated_setup_targets <- unique(setup_targets[duplicated(setup_targets)])
    if (length(duplicated_setup_targets)) {
      repeat {
        prior_count <- length(safe_setup_targets)
        for (idx in which(setup_candidate_mask)) {
          target <- candidate_targets[[idx]]
          if (!nzchar(target) || target %in% eq_targets || target %in% safe_setup_targets) {
            next
          }
          item <- candidate_specs[[idx]]
          compiled <- item$compiled %||% NULL
          if (is.null(compiled) && !is.null(item$expression)) {
            compiled <- compile_expression(item$expression)
          }
          refs <- compiled$references %||% NULL
          zero_refs <- if (is.null(refs) || !nrow(refs)) {
            character()
          } else {
            unique(toupper(as.character(refs$name[refs$lag == 0L])))
          }
          zero_refs <- zero_refs[nzchar(zero_refs)]
          candidate_zero_refs <- intersect(zero_refs, setup_targets)
          if (length(setdiff(candidate_zero_refs, safe_setup_targets))) {
            next
          }
          if (any(intersect(zero_refs, candidate_targets[!setup_candidate_mask]) %in% candidate_targets[!same_target_lhs])) {
            next
          }
          seeded_by_window_chain <- target %in% duplicated_setup_targets ||
            any(candidate_zero_refs %in% safe_setup_targets)
          if (!seeded_by_window_chain) {
            next
          }
          safe_setup_targets <- c(safe_setup_targets, target)
        }
        safe_setup_targets <- unique(safe_setup_targets)
        if (length(safe_setup_targets) == prior_count) {
          break
        }
      }
    }
  }
  safe_setup_mask <- if (length(candidate_specs)) {
    setup_candidate_mask &
      candidate_targets %in% safe_setup_targets &
      !same_target_setup
  } else {
    logical()
  }
  setup_only_mask <- same_target_setup | safe_setup_mask | pure_create_setup_mask
  setup_only_assignments <- if (length(candidate_specs)) candidate_specs[setup_only_mask] else list()
  post_solve_assignments <- if (length(candidate_specs)) candidate_specs[same_target_lhs] else list()
  candidate_solve_specs <- if (length(candidate_specs)) {
    candidate_specs[!(same_target_lhs | setup_only_mask)]
  } else {
    list()
  }
  if (length(candidate_solve_specs)) {
    solve_targets <- toupper(vapply(
      candidate_solve_specs,
      function(item) as.character(item$target %||% item$name %||% ""),
      character(1)
    ))
    solve_kinds <- tolower(vapply(
      candidate_solve_specs,
      function(item) as.character(item$kind %||% ""),
      character(1)
    ))
    keep_mask <- rep(TRUE, length(candidate_solve_specs))
    setup_indices <- which(solve_kinds %in% c("create", "genr", "ident") & nzchar(solve_targets))
    duplicated_targets <- unique(solve_targets[setup_indices][duplicated(solve_targets[setup_indices])])
    for (target in duplicated_targets) {
      target_indices <- setup_indices[solve_targets[setup_indices] == target]
      first_index <- target_indices[[1L]]
      last_index <- target_indices[[length(target_indices)]]
      candidate_solve_specs[[first_index]] <- candidate_solve_specs[[last_index]]
      keep_mask[target_indices[-1L]] <- FALSE
    }
    candidate_solve_specs <- candidate_solve_specs[keep_mask]
  }
  retained_eq_targets <- if (normalized_policy %in% c("retain_equation_targets", "retain_reduced_eq_only")) {
    intersect(eq_targets, toupper(as.character(exogenous_targets %||% character())))
  } else {
    character()
  }
  retained_candidate_targets <- if (identical(normalized_policy, "retain_equation_targets")) {
    retained_eq_targets
  } else {
    character()
  }
  list(
    specs = c(
      filter_standard_specs_for_exogenous(
        eq_specs %||% list(),
        exogenous_targets = exogenous_targets,
        retained_targets = retained_eq_targets
      ),
      filter_standard_specs_for_exogenous(
        candidate_solve_specs,
        exogenous_targets = exogenous_targets,
        retained_targets = retained_candidate_targets
      )
    ),
    setup_only_assignments = filter_standard_specs_for_exogenous(
      setup_only_assignments,
      exogenous_targets = exogenous_targets,
      retained_targets = character()
    ),
    post_solve_assignments = filter_standard_specs_for_exogenous(
      post_solve_assignments,
      exogenous_targets = exogenous_targets,
      retained_targets = character()
    )
  )
}

order_runtime_replay_assignment_block <- function(block_statements) {
  if (!length(block_statements)) {
    return(list(statements = list(), cyclic_targets = character()))
  }
  if (length(block_statements) == 1L) {
    return(list(statements = block_statements, cyclic_targets = character()))
  }
  specs <- lapply(block_statements, function(item) {
    list(
      target = as.character(item$name %||% item$target %||% ""),
      expression = item$expression %||% NULL,
      compiled = item$compiled %||% NULL
    )
  })
  details <- build_dependency_order_details(specs)
  block_targets <- toupper(vapply(block_statements, function(item) as.character(item$name %||% item$target %||% ""), character(1)))
  ordered_index <- match(details$order, block_targets)
  ordered_index <- ordered_index[is.finite(ordered_index)]
  list(
    statements = block_statements[ordered_index],
    cyclic_targets = unique(toupper(as.character(details$cyclic_targets %||% character())))
  )
}

build_runtime_replay_plan <- function(items, assignment_targets = character(), replay_inline_changevar = TRUE) {
  targets <- unique(toupper(as.character(assignment_targets %||% character())))
  targets <- targets[nzchar(targets)]
  plan <- list()
  pending_block <- list()
  pending_targets <- character()
  cyclic_targets <- character()

  flush_pending_block <- function() {
    if (!length(pending_block)) {
      return(NULL)
    }
    ordered <- order_runtime_replay_assignment_block(pending_block)
    plan[[length(plan) + 1L]] <<- list(
      type = "assignments",
      statements = ordered$statements
    )
    cyclic_targets <<- c(cyclic_targets, ordered$cyclic_targets)
    pending_block <<- list()
    pending_targets <<- character()
    NULL
  }

  idx <- 1L
  while (idx <= length(items)) {
    statement <- items[[idx]]
    command <- statement_command_runtime(statement)
    if (identical(command, "SMPL")) {
      flush_pending_block()
      plan[[length(plan) + 1L]] <- list(type = "smpl", statement = statement)
      idx <- idx + 1L
      next
    }
    if (identical(command, "CHANGEVAR")) {
      flush_pending_block()
      if (isTRUE(replay_inline_changevar)) {
        payload_raw <- inline_changevar_payload_raw(items, idx)
        if (nzchar(payload_raw)) {
          plan[[length(plan) + 1L]] <- list(type = "changevar", statement = statement, raw = payload_raw)
          idx <- idx + 2L
          next
        }
      }
      idx <- idx + 1L
      next
    }
    if (!is_runtime_assignment_statement(statement)) {
      idx <- idx + 1L
      next
    }
    target <- toupper(as.character(statement$name %||% statement$target %||% ""))
    if (length(targets) && !(target %in% targets)) {
      idx <- idx + 1L
      next
    }
    if (target %in% pending_targets) {
      flush_pending_block()
    }
    pending_block[[length(pending_block) + 1L]] <- statement
    pending_targets <- c(pending_targets, target)
    idx <- idx + 1L
  }
  flush_pending_block()

  revisit_targets <- unique(cyclic_targets[nzchar(cyclic_targets)])
  if (length(revisit_targets)) {
    changed <- TRUE
    while (changed) {
      changed <- FALSE
      for (plan_item in plan) {
        for (statement in plan_item$statements %||% list()) {
          target <- toupper(as.character(statement$name %||% statement$target %||% ""))
          if (!nzchar(target) || target %in% revisit_targets) {
            next
          }
          compiled <- statement$compiled %||% compile_expression(statement$expression %||% "")
          refs <- compiled$references %||% NULL
          deps <- if (!is.null(refs) && nrow(refs) > 0L) {
            unique(toupper(as.character(refs$name[refs$lag == 0L])))
          } else {
            character()
          }
          if (any(deps %in% revisit_targets)) {
            revisit_targets <- c(revisit_targets, target)
            changed <- TRUE
          }
        }
      }
    }
  }

  list(
    plan = plan,
    cyclic_targets = unique(cyclic_targets[nzchar(cyclic_targets)]),
    revisit_targets = unique(revisit_targets[nzchar(revisit_targets)])
  )
}

build_runtime_replay_plan_rows <- function(plan_payload, preserve_modes_by_target = NULL) {
  plan_items <- plan_payload$plan %||% list()
  if (!length(plan_items)) {
    return(data.frame(
      replay_order = integer(),
      plan_type = character(),
      command = character(),
      target = character(),
      active_window_start = character(),
      active_window_end = character(),
      preserve_mode = character(),
      changevar_applied = logical(),
      source_path = character(),
      source_order = integer(),
      cycle_revisit = logical(),
      raw = character(),
      stringsAsFactors = FALSE
    ))
  }
  active_window <- NULL
  revisit_targets <- unique(toupper(as.character(plan_payload$revisit_targets %||% character())))
  rows <- list()
  replay_order <- 1L
  for (plan_item in plan_items) {
    if (identical(plan_item$type, "smpl")) {
      statement <- plan_item$statement %||% list()
      parsed_window <- parse_smpl_statement(statement$raw %||% "")
      if (!is.null(parsed_window)) {
        active_window <- c(parsed_window$start, parsed_window$end)
      }
      rows[[length(rows) + 1L]] <- data.frame(
        replay_order = replay_order,
        plan_type = "smpl",
        command = "SMPL",
        target = "",
        active_window_start = as.character(active_window[[1]] %||% ""),
        active_window_end = as.character(active_window[[2]] %||% ""),
        preserve_mode = "",
        changevar_applied = FALSE,
        source_path = as.character(statement$source_path %||% ""),
        source_order = as.integer(statement$source_order %||% 0L),
        cycle_revisit = FALSE,
        raw = as.character(statement$raw %||% ""),
        stringsAsFactors = FALSE
      )
      replay_order <- replay_order + 1L
      next
    }
    if (identical(plan_item$type, "changevar")) {
      statement <- plan_item$statement %||% list()
      rows[[length(rows) + 1L]] <- data.frame(
        replay_order = replay_order,
        plan_type = "changevar",
        command = "CHANGEVAR",
        target = "",
        active_window_start = as.character(active_window[[1]] %||% ""),
        active_window_end = as.character(active_window[[2]] %||% ""),
        preserve_mode = "",
        changevar_applied = TRUE,
        source_path = as.character(statement$source_path %||% ""),
        source_order = as.integer(statement$source_order %||% 0L),
        cycle_revisit = FALSE,
        raw = as.character(plan_item$raw %||% statement$raw %||% ""),
        stringsAsFactors = FALSE
      )
      replay_order <- replay_order + 1L
      next
    }
    for (statement in plan_item$statements %||% list()) {
      target <- toupper(as.character(statement$name %||% statement$target %||% ""))
      rows[[length(rows) + 1L]] <- data.frame(
        replay_order = replay_order,
        plan_type = "assignment",
        command = statement_command_runtime(statement),
        target = target,
        active_window_start = as.character(active_window[[1]] %||% ""),
        active_window_end = as.character(active_window[[2]] %||% ""),
        preserve_mode = if (!is.null(preserve_modes_by_target) && target %in% names(preserve_modes_by_target)) as.character(preserve_modes_by_target[[target]]) else "",
        changevar_applied = FALSE,
        source_path = as.character(statement$source_path %||% ""),
        source_order = as.integer(statement$source_order %||% 0L),
        cycle_revisit = isTRUE(target %in% revisit_targets),
        raw = as.character(statement$raw %||% ""),
        stringsAsFactors = FALSE
      )
      replay_order <- replay_order + 1L
    }
  }
  do.call(rbind, rows)
}

replay_selected_runtime_assignments <- function(statements, frame, assignment_targets = character(), coef_values = NULL, preserve_existing = FALSE, preserve_mode = c("skip", "fallback"), preserve_modes_by_target = NULL, replay_inline_changevar = TRUE, replay_profile_path = "") {
  preserve_mode <- match.arg(preserve_mode)
  targets <- unique(toupper(as.character(assignment_targets %||% character())))
  targets <- targets[nzchar(targets)]
  if (!length(targets) || !length(statements)) {
    return(sort_frame_by_period(frame))
  }
  replay_profile_rows <- list()

  target_snapshot <- function(current_frame, tracked_targets = targets) {
    snapshot <- data.frame(
      period = as.character(current_frame$period %||% character()),
      stringsAsFactors = FALSE,
      check.names = FALSE
    )
    tracked_targets <- unique(toupper(as.character(tracked_targets %||% character())))
    tracked_targets <- tracked_targets[nzchar(tracked_targets)]
    for (target in tracked_targets) {
      resolved <- resolve_frame_column_name(current_frame, target)
      if (resolved %in% names(current_frame)) {
        snapshot[[target]] <- as.numeric(current_frame[[resolved]])
      } else {
        snapshot[[target]] <- rep(NA_real_, nrow(snapshot))
      }
    }
    snapshot
  }

  order_replay_assignment_block <- function(block_statements) {
    if (!length(block_statements)) {
      return(list(statements = list(), cyclic_targets = character()))
    }
    if (length(block_statements) == 1L) {
      target <- toupper(as.character(block_statements[[1L]]$name %||% block_statements[[1L]]$target %||% ""))
      return(list(
        statements = block_statements,
        cyclic_targets = character()
      ))
    }
    specs <- lapply(block_statements, function(item) {
      list(
        target = as.character(item$name %||% item$target %||% ""),
        expression = item$expression %||% NULL,
        compiled = item$compiled %||% NULL
      )
    })
    details <- build_dependency_order_details(specs)
    block_targets <- toupper(vapply(block_statements, function(item) as.character(item$name %||% item$target %||% ""), character(1)))
    ordered_index <- match(details$order, block_targets)
    ordered_index <- ordered_index[is.finite(ordered_index)]
    list(
      statements = block_statements[ordered_index],
      cyclic_targets = unique(toupper(as.character(details$cyclic_targets %||% character())))
    )
  }

  build_replay_plan <- function(items) {
    plan <- list()
    pending_block <- list()
    pending_targets <- character()
    cyclic_targets <- character()

    flush_pending_block <- function() {
      if (!length(pending_block)) {
        return(NULL)
      }
      ordered <- order_replay_assignment_block(pending_block)
      plan[[length(plan) + 1L]] <<- list(
        type = "assignments",
        statements = ordered$statements
      )
      cyclic_targets <<- c(cyclic_targets, ordered$cyclic_targets)
      pending_block <<- list()
      pending_targets <<- character()
      NULL
    }

    idx <- 1L
    while (idx <= length(items)) {
      statement <- items[[idx]]
      command <- statement_command_runtime(statement)
      raw <- statement$raw %||% ""
      if (identical(command, "SMPL")) {
        flush_pending_block()
        plan[[length(plan) + 1L]] <- list(
          type = "smpl",
          raw = raw
        )
        idx <- idx + 1L
        next
      }
      if (identical(command, "CHANGEVAR")) {
        flush_pending_block()
        if (isTRUE(replay_inline_changevar)) {
          payload_raw <- inline_changevar_payload_raw(items, idx)
          if (nzchar(payload_raw)) {
            plan[[length(plan) + 1L]] <- list(
              type = "changevar",
              raw = payload_raw
            )
            idx <- idx + 2L
            next
          }
        }
        idx <- idx + 1L
        next
      }
      if (!is_runtime_assignment_statement(statement)) {
        idx <- idx + 1L
        next
      }
      target <- toupper(as.character(statement$name %||% statement$target %||% ""))
      if (!(target %in% targets)) {
        idx <- idx + 1L
        next
      }
      if (target %in% pending_targets) {
        flush_pending_block()
      }
      pending_block[[length(pending_block) + 1L]] <- statement
      pending_targets <- c(pending_targets, target)
      idx <- idx + 1L
    }
    flush_pending_block()
    revisit_targets <- unique(cyclic_targets[nzchar(cyclic_targets)])
    if (length(revisit_targets)) {
      changed <- TRUE
      while (changed) {
        changed <- FALSE
        for (plan_item in plan) {
          for (statement in plan_item$statements %||% list()) {
            target <- toupper(as.character(statement$name %||% statement$target %||% ""))
            if (!nzchar(target) || target %in% revisit_targets) {
              next
            }
            compiled <- statement$compiled %||% compile_expression(statement$expression %||% "")
            refs <- compiled$references %||% NULL
            deps <- if (!is.null(refs) && nrow(refs) > 0L) {
              unique(toupper(as.character(refs$name[refs$lag == 0L])))
            } else {
              character()
            }
            if (any(deps %in% revisit_targets)) {
              revisit_targets <- c(revisit_targets, target)
              changed <- TRUE
            }
          }
        }
      }
    }
    list(
      plan = plan,
      cyclic_targets = unique(cyclic_targets[nzchar(cyclic_targets)]),
      revisit_targets = unique(revisit_targets[nzchar(revisit_targets)])
    )
  }

  build_reverse_dependents <- function(plan_items, tracked_targets) {
    tracked_targets <- unique(toupper(as.character(tracked_targets %||% character())))
    tracked_targets <- tracked_targets[nzchar(tracked_targets)]
    reverse_dependents <- stats::setNames(rep(list(character()), length(tracked_targets)), tracked_targets)
    if (!length(tracked_targets)) {
      return(reverse_dependents)
    }
    for (plan_item in plan_items) {
      for (statement in plan_item$statements %||% list()) {
        target <- toupper(as.character(statement$name %||% statement$target %||% ""))
        if (!nzchar(target) || !(target %in% tracked_targets)) {
          next
        }
        compiled <- statement$compiled %||% compile_expression(statement$expression %||% "")
        refs <- compiled$references %||% NULL
        deps <- if (!is.null(refs) && nrow(refs) > 0L) {
          unique(toupper(as.character(refs$name[refs$lag == 0L])))
        } else {
          character()
        }
        deps <- deps[deps %in% tracked_targets]
        deps <- deps[deps != target]
        if (!length(deps)) {
          next
        }
        for (dep in deps) {
          reverse_dependents[[dep]] <- unique(c(reverse_dependents[[dep]], target))
        }
      }
    }
    reverse_dependents
  }

  working <- sort_frame_by_period(frame)
  preserve_mask <- if (isTRUE(preserve_existing)) build_frame_finite_mask(working) else NULL
  state <- state_from_frame(working)
  state$coef_values <- coef_values %||% state$coef_values %||% list()
  replay_plan <- build_replay_plan(statements)
  if (!length(replay_plan$plan)) {
    return(working)
  }

  cyclic_targets <- replay_plan$cyclic_targets
  revisit_targets <- replay_plan$revisit_targets
  reverse_dependents <- build_reverse_dependents(replay_plan$plan, revisit_targets)
  max_passes <- if (length(cyclic_targets)) {
    max(2L, min(length(cyclic_targets), 8L))
  } else {
    1L
  }
  for (pass in seq_len(max_passes)) {
    active_targets <- if (pass > 1L) unique(cyclic_targets) else character()
    changed_targets <- character()
    active_window <- NULL
    applied_target_counts <- integer()
    for (plan_item in replay_plan$plan) {
      if (identical(plan_item$type, "smpl")) {
        parsed_window <- parse_smpl_statement(plan_item$raw %||% "")
        if (!is.null(parsed_window)) {
          active_window <- c(parsed_window$start, parsed_window$end)
          expanded <- ensure_frame_periods(working, seq_periods(active_window[[1]], active_window[[2]]))
          expanded <- sort_frame_by_period(expanded)
          if (!identical(as.character(expanded$period), as.character(working$period))) {
            if (isTRUE(preserve_existing)) {
              preserve_mask <- align_frame_finite_mask(preserve_mask, expanded)
            }
            working <- expanded
            state <- state_from_frame(working)
            state$coef_values <- coef_values %||% state$coef_values %||% list()
          } else {
            working <- expanded
          }
        }
        next
      }
      if (identical(plan_item$type, "changevar")) {
        working <- apply_inline_changevar_payload_frame(
          working,
          plan_item$raw %||% "",
          active_window
        )
        working <- sort_frame_by_period(working)
        if (isTRUE(preserve_existing)) {
          preserve_mask <- build_frame_finite_mask(working)
        }
        state <- state_from_frame(working)
        state$coef_values <- coef_values %||% state$coef_values %||% list()
        next
      }
      for (statement in plan_item$statements %||% list()) {
        target <- toupper(as.character(statement$name %||% ""))
        if (pass > 1L && length(active_targets) && !(target %in% active_targets)) {
          next
        }
        prior_target_applications <- if (nzchar(target) && target %in% names(applied_target_counts)) {
          as.integer(unname(applied_target_counts[target]))
        } else {
          0L
        }
        effective_preserve_existing <- isTRUE(preserve_existing) && (
          prior_target_applications <= 0L ||
            runtime_assignment_references_target(statement, target)
        )
        effective_preserve_mask <- if (isTRUE(effective_preserve_existing)) {
          current_mask <- preserve_mask %||% list()
          if (prior_target_applications > 0L && nzchar(target)) {
            target_values <- as.numeric(state$series[[target]] %||% working[[target]] %||% rep(NA_real_, nrow(working)))
            target_periods <- as.character(working$period %||% character())
            target_current_mask <- as.logical(is.finite(target_values) & abs(target_values + 99.0) > 1e-12)
            names(target_current_mask) <- target_periods
            current_mask[[target]] <- target_current_mask
          }
          current_mask
        } else {
          NULL
        }
        started <- proc.time()[["elapsed"]]
        applied <- apply_runtime_assignment_state_frame(
          working,
          statement,
          active_window = active_window,
          state = state,
          coef_values = coef_values,
          preserve_existing = effective_preserve_existing,
          preserve_mask = effective_preserve_mask,
          preserve_mode = if (!is.null(preserve_modes_by_target) && target %in% names(preserve_modes_by_target)) {
            as.character(preserve_modes_by_target[[target]])
          } else {
            preserve_mode
          }
        )
        elapsed_sec <- as.numeric(proc.time()[["elapsed"]] - started)
        if (nzchar(target)) {
          applied_target_counts[target] <- prior_target_applications + 1L
        }
        if (nzchar(replay_profile_path)) {
          current_preserve_mode <- if (!is.null(preserve_modes_by_target) && target %in% names(preserve_modes_by_target)) {
            as.character(preserve_modes_by_target[[target]])
          } else {
            preserve_mode
          }
          self_referential <- runtime_assignment_references_target(statement, target, compiled = statement$compiled %||% NULL)
          window_positions <- runtime_assignment_positions(working, active_window)
          replay_profile_rows[[length(replay_profile_rows) + 1L]] <- data.frame(
            pass = as.integer(pass),
            target = as.character(target),
            command = as.character(statement_command_runtime(statement)),
            preserve_mode = as.character(current_preserve_mode),
            self_referential = as.logical(self_referential),
            active_window_start = as.character(active_window[[1]] %||% ""),
            active_window_end = as.character(active_window[[2]] %||% ""),
            window_count = as.integer(length(window_positions)),
            elapsed_sec = as.numeric(elapsed_sec),
            changed = as.logical(applied$changed),
            stringsAsFactors = FALSE
          )
        }
        working <- applied$frame
        state <- applied$state
        if (pass > 1L && isTRUE(applied$changed) && target %in% revisit_targets) {
          changed_targets <- unique(c(changed_targets, target))
          active_targets <- unique(c(active_targets, reverse_dependents[[target]] %||% character()))
        }
      }
    }
    if (max_passes > 1L && pass > 1L && !length(changed_targets)) {
      break
    }
  }
  if (nzchar(replay_profile_path) && length(replay_profile_rows)) {
    replay_profile <- do.call(rbind, replay_profile_rows)
    utils::write.csv(replay_profile, replay_profile_path, row.names = FALSE)
  }

  working
}

infer_replay_preserve_modes <- function(statements, frame, assignment_targets = character(), coef_values = NULL, active_window = NULL) {
  targets <- unique(toupper(as.character(assignment_targets %||% character())))
  targets <- targets[nzchar(targets)]
  if (!length(targets) || !length(statements) || !nrow(frame)) {
    return(stats::setNames(character(), character()))
  }

  materially_differs_for_fallback <- function(candidate_value, protected_value) {
    candidate_value <- as.numeric(candidate_value)
    protected_value <- as.numeric(protected_value)
    if (!is.finite(candidate_value) || !is.finite(protected_value)) {
      return(FALSE)
    }
    max_abs_value <- max(abs(candidate_value), abs(protected_value))
    if (max_abs_value <= 5e-3) {
      return(FALSE)
    }
    abs(candidate_value - protected_value) > 1e-10
  }

  working <- sort_frame_by_period(frame)
  target_set <- unique(toupper(targets))
  fallback_targets <- character()
  active_smpl <- active_window
  audit_entries <- stats::setNames(rep(list(NULL), length(target_set)), target_set)

  eval_positions_for_target <- function(target_values) {
    finite_positions <- which(is.finite(target_values))
    if (!length(finite_positions)) {
      return(integer())
    }
    unique(as.integer(round(stats::quantile(
      finite_positions,
      probs = c(0, 0.5, 1),
      names = FALSE,
      type = 1
    ))))
  }

  for (statement in statements) {
    command <- statement_command_runtime(statement)
    raw <- statement$raw %||% ""
    if (identical(command, "SMPL")) {
      parsed_window <- parse_smpl_statement(raw)
      if (!is.null(parsed_window)) {
        active_smpl <- c(parsed_window$start, parsed_window$end)
      }
      next
    }
    if (!is_runtime_assignment_statement(statement)) {
      next
    }
    target <- toupper(as.character(statement$name %||% statement$target %||% ""))
    if (!(target %in% target_set) || target %in% fallback_targets) {
      next
    }
    if (is.null(audit_entries[[target]])) {
      audit_entries[[target]] <- list(
        target = target,
        mode = "skip",
        trigger_reason = "",
        trigger_period = "",
        protected_value = NA_real_,
        candidate_value = NA_real_,
        active_window_start = as.character(active_smpl[[1]] %||% ""),
        active_window_end = as.character(active_smpl[[2]] %||% ""),
        source_path = as.character(statement$source_path %||% ""),
        source_order = as.integer(statement$source_order %||% 0L),
        command = statement_command_runtime(statement),
        raw = as.character(statement$raw %||% "")
      )
    }
    resolved_target <- resolve_frame_column_name(working, target)
    if (!(resolved_target %in% names(working))) {
      next
    }
    protected_values <- as.numeric(working[[resolved_target]])
    if (!any(is.finite(protected_values))) {
      next
    }
    positions <- eval_positions_for_target(protected_values)
    if (!length(positions)) {
      next
    }
    state <- state_from_frame(working)
    state$coef_values <- coef_values %||% state$coef_values %||% list()
    compiled <- statement$compiled %||% compile_expression(statement$expression %||% "")
    for (period_pos in positions) {
      period_label <- as.character(working$period[[period_pos]])
      if (!is.null(active_smpl) && length(active_smpl) >= 2L) {
        if (!(period_label %in% seq_periods(active_smpl[[1]], active_smpl[[2]]))) {
          next
        }
      }
      protected_value <- protected_values[[period_pos]]
      candidate_value <- as.numeric(evaluate_compiled_expression(
        compiled,
        state,
        period_pos,
        strict = FALSE
      ))
      if (!is.finite(candidate_value) && is.finite(protected_value)) {
        fallback_targets <- c(fallback_targets, target)
        audit_entries[[target]]$mode <- "fallback"
        audit_entries[[target]]$trigger_reason <- "nonfinite_candidate"
        audit_entries[[target]]$trigger_period <- period_label
        audit_entries[[target]]$protected_value <- as.numeric(protected_value)
        audit_entries[[target]]$candidate_value <- as.numeric(candidate_value)
        break
      }
      if (materially_differs_for_fallback(candidate_value, protected_value)) {
        fallback_targets <- c(fallback_targets, target)
        audit_entries[[target]]$mode <- "fallback"
        audit_entries[[target]]$trigger_reason <- "material_difference"
        audit_entries[[target]]$trigger_period <- period_label
        audit_entries[[target]]$protected_value <- as.numeric(protected_value)
        audit_entries[[target]]$candidate_value <- as.numeric(candidate_value)
        break
      }
    }
  }

  modes <- rep("skip", length(target_set))
  names(modes) <- target_set
  if (length(fallback_targets)) {
    modes[unique(fallback_targets)] <- "fallback"
  }
  audit_rows <- lapply(target_set, function(target) {
    entry <- audit_entries[[target]] %||% list(
      target = target,
      mode = as.character(modes[[target]] %||% "skip"),
      trigger_reason = "",
      trigger_period = "",
      protected_value = NA_real_,
      candidate_value = NA_real_,
      active_window_start = "",
      active_window_end = "",
      source_path = "",
      source_order = 0L,
      command = "",
      raw = ""
    )
    data.frame(
      target = as.character(entry$target %||% target),
      mode = as.character(entry$mode %||% as.character(modes[[target]] %||% "skip")),
      trigger_reason = as.character(entry$trigger_reason %||% ""),
      trigger_period = as.character(entry$trigger_period %||% ""),
      protected_value = as.numeric(entry$protected_value %||% NA_real_),
      candidate_value = as.numeric(entry$candidate_value %||% NA_real_),
      active_window_start = as.character(entry$active_window_start %||% ""),
      active_window_end = as.character(entry$active_window_end %||% ""),
      source_path = as.character(entry$source_path %||% ""),
      source_order = as.integer(entry$source_order %||% 0L),
      command = as.character(entry$command %||% ""),
      raw = as.character(entry$raw %||% ""),
      stringsAsFactors = FALSE
    )
  })
  attr(modes, "audit") <- do.call(rbind, audit_rows)
  modes
}

standard_presolve_replay_context <- function(statements, frame, termination_index = 0L, solve_snapshot = NULL, exogenous_targets = character(), coef_values = NULL) {
  spec_limit <- if (as.integer(termination_index %||% 0L) > 0L) {
    as.integer(termination_index) - 1L
  } else {
    length(statements)
  }
  active_exogenous_targets <- unique(toupper(as.character(exogenous_targets %||% character())))
  if (!is.null(solve_snapshot)) {
    spec_limit <- min(spec_limit, max(0L, as.integer(solve_snapshot$solve_index %||% 0L) - 1L))
    active_exogenous_targets <- unique(c(
      active_exogenous_targets,
      toupper(as.character(solve_snapshot$exogenous_targets %||% character()))
    ))
  }
  if (spec_limit <= 0L) {
    return(list(
      spec_limit = as.integer(spec_limit),
      statements = list(),
      assignment_targets = character(),
      preserve_modes = stats::setNames(character(), character()),
      preserve_mode_audit = data.frame(
        target = character(),
        mode = character(),
        trigger_reason = character(),
        trigger_period = character(),
        protected_value = numeric(),
        candidate_value = numeric(),
        active_window_start = character(),
        active_window_end = character(),
        source_path = character(),
        source_order = integer(),
        command = character(),
        raw = character(),
        stringsAsFactors = FALSE
      ),
      replay_plan_rows = data.frame(
        replay_order = integer(),
        plan_type = character(),
        command = character(),
        target = character(),
        active_window_start = character(),
        active_window_end = character(),
        preserve_mode = character(),
        changevar_applied = logical(),
        source_path = character(),
        source_order = integer(),
        cycle_revisit = logical(),
        raw = character(),
        stringsAsFactors = FALSE
      ),
      replay_plan_meta = list(cyclic_targets = character(), revisit_targets = character())
    ))
  }

  pre_solve_statements <- statements[seq_len(spec_limit)]
  pre_solve_assignment_targets <- unique(vapply(
    Filter(
      function(item) {
        kind <- tolower(as.character(item$kind %||% ""))
        kind %in% c("create", "genr", "ident")
      },
      pre_solve_statements
    ),
    function(item) as.character(item$name %||% item$target %||% ""),
    character(1)
  ))
  preserve_modes <- infer_replay_preserve_modes(
    pre_solve_statements,
    frame,
    assignment_targets = pre_solve_assignment_targets,
    coef_values = coef_values
  )
  replay_plan <- build_runtime_replay_plan(
    pre_solve_statements,
    assignment_targets = pre_solve_assignment_targets,
    replay_inline_changevar = FALSE
  )
  list(
    spec_limit = as.integer(spec_limit),
    statements = pre_solve_statements,
    assignment_targets = pre_solve_assignment_targets,
    preserve_modes = preserve_modes,
    preserve_mode_audit = attr(preserve_modes, "audit") %||% data.frame(),
    replay_plan_rows = build_runtime_replay_plan_rows(replay_plan, preserve_modes_by_target = preserve_modes),
    replay_plan_meta = list(
      cyclic_targets = replay_plan$cyclic_targets %||% character(),
      revisit_targets = replay_plan$revisit_targets %||% character()
    )
  )
}

build_standard_solve_bundle <- function(sources, frame, history_statements, solve_index = 0L, active_window = NULL, setupsolve = list(), exogenous_targets = character(), solve_metadata = list()) {
  semantics_profile <- resolve_standard_input_semantics_profile(
    solve_metadata$semantics_profile %||% sources$semantics_profile %||% NULL
  )
  stage_progress_path <- as.character(solve_metadata$stage_build_progress_path %||% "")
  stage_progress_index <- as.integer(solve_metadata$solve_stage_index %||% 0L)
  build_started <- proc.time()[["elapsed"]]
  append_solve_stage_build_progress_row(stage_progress_path, stage_progress_index, "bundle_enter", elapsed_sec = 0)
  runtime_input_targets <- collect_runtime_input_targets(
    history_statements,
    search_dirs = sources$search_dirs %||% character()
  )
  append_solve_stage_build_progress_row(
    stage_progress_path,
    stage_progress_index,
    "runtime_input_targets_ready",
    elapsed_sec = as.numeric(proc.time()[["elapsed"]] - build_started),
    row_count = length(runtime_input_targets)
  )
  protected_input_targets <- unique(c(
    toupper(as.character(exogenous_targets %||% character())),
    runtime_input_targets
  ))
  eq_support <- build_reduced_eq_specs(
    history_statements,
    fmout_path = sources$fmout,
    setupsolve = setupsolve
  )
  append_solve_stage_build_progress_row(
    stage_progress_path,
    stage_progress_index,
    "eq_support_ready",
    elapsed_sec = as.numeric(proc.time()[["elapsed"]] - build_started),
    row_count = length(eq_support$specs %||% list())
  )
  candidate_specs <- Filter(
    function(item) item$kind != "control" && !is.null(item$expression),
    history_statements
  )
  spec_partition <- partition_standard_solve_specs(
    eq_specs = eq_support$specs,
    candidate_specs = candidate_specs,
    exogenous_targets = protected_input_targets,
    exogenous_equation_target_policy = semantics_profile$exogenous_equation_target_policy
  )
  append_solve_stage_build_progress_row(
    stage_progress_path,
    stage_progress_index,
    "spec_partition_ready",
    elapsed_sec = as.numeric(proc.time()[["elapsed"]] - build_started),
    row_count = length(spec_partition$specs %||% list())
  )
  specs <- spec_partition$specs
  post_solve_assignments <- spec_partition$post_solve_assignments
  pre_solve_assignment_targets <- unique(vapply(
    Filter(
      function(item) {
        kind <- tolower(as.character(item$kind %||% ""))
        kind %in% c("create", "genr", "ident")
      },
      candidate_specs
    ),
    function(item) as.character(item$name %||% item$target %||% ""),
    character(1)
  ))
  preserve_modes <- infer_replay_preserve_modes(
    history_statements,
    frame,
    assignment_targets = pre_solve_assignment_targets,
    coef_values = eq_support$coef_values %||% list(),
    active_window = active_window
  )
  append_solve_stage_build_progress_row(
    stage_progress_path,
    stage_progress_index,
    "preserve_modes_ready",
    elapsed_sec = as.numeric(proc.time()[["elapsed"]] - build_started),
    row_count = length(preserve_modes)
  )
  replay_statements <- resolve_simple_cust_compat_replay_statements(
    history_statements,
    fmout_path = sources$fmout,
    assignment_targets = pre_solve_assignment_targets,
    preserve_modes_by_target = preserve_modes,
    semantics_profile = semantics_profile$name
  )
  replay_plan <- build_runtime_replay_plan(
    replay_statements,
    assignment_targets = pre_solve_assignment_targets,
    replay_inline_changevar = FALSE
  )
  append_solve_stage_build_progress_row(
    stage_progress_path,
    stage_progress_index,
    "replay_plan_ready",
    elapsed_sec = as.numeric(proc.time()[["elapsed"]] - build_started),
    row_count = length(replay_plan$ordered_statements %||% list())
  )
  replay_plan_rows <- build_runtime_replay_plan_rows(
    replay_plan,
    preserve_modes_by_target = preserve_modes
  )
  pre_solve_frame <- replay_selected_runtime_assignments(
    replay_statements,
    frame,
    assignment_targets = pre_solve_assignment_targets,
    coef_values = eq_support$coef_values %||% list(),
    preserve_existing = TRUE,
    preserve_mode = "skip",
    preserve_modes_by_target = preserve_modes,
    replay_inline_changevar = FALSE,
    replay_profile_path = as.character(solve_metadata$replay_profile_path %||% "")
  )
  pre_solve_frame <- restore_historical_boundary_replay_targets(
    frame = frame,
    replayed_frame = pre_solve_frame,
    replay_plan_rows = replay_plan_rows,
    sample_start = active_window[[1]] %||% "",
    protected_targets = protected_input_targets
  )
  append_solve_stage_build_progress_row(
    stage_progress_path,
    stage_progress_index,
    "pre_solve_replay_ready",
    elapsed_sec = as.numeric(proc.time()[["elapsed"]] - build_started)
  )
  snapshot_vars <- character()
  snapshot_rows <- data.frame(
    phase = character(),
    period = character(),
    variable = character(),
    value = numeric(),
    stringsAsFactors = FALSE
  )
  solve_input_trace_rows <- data.frame(
    phase = character(),
    period = character(),
    variable = character(),
    value = numeric(),
    stringsAsFactors = FALSE
  )
  trace_variables <- resolve_live_frontier_trace_variables(
    watch_variables = solve_metadata$watch_variables %||% character(),
    spec_targets = vapply(specs %||% list(), function(item) as.character(item$target %||% item$name %||% ""), character(1))
  )
  if (!is.null(active_window) && length(active_window) >= 2L) {
    solve_input_trace_rows <- rbind(
      solve_input_trace_rows,
      build_frame_snapshot_rows(frame, trace_variables, active_window[[1]], "incoming_bundle_state"),
      build_frame_snapshot_rows(pre_solve_frame, trace_variables, active_window[[1]], "post_replay")
    )
  }
  if (!is.null(active_window) &&
      length(active_window) >= 2L &&
      isTRUE(solve_metadata$options$outside) &&
      isTRUE(solve_metadata$options$noreset)) {
    equation_targets <- unique(toupper(vapply(eq_support$specs %||% list(), function(item) {
      as.character(item$target %||% item$name %||% "")
    }, character(1))))
    equation_targets <- equation_targets[nzchar(equation_targets)]
    equation_support_refs <- unique(unlist(lapply(eq_support$specs %||% list(), function(item) {
      refs <- tokens_to_reference_frame(item$rhs_tokens %||% character())
      if (!is.data.frame(refs) || !nrow(refs)) {
        return(character())
      }
      unique(toupper(as.character(refs$name[as.integer(refs$lag) == 0L])))
    })))
    equation_support_refs <- equation_support_refs[nzchar(equation_support_refs)]
    outside_carry_plan <- build_outside_carry_plan(
      candidate_specs,
      statements = history_statements,
      sample_start = active_window[[1]],
      protected_targets = protected_input_targets,
      equation_targets = equation_targets,
      equation_support_refs = equation_support_refs
    )
    append_solve_stage_build_progress_row(
      stage_progress_path,
      stage_progress_index,
      "outside_carry_plan_ready",
      elapsed_sec = as.numeric(proc.time()[["elapsed"]] - build_started),
      row_count = length(outside_carry_plan$boundary_targets %||% character())
    )
    snapshot_vars <- resolve_outside_snapshot_variables(
      outside_carry_plan,
      watch_variables = solve_metadata$watch_variables %||% character(),
      spec_targets = vapply(specs %||% list(), function(item) as.character(item$target %||% item$name %||% ""), character(1))
    )
    snapshot_rows <- build_frame_snapshot_rows(
      pre_solve_frame,
      snapshot_vars,
      active_window[[1]],
      "pre_carry"
    )
    if (isTRUE(semantics_profile$apply_outside_boundary_carry)) {
      pre_solve_frame <- apply_outside_boundary_carry_frame(
        pre_solve_frame,
        sample_start = active_window[[1]],
        targets = outside_carry_plan$boundary_targets
      )
      snapshot_rows <- rbind(
        snapshot_rows,
        build_frame_snapshot_rows(
          pre_solve_frame,
          snapshot_vars,
          active_window[[1]],
          "post_boundary_carry"
        )
      )
      solve_input_trace_rows <- rbind(
        solve_input_trace_rows,
        build_frame_snapshot_rows(
          pre_solve_frame,
          trace_variables,
          active_window[[1]],
          "post_boundary_carry"
        )
      )
    }
    if (isTRUE(semantics_profile$apply_outside_first_period_carry)) {
      first_period_carry_targets <- resolve_outside_first_period_carry_targets(
        outside_carry_plan,
        semantics_profile,
        protected_targets = protected_input_targets
      )
      pre_solve_frame <- apply_outside_first_period_carry_frame(
        pre_solve_frame,
        sample_start = active_window[[1]],
        targets = first_period_carry_targets
      )
      solve_input_trace_rows <- rbind(
        solve_input_trace_rows,
        build_frame_snapshot_rows(
          pre_solve_frame,
          trace_variables,
          active_window[[1]],
          "post_first_period_carry"
        )
      )
    }
    snapshot_rows <- rbind(
      snapshot_rows,
      build_frame_snapshot_rows(
        pre_solve_frame,
        snapshot_vars,
        active_window[[1]],
        "final_pre_solve"
      )
    )
    solve_input_trace_rows <- rbind(
      solve_input_trace_rows,
      build_frame_snapshot_rows(
        pre_solve_frame,
        trace_variables,
        active_window[[1]],
        "final_pre_solve"
      )
    )
  } else {
    outside_carry_plan <- list(
      boundary_targets = character(),
      first_period_targets = character(),
      target_roles = data.frame(
        target = character(),
        boundary_role = character(),
        first_period_role = character(),
        boundary_materialized = logical(),
        direct_negative_lag = logical(),
        stringsAsFactors = FALSE,
        check.names = FALSE
      )
    )
  }

  state <- state_from_frame(pre_solve_frame)
  state$coef_values <- eq_support$coef_values %||% numeric()
  if (!length(state$periods) && !is.null(active_window) && length(active_window) >= 2L) {
    state$periods <- seq_periods(active_window[[1]], active_window[[2]])
  }
  equation_first_eval_rows <- data.frame(
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
  )
  first_eval_targets <- resolve_standard_input_first_eval_targets()
  if (length(first_eval_targets) && !is.null(active_window) && length(active_window) >= 2L) {
    eval_period_pos <- match(as.character(active_window[[1]]), state$periods)
    if (!is.na(eval_period_pos)) {
      normalized_specs <- normalize_specs(specs %||% list())
      first_eval_targets <- intersect(first_eval_targets, vapply(normalized_specs, `[[`, character(1), "target"))
      if (length(first_eval_targets)) {
        first_eval_rows <- lapply(first_eval_targets, function(target) {
          spec <- normalized_specs[[which(vapply(normalized_specs, function(item) identical(item$target, target), logical(1)))[1L]]]
          previous <- as.numeric(state$series[[target]][[eval_period_pos]])
          evaluation <- suppressWarnings(evaluate_spec_at_period(
            spec,
            state,
            eval_period_pos,
            strict = FALSE,
            resid_state = NULL
          ))
          rbind(
            build_spec_reference_trace_rows(spec, state, eval_period_pos, 0L),
            build_spec_active_fsr_trace_rows(spec, state, eval_period_pos, 0L),
            build_spec_result_trace_rows(spec, state, eval_period_pos, 0L, previous, evaluation)
          )
        })
        first_eval_rows <- Filter(function(item) is.data.frame(item) && nrow(item), first_eval_rows)
        if (length(first_eval_rows)) {
          equation_first_eval_rows <- do.call(rbind, first_eval_rows)
        }
      }
    }
  }

  control <- setupsolve %||% list()
  if (!is.null(active_window) && length(active_window) >= 2L) {
    control$sample_start <- active_window[[1]]
    control$sample_end <- active_window[[2]]
  }
  control <- apply_semantics_profile_to_solve_control(control, semantics_profile)
  control$order <- vapply(specs, function(item) as.character(item$target %||% item$name %||% ""), character(1))
  control$order <- control$order[nzchar(control$order)]
  control$outside_carry_plan <- outside_carry_plan
  append_solve_stage_build_progress_row(
    stage_progress_path,
    stage_progress_index,
    "bundle_ready",
    elapsed_sec = as.numeric(proc.time()[["elapsed"]] - build_started),
    row_count = length(specs %||% list())
  )

  list(
    name = tools::file_path_sans_ext(basename(sources$entry_path)),
    source = list(
      entry_input = sources$entry_path,
      fmdata = sources$fmdata,
      fmexog = sources$fmexog,
      fmout = sources$fmout,
      files_scanned = sources$tree$files_scanned,
      runtime_input_targets = runtime_input_targets
    ),
    runtime = list(
      statements = sources$tree$statements,
      solve_index = as.integer(solve_index),
      solve_window_start = active_window[[1]] %||% "",
      solve_window_end = active_window[[2]] %||% "",
      semantics_profile = semantics_profile$name,
      solver_policy = semantics_profile$solver_policy,
      solve_options = solve_metadata$options %||% list(),
      watch_variables = solve_metadata$watch_variables %||% character(),
      solve_option_text = solve_metadata$option_text %||% "",
      solve_watch_text = solve_metadata$watch_text %||% "",
      presolve_replay_plan_rows = replay_plan_rows,
      presolve_replay_plan_meta = list(
        cyclic_targets = replay_plan$cyclic_targets %||% character(),
        revisit_targets = replay_plan$revisit_targets %||% character()
      ),
      preserve_mode_audit = attr(preserve_modes, "audit") %||% data.frame(),
      outside_carry_plan = outside_carry_plan,
      outside_snapshot_variables = snapshot_vars,
      outside_carry_snapshots = unique(snapshot_rows),
      equation_first_eval_rows = unique(equation_first_eval_rows),
      solve_input_trace_rows = unique(solve_input_trace_rows)
    ),
    equations = eq_support,
    state = state,
    specs = specs,
    post_solve_assignments = post_solve_assignments,
    control = control,
    semantics_profile = semantics_profile$name,
    solver_policy = semantics_profile$solver_policy,
    input_text = paste(vapply(history_statements, function(item) paste0(item$raw, ";"), character(1)), collapse = "\n")
  )
}

empty_standard_diagnostics <- function() {
  data.frame(
    solve_stage = integer(),
    period = character(),
    iterations = integer(),
    converged = logical(),
    max_delta = numeric(),
    termination = character(),
    elapsed_sec = numeric(),
    spec_count = integer(),
    sample_start = character(),
    sample_end = character(),
    stringsAsFactors = FALSE
  )
}

append_solve_stage_progress_row <- function(work_dir, solve_stage, event, sample_start = "", sample_end = "", elapsed_sec = NA_real_, spec_count = NA_integer_) {
  if (is.null(work_dir) || !nzchar(work_dir)) {
    return(invisible(NULL))
  }
  progress_path <- file.path(work_dir, "SOLVE_STAGE_PROGRESS.csv")
  row <- data.frame(
    solve_stage = as.integer(solve_stage),
    event = as.character(event),
    sample_start = as.character(sample_start %||% ""),
    sample_end = as.character(sample_end %||% ""),
    elapsed_sec = as.numeric(elapsed_sec),
    spec_count = as.integer(spec_count),
    recorded_at = as.character(Sys.time()),
    stringsAsFactors = FALSE
  )
  utils::write.table(
    row,
    file = progress_path,
    sep = ",",
    row.names = FALSE,
    col.names = !file.exists(progress_path),
    append = file.exists(progress_path),
    quote = TRUE
  )
  invisible(progress_path)
}

append_solve_stage_build_progress_row <- function(progress_path, solve_stage, event, elapsed_sec = NA_real_, row_count = NA_integer_) {
  if (is.null(progress_path) || !nzchar(progress_path)) {
    return(invisible(NULL))
  }
  row <- data.frame(
    solve_stage = as.integer(solve_stage),
    event = as.character(event),
    elapsed_sec = as.numeric(elapsed_sec),
    row_count = as.integer(row_count),
    recorded_at = as.character(Sys.time()),
    stringsAsFactors = FALSE
  )
  utils::write.table(
    row,
    file = progress_path,
    sep = ",",
    row.names = FALSE,
    col.names = !file.exists(progress_path),
    append = file.exists(progress_path),
    quote = TRUE
  )
  invisible(progress_path)
}

emit_runtime_printvar <- function(frame, statement, active_window, work_dir, source_info = NULL, search_dirs = NULL) {
  if (is.null(work_dir) || !nzchar(work_dir)) {
    return(NULL)
  }
  parsed_printvar <- parse_printvar_statement(statement$raw %||% "")
  if (is.null(parsed_printvar)) {
    return(NULL)
  }
  output_path <- if (nzchar(parsed_printvar$fileout %||% "")) {
    resolve_runtime_output_path(parsed_printvar$fileout, work_dir)
  } else if (isTRUE(parsed_printvar$stats)) {
    resolve_generated_output_path("PRINTVAR_STATS.csv", work_dir)
  } else if (!isTRUE(parsed_printvar$loadformat)) {
    resolve_generated_output_path("PRINTVAR_TABLE.csv", work_dir)
  } else {
    return(NULL)
  }
  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  if (isTRUE(parsed_printvar$stats)) {
    write_printvar_stats(
      frame,
      output_path = output_path,
      variables = parsed_printvar$variables,
      active_window = active_window
    )
  } else if (isTRUE(parsed_printvar$loadformat)) {
    write_printvar_loadformat(
      frame,
      output_path = output_path,
      variables = parsed_printvar$variables,
      active_window = active_window,
      fmout_path = source_info$fmout %||% NULL,
      fallback_paths = resolve_printvar_fallback_paths(source_info, search_dirs = search_dirs)
    )
  } else {
    write_printvar_table(
      frame,
      output_path = output_path,
      variables = parsed_printvar$variables,
      active_window = active_window
    )
  }
  output_path
}

run_standard_input_deck <- function(entry_input, fmdata_path = NULL, fmexog_path = NULL, fmout_path = NULL, search_dirs = NULL, work_dir = NULL) {
  semantics_profile <- resolve_standard_input_semantics_profile()
  sources <- resolve_standard_input_sources(
    entry_input,
    fmdata_path = fmdata_path,
    fmexog_path = fmexog_path,
    fmout_path = fmout_path,
    search_dirs = search_dirs
  )
  sources$tree <- apply_base_helper_overlay_to_tree(
    sources$entry_path,
    sources$tree,
    search_dirs = sources$search_dirs,
    semantics_profile = semantics_profile$name
  )
  statements <- sources$tree$statements
  runtime_coef_values <- build_reduced_eq_specs(
    statements,
    fmout_path = sources$fmout,
    setupsolve = list()
  )$coef_values %||% list()
  working <- base_standard_input_frame(sources$fmdata)
  runtime_preview <- prepare_standard_runtime(
    statements,
    frame = working,
    search_dirs = sources$search_dirs,
    default_fmexog_path = sources$fmexog
  )
  presolve_replay <- standard_presolve_replay_context(
    statements,
    runtime_preview$frame,
    termination_index = runtime_preview$termination_index,
    solve_snapshot = runtime_preview$solve_snapshot,
    exogenous_targets = runtime_preview$exogenous_targets %||% character(),
    coef_values = runtime_coef_values
  )
  historical_preserve_targets <- character()
  if (!is.null(runtime_preview$solve_snapshot)) {
    history_statements <- statements[seq_len(max(0L, as.integer(runtime_preview$solve_snapshot$solve_index %||% 0L) - 1L))]
    if (length(history_statements)) {
      solve_eq_support <- build_reduced_eq_specs(
        history_statements,
        fmout_path = sources$fmout,
        setupsolve = runtime_preview$solve_snapshot$setupsolve %||% list()
      )
      solve_candidate_specs <- Filter(
        function(item) item$kind != "control" && !is.null(item$expression),
        history_statements
      )
      solve_partition <- partition_standard_solve_specs(
        eq_specs = solve_eq_support$specs,
        candidate_specs = solve_candidate_specs,
        exogenous_targets = unique(c(
          toupper(as.character(runtime_preview$solve_snapshot$exogenous_targets %||% character())),
          collect_runtime_input_targets(history_statements, search_dirs = sources$search_dirs)
        )),
        exogenous_equation_target_policy = semantics_profile$exogenous_equation_target_policy
      )
      spec_targets <- unique(toupper(vapply(
        solve_partition$specs %||% list(),
        function(item) as.character(item$name %||% item$target %||% ""),
        character(1)
      )))
      setup_targets <- unique(toupper(vapply(
        solve_partition$setup_only_assignments %||% list(),
        function(item) as.character(item$name %||% item$target %||% ""),
        character(1)
      )))
      historical_preserve_targets <- setdiff(spec_targets[nzchar(spec_targets)], setup_targets[nzchar(setup_targets)])
    }
  }
  active_window <- NULL
  exogenous_targets <- character()
  setupsolve <- list()
  solve_results <- list()
  emitted_files <- character()
  resid_ar1_states <- list()
  test_results <- list()
  estimation_records <- list()
  termination_command <- ""
  has_runtime_input <- isTRUE(runtime_preview$saw_runtime_input)
  protected_frame <- working
  pre_solve_applied_target_counts <- integer()
  exogenous_path_trace <- data.frame(
    phase = character(),
    period = character(),
    variable = character(),
    value = numeric(),
    stringsAsFactors = FALSE
  )
  forecast_trace_start <- as.character(runtime_preview$solve_snapshot$sample_start %||% "")

  if (!has_runtime_input && !is.null(sources$fmexog) && nzchar(sources$fmexog) && file.exists(sources$fmexog)) {
    working <- apply_fmexog_rows(working, parse_fmexog_file(sources$fmexog))
    working <- sort_frame_by_period(working)
    protected_frame <- apply_fmexog_rows(protected_frame, parse_fmexog_file(sources$fmexog))
    protected_frame <- sort_frame_by_period(protected_frame)
  }

  for (idx in seq_along(statements)) {
    statement <- statements[[idx]]
    raw <- statement$raw %||% ""
    command <- statement_command_runtime(statement)
    if (idx > 1L && identical(statement_command_runtime(statements[[idx - 1L]]), "CHANGEVAR")) {
      next
    }
    if (command %in% c("QUIT", "RETURN")) {
      termination_command <- command
      break
    }

    if (identical(command, "SMPL")) {
      parsed_window <- parse_smpl_statement(raw)
      if (!is.null(parsed_window)) {
        active_window <- c(parsed_window$start, parsed_window$end)
        working <- ensure_frame_periods(working, seq_periods(active_window[[1]], active_window[[2]]))
        if (!length(solve_results)) {
          protected_frame <- ensure_frame_periods(protected_frame, seq_periods(active_window[[1]], active_window[[2]]))
          protected_frame <- sort_frame_by_period(protected_frame)
        }
        if (nzchar(forecast_trace_start) && identical(as.character(active_window[[1]]), forecast_trace_start)) {
          exogenous_path_trace <- append_exogenous_path_trace_rows(
            exogenous_path_trace,
            working,
            active_window = active_window,
            exogenous_targets = exogenous_targets,
            phase = "forecast_window_entry"
          )
        }
      }
      next
    }

    if (identical(command, "LOADDATA")) {
      load_name <- extract_fp_file_arg(raw, key = "FILE")
      resolved_load <- resolve_fp_source_path(load_name, sources$search_dirs)
      if (!is.null(resolved_load)) {
        working <- merge_fm_numeric_frames(
          working,
          parse_fm_numeric_file(resolved_load, block_name = basename(resolved_load))
        )
        working <- sort_frame_by_period(working)
        if (!length(solve_results)) {
          protected_frame <- merge_fm_numeric_frames(
            protected_frame,
            parse_fm_numeric_file(resolved_load, block_name = basename(resolved_load))
          )
          protected_frame <- sort_frame_by_period(protected_frame)
        }
      }
      next
    }

    if (identical(command, "INPUT")) {
      input_name <- extract_fp_file_arg(raw, key = "FILE")
      resolved_input <- resolve_fp_source_path(input_name, sources$search_dirs)
      if (!is.null(resolved_input)) {
        working <- apply_fmexog_rows(working, parse_fmexog_file(resolved_input))
        working <- sort_frame_by_period(working)
        if (!length(solve_results)) {
          protected_frame <- apply_fmexog_rows(protected_frame, parse_fmexog_file(resolved_input))
          protected_frame <- sort_frame_by_period(protected_frame)
        }
        if (!length(solve_results) && nzchar(forecast_trace_start) && !is.null(active_window) &&
            identical(as.character(active_window[[1]]), forecast_trace_start)) {
          exogenous_path_trace <- append_exogenous_path_trace_rows(
            exogenous_path_trace,
            working,
            active_window = active_window,
            exogenous_targets = exogenous_targets,
            phase = sprintf("post_input_%s", basename(resolved_input))
          )
        }
      }
      next
    }

    if (identical(command, "CHANGEVAR")) {
      payload_raw <- inline_changevar_payload_raw(statements, idx)
      if (nzchar(payload_raw)) {
        working <- apply_inline_changevar_payload_frame(working, payload_raw, active_window)
        working <- sort_frame_by_period(working)
        if (!length(solve_results)) {
          protected_frame <- apply_inline_changevar_payload_frame(protected_frame, payload_raw, active_window)
          protected_frame <- sort_frame_by_period(protected_frame)
        }
        if (!length(solve_results) && nzchar(forecast_trace_start) && !is.null(active_window) &&
            identical(as.character(active_window[[1]]), forecast_trace_start)) {
          exogenous_path_trace <- append_exogenous_path_trace_rows(
            exogenous_path_trace,
            working,
            active_window = active_window,
            exogenous_targets = exogenous_targets,
            phase = "post_changevar"
          )
        }
      }
      next
    }

    if (identical(command, "EXOGENOUS")) {
      variable <- extract_fp_named_arg(raw, key = "VARIABLE")
      if (nzchar(variable %||% "")) {
        exogenous_targets <- unique(c(exogenous_targets, variable))
        if (!length(solve_results) && nzchar(forecast_trace_start) && !is.null(active_window) &&
            identical(as.character(active_window[[1]]), forecast_trace_start)) {
          exogenous_path_trace <- append_exogenous_path_trace_rows(
            exogenous_path_trace,
            working,
            active_window = active_window,
            exogenous_targets = exogenous_targets,
            phase = sprintf("post_exogenous_%s", toupper(as.character(variable)))
          )
        }
      }
      next
    }

    if (identical(command, "ENDOGENOUS")) {
      variable <- extract_fp_named_arg(raw, key = "VARIABLE")
      if (nzchar(variable %||% "")) {
        exogenous_targets <- exogenous_targets[toupper(exogenous_targets) != toupper(variable)]
      }
      next
    }

    if (identical(command, "EXTRAPOLATE")) {
      if (!is.null(active_window)) {
        working <- apply_extrapolate_frame(
          working,
          window_start = active_window[[1]],
          window_end = active_window[[2]],
          variables = exogenous_targets,
          include_all_columns = TRUE
        )
        if (!length(solve_results)) {
          protected_frame <- apply_extrapolate_frame(
            protected_frame,
            window_start = active_window[[1]],
            window_end = active_window[[2]],
            variables = exogenous_targets,
            include_all_columns = TRUE
          )
        }
        if (!length(solve_results) && nzchar(forecast_trace_start) &&
            identical(as.character(active_window[[1]]), forecast_trace_start)) {
          exogenous_path_trace <- append_exogenous_path_trace_rows(
            exogenous_path_trace,
            working,
            active_window = active_window,
            exogenous_targets = exogenous_targets,
            phase = "post_extrapolate"
          )
        }
      }
      next
    }

    if (identical(command, "SETUPSOLVE")) {
      setupsolve <- modifyList(setupsolve, parse_setupsolve_options(raw))
      next
    }

    if (is_runtime_assignment_statement(statement)) {
      target <- toupper(as.character(statement$name %||% statement$target %||% ""))
      command_kind <- statement_command_runtime(statement)
      prior_target_applications <- if (!length(solve_results) &&
        nzchar(target) &&
        target %in% names(pre_solve_applied_target_counts)) {
        as.integer(unname(pre_solve_applied_target_counts[[target]]))
      } else {
        0L
      }
      effective_preserve_existing <- !length(solve_results) && (
        prior_target_applications <= 0L ||
          runtime_assignment_references_target(statement, target)
      )
      effective_preserve_mask <- if (isTRUE(effective_preserve_existing)) {
        current_mask <- build_frame_finite_mask(protected_frame)
        if (prior_target_applications > 0L && nzchar(target)) {
          target_values <- as.numeric(working[[target]] %||% rep(NA_real_, nrow(working)))
          target_periods <- as.character(working$period %||% character())
          target_current_mask <- as.logical(is.finite(target_values) & abs(target_values + 99.0) > 1e-12)
          names(target_current_mask) <- target_periods
          current_mask[[target]] <- target_current_mask
        }
        current_mask
      } else {
        NULL
      }
      working <- apply_runtime_assignment_frame(
        working,
        statement,
        active_window = active_window,
        coef_values = runtime_coef_values,
        preserve_existing = effective_preserve_existing,
        preserve_mask = effective_preserve_mask,
        preserve_mode = if (!length(solve_results)) {
          if (target %in% names(presolve_replay$preserve_modes)) {
            as.character(presolve_replay$preserve_modes[[target]])
          } else {
            "skip"
          }
        } else {
          "skip"
        }
      )
      working <- sort_frame_by_period(working)
      if (!length(solve_results) && nzchar(target)) {
        pre_solve_applied_target_counts[[target]] <- prior_target_applications + 1L
      }
      if (!length(solve_results) && identical(command_kind, "CREATE")) {
        protected_frame <- update_pre_solve_protected_target(protected_frame, working, target)
      }
      next
    }

    if (identical(command, "SOLVE")) {
      if (nzchar(forecast_trace_start) && !is.null(active_window) &&
          identical(as.character(active_window[[1]]), forecast_trace_start)) {
        exogenous_path_trace <- append_exogenous_path_trace_rows(
          exogenous_path_trace,
          working,
          active_window = active_window,
          exogenous_targets = exogenous_targets,
          phase = sprintf("pre_solve_stage_%d", length(solve_results) + 1L)
        )
      }
      history_statements <- if (idx <= 1L) list() else statements[seq_len(idx - 1L)]
      following_statements <- if (idx < length(statements)) statements[seq.int(idx + 1L, length(statements))] else list()
      solve_metadata <- solve_statement_metadata(statement, following_statements)
      stage_index <- length(solve_results) + 1L
      stage_started <- proc.time()[["elapsed"]]
      append_solve_stage_progress_row(
        work_dir,
        solve_stage = stage_index,
        event = "stage_bundle_start",
        sample_start = active_window[[1]] %||% "",
        sample_end = active_window[[2]] %||% ""
      )
      stage_bundle <- build_standard_solve_bundle(
        sources,
        frame = working,
        history_statements = history_statements,
        solve_index = idx,
        active_window = active_window,
        setupsolve = setupsolve,
        exogenous_targets = exogenous_targets,
        solve_metadata = modifyList(solve_metadata, list(
          solve_stage_index = stage_index,
          stage_build_progress_path = if (!is.null(work_dir) && nzchar(work_dir)) {
            file.path(work_dir, "SOLVE_STAGE_BUILD_PROGRESS.csv")
          } else {
            ""
          },
          replay_profile_path = if (!is.null(work_dir) && nzchar(work_dir)) {
            file.path(work_dir, "REPLAY_PROFILE.csv")
          } else {
            ""
          }
        ))
      )
      append_solve_stage_progress_row(
        work_dir,
        solve_stage = stage_index,
        event = "stage_bundle_ready",
        sample_start = stage_bundle$control$sample_start %||% "",
        sample_end = stage_bundle$control$sample_end %||% "",
        elapsed_sec = as.numeric(proc.time()[["elapsed"]] - stage_started),
        spec_count = length(stage_bundle$specs %||% list())
      )
      solve_started <- proc.time()[["elapsed"]]
      stage_result <- mini_run(stage_bundle, control = list(
        resid_ar1_states = resid_ar1_states,
        solve_stage_index = stage_index,
        period_progress_path = if (!is.null(work_dir) && nzchar(work_dir)) {
          file.path(work_dir, "SOLVE_PERIOD_PROGRESS.csv")
        } else {
          ""
        },
        iteration_profile_path = if (!is.null(work_dir) && nzchar(work_dir)) {
          file.path(work_dir, "SOLVE_ITERATION_PROFILE.csv")
        } else {
          ""
        }
      ))
      append_solve_stage_progress_row(
        work_dir,
        solve_stage = stage_index,
        event = "stage_solve_complete",
        sample_start = stage_bundle$control$sample_start %||% "",
        sample_end = stage_bundle$control$sample_end %||% "",
        elapsed_sec = as.numeric(proc.time()[["elapsed"]] - solve_started),
        spec_count = length(stage_bundle$specs %||% list())
      )
      working <- sort_frame_by_period(stage_result$series)
      if (nzchar(forecast_trace_start) &&
          identical(as.character(stage_bundle$control$sample_start %||% ""), forecast_trace_start)) {
        exogenous_path_trace <- append_exogenous_path_trace_rows(
          exogenous_path_trace,
          working,
          active_window = c(stage_bundle$control$sample_start %||% "", stage_bundle$control$sample_end %||% ""),
          exogenous_targets = exogenous_targets,
          phase = sprintf("post_solve_stage_%d", stage_index)
        )
      }
      resid_ar1_states <- stage_result$resid_ar1_states %||% resid_ar1_states
      stage_diag <- stage_result$diagnostics
      stage_diag$solve_stage <- stage_index
      stage_diag$sample_start <- stage_bundle$control$sample_start %||% ""
      stage_diag$sample_end <- stage_bundle$control$sample_end %||% ""
      stage_diag <- stage_diag[, c("solve_stage", "period", "iterations", "converged", "max_delta", "termination", "elapsed_sec", "spec_count", "sample_start", "sample_end"), drop = FALSE]
      solve_results[[stage_index]] <- list(
        stage = stage_index,
        solve_index = idx,
        solve_metadata = solve_metadata,
        bundle = stage_bundle,
        result = stage_result,
        diagnostics = stage_diag
      )
      watch_path <- emit_solve_watch_output(
        stage_index = stage_index,
        frame = working,
        solve_metadata = solve_metadata,
        work_dir = work_dir,
        active_window = c(stage_bundle$control$sample_start %||% "", stage_bundle$control$sample_end %||% "")
      )
      if (!is.null(watch_path)) {
        emitted_files <- c(emitted_files, watch_path)
      }
      next
    }

    if (identical(command, "TEST")) {
      test_kind <- toupper(as.character(statement$body %||% ""))
      if (test_kind %in% c("IDENT", "LHS")) {
        test_occurrence <- sum(vapply(test_results, function(item) identical(item$kind, test_kind), logical(1))) + 1L
        test_result <- emit_test_output(
          kind = test_kind,
          frame = working,
          statements = if (idx <= 1L) list() else statements[seq_len(idx - 1L)],
          active_window = active_window,
          work_dir = work_dir,
          occurrence = test_occurrence
        )
        if (!is.null(test_result$path)) {
          emitted_files <- c(emitted_files, test_result$path)
        }
        test_results[[length(test_results) + 1L]] <- list(
          kind = test_kind,
          occurrence = as.integer(test_occurrence),
          solve_stage = as.integer(length(solve_results)),
          sample_start = as.character(active_window[[1]] %||% ""),
          sample_end = as.character(active_window[[2]] %||% ""),
          path = test_result$path,
          rows = nrow(test_result$data),
          max_abs_diff = test_result$max_abs_diff,
          targets = if (nrow(test_result$data)) paste(unique(as.character(test_result$data$target)), collapse = ",") else ""
        )
      }
      next
    }

    if (identical(command, "EST")) {
      estimation_records[[length(estimation_records) + 1L]] <- list(
        order = as.integer(idx),
        sample_start = as.character(active_window[[1]] %||% ""),
        sample_end = as.character(active_window[[2]] %||% ""),
        frame = working
      )
      next
    }

    if (is_bare_watch_statement(statement)) {
      next
    }

    if (identical(command, "SETYYTOY")) {
      if (!is.null(active_window)) {
        working <- apply_setyytoy_frame(working, active_window[[1]], active_window[[2]])
      }
      next
    }

    if (identical(command, "PRINTNAMES")) {
      emitted_path <- emit_printnames_output(working, work_dir)
      if (!is.null(emitted_path)) {
        emitted_files <- c(emitted_files, emitted_path)
      }
      next
    }

    if (identical(command, "PRINTMODEL")) {
      model_bundle <- if (length(solve_results)) {
        last_stage <- solve_results[[length(solve_results)]]
        list(
          bundle_name = tools::file_path_sans_ext(basename(sources$entry_path)),
          solve_stages = solve_results,
          presolve_replay_plan_rows = presolve_replay$replay_plan_rows %||% data.frame(),
          presolve_replay_plan_meta = presolve_replay$replay_plan_meta %||% list(),
          preserve_mode_audit = presolve_replay$preserve_mode_audit %||% data.frame(),
          specs = last_stage$bundle$specs %||% list(),
          equations = last_stage$bundle$equations %||% build_reduced_eq_specs(statements, fmout_path = sources$fmout, setupsolve = setupsolve),
          estimation_summary = collect_estimation_summary(statements),
          header_summary = collect_header_summary(statements)
        )
      } else {
        eq_support <- build_reduced_eq_specs(statements, fmout_path = sources$fmout, setupsolve = setupsolve)
        list(
          bundle_name = tools::file_path_sans_ext(basename(sources$entry_path)),
          solve_stages = list(),
          presolve_replay_plan_rows = presolve_replay$replay_plan_rows %||% data.frame(),
          presolve_replay_plan_meta = presolve_replay$replay_plan_meta %||% list(),
          preserve_mode_audit = presolve_replay$preserve_mode_audit %||% data.frame(),
          specs = eq_support$specs %||% list(),
          equations = eq_support,
          estimation_summary = collect_estimation_summary(statements),
          header_summary = collect_header_summary(statements)
        )
      }
      emitted_paths <- c(
        emit_printmodel_output(model_bundle, work_dir),
        emit_printmodel_support_outputs(model_bundle, work_dir)
      )
      if (length(emitted_paths)) {
        emitted_files <- c(emitted_files, emitted_paths)
      }
      next
    }

    if (identical(command, "PRINTVAR")) {
      emitted_path <- emit_runtime_printvar(
        working,
        statement,
        active_window,
        work_dir,
        source_info = sources,
        search_dirs = sources$search_dirs
      )
      if (!is.null(emitted_path)) {
        emitted_files <- c(emitted_files, emitted_path)
      }
      next
    }
  }

  last_stage <- if (length(solve_results)) solve_results[[length(solve_results)]] else NULL
  stage_summary <- if (!length(solve_results)) {
    data.frame(
      solve_stage = integer(),
      solve_index = integer(),
      sample_start = character(),
      sample_end = character(),
      solve_options = character(),
      watch_variables = character(),
      order = character(),
      eq_targets = character(),
      stringsAsFactors = FALSE
    )
  } else {
    data.frame(
      solve_stage = vapply(solve_results, `[[`, integer(1), "stage"),
      solve_index = vapply(solve_results, `[[`, integer(1), "solve_index"),
      sample_start = vapply(solve_results, function(item) as.character(item$bundle$control$sample_start %||% ""), character(1)),
      sample_end = vapply(solve_results, function(item) as.character(item$bundle$control$sample_end %||% ""), character(1)),
      solve_options = vapply(solve_results, function(item) as.character(item$solve_metadata$option_text %||% ""), character(1)),
      watch_variables = vapply(solve_results, function(item) paste(as.character(item$solve_metadata$watch_variables %||% character()), collapse = ","), character(1)),
      order = vapply(solve_results, function(item) paste(item$result$order, collapse = ","), character(1)),
      eq_targets = vapply(solve_results, function(item) {
        eq_specs <- Filter(function(spec) !is.null(spec$equation_number), item$bundle$specs %||% list())
        paste(vapply(eq_specs, function(spec) as.character(spec$target %||% spec$name), character(1)), collapse = ",")
      }, character(1)),
      stringsAsFactors = FALSE
    )
  }
  diagnostics <- if (!length(solve_results)) {
    empty_standard_diagnostics()
  } else {
    do.call(rbind, lapply(solve_results, `[[`, "diagnostics"))
  }
  test_summary <- if (!length(test_results)) {
    data.frame(
      kind = character(),
      occurrence = integer(),
      solve_stage = integer(),
      sample_start = character(),
      sample_end = character(),
      path = character(),
      rows = integer(),
      max_abs_diff = numeric(),
      targets = character(),
      stringsAsFactors = FALSE
    )
  } else {
    data.frame(
      kind = vapply(test_results, `[[`, character(1), "kind"),
      occurrence = vapply(test_results, `[[`, integer(1), "occurrence"),
      solve_stage = vapply(test_results, `[[`, integer(1), "solve_stage"),
      sample_start = vapply(test_results, `[[`, character(1), "sample_start"),
      sample_end = vapply(test_results, `[[`, character(1), "sample_end"),
      path = vapply(test_results, function(item) as.character(item$path %||% ""), character(1)),
      rows = vapply(test_results, `[[`, integer(1), "rows"),
      max_abs_diff = vapply(test_results, `[[`, numeric(1), "max_abs_diff"),
      targets = vapply(test_results, `[[`, character(1), "targets"),
      stringsAsFactors = FALSE
    )
  }
  equation_support <- last_stage$bundle$equations %||% build_reduced_eq_specs(statements, fmout_path = sources$fmout, setupsolve = setupsolve)
  solve_paths <- emit_solve_outputs(stage_summary, diagnostics, work_dir)
  if (length(solve_paths)) {
    emitted_files <- c(emitted_files, solve_paths)
  }
  solve_support_paths <- emit_solve_support_outputs(list(
    solve_stages = solve_results,
    presolve_replay_plan_rows = presolve_replay$replay_plan_rows %||% data.frame(),
    presolve_replay_plan_meta = presolve_replay$replay_plan_meta %||% list(),
    preserve_mode_audit = presolve_replay$preserve_mode_audit %||% data.frame(),
    exogenous_path_trace = exogenous_path_trace,
    control = if (is.null(last_stage)) list() else last_stage$bundle$control %||% list(),
    runtime = if (is.null(last_stage)) list() else last_stage$bundle$runtime %||% list()
  ), work_dir)
  if (length(solve_support_paths)) {
    emitted_files <- c(emitted_files, solve_support_paths)
  }
  estimation_paths <- emit_estimation_outputs(
    collect_estimation_summary(statements),
    equation_support,
    estimation_records,
    work_dir,
    fmout_path = sources$fmout
  )
  if (length(estimation_paths)) {
    emitted_files <- c(emitted_files, estimation_paths)
  }
  source_paths <- emit_source_outputs(list(
    entry_input = sources$entry_path,
    fmdata = sources$fmdata,
    fmexog = sources$fmexog,
    fmout = sources$fmout,
    files_scanned = sources$tree$files_scanned
  ), work_dir)
  if (length(source_paths)) {
    emitted_files <- c(emitted_files, source_paths)
  }
  test_summary_path <- emit_test_summary_output(test_summary, work_dir)
  if (!is.null(test_summary_path)) {
    emitted_files <- c(emitted_files, test_summary_path)
  }

  list(
    bundle_name = tools::file_path_sans_ext(basename(sources$entry_path)),
    semantics_profile = semantics_profile$name,
    series = working,
    diagnostics = diagnostics,
    order = if (is.null(last_stage)) character() else last_stage$result$order,
    solve_stages = solve_results,
    solve_stage_summary = stage_summary,
    test_summary = test_summary,
    termination_command = termination_command,
    solve_options = if (is.null(last_stage)) list() else last_stage$solve_metadata$options %||% list(),
    watch_variables = if (is.null(last_stage)) character() else as.character(last_stage$solve_metadata$watch_variables %||% character()),
    emitted_files = unique(normalizePath(emitted_files, winslash = "/", mustWork = FALSE)),
    presolve_replay_plan_rows = presolve_replay$replay_plan_rows %||% data.frame(),
    presolve_replay_plan_meta = presolve_replay$replay_plan_meta %||% list(),
    preserve_mode_audit = presolve_replay$preserve_mode_audit %||% data.frame(),
    exogenous_path_trace = exogenous_path_trace,
    header_summary = collect_header_summary(statements),
    estimation_summary = collect_estimation_summary(statements),
    source = list(
      entry_input = sources$entry_path,
      fmdata = sources$fmdata,
      fmexog = sources$fmexog,
      fmout = sources$fmout,
      files_scanned = sources$tree$files_scanned,
      semantics_profile = semantics_profile$name
    ),
    test_outputs = test_results,
    specs = last_stage$bundle$specs %||% list(),
    equations = equation_support
  )
}

read_standard_input_bundle <- function(entry_input, fmdata_path = NULL, fmexog_path = NULL, fmout_path = NULL, search_dirs = NULL) {
  semantics_profile <- resolve_standard_input_semantics_profile()
  sources <- resolve_standard_input_sources(
    entry_input,
    fmdata_path = fmdata_path,
    fmexog_path = fmexog_path,
    fmout_path = fmout_path,
    search_dirs = search_dirs
  )
  sources$tree <- apply_base_helper_overlay_to_tree(
    sources$entry_path,
    sources$tree,
    search_dirs = sources$search_dirs,
    semantics_profile = semantics_profile$name
  )
  entry_path <- sources$entry_path
  tree <- sources$tree
  parsed <- sources$parsed
  resolved_search_dirs <- sources$search_dirs
  resolved_fmdata <- sources$fmdata
  resolved_fmexog <- sources$fmexog
  resolved_fmout <- sources$fmout

  base_frame <- base_standard_input_frame(resolved_fmdata)
  runtime <- prepare_standard_runtime(
    tree$statements,
    frame = base_frame,
    search_dirs = resolved_search_dirs,
    default_fmexog_path = resolved_fmexog
  )
  termination_index <- as.integer(runtime$termination_index %||% 0L)
  solve_snapshot <- runtime$solve_snapshot
  presolve_replay <- standard_presolve_replay_context(
    tree$statements,
    runtime$frame,
    termination_index = termination_index,
    solve_snapshot = solve_snapshot,
    exogenous_targets = runtime$exogenous_targets %||% character(),
    coef_values = list()
  )
  eq_support <- build_reduced_eq_specs(
    presolve_replay$statements,
    fmout_path = resolved_fmout,
    setupsolve = solve_snapshot$setupsolve %||% list()
  )
  frame <- runtime$frame
  presolve_replay$preserve_modes <- infer_replay_preserve_modes(
    presolve_replay$statements,
    frame,
    assignment_targets = presolve_replay$assignment_targets,
    coef_values = eq_support$coef_values %||% list()
  )
  replay_statements <- resolve_simple_cust_compat_replay_statements(
    presolve_replay$statements,
    fmout_path = resolved_fmout,
    assignment_targets = presolve_replay$assignment_targets,
    preserve_modes_by_target = presolve_replay$preserve_modes,
    semantics_profile = semantics_profile$name
  )
  replay_plan <- build_runtime_replay_plan(
    replay_statements,
    assignment_targets = presolve_replay$assignment_targets,
    replay_inline_changevar = FALSE
  )
  presolve_replay$replay_plan_rows <- build_runtime_replay_plan_rows(
    replay_plan,
    preserve_modes_by_target = presolve_replay$preserve_modes
  )
  presolve_replay$replay_plan_meta <- list(
    cyclic_targets = replay_plan$cyclic_targets %||% character(),
    revisit_targets = replay_plan$revisit_targets %||% character()
  )
  presolve_replay$preserve_mode_audit <- attr(presolve_replay$preserve_modes, "audit") %||% data.frame()
  if (length(presolve_replay$assignment_targets)) {
    frame <- replay_selected_runtime_assignments(
      replay_statements,
      frame,
      assignment_targets = presolve_replay$assignment_targets,
      coef_values = eq_support$coef_values %||% list(),
      preserve_existing = TRUE,
      preserve_mode = "skip",
      preserve_modes_by_target = presolve_replay$preserve_modes,
      replay_inline_changevar = FALSE
    )
  }
  exogenous_targets <- as.character(runtime$exogenous_targets %||% character())
  exogenous_path_trace <- data.frame(
    phase = character(),
    period = character(),
    variable = character(),
    value = numeric(),
    stringsAsFactors = FALSE
  )
  if (!is.null(solve_snapshot) &&
      nzchar(solve_snapshot$sample_start %||% "") &&
      nzchar(solve_snapshot$sample_end %||% "")) {
    exogenous_path_trace <- append_exogenous_path_trace_rows(
      exogenous_path_trace,
      frame,
      active_window = c(solve_snapshot$sample_start, solve_snapshot$sample_end),
      exogenous_targets = exogenous_targets,
      watch_variables = solve_snapshot$watch_variables %||% character(),
      phase = "bundle_state_post_replay"
    )
  }
  if (!is.null(solve_snapshot) &&
      nzchar(solve_snapshot$sample_start %||% "") &&
      nzchar(solve_snapshot$sample_end %||% "") &&
      length(runtime$exogenous_targets %||% character())) {
    frame <- apply_extrapolate_frame(
      frame,
      window_start = solve_snapshot$sample_start,
      window_end = solve_snapshot$sample_end,
      variables = runtime$exogenous_targets %||% character(),
      include_all_columns = TRUE
    )
    exogenous_path_trace <- append_exogenous_path_trace_rows(
      exogenous_path_trace,
      frame,
      active_window = c(solve_snapshot$sample_start, solve_snapshot$sample_end),
      exogenous_targets = exogenous_targets,
      watch_variables = solve_snapshot$watch_variables %||% character(),
      phase = "bundle_state_post_extrapolate"
    )
  }
  state <- state_from_frame(frame)
  state$coef_values <- eq_support$coef_values %||% numeric()
  if (!length(state$periods)) {
    sample_windows <- Filter(function(item) length(item) >= 2L, parsed$sample_windows)
    if (length(sample_windows)) {
      periods <- seq_periods(sample_windows[[1L]][[1]], sample_windows[[1L]][[2]])
      state$periods <- periods
    }
  }

  control <- list()
  if (!is.null(solve_snapshot)) {
    control <- modifyList(control, solve_snapshot$setupsolve %||% list())
    if (nzchar(solve_snapshot$sample_start %||% "")) {
      control$sample_start <- solve_snapshot$sample_start
    }
    if (nzchar(solve_snapshot$sample_end %||% "")) {
      control$sample_end <- solve_snapshot$sample_end
    }
  } else {
    control <- modifyList(control, runtime$setupsolve %||% list())
    sample_windows <- Filter(function(item) length(item) >= 2L, parsed$sample_windows)
    if (length(sample_windows)) {
      window <- sample_windows[[1L]]
      control$sample_start <- window[[1]]
      control$sample_end <- window[[2]]
    }
  }
  candidate_specs <- if (presolve_replay$spec_limit <= 0L) {
    list()
  } else {
    Filter(
      function(item) item$kind != "control" && !is.null(item$expression),
      presolve_replay$statements
    )
  }
  specs <- if (presolve_replay$spec_limit <= 0L) {
    list()
  } else {
    partition_standard_solve_specs(
      eq_specs = eq_support$specs,
      candidate_specs = candidate_specs,
      exogenous_targets = exogenous_targets,
      exogenous_equation_target_policy = semantics_profile$exogenous_equation_target_policy
    )
  }
  control$order <- vapply(specs$specs %||% list(), function(item) as.character(item$target %||% item$name %||% ""), character(1))
  control$order <- control$order[nzchar(control$order)]
  control <- apply_semantics_profile_to_solve_control(control, semantics_profile)
  equation_trace <- resolve_standard_input_equation_trace_config(
    sample_start = solve_snapshot$sample_start %||% ""
  )
  if (isTRUE(equation_trace$enabled)) {
    control$equation_input_trace_periods <- equation_trace$periods
    control$equation_input_trace_targets <- equation_trace$targets
    control$equation_input_trace_max_iterations <- equation_trace$max_iterations
  }
  if (!is.null(solve_snapshot) &&
      nzchar(solve_snapshot$sample_start %||% "") &&
      isTRUE((solve_snapshot$solve_options %||% list())$outside) &&
      isTRUE((solve_snapshot$solve_options %||% list())$noreset)) {
    equation_targets <- unique(toupper(vapply(eq_support$specs %||% list(), function(item) {
      as.character(item$target %||% item$name %||% "")
    }, character(1))))
    equation_targets <- equation_targets[nzchar(equation_targets)]
    equation_support_refs <- unique(unlist(lapply(eq_support$specs %||% list(), function(item) {
      refs <- tokens_to_reference_frame(item$rhs_tokens %||% character())
      if (!is.data.frame(refs) || !nrow(refs)) {
        return(character())
      }
      unique(toupper(as.character(refs$name[as.integer(refs$lag) == 0L])))
    })))
    equation_support_refs <- equation_support_refs[nzchar(equation_support_refs)]
    outside_carry_plan <- build_outside_carry_plan(
      candidate_specs,
      statements = presolve_replay$statements,
      sample_start = solve_snapshot$sample_start,
      protected_targets = exogenous_targets,
      equation_targets = equation_targets,
      equation_support_refs = equation_support_refs
    )
    outside_snapshot_variables <- resolve_outside_snapshot_variables(
      outside_carry_plan,
      watch_variables = solve_snapshot$watch_variables %||% character(),
      spec_targets = vapply(specs$specs %||% list(), function(item) as.character(item$target %||% item$name %||% ""), character(1))
    )
    outside_carry_snapshots <- build_frame_snapshot_rows(
      frame,
      outside_snapshot_variables,
      solve_snapshot$sample_start,
      "bundle_state"
    )
    frame <- apply_outside_carry_plan_frame(
      frame,
      sample_start = solve_snapshot$sample_start,
      plan = outside_carry_plan,
      semantics_profile = semantics_profile,
      protected_targets = exogenous_targets
    )
    outside_carry_snapshots <- rbind(
      outside_carry_snapshots,
      build_frame_snapshot_rows(
        frame,
        outside_snapshot_variables,
        solve_snapshot$sample_start,
        "bundle_state_post_carry"
      )
    )
    exogenous_path_trace <- append_exogenous_path_trace_rows(
      exogenous_path_trace,
      frame,
      active_window = c(solve_snapshot$sample_start, solve_snapshot$sample_end),
      exogenous_targets = exogenous_targets,
      watch_variables = solve_snapshot$watch_variables %||% character(),
      phase = "bundle_state_post_carry"
    )
  } else {
    outside_carry_plan <- list(
      boundary_targets = character(),
      first_period_targets = character(),
      target_roles = data.frame(
        target = character(),
        boundary_role = character(),
        first_period_role = character(),
        boundary_materialized = logical(),
        direct_negative_lag = logical(),
        stringsAsFactors = FALSE,
        check.names = FALSE
      )
    )
    outside_snapshot_variables <- character()
    outside_carry_snapshots <- data.frame(
      phase = character(),
      period = character(),
      variable = character(),
      value = numeric(),
      stringsAsFactors = FALSE
    )
  }
  control$outside_carry_plan <- outside_carry_plan
  state <- state_from_frame(frame)
  state$coef_values <- eq_support$coef_values %||% numeric()

  list(
    name = tools::file_path_sans_ext(basename(entry_path)),
    source = list(
      entry_input = entry_path,
      fmdata = resolved_fmdata,
      fmexog = resolved_fmexog,
      fmout = resolved_fmout,
      loaddata = unique(vapply(
        Filter(function(item) identical(item$command, "LOADDATA"), tree$statements),
        function(item) extract_fp_file_arg(item$raw %||% "", key = "FILE") %||% "",
        character(1)
      )),
      files_scanned = tree$files_scanned,
      semantics_profile = semantics_profile$name
    ),
    runtime = list(
      statements = tree$statements,
      solve_index = solve_snapshot$solve_index %||% 0L,
      solve_window_start = solve_snapshot$sample_start %||% "",
      solve_window_end = solve_snapshot$sample_end %||% "",
      solve_options = solve_snapshot$solve_options %||% list(),
      watch_variables = solve_snapshot$watch_variables %||% character(),
      solve_option_text = solve_snapshot$solve_option_text %||% "",
      solve_watch_text = solve_snapshot$solve_watch_text %||% "",
      termination_command = runtime$termination_command %||% "",
      termination_index = as.integer(runtime$termination_index %||% 0L),
      semantics_profile = semantics_profile$name,
      solver_policy = semantics_profile$solver_policy,
      presolve_replay_plan_rows = presolve_replay$replay_plan_rows %||% data.frame(),
      presolve_replay_plan_meta = presolve_replay$replay_plan_meta %||% list(),
      preserve_mode_audit = presolve_replay$preserve_mode_audit %||% data.frame(),
      outside_carry_plan = outside_carry_plan,
      outside_snapshot_variables = outside_snapshot_variables,
      outside_carry_snapshots = outside_carry_snapshots,
      exogenous_path_trace = exogenous_path_trace
    ),
    equations = eq_support,
    header_summary = collect_header_summary(tree$statements),
    estimation_summary = collect_estimation_summary(tree$statements),
    state = state,
    specs = specs$specs %||% list(),
    post_solve_assignments = specs$post_solve_assignments %||% list(),
    control = control,
    semantics_profile = semantics_profile$name,
    solver_policy = semantics_profile$solver_policy,
    input_text = tree$text
  )
}
