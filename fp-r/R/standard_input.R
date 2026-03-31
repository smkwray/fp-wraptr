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

emit_printmodel_support_outputs <- function(bundle, work_dir) {
  Filter(Negate(is.null), c(
    emit_summary_output(bundle$estimation_summary %||% data.frame(), "ESTIMATION_SUMMARY.csv", work_dir),
    emit_summary_output(bundle$header_summary %||% data.frame(), "HEADER_SUMMARY.csv", work_dir)
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
  lines <- strsplit(gsub("\r", "", text), "\n", fixed = TRUE)[[1]]
  lines <- trimws(lines)
  lines <- lines[nzchar(lines)]
  if (!length(lines)) {
    return(FALSE)
  }
  any(grepl("^CHANGEVAR\\b", lines, ignore.case = TRUE, perl = TRUE)) ||
    any(grepl("^RETURN\\b", lines, ignore.case = TRUE, perl = TRUE))
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
    parsed_statements <- parsed$statements
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
        statements <- c(statements, nested$statements)
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
  changed <- FALSE
  for (period_pos in target_positions) {
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
    state$series[[target]] <- target_values
  }
  working[[target]] <- target_values

  list(frame = working, state = state, changed = changed)
}

base_standard_input_frame <- function(fmdata_path) {
  if (is.null(fmdata_path) || !nzchar(fmdata_path)) {
    return(data.frame(period = character(), stringsAsFactors = FALSE, check.names = FALSE))
  }
  parse_fm_numeric_file(fmdata_path, block_name = basename(fmdata_path))
}

filter_standard_specs_for_exogenous <- function(specs, exogenous_targets = character()) {
  if (!length(exogenous_targets)) {
    return(specs)
  }
  Filter(
    function(item) !(toupper(item$name %||% item$target %||% "") %in% toupper(exogenous_targets)),
    specs
  )
}

partition_standard_solve_specs <- function(eq_specs = list(), candidate_specs = list(), exogenous_targets = character()) {
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
  setup_only_mask <- same_target_setup | safe_setup_mask
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
  solve_specs <- c(
    eq_specs %||% list(),
    candidate_solve_specs
  )
  list(
    specs = filter_standard_specs_for_exogenous(solve_specs, exogenous_targets = exogenous_targets),
    setup_only_assignments = filter_standard_specs_for_exogenous(setup_only_assignments, exogenous_targets = exogenous_targets),
    post_solve_assignments = filter_standard_specs_for_exogenous(post_solve_assignments, exogenous_targets = exogenous_targets)
  )
}

replay_selected_runtime_assignments <- function(statements, frame, assignment_targets = character(), coef_values = NULL, preserve_existing = FALSE, preserve_mode = c("skip", "fallback"), preserve_modes_by_target = NULL) {
  preserve_mode <- match.arg(preserve_mode)
  targets <- unique(toupper(as.character(assignment_targets %||% character())))
  targets <- targets[nzchar(targets)]
  if (!length(targets) || !length(statements)) {
    return(sort_frame_by_period(frame))
  }

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

    for (statement in items) {
      command <- statement_command_runtime(statement)
      raw <- statement$raw %||% ""
      if (identical(command, "SMPL")) {
        flush_pending_block()
        plan[[length(plan) + 1L]] <- list(
          type = "smpl",
          raw = raw
        )
        next
      }
      if (!is_runtime_assignment_statement(statement)) {
        next
      }
      target <- toupper(as.character(statement$name %||% statement$target %||% ""))
      if (!(target %in% targets)) {
        next
      }
      if (target %in% pending_targets) {
        flush_pending_block()
      }
      pending_block[[length(pending_block) + 1L]] <- statement
      pending_targets <- c(pending_targets, target)
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
      for (statement in plan_item$statements %||% list()) {
        target <- toupper(as.character(statement$name %||% ""))
        if (pass > 1L && length(active_targets) && !(target %in% active_targets)) {
          next
        }
        applied <- apply_runtime_assignment_state_frame(
          working,
          statement,
          active_window = active_window,
          state = state,
          coef_values = coef_values,
          preserve_existing = preserve_existing,
          preserve_mask = preserve_mask,
          preserve_mode = if (!is.null(preserve_modes_by_target) && target %in% names(preserve_modes_by_target)) {
            as.character(preserve_modes_by_target[[target]])
          } else {
            preserve_mode
          }
        )
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
        break
      }
      if (materially_differs_for_fallback(candidate_value, protected_value)) {
        fallback_targets <- c(fallback_targets, target)
        break
      }
    }
  }

  modes <- rep("skip", length(target_set))
  names(modes) <- target_set
  if (length(fallback_targets)) {
    modes[unique(fallback_targets)] <- "fallback"
  }
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
      preserve_modes = stats::setNames(character(), character())
    ))
  }

  pre_solve_statements <- statements[seq_len(spec_limit)]
  pre_solve_assignment_targets <- unique(vapply(
    Filter(
      function(item) {
        kind <- tolower(as.character(item$kind %||% ""))
        target <- toupper(as.character(item$name %||% item$target %||% ""))
        kind %in% c("create", "genr", "ident") &&
          !(target %in% active_exogenous_targets)
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
  list(
    spec_limit = as.integer(spec_limit),
    statements = pre_solve_statements,
    assignment_targets = pre_solve_assignment_targets,
    preserve_modes = preserve_modes
  )
}

build_standard_solve_bundle <- function(sources, frame, history_statements, solve_index = 0L, active_window = NULL, setupsolve = list(), exogenous_targets = character(), solve_metadata = list()) {
  eq_support <- build_reduced_eq_specs(
    history_statements,
    fmout_path = sources$fmout,
    setupsolve = setupsolve
  )
  candidate_specs <- Filter(
    function(item) item$kind != "control" && !is.null(item$expression),
    history_statements
  )
  spec_partition <- partition_standard_solve_specs(
    eq_specs = eq_support$specs,
    candidate_specs = candidate_specs,
    exogenous_targets = exogenous_targets
  )
  specs <- spec_partition$specs
  post_solve_assignments <- spec_partition$post_solve_assignments
  pre_solve_assignment_targets <- unique(vapply(
    Filter(
      function(item) {
        kind <- tolower(as.character(item$kind %||% ""))
        target <- toupper(as.character(item$name %||% item$target %||% ""))
        kind %in% c("create", "genr", "ident") &&
          !(target %in% toupper(as.character(exogenous_targets %||% character())))
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
  pre_solve_frame <- replay_selected_runtime_assignments(
    history_statements,
    frame,
    assignment_targets = pre_solve_assignment_targets,
    coef_values = eq_support$coef_values %||% list(),
    preserve_existing = TRUE,
    preserve_mode = "skip",
    preserve_modes_by_target = preserve_modes
  )

  state <- state_from_frame(pre_solve_frame)
  state$coef_values <- eq_support$coef_values %||% numeric()
  if (!length(state$periods) && !is.null(active_window) && length(active_window) >= 2L) {
    state$periods <- seq_periods(active_window[[1]], active_window[[2]])
  }

  control <- setupsolve %||% list()
  if (!is.null(active_window) && length(active_window) >= 2L) {
    control$sample_start <- active_window[[1]]
    control$sample_end <- active_window[[2]]
  }
  control$order <- vapply(specs, function(item) as.character(item$target %||% item$name %||% ""), character(1))
  control$order <- control$order[nzchar(control$order)]

  list(
    name = tools::file_path_sans_ext(basename(sources$entry_path)),
    source = list(
      entry_input = sources$entry_path,
      fmdata = sources$fmdata,
      fmexog = sources$fmexog,
      fmout = sources$fmout,
      files_scanned = sources$tree$files_scanned
    ),
    runtime = list(
      statements = sources$tree$statements,
      solve_index = as.integer(solve_index),
      solve_window_start = active_window[[1]] %||% "",
      solve_window_end = active_window[[2]] %||% "",
      solve_options = solve_metadata$options %||% list(),
      watch_variables = solve_metadata$watch_variables %||% character(),
      solve_option_text = solve_metadata$option_text %||% "",
      solve_watch_text = solve_metadata$watch_text %||% ""
    ),
    equations = eq_support,
    state = state,
    specs = specs,
    post_solve_assignments = post_solve_assignments,
    control = control,
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
    sample_start = character(),
    sample_end = character(),
    stringsAsFactors = FALSE
  )
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
  sources <- resolve_standard_input_sources(
    entry_input,
    fmdata_path = fmdata_path,
    fmexog_path = fmexog_path,
    fmout_path = fmout_path,
    search_dirs = search_dirs
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
        if (!length(solve_results)) {
          protected_frame <- apply_extrapolate_frame(
            protected_frame,
            window_start = active_window[[1]],
            window_end = active_window[[2]],
            variables = exogenous_targets,
            include_all_columns = TRUE
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
      working <- apply_runtime_assignment_frame(
        working,
        statement,
        active_window = active_window,
        coef_values = runtime_coef_values,
        preserve_existing = !length(solve_results),
        preserve_mask = if (!length(solve_results)) build_frame_finite_mask(protected_frame) else NULL,
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
      next
    }

    if (identical(command, "SOLVE")) {
      history_statements <- if (idx <= 1L) list() else statements[seq_len(idx - 1L)]
      following_statements <- if (idx < length(statements)) statements[seq.int(idx + 1L, length(statements))] else list()
      solve_metadata <- solve_statement_metadata(statement, following_statements)
      stage_bundle <- build_standard_solve_bundle(
        sources,
        frame = working,
        history_statements = history_statements,
        solve_index = idx,
        active_window = active_window,
        setupsolve = setupsolve,
        exogenous_targets = exogenous_targets,
        solve_metadata = solve_metadata
      )
      stage_result <- mini_run(stage_bundle, control = list(resid_ar1_states = resid_ar1_states))
      working <- sort_frame_by_period(stage_result$series)
      resid_ar1_states <- stage_result$resid_ar1_states %||% resid_ar1_states
      stage_index <- length(solve_results) + 1L
      stage_diag <- stage_result$diagnostics
      stage_diag$solve_stage <- stage_index
      stage_diag$sample_start <- stage_bundle$control$sample_start %||% ""
      stage_diag$sample_end <- stage_bundle$control$sample_end %||% ""
      stage_diag <- stage_diag[, c("solve_stage", "period", "iterations", "converged", "max_delta", "termination", "sample_start", "sample_end"), drop = FALSE]
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
    header_summary = collect_header_summary(statements),
    estimation_summary = collect_estimation_summary(statements),
    source = list(
      entry_input = sources$entry_path,
      fmdata = sources$fmdata,
      fmexog = sources$fmexog,
      fmout = sources$fmout,
      files_scanned = sources$tree$files_scanned
    ),
    test_outputs = test_results,
    specs = last_stage$bundle$specs %||% list(),
    equations = equation_support
  )
}

read_standard_input_bundle <- function(entry_input, fmdata_path = NULL, fmexog_path = NULL, fmout_path = NULL, search_dirs = NULL) {
  sources <- resolve_standard_input_sources(
    entry_input,
    fmdata_path = fmdata_path,
    fmexog_path = fmexog_path,
    fmout_path = fmout_path,
    search_dirs = search_dirs
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
  if (length(presolve_replay$assignment_targets)) {
    frame <- replay_selected_runtime_assignments(
      presolve_replay$statements,
      frame,
      assignment_targets = presolve_replay$assignment_targets,
      coef_values = eq_support$coef_values %||% list(),
      preserve_existing = TRUE,
      preserve_mode = "skip",
      preserve_modes_by_target = presolve_replay$preserve_modes
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
  exogenous_targets <- as.character(runtime$exogenous_targets %||% character())
  specs <- if (presolve_replay$spec_limit <= 0L) {
    list()
  } else {
    candidate_specs <- Filter(
      function(item) item$kind != "control" && !is.null(item$expression),
      presolve_replay$statements
    )
    partition_standard_solve_specs(
      eq_specs = eq_support$specs,
      candidate_specs = candidate_specs,
      exogenous_targets = exogenous_targets
    )
  }
  control$order <- vapply(specs$specs %||% list(), function(item) as.character(item$target %||% item$name %||% ""), character(1))
  control$order <- control$order[nzchar(control$order)]

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
      files_scanned = tree$files_scanned
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
      termination_index = as.integer(runtime$termination_index %||% 0L)
    ),
    equations = eq_support,
    header_summary = collect_header_summary(tree$statements),
    estimation_summary = collect_estimation_summary(tree$statements),
    state = state,
    specs = specs$specs %||% list(),
    post_solve_assignments = specs$post_solve_assignments %||% list(),
    control = control,
    input_text = tree$text
  )
}
