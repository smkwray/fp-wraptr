fp_statement_leading_keyword <- function(text) {
  lines <- strsplit(gsub("\r", "", as.character(text %||% "")), "\n", fixed = TRUE)[[1]]
  lines <- trimws(lines)
  lines <- lines[nzchar(lines)]
  if (!length(lines)) {
    return("")
  }
  toupper(sub("\\s+.*$", "", lines[[1]], perl = TRUE))
}

is_fp_line_terminated_command <- function(keyword) {
  toupper(as.character(keyword %||% "")) %in% c(
    "US", "SPACE", "SETUPEST", "SETUPSOLVE", "2SLS", "SMPL", "LOADDATA",
    "PRINTMODEL", "PRINTNAMES", "PRINTVAR", "QUIT", "RETURN", "EXTRAPOLATE",
    "EXOGENOUS", "ENDOGENOUS", "SETYYTOY", "TEST", "EST", "END", "SOLVE", "INPUT"
  )
}

is_fp_known_command <- function(keyword) {
  toupper(as.character(keyword %||% "")) %in% c(
    "2SLS", "CHANGEVAR", "CREATE", "END", "ENDOGENOUS", "EQ", "EST", "EXOGENOUS",
    "EXTRAPOLATE", "FSR", "GENR", "IDENT", "INPUT", "LHS", "LOADDATA", "MODEQ",
    "PRINTMODEL", "PRINTNAMES", "PRINTVAR", "QUIT", "RETURN", "SETUPEST",
    "SETUPSOLVE", "SETYYTOY", "SMPL", "SOLVE", "SPACE", "TEST", "US"
  )
}

split_fp_statements <- function(text) {
  lines <- strsplit(gsub("\r", "", text), "\n", fixed = TRUE)[[1]]
  filtered <- lines[!grepl("^\\s*@", lines)]
  statements <- list()
  current <- ""

  flush_current <- function() {
    statement <- trimws(current)
    if (nzchar(statement)) {
      statements[[length(statements) + 1L]] <<- statement
    }
    current <<- ""
  }

  for (raw_line in filtered) {
    line <- trimws(raw_line)
    if (!nzchar(line)) {
      next
    }

    if (nzchar(current)) {
      current_keyword <- fp_statement_leading_keyword(current)
      next_keyword <- fp_statement_leading_keyword(line)
      if (!grepl(";", current, fixed = TRUE) &&
          is_fp_line_terminated_command(current_keyword) &&
          is_fp_known_command(next_keyword)) {
        flush_current()
      }
    }

    current <- if (nzchar(current)) paste(current, raw_line, sep = "\n") else raw_line
    remaining <- current
    repeat {
      semi <- regexpr(";", remaining, fixed = TRUE)[[1]]
      if (semi < 0L) {
        break
      }
      statement <- trimws(substr(remaining, 1L, semi - 1L))
      if (nzchar(statement)) {
        statements[[length(statements) + 1L]] <- statement
      }
      remaining <- substr(remaining, semi + 1L, nchar(remaining))
    }
    current <- trimws(remaining)
  }

  if (nzchar(current)) {
    flush_current()
  }
  statements
}

parse_assignment_statement <- function(statement) {
  normalized <- trimws(statement)
  keyword <- toupper(sub("\\s+.*$", "", normalized))
  if (keyword %in% c("GENR", "IDENT", "LHS")) {
    matches <- regexec(
      "^([A-Za-z]+)\\s+([A-Za-z][A-Za-z0-9_]*)\\s*=\\s*([\\s\\S]+)$",
      normalized,
      perl = TRUE
    )
    parts <- regmatches(normalized, matches)[[1]]
    if (length(parts) != 4L) {
      stopf("Could not parse assignment statement: %s", normalized)
    }
    return(list(
      kind = tolower(parts[2]),
      name = parts[3],
      expression = trimws(parts[4]),
      raw = normalized
    ))
  }
  if (keyword == "CREATE") {
    matches <- regexec(
      "^CREATE\\s+([A-Za-z][A-Za-z0-9_]*)(?:\\s*=\\s*([\\s\\S]+))?$",
      normalized,
      perl = TRUE
    )
    parts <- regmatches(normalized, matches)[[1]]
    if (length(parts) < 2L) {
      stopf("Could not parse CREATE statement: %s", normalized)
    }
    expression <- if (length(parts) >= 3L && nzchar(parts[3])) trimws(parts[3]) else NULL
    return(list(
      kind = "create",
      name = parts[2],
      expression = expression,
      raw = normalized
    ))
  }
  NULL
}

parse_solve_statement <- function(statement) {
  normalized <- trimws(as.character(statement %||% ""))
  if (!nzchar(normalized)) {
    return(NULL)
  }

  tokens <- strsplit(gsub(";", " ", gsub("[\r\n]", " ", normalized), fixed = FALSE), "\\s+", perl = TRUE)[[1]]
  tokens <- tokens[nzchar(tokens)]
  if (length(tokens) < 2L || !identical(toupper(tokens[[1]]), "SOLVE")) {
    return(NULL)
  }

  solve_options <- list(
    dynamic = FALSE,
    outside = FALSE,
    filevar = NULL,
    noreset = FALSE
  )
  watch_variables <- character()
  seen_watch_payload <- FALSE

  for (token in tokens[-1L]) {
    upper <- toupper(token)
    if (!seen_watch_payload && upper == "DYNAMIC") {
      solve_options$dynamic <- TRUE
      next
    }
    if (!seen_watch_payload && upper == "OUTSIDE") {
      solve_options$outside <- TRUE
      next
    }
    if (!seen_watch_payload && upper == "NORESET") {
      solve_options$noreset <- TRUE
      next
    }
    if (!seen_watch_payload && grepl("^FILEVAR\\s*=", upper, perl = TRUE)) {
      solve_options$filevar <- clean_fp_token(sub("^FILEVAR\\s*=\\s*", "", token, ignore.case = TRUE, perl = TRUE))
      next
    }
    if (!seen_watch_payload && grepl("=", token, fixed = TRUE)) {
      seen_watch_payload <- TRUE
      next
    }
    if (!seen_watch_payload && !grepl("^[A-Za-z][A-Za-z0-9_]*$", token, perl = TRUE)) {
      next
    }
    seen_watch_payload <- TRUE
    if (grepl("^[A-Za-z][A-Za-z0-9_]*$", token, perl = TRUE)) {
      watch_variables <- c(watch_variables, clean_fp_token(token))
    }
  }

  list(
    command = "SOLVE",
    kind = "control",
    name = "SOLVE",
    body = trimws(sub("^[A-Za-z]+\\s*", "", normalized)),
    raw = normalized,
    solve_options = solve_options,
    watch_variables = unique(watch_variables)
  )
}

parse_watch_variables_statement <- function(statement) {
  normalized <- trimws(as.character(statement %||% ""))
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

parse_fp_input <- function(text) {
  statements <- split_fp_statements(text)
  parsed <- list()
  idx <- 1L
  while (idx <= length(statements)) {
    statement <- statements[[idx]]
    normalized <- trimws(statement)
    keyword <- toupper(sub("\\s+.*$", "", normalized))
    assignment <- parse_assignment_statement(normalized)
    if (!is.null(assignment)) {
      parsed[[length(parsed) + 1L]] <- c(list(command = keyword), assignment)
      idx <- idx + 1L
      next
    }
    if (identical(keyword, "SOLVE")) {
      parsed_solve <- parse_solve_statement(normalized)
      if (!is.null(parsed_solve)) {
        if (idx < length(statements)) {
          watch_variables <- parse_watch_variables_statement(statements[[idx + 1L]])
          if (length(watch_variables)) {
            parsed_solve$watch_variables <- unique(c(
              parsed_solve$watch_variables %||% character(),
              watch_variables
            ))
            idx <- idx + 1L
          }
        }
        parsed[[length(parsed) + 1L]] <- parsed_solve
        idx <- idx + 1L
        next
      }
    }
    parsed[[length(parsed) + 1L]] <- list(
      command = keyword,
      kind = "control",
      name = keyword,
      body = trimws(sub("^[A-Za-z]+\\s*", "", normalized)),
      raw = normalized
    )
    idx <- idx + 1L
  }
  assignments <- Filter(function(item) item$kind != "control", parsed)
  control_commands <- Filter(function(item) item$kind == "control", parsed)
  equations <- Filter(Negate(is.null), lapply(
    Filter(function(item) identical(item$command, "EQ"), control_commands),
    function(item) parse_eq_statement(item$raw %||% item$body %||% "")
  ))
  modeq <- Filter(Negate(is.null), lapply(
    Filter(function(item) identical(item$command, "MODEQ"), control_commands),
    function(item) parse_modeq_statement(item$raw %||% item$body %||% "")
  ))
  sample_windows <- lapply(
    Filter(function(item) identical(item$command, "SMPL"), control_commands),
    function(item) strsplit(item$body, "\\s+", perl = TRUE)[[1]]
  )
  list(
    statements = parsed,
    assignments = assignments,
    control_commands = control_commands,
    equations = equations,
    modeq = modeq,
    sample_windows = sample_windows
  )
}
