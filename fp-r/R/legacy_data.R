generate_smpl_periods <- function(start, end) {
  seq_periods(as.character(start), as.character(end))
}

parse_legacy_numeric_tokens <- function(line) {
  matches <- gregexpr(
    "(?:[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[Ee][+-]?\\d+)?|NA|NAN)",
    line,
    perl = TRUE,
    ignore.case = TRUE
  )
  found <- regmatches(line, matches)[[1]]
  if (!length(found)) {
    return(numeric())
  }
  vapply(found, function(token) {
    upper <- toupper(trimws(token))
    if (upper %in% c("NA", "NAN")) {
      return(NA_real_)
    }
    as.numeric(token)
  }, numeric(1))
}

parse_fm_numeric_text <- function(text, block_name = "fmdata") {
  lines <- strsplit(gsub("\r", "", text), "\n", fixed = TRUE)[[1]]
  current_periods <- NULL
  series_by_variable <- list()
  i <- 1L

  while (i <= length(lines)) {
    raw_line <- trimws(lines[[i]])
    i <- i + 1L

    if (!nzchar(raw_line) || grepl("^\\s*@", raw_line)) {
      next
    }

    tokens <- strsplit(raw_line, "\\s+", perl = TRUE)[[1]]
    if (!length(tokens)) {
      next
    }

    command <- toupper(tokens[[1]])
    if (identical(command, "SMPL")) {
      if (length(tokens) < 3L) {
        stopf("SMPL statement missing boundaries in %s", block_name)
      }
      current_periods <- generate_smpl_periods(tokens[[2]], tokens[[3]])
      next
    }

    if (!identical(command, "LOAD")) {
      next
    }
    if (is.null(current_periods)) {
      stopf("LOAD block for %s before SMPL statement", block_name)
    }
    if (length(tokens) < 2L) {
      stopf("LOAD block in %s missing variable name", block_name)
    }

    variable <- gsub("^['\"]|['\"]$", "", tokens[[2]])
    variable <- sub(";\\s*$", "", variable)
    values <- numeric()
    closed <- FALSE

    while (i <= length(lines)) {
      current <- trimws(lines[[i]])
      i <- i + 1L

      if (!nzchar(current)) {
        next
      }
      if (grepl("^\\s*'END'\\s*$", current, perl = TRUE) || grepl("^\\s*END\\s*;?\\s*$", current, perl = TRUE)) {
        closed <- TRUE
        break
      }
      numbers <- parse_legacy_numeric_tokens(current)
      if (!length(numbers)) {
        stopf("Expected numeric values in %s LOAD %s", block_name, variable)
      }
      values <- c(values, as.numeric(numbers))
    }

    if (!closed) {
      stopf(
        "Unterminated LOAD block for %s in %s block %s..%s",
        variable,
        block_name,
        current_periods[[1]],
        current_periods[[length(current_periods)]]
      )
    }
    if (length(values) != length(current_periods)) {
      stopf(
        "Value count mismatch for %s variable %s: expected %d, found %d",
        block_name,
        variable,
        length(current_periods),
        length(values)
      )
    }

    existing <- series_by_variable[[variable]]
    block <- setNames(as.numeric(values), current_periods)
    if (is.null(existing)) {
      series_by_variable[[variable]] <- block
      next
    }
    combined_names <- unique(c(names(existing), names(block)))
    combined <- rep(NA_real_, length(combined_names))
    names(combined) <- combined_names
    combined[names(existing)] <- as.numeric(existing)
    combined[names(block)] <- as.numeric(block)
    series_by_variable[[variable]] <- combined
  }

  if (!length(series_by_variable)) {
    return(data.frame(stringsAsFactors = FALSE, check.names = FALSE))
  }

  all_periods <- sort(unique(unlist(lapply(series_by_variable, names))), decreasing = FALSE)
  frame <- data.frame(
    period = all_periods,
    stringsAsFactors = FALSE,
    check.names = FALSE
  )
  for (name in names(series_by_variable)) {
    values <- rep(NA_real_, length(all_periods))
    names(values) <- all_periods
    values[names(series_by_variable[[name]])] <- as.numeric(series_by_variable[[name]])
    frame[[name]] <- as.numeric(values)
  }
  frame
}

parse_fm_numeric_file <- function(path, block_name = NULL) {
  normalized_path <- normalizePath(path, winslash = "/", mustWork = TRUE)
  inferred_name <- if (is.null(block_name)) basename(normalized_path) else block_name
  parse_fm_numeric_text(readChar(normalized_path, nchars = file.info(normalized_path)$size, useBytes = TRUE), block_name = inferred_name)
}

parse_fmexog_text <- function(text) {
  lines <- strsplit(gsub("\r", "", text), "\n", fixed = TRUE)[[1]]
  rows <- list()

  current_window_start <- NULL
  current_window_end <- NULL
  current_window_size <- NULL
  in_changevar <- FALSE

  pending_scalar <- FALSE
  pending_variable <- NULL
  pending_method <- NULL
  pending_values <- numeric()
  pending_vector <- FALSE

  clean_token <- function(token) {
    raw <- trimws(token)
    raw <- gsub("^['\"]|['\"]$", "", raw)
    sub(";\\s*$", "", raw)
  }
  extract_numbers <- function(line) {
    matches <- gregexpr(
      "[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[Ee][+-]?\\d+)?",
      line,
      perl = TRUE
    )
    found <- regmatches(line, matches)[[1]]
    if (!length(found)) {
      return(numeric())
    }
    as.numeric(found)
  }
  is_numeric_token <- function(token) {
    grepl(
      "^[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[Ee][+-]?\\d+)?$",
      clean_token(token),
      perl = TRUE
    )
  }
  close_pending <- function() {
    if (is.null(pending_variable)) {
      return(invisible(NULL))
    }
    if (is.null(current_window_start) || is.null(current_window_end)) {
      stopf("CHANGEVAR instruction found outside an SMPL window")
    }
    if (!length(pending_values)) {
      stopf("No values parsed for instruction %s", pending_variable)
    }
    rows[[length(rows) + 1L]] <<- list(
      window_start = current_window_start,
      window_end = current_window_end,
      variable = pending_variable,
      method = pending_method %||% "",
      is_vector = isTRUE(pending_vector),
      n_values = length(pending_values),
      values = as.numeric(pending_values)
    )
    pending_scalar <<- FALSE
    pending_variable <<- NULL
    pending_method <<- NULL
    pending_values <<- numeric()
    pending_vector <<- FALSE
    invisible(NULL)
  }

  for (raw_line in lines) {
    line <- trimws(raw_line)
    if (!nzchar(line) || grepl("^\\s*@", line)) {
      next
    }
    line_has_semicolon <- grepl(";", line, fixed = TRUE)
    tokens <- strsplit(line, "\\s+", perl = TRUE)[[1]]
    tokens <- tokens[nzchar(tokens)]
    if (!length(tokens)) {
      next
    }

    first <- toupper(clean_token(tokens[[1]]))
    if (identical(first, "SMPL")) {
      if (length(tokens) < 3L) {
        stopf("SMPL statements require start and end periods")
      }
      close_pending()
      current_window_start <- clean_token(tokens[[2]])
      current_window_end <- clean_token(tokens[[3]])
      current_window_size <- length(generate_smpl_periods(current_window_start, current_window_end))
      in_changevar <- FALSE
      next
    }
    if (identical(first, "CHANGEVAR")) {
      if (is.null(current_window_start)) {
        stopf("CHANGEVAR encountered before SMPL statement")
      }
      close_pending()
      in_changevar <- TRUE
      next
    }
    if (identical(first, "RETURN")) {
      close_pending()
      break
    }
    if (!in_changevar) {
      next
    }

    if (pending_scalar || pending_vector) {
      if (!is_numeric_token(tokens[[1]])) {
        close_pending()
      } else {
        values <- extract_numbers(line)
        if (length(values)) {
          pending_values <- c(pending_values, values)
          if (pending_scalar && length(pending_values) > 1L) {
            pending_scalar <- FALSE
            pending_vector <- TRUE
          }
        }
        if (isTRUE(pending_vector) &&
          !is.null(current_window_size) &&
          length(pending_values) >= current_window_size) {
          if (length(pending_values) > current_window_size) {
            stopf(
              "Too many values parsed for vector instruction %s: expected %d, found %d",
              pending_variable,
              current_window_size,
              length(pending_values)
            )
          }
          close_pending()
          next
        }
        if (line_has_semicolon) {
          close_pending()
        }
        next
      }
    }

    variable <- clean_token(tokens[[1]])
    if (!nzchar(variable)) {
      next
    }
    method <- NULL
    is_vector <- FALSE
    value_text <- line

    if (length(tokens) >= 2L && !is_numeric_token(tokens[[2]])) {
      method <- clean_token(tokens[[2]])
      split_idx <- regexpr(tokens[[2]], line, fixed = TRUE)[[1]]
      value_text <- substr(line, split_idx + nchar(tokens[[2]]), nchar(line))
      if (line_has_semicolon) {
        is_vector <- TRUE
      }
    }
    if (!line_has_semicolon && length(tokens) == 1L) {
      is_vector <- FALSE
    } else if (line_has_semicolon && length(tokens) == 1L) {
      is_vector <- TRUE
    }

    extracted <- extract_numbers(value_text)
    if (isTRUE(is_vector)) {
      pending_scalar <- FALSE
      pending_vector <- TRUE
      pending_variable <- variable
      pending_method <- method
      pending_values <- extracted
      if (length(pending_values) && line_has_semicolon) {
        close_pending()
      }
      next
    }

    pending_scalar <- TRUE
    pending_vector <- FALSE
    pending_variable <- variable
    pending_method <- method
    pending_values <- extracted
    if (length(pending_values)) {
      close_pending()
    }
  }

  close_pending()
  if (!length(rows)) {
    empty <- data.frame(
      window_start = character(),
      window_end = character(),
      variable = character(),
      method = character(),
      is_vector = logical(),
      n_values = integer(),
      stringsAsFactors = FALSE
    )
    empty$values <- I(list())
    return(empty)
  }
  out <- data.frame(
    window_start = vapply(rows, `[[`, "", "window_start"),
    window_end = vapply(rows, `[[`, "", "window_end"),
    variable = vapply(rows, `[[`, "", "variable"),
    method = vapply(rows, `[[`, "", "method"),
    is_vector = vapply(rows, `[[`, FALSE, "is_vector"),
    n_values = vapply(rows, `[[`, integer(1), "n_values"),
    stringsAsFactors = FALSE
  )
  out$values <- I(lapply(rows, `[[`, "values"))
  out
}

parse_fmexog_file <- function(path) {
  normalized_path <- normalizePath(path, winslash = "/", mustWork = TRUE)
  parse_fmexog_text(readChar(normalized_path, nchars = file.info(normalized_path)$size, useBytes = TRUE))
}

merge_fm_numeric_frames <- function(base_frame, loaded_frame) {
  if (!nrow(loaded_frame)) {
    return(base_frame)
  }
  if (!nrow(base_frame)) {
    return(loaded_frame)
  }

  all_periods <- sort(unique(c(as.character(base_frame$period), as.character(loaded_frame$period))))
  merged <- data.frame(
    period = all_periods,
    stringsAsFactors = FALSE,
    check.names = FALSE
  )

  merge_one <- function(frame, name, carry_forward = FALSE) {
    values <- rep(NA_real_, length(all_periods))
    names(values) <- all_periods
    if (name %in% names(frame)) {
      values[as.character(frame$period)] <- as.numeric(frame[[name]])
    }
    if (carry_forward) {
      last_value <- NA_real_
      for (idx in seq_along(values)) {
        if (is.finite(values[[idx]])) {
          last_value <- values[[idx]]
        } else if (is.finite(last_value)) {
          values[[idx]] <- last_value
        }
      }
    }
    as.numeric(values)
  }

  base_columns <- setdiff(names(base_frame), "period")
  loaded_columns <- setdiff(names(loaded_frame), "period")
  for (name in base_columns) {
    merged[[name]] <- merge_one(base_frame, name, carry_forward = TRUE)
  }
  for (name in loaded_columns) {
    if (!(name %in% names(merged))) {
      merged[[name]] <- 0
    }
    incoming <- merge_one(loaded_frame, name, carry_forward = FALSE)
    base_missing <- is.na(merged[[name]])
    merged[[name]][base_missing] <- 0
    have_incoming <- !is.na(incoming)
    merged[[name]][have_incoming] <- incoming[have_incoming]
  }
  merged
}

apply_fmexog_rows <- function(base_frame, rows) {
  if (!nrow(rows)) {
    return(base_frame)
  }
  working <- base_frame
  working$period <- as.character(working$period)
  index_positions <- seq_along(working$period)
  names(index_positions) <- working$period

  first_finite <- function(values) {
    for (value in values) {
      if (is.finite(value)) {
        return(as.numeric(value))
      }
    }
    0
  }

  for (row_idx in seq_len(nrow(rows))) {
    row <- rows[row_idx, , drop = FALSE]
    variable <- trimws(as.character(row$variable[[1]]))
    if (!nzchar(variable)) {
      next
    }
    method <- toupper(trimws(as.character(row$method[[1]])))
    if (!nzchar(method)) {
      method <- "CHGSAMEPCT"
    }
    values <- as.numeric(unlist(row$values[[1]]))
    if (!length(values)) {
      next
    }

    periods <- generate_smpl_periods(row$window_start[[1]], row$window_end[[1]])
    periods <- periods[periods %in% working$period]
    if (!length(periods)) {
      next
    }

    if (!(variable %in% names(working))) {
      working[[variable]] <- NA_real_
    }
    is_vector <- isTRUE(row$is_vector[[1]])

    if (is_vector) {
      if (length(values) == length(periods)) {
        padded <- values
      } else if (length(values) > 1L) {
        padded <- values[seq_len(min(length(values), length(periods)))]
        if (length(padded) < length(periods)) {
          padded <- c(padded, rep(tail(padded, 1L), length(periods) - length(padded)))
        }
      } else {
        padded <- rep(values[[1]], length(periods))
      }

      if (method %in% c("ADDDIFABS", "ADDDIFPCT")) {
        existing <- as.numeric(working[match(periods, working$period), variable])
        existing[!is.finite(existing)] <- 0
        updated <- if (identical(method, "ADDDIFABS")) {
          existing + padded
        } else {
          existing * (1 + padded)
        }
        working[match(periods, working$period), variable] <- updated
        next
      }

      working[match(periods, working$period), variable] <- padded
      next
    }

    scalar <- as.numeric(values[[1]])
    if (identical(method, "SAMEVALUE")) {
      working[match(periods, working$period), variable] <- scalar
      next
    }

    if (method %in% c("CHGSAMEABS", "CHGSAMEPCT")) {
      series <- as.numeric(working[[variable]])
      names(series) <- working$period
      for (period in periods) {
        position <- index_positions[[period]]
        prev_value <- NA_real_
        if (!is.null(position) && position > 1L) {
          prev_period <- working$period[[position - 1L]]
          prev_value <- as.numeric(series[[prev_period]])
        }
        current_value <- if (period %in% names(series)) as.numeric(series[[period]]) else NA_real_
        baseline <- first_finite(c(prev_value, current_value, 0))
        updated <- if (identical(method, "CHGSAMEABS")) baseline + scalar else baseline * (1 + scalar)
        working[working$period == period, variable] <- updated
        series[[period]] <- updated
      }
      next
    }

    if (method %in% c("ADDDIFABS", "ADDDIFPCT")) {
      series <- as.numeric(working[[variable]])
      names(series) <- working$period
      for (period in periods) {
        current_value <- if (period %in% names(series)) as.numeric(series[[period]]) else NA_real_
        baseline <- first_finite(c(current_value, 0))
        updated <- if (identical(method, "ADDDIFABS")) baseline + scalar else baseline * (1 + scalar)
        working[working$period == period, variable] <- updated
        series[[period]] <- updated
      }
      next
    }

    working[match(periods, working$period), variable] <- scalar
  }

  working
}
