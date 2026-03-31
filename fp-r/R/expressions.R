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
  list(
    original_text = as.character(text),
    normalized_text = normalized_text,
    r_expression = r_expression,
    parsed_expression = parse(text = r_expression)[[1]],
    tokens = tokens,
    references = references
  )
}

evaluate_compiled_expression <- function(
  compiled,
  state,
  period_index,
  strict = TRUE,
  series_overrides = NULL,
  lag_only_overrides = FALSE
) {
  expr <- compiled$parsed_expression %||% parse(text = compiled$r_expression)[[1]]
  coef_values <- state$coef_values %||% state$coefficients %||% list()
  normalize_fp_series_value <- function(value) {
    numeric_value <- as.numeric(value)
    if (!is.finite(numeric_value)) {
      return(NA_real_)
    }
    if (abs(numeric_value + 99.0) <= 1e-12) {
      return(NA_real_)
    }
    numeric_value
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
      stopf("Unknown series referenced in expression: %s", source_name)
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
    value
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
    return(0.0)
  }
  eval(
    expr,
    envir = list(.fp_value = .fp_value, .fp_coef = .fp_coef),
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
