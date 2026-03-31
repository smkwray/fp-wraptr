clean_fp_token <- function(token) {
  clean_fp_filename(token)
}

new_eq_term <- function(variable, coefficient, lag = 0L, index = NA_integer_) {
  list(
    variable = as.character(variable),
    coefficient = as.numeric(coefficient),
    lag = as.integer(lag),
    index = as.integer(index)
  )
}

new_eq_spec <- function(lhs, terms = list(), equation_number = NA_integer_) {
  list(
    lhs = as.character(lhs),
    terms = terms,
    equation_number = as.integer(equation_number)
  )
}

parse_fp_reference_token <- function(token) {
  cleaned <- clean_fp_token(token)
  if (!nzchar(cleaned)) {
    return(NULL)
  }
  matches <- regexec(
    "^([A-Za-z_][A-Za-z0-9_]*)(?:\\(([+-]?\\d+)\\))?$",
    cleaned,
    perl = TRUE
  )
  parts <- regmatches(cleaned, matches)[[1]]
  if (length(parts) != 3L) {
    return(NULL)
  }
  lag_text <- trimws(as.character(parts[[3]] %||% ""))
  list(
    token = cleaned,
    name = toupper(parts[[2]]),
    lag = if (nzchar(lag_text)) as.integer(lag_text) else 0L
  )
}

tokens_to_reference_frame <- function(tokens) {
  if (is.null(tokens) || !length(tokens)) {
    return(data.frame(token = character(), name = character(), lag = integer(), stringsAsFactors = FALSE))
  }
  parsed <- Filter(Negate(is.null), lapply(tokens, parse_fp_reference_token))
  if (!length(parsed)) {
    return(data.frame(token = character(), name = character(), lag = integer(), stringsAsFactors = FALSE))
  }
  data.frame(
    token = vapply(parsed, `[[`, character(1), "token"),
    name = vapply(parsed, `[[`, character(1), "name"),
    lag = vapply(parsed, `[[`, integer(1), "lag"),
    stringsAsFactors = FALSE
  )
}

collapse_unique_values <- function(values) {
  cleaned <- unique(as.character(values %||% character()))
  cleaned <- cleaned[nzchar(cleaned)]
  paste(cleaned, collapse = " ")
}

parse_eq_statement <- function(statement) {
  text <- trimws(as.character(statement %||% ""))
  if (!nzchar(text)) {
    return(NULL)
  }
  tokens <- strsplit(gsub("[\r\n]", " ", text), "\\s+", perl = TRUE)[[1]]
  tokens <- tokens[nzchar(tokens)]
  if (length(tokens) < 3L || !identical(toupper(tokens[[1]]), "EQ")) {
    return(NULL)
  }
  eq_number <- suppressWarnings(as.integer(clean_fp_token(tokens[[2]])))
  if (!is.finite(eq_number)) {
    return(NULL)
  }
  target <- clean_fp_token(tokens[[3]])
  is_fsr <- identical(toupper(target), "FSR")
  rhs_tokens <- if (length(tokens) > 3L) tokens[4:length(tokens)] else character()
  rho_order <- 0L
  filtered_rhs <- character()
  for (token in rhs_tokens) {
    cleaned <- clean_fp_token(token)
    if (grepl("^RHO\\s*=\\s*\\d+$", toupper(cleaned), perl = TRUE)) {
      rho_order <- as.integer(sub("^RHO\\s*=\\s*", "", toupper(cleaned), perl = TRUE))
      next
    }
    filtered_rhs <- c(filtered_rhs, cleaned)
  }
  list(
    equation_number = as.integer(eq_number),
    target = target,
    rhs_tokens = filtered_rhs,
    references = tokens_to_reference_frame(filtered_rhs),
    rho_order = as.integer(rho_order),
    is_fsr = is_fsr,
    raw = text
  )
}

reference_token_from_name_lag <- function(name, lag = 0L) {
  normalized_name <- toupper(clean_fp_token(name))
  normalized_lag <- as.integer(lag %||% 0L)
  if (!nzchar(normalized_name)) {
    return("")
  }
  if (normalized_lag == 0L) {
    return(normalized_name)
  }
  token <- sprintf("%s(%+d)", normalized_name, normalized_lag)
  gsub("\\+", "", token, fixed = TRUE)
}

eq_term_to_reference_token <- function(term) {
  reference_token_from_name_lag(
    term$variable %||% term$name %||% term$token %||% "",
    term$lag %||% 0L
  )
}

unique_reference_tokens <- function(tokens) {
  cleaned <- as.character(tokens %||% character())
  cleaned <- cleaned[nzchar(cleaned)]
  if (!length(cleaned)) {
    return(character())
  }
  cleaned[!duplicated(cleaned)]
}

parse_modeq_term_list <- function(source) {
  if (length(source) == 1L) {
    tokens <- strsplit(gsub("[\r\n]", " ", as.character(source %||% "")), "\\s+", perl = TRUE)[[1]]
  } else {
    tokens <- as.character(source %||% character())
  }
  tokens <- tokens[nzchar(tokens)]
  add_terms <- list()
  sub_terms <- list()
  mode <- "add"
  for (token in tokens) {
    cleaned <- clean_fp_token(token)
    if (!nzchar(cleaned)) {
      next
    }
    if (identical(cleaned, "+")) {
      mode <- "add"
      next
    }
    if (identical(cleaned, "-")) {
      mode <- "sub"
      next
    }
    if (grepl("^RHO\\s*=\\s*\\d+$", toupper(cleaned), perl = TRUE)) {
      next
    }
    parsed <- parse_fp_reference_token(cleaned)
    if (is.null(parsed)) {
      next
    }
    entry <- list(
      token = as.character(parsed$token),
      name = as.character(parsed$name),
      lag = as.integer(parsed$lag)
    )
    if (identical(mode, "sub")) {
      sub_terms[[length(sub_terms) + 1L]] <- entry
    } else {
      add_terms[[length(add_terms) + 1L]] <- entry
    }
  }
  list(add_terms = add_terms, sub_terms = sub_terms)
}

parse_modeq_statement <- function(statement) {
  text <- trimws(as.character(statement %||% ""))
  if (!nzchar(text)) {
    return(NULL)
  }
  tokens <- strsplit(gsub("[\r\n]", " ", text), "\\s+", perl = TRUE)[[1]]
  tokens <- tokens[nzchar(tokens)]
  if (length(tokens) < 2L || !identical(toupper(tokens[[1]]), "MODEQ")) {
    return(NULL)
  }
  eq_number <- suppressWarnings(as.integer(clean_fp_token(tokens[[2]])))
  if (!is.finite(eq_number)) {
    return(NULL)
  }
  body_tokens <- if (length(tokens) > 2L) unname(vapply(tokens[3:length(tokens)], clean_fp_token, character(1))) else character()
  fsr_start <- match("FSR", toupper(body_tokens))
  if (is.na(fsr_start)) {
    modeq_tokens <- body_tokens
    fsr_tokens <- character()
  } else {
    modeq_tokens <- if (fsr_start > 1L) body_tokens[seq_len(fsr_start - 1L)] else character()
    fsr_tokens <- body_tokens[seq.int(fsr_start + 1L, length(body_tokens))]
  }
  parsed_modeq_terms <- parse_modeq_term_list(modeq_tokens)
  parsed_fsr_terms <- parse_modeq_term_list(fsr_tokens)
  modeq_add_tokens <- unique_reference_tokens(vapply(parsed_modeq_terms$add_terms, `[[`, character(1), "token"))
  modeq_sub_tokens <- unique_reference_tokens(vapply(parsed_modeq_terms$sub_terms, `[[`, character(1), "token"))
  fsr_add_tokens <- unique_reference_tokens(vapply(parsed_fsr_terms$add_terms, `[[`, character(1), "token"))
  fsr_sub_tokens <- unique_reference_tokens(vapply(parsed_fsr_terms$sub_terms, `[[`, character(1), "token"))
  list(
    equation_number = as.integer(eq_number),
    tokens = modeq_add_tokens,
    sub_tokens = modeq_sub_tokens,
    fsr_tokens = fsr_add_tokens,
    fsr_sub_tokens = fsr_sub_tokens,
    add_terms = parsed_modeq_terms$add_terms,
    sub_terms = parsed_modeq_terms$sub_terms,
    fsr_add_terms = parsed_fsr_terms$add_terms,
    fsr_sub_terms = parsed_fsr_terms$sub_terms,
    references = tokens_to_reference_frame(modeq_add_tokens),
    sub_references = tokens_to_reference_frame(modeq_sub_tokens),
    fsr_references = tokens_to_reference_frame(fsr_add_tokens),
    fsr_sub_references = tokens_to_reference_frame(fsr_sub_tokens),
    raw = text
  )
}

parse_reduced_fmout_coefficients <- function(path) {
  normalized_path <- normalizePath(path, winslash = "/", mustWork = TRUE)
  parse_eq_specs_from_fmout_text(paste(readLines(normalized_path, warn = FALSE, encoding = "UTF-8"), collapse = "\n"))
}

parse_eq_specs_from_fmout_text <- function(text) {
  lines <- strsplit(gsub("\r", "", text), "\n", fixed = TRUE)[[1]]
  current_lhs <- NULL
  specs <- list()
  header_re <- "^[[:space:]]*(?:([A-Za-z_][A-Za-z0-9_]*)[[:space:]]+EQUATION[[:space:]]+(\\d+)|EQUATION[[:space:]]+(\\d+)[[:space:]]+([A-Za-z_][A-Za-z0-9_]*))"
  term_re <- "^[[:space:]]*([+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[Ee][+-]?\\d+)?)[[:space:]]+(\\d+)[[:space:]]+([A-Za-z_][A-Za-z0-9_]*)(?:[[:space:]]*\\([[:space:]]*([+-]?\\d+)[[:space:]]*\\))?"
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

  for (raw in lines) {
    line <- trimws(raw)
    if (!nzchar(line)) {
      next
    }
    eq_match <- regexec(header_re, raw, perl = TRUE, ignore.case = TRUE)
    eq_parts <- regmatches(raw, eq_match)[[1]]
    if (length(eq_parts) >= 5L && any(nzchar(eq_parts[-1L]))) {
      lhs <- eq_parts[[2]]
      number <- eq_parts[[3]]
      if (!nzchar(lhs %||% "")) {
        lhs <- eq_parts[[5]]
        number <- eq_parts[[4]]
      }
      current_lhs <- toupper(clean_fp_token(lhs))
      specs[[current_lhs]] <- new_eq_spec(
        lhs = current_lhs,
        terms = specs[[current_lhs]]$terms %||% list(),
        equation_number = as.integer(number)
      )
      next
    }

    term_match <- regexec(term_re, raw, perl = TRUE, ignore.case = TRUE)
    term_parts <- regmatches(raw, term_match)[[1]]
    if (length(term_parts) == 5L && nzchar(current_lhs %||% "")) {
      spec <- specs[[current_lhs]]
      lag_text <- trimws(as.character(term_parts[[5]] %||% ""))
      spec$terms[[length(spec$terms) + 1L]] <- new_eq_term(
        variable = toupper(clean_fp_token(term_parts[[4]])),
        coefficient = as.numeric(term_parts[[2]]),
        lag = if (nzchar(lag_text)) as.integer(lag_text) else 0L,
        index = as.integer(term_parts[[3]])
      )
      specs[[current_lhs]] <- spec
      next
    }

    coef_match <- regexec("^COEFS?\\s+(.+)$", line, perl = TRUE, ignore.case = TRUE)
    coef_parts <- regmatches(line, coef_match)[[1]]
    if (length(coef_parts) == 2L && nzchar(current_lhs %||% "")) {
      current_spec <- specs[[current_lhs]]
      if (length(current_spec$terms)) {
        next
      }
      coefficients <- extract_numbers(coef_parts[[2]])
      if (!length(coefficients)) {
        next
      }
      for (idx in seq_along(coefficients)) {
        current_spec$terms[[length(current_spec$terms) + 1L]] <- new_eq_term(
          variable = if (idx == 1L) "C" else sprintf("TERM%d", idx),
          coefficient = coefficients[[idx]],
          lag = 0L,
          index = idx
        )
      }
      specs[[current_lhs]] <- current_spec
      next
    }

    if (nzchar(current_lhs %||% "")) {
      current_lhs <- NULL
    }
  }

  specs
}

build_coef_table <- function(specs) {
  values <- numeric()
  names(values) <- character()
  if (is.null(specs) || !length(specs)) {
    return(values)
  }
  for (spec in specs) {
    eq_number <- as.integer(spec$equation_number %||% NA_integer_)
    if (!is.finite(eq_number)) {
      next
    }
    for (term in spec$terms %||% list()) {
      term_index <- as.integer(term$index %||% NA_integer_)
      if (!is.finite(term_index)) {
        next
      }
      key <- sprintf("%d,%d", term_index, eq_number)
      if (!(key %in% names(values))) {
        values[[key]] <- as.numeric(term$coefficient)
      }
    }
  }
  values
}

format_eq_coefficient <- function(value) {
  format(as.numeric(value), scientific = FALSE, digits = 16, trim = TRUE)
}

build_eq_expression <- function(rhs_tokens, coefficients) {
  if (!length(rhs_tokens)) {
    return(NULL)
  }
  if (length(coefficients) != length(rhs_tokens)) {
    stopf(
      "EQ coefficient count mismatch: expected %d, found %d",
      length(rhs_tokens),
      length(coefficients)
    )
  }
  terms <- character()
  for (idx in seq_along(rhs_tokens)) {
    token <- as.character(rhs_tokens[[idx]])
    coefficient <- as.numeric(coefficients[[idx]])
    if (!is.finite(coefficient)) {
      stopf("Non-finite coefficient encountered for token %s", token)
    }
    if (identical(toupper(token), "C")) {
      terms <- c(terms, format_eq_coefficient(coefficient))
      next
    }
    terms <- c(terms, sprintf("%s * %s", format_eq_coefficient(coefficient), token))
  }
  paste(terms, collapse = " + ")
}

build_eq_expression_from_terms <- function(terms, rhs_tokens = character()) {
  if (!length(terms)) {
    return(NULL)
  }
  if (length(rhs_tokens)) {
    return(build_eq_expression(
      rhs_tokens,
      vapply(terms[seq_len(min(length(rhs_tokens), length(terms)))], function(item) as.numeric(item$coefficient), numeric(1))
    ))
  }
  pieces <- character()
  for (term in terms) {
    variable <- as.character(term$variable)
    coefficient <- as.numeric(term$coefficient)
    lag <- as.integer(term$lag %||% 0L)
    if (identical(toupper(variable), "C")) {
      pieces <- c(pieces, format_eq_coefficient(coefficient))
      next
    }
    token <- if (lag == 0L) variable else sprintf("%s(%+d)", variable, lag)
    token <- gsub("\\+","", token, fixed = TRUE)
    pieces <- c(pieces, sprintf("%s * %s", format_eq_coefficient(coefficient), token))
  }
  paste(pieces, collapse = " + ")
}

apply_modeq_term_key_update <- function(existing_tokens, add_terms = list(), sub_terms = list()) {
  tokens <- unique_reference_tokens(existing_tokens)
  sub_tokens <- unique_reference_tokens(vapply(sub_terms, `[[`, character(1), "token"))
  if (length(sub_tokens)) {
    tokens <- tokens[!tokens %in% sub_tokens]
  }
  add_tokens <- unique_reference_tokens(vapply(add_terms, `[[`, character(1), "token"))
  for (token in add_tokens) {
    if (!(token %in% tokens)) {
      tokens <- c(tokens, token)
    }
  }
  tokens
}

summarize_reference_tokens <- function(tokens) {
  unique_tokens <- unique_reference_tokens(tokens)
  references <- tokens_to_reference_frame(unique_tokens)
  list(
    tokens = unique_tokens,
    token_text = paste(unique_tokens, collapse = " "),
    token_count = length(unique_tokens),
    reference_names = collapse_unique_values(references$name),
    name_count = length(unique(references$name %||% character())),
    max_lag = if (nrow(references)) max(abs(references$lag)) else 0L,
    has_lags = any((references$lag %||% integer()) != 0L)
  )
}

build_reduced_eq_spec <- function(parsed_eq, terms, setupsolve = list()) {
  reduced_rho <- extract_reduced_rho_terms(parsed_eq, terms)
  structural_terms <- terms[reduced_rho$keep_indexes]
  structural_tokens <- if (length(parsed_eq$rhs_tokens) == length(terms)) {
    parsed_eq$rhs_tokens[reduced_rho$keep_indexes]
  } else if (length(structural_terms)) {
    vapply(structural_terms, eq_term_to_reference_token, character(1))
  } else {
    character()
  }
  expression <- if (length(structural_terms)) {
    build_eq_expression_from_terms(structural_terms, rhs_tokens = structural_tokens)
  } else {
    "0"
  }
  use_resid_ar1 <- isTRUE(setupsolve$eq_rho_resid_ar1) &&
    parsed_eq$rho_order == 1L &&
    nrow(reduced_rho$rho_terms) == 1L &&
    as.integer(reduced_rho$rho_terms$order[[1]]) == 1L
  spec <- list(
    target = parsed_eq$target,
    expression = expression,
    equation_number = parsed_eq$equation_number,
    rhs_tokens = structural_tokens,
    coefficients = vapply(terms, function(item) as.numeric(item$coefficient), numeric(1)),
    terms = terms,
    active_fsr_terms = character()
  )
  lag_suffix <- as.character(setupsolve$eq_target_lag_suffix %||% "")
  if (nzchar(lag_suffix)) {
    spec$target_lag_source <- sprintf("%s%s", parsed_eq$target, lag_suffix)
  }
  if (use_resid_ar1) {
    source_suffix <- as.character(setupsolve$eq_rho_resid_source_suffix %||% "")
    spec$resid_ar1 <- list(
      rho_lag1 = as.numeric(reduced_rho$rho_terms$coefficient[[1]]),
      source_series = if (nzchar(source_suffix)) sprintf("%s%s", parsed_eq$target, source_suffix) else "",
      update_source = as.character(setupsolve$eq_rho_resid_update_source %||% "structural"),
      carry_lag = 0L,
      carry_damp = as.numeric(setupsolve$eq_rho_resid_carry_damp %||% 1.0),
      carry_damp_mode = as.character(setupsolve$eq_rho_resid_carry_damp_mode %||% "term"),
      carry_multipass = FALSE
    )
  } else {
    spec$rho_terms <- reduced_rho$rho_terms
  }
  spec
}

extract_reduced_rho_terms <- function(parsed_eq, terms) {
  if (is.null(parsed_eq) || as.integer(parsed_eq$rho_order %||% 0L) <= 0L || !length(terms)) {
    return(list(
      rho_terms = data.frame(order = integer(), coefficient = numeric(), stringsAsFactors = FALSE),
      keep_indexes = seq_along(terms)
    ))
  }

  target <- toupper(as.character(parsed_eq$target))
  rho_indexes <- which(vapply(terms, function(term) {
    variable <- toupper(as.character(term$variable %||% ""))
    lag <- as.integer(term$lag %||% 0L)
    identical(variable, "RHO") && lag < 0L
  }, logical(1)))
  if (!length(rho_indexes)) {
    rho_indexes <- which(vapply(terms, function(term) {
      identical(toupper(as.character(term$variable %||% "")), target) &&
        as.integer(term$lag %||% 0L) < 0L
    }, logical(1)))
  }

  if (!length(rho_indexes)) {
    return(list(
      rho_terms = data.frame(order = integer(), coefficient = numeric(), stringsAsFactors = FALSE),
      keep_indexes = seq_along(terms)
    ))
  }

  rho_orders <- vapply(rho_indexes, function(index) abs(as.integer(terms[[index]]$lag %||% 0L)), integer(1))
  rho_frame <- data.frame(
    order = rho_orders,
    coefficient = vapply(rho_indexes, function(index) as.numeric(terms[[index]]$coefficient), numeric(1)),
    stringsAsFactors = FALSE
  )
  rho_frame <- rho_frame[rho_frame$order > 0L & rho_frame$order <= as.integer(parsed_eq$rho_order), , drop = FALSE]
  if (!nrow(rho_frame)) {
    return(list(
      rho_terms = data.frame(order = integer(), coefficient = numeric(), stringsAsFactors = FALSE),
      keep_indexes = seq_along(terms)
    ))
  }

  rho_frame <- stats::aggregate(coefficient ~ order, data = rho_frame, FUN = sum)
  rho_frame <- rho_frame[order(rho_frame$order), , drop = FALSE]
  keep_indexes <- seq_along(terms)
  keep_indexes <- keep_indexes[!keep_indexes %in% rho_indexes[rho_orders <= as.integer(parsed_eq$rho_order)]]

  list(
    rho_terms = rho_frame,
    keep_indexes = keep_indexes
  )
}

build_reduced_eq_specs <- function(statements, fmout_path = NULL, setupsolve = list()) {
  fmout_specs <- if (!is.null(fmout_path) && nzchar(fmout_path) && file.exists(fmout_path)) {
    parse_reduced_fmout_coefficients(fmout_path)
  } else {
    list()
  }

  eq_rows <- list()
  eq_fsr_rows <- list()
  modeq_rows <- list()
  eq_statements <- list()
  modeq_statements <- list()
  specs <- list()
  for (statement in statements) {
    command <- toupper(as.character(statement$command %||% ""))
    raw <- statement$raw %||% ""
    if (identical(command, "EQ")) {
      parsed_eq <- parse_eq_statement(raw)
      if (is.null(parsed_eq)) {
        next
      }
      if (isTRUE(parsed_eq$is_fsr)) {
        eq_fsr_rows[[length(eq_fsr_rows) + 1L]] <- list(
          equation_number = parsed_eq$equation_number,
          token_count = length(parsed_eq$rhs_tokens),
          tokens = paste(parsed_eq$rhs_tokens, collapse = " "),
          name_count = length(unique(parsed_eq$references$name %||% character())),
          reference_names = collapse_unique_values(parsed_eq$references$name),
          max_lag = if (nrow(parsed_eq$references)) max(abs(parsed_eq$references$lag)) else 0L,
          has_lags = any((parsed_eq$references$lag %||% integer()) != 0L)
        )
        next
      }
      eq_statements[[length(eq_statements) + 1L]] <- parsed_eq
      key <- toupper(parsed_eq$target)
      spec_from_fmout <- fmout_specs[[key]] %||% NULL
      eq_rows[[length(eq_rows) + 1L]] <- list(
        equation_number = parsed_eq$equation_number,
        target = parsed_eq$target,
        rho_order = parsed_eq$rho_order,
        rhs_count = length(parsed_eq$rhs_tokens)
      )
      next
    }
    if (identical(command, "MODEQ")) {
      parsed_modeq <- parse_modeq_statement(raw)
      if (is.null(parsed_modeq)) {
        next
      }
      modeq_statements[[length(modeq_statements) + 1L]] <- parsed_modeq
    }
  }

  parsed_eq_by_number <- list()
  spec_index_by_number <- integer()
  for (parsed_eq in eq_statements) {
    key <- toupper(parsed_eq$target)
    spec_from_fmout <- fmout_specs[[key]] %||% NULL
    equation_key <- as.character(parsed_eq$equation_number)
    parsed_eq_by_number[[equation_key]] <- parsed_eq
    if (is.null(spec_from_fmout) || !length(spec_from_fmout$terms %||% list())) {
      next
    }
    spec <- build_reduced_eq_spec(parsed_eq, spec_from_fmout$terms, setupsolve = setupsolve)
    specs[[length(specs) + 1L]] <- spec
    spec_index_by_number[[equation_key]] <- length(specs)
  }

  active_fsr_tokens_by_equation <- list()
  for (parsed_modeq in modeq_statements) {
    equation_key <- as.character(parsed_modeq$equation_number)
    active_fsr_tokens <- apply_modeq_term_key_update(
      active_fsr_tokens_by_equation[[equation_key]] %||% character(),
      add_terms = parsed_modeq$fsr_add_terms %||% list(),
      sub_terms = parsed_modeq$fsr_sub_terms %||% list()
    )
    active_fsr_tokens_by_equation[[equation_key]] <- active_fsr_tokens
    active_fsr_summary <- summarize_reference_tokens(active_fsr_tokens)
    modeq_rows[[length(modeq_rows) + 1L]] <- list(
      equation_number = parsed_modeq$equation_number,
      token_count = length(parsed_modeq$tokens),
      tokens = paste(parsed_modeq$tokens, collapse = " "),
      sub_token_count = length(parsed_modeq$sub_tokens %||% character()),
      sub_tokens = paste(parsed_modeq$sub_tokens %||% character(), collapse = " "),
      modeq_name_count = length(unique(c(
        parsed_modeq$references$name %||% character(),
        parsed_modeq$sub_references$name %||% character()
      ))),
      modeq_reference_names = collapse_unique_values(c(
        parsed_modeq$references$name %||% character(),
        parsed_modeq$sub_references$name %||% character()
      )),
      fsr_token_count = length(parsed_modeq$fsr_tokens %||% character()),
      fsr_tokens = paste(parsed_modeq$fsr_tokens %||% character(), collapse = " "),
      fsr_sub_token_count = length(parsed_modeq$fsr_sub_tokens %||% character()),
      fsr_sub_tokens = paste(parsed_modeq$fsr_sub_tokens %||% character(), collapse = " "),
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
      fsr_name_count = length(unique(c(
        parsed_modeq$fsr_references$name %||% character(),
        parsed_modeq$fsr_sub_references$name %||% character()
      ))),
      fsr_reference_names = collapse_unique_values(c(
        parsed_modeq$fsr_references$name %||% character(),
        parsed_modeq$fsr_sub_references$name %||% character()
      )),
      active_fsr_token_count = active_fsr_summary$token_count,
      active_fsr_tokens = active_fsr_summary$token_text,
      active_fsr_name_count = active_fsr_summary$name_count,
      active_fsr_reference_names = active_fsr_summary$reference_names,
      active_max_fsr_lag = as.integer(active_fsr_summary$max_lag),
      active_fsr_has_lags = isTRUE(active_fsr_summary$has_lags)
    )
    if (equation_key %in% names(spec_index_by_number)) {
      spec_index <- as.integer(spec_index_by_number[[equation_key]])
      specs[[spec_index]]$active_fsr_terms <- active_fsr_tokens
    }
  }

  equations_frame <- if (!length(eq_rows)) {
    data.frame(
      equation_number = integer(),
      target = character(),
      rho_order = integer(),
      rhs_count = integer(),
      stringsAsFactors = FALSE
    )
  } else {
    data.frame(
      equation_number = vapply(eq_rows, `[[`, integer(1), "equation_number"),
      target = vapply(eq_rows, `[[`, character(1), "target"),
      rho_order = vapply(eq_rows, `[[`, integer(1), "rho_order"),
      rhs_count = vapply(eq_rows, `[[`, integer(1), "rhs_count"),
      stringsAsFactors = FALSE
    )
  }
  eq_fsr_frame <- if (!length(eq_fsr_rows)) {
    data.frame(
      equation_number = integer(),
      token_count = integer(),
      tokens = character(),
      name_count = integer(),
      reference_names = character(),
      max_lag = integer(),
      has_lags = logical(),
      stringsAsFactors = FALSE
    )
  } else {
    data.frame(
      equation_number = vapply(eq_fsr_rows, `[[`, integer(1), "equation_number"),
      token_count = vapply(eq_fsr_rows, `[[`, integer(1), "token_count"),
      tokens = vapply(eq_fsr_rows, `[[`, character(1), "tokens"),
      name_count = vapply(eq_fsr_rows, `[[`, integer(1), "name_count"),
      reference_names = vapply(eq_fsr_rows, `[[`, character(1), "reference_names"),
      max_lag = vapply(eq_fsr_rows, `[[`, integer(1), "max_lag"),
      has_lags = vapply(eq_fsr_rows, `[[`, logical(1), "has_lags"),
      stringsAsFactors = FALSE
    )
  }
  modeq_frame <- if (!length(modeq_rows)) {
    data.frame(
      equation_number = integer(),
      token_count = integer(),
      tokens = character(),
      sub_token_count = integer(),
      sub_tokens = character(),
      modeq_name_count = integer(),
      modeq_reference_names = character(),
      fsr_token_count = integer(),
      fsr_tokens = character(),
      fsr_sub_token_count = integer(),
      fsr_sub_tokens = character(),
      max_fsr_lag = integer(),
      fsr_has_lags = logical(),
      fsr_name_count = integer(),
      fsr_reference_names = character(),
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
      token_count = vapply(modeq_rows, `[[`, integer(1), "token_count"),
      tokens = vapply(modeq_rows, `[[`, character(1), "tokens"),
      sub_token_count = vapply(modeq_rows, `[[`, integer(1), "sub_token_count"),
      sub_tokens = vapply(modeq_rows, `[[`, character(1), "sub_tokens"),
      modeq_name_count = vapply(modeq_rows, `[[`, integer(1), "modeq_name_count"),
      modeq_reference_names = vapply(modeq_rows, `[[`, character(1), "modeq_reference_names"),
      fsr_token_count = vapply(modeq_rows, `[[`, integer(1), "fsr_token_count"),
      fsr_tokens = vapply(modeq_rows, `[[`, character(1), "fsr_tokens"),
      fsr_sub_token_count = vapply(modeq_rows, `[[`, integer(1), "fsr_sub_token_count"),
      fsr_sub_tokens = vapply(modeq_rows, `[[`, character(1), "fsr_sub_tokens"),
      max_fsr_lag = vapply(modeq_rows, `[[`, integer(1), "max_fsr_lag"),
      fsr_has_lags = vapply(modeq_rows, `[[`, logical(1), "fsr_has_lags"),
      fsr_name_count = vapply(modeq_rows, `[[`, integer(1), "fsr_name_count"),
      fsr_reference_names = vapply(modeq_rows, `[[`, character(1), "fsr_reference_names"),
      active_fsr_token_count = vapply(modeq_rows, `[[`, integer(1), "active_fsr_token_count"),
      active_fsr_tokens = vapply(modeq_rows, `[[`, character(1), "active_fsr_tokens"),
      active_fsr_name_count = vapply(modeq_rows, `[[`, integer(1), "active_fsr_name_count"),
      active_fsr_reference_names = vapply(modeq_rows, `[[`, character(1), "active_fsr_reference_names"),
      active_max_fsr_lag = vapply(modeq_rows, `[[`, integer(1), "active_max_fsr_lag"),
      active_fsr_has_lags = vapply(modeq_rows, `[[`, logical(1), "active_fsr_has_lags"),
      stringsAsFactors = FALSE
    )
  }

  modeq_summary <- if (!nrow(modeq_frame)) {
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
    summary_rows <- list()
    for (equation_number in unique(as.integer(modeq_frame$equation_number))) {
      subset_rows <- modeq_frame[modeq_frame$equation_number == equation_number, , drop = FALSE]
      modeq_names <- unique(unlist(strsplit(paste(subset_rows$modeq_reference_names, collapse = " "), "\\s+", perl = TRUE)))
      modeq_names <- modeq_names[nzchar(modeq_names)]
      fsr_names <- unique(unlist(strsplit(paste(subset_rows$fsr_reference_names, collapse = " "), "\\s+", perl = TRUE)))
      fsr_names <- fsr_names[nzchar(fsr_names)]
      active_summary <- summarize_reference_tokens(active_fsr_tokens_by_equation[[as.character(equation_number)]] %||% character())
      summary_rows[[length(summary_rows) + 1L]] <- list(
        equation_number = as.integer(equation_number),
        modeq_name_count = length(modeq_names),
        fsr_name_count = length(fsr_names),
        shared_name_count = length(intersect(modeq_names, fsr_names)),
        max_fsr_lag = if (nrow(subset_rows)) max(as.integer(subset_rows$max_fsr_lag)) else 0L,
        fsr_has_lags = any(as.logical(subset_rows$fsr_has_lags)),
        active_fsr_token_count = active_summary$token_count,
        active_fsr_tokens = active_summary$token_text,
        active_fsr_name_count = active_summary$name_count,
        active_fsr_reference_names = active_summary$reference_names,
        active_max_fsr_lag = as.integer(active_summary$max_lag),
        active_fsr_has_lags = isTRUE(active_summary$has_lags)
      )
    }
    data.frame(
      equation_number = vapply(summary_rows, `[[`, integer(1), "equation_number"),
      modeq_name_count = vapply(summary_rows, `[[`, integer(1), "modeq_name_count"),
      fsr_name_count = vapply(summary_rows, `[[`, integer(1), "fsr_name_count"),
      shared_name_count = vapply(summary_rows, `[[`, integer(1), "shared_name_count"),
      max_fsr_lag = vapply(summary_rows, `[[`, integer(1), "max_fsr_lag"),
      fsr_has_lags = vapply(summary_rows, `[[`, logical(1), "fsr_has_lags"),
      active_fsr_token_count = vapply(summary_rows, `[[`, integer(1), "active_fsr_token_count"),
      active_fsr_tokens = vapply(summary_rows, `[[`, character(1), "active_fsr_tokens"),
      active_fsr_name_count = vapply(summary_rows, `[[`, integer(1), "active_fsr_name_count"),
      active_fsr_reference_names = vapply(summary_rows, `[[`, character(1), "active_fsr_reference_names"),
      active_max_fsr_lag = vapply(summary_rows, `[[`, integer(1), "active_max_fsr_lag"),
      active_fsr_has_lags = vapply(summary_rows, `[[`, logical(1), "active_fsr_has_lags"),
      stringsAsFactors = FALSE
    )
  }
  eq_fsr_summary <- if (!nrow(eq_fsr_frame)) {
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
      equation_number = as.integer(eq_fsr_frame$equation_number),
      token_count = as.integer(eq_fsr_frame$token_count),
      name_count = as.integer(eq_fsr_frame$name_count),
      max_lag = as.integer(eq_fsr_frame$max_lag),
      has_lags = as.logical(eq_fsr_frame$has_lags),
      reference_names = as.character(eq_fsr_frame$reference_names),
      stringsAsFactors = FALSE
    )
  }

  list(
    specs = specs,
    equations = equations_frame,
    eq_fsr = eq_fsr_frame,
    eq_fsr_summary = eq_fsr_summary,
    modeq = modeq_frame,
    modeq_summary = modeq_summary,
    coef_values = build_coef_table(fmout_specs)
  )
}
