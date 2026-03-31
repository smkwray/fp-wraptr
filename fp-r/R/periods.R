parse_period <- function(period) {
  raw <- gsub("^['\"]|['\"]$", "", as.character(period))
  raw <- sub(";\\s*$", "", raw)
  if (length(raw) != 1L || is.na(raw)) {
    stopf("Expected a single non-missing period, got %s", deparse(period))
  }
  matches <- regexec("^(\\d{4})\\.(\\d)$", raw, perl = TRUE)
  parts <- regmatches(raw, matches)[[1]]
  if (length(parts) != 3L) {
    stopf("Invalid period label: %s", raw)
  }
  year <- as.integer(parts[2])
  quarter <- as.integer(parts[3])
  if (quarter < 1L || quarter > 4L) {
    stopf("Quarter must be between 1 and 4: %s", raw)
  }
  list(
    label = raw,
    year = year,
    quarter = quarter,
    index = year * 4L + quarter - 1L
  )
}

format_period <- function(index) {
  idx <- as.integer(index)
  if (length(idx) != 1L || is.na(idx)) {
    stopf("Expected a single integer index, got %s", deparse(index))
  }
  year <- idx %/% 4L
  quarter <- idx %% 4L + 1L
  sprintf("%04d.%d", year, quarter)
}

seq_periods <- function(start, end) {
  start_idx <- parse_period(start)$index
  end_idx <- parse_period(end)$index
  if (end_idx < start_idx) {
    stopf("End period %s precedes start period %s", end, start)
  }
  vapply(seq.int(start_idx, end_idx), format_period, character(1))
}

resolve_period_position <- function(periods, value = NULL, default = NULL) {
  if (is.null(value)) {
    return(default)
  }
  if (is.numeric(value) && length(value) == 1L && !is.na(value)) {
    pos <- as.integer(value)
    if (pos < 1L || pos > length(periods)) {
      stopf("Period position %d is outside the available range", pos)
    }
    return(pos)
  }
  target <- as.character(value)
  pos <- match(target, periods)
  if (is.na(pos)) {
    stopf("Period %s is not present in the bundle window", target)
  }
  pos
}

resolve_sample_window <- function(periods, sample_start = NULL, sample_end = NULL) {
  start_pos <- resolve_period_position(periods, sample_start, 1L)
  end_pos <- resolve_period_position(periods, sample_end, length(periods))
  if (end_pos < start_pos) {
    stopf("Sample window end precedes sample window start")
  }
  c(start = start_pos, end = end_pos)
}
