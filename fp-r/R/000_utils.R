`%||%` <- function(lhs, rhs) {
  if (is.null(lhs)) {
    rhs
  } else {
    lhs
  }
}

stopf <- function(fmt, ...) {
  stop(sprintf(fmt, ...), call. = FALSE)
}
