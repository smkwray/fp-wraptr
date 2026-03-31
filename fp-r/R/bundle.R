read_model_bundle <- function(path) {
  normalized_path <- normalizePath(path, winslash = "/", mustWork = TRUE)
  if (dir.exists(normalized_path)) {
    bundle_file <- file.path(normalized_path, "bundle.R")
    if (!file.exists(bundle_file)) {
      stopf("Bundle directory %s does not contain bundle.R", normalized_path)
    }
    return(read_model_bundle(bundle_file))
  }
  if (tolower(tools::file_ext(normalized_path)) != "r") {
    stopf("Unsupported bundle file: %s", normalized_path)
  }
  env <- new.env(parent = baseenv())
  sys.source(normalized_path, envir = env)
  if (!exists("bundle", envir = env, inherits = FALSE)) {
    stopf("Bundle file %s did not define `bundle`", normalized_path)
  }
  bundle <- get("bundle", envir = env, inherits = FALSE)
  if (is.null(bundle$state) || is.null(bundle$state$periods)) {
    stopf("Bundle %s is missing state periods", normalized_path)
  }
  bundle$path <- normalized_path
  bundle
}

bundle_specs <- function(bundle) {
  if (!is.null(bundle$specs)) {
    return(bundle$specs)
  }
  if (is.null(bundle$input_text)) {
    stopf("Bundle %s has neither specs nor input_text", bundle$name %||% "<unnamed>")
  }
  parsed <- parse_fp_input(bundle$input_text)
  Filter(function(item) !is.null(item$expression), parsed$assignments)
}
