args <- commandArgs(trailingOnly = TRUE)

read_flag <- function(name) {
  idx <- match(name, args)
  if (is.na(idx) || idx >= length(args)) {
    stop(sprintf("Missing required flag %s", name), call. = FALSE)
  }
  args[[idx + 1L]]
}

bundle_path <- normalizePath(read_flag("--bundle"), winslash = "/", mustWork = TRUE)
work_dir <- normalizePath(read_flag("--work-dir"), winslash = "/", mustWork = FALSE)
dir.create(work_dir, recursive = TRUE, showWarnings = FALSE)
semantics_profile <- if ("--semantics-profile" %in% args) read_flag("--semantics-profile") else "compat"
options(fp_r.semantics_profile = semantics_profile)

resolve_script_path <- function() {
  args_all <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", args_all, value = TRUE)
  if (!length(file_arg)) {
    stop("Unable to resolve run_backend_bundle.R script path", call. = FALSE)
  }
  normalizePath(sub("^--file=", "", file_arg[[1L]]), winslash = "/", mustWork = TRUE)
}

script_path <- resolve_script_path()
r_dir <- normalizePath(file.path(dirname(script_path), "..", "R"), winslash = "/", mustWork = TRUE)
runtime_files <- c(
  "000_utils.R",
  "bundle.R",
  "dependency.R",
  "equations.R",
  "expressions.R",
  "legacy_data.R",
  "mini_run.R",
  "parser.R",
  "periods.R",
  "solver.R",
  "standard_input.R"
)
missing_runtime <- runtime_files[!file.exists(file.path(r_dir, runtime_files))]
if (length(missing_runtime)) {
  stop(
    sprintf(
      "Bundle runtime is missing required R files: %s",
      paste(missing_runtime, collapse = ", ")
    ),
    call. = FALSE
  )
}
for (name in runtime_files) {
  path <- file.path(r_dir, name)
  source(path, local = globalenv())
}

bundle <- read_model_bundle(bundle_path)
bundle$control <- modifyList(bundle$control %||% list(), list(semantics_profile = semantics_profile))
result <- mini_run(bundle)

series_path <- file.path(work_dir, "fp_r_series.csv")
diag_path <- file.path(work_dir, "fp_r_diagnostics.csv")
report_path <- file.path(work_dir, "fp_r_report.txt")
pabev_path <- file.path(work_dir, "PABEV.TXT")

write.csv(result$series, series_path, row.names = FALSE)
write.csv(result$diagnostics, diag_path, row.names = FALSE)

write_pabev <- function(frame, path) {
  periods <- as.character(frame$period)
  values_only <- frame[, setdiff(names(frame), "period"), drop = FALSE]
  con <- file(path, open = "wt", encoding = "UTF-8")
  on.exit(close(con), add = TRUE)
  writeLines(sprintf("SMPL %s %s;", periods[[1L]], periods[[length(periods)]]), con)
  for (column in names(values_only)) {
    writeLines(sprintf("LOAD %s;", column), con)
    values <- as.numeric(values_only[[column]])
    chunks <- split(
      sprintf("%.12f", values),
      ceiling(seq_along(values) / 4L)
    )
    for (chunk in chunks) {
      writeLines(paste(chunk, collapse = " "), con)
    }
    writeLines("'END'", con)
  }
}

write_pabev(result$series, pabev_path)

report <- list(
  bundle_name = result$bundle_name %||% bundle$name %||% "<unnamed>",
  bundle_path = bundle_path,
  series_path = series_path,
  diagnostics_path = diag_path,
  pabev_path = pabev_path,
  periods = as.character(result$series$period),
  order = result$order
)
report_lines <- c(
  sprintf("bundle_name=%s", report$bundle_name),
  sprintf("semantics_profile=%s", semantics_profile),
  sprintf("bundle_path=%s", report$bundle_path),
  sprintf("series_path=%s", report$series_path),
  sprintf("diagnostics_path=%s", report$diagnostics_path),
  sprintf("pabev_path=%s", report$pabev_path),
  sprintf("periods=%s", paste(report$periods, collapse = ",")),
  sprintf("order=%s", paste(report$order, collapse = ","))
)
writeLines(report_lines, report_path)
