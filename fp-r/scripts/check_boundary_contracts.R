args <- commandArgs(trailingOnly = TRUE)

read_flag <- function(name, default = NULL) {
  idx <- match(name, args)
  if (is.na(idx)) {
    return(default)
  }
  if (idx >= length(args)) {
    stop(sprintf("Missing value for flag %s", name), call. = FALSE)
  }
  args[[idx + 1L]]
}

work_dir_raw <- read_flag("--work-dir", default = getwd())
work_dir <- normalizePath(work_dir_raw, winslash = "/", mustWork = FALSE)
dir.create(work_dir, recursive = TRUE, showWarnings = FALSE)

resolve_script_path <- function() {
  args_all <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", args_all, value = TRUE)
  if (!length(file_arg)) {
    stop("Unable to resolve check_boundary_contracts.R script path", call. = FALSE)
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
for (name in runtime_files) {
  source(file.path(r_dir, name), local = globalenv())
}

assert_identical <- function(actual, expected, label) {
  if (!identical(actual, expected)) {
    stop(
      sprintf(
        "%s mismatch: expected %s, got %s",
        label,
        paste(expected, collapse = ","),
        paste(actual, collapse = ",")
      ),
      call. = FALSE
    )
  }
}

specs_boundary <- list(
  list(target = "SUPPORT", kind = "ident", equation_number = NULL, compiled = compile_expression("RAWA(-1) + RAWB")),
  list(target = "DOWN", kind = "genr", equation_number = NULL, compiled = compile_expression("SUPPORT + C")),
  list(target = "LY", kind = "", equation_number = 11L, compiled = compile_expression("LX + LV"))
)
statements_boundary <- list(
  list(command = "SMPL", raw = "SMPL 1952.1 2025.3;"),
  list(command = "IDENT", kind = "ident", name = "SUPPORT", raw = "IDENT SUPPORT=RAWA(-1)+RAWB;"),
  list(command = "GENR", kind = "genr", name = "DOWN", raw = "GENR DOWN=SUPPORT+C;")
)
boundary_refs <- collect_outside_boundary_materialization_refs(
  specs_boundary,
  statements = statements_boundary,
  sample_start = "2025.4",
  protected_targets = "B",
  equation_targets = "LY",
  equation_support_refs = "C"
)
assert_identical(boundary_refs, "RAWB", "boundary_refs")

specs_first_period <- list(
  list(target = "POPP", kind = "ident", equation_number = NULL, compiled = compile_expression("POP1 + POP2 + POP3")),
  list(target = "YPOP", kind = "ident", equation_number = NULL, compiled = compile_expression("Y / POPP")),
  list(target = "JGPTJ1", kind = "ident", equation_number = NULL, compiled = compile_expression("JGSWITCH * (YPOP(-1) - YPOP(-2))")),
  list(target = "Y", kind = "lhs", equation_number = 1L, compiled = compile_expression("LY"))
)
statements_first_period <- list(
  list(command = "SMPL", raw = "SMPL 1952.1 2025.3;"),
  list(command = "IDENT", kind = "ident", name = "POPP", raw = "IDENT POPP=POP1+POP2+POP3;"),
  list(command = "IDENT", kind = "ident", name = "YPOP", raw = "IDENT YPOP=Y/POPP;"),
  list(command = "SMPL", raw = "SMPL 1952.1 2029.4;"),
  list(command = "IDENT", kind = "ident", name = "JGPTJ1", raw = "IDENT JGPTJ1=JGSWITCH*(YPOP(-1)-YPOP(-2));")
)
first_period_refs <- sort(collect_outside_first_period_materialization_input_refs(
  specs_first_period,
  statements = statements_first_period,
  sample_start = "2025.4",
  protected_targets = "POP2",
  equation_targets = "LY"
))
assert_identical(first_period_refs, c("POP1", "POP3"), "first_period_refs")

frame <- data.frame(
  period = c("2025.3", "2025.4", "2026.1"),
  AUX = c(10, 14, 20),
  PSI4 = c(1, 1.1, 1.2),
  PD = c(2, 2, 2),
  stringsAsFactors = FALSE,
  check.names = FALSE
)
history <- list(
  list(command = "SMPL", raw = "SMPL 1952.1 2025.4;"),
  list(command = "IDENT", kind = "ident", name = "PCD", raw = "IDENT PCD=PSI4*PD;", expression = "PSI4*PD"),
  list(command = "IDENT", kind = "ident", name = "LPIEFAZ", raw = "IDENT LPIEFAZ=PCD(-1)+AUX;", expression = "PCD(-1)+AUX")
)
sources <- list(entry_path = "demo", fmdata = "", fmexog = "", fmout = "", tree = list(files_scanned = character()))
compat_bundle <- build_standard_solve_bundle(
  sources,
  frame,
  history,
  solve_index = 3L,
  active_window = c("2026.1", "2026.1"),
  solve_metadata = list(options = list(outside = TRUE, noreset = TRUE), semantics_profile = "compat")
)
canonical_bundle <- build_standard_solve_bundle(
  sources,
  frame,
  history,
  solve_index = 3L,
  active_window = c("2026.1", "2026.1"),
  solve_metadata = list(options = list(outside = TRUE, noreset = TRUE), semantics_profile = "canonical")
)
boundary_pos <- match("2025.4", compat_bundle$state$periods)
compat_aux <- as.numeric(compat_bundle$state$series$AUX[[boundary_pos]])
canonical_aux <- as.numeric(canonical_bundle$state$series$AUX[[boundary_pos]])
if (!identical(compat_aux, 14) || !identical(canonical_aux, 14)) {
  stop(
    sprintf("profile boundary carry mismatch: compat=%s canonical=%s", compat_aux, canonical_aux),
    call. = FALSE
  )
}

writeLines(
  c(
    "status=ok",
    sprintf("boundary_refs=%s", paste(boundary_refs, collapse = ",")),
    sprintf("first_period_refs=%s", paste(first_period_refs, collapse = ",")),
    sprintf("compat_boundary_value=%s", compat_aux),
    sprintf("canonical_boundary_value=%s", canonical_aux)
  ),
  file.path(work_dir, "boundary_contract_report.txt")
)
