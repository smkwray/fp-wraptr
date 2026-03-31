normalize_rho_terms <- function(rho_terms) {
  if (is.null(rho_terms) || length(rho_terms) == 0L) {
    return(data.frame(order = integer(), coefficient = numeric(), stringsAsFactors = FALSE))
  }
  if (is.data.frame(rho_terms)) {
    normalized <- data.frame(
      order = as.integer(rho_terms$order),
      coefficient = as.numeric(rho_terms$coefficient),
      stringsAsFactors = FALSE
    )
    return(normalized[order(normalized$order), , drop = FALSE])
  }
  if (!is.list(rho_terms)) {
    stopf("Unsupported rho_terms format")
  }
  normalized <- lapply(rho_terms, function(term) {
    list(
      order = as.integer(term$order %||% term$lag %||% 0L),
      coefficient = as.numeric(term$coefficient %||% term$value %||% 0)
    )
  })
  normalized_frame <- data.frame(
    order = vapply(normalized, `[[`, integer(1), "order"),
    coefficient = vapply(normalized, `[[`, numeric(1), "coefficient"),
    stringsAsFactors = FALSE
  )
  normalized_frame[order(normalized_frame$order), , drop = FALSE]
}

normalize_resid_ar1 <- function(resid_ar1, rho_terms = NULL) {
  if (is.null(resid_ar1)) {
    return(NULL)
  }
  update_source <- as.character(resid_ar1$update_source %||% "structural")
  carry_damp_mode <- as.character(resid_ar1$carry_damp_mode %||% "term")
  if (!update_source %in% c("structural", "result", "solved")) {
    stopf("Unsupported resid_ar1 update_source: %s", update_source)
  }
  if (!carry_damp_mode %in% c("term", "state", "sol4")) {
    stopf("Unsupported resid_ar1 carry_damp_mode: %s", carry_damp_mode)
  }
  rho_lag1 <- as.numeric(resid_ar1$rho_lag1 %||% NA_real_)
  if (!is.finite(rho_lag1) && !is.null(rho_terms) && nrow(rho_terms) > 0L) {
    rho_lag1 <- as.numeric(rho_terms$coefficient[[which(rho_terms$order == 1L)[1L]]])
  }
  if (!is.finite(rho_lag1)) {
    stopf("resid_ar1 requires a finite rho_lag1 or a lag-1 rho term")
  }
  list(
    rho_lag1 = rho_lag1,
    source_series = as.character(resid_ar1$source_series %||% ""),
    update_source = update_source,
    carry_lag = as.integer(resid_ar1$carry_lag %||% 0L),
    carry_damp = as.numeric(resid_ar1$carry_damp %||% 1.0),
    carry_damp_mode = carry_damp_mode,
    carry_multipass = isTRUE(resid_ar1$carry_multipass)
  )
}

normalize_specs <- function(specs) {
  lapply(specs, function(spec) {
    target <- spec$target %||% spec$name
    expression <- spec$expression %||% NULL
    compiled <- spec$compiled %||% NULL
    if (is.null(compiled) && !is.null(expression)) {
      compiled <- compile_expression(expression)
    }
    rho_terms <- normalize_rho_terms(spec$rho_terms %||% NULL)
    resid_ar1 <- normalize_resid_ar1(spec$resid_ar1 %||% NULL, rho_terms = rho_terms)
    if (!is.null(resid_ar1) && nrow(rho_terms) > 0L) {
      stopf("Spec %s cannot enable both rho_terms and resid_ar1", target)
    }
    list(
      target = as.character(target),
      kind = as.character(spec$kind %||% ""),
      expression = expression,
      compiled = compiled,
      rho_terms = rho_terms,
      resid_ar1 = resid_ar1,
      target_lag_source = as.character(spec$target_lag_source %||% "")
    )
  })
}

build_dependency_order_details <- function(specs) {
  normalized <- normalize_specs(specs)
  targets <- vapply(normalized, function(spec) spec$target, character(1))
  graph <- setNames(vector("list", length(normalized)), targets)
  indegree <- setNames(integer(length(normalized)), targets)
  for (spec in normalized) {
    refs <- spec$compiled$references
    deps <- character()
    if (!is.null(refs) && nrow(refs) > 0L) {
      deps <- unique(refs$name[refs$lag == 0L])
    }
    deps <- deps[deps %in% targets]
    deps <- deps[deps != spec$target]
    graph[[spec$target]] <- deps
    indegree[[spec$target]] <- length(deps)
  }
  order <- character()
  queue <- targets[indegree[targets] == 0L]
  while (length(queue) > 0L) {
    node <- queue[[1L]]
    queue <- queue[-1L]
    if (node %in% order) {
      next
    }
    order <- c(order, node)
    children <- targets[vapply(graph, function(deps) node %in% deps, logical(1))]
    for (child in children) {
      indegree[[child]] <- indegree[[child]] - 1L
      if (indegree[[child]] == 0L) {
        queue <- c(queue, child)
      }
    }
  }
  remaining <- targets[!targets %in% order]
  cyclic_targets <- character()
  if (length(remaining)) {
    remaining_graph <- graph[remaining]
    reachable_cache <- new.env(parent = emptyenv())

    reachable_from <- function(node) {
      if (exists(node, envir = reachable_cache, inherits = FALSE)) {
        return(get(node, envir = reachable_cache, inherits = FALSE))
      }
      seen <- character()
      stack <- unique(as.character(remaining_graph[[node]] %||% character()))
      while (length(stack)) {
        current <- stack[[1L]]
        stack <- stack[-1L]
        if (!nzchar(current) || !(current %in% remaining) || current %in% seen) {
          next
        }
        seen <- c(seen, current)
        stack <- c(stack, setdiff(unique(as.character(remaining_graph[[current]] %||% character())), seen))
      }
      assign(node, unique(seen), envir = reachable_cache)
      unique(seen)
    }

    cyclic_targets <- remaining[vapply(remaining, function(node) {
      deps <- unique(as.character(remaining_graph[[node]] %||% character()))
      deps <- deps[nzchar(deps)]
      if (node %in% deps) {
        return(TRUE)
      }
      reachable <- reachable_from(node)
      any(vapply(deps, function(dep) {
        dep %in% remaining && node %in% reachable_from(dep) && dep %in% reachable
      }, logical(1)))
    }, logical(1))]
  }
  list(
    order = c(order, remaining),
    unresolved_targets = remaining,
    cyclic_targets = cyclic_targets
  )
}

build_dependency_order <- function(specs) {
  build_dependency_order_details(specs)$order
}
