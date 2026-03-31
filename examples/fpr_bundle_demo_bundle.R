periods <- c("2024.4", "2025.1", "2025.2", "2025.3", "2025.4")

bundle <- list(
  name = "fpr_bundle_demo",
  state = list(
    periods = periods,
    series = list(
      GDP = c(100, NA, NA, NA, NA),
      CONS = c(NA, NA, NA, NA, NA),
      INV = c(NA, NA, NA, NA, NA),
      GOV = c(20, 20, 20, 21, 21),
      CONS_EXOG = c(25, 25, 26, 27, 28),
      RATE = c(4.0, 4.0, 4.1, 4.2, 4.3)
    )
  ),
  specs = list(
    list(target = "CONS", expression = "0.6 * GDP + CONS_EXOG"),
    list(target = "INV", expression = "0.15 * GDP(-1) - 2 * RATE"),
    list(target = "GDP", expression = "CONS + INV + GOV")
  ),
  control = list(
    sample_start = "2025.1",
    sample_end = "2025.4",
    max_iter = 100L,
    tolerance = 1e-10,
    damping = 1.0,
    strict = TRUE
  )
)
