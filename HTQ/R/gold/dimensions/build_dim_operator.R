build_dim_operator <- function() {

  # ----------------------------------------
  # Build candidate dimension from Silver
  # ----------------------------------------
  new_dim <- read_silver("journey") %>%
    dplyr::select(OperatorName) %>%
    dplyr::mutate(
      OperatorName = trimws(OperatorName)
    ) %>%
    dplyr::filter(
      !is.na(OperatorName),
      OperatorName != ""
    ) %>%
    dplyr::distinct(OperatorName, .keep_all = TRUE)

  # ----------------------------------------
  # Read existing Gold dimension (if exists)
  # ----------------------------------------
  gold_path <- get_gold_path("dim_operator")

  if (dir.exists(gold_path)) {
    existing_dim <- arrow::open_dataset(gold_path) %>%
      dplyr::collect()
  } else {
    existing_dim <- dplyr::tibble()
  }

  # ----------------------------------------
  # Guard against duplicate natural keys
  # ----------------------------------------
  if (any(duplicated(new_dim$OperatorName))) {
    stop("Duplicate OperatorName detected in new_dim.")
  }

  if (nrow(existing_dim) > 0 &&
      any(duplicated(existing_dim$OperatorName))) {
    stop("Duplicate OperatorName detected in existing_dim.")
  }

  # ----------------------------------------
  # Upsert using helper (Type 1 + audit)
  # ----------------------------------------
  updated_dim <- upsert_dimension(
    existing_dim  = existing_dim,
    new_keys      = new_dim,
    natural_key   = "OperatorName",
    surrogate_key = "OperatorKey"
  )

    # ----------------------------------------
  # Schema validation (after upsert)
  # ----------------------------------------
  expected_cols <- c(
    "OperatorKey",
    "OperatorName",
    "CreatedDate",
    "LastUpdatedDate"
  )

  validate_gold_schema(updated_dim, expected_cols)

  # ----------------------------------------
  # data quality validation (after upsert)
  # ----------------------------------------

  validate_not_null(updated_dim, c("OperatorKey", "OperatorName"))
  validate_no_duplicates(updated_dim, c("OperatorName"))

  # ----------------------------------------
  # Final column order (KEEP audit cols)
  # ----------------------------------------
  updated_dim %>%
    dplyr::select(
      OperatorKey,
      OperatorName,
      CreatedDate,
      LastUpdatedDate
    )
}