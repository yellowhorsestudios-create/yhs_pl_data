build_dim_line <- function() {

  # ----------------------------------------
  # Build candidate dimension from Silver
  # ----------------------------------------
  new_dim <- read_silver("journey") %>%
    dplyr::select(
      LineNumber,
      ExtendedLineDesignation,
      DefaultTransportModeCode,
      TransportAuthorityNumber
    ) %>%
    dplyr::filter(!is.na(LineNumber)) %>%
    dplyr::distinct(LineNumber, .keep_all = TRUE)

  # ----------------------------------------
  # Read existing Gold dimension (if exists)
  # ----------------------------------------
  gold_path <- get_gold_path("dim_line")

  if (dir.exists(gold_path)) {
    existing_dim <- arrow::open_dataset(gold_path) %>%
      dplyr::collect()
  } else {
    existing_dim <- dplyr::tibble()
  }

  # ----------------------------------------
  # Guard against duplicate natural keys
  # ----------------------------------------
  if (any(duplicated(new_dim$LineNumber))) {
    stop("Duplicate LineNumber detected in new_dim.")
  }

  if (nrow(existing_dim) > 0 &&
      any(duplicated(existing_dim$LineNumber))) {
    stop("Duplicate LineNumber detected in existing_dim.")
  }

  # ----------------------------------------
  # Upsert using helper (Type 1 + audit)
  # ----------------------------------------
  updated_dim <- upsert_dimension(
    existing_dim  = existing_dim,
    new_keys      = new_dim,
    natural_key   = "LineNumber",
    surrogate_key = "LineKey"
  )

  # ----------------------------------------
  # Schema validation (after upsert)
  # ----------------------------------------
  expected_cols <- c(
    "LineKey",
    "LineNumber",
    "ExtendedLineDesignation",
    "DefaultTransportModeCode",
    "TransportAuthorityNumber",
    "CreatedDate",
    "LastUpdatedDate"
  )

  validate_gold_schema(updated_dim, expected_cols)

  # ----------------------------------------
  # data quality validation (after upsert)
  # ----------------------------------------

  validate_not_null(updated_dim, c("LineKey", "LineNumber"))
  validate_no_duplicates(updated_dim, c("LineNumber"))

  # ----------------------------------------
  # Final column order
  # ----------------------------------------
  updated_dim %>%
    dplyr::select(
      LineKey,
      LineNumber,
      ExtendedLineDesignation,
      DefaultTransportModeCode,
      TransportAuthorityNumber,
      CreatedDate,
      LastUpdatedDate
    )
}