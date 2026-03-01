build_dim_stop <- function() {

  # ----------------------------------------
  # Build candidate dimension from Silver
  # ----------------------------------------
  new_dim <- read_silver("journeycall") %>%
    dplyr::select(
      StopAreaNumber,
      StopName,
      MunicipalityName,
      TimingPointYesNo,
      StopTransportAuthorityNumber,
      Coord_Latitude,
      Coord_Longitude
    ) %>%
    dplyr::mutate(
      StopAreaNumber = trimws(as.character(StopAreaNumber)),
      StopName = trimws(StopName),
      MunicipalityName = trimws(MunicipalityName)
    ) %>%
    dplyr::filter(!is.na(StopAreaNumber), StopAreaNumber != "") %>%
    dplyr::distinct(StopAreaNumber, .keep_all = TRUE)

  # ----------------------------------------
  # Read existing Gold dimension (if exists)
  # ----------------------------------------
  gold_path <- get_gold_path("dim_stop")

  if (dir.exists(gold_path)) {
    existing_dim <- arrow::open_dataset(gold_path) %>%
      dplyr::collect()
  } else {
    existing_dim <- dplyr::tibble()
  }

  # ----------------------------------------
  # Guard against duplicate natural keys
  # ----------------------------------------
  if (any(duplicated(new_dim$StopAreaNumber))) {
    stop("Duplicate StopAreaNumber detected in new_dim.")
  }

  if (nrow(existing_dim) > 0 &&
      any(duplicated(existing_dim$StopAreaNumber))) {
    stop("Duplicate StopAreaNumber detected in existing_dim.")
  }

  # ----------------------------------------
  # Upsert using helper (Type 1 + audit)
  # ----------------------------------------
  updated_dim <- upsert_dimension(
    existing_dim  = existing_dim,
    new_keys      = new_dim,
    natural_key   = "StopAreaNumber",
    surrogate_key = "StopKey"
  )

  # ----------------------------------------
  # Schema validation (after upsert)
  # ----------------------------------------
  expected_cols <- c(
    "StopKey",
    "StopAreaNumber",
    "StopName",
    "MunicipalityName",
    "TimingPointYesNo",
    "StopTransportAuthorityNumber",
    "Coord_Latitude",
    "Coord_Longitude",
    "CreatedDate",
    "LastUpdatedDate"
  )

  validate_gold_schema(updated_dim, expected_cols)

  # ----------------------------------------
  # data quality validation (after upsert)
  # ----------------------------------------

  validate_not_null(updated_dim, c("StopKey", "StopAreaNumber"))
  validate_no_duplicates(updated_dim, c("StopAreaNumber"))

  # ----------------------------------------
  # Final column order
  # ----------------------------------------
  updated_dim %>%
    dplyr::select(
      StopKey,
      StopAreaNumber,
      StopName,
      MunicipalityName,
      TimingPointYesNo,
      StopTransportAuthorityNumber,
      Coord_Latitude,
      Coord_Longitude,
      CreatedDate,
      LastUpdatedDate
    )
}