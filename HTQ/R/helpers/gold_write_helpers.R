#####################################################
# WRITE GOLD DIMENSIONS GOVERNED
#####################################################

write_gold_dimension_governed <- function(
  df,
  table_name,
  surrogate_key,
  natural_key,
  expected_columns
) {

  # -----------------------------------------
  # Schema validation
  # -----------------------------------------
  validate_gold_schema(df, expected_columns)

  # -----------------------------------------
  # Surrogate key integrity
  # -----------------------------------------
  if (any(is.na(df[[surrogate_key]]))) {
    stop(paste("NULL surrogate keys detected in", table_name))
  }

  if (any(duplicated(df[[surrogate_key]]))) {
    stop(paste("Duplicate surrogate keys detected in", table_name))
  }

  # -----------------------------------------
  # Natural key integrity
  # -----------------------------------------
  if (any(is.na(df[[natural_key]]))) {
    stop(paste("NULL natural keys detected in", table_name))
  }

  if (any(duplicated(df[[natural_key]]))) {
    stop(paste("Duplicate natural keys detected in", table_name))
  }

  # -----------------------------------------
  # Write dataset
  # -----------------------------------------
  path <- get_gold_path(table_name)

  arrow::write_dataset(
    df,
    path = path,
    format = "parquet",
    existing_data_behavior = "overwrite"
  )

  log_info(paste0(
    "Gold dimension written: ",
    table_name,
    " | Rows: ",
    nrow(df)
  ))
}

#####################################################
# WRITE GOLD FACTS GOVERNED
#####################################################

write_gold_fact_governed <- function(
  df,
  table_name,
  grain_columns,
  foreign_keys,
  expected_columns,
  min_row_threshold = 0.5  # 50% drop protection
) {

  # -----------------------------------------
  # Schema validation
  # -----------------------------------------
  validate_schema(df, expected_columns)

  # -----------------------------------------
  # Grain uniqueness
  # -----------------------------------------
  if (any(duplicated(df[grain_columns]))) {
    stop(paste(
      "Duplicate grain detected in",
      table_name,
      "on columns:",
      paste(grain_columns, collapse = ", ")
    ))
  }

  # -----------------------------------------
  # Foreign key integrity
  # -----------------------------------------
  for (fk in foreign_keys) {
    if (any(is.na(df[[fk]]))) {
      stop(paste("NULL foreign key detected:", fk, "in", table_name))
    }
  }

  # -----------------------------------------
  # Row-count anomaly detection
  # -----------------------------------------
  path <- get_gold_path(table_name)

  if (dir.exists(path)) {

    existing_rows <- arrow::open_dataset(path) %>%
      dplyr::collect() %>%
      nrow()

    new_rows <- nrow(df)

    if (existing_rows > 0) {

      ratio <- new_rows / existing_rows

      if (ratio < min_row_threshold) {
        stop(paste(
          "Row count anomaly detected in",
          table_name,
          "| Existing:",
          existing_rows,
          "| New:",
          new_rows
        ))
      }
    }
  }

  # -----------------------------------------
  # Write dataset
  # -----------------------------------------
  arrow::write_dataset(
    df,
    path = path,
    format = "parquet",
    partitioning = gold_config$fact_partition_column,
    existing_data_behavior = "delete_matching"
  )

  log_info(paste0(
    "Gold fact written: ",
    table_name,
    " | Rows: ",
    nrow(df)
  ))
}