
#####################################################
# PATH RESOLVER BRONZE SILVER GOLD
#####################################################

get_silver_path <- function(table_name) {
  file.path(Sys.getenv("HTQ_SILVER_PATH"), table_name)
}

get_bronze_path <- function(table_name) {
  file.path(Sys.getenv("HTQ_BRONZE_PATH"), table_name)
}

get_gold_path <- function(table_name) {
  file.path(Sys.getenv("HTQ_GOLD_PATH"), table_name)
}



#####################################################
# READ BRONZE
#####################################################

read_bronze <- function(table_name, start_date = NULL, end_date = NULL) {

  path <- get_bronze_path(table_name)

  ds <- arrow::open_dataset(path)

  if (!is.null(start_date)) {
    ds <- ds %>%
      dplyr::filter(
        OperatingDate >= as.Date(start_date) &
        OperatingDate <= as.Date(end_date)
      )
  }

  return(ds)
}

#####################################################
# READ SILVER
#####################################################

read_silver <- function(table_name, start_date = NULL, end_date = NULL) {

  path <- get_silver_path(table_name)

  if (!dir.exists(path)) {
    return(dplyr::tibble())
  }

  ds <- arrow::open_dataset(path)

  if (!is.null(start_date) && !is.null(end_date)) {
    ds <- ds %>%
      dplyr::filter(
        OperatingDate >= as.Date(start_date) &
        OperatingDate <= as.Date(end_date)
      )
  }

  df <- ds %>% dplyr::collect()

  # Enforce Date type once centrally
  if ("OperatingDate" %in% names(df)) {
    df$OperatingDate <- as.Date(df$OperatingDate)
  }

  return(df)
}

#####################################################
# WRITE SILVER partitioned
#####################################################

write_silver_partitioned <- function(df, table_name) {

  path <- get_silver_path(table_name)

  arrow::write_dataset(
    df,
    path = path,
    format = "parquet",
    partitioning = "OperatingDate",
    existing_data_behavior = "overwrite"
  )

  log_info(paste0("Silver write complete: ", table_name))
}

#####################################################
# WRITE gold FACT
#####################################################

write_gold_fact <- function(df, table_name) {

  path <- get_silver_path(table_name)

  arrow::write_dataset(
    df,
    path = path,
    format = "parquet",
    partitioning = "OperatingDay",
    existing_data_behavior = "overwrite_or_ignore"
  )
}

#####################################################
# DELETE FACT PARTITIONS
#####################################################

delete_silver_partitions <- function(table_name, start_date) {

  path <- get_silver_path(table_name)

  if (!dir.exists(path)) return()

  partitions <- list.dirs(path, recursive = FALSE, full.names = TRUE)

  for (p in partitions) {

    part_date <- gsub("OperatingDay=", "", basename(p))

    if (as.Date(part_date) >= as.Date(start_date)) {
      unlink(p, recursive = TRUE)
    }
  }
}

#####################################################
# WRITE GOLD DIMENSIONS
#####################################################

write_gold_dimension <- function(df, table_name, expected_cols) {

  validate_gold_schema(df, expected_cols)

  path <- get_gold_path(table_name)

  arrow::write_dataset(
    df,
    path = path,
    format = "parquet",
    existing_data_behavior = "overwrite"
  )
}

#####################################################
# WRITE FACT PARTITIONED
#####################################################

write_gold_fact <- function(df, table_name) {

  path <- get_gold_path(table_name)

  arrow::write_dataset(
    df,
    path = path,
    format = "parquet",
    partitioning = gold_config$fact_partition_column,
    existing_data_behavior = "delete_matching"
  )
}