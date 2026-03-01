##############################################################
# delete bronze partitions
##############################################################



delete_bronze_partitions <- function(table_name, start_date) {

  bronze_path <- file.path(
    Sys.getenv("HTQ_BRONZE_PATH"),
    table_name
  )

  if (!dir.exists(bronze_path)) return()

  partitions <- list.dirs(bronze_path,
                          recursive = FALSE,
                          full.names = TRUE)

  for (p in partitions) {

    part_date <- gsub("OperatingDay=", "", basename(p))

    if (as.Date(part_date) >= as.Date(start_date)) {
      unlink(p, recursive = TRUE)
      log_info(paste0("Deleted Bronze partition: ", p))
    }
  }
}

##############################################################
# write bronze partitions
##############################################################

write_bronze_partitioned <- function(df, table_name, window) {

  bronze_path <- file.path(
    Sys.getenv("HTQ_BRONZE_PATH"),
    table_name
  )

  arrow::write_dataset(
    df,
    path = bronze_path,
    format = "parquet",
    partitioning = "OperatingDate"
  )

  log_info(paste0("Bronze write complete: ", table_name))
}


