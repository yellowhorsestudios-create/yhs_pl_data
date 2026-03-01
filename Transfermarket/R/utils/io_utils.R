#################################################################
# write raw partitioned
#################################################################

write_raw_partitioned <- function(data, output_table, partition_cols) {
  
  base_path <- file.path(
    "C:/R_Scripts/Scripts/transfermarket_project/data/raw",
    output_table
  )
  
  arrow::write_dataset(
    data,
    path = base_path,
    partitioning = partition_cols,
    existing_data_behavior = "overwrite_or_ignore"
  )
}

#################################################################
# write dataset safe
#################################################################

write_dataset_safe <- function(df, name) {

  if (is.null(df) || nrow(df) == 0) {
    message(glue("No data returned for {name}"))
    return(NULL)
  }

  path <- glue("data/{name}.parquet")

  arrow::write_parquet(df, path)

  message(glue("Written: {path} ({nrow(df)} rows)"))
}