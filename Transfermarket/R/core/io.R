write_dataset_safe <- function(df, name) {

  if (is.null(df) || nrow(df) == 0) {
    message(glue("No data returned for {name}"))
    return(NULL)
  }

  path <- glue("data/{name}.parquet")

  arrow::write_parquet(df, path)

  message(glue("Written: {path} ({nrow(df)} rows)"))
}