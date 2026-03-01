generate_run_id <- function() {
  paste0(format(Sys.time(), "%Y%m%d%H%M%S"), "_", sample(1000:9999, 1))
}

write_pipeline_run_log <- function(run_log) {

  path <- get_gold_path("pipeline_run_log")

  if (dir.exists(path)) {
    existing <- arrow::open_dataset(path) %>% dplyr::collect()
    updated <- dplyr::bind_rows(existing, run_log)
  } else {
    updated <- run_log
  }

  arrow::write_dataset(
    updated,
    path = path,
    format = "parquet",
    existing_data_behavior = "overwrite"
  )
}