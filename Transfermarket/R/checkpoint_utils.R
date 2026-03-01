#################################################################
# update_ingestion_log
#################################################################

update_ingestion_log <- function(stage, job_key, status) {
  
  log_path <- "C:/R_Scripts/Scripts/transfermarket_project/data/meta/ingestion_log.parquet"
  
  new_entry <- tibble::tibble(
    stage = stage,
    job_key = job_key,
    status = status,
    timestamp = Sys.time()
  )
  
  if (file.exists(log_path)) {
    existing <- arrow::read_parquet(log_path)
    updated <- dplyr::bind_rows(existing, new_entry)
  } else {
    updated <- new_entry
  }
  
  arrow::write_parquet(updated, log_path)
}


#################################################################
# should skip job
#################################################################