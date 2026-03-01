write_audit_record <- function(layer,
                               table_name,
                               start_date,
                               end_date,
                               extracted_rows,
                               written_rows,
                               status,
                               error_message = NULL) {

  audit_path <- file.path(Sys.getenv("HTQ_GOLD_PATH"), "_audit", "ingestion_audit")

  error_message_clean <- if (is.null(error_message)) NA_character_ else error_message

  record <- data.frame(
    layer = layer,
    table_name = table_name,
    start_date = as.Date(start_date),
    end_date = as.Date(end_date),
    extracted_rows = extracted_rows,
    written_rows = written_rows,
    run_timestamp = Sys.time(),
    status = status,
    error_message = error_message_clean,
    stringsAsFactors = FALSE
  )

  arrow::write_dataset(
    record,
    path = audit_path,
    format = "parquet",
    existing_data_behavior = "delete_matching"
  )
}