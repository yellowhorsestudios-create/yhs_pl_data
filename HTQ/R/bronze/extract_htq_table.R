extract_htq_table <- function(view_name, window) {

  log_info(paste0("Extracting Bronze: ", view_name))

  con <- get_sql_connection()

  query <- glue::glue("
    SELECT *
    FROM {Sys.getenv('HTQSCHEMA')}.{view_name}
    WHERE OperatingDate >= '{window$start_date}'
    AND OperatingDate <= '{window$end_date}'
  ")

  # ✅ Log BEFORE execution
  log_info(glue("Running query:\n{query}"))

  df <- DBI::dbGetQuery(con, query)

  DBI::dbDisconnect(con)

  log_info(paste0("Extracted rows: ", nrow(df)))

  df
}
