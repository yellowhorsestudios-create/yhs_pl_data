run_bronze_pipeline <- function(days_back = 7) {

  log_info("Starting Bronze Pipeline")

  window <- get_operating_window(days_back)

  # ---------------------------------
  # Mapping: Bronze name → SQL view
  # ---------------------------------

  tables <- list(
    journey     = "Journey",
    journeycall = "JourneyCall",
    journeylink = "JourneyLink"
  )

  for (bronze_name in names(tables)) {

    tryCatch({

      sql_view <- tables[[bronze_name]]

      log_info(paste0(
        "Processing Bronze table: ",
        bronze_name,
        " (Source view: ",
        sql_view,
        ")"
      ))

      df <- extract_htq_table(sql_view, window)

      if (nrow(df) > 0) {
        write_bronze_partitioned(df, bronze_name, window)
      } else {
        log_warning(paste0(
          "No data returned for view: ",
          sql_view
        ))
      }

    }, error = function(e) {

      log_error(paste0(
        "Bronze stage failed for ",
        bronze_name,
        " | Error: ",
        e$message
      ))

      stop("Bronze pipeline halted.")
    })
  }

  log_info("Bronze Pipeline Completed Successfully")
}
