run_gold_pipeline <- function(days_back = 7) {

  log_info("Starting Gold Pipeline")

  window <- get_operating_window(days_back)

  run_id <- generate_run_id()
  start_time <- Sys.time()

  status <- "SUCCESS"
  error_message <- NA_character_

  tryCatch({

    # -------------------------------
    # Dimensions (full rebuild)
    # -------------------------------

    if (gold_config$rebuild_dimensions_each_run) {

      dim_date <- build_dim_date(
        window$start_date,
        window$end_date
      )
      write_gold_dimension(dim_date, "dim_date")

      dim_line <- build_dim_line()
      write_gold_dimension_governed(
        df = dim_line,
        table_name = "dim_line",
        surrogate_key = "LineKey",
        natural_key = "LineNumber",
        expected_columns = c(
          "LineKey",
          "LineNumber",
          "ExtendedLineDesignation",
          "DefaultTransportModeCode",
          "TransportAuthorityNumber",
          "CreatedDate",
          "LastUpdatedDate"
        )
      )

      dim_stop <- build_dim_stop()
      write_gold_dimension_governed(
      df = dim_stop,
      table_name = "dim_stop",
      surrogate_key = "StopKey",
      natural_key = "StopAreaNumber",
      expected_columns = c(
        "StopKey",
        "StopAreaNumber",
        "StopName",
        "MunicipalityName",
        "TimingPointYesNo",
        "StopTransportAuthorityNumber",
        "Coord_Latitude",
        "Coord_Longitude",
        "CreatedDate",
        "LastUpdatedDate"
      )
    )

      dim_operator <- build_dim_operator()
      write_gold_dimension_governed(
      df = dim_operator,
      table_name = "dim_operator",
      surrogate_key = "OperatorKey",
      natural_key = "OperatorName",
      expected_columns = c(
        "OperatorKey",
        "OperatorName",
        "CreatedDate",
        "LastUpdatedDate"
      )
    )
    }

    # -------------------------------
    # Facts (incremental)
    # -------------------------------

    fact_journey <- build_fact_journey(
      window$start_date,
      window$end_date
    )
    write_gold_fact_governed(
      df = fact_journey,
      table_name = "fact_journey",
      grain_columns = c("DVJId"),
      foreign_keys = c("DateKey", "LineKey", "OperatorKey"),
      expected_columns = c(
        "DateKey",
        "OperatingDate",
        "DVJId",
        "LineKey",
        "OperatorKey",
        "DirectionNumber",
        "VehicleNumber",
        "JourneyCount",
        "TotalStops",
        "TotalArrivalDelaySeconds",
        "AvgArrivalDelaySeconds",
        "MaxArrivalDelaySeconds"
      )
    )

    fact_journeycall <- build_fact_journeycall(
      window$start_date,
      window$end_date
    )
    write_gold_fact_governed(
      df = fact_journeycall,
      table_name = "fact_journeycall",
      grain_columns = c("DVJId", "SequenceNumber"),
      foreign_keys = c("DateKey", "LineKey", "OperatorKey", "StopKey"),
      expected_columns = c(
        "DateKey",
        "OperatingDate",
        "DVJId",
        "SequenceNumber",
        "StopKey",
        "LineKey",
        "OperatorKey",
        "ArrivalDelaySeconds",
        "DepartureDelaySeconds",
        "DistanceFromPreviousStop",
        "DistanceToNextStop"
      )
    )

  }, error = function(e) {

    status <<- "FAILED"
    error_message <<- e$message

  })

  end_time <- Sys.time()

  run_log <- dplyr::tibble(
    RunId = run_id,
    StartTime = start_time,
    EndTime = end_time,
    Status = status,
    ErrorMessage = error_message
  )

  write_pipeline_run_log(run_log)

  if (status == "FAILED") {
    log_error("Gold Pipeline FAILED")
    stop(error_message)
  }

  log_info("Gold Pipeline Completed Successfully")
}