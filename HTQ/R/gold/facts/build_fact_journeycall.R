build_fact_journeycall <- function(start_date, end_date) {

  # --------------------------------------------------
  # Read Silver JourneyCall
  # --------------------------------------------------
  df <- read_silver(
    "journeycall",
    start_date = start_date,
    end_date   = end_date
  )

  # --------------------------------------------------
  # Add DateKey (Power BI join to DimDate)
  # --------------------------------------------------
  df$OperatingDate <- as.Date(df$OperatingDate)

  df$DateKey <- as.integer(
    format(df$OperatingDate, "%Y%m%d")
  )

  # --------------------------------------------------
  # Calculate Arrival Delay (seconds)
  # --------------------------------------------------
  df$ArrivalDelaySeconds <- ifelse(
    is.na(df$ObservedArrivalDateTime) |
    is.na(df$TimetabledLatestArrivalDateTime),
    NA_real_,
    as.numeric(
      difftime(
        df$ObservedArrivalDateTime,
        df$TimetabledLatestArrivalDateTime,
        units = "secs"
      )
    )
  )

  # --------------------------------------------------
  # Calculate Departure Delay (seconds)
  # --------------------------------------------------
  df$DepartureDelaySeconds <- ifelse(
    is.na(df$ObservedDepartureDateTime) |
    is.na(df$TimetabledEarliestDepartureDateTime),
    NA_real_,
    as.numeric(
      difftime(
        as.POSIXct(df$ObservedDepartureDateTime),
        as.POSIXct(df$TimetabledEarliestDepartureDateTime),
        units = "secs"
      )
    )
  )

  # --------------------------------------------------
  # Read DimOperator for surrogate key lookup
  # --------------------------------------------------
  dim_operator <- arrow::open_dataset(
    get_gold_path("dim_operator")
  ) %>%
    dplyr::collect()
  
  # --------------------------------------------------
  # Normalize natural key type before join
  # --------------------------------------------------

  df$OperatorName <- trimws(as.character(df$OperatorName))
  
  # --------------------------------------------------
  # Join OperatorKey
  # --------------------------------------------------
  df <- resolve_fk(
    df,
    dim_operator,
    natural_key = "OperatorName",
    surrogate_key = "OperatorKey",
    table_name = "fact_journeycall"
  )

  # --------------------------------------------------
  # Read DimLine for surrogate key lookup
  # --------------------------------------------------

  dim_line <- arrow::open_dataset(
    get_gold_path("dim_line")
    ) %>%
    dplyr::collect()
  
  # --------------------------------------------------
  # Normalize natural key type before join
  # --------------------------------------------------

  df$LineNumber   <- as.character(df$LineNumber)

  # --------------------------------------------------
  # Join LineKey
  # --------------------------------------------------
  df <- resolve_fk(
    df,
    dim_line,
    natural_key = "LineNumber",
    surrogate_key = "LineKey",
    table_name = "fact_journeycall"
  )

  # --------------------------------------------------
  # Read DimStop for surrogate key lookup
  # --------------------------------------------------
  
  dim_stop <- arrow::open_dataset(
    get_gold_path("dim_stop")
    ) %>%
    dplyr::collect()
  
  # --------------------------------------------------
  # Normalize natural key type before join
  # --------------------------------------------------

  df$StopAreaNumber <- trimws(as.character(df$StopAreaNumber))

  # --------------------------------------------------
  # Join StopKey
  # --------------------------------------------------

    df <- resolve_fk(
    df,
    dim_stop,
    natural_key = "StopAreaNumber",
    surrogate_key = "StopKey",
    table_name = "fact_journeycall"
  )

  # --------------------------------------------------
  # Fact grain validation
  # --------------------------------------------------

  validate_no_duplicates(df, c("DVJId", "SequenceNumber"))

  validate_not_null(
    df,
    c("DateKey", "LineKey", "StopKey", "OperatorKey")
  )

  # --------------------------------------------------
  # validate partition freshness
  # --------------------------------------------------

  validate_partition_freshness(
    fact_journeycall,
    window$start_date,
    window$end_date
  )

  # --------------------------------------------------
  # Final analytics selection
  # --------------------------------------------------
  
  df <- df %>%
    dplyr::select(
      DateKey,
      OperatingDate,
      DVJId,
      SequenceNumber,
      StopKey,
      LineKey,
      OperatorKey,
      ArrivalDelaySeconds,
      DepartureDelaySeconds,
      DistanceFromPreviousStop,
      DistanceToNextStop
    )

  df
}