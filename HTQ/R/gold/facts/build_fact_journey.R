build_fact_journey <- function(start_date, end_date) {

  # --------------------------------------------------
  # Read Silver Journey (base)
  # --------------------------------------------------
  df_journey <- read_silver(
    "journey",
    start_date = start_date,
    end_date   = end_date
  )

  # --------------------------------------------------
  # Read Silver JourneyCall (for aggregation)
  # --------------------------------------------------
  df_call <- read_silver(
    "journeycall",
    start_date = start_date,
    end_date   = end_date
  )

  # --------------------------------------------------
  # Aggregate stop-level data to journey level
  # --------------------------------------------------
  agg_call <- df_call %>%
  dplyr::mutate(
    ArrivalDelaySeconds = as.numeric(
      difftime(
        ObservedArrivalDateTime,
        TimetabledLatestArrivalDateTime,
        units = "secs"
      )
    )
  ) %>%
  dplyr::group_by(DVJId) %>%
  dplyr::summarise(
    TotalStops = dplyr::n(),
    TotalArrivalDelaySeconds = safe_sum(ArrivalDelaySeconds),
    AvgArrivalDelaySeconds   = safe_mean(ArrivalDelaySeconds),
    MaxArrivalDelaySeconds   = safe_max(ArrivalDelaySeconds),
    .groups = "drop"
  )

  # --------------------------------------------------
  # Join aggregation back to journey
  # --------------------------------------------------
  df <- df_journey %>%
    dplyr::left_join(agg_call, by = "DVJId")

  # --------------------------------------------------
  # Add DateKey
  # --------------------------------------------------
  df$OperatingDate <- as.Date(df$OperatingDate)

  df$DateKey <- as.integer(
    format(df$OperatingDate, "%Y%m%d")
  )

  # --------------------------------------------------
  # JourneyCount
  # --------------------------------------------------
  df$JourneyCount <- 1

  # --------------------------------------------------
  # Replace NA aggregates with 0
  # --------------------------------------------------
  df$TotalStops[is.na(df$TotalStops)] <- 0

  # --------------------------------------------------
  # Read DimOperator (for surrogate key lookup)
  # --------------------------------------------------

  dim_operator <- arrow::open_dataset(
    get_gold_path("dim_operator")
  ) %>%
    dplyr::collect()
  
  # --------------------------------------------------
  # Normalize natural keys before FK resolution
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
    table_name = "fact_journey"
  )

  # ----------------------------------------
  # Read DimLine
  # ----------------------------------------

  dim_line <- arrow::open_dataset(
    get_gold_path("dim_line")
  ) %>%
    dplyr::collect()
  
  # --------------------------------------------------
  # Normalize natural keys before FK resolution
  # --------------------------------------------------

  df$LineNumber   <- as.character(df$LineNumber)

  # ----------------------------------------
  # Join LineKey
  # ----------------------------------------
  df <- resolve_fk(
    df,
    dim_line,
    natural_key = "LineNumber",
    surrogate_key = "LineKey",
    table_name = "fact_journey"
  )

  # --------------------------------------------------
  # Fact grain validation
  # --------------------------------------------------

  validate_no_duplicates(df, c("DVJId"))
  validate_not_null(df, c("DateKey", "LineKey", "OperatorKey"))

  # --------------------------------------------------
  # validate partition freshness
  # --------------------------------------------------

  validate_partition_freshness(
    fact_journey,
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
    LineKey,
    OperatorKey,
    DirectionNumber,
    VehicleNumber,
    JourneyCount,
    TotalStops,
    TotalArrivalDelaySeconds,
    AvgArrivalDelaySeconds,
    MaxArrivalDelaySeconds
  )

  df
}
