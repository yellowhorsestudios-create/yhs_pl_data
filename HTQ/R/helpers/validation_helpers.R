##################################################################
# Strict FK Validation (Fail Fast)
##################################################################

validate_fk_not_null <- function(df, column_name, stage_name) {

  if (any(is.na(df[[column_name]]))) {

    missing_count <- sum(is.na(df[[column_name]]))

    log_error(
      paste0(
        stage_name,
        " - Foreign key resolution failed for column: ",
        column_name,
        " (", missing_count, " missing values)"
      )
    )

    stop(paste0("Pipeline stopped: Missing FK in ", stage_name))
  }

  invisible(TRUE)
}

##################################################################
# Fact-to-Fact Referential Validation
##################################################################


validate_journey_exists <- function(stop_df, journey_df) {

  missing_journeys <- anti_join(
    stop_df %>% select(OperatingDay, JourneyId) %>% distinct(),
    journey_df %>% select(OperatingDay, JourneyId) %>% distinct(),
    by = c("OperatingDay", "JourneyId")
  )

  if (nrow(missing_journeys) > 0) {

    log_error(
      paste0(
        "FactStopPassage contains ",
        nrow(missing_journeys),
        " orphan journeys."
      )
    )

    stop("Pipeline stopped: Orphan JourneyId detected.")
  }

  invisible(TRUE)
}

##################################################################
# Stage-Level Execution Guard
##################################################################

run_stage_safe <- function(stage_fun, stage_name, window = NULL) {

  log_info(paste0("Starting stage: ", stage_name))

  start_time <- Sys.time()

  tryCatch({

    if (is.null(window)) {
      stage_fun()
    } else {
      stage_fun(window)
    }

    duration <- round(
      as.numeric(difftime(Sys.time(), start_time, units = "secs")),
      2
    )

    log_info(
      paste0("Completed stage: ", stage_name,
             " (", duration, " seconds)")
    )

  }, error = function(e) {

    log_error(
      paste0("Stage failed: ", stage_name,
             " | Error: ", e$message)
    )

    stop("Silver pipeline halted.")
  })
}

###################################################
# validate gold schema
###################################################


validate_gold_schema <- function(df, expected_columns) {

  missing <- setdiff(expected_columns, names(df))
  extra   <- setdiff(names(df), expected_columns)

  if (length(missing) > 0) {
    stop(paste("Schema validation failed. Missing columns:",
               paste(missing, collapse = ", ")))
  }

  if (length(extra) > 0) {
    warning(paste("Schema contains extra columns:",
                  paste(extra, collapse = ", ")))
  }
}

###################################################
# validate freshness silver
###################################################

validate_silver_freshness <- function(
  df,
  max_allowed_lag_days = 2
) {

  if (!"OperatingDate" %in% names(df)) {
    stop("Silver freshness validation failed: OperatingDate missing.")
  }

  max_date <- max(as.Date(df$OperatingDate), na.rm = TRUE)

  if (is.infinite(max_date)) {
    stop("Silver freshness validation failed: no valid dates found.")
  }

  today <- Sys.Date()
  lag_days <- as.numeric(today - max_date)

  if (lag_days > max_allowed_lag_days) {
    stop(paste0(
      "Silver freshness validation failed. Latest OperatingDate: ",
      max_date,
      " | Lag days: ",
      lag_days
    ))
  }

  TRUE
}

###################################################
# validate partition freshness gold
###################################################


validate_partition_freshness <- function(
  df,
  start_date,
  end_date,
  max_allowed_lag_days = 2
) {

  # -----------------------------------------
  # Basic freshness (latest date check)
  # -----------------------------------------
  max_date <- max(as.Date(df$OperatingDate), na.rm = TRUE)
  today <- Sys.Date()

  lag_days <- as.numeric(today - max_date)

  if (lag_days > max_allowed_lag_days) {
    stop(paste0(
      "Freshness validation failed. Latest OperatingDate: ",
      max_date,
      " | Lag days: ",
      lag_days
    ))
  }

  # -----------------------------------------
  # Window completeness validation
  # -----------------------------------------
  expected_dates <- seq.Date(
    from = as.Date(start_date),
    to   = as.Date(end_date),
    by   = "day"
  )

  actual_dates <- unique(as.Date(df$OperatingDate))

  missing_dates <- setdiff(expected_dates, actual_dates)

  if (length(missing_dates) > 0) {
    stop(paste0(
      "Partition completeness validation failed. Missing dates: ",
      paste(head(missing_dates, 5), collapse = ", ")
    ))
  }

  TRUE
}

###################################################
# dedupe silver
###################################################

deduplicate_silver <- function(df, table_name) {

  keys <- silver_keys[[table_name]]

  if (is.null(keys)) {
    stop(paste0("No key defined for table: ", table_name))
  }

  df %>%
    dplyr::distinct(
      dplyr::across(dplyr::all_of(keys)),
      .keep_all = TRUE
    )
}

###################################################
# validate silverschema
###################################################

validate_schema <- function(df, table_name) {

  expected_cols <- silver_schema[[table_name]]
  actual_cols <- colnames(df)

  missing_cols <- setdiff(expected_cols, actual_cols)

  if (length(missing_cols) > 0) {
    stop(paste0(
      "Schema validation failed. Missing columns: ",
      paste(missing_cols, collapse = ", ")
    ))
  }
}