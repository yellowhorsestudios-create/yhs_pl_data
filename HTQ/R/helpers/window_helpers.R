get_operating_window <- function(days_back = 7) {

  today <- Sys.Date()

  start_date <- today - days_back

  return(list(
    start_date = start_date,
    end_date   = today
  ))
}

###########################################
# WATERMARK FILTER
###########################################


filter_window <- function(df, window) {
  df %>%
    dplyr::filter(
      OperatingDay >= as.Date(window$start_date) &
      OperatingDay <= as.Date(window$end_date)
    )
}