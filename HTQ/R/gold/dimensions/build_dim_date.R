build_dim_date <- function(start_date, end_date) {

  dates <- seq.Date(
    from = as.Date(start_date),
    to   = as.Date(end_date),
    by   = "day"
  )

  df <- data.frame(
    Date = dates
  )

  df$DateKey <- as.integer(format(df$Date, "%Y%m%d"))
  df$Year <- as.integer(format(df$Date, "%Y"))
  df$Month <- as.integer(format(df$Date, "%m"))
  df$MonthName <- format(df$Date, "%B")
  df$Week <- as.integer(format(df$Date, "%U"))
  df$Weekday <- weekdays(df$Date)
  df$IsWeekend <- df$Weekday %in% c("Saturday", "Sunday")

  df
}