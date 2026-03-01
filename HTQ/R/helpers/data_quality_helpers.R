###################################################
# validate not null
###################################################

validate_not_null <- function(df, cols) {

  for (col in cols) {
    if (any(is.na(df[[col]]))) {
      stop(paste("Data quality failure: NULL values detected in", col))
    }
  }
}

###################################################
# validate no duplicates
###################################################

validate_no_duplicates <- function(df, cols) {

  if (any(duplicated(df[cols]))) {
    stop(paste("Data quality failure: duplicate keys detected in",
               paste(cols, collapse = ", ")))
  }
}