#################################################
# resolve fk
#################################################

resolve_fk <- function(
  df,
  dim_df,
  natural_key,
  surrogate_key,
  table_name
) {

  # -----------------------------------------
  # Type alignment (force character)
  # -----------------------------------------
  df[[natural_key]] <- as.character(df[[natural_key]])
  dim_df[[natural_key]] <- as.character(dim_df[[natural_key]])

  # -----------------------------------------
  # Join surrogate key
  # -----------------------------------------
  df_joined <- df %>%
    dplyr::left_join(
      dim_df %>% dplyr::select({{ natural_key }}, {{ surrogate_key }}),
      by = natural_key
    )

  # -----------------------------------------
  # Referential integrity validation
  # -----------------------------------------
  if (any(is.na(df_joined[[surrogate_key]]))) {

    missing_keys <- df_joined %>%
      dplyr::filter(is.na(.data[[surrogate_key]])) %>%
      dplyr::distinct(.data[[natural_key]]) %>%
      head(10)

    stop(paste(
      "Foreign key resolution failed in",
      table_name,
      "| Missing natural keys:",
      paste(missing_keys[[natural_key]], collapse = ", ")
    ))
  }

  return(df_joined)
}

#################################################
# safe max
#################################################

safe_max <- function(x) {
  if (all(is.na(x))) return(NA_real_)
  max(x, na.rm = TRUE)
}

#################################################
# safe mean
#################################################

safe_mean <- function(x) {
  if (all(is.na(x))) return(NA_real_)
  mean(x, na.rm = TRUE)
}

#################################################
# safe sum
#################################################

safe_sum <- function(x) {
  if (all(is.na(x))) return(NA_real_)
  sum(x, na.rm = TRUE)
}

