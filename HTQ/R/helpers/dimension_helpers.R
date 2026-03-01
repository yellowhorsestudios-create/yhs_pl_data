upsert_dimension <- function(
  existing_dim,
  new_keys,
  natural_key,
  surrogate_key
) {

  now_ts <- Sys.time()

  # -------------------------------------------------
  # Initial load
  # -------------------------------------------------
  if (nrow(existing_dim) == 0) {

    new_keys[[surrogate_key]] <- seq_len(nrow(new_keys))
    new_keys$CreatedDate <- now_ts
    new_keys$LastUpdatedDate <- now_ts

    return(new_keys)
  }

  # Ensure audit columns exist in existing dim
  if (!"CreatedDate" %in% names(existing_dim)) {
    existing_dim$CreatedDate <- now_ts
  }

  if (!"LastUpdatedDate" %in% names(existing_dim)) {
    existing_dim$LastUpdatedDate <- now_ts
  }

  # -------------------------------------------------
  # Identify new natural keys
  # -------------------------------------------------
  missing <- dplyr::anti_join(
    new_keys,
    existing_dim,
    by = natural_key
  )

  if (nrow(missing) > 0) {

    max_key <- max(existing_dim[[surrogate_key]], na.rm = TRUE)

    missing[[surrogate_key]] <-
      seq_len(nrow(missing)) + max_key

    missing$CreatedDate <- now_ts
    missing$LastUpdatedDate <- now_ts
  }

  # -------------------------------------------------
  # Identify updates (Type 1)
  # -------------------------------------------------
  joined <- dplyr::left_join(
    existing_dim,
    new_keys,
    by = natural_key,
    suffix = c("_old", "_new")
  )

  attribute_cols <- setdiff(
    colnames(new_keys),
    natural_key
  )

  for (col in attribute_cols) {

    old_col <- paste0(col, "_old")
    new_col <- paste0(col, "_new")

    changed <- !is.na(joined[[new_col]]) &
               joined[[old_col]] != joined[[new_col]]

    joined[[col]] <- ifelse(
      changed,
      joined[[new_col]],
      joined[[old_col]]
    )

    # Update LastUpdatedDate only if changed
    joined$LastUpdatedDate <- ifelse(
      changed,
      now_ts,
      joined$LastUpdatedDate
    )
  }

  # Clean helper columns
  updated_existing <- joined %>%
    dplyr::select(
      all_of(surrogate_key),
      all_of(natural_key),
      all_of(attribute_cols),
      CreatedDate,
      LastUpdatedDate
    )

  # -------------------------------------------------
  # Final combine
  # -------------------------------------------------
  final_dim <- dplyr::bind_rows(
    updated_existing,
    missing
  )

  return(final_dim)
}