calculate_checksum <- function(df, key_cols) {
  paste0(
    sum(as.numeric(as.factor(
      apply(df[key_cols], 1, paste, collapse = "|")
    ))),
    collapse = ""
  )
}