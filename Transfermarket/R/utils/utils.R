library(dplyr)
library(glue)
library(stringr)
library(purrr)

# ---------------------------------------------------------
# 1️⃣ Rate limiting (politeness delay)
# ---------------------------------------------------------

rate_limit <- function(min_delay = 1) {
  Sys.sleep(runif(1, min_delay, min_delay + 0.5))
}

# ---------------------------------------------------------
# 2️⃣ Retry wrapper with exponential backoff
# ---------------------------------------------------------

retry <- function(expr, max_attempts = 3) {

  attempt <- 1

  while (attempt <= max_attempts) {

    result <- tryCatch(expr, error = function(e) e)

    if (!inherits(result, "error")) {
      return(result)
    }

    Sys.sleep(2 ^ attempt)
    attempt <- attempt + 1
  }

  stop(result$message)
}

# ---------------------------------------------------------
# 3️⃣ Safe HTML table extractor
# ---------------------------------------------------------

safe_table_extract <- function(page, css = "table.items") {

  node <- rvest::html_element(page, css)

  if (inherits(node, "xml_missing") || length(node) == 0) {
    return(tibble())
  }

  df <- rvest::html_table(node, fill = TRUE)

  # Convert to tibble with repaired names
  tibble::as_tibble(df, .name_repair = "unique")
}
# ---------------------------------------------------------
# 4️⃣ Clean numeric values (€, commas, etc.)
# ---------------------------------------------------------

clean_numeric <- function(x) {
  x %>%
    str_replace_all("[^0-9\\.]", "") %>%
    na_if("") %>%
    as.numeric()
}

# ---------------------------------------------------------
# 5️⃣ Clean text safely
# ---------------------------------------------------------

clean_text <- function(x) {
  x %>%
    str_squish() %>%
    na_if("")
}

# ---------------------------------------------------------
# 6️⃣ Safe bind_rows
# ---------------------------------------------------------

safe_bind_rows <- function(lst) {

  lst %>%
    purrr::compact() %>%
    { if (length(.) == 0) tibble() else bind_rows(.) }
}

# ---------------------------------------------------------
# 7️⃣ Add metadata columns
# ---------------------------------------------------------

add_metadata <- function(df, ...) {

  if (is.null(df)) {
    return(tibble())
  }

  df %>%
    mutate(
      scraped_at = Sys.time(),
      ...
    )
}

# ---------------------------------------------------------
# 8️⃣ Generate unique row id
# ---------------------------------------------------------

generate_row_id <- function(...) {
  glue::glue_collapse(c(...), sep = "_")
}