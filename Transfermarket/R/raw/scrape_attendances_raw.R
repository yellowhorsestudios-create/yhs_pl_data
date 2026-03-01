library(dplyr)
library(stringr)
library(rvest)
library(glue)
library(purrr)
library(tibble)

scrape_attendances_raw <- function(team_name, team_slug, team_id) {
  
  start_time <- Sys.time()
  
  url <- glue(
    "https://www.transfermarkt.com/{team_slug}/besucherzahlenentwicklung/verein/{team_id}"
  )
  
  log_info(glue("Loading attendance data: {team_name}"))
  
  page <- safe_read_html(url)
  
  if (is.null(page)) {
    log_warning("Attendance page returned NULL")
    return(empty_attendance_schema())
  }
  
  table_node <- page %>% html_element("table.items")
  
  if (is.na(table_node)) {
    log_warning("Attendance table not found")
    return(empty_attendance_schema())
  }
  
  rows <- table_node %>% html_elements("tbody tr")
  
  if (length(rows) == 0) {
    log_warning("No attendance rows found")
    return(empty_attendance_schema())
  }
  
  raw_data <- purrr::map_dfr(rows, function(row) {
    
    cols <- row %>% html_elements("td")
    
    if (length(cols) < 5) return(NULL)
    
    tibble(
      season             = cols[1] %>% html_text2(),
      competition        = cols[2] %>% html_text2(),
      matches_total      = cols[3] %>% html_text2(),
      sold_out_total     = cols[4] %>% html_text2(),
      spectators_total   = cols[5] %>% html_text2(),
      average_attendance = cols[6] %>% html_text2()
    )
  })
  
  if (nrow(raw_data) == 0) {
    log_warning("Attendance extraction returned zero valid rows")
    return(empty_attendance_schema())
  }
  
  # Clean empty strings
  raw_data <- raw_data %>%
    mutate(across(everything(), ~ na_if(.x, ""))) %>%
    mutate(across(everything(), as.character))
  
  # Add metadata
  raw_data <- raw_data %>%
    mutate(
      team_name = team_name,
      team_slug = team_slug,
      team_id   = as.character(team_id),
      source_url = url,
      scraped_at = Sys.time()
    )
  
  # Enforce final column order
  raw_data %>%
    select(
      team_name,
      team_slug,
      team_id,
      season,
      competition,
      matches_total,
      sold_out_total,
      spectators_total,
      average_attendance,
      source_url,
      scraped_at
    )
}