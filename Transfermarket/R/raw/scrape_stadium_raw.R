library(dplyr)
library(stringr)
library(rvest)
library(glue)
library(purrr)
library(tibble)

scrape_stadium_raw <- function(team_name,
                               team_slug,
                               team_id,
                               season) {
  
  start_time <- Sys.time()
  
  url <- glue(
    "https://www.transfermarkt.com/{team_slug}/stadion/verein/{team_id}/saison_id/{season}"
  )
  
  log_info(glue("Loading stadium data: {team_name} | Season {season}"))
  
  page <- safe_read_html(url)
  
  if (is.null(page)) {
    log_warning("Stadium page returned NULL")
    return(empty_stadium_schema())
  }
  
  # ---------------------------
  # STADIUM NAME
  # ---------------------------
  
  stadium_name <- page %>%
    html_element("h1") %>%
    html_text2()
  
  # ---------------------------
  # LABEL / VALUE TABLE
  # ---------------------------
  
  labels <- page %>%
    html_elements("div.data-header__box--small span.data-header__label") %>%
    html_text2()
  
  values <- page %>%
    html_elements("div.data-header__box--small span.data-header__content") %>%
    html_text2()
  
  if (length(labels) == 0) {
    log_warning("No stadium detail labels found")
    return(empty_stadium_schema())
  }
  
  info_tbl <- tibble(
    label = labels,
    value = values
  )
  
  get_value <- function(lbl) {
    info_tbl %>%
      filter(str_detect(label, fixed(lbl, ignore_case = TRUE))) %>%
      pull(value) %>%
      first()
  }
  
  raw_data <- tibble(
    stadium_name       = stadium_name,
    total_capacity     = get_value("Total capacity"),
    seats              = get_value("Seats"),
    built              = get_value("Built"),
    construction_costs = get_value("Construction costs"),
    formerly           = get_value("Formerly"),
    undersoil_heating  = get_value("Undersoil heating"),
    running_track      = get_value("Running track"),
    surface            = get_value("Surface"),
    pitch_size         = get_value("Pitch size")
  )
  
  raw_data <- raw_data %>%
    mutate(across(everything(), ~ na_if(.x, ""))) %>%
    mutate(across(everything(), as.character))
  
  raw_data <- raw_data %>%
    mutate(
      team_name  = team_name,
      team_slug  = team_slug,
      team_id    = as.character(team_id),
      season     = as.character(season),
      source_url = url,
      scraped_at = Sys.time()
    )
  
  raw_data %>%
    select(
      team_name,
      team_slug,
      team_id,
      season,
      stadium_name,
      total_capacity,
      seats,
      built,
      construction_costs,
      formerly,
      undersoil_heating,
      running_track,
      surface,
      pitch_size,
      source_url,
      scraped_at
    )
}