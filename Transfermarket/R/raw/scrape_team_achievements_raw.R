library(dplyr)
library(stringr)
library(rvest)
library(glue)
library(purrr)
library(tibble)

scrape_team_achievements_raw <- function(team_name,
                                         team_slug,
                                         team_id) {
  
  start_time <- Sys.time()
  
  url <- glue(
    "https://www.transfermarkt.com/{team_slug}/erfolge/verein/{team_id}"
  )
  
  log_info(glue("Loading team achievements: {team_name}"))
  
  page <- safe_read_html(url)
  
  if (is.null(page)) {
    log_warning("Achievements page returned NULL")
    return(empty_team_achievements_schema())
  }
  
  # ------------------------------------
  # Locate "All titles" section
  # ------------------------------------
  
  title_boxes <- page %>%
    html_elements("div.box")
  
  all_titles_box <- title_boxes %>%
    keep(~ str_detect(.x %>% html_text2(), 
                      fixed("All titles", ignore_case = TRUE))) %>%
    first()
  
  if (is.null(all_titles_box)) {
    log_warning("All titles section not found")
    return(empty_team_achievements_schema())
  }
  
  # ------------------------------------
  # Extract competitions inside section
  # ------------------------------------
  
  competitions <- all_titles_box %>%
    html_elements("div.erfolg_infotext_box")
  
  if (length(competitions) == 0) {
    log_warning("No title entries found")
    return(empty_team_achievements_schema())
  }
  
  raw_data <- purrr::map_dfr(competitions, function(comp) {
    
    title_name <- comp %>%
      html_element("h2") %>%
      html_text2()
    
    seasons_text <- comp %>%
      html_element("div.erfolg_infotext") %>%
      html_text2()
    
    if (is.na(seasons_text)) return(NULL)
    
    seasons <- str_split(seasons_text, ",")[[1]] %>%
      str_trim()
    
    tibble(
      title  = title_name,
      season = seasons
    )
  })
  
  if (nrow(raw_data) == 0) {
    log_warning("No valid achievements extracted")
    return(empty_team_achievements_schema())
  }
  
  raw_data <- raw_data %>%
    mutate(across(everything(), ~ na_if(.x, ""))) %>%
    mutate(across(everything(), as.character))
  
  raw_data <- raw_data %>%
    mutate(
      team_name  = team_name,
      team_slug  = team_slug,
      team_id    = as.character(team_id),
      source_url = url,
      scraped_at = Sys.time()
    )
  
  raw_data %>%
    select(
      team_name,
      team_slug,
      team_id,
      season,
      title,
      source_url,
      scraped_at
    )
}