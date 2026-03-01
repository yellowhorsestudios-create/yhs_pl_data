library(dplyr)
library(stringr)
library(rvest)
library(glue)
library(purrr)
library(tibble)

scrape_fixtures_raw <- function(team_name,
                                team_slug,
                                team_id,
                                season_id,
                                competition_id) {
  
  start_time <- Sys.time()
  
  url <- glue(
    "https://www.transfermarkt.com/{team_slug}/spielplandatum/verein/{team_id}/plus/0?saison_id={season_id}&wettbewerb_id={competition_id}&day=&heim_gast=&punkte=&datum_von=&datum_bis="
  )
  
  log_info(glue("Loading fixtures: {team_name} | Season {season_id}"))
  
  page <- safe_read_html(url)
  
  if (is.null(page)) {
    log_warning("Fixtures page returned NULL")
    return(empty_fixtures_schema())
  }
  
  table_node <- page %>% html_element("table.items")
  
  if (is.na(table_node)) {
    log_warning("Fixtures table not found")
    return(empty_fixtures_schema())
  }
  
  rows <- table_node %>% html_elements("tbody tr")
  
  if (length(rows) == 0) {
    log_warning("No fixture rows found")
    return(empty_fixtures_schema())
  }
  
  raw_data <- purrr::map_dfr(rows, function(row) {
    
    cols <- row %>% html_elements("td")
    if (length(cols) < 9) return(NULL)
    
    opponent_node <- row %>% html_element("td:nth-child(6) a")
    
    opponent_name <- opponent_node %>% html_text2()
    opponent_href <- opponent_node %>% html_attr("href")
    opponent_id   <- stringr::str_extract(opponent_href, "\\d+$")
    
    tibble(
      matchday        = cols[1] %>% html_text2(),
      match_date      = cols[2] %>% html_text2(),
      match_time      = cols[3] %>% html_text2(),
      venue           = cols[4] %>% html_text2(),
      ranking         = cols[5] %>% html_text2(),
      opponent_name   = opponent_name,
      opponent_id     = opponent_id,
      system_of_play  = cols[7] %>% html_text2(),
      attendance      = cols[8] %>% html_text2(),
      result          = cols[9] %>% html_text2()
    )
  })
  
  if (nrow(raw_data) == 0) {
    log_warning("Fixtures extraction returned zero valid rows")
    return(empty_fixtures_schema())
  }
  
  raw_data <- raw_data %>%
    mutate(across(everything(), ~ na_if(.x, ""))) %>%
    mutate(across(everything(), as.character))
  
  raw_data <- raw_data %>%
    mutate(
      team_name      = team_name,
      team_slug      = team_slug,
      team_id        = as.character(team_id),
      season_id      = as.character(season_id),
      competition_id = as.character(competition_id),
      source_url     = url,
      scraped_at     = Sys.time()
    )
  
  raw_data %>%
    select(
      team_name,
      team_slug,
      team_id,
      season_id,
      competition_id,
      matchday,
      match_date,
      match_time,
      venue,
      ranking,
      opponent_name,
      opponent_id,
      system_of_play,
      attendance,
      result,
      source_url,
      scraped_at
    )
}