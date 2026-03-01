library(dplyr)
library(stringr)
library(janitor)
library(rvest)
library(glue)
library(purrr)
library(tibble)

scrape_squad_stats_raw <- function(team_name, team_slug, team_id, season) {
  
  url <- glue(
    "https://www.transfermarkt.com/{team_slug}/leistungsdaten/verein/{team_id}/plus/1?reldata=GB1%26{season}"
  )
  
  log_info(glue("Loading squad stats: {team_name} | {season}"))
  
  page <- safe_read_html(url)
  
  if (is.null(page)) {
    log_warning("Page returned NULL")
    return(empty_squad_schema())
  }
  
  table_node <- page %>% html_element("table.items")
  
  if (is.na(table_node)) {
    log_warning("Squad table not found")
    return(empty_squad_schema())
  }
  
  rows <- table_node %>% html_elements("tbody tr")
  
  if (length(rows) == 0) {
    log_warning("No table rows found")
    return(empty_squad_schema())
  }
  
  # ---- Extract row-level data ----
  
  raw_data <- map_dfr(rows, function(row) {
    
    cols <- row %>% html_elements("td")
    
    if (length(cols) < 10) {
      return(NULL)
    }
    
    player_node <- row %>% html_element("td.hauptlink a")
    
    player_name <- player_node %>% html_text2()
    player_href <- player_node %>% html_attr("href")
    
    nationality <- row %>%
      html_element("img.flaggenrahmen") %>%
      html_attr("title")
    
    tibble(
      squad_number   = cols[1] %>% html_text2(),
      player_name    = player_name,
      age            = cols[3] %>% html_text2(),
      in_squad       = cols[4] %>% html_text2(),
      appearances    = cols[5] %>% html_text2(),
      goals          = cols[6] %>% html_text2(),
      assists        = cols[7] %>% html_text2(),
      yellow_cards   = cols[8] %>% html_text2(),
      second_yellow  = cols[9] %>% html_text2(),
      red_cards      = cols[10] %>% html_text2(),
      sub_on         = cols[11] %>% html_text2(),
      sub_off        = cols[12] %>% html_text2(),
      points_per_game= cols[13] %>% html_text2(),
      minutes        = cols[14] %>% html_text2(),
      nationality    = nationality,
      profile_url    = paste0("https://www.transfermarkt.com", player_href),
      player_id      = stringr::str_extract(player_href, "\\d+$")
    )
  })
  
  if (nrow(raw_data) == 0) {
    log_warning("No valid player rows extracted")
    return(empty_squad_schema())
  }
  
  # ---- Clean values ----
  
  raw_data <- raw_data %>%
    mutate(across(everything(), ~na_if(.x, ""))) %>%
    mutate(across(everything(), as.character))
  
  # ---- Enforce schema ----
  
  raw_data <- raw_data %>%
    select(
      squad_number,
      player_id,
      player_name,
      profile_url,
      age,
      nationality,
      in_squad,
      appearances,
      goals,
      assists,
      yellow_cards,
      second_yellow,
      red_cards,
      sub_on,
      sub_off,
      points_per_game,
      minutes
    )
  
  # ---- Add metadata ----
  
  raw_data <- raw_data %>%
    mutate(
      team_name = team_name,
      team_slug = team_slug,
      team_id   = as.character(team_id),
      season    = as.character(season),
      source_url = url,
      scraped_at = Sys.time()
    )
  
  # Final column order
  raw_data %>%
    select(
      team_name,
      team_slug,
      team_id,
      season,
      squad_number,
      player_id,
      player_name,
      profile_url,
      age,
      nationality,
      in_squad,
      appearances,
      goals,
      assists,
      yellow_cards,
      second_yellow,
      red_cards,
      sub_on,
      sub_off,
      points_per_game,
      minutes,
      source_url,
      scraped_at
    )
}