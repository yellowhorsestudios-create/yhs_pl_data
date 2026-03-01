library(dplyr)
library(stringr)
library(rvest)
library(glue)
library(purrr)
library(tibble)

scrape_managers_raw <- function(team_name,
                                team_slug,
                                team_id) {
  
  start_time <- Sys.time()
  
  entry_url <- glue(
    "https://www.transfermarkt.com/{team_slug}/mitarbeiterhistorie/verein/{team_id}/personalie_id/1"
  )
  
  log_info(glue("Loading managers for {team_name}"))
  
  entry_page <- safe_read_html(entry_url)
  
  if (is.null(entry_page)) {
    log_warning("Manager entry page NULL")
    return(empty_managers_bundle())
  }
  
  # ---------------------------------------------------
  # Extract manager links
  # ---------------------------------------------------
  
  manager_nodes <- entry_page %>%
    html_elements("table.items tbody tr td.hauptlink a")
  
  manager_names <- manager_nodes %>% html_text2()
  manager_links <- manager_nodes %>% html_attr("href")
  
  manager_urls <- paste0("https://www.transfermarkt.com", manager_links)
  manager_ids  <- stringr::str_extract(manager_links, "\\d+$")
  
  team_managers <- tibble(
    team_name = team_name,
    team_slug = team_slug,
    team_id   = as.character(team_id),
    manager_name = manager_names,
    manager_id   = manager_ids,
    profile_url  = manager_urls,
    source_url   = entry_url,
    scraped_at   = Sys.time()
  )
  
  # ---------------------------------------------------
  # Visit each manager profile
  # ---------------------------------------------------
  
  results <- purrr::map(
    manager_urls,
    purrr::safely(function(url) {
      
      page <- safe_read_html(url)
      if (is.null(page)) return(NULL)
      
      manager_id <- stringr::str_extract(url, "\\d+$")
      
      # ---------------------------
      # PERSONAL DETAILS
      # ---------------------------
      
      labels <- page %>%
        html_elements("div.info-table span.info-table__label") %>%
        html_text2()
      
      values <- page %>%
        html_elements("div.info-table span.info-table__content") %>%
        html_text2()
      
      info_tbl <- tibble(label = labels, value = values)
      
      get_val <- function(lbl) {
        info_tbl %>%
          filter(str_detect(label, fixed(lbl, ignore_case = TRUE))) %>%
          pull(value) %>%
          first()
      }
      
      citizenship <- page %>%
        html_element("span[itemprop='nationality'] img") %>%
        html_attr("title")
      
      personal <- tibble(
        manager_id = manager_id,
        profile_url = url,
        full_name = page %>% html_element("h1") %>% html_text2(),
        name_home_country = get_val("Name in home country"),
        date_of_birth = get_val("Date of birth"),
        age = str_extract(get_val("Date of birth"), "\\d+"),
        place_of_birth = get_val("Place of birth"),
        citizenship = citizenship,
        contract_until = get_val("Contract until"),
        avg_term_as_coach = get_val("Avg.term as coach"),
        coaching_licence = get_val("Coaching Licence"),
        preferred_formation = get_val("Preferred formation"),
        agent = get_val("Agent"),
        source_url = url,
        scraped_at = Sys.time()
      )
      
      # ---------------------------
      # COACHING STATS
      # ---------------------------
      
      stats_table <- page %>%
        html_element("div.box:contains('Stats') table")
      
      stats_data <- tibble()
      
      if (!is.na(stats_table)) {
        rows <- stats_table %>% html_elements("tbody tr")
        
        stats_data <- purrr::map_dfr(rows, function(row) {
          cols <- row %>% html_elements("td")
          if (length(cols) < 7) return(NULL)
          
          tibble(
            manager_id = manager_id,
            profile_url = url,
            season = cols[1] %>% html_text2(),
            competition = cols[2] %>% html_text2(),
            matches = cols[3] %>% html_text2(),
            wins = cols[4] %>% html_text2(),
            draws = cols[5] %>% html_text2(),
            losses = cols[6] %>% html_text2(),
            points = cols[7] %>% html_text2(),
            ppm = cols[8] %>% html_text2(),
            source_url = url,
            scraped_at = Sys.time()
          )
        })
      }
      
      # ---------------------------
      # TRANSFER HISTORY (PLAYER)
      # ---------------------------
      
      transfer_table <- page %>%
        html_element("div.box:contains('Transfer history') table")
      
      transfer_data <- tibble()
      
      if (!is.na(transfer_table)) {
        rows <- transfer_table %>% html_elements("tbody tr")
        
        transfer_data <- purrr::map_dfr(rows, function(row) {
          cols <- row %>% html_elements("td")
          if (length(cols) < 6) return(NULL)
          
          tibble(
            manager_id = manager_id,
            profile_url = url,
            season = cols[1] %>% html_text2(),
            transfer_date = cols[2] %>% html_text2(),
            left_club = cols[3] %>% html_text2(),
            joined_club = cols[4] %>% html_text2(),
            market_value = cols[5] %>% html_text2(),
            fee = cols[6] %>% html_text2(),
            source_url = url,
            scraped_at = Sys.time()
          )
        })
      }
      
      list(
        personal = personal,
        stats = stats_data,
        transfers = transfer_data
      )
    })
  )
  
  personal_raw <- safe_bind_rows(map(results, ~ .x$result$personal))
  stats_raw    <- safe_bind_rows(map(results, ~ .x$result$stats))
  transfers_raw<- safe_bind_rows(map(results, ~ .x$result$transfers))
  
  duration <- round(difftime(Sys.time(), start_time, units = "secs"), 2)
  log_info(glue("Manager scraping completed in {duration} seconds"))
  
  list(
    team_managers = team_managers,
    manager_profiles = personal_raw,
    manager_stats = stats_raw,
    manager_transfers = transfers_raw
  )
}