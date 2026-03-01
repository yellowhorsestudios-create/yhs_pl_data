library(dplyr)
library(stringr)
library(rvest)
library(glue)
library(purrr)
library(tibble)

scrape_player_profiles_raw <- function(player_urls) {
  
  start_time <- Sys.time()
  log_info("Running RAW player profile scraper")
  
  unique_urls <- unique(player_urls)
  log_info(glue("Unique players to scrape: {length(unique_urls)}"))
  
  results <- purrr::map(
    unique_urls,
    purrr::safely(function(url) {
      
      page <- safe_read_html(url)
      if (is.null(page)) return(NULL)
      
      player_id <- stringr::str_extract(url, "\\d+$")
      
      # ---------------------------
      # PLAYER NAME
      # ---------------------------
      
      player_name <- page %>%
        html_element("h1") %>%
        html_text2()
      
      # ---------------------------
      # PLAYER DATA TABLE
      # ---------------------------
      
      info_nodes <- page %>%
        html_elements("div.info-table span.info-table__content")
      
      labels <- page %>%
        html_elements("div.info-table span.info-table__label") %>%
        html_text2()
      
      values <- info_nodes %>% html_text2()
      
      info_tbl <- tibble(label = labels, value = values)
      
      get_value <- function(lbl) {
        info_tbl %>%
          filter(str_detect(label, fixed(lbl, ignore_case = TRUE))) %>%
          pull(value) %>%
          first()
      }
      
      citizenship <- page %>%
        html_element("span[itemprop='nationality'] img") %>%
        html_attr("title")
      
      social_media <- page %>%
        html_elements("div.socialmedia a") %>%
        html_attr("title") %>%
        paste(collapse = ", ")
      
      player_data <- tibble(
        player_id = player_id,
        profile_url = url,
        player_name = player_name,
        name_home_country = get_value("Name in home country"),
        date_of_birth = get_value("Date of birth"),
        age = str_extract(get_value("Date of birth"), "\\d+"),
        place_of_birth = get_value("Place of birth"),
        height = get_value("Height"),
        citizenship = citizenship,
        position = get_value("Position"),
        foot = get_value("Foot"),
        player_agent = get_value("Player agent"),
        current_club = get_value("Current club"),
        joined = get_value("Joined"),
        contract_expires = get_value("Contract expires"),
        social_media = social_media,
        source_url = url,
        scraped_at = Sys.time()
      )
      
      # ---------------------------
      # TRANSFER HISTORY TABLE
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
            player_id = player_id,
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
        player = player_data,
        transfers = transfer_data
      )
    })
  )
  
  # ---------------------------
  # ERROR HANDLING
  # ---------------------------
  
  errors <- purrr::map(results, "error")
  failure_index <- which(!purrr::map_lgl(errors, is.null))
  
  if (length(failure_index) > 0) {
    
    log_warning(glue("{length(failure_index)} player profile failures"))
    
    failed_urls <- unique_urls[failure_index]
    failed_errors <- errors[failure_index]
    
    for (i in seq_len(min(5, length(failed_urls)))) {
      log_warning(glue("FAILED URL: {failed_urls[i]}"))
      log_warning(glue("ERROR: {failed_errors[[i]]$message}"))
    }
  }
  
  # ---------------------------
  # BIND RESULTS
  # ---------------------------
  
  players_raw <- safe_bind_rows(
    purrr::map(results, ~ .x$result$player)
  )
  
  transfers_raw <- safe_bind_rows(
    purrr::map(results, ~ .x$result$transfers)
  )
  
  write_dataset_safe(players_raw, "raw_player_data")
  write_dataset_safe(transfers_raw, "raw_player_transfer_history")
  
  duration <- round(difftime(Sys.time(), start_time, units = "secs"), 2)
  log_info(glue("RAW player profile scraping completed in {duration} seconds"))
  
  invisible(list(players = players_raw, transfers = transfers_raw))
}