# ---------------------------------------------------------
# Global options
# ---------------------------------------------------------

options(
  tm_use_cache = TRUE,
  tm_cache_dir = "data/cache"
)

library(dplyr)
library(purrr)
library(tidyr)
library(glue)
library(arrow)
library(lubridate)

# ---------------------------------------------------------
# Load configuration
# ---------------------------------------------------------

source("R/config/teams.R")
source("R/config/seasons.R")

# ---------------------------------------------------------
# Load core modules
# ---------------------------------------------------------

source("R/core/logger.R")
source("R/core/http.R")
source("R/core/utils.R")
source("R/core/runner.R")
source("R/core/io.R")

# ---------------------------------------------------------
# Load scrapers
# ---------------------------------------------------------

source("R/scrapers/scrape_squad_stats.R")
source("R/scrapers/scrape_attendances.R")
source("R/scrapers/scrape_fixtures.R")
source("R/scrapers/scrape_staff_history.R")
source("R/scrapers/scrape_market_values.R")
source("R/scrapers/collect_player_links.R")
source("R/scrapers/scrape_stadium.R")
source("R/scrapers/scrape_player_profiles_unique.R")

# ---------------------------------------------------------
# Initialize logging
# ---------------------------------------------------------

init_logger("logs")

# ---------------------------------------------------------
# Build jobs
# ---------------------------------------------------------

jobs <- tidyr::crossing(
  teams,
  season = seasons
)

log_info(glue("Total jobs to process: {nrow(jobs)}"))

# ---------------------------------------------------------
# Register scrapers
# ---------------------------------------------------------

team_scrapers <- list(
  squad_stats   = scrape_squad_stats,
  fixtures      = scrape_fixtures,
  staff_history = scrape_staff_history,
  stadiums      = scrape_stadium
)

standalone_scrapers <- list(
  player_links  = collect_player_links,
  market_values = scrape_market_values
)

# =========================================================
# STAGE 1 — Team + Season Scrapers
# =========================================================

log_info("STAGE 1 — Running team + season scrapers")

purrr::iwalk(team_scrapers, function(scraper_fun, scraper_name) {

  run_scraper(
    jobs = jobs,
    scraper_fun = scraper_fun,
    scraper_name = scraper_name
  )

})

# =========================================================
# STAGE 2 — Team-Level Scrapers
# =========================================================

log_info("STAGE 2 — Running team-level scrapers")

run_team_scraper(
  teams = teams,
  scraper_fun = scrape_attendances,
  scraper_name = "attendances"
)

# =========================================================
# STAGE 3 — Standalone + Derived Datasets
# =========================================================

log_info("STAGE 3 — Running standalone scrapers")

purrr::iwalk(standalone_scrapers, function(scraper_fun, scraper_name) {

  log_info(glue("Running standalone scraper: {scraper_name}"))

  result <- purrr::safely(scraper_fun)()

  if (!is.null(result$error)) {
    log_error(glue("[{scraper_name}] {result$error$message}"))
    return(NULL)
  }

  if (is.null(result$result) || nrow(result$result) == 0) {
    log_info(glue("No data returned for {scraper_name}"))
    return(NULL)
  }

  arrow::write_parquet(
    result$result,
    glue("data/{scraper_name}.parquet")
  )

  log_info(glue("✅ {scraper_name} completed ({nrow(result$result)} rows)"))
})

# =========================================================
# STAGE 4 — Player Profile Enrichment
# =========================================================

log_info("STAGE 4 — Scraping unique player profiles")

if (!file.exists("data/player_links.parquet")) {
  stop("player_links.parquet not found — ingestion incomplete.")
}

player_links_df <- arrow::read_parquet("data/player_links.parquet")
unique_urls <- player_links_df$profile_url

profile_result <- purrr::safely(scrape_player_profiles_unique)(unique_urls)

if (!is.null(profile_result$error)) {
  log_error(glue("[player_profiles] {profile_result$error$message}"))
} else {
  log_info("✅ player profile scraping completed")
}

log_info("INGESTION COMPLETE")