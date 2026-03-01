cat("Sourcing http_utils\n")
source("R/utils/http_utils.R")
cat("Done sourcing http_utils\n")

# Load libraries
library(dplyr)
library(arrow)
library(httr)
library(rvest)
library(glue)
library(digest)


# Source all utils
source("R/utils/logger.R")
source("R/utils/http_utils.R")
source("R/utils/orchestration_utils.R")
source("R/utils/io_utils.R")
source("R/utils/utils.R")


# Source scrapers
source("R/raw/scrape_squad_stats_raw.R")
source("R/raw/scrape_player_profiles_raw.R")
source("R/raw/scrape_fixtures_raw.R")
source("R/raw/scrape_attendances_raw.R")
source("R/raw/scrape_stadium_raw.R")
source("R/raw/scrape_team_achievements_raw.R")
source("R/raw/scrape_managers_raw.R")

# Source pipeline
source("R/pipeline/pipeline_registry.R")
source("R/pipeline/pipeline_runner.R")