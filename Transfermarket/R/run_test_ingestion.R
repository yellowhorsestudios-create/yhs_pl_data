source("R/bootstrap.R")

init_logger(log_level = "DEBUG")

job_grid <- data.frame(
  team_name = "Arsenal",
  team_slug = "arsenal-fc",
  team_id = 11,
  season = 2025,
  stringsAsFactors = FALSE
)

test_pipeline <- RAW_PIPELINE %>%
  dplyr::filter(stage == "squad_stats")

run_raw_pipeline(test_pipeline, job_grid)