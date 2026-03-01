library(dplyr)
library(purrr)
library(glue)

run_scraper <- function(jobs, scraper_fun, scraper_name) {

  start_time <- Sys.time()

  log_info(glue("Running scraper: {scraper_name}"))

  results <- purrr::pmap(
    list(jobs$team_name, jobs$team_slug, jobs$team_id, jobs$season),
    purrr::safely(scraper_fun)
  )

  errors <- purrr::map(results, "error")
  failure_index <- which(!purrr::map_lgl(errors, is.null))

  if (length(failure_index) > 0) {

    log_warning(glue("Failures detected in {scraper_name}"))

    failure_df <- jobs[failure_index, ] %>%
      mutate(error_message = purrr::map_chr(errors[failure_index], "message"))

    print(failure_df[, c("team_slug", "season", "error_message")], n = 50)

    log_warning(glue("{length(failure_index)} failures in {scraper_name}"))
  }

  result_list <- purrr::map(results, "result")
  data <- safe_bind_rows(result_list)

  write_dataset_safe(data, scraper_name)

  duration <- round(difftime(Sys.time(), start_time, units = "secs"), 2)

  log_info(glue(
    "Scraper {scraper_name} completed in {duration} seconds"
  ))

  invisible(data)
}

run_team_scraper <- function(teams, scraper_fun, scraper_name) {

  start_time <- Sys.time()

  log_info(glue("Running team-level scraper: {scraper_name}"))

  results <- purrr::pmap(
    list(teams$team_name, teams$team_slug, teams$team_id),
    purrr::safely(scraper_fun)
  )

  errors <- purrr::map(results, "error")
  failure_index <- which(!purrr::map_lgl(errors, is.null))

  if (length(failure_index) > 0) {

    log_warning(glue("Failures detected in {scraper_name}"))

    failure_df <- teams[failure_index, ] %>%
      mutate(error_message = purrr::map_chr(errors[failure_index], "message"))

    print(failure_df[, c("team_slug", "error_message")], n = 50)

    log_warning(glue("{length(failure_index)} failures in {scraper_name}"))
  }

  result_list <- purrr::map(results, "result")
  data <- safe_bind_rows(result_list)

  write_dataset_safe(data, scraper_name)

  duration <- round(difftime(Sys.time(), start_time, units = "secs"), 2)

  log_info(glue(
    "Team-level scraper {scraper_name} completed in {duration} seconds"
  ))

  invisible(data)
}