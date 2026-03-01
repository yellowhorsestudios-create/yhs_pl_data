run_pipeline_stage <- function(step, jobs) {

  log_info(glue::glue("===== PIPELINE STEP: {step$name} ====="))

  # ---------------- RAW ----------------
  if (!is.null(step$scraper)) {

    log_info("Running RAW ingestion")

    raw_data <- run_scraper(
      jobs,
      step$scraper,
      step$name
    )

    write_dataset_safe(raw_data, glue::glue("raw/{step$name}"))
  }

  # ---------------- STAGING ----------------
  if (!is.null(step$staging)) {

    log_info("Running STAGING transform")

    raw <- read_dataset_safe(glue::glue("raw/{step$name}"))

    clean <- step$staging(raw)

    validate_staging(step$name, clean)

    write_dataset_safe(clean,
                       glue::glue("staging/{step$name}"))
  }

  # ---------------- CORE ----------------
  if (!is.null(step$core)) {

    log_info("Building CORE table")

    step$core()
  }

  invisible(TRUE)
}