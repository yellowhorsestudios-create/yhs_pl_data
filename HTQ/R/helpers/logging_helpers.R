# ---------------------------------------
# Logging Configuration
# ---------------------------------------

.LOG_LEVELS <- c("DEBUG", "INFO", "WARNING", "ERROR")

.LOG_CONFIG <- list(
  level = "INFO",
  log_to_file = FALSE,
  file_path = NULL
)

# ---------------------------------------
# Initialize Logger
# ---------------------------------------

init_logger <- function(
  level = "INFO",
  log_to_file = FALSE,
  file_path = "logs/silver_pipeline.log"
) {

  level <- toupper(level)

  if (!level %in% .LOG_LEVELS) {
    stop("Invalid log level.")
  }

  .LOG_CONFIG$level <<- level
  .LOG_CONFIG$log_to_file <<- log_to_file
  .LOG_CONFIG$file_path <<- file_path

  if (log_to_file) {
    dir.create(dirname(file_path), recursive = TRUE, showWarnings = FALSE)
  }

  invisible(TRUE)
}

# ---------------------------------------
# Internal Log Writer
# ---------------------------------------

.write_log <- function(level, message) {

  current_level_index <- match(.LOG_CONFIG$level, .LOG_LEVELS)
  message_level_index <- match(level, .LOG_LEVELS)

  if (message_level_index < current_level_index) {
    return(invisible(NULL))
  }

  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

  formatted_message <- paste0(
    "[", level, "] ",
    timestamp,
    " | ",
    message
  )

  # Console
  cat(formatted_message, "\n")

  # Optional file logging
  if (.LOG_CONFIG$log_to_file) {
    cat(
      formatted_message,
      "\n",
      file = .LOG_CONFIG$file_path,
      append = TRUE
    )
  }

  invisible(TRUE)
}

# ---------------------------------------
# Public Log Functions
# ---------------------------------------

log_info <- function(message) {
  .write_log("INFO", message)
}

log_warning <- function(message) {
  .write_log("WARNING", message)
}

log_error <- function(message) {
  .write_log("ERROR", message)
}

log_debug <- function(message) {
  .write_log("DEBUG", message)
}