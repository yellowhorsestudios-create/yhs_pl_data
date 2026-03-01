library(glue)

# ---------------------------------------------------------
# Initialize logger
# ---------------------------------------------------------

init_logger <- function(log_dir = "logs",
                        log_level = "INFO") {

  if (!dir.exists(log_dir)) {
    dir.create(log_dir, recursive = TRUE)
  }

  timestamp <- format(Sys.time(), "%Y%m%d%H%M%S")

  log_file <- file.path(
    log_dir,
    glue("transfermarket_ingestion_{timestamp}.log")
  )

  options(
    tm_log_file = log_file,
    tm_log_level = toupper(log_level)
  )

  cat(glue("Log started at {Sys.time()}\n"), file = log_file)

  invisible(log_file)
}

# ---------------------------------------------------------
# Log level management
# ---------------------------------------------------------

.log_levels <- c("DEBUG", "INFO", "WARN", "ERROR")

.should_log <- function(level) {
  current_level <- getOption("tm_log_level", "INFO")
  match(level, .log_levels) >= match(current_level, .log_levels)
}

# ---------------------------------------------------------
# Internal write helper
# ---------------------------------------------------------

.write_log <- function(level, msg, context = NULL) {

  if (!.should_log(level)) return(invisible())

  log_file <- getOption("tm_log_file")
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

  context_str <- ""
  if (!is.null(context)) {
    kv <- paste(names(context), context, sep = "=", collapse = " ")
    context_str <- paste0(kv, " | ")
  }

  log_line <- glue("[{level}] {timestamp} | {context_str}{msg}")

  message(log_line)

  if (!is.null(log_file)) {
    cat(log_line, "\n", file = log_file, append = TRUE)
  }
}

# ---------------------------------------------------------
# Public logging functions
# ---------------------------------------------------------

log_info <- function(msg, context = NULL) {
  .write_log("INFO", msg, context)
}

log_warning <- function(msg, context = NULL) {
  .write_log("WARN", msg, context)
}

log_error <- function(msg, context = NULL) {
  .write_log("ERROR", msg, context)
}

log_debug <- function(msg, context = NULL) {
  .write_log("DEBUG", msg, context)
}