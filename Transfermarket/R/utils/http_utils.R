#################################################################
# throttle
#################################################################

throttle <- function(min = 1.5, max = 3) {
  Sys.sleep(runif(1, min, max))
}

#################################################################
# retry call
#################################################################

retry_call <- function(fn, args, retries = 3, wait = 3) {
  
  for (i in seq_len(retries)) {
    try_result <- try(do.call(fn, args), silent = TRUE)
    
    if (!inherits(try_result, "try-error")) {
      return(try_result)
    }
    
    Sys.sleep(wait * i)
  }
  
  stop("Max retries exceeded")
}

#################################################################
# safe_read_html
#################################################################


safe_read_html <- function(
  url,
  min_delay = 1,
  max_delay = 2,
  retries = 3
) {

  log_debug(glue("Requesting URL: {url}"))

  use_cache <- getOption("tm_use_cache", TRUE)
  cache_dir <- getOption("tm_cache_dir", "data/cache")

  if (!dir.exists(cache_dir)) {
    dir.create(cache_dir, recursive = TRUE)
  }

  cache_file <- file.path(
    cache_dir,
    paste0(digest::digest(url), ".html")
  )

  # ---- Cache hit ----
  if (use_cache && file.exists(cache_file)) {
    log_debug(glue("Cache hit: {url}"))
    return(read_html(cache_file))
  }

  attempt <- 1

  while (attempt <= retries) {

    Sys.sleep(runif(1, min_delay, max_delay))

    response <- tryCatch(
      GET(
        url,
        timeout(20),
        add_headers(
          "User-Agent" =
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        )
      ),
      error = function(e) {
        log_warning(glue("Request error on attempt {attempt}: {url}"))
        return(NULL)
      }
    )

    if (is.null(response)) {
      attempt <- attempt + 1
      next
    }

    status <- status_code(response)

    # ---- Handle blocking ----
    if (status %in% c(403, 429)) {
      log_warning(glue("Rate limited ({status}) - backing off"))
      Sys.sleep(30)
      attempt <- attempt + 1
      next
    }

    if (status == 200) {

      html_content <- content(response, "text", encoding = "UTF-8")

      if (grepl("Access denied|captcha|blocked", html_content, ignore.case = TRUE)) {
        log_warning("Blocked page detected")
        attempt <- attempt + 1
        next
      }

      if (use_cache) {
        writeLines(html_content, cache_file)
      }

      return(read_html(html_content))
    }

    log_warning(glue("Attempt {attempt} failed (status {status})"))
    Sys.sleep(2 ^ attempt)
    attempt <- attempt + 1
  }

  log_error(glue("Failed after {retries} attempts: {url}"))
  return(NULL)
}

#################################################################
# retry call
#################################################################

retry_call <- function(fn, args, retries = 3, wait = 3) {
  
  for (i in seq_len(retries)) {
    try_result <- try(do.call(fn, args), silent = TRUE)
    
    if (!inherits(try_result, "try-error")) {
      return(try_result)
    }
    
    Sys.sleep(wait * i)
  }
  
  stop("Max retries exceeded")
}