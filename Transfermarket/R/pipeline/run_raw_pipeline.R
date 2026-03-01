run_raw_pipeline <- function(pipeline_tbl,
                             job_grid,
                             force = FALSE) {
  
  overall_start <- Sys.time()
  
  order <- resolve_order(pipeline_tbl)
  
  log_info("RAW pipeline started",
           context = list(stages = length(order)))
  
  for (stage_name in order) {
    
    stage_row <- pipeline_tbl %>%
      dplyr::filter(stage == stage_name)
    
    fn <- stage_row$fn[[1]]
    output_table <- stage_row$output_table
    partition_cols <- stage_row$partition_cols[[1]]
    
    stage_start <- Sys.time()
    
    log_info("Stage started",
             context = list(stage = stage_name))
    
    success_count <- 0
    fail_count <- 0
    
    for (i in seq_len(nrow(job_grid))) {
      
      args <- as.list(job_grid[i, ])
      job_key <- make_job_key(args)
      
      job_start <- Sys.time()
      
      log_debug("Job started",
                context = list(stage = stage_name,
                               job = job_key))
      
      result <- try(
        retry_call(fn, args),
        silent = TRUE
      )
      
      if (inherits(result, "try-error") || is.null(result)) {
        fail_count <- fail_count + 1
        
        log_error("Job failed",
                  context = list(stage = stage_name,
                                 job = job_key))
        next
      }
      
      if (nrow(result) > 0) {
        write_raw_partitioned(
          result,
          output_table,
          partition_cols
        )
      }
      
      job_duration <- round(
        as.numeric(difftime(Sys.time(), job_start, units = "secs")),
        2
      )
      
      success_count <- success_count + 1
      
      log_debug("Job finished",
                context = list(stage = stage_name,
                               job = job_key,
                               duration_sec = job_duration))
    }
    
    stage_duration <- round(
      as.numeric(difftime(Sys.time(), stage_start, units = "secs")),
      2
    )
    
    log_info("Stage completed",
             context = list(
               stage = stage_name,
               duration_sec = stage_duration,
               success = success_count,
               failed = fail_count
             ))
  }
  
  total_duration <- round(
    as.numeric(difftime(Sys.time(), overall_start, units = "secs")),
    2
  )
  
  log_info("RAW pipeline finished",
           context = list(
             total_duration_sec = total_duration
           ))
}