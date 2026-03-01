#################################################################
# retry call
#################################################################

make_job_key <- function(args) {
  paste(unlist(args), collapse = "_")
}
resolve_order()

#################################################################
# resolve order
#################################################################

resolve_order <- function(pipeline_tbl) {
  
  resolved <- character()
  remaining <- pipeline_tbl
  
  while (nrow(remaining) > 0) {
    
    ready <- remaining %>%
      dplyr::rowwise() %>%
      dplyr::filter(
        is.null(depends_on) ||
        length(depends_on) == 0 ||
        all(depends_on %in% resolved)
      ) %>%
      dplyr::ungroup()
    
    if (nrow(ready) == 0) {
      stop("Circular or unresolved dependency detected")
    }
    
    resolved <- c(resolved, ready$stage)
    
    remaining <- remaining %>%
      dplyr::filter(!stage %in% ready$stage)
  }
  
  resolved
}