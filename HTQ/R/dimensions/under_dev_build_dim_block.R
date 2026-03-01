build_dim_block <- function() {

  log_info("Building DimBlock")

  bronze_journey <- read_bronze("journey")

  distinct_blocks <- bronze_journey %>%
    dplyr::select(BlockId) %>%
    dplyr::distinct() %>%
    dplyr::collect()

  if (nrow(distinct_blocks) == 0) {
    log_info("No Block records found.")
    return(invisible(NULL))
  }

  existing_dim <- read_silver("dim_block")

  updated_keys <- upsert_dimension(
    existing_dim,
    distinct_blocks,
    natural_key = "BlockId",
    surrogate_key = "BlockKey"
  )

  final_dim <- updated_keys %>%
    dplyr::mutate(CreatedAt = Sys.time()) %>%
    dplyr::select(
      BlockKey,
      BlockId,
      CreatedAt
    )

  write_silver_dimension(final_dim, "dim_block")

  log_info(glue::glue("DimBlock complete. Total rows: {nrow(final_dim)}"))
}
