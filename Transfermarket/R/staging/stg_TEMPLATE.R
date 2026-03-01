stg_TEMPLATE <- function(raw_df) {

  log_info("Staging: TEMPLATE")

  # -------------------------------------------------
  # 1. RAW SCHEMA VALIDATION
  # -------------------------------------------------

  expected_cols <- c(
    "example_col1",
    "example_col2"
  )

  validate_schema(raw_df, expected_cols)


  # -------------------------------------------------
  # 2. STANDARDIZE COLUMN NAMES
  # -------------------------------------------------

  df <- raw_df %>%
    janitor::clean_names()


  # -------------------------------------------------
  # 3. TYPE CASTING
  # -------------------------------------------------

  df <- df %>%
    mutate(
      season = as.integer(season),
      team_id = as.integer(team_id),
      matches = readr::parse_number(matches)
    )


  # -------------------------------------------------
  # 4. VALUE NORMALIZATION
  # -------------------------------------------------

  df <- df %>%
    mutate(
      competition = toupper(competition),
      player_name = stringr::str_squish(player_name)
    )


  # -------------------------------------------------
  # 5. GRAIN ENFORCEMENT (CRITICAL)
  # -------------------------------------------------

  df <- df %>%
    distinct()

  assert_unique_key(
    df,
    c("player_id","team_id","season","competition")
  )


  # -------------------------------------------------
  # 6. KEY VALIDATION
  # -------------------------------------------------

  assert_fk(df, dim_team, "team_id")
  assert_fk(df, dim_season, "season")

  log_info(glue::glue("Rows staged: {nrow(df)}"))

  df
}
