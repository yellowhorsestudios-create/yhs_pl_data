library(tibble)

RAW_PIPELINE <- tribble(
  ~stage,                 ~fn,                               ~output_table,                    ~partition_cols,               ~depends_on,
  
  "team_achievements",    scrape_team_achievements_raw,      "raw_team_achievements",          c("team_slug"),                NULL,
  
  "squad_stats",          scrape_squad_stats_raw,            "raw_squad_stats",                c("team_slug","season"),       NULL,
  
  "player_profiles",      scrape_player_profiles_raw,        "raw_player_profiles",            c("player_id"),                "squad_stats",
  
  "fixtures",             scrape_fixtures_raw,               "raw_fixtures",                   c("team_slug","season_id"),    NULL,
  
  "attendances",          scrape_attendances_raw,            "raw_attendances",                c("team_slug"),                NULL,
  
  "stadium",              scrape_stadium_raw,                "raw_stadium",                    c("team_slug","season"),       NULL,
  
  "managers",             scrape_managers_raw,               "raw_managers",                   c("team_slug"),                NULL
)