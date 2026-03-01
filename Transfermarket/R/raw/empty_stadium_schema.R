empty_stadium_schema <- function() {
  tibble(
    team_name = character(),
    team_slug = character(),
    team_id = character(),
    season = character(),
    stadium_name = character(),
    total_capacity = character(),
    seats = character(),
    built = character(),
    construction_costs = character(),
    formerly = character(),
    undersoil_heating = character(),
    running_track = character(),
    surface = character(),
    pitch_size = character(),
    source_url = character(),
    scraped_at = as.POSIXct(character())
  )
}