empty_squad_schema <- function() {
  tibble(
    team_name = character(),
    team_slug = character(),
    team_id = character(),
    season = character(),
    squad_number = character(),
    player_id = character(),
    player_name = character(),
    profile_url = character(),
    age = character(),
    nationality = character(),
    in_squad = character(),
    appearances = character(),
    goals = character(),
    assists = character(),
    yellow_cards = character(),
    second_yellow = character(),
    red_cards = character(),
    sub_on = character(),
    sub_off = character(),
    points_per_game = character(),
    minutes = character(),
    source_url = character(),
    scraped_at = as.POSIXct(character())
  )
}
