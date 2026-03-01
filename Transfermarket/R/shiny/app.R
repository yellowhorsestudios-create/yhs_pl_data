library(shiny)
library(dplyr)
library(arrow)
library(ggplot2)
library(plotly)

# -----------------------------
# Load data
# -----------------------------
squad <- read_parquet("data/squad_stats.parquet")

# Ensure numeric fields
squad <- squad %>%
  mutate(
    age = as.numeric(player_age_in_season),
    minutes = readr::parse_number(total_minutes_played_in_season),
    goals = as.numeric(total_goals_in_season)
  )

# -----------------------------
# UI
# -----------------------------
ui <- fluidPage(

  titlePanel("⚽ Transfermarkt Analytics"),

  sidebarLayout(
    sidebarPanel(
      selectInput(
        "team",
        "Team",
        choices = sort(unique(squad$team))
      ),

      selectInput(
        "season",
        "Season",
        choices = sort(unique(squad$season)),
        selected = max(squad$season)
      )
    ),

    mainPanel(
      tabsetPanel(

        tabPanel("Squad Table",
                 tableOutput("squad_table")
        ),

        tabPanel("Age Profile",
                 plotlyOutput("age_plot")
        ),

        tabPanel("Performance",
                 plotlyOutput("performance_plot")
        )
      )
    )
  )
)

# -----------------------------
# SERVER
# -----------------------------
server <- function(input, output) {

  filtered_data <- reactive({
    squad %>%
      filter(
        team == input$team,
        season == input$season
      )
  })

  # ---- table ----
  output$squad_table <- renderTable({
    filtered_data() %>%
      select(
        player_name,
        position,
        age,
        total_appearances_in_season,
        total_goals_in_season,
        total_minutes_played_in_season
      )
  })

  # ---- age distribution ----
  output$age_plot <- renderPlotly({

    p <- ggplot(filtered_data(),
                aes(x = age)) +
      geom_histogram(bins = 10) +
      labs(title = "Age Distribution")

    ggplotly(p)
  })

  # ---- performance scatter ----
  output$performance_plot <- renderPlotly({

    p <- ggplot(
      filtered_data(),
      aes(
        x = minutes,
        y = goals,
        label = player_name
      )
    ) +
      geom_point(size = 3) +
      labs(
        title = "Minutes vs Goals"
      )

    ggplotly(p, tooltip = "label")
  })
}

# -----------------------------
shinyApp(ui, server)
