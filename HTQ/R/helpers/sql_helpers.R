#########################################
# define connection string
#########################################

get_sql_connection <- function() {

  log_info("Opening SQL connection (SQL Authentication)")

  con <- DBI::dbConnect(
    odbc::odbc(),
    Driver   = "ODBC Driver 17 for SQL Server",
    Server   = Sys.getenv("HTQSERVER"),
    Database = Sys.getenv("HTQDATABASE"),
    UID      = Sys.getenv("HTQUID"),
    PWD      = Sys.getenv("HTQPWD"),
    TrustServerCertificate = "yes"
  )

  con
}