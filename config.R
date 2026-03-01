# ---------------------------------------
# HTQ Configuration
# ---------------------------------------

Sys.setenv(
  HTQSERVER       = "PUBT-DB-09",  # <-- change to real value
  HTQDATABASE     = "HTQ2",
  HTQSCHEMA       = "BI_EDA",
  HTQUID          = "vida"  ,
  HTQPWD          = "qN4qQ2RbisU62K1LI",
  HTQ_BRONZE_PATH = "C:/R_Scripts/Scripts/HTQ/data/bronze",
  HTQ_SILVER_PATH = "C:/R_Scripts/Scripts/HTQ/data/silver",
  HTQ_GOLD_PATH   = "C:/R_Scripts/Scripts/HTQ/data/gold"
)