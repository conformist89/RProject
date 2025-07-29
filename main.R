# Haupt-Pipeline Skript für Lebensversicherungsdaten
# Datei: main.R
# 
# Dieses Skript führt die komplette ETL-Pipeline aus
# Alternativ können Sie pipeline.qmd verwenden für einen interaktiven Bericht

# Setup
library(here)
library(dplyr)

# Helper-Funktionen laden
source(here("R", "etl_functions.R"))
source(here("R", "data_quality.R"))

# Hauptfunktion
main <- function() {
  cat("=== LEBENSVERSICHERUNGS ETL-PIPELINE ===\n")
  cat("Gestartet:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")
  
  # Pipeline ausführen
  tryCatch({
    # Vollständige Pipeline
    result <- run_full_pipeline(
      data_dir = here("data", "raw"),
      output_dir = here("data", "processed")
    )
    
    cat("\n=== PIPELINE ERFOLGREICH ABGESCHLOSSEN ===\n")
    cat("Datensätze verarbeitet:", nrow(result), "\n")
    cat("Ausgabedateien in:", here("data", "processed"), "\n")
    
    return(result)
    
  }, error = function(e) {
    cat("\n❌ PIPELINE FEHLER:", e$message, "\n")
    stop(e)
  })
}

# Pipeline ausführen wenn Skript direkt aufgerufen wird
if (!interactive()) {
  main()
}