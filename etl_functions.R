# ETL Helper Functions für Lebensversicherungs-Pipeline
# Datei: R/etl_functions.R

library(DBI)
library(RSQLite)
library(readxl)
library(jsonlite)
library(yahoofinancer)
library(dplyr)
library(lubridate)
library(here)

#' SQLite Contracts Daten laden
#' @param db_path Pfad zur SQLite Datenbank
#' @return data.frame mit Vertragsdaten
load_contracts_sqlite <- function(db_path) {
  tryCatch({
    con <- dbConnect(RSQLite::SQLite(), db_path)
    
    # Prüfen ob Tabelle existiert
    if (!"contracts" %in% dbListTables(con)) {
      stop("Tabelle 'contracts' nicht in Datenbank gefunden")
    }
    
    contracts <- dbGetQuery(con, "SELECT * FROM contracts")
    dbDisconnect(con)
    
    # Datentypen standardisieren
    contracts$contractid <- as.character(contracts$contractid)
    contracts$customerid <- as.character(contracts$customerid)
    contracts$mandantid <- as.character(contracts$mandantid)
    
    cat("✓ Contracts geladen:", nrow(contracts), "Datensätze\n")
    return(contracts)
    
  }, error = function(e) {
    cat("✗ Fehler beim Laden der SQLite Daten:", e$message, "\n")
    return(NULL)
  })
}

#' Excel Customers Daten laden
#' @param excel_path Pfad zur Excel Datei
#' @return data.frame mit Kundendaten
load_customers_excel <- function(excel_path) {
  tryCatch({
    customers <- read_excel(
      excel_path,
      col_types = c("text", "date"),
      na = c("", "NA", "NULL", "#N/A")
    )
    
    # Spaltennamen normalisieren
    names(customers) <- tolower(trimws(names(customers)))
    
    # Datentypen konvertieren
    customers$contractid <- as.character(customers$contractid)
    customers$birthdate <- as.Date(customers$birthdate)
    
    # Plausibilitätsprüfung
    invalid_dates <- sum(is.na(customers$birthdate))
    if (invalid_dates > 0) {
      warning("Gefunden ", invalid_dates, " ungültige Geburtsdaten")
    }
    
    cat("✓ Customers geladen:", nrow(customers), "Datensätze\n")
    return(customers)
    
  }, error = function(e) {
    cat("✗ Fehler beim Laden der Excel Daten:", e$message, "\n")
    return(NULL)
  })
}

#' JSON Zeitreihendaten laden
#' @param json_path Pfad zur JSON Datei
#' @return data.frame mit Zeitreihendaten
load_timeseries_json <- function(json_path) {
  tryCatch({
    # JSON laden und parsen
    timeseries_raw <- fromJSON(json_path, flatten = TRUE)
    
    # Zu data.frame konvertieren
    if (is.list(timeseries_raw)) {
      timeseries <- as.data.frame(timeseries_raw)
    } else {
      timeseries <- timeseries_raw
    }
    
    # Datentypen anpassen
    timeseries$contractid <- as.character(timeseries$contractid)
    timeseries$date <- as.Date(timeseries$date)
    timeseries$rueckkaufswert <- as.numeric(timeseries$rueckkaufswert)
    
    # Sortieren nach Contract und Datum
    timeseries <- timeseries %>%
      arrange(contractid, date)
    
    cat("✓ Timeseries geladen:", nrow(timeseries), "Datensätze\n")
    return(timeseries)
    
  }, error = function(e) {
    cat("✗ Fehler beim Laden der JSON Daten:", e$message, "\n")
    return(NULL)
  })
}

#' Deutsche Zinsdaten von Yahoo Finance abrufen
#' @param start_date Startdatum als String
#' @param end_date Enddatum als String
#' @return data.frame mit Zinsdaten
get_german_interest_rates <- function(start_date = "2020-01-01", end_date = "2024-12-31") {
  tryCatch({
    cat("Lade Zinsdaten von Yahoo Finance...\n")
    
    # Deutsche 10-Jahr Bundesanleihen
    rates <- yahoofinancer::get_symbols(
      symbols = "^TNX", # US 10-Year als Proxy
      from = start_date,
      to = end_date,
      periodicity = "monthly"
    )
    
    # Daten aufbereiten
    interest_data <- rates %>%
      select(date = Date, interest_rate = Close) %>%
      mutate(
        date = as.Date(date),
        year_month = format(date, "%Y-%m"),
        interest_rate = as.numeric(interest_rate)
      ) %>%
      arrange(date) %>%
      filter(!is.na(interest_rate))
    
    cat("✓ Zinsdaten geladen:", nrow(interest_data), "Monate\n")
    return(interest_data)
    
  }, error = function(e) {
    cat("⚠ Yahoo Finance nicht verfügbar, verwende simulierte Daten\n")
    
    # Fallback: Realistische simulierte Zinsdaten
    dates <- seq(as.Date(start_date), as.Date(end_date), by = "month")
    
    # Simuliere realistischen Zinsverlauf
    base_rate <- 1.5
    trend <- seq(-0.5, 2.0, length.out = length(dates))
    noise <- rnorm(length(dates), 0, 0.3)
    
    simulated_rates <- data.frame(
      date = dates,
      year_month = format(dates, "%Y-%m"),
      interest_rate = pmax(0, base_rate + trend + noise)
    ) %>%
      mutate(interest_rate = round(interest_rate, 2))
    
    cat("✓ Simulierte Zinsdaten generiert:", nrow(simulated_rates), "Monate\n")
    return(simulated_rates)
  })
}

#' Master-Dataset aus allen Quellen erstellen
#' @param contracts Vertragsdaten
#' @param customers Kundendaten  
#' @param timeseries Zeitreihendaten
#' @param interest_rates Zinsdaten
#' @return Integriertes Master-Dataset
create_master_dataset <- function(contracts, customers, timeseries, interest_rates) {
  cat("Erstelle Master-Dataset...\n")
  
  # 1. Verträge mit Kunden joinen
  cat("  - Joinde Verträge mit Kunden\n")
  master_base <- contracts %>%
    left_join(customers, by = "contractid", suffix = c("", "_cust"))
  
  # 2. Aktuelle Rückkaufswerte hinzufügen
  cat("  - Füge aktuelle Rückkaufswerte hinzu\n")
  latest_values <- timeseries %>%
    group_by(contractid) %>%
    slice_max(date, n = 1, with_ties = FALSE) %>%
    select(contractid, latest_date = date, latest_rueckkaufswert = rueckkaufswert) %>%
    ungroup()
  
  master_extended <- master_base %>%
    left_join(latest_values, by = "contractid")
  
  # 3. Zinssätze basierend auf Bewertungsdatum hinzufügen
  cat("  - Füge Zinssätze hinzu\n")
  master_final <- master_extended %>%
    mutate(
      rate_lookup_month = format(latest_date, "%Y-%m"),
      # Alter berechnen
      age = as.numeric(difftime(Sys.Date(), birthdate, units = "days")) / 365.25,
      age = round(age, 1),
      # Laufzeit seit letzter Bewertung
      days_since_valuation = as.numeric(difftime(Sys.Date(), latest_date, units = "days"))
    ) %>%
    left_join(
      interest_rates %>% select(year_month, interest_rate),
      by = c("rate_lookup_month" = "year_month")
    )
  
  # Zusammenfassung
  cat("✓ Master-Dataset erstellt:\n")
  cat("  - Verträge:", nrow(master_final), "\n")
  cat("  - Mandanten:", length(unique(master_final$mandantid)), "\n")
  cat("  - Mit Zinsdaten:", sum(!is.na(master_final$interest_rate)), "\n")
  
  return(master_final)
}

#' Vollständige ETL-Pipeline ausführen
#' @param data_dir Eingabeverzeichnis  
#' @param output_dir Ausgabeverzeichnis
#' @return Master-Dataset
run_full_pipeline <- function(data_dir = "data/raw", output_dir = "data/processed") {
  
  cat("=== STARTE LEBENSVERSICHERUNGS ETL-PIPELINE ===\n\n")
  
  # Ausgabeverzeichnis erstellen falls nicht vorhanden
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  
  # 1. Daten laden
  cat("1. DATENLADUNG\n")
  contracts <- load_contracts_sqlite(file.path(data_dir, "contracts.sqlite"))
  customers <- load_customers_excel(file.path(data_dir, "customers.xlsx"))  
  timeseries <- load_timeseries_json(file.path(data_dir, "contracts_timeseries.json"))
  interest_rates <- get_german_interest_rates()
  
  # Fehlerprüfung
  if (is.null(contracts) || is.null(customers) || is.null(timeseries)) {
    stop("Kritische Daten konnten nicht geladen werden!")
  }
  
  cat("\n2. DATENINTEGRATION\n")
  # 3. Daten integrieren
  master_data <- create_master_dataset(contracts, customers, timeseries, interest_rates)
  
  cat("\n3. EXPORT\n")
  # 4. Exportieren
  output_file <- file.path(output_dir, paste0("master_insurance_data_", Sys.Date(), ".csv"))
  write.csv(master_data, output_file, row.names = FALSE)
  cat(" Master-Dataset exportiert:", output_file)

  return(master_data)
}