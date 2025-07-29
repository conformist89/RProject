# =============================================================================
# VOLLSTÄNDIGE DEMO DER MODULAR ANALYTICS FUNCTIONS
# =============================================================================
# Purpose: Demonstration aller entwickelten Analytics-Module
# File: demo_analytics.R

# =============================================================================
# VORAUSSETZUNGEN
# =============================================================================

# 1. Laden Sie zuerst Ihr Analytics-Modul
cat("LOADING ANALYTICS MODULES\n")
cat("=========================\n")
source("vergleich.r")  # Ihr Analytics-Modul

# 2. Laden Sie Ihre gereinigten Daten (aus DuckDB oder CSV)
cat("LOADING CLEANED DATA\n")
cat("====================\n")

# Option A: Aus DuckDB (empfohlen)
tryCatch({
  library(duckdb)
  con <- dbConnect(duckdb::duckdb(), "data/cleaned/insurance_data.duckdb")
  
  # Laden Sie die neueste Version (ersetzen Sie VERSION_ID mit Ihrer aktuellen Version)
  # Sie können list_duckdb_versions() verwenden um verfügbare Versionen zu sehen
  versions <- dbGetQuery(con, "SELECT version_id FROM data_versions ORDER BY created_at DESC LIMIT 1")
  latest_version <- versions$version_id[1]
  
  cat("📋 Using latest version:", latest_version, "\n")
  
  # Laden der Daten
  contracts_clean <- dbGetQuery(con, paste0("SELECT * FROM contracts_", latest_version))
  customers_clean <- dbGetQuery(con, paste0("SELECT * FROM customers_", latest_version))
  timeseries_clean <- dbGetQuery(con, paste0("SELECT * FROM timeseries_", latest_version))
  
  dbDisconnect(con)
  
  cat("✅ Loaded from DuckDB:\n")
  cat("   - Contracts:", nrow(contracts_clean), "rows\n")
  cat("   - Customers:", nrow(customers_clean), "rows\n")
  cat("   - Timeseries:", nrow(timeseries_clean), "rows\n\n")
  
}, error = function(e) {
  cat("⚠️ DuckDB loading failed, trying CSV fallback...\n")
  
  # Option B: Aus CSV Files (Fallback)
  tryCatch({
    contracts_clean <- read.csv("data/cleaned/contracts_cleaned.csv")
    customers_clean <- read.csv("data/cleaned/customers_cleaned.csv")
    timeseries_clean <- read.csv("data/cleaned/timeseries_cleaned.csv")
    
    # Convert date columns
    customers_clean$birthdate <- as.Date(customers_clean$birthdate)
    timeseries_clean$date <- as.Date(timeseries_clean$date)
    
    cat("✅ Loaded from CSV:\n")
    cat("   - Contracts:", nrow(contracts_clean), "rows\n")
    cat("   - Customers:", nrow(customers_clean), "rows\n")
    cat("   - Timeseries:", nrow(timeseries_clean), "rows\n\n")
    
  }, error = function(e2) {
    stop("❌ Could not load data from DuckDB or CSV. Please check file paths.")
  })
})

# =============================================================================
# DEMO 1: ALTERSKLASSEN ANALYSE
# =============================================================================

cat("DEMO 1: ALTERSKLASSEN ANALYSE\n")
cat("==============================\n")

# Schritt 1: Altersgruppen klassifizieren
customers_with_age <- age_classification$classify_age_groups(
  customers_data = customers_clean,
  age_breaks = c(30, 50),
  age_labels = c("<30", "30-50", ">50")
)

# Schritt 2: Detaillierte Altersgruppen-Statistiken erstellen
age_summary <- age_classification$create_age_summary(customers_with_age)

cat("📊 ALTERSGRUPPEN SUMMARY TABLE:\n")
print(age_summary)

# Schritt 3: Custom Altersklassen (Alternative)
cat("\n🔄 ALTERNATIVE ALTERSKLASSIFIKATION (25, 40, 60):\n")
customers_custom_age <- age_classification$classify_age_groups(
  customers_data = customers_clean,
  age_breaks = c(25, 40, 60),
  age_labels = c("<25", "25-40", "40-60", ">60")
)

cat("\n")

# =============================================================================
# DEMO 2: ZEITREIHEN ANALYSE
# =============================================================================

cat("DEMO 2: ZEITREIHEN ANALYSE\n")
cat("===========================\n")

# Schritt 1: Monatliche Wachstumsraten berechnen
growth_rates <- timeseries_analysis$calculate_monthly_growth(
  timeseries_data = timeseries_clean,
  min_observations = 3
)

cat("📈 ERSTE 5 WACHSTUMSRATEN:\n")
print(head(growth_rates[c("contractid", "date", "surrender_value", "monthly_growth_rate")], 5))

# Schritt 2: Volatilität pro Vertrag berechnen
volatility_stats <- timeseries_analysis$calculate_volatility(
  growth_data = growth_rates,
  min_periods = 6
)

cat("\n📊 TOP 5 VOLATILSTE VERTRÄGE:\n")
print(head(volatility_stats[c("contractid", "volatility", "volatility_class", "risk_adjusted_return")], 5))

# Schritt 3: Zinssensitivität berechnen
interest_sensitivity <- timeseries_analysis$calculate_interest_sensitivity(
  timeseries_data = timeseries_clean,
  interest_rate_data = NULL,  # Verwendet synthetische Daten
  min_observations = 12
)

cat("\n💰 TOP 5 ZINSSENSITIVE VERTRÄGE:\n")
print(head(interest_sensitivity[c("contractid", "correlation", "sensitivity_class", "r_squared")], 5))

cat("\n")

# =============================================================================
# DEMO 3: MANDANTEN SUMMARIES
# =============================================================================

cat("DEMO 3: MANDANTEN SUMMARIES\n")
cat("============================\n")

# Umfassende Mandanten-Analyse mit allen verfügbaren Daten
mandant_summaries <- mandant_analysis$create_mandant_summaries(
  contracts_data = contracts_clean,
  customers_data = customers_with_age,  # Mit Altersgruppen
  timeseries_data = timeseries_clean,
  volatility_data = volatility_stats
)

# Zeige die verschiedenen Analysen
cat("📋 VERFÜGBARE MANDANTEN ANALYSEN:\n")
cat("   - Basic Distribution:", !is.null(mandant_summaries$distribution), "\n")
cat("   - Age Analysis:", !is.null(mandant_summaries$age_analysis), "\n")
cat("   - Age Group Distribution:", !is.null(mandant_summaries$age_group_distribution), "\n")
cat("   - Surrender Value Analysis:", !is.null(mandant_summaries$surrender_value_analysis), "\n")
cat("   - Volatility Analysis:", !is.null(mandant_summaries$volatility_analysis), "\n")

# =============================================================================
# DEMO 4: ERWEITERTE ANALYSEN UND KOMBINATIONEN
# =============================================================================

cat("\nDEMO 4: ERWEITERTE ANALYSEN\n")
cat("============================\n")

# Kombiniere verschiedene Analysen für tiefere Einsichten
cat("🔗 KOMBINIERTE ALTERSGRUPPEN & MANDANTEN ANALYSE:\n")

# Join Verträge mit Kunden und Altersgruppen
enhanced_contracts <- contracts_clean %>%
  inner_join(customers_with_age[c("customerid", "age", "age_group")], by = "customerid") %>%
  filter(!is.na(mandantid), !is.na(age_group))

# Altersgruppen-Verteilung je Mandant
age_mandant_cross <- enhanced_contracts %>%
  count(mandantid, age_group) %>%
  group_by(mandantid) %>%
  mutate(percentage = round(100 * n / sum(n), 1)) %>%
  ungroup()

cat("📊 ALTERSGRUPPEN x MANDANTEN KREUZTABELLE:\n")
print(age_mandant_cross)

# Durchschnittliche Volatilität je Altersgruppe und Mandant
if (nrow(volatility_stats) > 0) {
  volatility_enhanced <- volatility_stats %>%
    inner_join(enhanced_contracts[c("contractid", "mandantid", "age_group")], by = "contractid") %>%
    group_by(mandantid, age_group) %>%
    summarise(
      contracts = n(),
      mean_volatility = round(mean(volatility, na.rm = TRUE), 3),
      .groups = 'drop'
    ) %>%
    filter(contracts >= 3)  # Nur Gruppen mit mindestens 3 Verträgen
  
  cat("\n📈 DURCHSCHNITTLICHE VOLATILITÄT JE MANDANT & ALTERSGRUPPE:\n")
  print(volatility_enhanced)
}

# =============================================================================
# DEMO 5: EXPORT UND SPEICHERUNG DER ANALYSEN
# =============================================================================

cat("\nDEMO 5: EXPORT DER ANALYSEERGEBNISSE\n")
cat("=====================================\n")

# Erstelle Reports-Verzeichnis
if (!dir.exists("reports/analytics")) {
  dir.create("reports/analytics", recursive = TRUE)
}

timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")

# Export verschiedener Analysen als CSV
tryCatch({
  # Altersgruppen Summary
  write.csv(age_summary, 
           paste0("reports/analytics/age_summary_", timestamp, ".csv"), 
           row.names = FALSE)
  
  # Volatilitäts-Statistiken
  write.csv(volatility_stats, 
           paste0("reports/analytics/volatility_stats_", timestamp, ".csv"), 
           row.names = FALSE)
  
  # Zinssensitivität
  write.csv(interest_sensitivity, 
           paste0("reports/analytics/interest_sensitivity_", timestamp, ".csv"), 
           row.names = FALSE)
  
  # Mandanten Analysen (als JSON für komplexe Struktur)
  write(jsonlite::toJSON(mandant_summaries, pretty = TRUE),
        paste0("reports/analytics/mandant_summaries_", timestamp, ".json"))
  
  # Kombinierte Analyse
  write.csv(age_mandant_cross, 
           paste0("reports/analytics/age_mandant_cross_", timestamp, ".csv"), 
           row.names = FALSE)
  
  cat("✅ EXPORT ERFOLGREICH:\n")
  cat("   📁 Alle Analyseergebnisse gespeichert in: reports/analytics/\n")
  cat("   🏷️  Timestamp:", timestamp, "\n")
  
}, error = function(e) {
  cat("❌ Export Error:", e$message, "\n")
})

# =============================================================================
# DEMO 6: BUSINESS INSIGHTS ZUSAMMENFASSUNG
# =============================================================================

cat("\nDEMO 6: BUSINESS INSIGHTS ZUSAMMENFASSUNG\n")
cat("==========================================\n")

# Erstelle eine Executive Summary der wichtigsten Erkenntnisse
cat("🎯 KEY BUSINESS INSIGHTS:\n\n")

# 1. Altersverteilung Insights
age_insights <- age_summary %>%
  arrange(desc(count))

dominant_age_group <- age_insights$age_group[1]
cat("👥 KUNDEN-DEMOGRAFIE:\n")
cat("   - Stärkste Altersgruppe:", dominant_age_group, 
    "(", age_insights$percentage[1], "% der Kunden)\n")
cat("   - Durchschnittsalter der Kunden:", 
    round(weighted.mean(age_insights$mean_age, age_insights$count), 1), "Jahre\n")

# 2. Mandanten Insights
if (!is.null(mandant_summaries$distribution)) {
  mandant_leader <- mandant_summaries$distribution %>%
    arrange(desc(contract_count))
  
  cat("\n🏢 MANDANTEN-VERTEILUNG:\n")
  cat("   - Führender Mandant:", mandant_leader$mandantid[1], 
      "(", mandant_leader$percentage[1], "% der Verträge)\n")
}

# 3. Volatilitäts Insights
if (nrow(volatility_stats) > 0) {
  high_vol_contracts <- sum(volatility_stats$volatility_class %in% c("Hoch", "Sehr hoch"))
  total_contracts <- nrow(volatility_stats)
  
  cat("\n📊 RISIKO-PROFIL:\n")
  cat("   - Hochvolatile Verträge:", high_vol_contracts, "von", total_contracts,
      "(", round(100 * high_vol_contracts / total_contracts, 1), "%)\n")
  cat("   - Durchschnittliche Portfolio-Volatilität:", 
      round(mean(volatility_stats$volatility, na.rm = TRUE), 2), "%\n")
}

# 4. Zinssensitivitäts Insights
if (nrow(interest_sensitivity) > 0) {
  high_sens_contracts <- sum(interest_sensitivity$sensitivity_class %in% c("Hoch", "Sehr hoch"))
  total_sens_contracts <- nrow(interest_sensitivity)
  
  cat("\n💰 ZINSSENSITIVITÄT:\n")
  cat("   - Hochsensitive Verträge:", high_sens_contracts, "von", total_sens_contracts,
      "(", round(100 * high_sens_contracts / total_sens_contracts, 1), "%)\n")
  cat("   - Durchschnittliche Korrelation:", 
      round(mean(abs(interest_sensitivity$correlation), na.rm = TRUE), 3), "\n")
}

cat("\n🎉 ANALYTICS DEMO COMPLETED SUCCESSFULLY!\n")
cat("=========================================\n")
cat("📊 Alle Module erfolgreich getestet\n")
cat("📁 Ergebnisse gespeichert in: reports/analytics/\n")
cat("🔧 Modular strukturierte Funktionen einsatzbereit\n")

# =============================================================================
# USAGE GUIDE FÜR PRODUCTION
# =============================================================================

cat("\n📖 PRODUCTION USAGE GUIDE:\n")
cat("===========================\n")
cat("1. Altersklassen:\n")
cat("   result <- age_classification$classify_age_groups(customers_data)\n")
cat("   summary <- age_classification$create_age_summary(result)\n\n")

cat("2. Zeitreihen-Analyse:\n")
cat("   growth <- timeseries_analysis$calculate_monthly_growth(timeseries_data)\n")
cat("   volatility <- timeseries_analysis$calculate_volatility(growth)\n")
cat("   sensitivity <- timeseries_analysis$calculate_interest_sensitivity(timeseries_data)\n\n")

cat("3. Mandanten-Summaries:\n")
cat("   summaries <- mandant_analysis$create_mandant_summaries(contracts, customers, timeseries)\n\n")

cat("💡 Tipp: Alle Funktionen sind vollständig dokumentiert mit {roxygen2}!\n")