# =============================================================================
# KORRIGIERTE DEMO DER MODULAR ANALYTICS FUNCTIONS - DATE COLUMN FIX
# =============================================================================
# Purpose: Demonstration aller entwickelten Analytics-Module (mit Date-Fix)
# File: fixed_demo_analytics.R

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
  
  # Laden Sie die neueste Version
  versions <- dbGetQuery(con, "SELECT version_id FROM data_versions ORDER BY created_at DESC LIMIT 1")
  latest_version <- versions$version_id[1]
  
  cat("📋 Using latest version:", latest_version, "\n")
  
  # Laden der Daten
  contracts_clean <- dbGetQuery(con, paste0("SELECT * FROM contracts_", latest_version))
  customers_clean <- dbGetQuery(con, paste0("SELECT * FROM customers_", latest_version))
  timeseries_clean <- dbGetQuery(con, paste0("SELECT * FROM timeseries_", latest_version))
  
  dbDisconnect(con)
  
  # WICHTIGER FIX: Konvertiere Date-Spalten explizit
  customers_clean$birthdate <- as.Date(customers_clean$birthdate)
  timeseries_clean$date <- as.Date(timeseries_clean$date)
  
  cat("✅ Loaded from DuckDB:\n")
  cat("   - Contracts:", nrow(contracts_clean), "rows\n")
  cat("   - Customers:", nrow(customers_clean), "rows\n")
  cat("   - Timeseries:", nrow(timeseries_clean), "rows\n")
  
  # DEBUG: Check data types
  cat("📋 DATA TYPES CHECK:\n")
  cat("   - Customers birthdate:", class(customers_clean$birthdate), "\n")
  cat("   - Timeseries date:", class(timeseries_clean$date), "\n")
  cat("   - Timeseries date sample:", head(timeseries_clean$date, 3), "\n\n")
  
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

# # =============================================================================
# # DEMO 2: ZEITREIHEN ANALYSE - FIXED VERSION
# # =============================================================================

# cat("DEMO 2: ZEITREIHEN ANALYSE\n")
# cat("===========================\n")

# # DEBUG: Prüfe Timeseries-Daten vor der Analyse
# cat("🔍 DEBUGGING TIMESERIES DATA:\n")
# cat("   Columns:", paste(names(timeseries_clean), collapse = ", "), "\n")
# cat("   Date column class:", class(timeseries_clean$date), "\n")
# cat("   Date sample:", paste(head(timeseries_clean$date, 3), collapse = ", "), "\n")
# cat("   Surrender value sample:", paste(head(timeseries_clean$surrender_value, 3), collapse = ", "), "\n")

# # Zusätzliche Datenbereinigung für die Zeitreihen-Analyse
# timeseries_for_analysis <- timeseries_clean %>%
#   filter(
#     !is.na(contractid), 
#     !is.na(date), 
#     !is.na(surrender_value),
#     contractid != "",
#     surrender_value > 0  # Nur positive Werte
#   ) %>%
#   arrange(contractid, date)

# cat("   Cleaned timeseries rows:", nrow(timeseries_for_analysis), "\n")
# cat("   Unique contracts:", length(unique(timeseries_for_analysis$contractid)), "\n\n")

# # FIXED: Schritt 1 - Manuelle Wachstumsraten-Berechnung (Umgehung des Fehlers)
# tryCatch({
  
#   cat("📈 BERECHNE MONATLICHE WACHSTUMSRATEN...\n")
  
#   # Manuelle Berechnung statt der fehlerhaften Funktion
#   growth_rates <- timeseries_for_analysis %>%
#     group_by(contractid) %>%
#     arrange(contractid, date) %>%
#     mutate(
#       # Vorheriger Wert für Wachstumsberechnung
#       prev_value = lag(surrender_value),
#       prev_date = lag(date),
      
#       # Zeitdifferenz in Tagen
#       days_diff = as.numeric(difftime(date, prev_date, units = "days")),
      
#       # Monatliche Wachstumsrate (annualisiert auf Monatsbasis)
#       monthly_growth_rate = ifelse(
#         !is.na(prev_value) & prev_value > 0 & days_diff > 0,
#         # Formel: ((Neuer_Wert / Alter_Wert)^(30.44/Tage) - 1) * 100
#         ((surrender_value / prev_value)^(30.44/days_diff) - 1) * 100,
#         NA
#       )
#     ) %>%
#     filter(!is.na(monthly_growth_rate)) %>%
#     ungroup()
  
#   # Filtere Verträge mit Mindestanzahl Beobachtungen
#   contract_obs_count <- growth_rates %>%
#     count(contractid, name = "observations")
  
#   valid_contracts <- contract_obs_count %>%
#     filter(observations >= 3) %>%
#     pull(contractid)
  
#   growth_rates <- growth_rates %>%
#     filter(contractid %in% valid_contracts)
  
#   if (nrow(growth_rates) > 0) {
#     cat("✅ Monatliche Wachstumsraten erfolgreich berechnet\n")
#     cat("📊 ERSTE 5 WACHSTUMSRATEN:\n")
#     growth_sample <- growth_rates %>%
#       select(contractid, date, surrender_value, monthly_growth_rate) %>%
#       head(5)
#     print(growth_sample)
    
#     # FIXED: Schritt 2 - Manuelle Volatilitäts-Berechnung
#     cat("\n📊 BERECHNE VOLATILITÄT...\n")
    
#     volatility_stats <- growth_rates %>%
#       group_by(contractid) %>%
#       summarise(
#         observations = n(),
#         mean_growth = mean(monthly_growth_rate, na.rm = TRUE),
#         volatility = sd(monthly_growth_rate, na.rm = TRUE),
#         min_growth = min(monthly_growth_rate, na.rm = TRUE),
#         max_growth = max(monthly_growth_rate, na.rm = TRUE),
#         .groups = 'drop'
#       ) %>%
#       filter(observations >= 6, !is.na(volatility)) %>%
#       mutate(
#         # Volatilitätsklassen definieren
#         volatility_class = case_when(
#           volatility <= 2 ~ "Niedrig",
#           volatility <= 5 ~ "Mittel", 
#           volatility <= 10 ~ "Hoch",
#           TRUE ~ "Sehr hoch"
#         ),
#         # Risk-adjusted return (Sharpe-Ratio Konzept)
#         risk_adjusted_return = ifelse(volatility > 0, mean_growth / volatility, 0)
#       )
    
#     if (nrow(volatility_stats) > 0) {
#       cat("✅ Volatilitäts-Statistiken erfolgreich berechnet\n")
#       cat("📊 TOP 5 VOLATILSTE VERTRÄGE:\n")
#       volatility_sample <- volatility_stats %>%
#         arrange(desc(volatility)) %>%
#         select(contractid, volatility, volatility_class, risk_adjusted_return) %>%
#         head(5)
#       print(volatility_sample)
#     } else {
#       cat("⚠️ Keine Volatilitätsdaten verfügbar (nicht genügend Perioden)\n")
#       volatility_stats <- data.frame()
#     }
    
#     # FIXED: Schritt 3 - Vereinfachte Zinssensitivitäts-Analyse
#     cat("\n💰 BERECHNE ZINSSENSITIVITÄT...\n")
    
#     # Vereinfachte Zinssensitivität basierend auf zeitlichen Trends
#     interest_sensitivity <- growth_rates %>%
#       group_by(contractid) %>%
#       filter(n() >= 12) %>%  # Mindestens 12 Beobachtungen
#       summarise(
#         observations = n(),
#         # Korrelation zwischen Zeit und Wachstumsrate als Proxy für Sensitivität
#         time_correlation = cor(as.numeric(date), monthly_growth_rate, use = "complete.obs"),
#         mean_growth = mean(monthly_growth_rate, na.rm = TRUE),
#         .groups = 'drop'
#       ) %>%
#       filter(!is.na(time_correlation)) %>%
#       mutate(
#         # Absolute Korrelation für Sensitivitätsklassifikation
#         correlation = abs(time_correlation),
#         sensitivity_class = case_when(
#           correlation <= 0.2 ~ "Niedrig",
#           correlation <= 0.4 ~ "Mittel",
#           correlation <= 0.7 ~ "Hoch",
#           TRUE ~ "Sehr hoch"
#         ),
#         r_squared = correlation^2
#       ) %>%
#       select(contractid, correlation, sensitivity_class, r_squared, time_correlation)
    
#     if (nrow(interest_sensitivity) > 0) {
#       cat("✅ Zinssensitivitäts-Analyse erfolgreich berechnet\n")
#       cat("💰 TOP 5 ZINSSENSITIVE VERTRÄGE:\n")
#       sensitivity_sample <- interest_sensitivity %>%
#         arrange(desc(correlation)) %>%
#         select(contractid, correlation, sensitivity_class, r_squared) %>%
#         head(5)
#       print(sensitivity_sample)
#     } else {
#       cat("⚠️ Keine Zinssensitivitätsdaten verfügbar (nicht genügend Beobachtungen)\n")
#       interest_sensitivity <- data.frame()
#     }
    
#   } else {
#     cat("❌ Keine gültigen Wachstumsraten berechnet - prüfen Sie die Datenqualität\n")
#     growth_rates <- data.frame()
#     volatility_stats <- data.frame()
#     interest_sensitivity <- data.frame()
#   }
  
# }, error = function(e) {
#   cat("❌ Zeitreihen-Analyse Fehler:", e$message, "\n")
#   cat("🔍 Debug Info:\n")
#   cat("   - Timeseries rows:", nrow(timeseries_for_analysis), "\n")
#   if(nrow(timeseries_for_analysis) > 0) {
#     cat("   - Date range:", min(timeseries_for_analysis$date), "to", max(timeseries_for_analysis$date), "\n")
#   }
  
#   # Erstelle leere DataFrames als Fallback
#   growth_rates <<- data.frame()
#   volatility_stats <<- data.frame()
#   interest_sensitivity <<- data.frame()
# })

# cat("\n✅ DEMO 2: ZEITREIHEN ANALYSE ABGESCHLOSSEN\n")
# cat("============================================\n")

# # =============================================================================
# # DEMO 3: MANDANTEN SUMMARIES
# # =============================================================================

# cat("DEMO 3: MANDANTEN SUMMARIES\n")
# cat("============================\n")

# # Umfassende Mandanten-Analyse mit allen verfügbaren Daten
# mandant_summaries <- mandant_analysis$create_mandant_summaries(
#   contracts_data = contracts_clean,
#   customers_data = customers_with_age,  # Mit Altersgruppen
#   timeseries_data = timeseries_for_analysis,  # Verwende gereinigte Daten
#   volatility_data = if(nrow(volatility_stats) > 0) volatility_stats else NULL
# )

# # Zeige die verschiedenen Analysen
# cat("📋 VERFÜGBARE MANDANTEN ANALYSEN:\n")
# cat("   - Basic Distribution:", !is.null(mandant_summaries$distribution), "\n")
# cat("   - Age Analysis:", !is.null(mandant_summaries$age_analysis), "\n")
# cat("   - Age Group Distribution:", !is.null(mandant_summaries$age_group_distribution), "\n")
# cat("   - Surrender Value Analysis:", !is.null(mandant_summaries$surrender_value_analysis), "\n")
# cat("   - Volatility Analysis:", !is.null(mandant_summaries$volatility_analysis), "\n")

# # =============================================================================
# # DEMO 4: ERWEITERTE ANALYSEN UND KOMBINATIONEN
# # =============================================================================

# cat("\nDEMO 4: ERWEITERTE ANALYSEN\n")
# cat("============================\n")

# # Kombiniere verschiedene Analysen für tiefere Einsichten
# cat("🔗 KOMBINIERTE ALTERSGRUPPEN & MANDANTEN ANALYSE:\n")

# # Join Verträge mit Kunden und Altersgruppen
# enhanced_contracts <- contracts_clean %>%
#   inner_join(customers_with_age[c("customerid", "age", "age_group")], by = "customerid") %>%
#   filter(!is.na(mandantid), !is.na(age_group))

# # Altersgruppen-Verteilung je Mandant
# age_mandant_cross <- enhanced_contracts %>%
#   count(mandantid, age_group) %>%
#   group_by(mandantid) %>%
#   mutate(percentage = round(100 * n / sum(n), 1)) %>%
#   ungroup()

# cat("📊 ALTERSGRUPPEN x MANDANTEN KREUZTABELLE:\n")
# print(age_mandant_cross)

# # Durchschnittliche Volatilität je Altersgruppe und Mandant (nur wenn Daten verfügbar)
# if (nrow(volatility_stats) > 0) {
#   tryCatch({
#     volatility_enhanced <- volatility_stats %>%
#       inner_join(enhanced_contracts[c("contractid", "mandantid", "age_group")], by = "contractid") %>%
#       group_by(mandantid, age_group) %>%
#       summarise(
#         contracts = n(),
#         mean_volatility = round(mean(volatility, na.rm = TRUE), 3),
#         .groups = 'drop'
#       ) %>%
#       filter(contracts >= 2)  # Reduziert auf mindestens 2 Verträge
    
#     if (nrow(volatility_enhanced) > 0) {
#       cat("\n📈 DURCHSCHNITTLICHE VOLATILITÄT JE MANDANT & ALTERSGRUPPE:\n")
#       print(volatility_enhanced)
#     } else {
#       cat("\n⚠️ Nicht genügend Volatilitätsdaten für Altersgruppen-Analyse\n")
#     }
#   }, error = function(e) {
#     cat("\n⚠️ Volatilitäts-Altersgruppen-Analyse nicht möglich:", e$message, "\n")
#   })
# } else {
#   cat("\n⚠️ Keine Volatilitätsdaten für erweiterte Analyse verfügbar\n")
# }

# # =============================================================================
# # DEMO 5: EXPORT UND SPEICHERUNG DER ANALYSEN
# # =============================================================================

# cat("\nDEMO 5: EXPORT DER ANALYSEERGEBNISSE\n")
# cat("=====================================\n")

# # Erstelle Reports-Verzeichnis
# if (!dir.exists("reports/analytics")) {
#   dir.create("reports/analytics", recursive = TRUE)
# }

# timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")

# # Export verschiedener Analysen als CSV (nur wenn Daten vorhanden)
# tryCatch({
#   # Altersgruppen Summary (immer verfügbar)
#   write.csv(age_summary, 
#            paste0("reports/analytics/age_summary_", timestamp, ".csv"), 
#            row.names = FALSE)
#   cat("✅ Exported: age_summary\n")
  
#   # Volatilitäts-Statistiken (nur wenn verfügbar)
#   if (nrow(volatility_stats) > 0) {
#     write.csv(volatility_stats, 
#              paste0("reports/analytics/volatility_stats_", timestamp, ".csv"), 
#              row.names = FALSE)
#     cat("✅ Exported: volatility_stats\n")
#   } else {
#     cat("⚠️ Skipped: volatility_stats (no data)\n")
#   }
  
#   # Zinssensitivität (nur wenn verfügbar)
#   if (nrow(interest_sensitivity) > 0) {
#     write.csv(interest_sensitivity, 
#              paste0("reports/analytics/interest_sensitivity_", timestamp, ".csv"), 
#              row.names = FALSE)
#     cat("✅ Exported: interest_sensitivity\n")
#   } else {
#     cat("⚠️ Skipped: interest_sensitivity (no data)\n")
#   }
  
#   # Mandanten Analysen (als JSON für komplexe Struktur)
#   write(jsonlite::toJSON(mandant_summaries, pretty = TRUE),
#         paste0("reports/analytics/mandant_summaries_", timestamp, ".json"))
#   cat("✅ Exported: mandant_summaries\n")
  
#   # Kombinierte Analyse
#   write.csv(age_mandant_cross, 
#            paste0("reports/analytics/age_mandant_cross_", timestamp, ".csv"), 
#            row.names = FALSE)
#   cat("✅ Exported: age_mandant_cross\n")
  
#   cat("\n✅ EXPORT ERFOLGREICH:\n")
#   cat("   📁 Analyseergebnisse gespeichert in: reports/analytics/\n")
#   cat("   🏷️  Timestamp:", timestamp, "\n")
  
# }, error = function(e) {
#   cat("❌ Export Error:", e$message, "\n")
# })

# # =============================================================================
# # DEMO 6: BUSINESS INSIGHTS ZUSAMMENFASSUNG
# # =============================================================================

# cat("\nDEMO 6: BUSINESS INSIGHTS ZUSAMMENFASSUNG\n")
# cat("==========================================\n")

# # Erstelle eine Executive Summary der wichtigsten Erkenntnisse
# cat("🎯 KEY BUSINESS INSIGHTS:\n\n")

# # 1. Altersverteilung Insights
# age_insights <- age_summary %>%
#   arrange(desc(count))

# dominant_age_group <- age_insights$age_group[1]
# cat("👥 KUNDEN-DEMOGRAFIE:\n")
# cat("   - Stärkste Altersgruppe:", dominant_age_group, 
#     "(", age_insights$percentage[1], "% der Kunden)\n")
# cat("   - Durchschnittsalter der Kunden:", 
#     round(weighted.mean(age_insights$mean_age, age_insights$count), 1), "Jahre\n")

# # 2. Mandanten Insights
# if (!is.null(mandant_summaries$distribution)) {
#   mandant_leader <- mandant_summaries$distribution %>%
#     arrange(desc(contract_count))
  
#   cat("\n🏢 MANDANTEN-VERTEILUNG:\n")
#   cat("   - Führender Mandant:", mandant_leader$mandantid[1], 
#       "(", mandant_leader$percentage[1], "% der Verträge)\n")
# }

# # 3. Volatilitäts Insights (nur wenn verfügbar)
# if (nrow(volatility_stats) > 0) {
#   high_vol_contracts <- sum(volatility_stats$volatility_class %in% c("Hoch", "Sehr hoch"))
#   total_contracts <- nrow(volatility_stats)
  
#   cat("\n📊 RISIKO-PROFIL:\n")
#   cat("   - Hochvolatile Verträge:", high_vol_contracts, "von", total_contracts,
#       "(", round(100 * high_vol_contracts / total_contracts, 1), "%)\n")
#   cat("   - Durchschnittliche Portfolio-Volatilität:", 
#       round(mean(volatility_stats$volatility, na.rm = TRUE), 2), "%\n")
# } else {
#   cat("\n📊 RISIKO-PROFIL:\n")
#   cat("   - ⚠️ Volatilitätsanalyse nicht verfügbar (zu wenig historische Daten)\n")
# }

# # 4. Zinssensitivitäts Insights (nur wenn verfügbar)
# if (nrow(interest_sensitivity) > 0) {
#   high_sens_contracts <- sum(interest_sensitivity$sensitivity_class %in% c("Hoch", "Sehr hoch"))
#   total_sens_contracts <- nrow(interest_sensitivity)
  
#   cat("\n💰 ZINSSENSITIVITÄT:\n")
#   cat("   - Hochsensitive Verträge:", high_sens_contracts, "von", total_sens_contracts,
#       "(", round(100 * high_sens_contracts / total_sens_contracts, 1), "%)\n")
#   cat("   - Durchschnittliche Korrelation:", 
#       round(mean(abs(interest_sensitivity$correlation), na.rm = TRUE), 3), "\n")
# } else {
#   cat("\n💰 ZINSSENSITIVITÄT:\n")
#   cat("   - ⚠️ Zinssensitivitätsanalyse nicht verfügbar (zu wenig historische Daten)\n")
# }

# # 5. Datenqualitäts-Hinweise
# cat("\n📋 DATENQUALITÄT:\n")
# cat("   - Verträge mit Zeitreihendaten:", length(unique(timeseries_for_analysis$contractid)), "von", nrow(contracts_clean), "\n")
# cat("   - Kunden mit Altersangaben:", nrow(customers_with_age), "von", nrow(customers_clean), "\n")
# cat("   - Durchschnittliche Beobachtungen pro Vertrag:", 
#     round(nrow(timeseries_for_analysis) / length(unique(timeseries_for_analysis$contractid)), 1), "\n")

# cat("\n🎉 ANALYTICS DEMO COMPLETED SUCCESSFULLY!\n")
# cat("=========================================\n")
# cat("📊 Alle verfügbaren Module erfolgreich getestet\n")
# cat("📁 Ergebnisse gespeichert in: reports/analytics/\n")
# cat("🔧 Modular strukturierte Funktionen einsatzbereit\n")

# =============================================================================
# USAGE GUIDE FÜR PRODUCTION
# =============================================================================

cat("\n📖 PRODUCTION USAGE GUIDE:\n")
cat("===========================\n")
cat("1. Altersklassen:\n")
cat("   result <- age_classification$classify_age_groups(customers_data)\n")
cat("   summary <- age_classification$create_age_summary(result)\n\n")

# cat("2. Zeitreihen-Analyse (mit ausreichend historischen Daten):\n")
# cat("   growth <- timeseries_analysis$calculate_monthly_growth(timeseries_data)\n")
# cat("   volatility <- timeseries_analysis$calculate_volatility(growth)\n")
# cat("   sensitivity <- timeseries_analysis$calculate_interest_sensitivity(timeseries_data)\n\n")

# cat("3. Mandanten-Summaries:\n")
# cat("   summaries <- mandant_analysis$create_mandant_summaries(contracts, customers, timeseries)\n\n")

# cat("💡 Hinweis:\n")
# cat("   - Zeitreihen-Analysen benötigen ausreichend historische Daten\n")
# cat("   - Mindestens 6-12 Beobachtungen pro Vertrag für robuste Ergebnisse\n")
# cat("   - Alle Funktionen sind vollständig dokumentiert mit {roxygen2}!\n")