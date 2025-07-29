# =============================================================================
# MODULAR INSURANCE ANALYTICS - VOLLSTÄNDIGE BOX + ROXYGEN2 IMPLEMENTIERUNG
# =============================================================================
# File: insurance_analytics.R
# Purpose: Modular analytics functions for insurance data
# Dependencies: {box}, {roxygen2}, {dplyr}, {lubridate}

# =============================================================================
# PACKAGE SETUP AND DEPENDENCIES
# =============================================================================

cat("=== MODULAR INSURANCE ANALYTICS SETUP ===\n")

# Install required packages (but handle box specially)
required_packages <- c("dplyr", "lubridate", "ggplot2", "duckdb")

for (pkg in required_packages) {
  if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
    cat("Installing", pkg, "...\n")
    install.packages(pkg)
    library(pkg, character.only = TRUE)
  }
}

# Handle box package specially - it should NOT be loaded with library()
if (!requireNamespace("box", quietly = TRUE)) {
  cat("Installing box package...\n")
  install.packages("box")
}

# Use box::use() instead of library(box)
# Box module setup - import functions we need
box::use(
  dplyr[...],
  lubridate[...],
  stats[...]
)

cat("✓ Modular Insurance Analytics loaded with {box}\n")

# =============================================================================
# MODULE 1: ALTERSKLASSEN ANALYSIS
# =============================================================================

#' @export
age_classification <- list()

#' Berechne Alter basierend auf Geburtsdatum
#' 
#' @description
#' Berechnet das Alter einer Person basierend auf dem Geburtsdatum
#' zum aktuellen Datum oder einem spezifizierten Stichtag.
#' 
#' @param birthdate Date vector - Geburtsdaten
#' @param reference_date Date - Referenzdatum (Standard: heute)
#' 
#' @return Numeric vector mit Altern in Jahren
#' 
#' @examples
#' birthdates <- as.Date(c("1990-05-15", "1985-12-01", "1975-03-20"))
#' age_classification$calculate_age(birthdates)
#' 
#' @export
age_classification$calculate_age <- function(birthdate, reference_date = Sys.Date()) {
  if (!inherits(birthdate, "Date")) {
    stop("birthdate must be a Date object")
  }
  
  if (!inherits(reference_date, "Date")) {
    stop("reference_date must be a Date object")
  }
  
  age_years <- as.numeric(difftime(reference_date, birthdate, units = "days")) / 365.25
  return(round(age_years, 1))
}

#' Klassifiziere Kunden nach Altersgruppen
#' 
#' @description
#' Teilt Kunden in definierte Altersklassen ein: <30, 30-50, >50.
#' Kann auch für custom Altersklassen verwendet werden.
#' 
#' @param customers_data data.frame - Kundendaten mit birthdate Spalte
#' @param age_breaks numeric vector - Altersgrenzen (Standard: c(30, 50))
#' @param age_labels character vector - Labels für Altersgruppen
#' @param reference_date Date - Stichtag für Altersberechnung
#' 
#' @return data.frame mit zusätzlichen Spalten: age, age_group
#' 
#' @examples
#' result <- age_classification$classify_age_groups(customers_data)
#' table(result$age_group)
#' 
#' @export
age_classification$classify_age_groups <- function(customers_data, 
                                                 age_breaks = c(30, 50),
                                                 age_labels = c("<30", "30-50", ">50"),
                                                 reference_date = Sys.Date()) {
  
  cat("ALTERSKLASSEN ANALYSE\n")
  cat("=====================\n")
  
  if (!"birthdate" %in% names(customers_data)) {
    stop("customers_data must contain 'birthdate' column")
  }
  
  # Remove rows with missing birthdates
  valid_customers <- customers_data[!is.na(customers_data$birthdate), ]
  
  if (nrow(valid_customers) == 0) {
    stop("No valid birthdates found in data")
  }
  
  # Calculate ages
  valid_customers$age <- age_classification$calculate_age(
    valid_customers$birthdate, 
    reference_date
  )
  
  # Create age groups
  valid_customers$age_group <- cut(
    valid_customers$age,
    breaks = c(0, age_breaks, Inf),
    labels = age_labels,
    right = FALSE
  )
  
  # Statistics
  cat("📊 ALTERSVERTEILUNG:\n")
  age_summary <- table(valid_customers$age_group)
  for (i in 1:length(age_summary)) {
    percentage <- round(100 * age_summary[i] / sum(age_summary), 1)
    cat("   ", names(age_summary)[i], ":", age_summary[i], 
        "Kunden (", percentage, "%)\n")
  }
  
  cat("📈 ALTERSSTATISTIKEN:\n")
  cat("   Jüngster Kunde:", round(min(valid_customers$age), 1), "Jahre\n")
  cat("   Ältester Kunde:", round(max(valid_customers$age), 1), "Jahre\n")
  cat("   Durchschnittsalter:", round(mean(valid_customers$age), 1), "Jahre\n")
  cat("   Median-Alter:", round(median(valid_customers$age), 1), "Jahre\n\n")
  
  return(valid_customers)
}

#' Erstelle Altersgruppen-Summary mit Statistiken
#' 
#' @description
#' Generiert detaillierte Statistiken für jede Altersgruppe.
#' 
#' @param age_classified_data data.frame - Daten mit age_group Spalte
#' 
#' @return data.frame mit Altersgruppen-Statistiken
#' 
#' @export
age_classification$create_age_summary <- function(age_classified_data) {
  
  if (!"age_group" %in% names(age_classified_data)) {
    stop("Data must contain 'age_group' column")
  }
  
  summary_stats <- age_classified_data %>%
    group_by(age_group) %>%
    summarise(
      count = n(),
      min_age = round(min(age, na.rm = TRUE), 1),
      max_age = round(max(age, na.rm = TRUE), 1),
      mean_age = round(mean(age, na.rm = TRUE), 1),
      median_age = round(median(age, na.rm = TRUE), 1),
      std_age = round(sd(age, na.rm = TRUE), 1),
      .groups = 'drop'
    ) %>%
    mutate(
      percentage = round(100 * count / sum(count), 1)
    )
  
  return(summary_stats)
}

# =============================================================================
# MODULE 2: ZEITREIHEN ANALYSIS  
# =============================================================================

#' @export
timeseries_analysis <- list()

#' Berechne monatliche Wachstumsraten für Rückkaufswerte
#' 
#' @description
#' Berechnet monatliche Wachstumsraten für jeden Vertrag basierend auf
#' den Rückkaufswerten über die Zeit.
#' 
#' @param timeseries_data data.frame - Zeitreihendaten mit contractid, date, surrender_value
#' @param min_observations integer - Mindestanzahl Beobachtungen pro Vertrag
#' 
#' @return data.frame mit monatlichen Wachstumsraten
#' 
#' @examples
#' growth_rates <- timeseries_analysis$calculate_monthly_growth(timeseries_data)
#' 
#' @export
timeseries_analysis$calculate_monthly_growth <- function(timeseries_data, 
                                                       min_observations = 3) {
  
  cat("MONATLICHE WACHSTUMSRATEN ANALYSE\n")
  cat("=================================\n")
  
  required_cols <- c("contractid", "date", "surrender_value")
  if (!all(required_cols %in% names(timeseries_data))) {
    stop(paste("Data must contain columns:", paste(required_cols, collapse = ", ")))
  }
  
  # Data preparation
  clean_data <- timeseries_data %>%
    filter(!is.na(contractid), !is.na(date), !is.na(surrender_value)) %>%
    arrange(contractid, date)
  
  cat("📊 Verarbeite", length(unique(clean_data$contractid)), "Verträge\n")
  cat("📅 Zeitraum:", min(clean_data$date), "bis", max(clean_data$date), "\n")
  
  # Calculate monthly growth rates
  growth_data <- clean_data %>%
    group_by(contractid) %>%
    filter(n() >= min_observations) %>%  # Minimum observations
    arrange(date) %>%
    mutate(
      previous_value = lag(surrender_value),
      days_diff = as.numeric(date - lag(date)),
      # Monatliche Wachstumsrate (annualisiert)
      monthly_growth_rate = ifelse(
        !is.na(previous_value) & previous_value > 0,
        ((surrender_value / previous_value) ^ (30 / days_diff) - 1) * 100,
        NA
      ),
      # Absolute Veränderung
      absolute_change = surrender_value - previous_value
    ) %>%
    filter(!is.na(monthly_growth_rate)) %>%
    ungroup()
  
  # Summary statistics
  cat("📈 WACHSTUMSRATEN STATISTIKEN:\n")
  cat("   Durchschnittliche monatliche Wachstumsrate:", 
      round(mean(growth_data$monthly_growth_rate, na.rm = TRUE), 2), "%\n")
  cat("   Median monatliche Wachstumsrate:", 
      round(median(growth_data$monthly_growth_rate, na.rm = TRUE), 2), "%\n")
  cat("   Standardabweichung:", 
      round(sd(growth_data$monthly_growth_rate, na.rm = TRUE), 2), "%\n")
  cat("   Min/Max Wachstumsraten:", 
      round(min(growth_data$monthly_growth_rate, na.rm = TRUE), 2), "% /", 
      round(max(growth_data$monthly_growth_rate, na.rm = TRUE), 2), "%\n\n")
  
  return(growth_data)
}

#' Berechne Volatilität pro Vertrag
#' 
#' @description
#' Berechnet die Volatilität der Rückkaufswerte für jeden Vertrag
#' basierend auf der Standardabweichung der Wachstumsraten.
#' 
#' @param growth_data data.frame - Daten mit monatlichen Wachstumsraten
#' @param min_periods integer - Mindestanzahl Perioden für Volatilitätsberechnung
#' 
#' @return data.frame mit Volatilitätskennzahlen pro Vertrag
#' 
#' @export
timeseries_analysis$calculate_volatility <- function(growth_data, min_periods = 6) {
  
  cat("VOLATILITÄTS ANALYSE\n")
  cat("====================\n")
  
  if (!"monthly_growth_rate" %in% names(growth_data)) {
    stop("Data must contain 'monthly_growth_rate' column")
  }
  
  volatility_stats <- growth_data %>%
    group_by(contractid) %>%
    filter(n() >= min_periods) %>%
    summarise(
      observations = n(),
      mean_growth_rate = round(mean(monthly_growth_rate, na.rm = TRUE), 3),
      volatility = round(sd(monthly_growth_rate, na.rm = TRUE), 3),
      min_growth = round(min(monthly_growth_rate, na.rm = TRUE), 3),
      max_growth = round(max(monthly_growth_rate, na.rm = TRUE), 3),
      growth_range = round(max_growth - min_growth, 3),
      # Risk-adjusted return (Sharpe-like ratio)
      risk_adjusted_return = ifelse(volatility > 0, 
                                   round(mean_growth_rate / volatility, 3), 
                                   NA),
      .groups = 'drop'
    ) %>%
    arrange(desc(volatility))
  
  # Volatility classification
  volatility_stats$volatility_class <- cut(
    volatility_stats$volatility,
    breaks = c(0, 1, 3, 5, Inf),
    labels = c("Niedrig", "Mittel", "Hoch", "Sehr hoch"),
    right = FALSE
  )
  
  cat("📊 VOLATILITÄTS VERTEILUNG:\n")
  vol_distribution <- table(volatility_stats$volatility_class)
  for (i in 1:length(vol_distribution)) {
    percentage <- round(100 * vol_distribution[i] / sum(vol_distribution), 1)
    cat("   ", names(vol_distribution)[i], ":", vol_distribution[i], 
        "Verträge (", percentage, "%)\n")
  }
  
  cat("📈 VOLATILITÄTS STATISTIKEN:\n")
  cat("   Durchschnittliche Volatilität:", 
      round(mean(volatility_stats$volatility, na.rm = TRUE), 2), "%\n")
  cat("   Median Volatilität:", 
      round(median(volatility_stats$volatility, na.rm = TRUE), 2), "%\n")
  cat("   Höchste Volatilität:", 
      round(max(volatility_stats$volatility, na.rm = TRUE), 2), "%\n\n")
  
  return(volatility_stats)
}

#' Berechne Zinssensitivität pro Vertrag
#' 
#' @description
#' Schätzt die Zinssensitivität durch Korrelation zwischen Rückkaufswerten
#' und einem Referenzzinssatz (z.B. 10-Jahr Treasury).
#' 
#' @param timeseries_data data.frame - Zeitreihendaten
#' @param interest_rate_data data.frame - Zinsdaten mit date und interest_rate
#' @param min_observations integer - Mindestbeobachtungen
#' 
#' @return data.frame mit Zinssensitivitäts-Kennzahlen
#' 
#' @export
timeseries_analysis$calculate_interest_sensitivity <- function(timeseries_data, 
                                                             interest_rate_data = NULL,
                                                             min_observations = 12) {
  
  cat("ZINSSENSITIVITÄTS ANALYSE\n")
  cat("=========================\n")
  
  # If no interest rate data provided, create synthetic data
  if (is.null(interest_rate_data)) {
    cat("⚠️ Keine Zinsdaten bereitgestellt - verwende synthetische 10Y Treasury Daten\n")
    
    date_range <- seq(
      from = min(timeseries_data$date, na.rm = TRUE),
      to = max(timeseries_data$date, na.rm = TRUE),
      by = "month"
    )
    
    # Synthetic interest rate trend (realistic 10Y Treasury pattern)
    set.seed(123)
    base_rate <- 2.5
    trend <- seq(-0.5, 1.5, length.out = length(date_range))
    noise <- rnorm(length(date_range), 0, 0.3)
    
    interest_rate_data <- data.frame(
      date = date_range,
      interest_rate = pmax(0.1, base_rate + trend + noise)
    )
  }
  
  cat("📊 Zinsdaten verfügbar für Zeitraum:", 
      min(interest_rate_data$date), "bis", max(interest_rate_data$date), "\n")
  
  # Prepare monthly aggregated data
  monthly_surrender <- timeseries_data %>%
    filter(!is.na(surrender_value)) %>%
    mutate(year_month = floor_date(date, "month")) %>%
    group_by(contractid, year_month) %>%
    summarise(
      avg_surrender_value = mean(surrender_value, na.rm = TRUE),
      .groups = 'drop'
    )
  
  monthly_rates <- interest_rate_data %>%
    mutate(year_month = floor_date(date, "month")) %>%
    group_by(year_month) %>%
    summarise(
      avg_interest_rate = mean(interest_rate, na.rm = TRUE),
      .groups = 'drop'
    )
  
  # Join surrender values with interest rates
  combined_data <- monthly_surrender %>%
    inner_join(monthly_rates, by = "year_month") %>%
    group_by(contractid) %>%
    filter(n() >= min_observations) %>%
    arrange(year_month)
  
  # Calculate interest sensitivity for each contract
  sensitivity_stats <- combined_data %>%
    group_by(contractid) %>%
    summarise(
      observations = n(),
      correlation = round(cor(avg_surrender_value, avg_interest_rate, 
                             use = "complete.obs"), 4),
      # Simple regression: surrender_value ~ interest_rate
      beta_coefficient = {
        if (n() >= min_observations) {
          lm_result <- lm(avg_surrender_value ~ avg_interest_rate)
          round(coef(lm_result)[2], 2)
        } else {
          NA
        }
      },
      r_squared = {
        if (n() >= min_observations) {
          lm_result <- lm(avg_surrender_value ~ avg_interest_rate)
          round(summary(lm_result)$r.squared, 4)
        } else {
          NA
        }
      },
      .groups = 'drop'
    ) %>%
    filter(!is.na(correlation))
  
  # Classify sensitivity
  sensitivity_stats$sensitivity_class <- cut(
    abs(sensitivity_stats$correlation),
    breaks = c(0, 0.3, 0.6, 0.8, 1),
    labels = c("Niedrig", "Mittel", "Hoch", "Sehr hoch"),
    right = FALSE
  )
  
  cat("📊 ZINSSENSITIVITÄTS VERTEILUNG:\n")
  sens_distribution <- table(sensitivity_stats$sensitivity_class)
  for (i in 1:length(sens_distribution)) {
    percentage <- round(100 * sens_distribution[i] / sum(sens_distribution), 1)
    cat("   ", names(sens_distribution)[i], ":", sens_distribution[i], 
        "Verträge (", percentage, "%)\n")
  }
  
  cat("📈 ZINSSENSITIVITÄTS STATISTIKEN:\n")
  cat("   Durchschnittliche Korrelation:", 
      round(mean(sensitivity_stats$correlation, na.rm = TRUE), 3), "\n")
  cat("   Median Korrelation:", 
      round(median(sensitivity_stats$correlation, na.rm = TRUE), 3), "\n")
  cat("   Durchschnittliches R²:", 
      round(mean(sensitivity_stats$r_squared, na.rm = TRUE), 3), "\n\n")
  
  return(sensitivity_stats)
}

# =============================================================================
# MODULE 3: MANDANTEN SUMMARIES
# =============================================================================

#' @export  
mandant_analysis <- list()

#' Erstelle umfassende Mandanten-Summaries
#' 
#' @description
#' Generiert detaillierte Statistiken und Vergleiche zwischen den
#' Mandanten LV1 und LV2 für verschiedene Kennzahlen.
#' 
#' @param contracts_data data.frame - Vertragsdaten mit mandantid
#' @param customers_data data.frame - Kundendaten mit Altersgruppen
#' @param timeseries_data data.frame - Zeitreihendaten für Wertentwicklung
#' @param volatility_data data.frame - Volatilitätsdaten (optional)
#' 
#' @return list mit verschiedenen Mandanten-Analysen
#' 
#' @export
mandant_analysis$create_mandant_summaries <- function(contracts_data,
                                                    customers_data = NULL,
                                                    timeseries_data = NULL,
                                                    volatility_data = NULL) {
  
  cat("MANDANTEN SUMMARIES ANALYSE\n")
  cat("===========================\n")
  
  if (!"mandantid" %in% names(contracts_data)) {
    stop("contracts_data must contain 'mandantid' column")
  }
  
  # Basic mandant distribution
  mandant_distribution <- contracts_data %>%
    filter(!is.na(mandantid)) %>%
    count(mandantid, name = "contract_count") %>%
    mutate(
      percentage = round(100 * contract_count / sum(contract_count), 1)
    )
  
  cat("📊 MANDANTEN VERTEILUNG:\n")
  for (i in 1:nrow(mandant_distribution)) {
    row <- mandant_distribution[i, ]
    cat("   ", row$mandantid, ":", row$contract_count, 
        "Verträge (", row$percentage, "%)\n")
  }
  cat("\n")
  
  results <- list(
    distribution = mandant_distribution
  )
  
  # Age analysis by mandant (if customer data available)
  if (!is.null(customers_data) && "age" %in% names(customers_data)) {
    
    # Join contracts with customers
    contracts_with_customers <- contracts_data %>%
      inner_join(customers_data, by = "customerid") %>%
      filter(!is.na(mandantid), !is.na(age))
    
    age_by_mandant <- contracts_with_customers %>%
      group_by(mandantid) %>%
      summarise(
        count = n(),
        mean_age = round(mean(age, na.rm = TRUE), 1),
        median_age = round(median(age, na.rm = TRUE), 1),
        std_age = round(sd(age, na.rm = TRUE), 1),
        min_age = round(min(age, na.rm = TRUE), 1),
        max_age = round(max(age, na.rm = TRUE), 1),
        .groups = 'drop'
      )
    
    cat("📈 ALTERSSTATISTIKEN JE MANDANT:\n")
    for (i in 1:nrow(age_by_mandant)) {
      row <- age_by_mandant[i, ]
      cat("   ", row$mandantid, ":\n")
      cat("      Durchschnittsalter:", row$mean_age, "Jahre\n")
      cat("      Median-Alter:", row$median_age, "Jahre\n") 
      cat("      Standardabweichung:", row$std_age, "Jahre\n")
      cat("      Altersbereich:", row$min_age, "-", row$max_age, "Jahre\n\n")
    }
    
    results$age_analysis <- age_by_mandant
    
    # Age group distribution by mandant
    if ("age_group" %in% names(customers_data)) {
      age_group_mandant <- contracts_with_customers %>%
        filter(!is.na(age_group)) %>%
        count(mandantid, age_group) %>%
        group_by(mandantid) %>%
        mutate(
          percentage = round(100 * n / sum(n), 1)
        ) %>%
        ungroup()
      
      cat("👥 ALTERSGRUPPEN JE MANDANT:\n")
      for (mandant in unique(age_group_mandant$mandantid)) {
        cat("   ", mandant, ":\n")
        mandant_data <- age_group_mandant[age_group_mandant$mandantid == mandant, ]
        for (j in 1:nrow(mandant_data)) {
          row <- mandant_data[j, ]
          cat("      ", row$age_group, ":", row$n, "(", row$percentage, "%)\n")
        }
        cat("\n")
      }
      
      results$age_group_distribution <- age_group_mandant
    }
  }
  
  # Surrender value analysis by mandant (if timeseries data available)
  if (!is.null(timeseries_data) && "surrender_value" %in% names(timeseries_data)) {
    
    # Join timeseries with contracts to get mandant info
    timeseries_with_mandant <- timeseries_data %>%
      inner_join(contracts_data[c("contractid", "mandantid")], by = "contractid") %>%
      filter(!is.na(mandantid), !is.na(surrender_value))
    
    surrender_by_mandant <- timeseries_with_mandant %>%
      group_by(mandantid) %>%
      summarise(
        observations = n(),
        contracts = n_distinct(contractid),
        mean_surrender_value = round(mean(surrender_value, na.rm = TRUE), 2),
        median_surrender_value = round(median(surrender_value, na.rm = TRUE), 2),
        std_surrender_value = round(sd(surrender_value, na.rm = TRUE), 2),
        min_surrender_value = round(min(surrender_value, na.rm = TRUE), 2),
        max_surrender_value = round(max(surrender_value, na.rm = TRUE), 2),
        q25_surrender_value = round(quantile(surrender_value, 0.25, na.rm = TRUE), 2),
        q75_surrender_value = round(quantile(surrender_value, 0.75, na.rm = TRUE), 2),
        .groups = 'drop'
      )
    
    cat("💰 RÜCKKAUFSWERT STATISTIKEN JE MANDANT:\n")
    for (i in 1:nrow(surrender_by_mandant)) {
      row <- surrender_by_mandant[i, ]
      cat("   ", row$mandantid, ":\n")
      cat("      Verträge:", row$contracts, "\n")
      cat("      Beobachtungen:", row$observations, "\n")
      cat("      Durchschnitt:", row$mean_surrender_value, "€\n")
      cat("      Median:", row$median_surrender_value, "€\n")
      cat("      Standardabweichung:", row$std_surrender_value, "€\n")
      cat("      25%-75% Quartile:", row$q25_surrender_value, "-", row$q75_surrender_value, "€\n")
      cat("      Min-Max:", row$min_surrender_value, "-", row$max_surrender_value, "€\n\n")
    }
    
    results$surrender_value_analysis <- surrender_by_mandant
  }
  
  # Volatility analysis by mandant (if volatility data available)
  if (!is.null(volatility_data) && "volatility" %in% names(volatility_data)) {
    
    # Join volatility with contracts to get mandant info
    volatility_with_mandant <- volatility_data %>%
      inner_join(contracts_data[c("contractid", "mandantid")], by = "contractid") %>%
      filter(!is.na(mandantid), !is.na(volatility))
    
    volatility_by_mandant <- volatility_with_mandant %>%
      group_by(mandantid) %>%
      summarise(
        contracts = n(),
        mean_volatility = round(mean(volatility, na.rm = TRUE), 3),
        median_volatility = round(median(volatility, na.rm = TRUE), 3),
        std_volatility = round(sd(volatility, na.rm = TRUE), 3),
        min_volatility = round(min(volatility, na.rm = TRUE), 3),
        max_volatility = round(max(volatility, na.rm = TRUE), 3),
        mean_risk_adjusted_return = round(mean(risk_adjusted_return, na.rm = TRUE), 3),
        .groups = 'drop'
      )
    
    cat("📊 VOLATILITÄTS STATISTIKEN JE MANDANT:\n")
    for (i in 1:nrow(volatility_by_mandant)) {
      row <- volatility_by_mandant[i, ]
      cat("   ", row$mandantid, ":\n")
      cat("      Verträge:", row$contracts, "\n")
      cat("      Durchschnittliche Volatilität:", row$mean_volatility, "%\n")
      cat("      Median Volatilität:", row$median_volatility, "%\n")
      cat("      Volatilitäts-Streuung:", row$std_volatility, "%\n")
      cat("      Volatilitäts-Range:", row$min_volatility, "-", row$max_volatility, "%\n")
      cat("      Risiko-adjustierte Rendite:", row$mean_risk_adjusted_return, "\n\n")
    }
    
    results$volatility_analysis <- volatility_by_mandant
  }
  
  return(results)
}