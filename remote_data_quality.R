# Remote/External Data Quality Assessment
# File: remote_data_quality.R
# 
# Specialized quality checks for external data sources (interest rates, etc.)

cat("=== REMOTE/EXTERNAL DATA QUALITY ASSESSMENT ===\n")
cat("Started:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")

# Load required packages
required_packages <- c("quantmod", "dplyr", "jsonlite")

for (pkg in required_packages) {
  if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
    cat("Installing", pkg, "...\n")
    renv::install(pkg)
    library(pkg, character.only = TRUE)
  }
}

cat("✓ Required packages loaded\n\n")

# CREATE OUTPUT DIRECTORY
if (!dir.exists("reports")) {
  dir.create("reports", recursive = TRUE)
}

# FUNCTION: Quality checks for remote financial data
check_remote_financial_data <- function(data, data_name, expected_columns = NULL) {
  cat("=== QUALITY CHECKS FOR", toupper(data_name), "===\n")
  
  if (is.null(data) || (is.data.frame(data) && nrow(data) == 0)) {
    cat("❌ No data available for", data_name, "\n\n")
    return(list(issues = 999, status = "no_data"))
  }
  
  issues_found <- 0
  
  # Basic information
  cat("BASIC INFORMATION:\n")
  cat("- Data type:", class(data)[1], "\n")
  
  if (is.data.frame(data)) {
    cat("- Total rows:", nrow(data), "\n")
    cat("- Total columns:", ncol(data), "\n")
    cat("- Column names:", paste(names(data), collapse = ", "), "\n")
    
    # Data types
    col_types <- sapply(data, function(x) class(x)[1])
    cat("- Column types:", paste(names(col_types), "=", col_types, collapse = ", "), "\n")
  } else {
    cat("- Length:", length(data), "\n")
  }
  
  cat("\n")
  
  # MISSING VALUES CHECK for data frames
  if (is.data.frame(data)) {
    cat("MISSING VALUES ANALYSIS:\n")
    
    missing_summary <- sapply(data, function(x) {
      if (is.numeric(x)) {
        sum(is.na(x) | is.infinite(x))
      } else {
        sum(is.na(x) | x == "" | x == "NULL")
      }
    })
    
    total_missing <- sum(missing_summary)
    
    for (col in names(missing_summary)) {
      if (missing_summary[col] > 0) {
        percentage <- round(100 * missing_summary[col] / nrow(data), 1)
        cat("  ❌", col, ":", missing_summary[col], "missing (", percentage, "%)\n")
        issues_found <- issues_found + missing_summary[col]
      } else {
        cat("  ✅", col, ": complete (0 missing)\n")
      }
    }
    
    if (total_missing == 0) {
      cat("🎉 EXCELLENT: No missing values found!\n")
    }
    cat("\n")
  }
  
  # EXPECTED COLUMNS CHECK
  if (!is.null(expected_columns) && is.data.frame(data)) {
    cat("EXPECTED COLUMNS CHECK:\n")
    
    missing_cols <- setdiff(expected_columns, names(data))
    extra_cols <- setdiff(names(data), expected_columns)
    
    if (length(missing_cols) > 0) {
      cat("  ❌ MISSING COLUMNS:", paste(missing_cols, collapse = ", "), "\n")
      issues_found <- issues_found + length(missing_cols)
    }
    
    if (length(extra_cols) > 0) {
      cat("  ⚠️ EXTRA COLUMNS:", paste(extra_cols, collapse = ", "), "\n")
    }
    
    if (length(missing_cols) == 0 && length(extra_cols) == 0) {
      cat("  ✅ SCHEMA MATCH: All expected columns present\n")
    }
    cat("\n")
  }
  
  # DATE VALIDATION (for financial time series)
  date_cols <- names(data)[sapply(data, function(x) inherits(x, "Date") || inherits(x, "POSIXt"))]
  
  if (length(date_cols) > 0) {
    cat("DATE VALIDATION:\n")
    
    for (col in date_cols) {
      dates <- data[[col]]
      valid_dates <- dates[!is.na(dates)]
      
      if (length(valid_dates) > 0) {
        # Check date range
        min_date <- min(valid_dates)
        max_date <- max(valid_dates)
        
        cat("  📅", toupper(col), "RANGE:", min_date, "to", max_date, "\n")
        
        # Check for future dates (may be inappropriate for historical data)
        future_dates <- sum(valid_dates > Sys.Date())
        if (future_dates > 0) {
          cat("    ⚠️ FUTURE DATES: Found", future_dates, "dates in the future\n")
        }
        
        # Check for very old dates (before 1900 - probably errors)
        very_old <- sum(valid_dates < as.Date("1900-01-01"))
        if (very_old > 0) {
          cat("    ❌ VERY OLD DATES: Found", very_old, "dates before 1900\n")
          issues_found <- issues_found + very_old
        }
        
        # Check for gaps in time series
        if (length(valid_dates) > 1) {
          sorted_dates <- sort(valid_dates)
          gaps <- diff(sorted_dates)
          large_gaps <- sum(gaps > 35) # More than 35 days gap
          
          if (large_gaps > 0) {
            cat("    ⚠️ TIME GAPS: Found", large_gaps, "gaps larger than 35 days\n")
          } else {
            cat("    ✅ CONTINUITY: No large time gaps\n")
          }
        }
      }
    }
    cat("\n")
  }
  
  # NUMERIC VALUE VALIDATION
  numeric_cols <- names(data)[sapply(data, is.numeric)]
  
  if (length(numeric_cols) > 0) {
    cat("NUMERIC VALUE VALIDATION:\n")
    
    for (col in numeric_cols) {
      values <- data[[col]]
      valid_values <- values[!is.na(values) & !is.infinite(values)]
      
      if (length(valid_values) > 0) {
        # Basic statistics
        cat("  📊", toupper(col), "STATISTICS:\n")
        cat("    - Range:", round(min(valid_values), 4), "to", round(max(valid_values), 4), "\n")
        cat("    - Mean:", round(mean(valid_values), 4), "\n")
        cat("    - Median:", round(median(valid_values), 4), "\n")
        cat("    - Std Dev:", round(sd(valid_values), 4), "\n")
        
        # Check for negative values (unusual for interest rates)
        if (grepl("rate|interest|yield", col, ignore.case = TRUE)) {
          negative_count <- sum(valid_values < 0)
          if (negative_count > 0) {
            cat("    ⚠️ NEGATIVE RATES: Found", negative_count, "negative values\n")
            cat("    (Note: Negative rates can be normal in some markets)\n")
          } else {
            cat("    ✅ NO NEGATIVE RATES\n")
          }
        }
        
        # Check for extreme outliers (values beyond 3 standard deviations)
        if (length(valid_values) > 10) {
          mean_val <- mean(valid_values)
          sd_val <- sd(valid_values)
          outliers <- sum(abs(valid_values - mean_val) > 3 * sd_val)
          
          if (outliers > 0) {
            cat("    ⚠️ OUTLIERS: Found", outliers, "extreme values (>3 std dev)\n")
          } else {
            cat("    ✅ NO EXTREME OUTLIERS\n")
          }
        }
        
        # Check for constant values (no variation)
        if (sd(valid_values) < 0.001) {
          cat("    ⚠️ CONSTANT VALUES: Little to no variation in data\n")
          issues_found <- issues_found + 1
        }
        
      } else {
        cat("  ❌", toupper(col), ": All values are missing or invalid\n")
        issues_found <- issues_found + 1
      }
    }
    cat("\n")
  }
  
  # DATA RECENCY CHECK
  if (is.data.frame(data) && length(date_cols) > 0) {
    cat("DATA RECENCY CHECK:\n")
    
    latest_date <- max(data[[date_cols[1]]], na.rm = TRUE)
    days_old <- as.numeric(Sys.Date() - latest_date)
    
    cat("  📅 Latest data point:", latest_date, "\n")
    cat("  ⏰ Days since latest data:", days_old, "\n")
    
    if (days_old > 30) {
      cat("  ⚠️ DATA STALENESS: Data is more than 30 days old\n")
    } else if (days_old > 7) {
      cat("  ⚠️ DATA AGE: Data is more than 7 days old\n")
    } else {
      cat("  ✅ FRESH DATA: Recent data available\n")
    }
  }
  
  cat("ISSUES FOUND FOR", toupper(data_name), ":", issues_found, "\n\n")
  return(list(issues = issues_found, data = data))
}

# RETRIEVE AND CHECK EXTERNAL INTEREST RATES
cat("RETRIEVING AND CHECKING EXTERNAL INTEREST RATES\n")
cat("===============================================\n")

# Try to get real interest rate data
interest_data <- NULL
source_method <- "none"

tryCatch({
  cat("Attempting to retrieve 10-Year Treasury rates...\n")
  
  if (requireNamespace("quantmod", quietly = TRUE)) {
    library(quantmod)
    
    # Download Treasury data
    rates_xts <- getSymbols("^TNX", src = "yahoo", 
                           from = "2020-01-01", 
                           to = "2024-12-31", 
                           auto.assign = FALSE)
    
    if (!is.null(rates_xts) && nrow(rates_xts) > 0) {
      # Convert to data frame
      interest_data <- data.frame(
        date = index(rates_xts),
        close = as.numeric(Cl(rates_xts)),
        high = as.numeric(Hi(rates_xts)),
        low = as.numeric(Lo(rates_xts)),
        volume = as.numeric(Vo(rates_xts))
      )
      
      # Clean column names
      names(interest_data) <- c("date", "interest_rate", "high", "low", "volume")
      
      # Create monthly aggregation
      interest_data$year_month <- format(interest_data$date, "%Y-%m")
      
      monthly_rates <- interest_data %>%
        group_by(year_month) %>%
        summarise(
          date = max(date),
          interest_rate = last(interest_rate),
          avg_rate = mean(interest_rate, na.rm = TRUE),
          .groups = 'drop'
        )
      
      source_method <- "Yahoo Finance ^TNX"
      cat("✓ Successfully retrieved", nrow(interest_data), "daily data points\n")
      cat("✓ Aggregated to", nrow(monthly_rates), "monthly data points\n\n")
      
      # Use monthly data for quality checks
      interest_data <- monthly_rates
    }
  }
  
}, error = function(e) {
  cat("❌ Failed to retrieve external data:", e$message, "\n")
})

# If external data retrieval failed, check if we have saved data
if (is.null(interest_data)) {
  cat("Checking for previously saved interest rate data...\n")
  
  if (file.exists("data/processed/real_interest_rates.csv")) {
    tryCatch({
      interest_data <- read.csv("data/processed/real_interest_rates.csv")
      interest_data$date <- as.Date(interest_data$date)
      source_method <- "Saved CSV file"
      cat("✓ Loaded", nrow(interest_data), "data points from saved file\n\n")
    }, error = function(e) {
      cat("❌ Could not load saved data:", e$message, "\n")
    })
  }
}

# PERFORM QUALITY CHECKS ON INTEREST RATE DATA
if (!is.null(interest_data)) {
  
  expected_cols <- c("date", "interest_rate", "year_month")
  interest_quality <- check_remote_financial_data(
    interest_data, 
    "INTEREST_RATES", 
    expected_columns = expected_cols
  )
  
  # ADDITIONAL BUSINESS LOGIC FOR INTEREST RATES
  cat("INTEREST RATE SPECIFIC CHECKS:\n")
  cat("------------------------------\n")
  
  if ("interest_rate" %in% names(interest_data)) {
    rates <- interest_data$interest_rate[!is.na(interest_data$interest_rate)]
    
    # Check for reasonable range (0% to 20% is normal for most economies)
    unreasonable_rates <- sum(rates < -5 | rates > 25)
    if (unreasonable_rates > 0) {
      cat("  ❌ UNREASONABLE RATES: Found", unreasonable_rates, "rates outside -5% to 25% range\n")
    } else {
      cat("  ✅ REASONABLE RANGE: All rates within expected bounds\n")
    }
    
    # Check for volatility (sudden jumps > 2%)
    if (length(rates) > 1 && "date" %in% names(interest_data)) {
      ordered_data <- interest_data[order(interest_data$date), ]
      rate_changes <- diff(ordered_data$interest_rate)
      large_jumps <- sum(abs(rate_changes) > 2, na.rm = TRUE)
      
      if (large_jumps > 0) {
        cat("  ⚠️ VOLATILITY: Found", large_jumps, "rate changes > 2 percentage points\n")
      } else {
        cat("  ✅ STABLE: No extreme rate jumps detected\n")
      }
    }
  }
  
  cat("\n")
  
} else {
  cat("❌ NO INTEREST RATE DATA AVAILABLE FOR QUALITY CHECKS\n\n")
  interest_quality <- list(issues = 999, status = "no_data")
}

# CHECK OTHER EXTERNAL DATA SOURCES (if any)
# You can add more external data checks here as needed

# FINAL SUMMARY
cat("==================================================\n")
cat("REMOTE DATA QUALITY ASSESSMENT SUMMARY\n")
cat("==================================================\n")

cat("DATA SOURCES CHECKED:\n")
cat("- Interest Rates (", source_method, "):", 
    ifelse(is.null(interest_data), "❌ FAILED", "✅ SUCCESS"), "\n")

if (!is.null(interest_quality)) {
  cat("- Interest Rate Issues:", interest_quality$issues, "\n")
}

cat("\nOVERALL STATUS:\n")
total_remote_issues <- ifelse(is.null(interest_quality), 999, interest_quality$issues)

if (total_remote_issues == 0) {
  cat("🎉 EXCELLENT: All remote data sources are high quality\n")
  cat("✅ External data is ready for ETL integration\n")
} else if (total_remote_issues < 10) {
  cat("⚠️ MINOR ISSUES: Found", total_remote_issues, "quality issues\n")
  cat("🔍 Review issues above but data may still be usable\n")
} else {
  cat("❌ MAJOR ISSUES: Found", total_remote_issues, "quality issues\n")
  cat("🛠️ Recommend fixing issues before using in ETL pipeline\n")
}

cat("\nRECOMMENDATIONS:\n")
cat("1. 🔄 Set up automated data refresh schedule\n")
cat("2. 📊 Monitor data quality over time\n")
cat("3. 🚨 Set up alerts for data staleness\n")
cat("4. 🔍 Validate data against alternative sources\n")

# Save results
remote_summary <- data.frame(
  data_source = "interest_rates",
  source_method = source_method,
  issues_found = total_remote_issues,
  data_points = ifelse(is.null(interest_data), 0, nrow(interest_data)),
  assessment_time = Sys.time(),
  status = ifelse(total_remote_issues == 0, "good", 
                 ifelse(total_remote_issues < 10, "warning", "critical"))
)

write.csv(remote_summary, "reports/remote_data_quality_summary.csv", row.names = FALSE)

cat("\n📊 Remote data summary saved to: reports/remote_data_quality_summary.csv\n")
cat("📅 Remote assessment completed:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("==================================================\n")