# Simple Data Quality Assessment for Insurance Data
# File: simple_data_quality.R
# 
# Comprehensive data quality checks without external dependencies
# Uses basic R functions for maximum compatibility

cat("=== INSURANCE DATA QUALITY ASSESSMENT ===\n")
cat("Started:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("R Version:", R.version.string, "\n\n")

# Load required packages (basic ones only)
required_packages <- c("DBI", "RSQLite", "readxl", "jsonlite", "dplyr")

for (pkg in required_packages) {
  if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
    cat("Installing", pkg, "...\n")
    renv::install(pkg)
    library(pkg, character.only = TRUE)
  }
}

cat("✓ All required packages loaded\n\n")

# CREATE OUTPUT DIRECTORY
if (!dir.exists("reports")) {
  dir.create("reports", recursive = TRUE)
  cat("✓ Created reports directory\n")
}

# LOAD DATA
cat("LOADING DATA FOR QUALITY CHECKS\n")
cat("================================\n")

# 1. Load SQLite data
tryCatch({
  con <- dbConnect(RSQLite::SQLite(), "data/raw/contracts.sqlite")
  tables <- dbListTables(con)
  
  if (length(tables) > 0) {
    contracts_data <- dbGetQuery(con, paste("SELECT * FROM", tables[1]))
    cat("✓ Loaded contracts from table '", tables[1], "':", nrow(contracts_data), "rows\n")
  } else {
    stop("No tables found in SQLite database")
  }
  
  dbDisconnect(con)
}, error = function(e) {
  cat("❌ Error loading SQLite data:", e$message, "\n")
  contracts_data <- NULL
})

# 2. Load Excel data
tryCatch({
  customers_data <- read_excel("data/raw/customers.xlsx")
  # Normalize column names
  names(customers_data) <- tolower(trimws(names(customers_data)))
  cat("✓ Loaded customers:", nrow(customers_data), "rows\n")
}, error = function(e) {
  cat("❌ Error loading Excel data:", e$message, "\n")
  customers_data <- NULL
})

# 3. Load JSON data
tryCatch({
  json_raw <- fromJSON("data/raw/contracts_timeseries.json")
  
  # Handle different JSON structures
  if (is.list(json_raw) && !is.data.frame(json_raw)) {
    # Array of objects - convert to dataframe
    timeseries_data <- do.call(rbind, lapply(json_raw, function(x) {
      if (is.list(x)) {
        data.frame(x, stringsAsFactors = FALSE)
      } else {
        data.frame(value = x, stringsAsFactors = FALSE)
      }
    }))
  } else {
    timeseries_data <- as.data.frame(json_raw)
  }
  
  cat("✓ Loaded timeseries:", nrow(timeseries_data), "rows\n")
}, error = function(e) {
  cat("❌ Error loading JSON data:", e$message, "\n")
  timeseries_data <- NULL
})

cat("\n")

# QUALITY CHECK FUNCTIONS
perform_basic_quality_checks <- function(data, table_name) {
  cat("=== QUALITY CHECKS FOR", toupper(table_name), "===\n")
  
  if (is.null(data)) {
    cat("❌ Data not available for quality checks\n\n")
    return(list(issues = 999, summary = "Data not loaded"))
  }
  
  issues_found <- 0
  
  # Basic information
  cat("BASIC INFORMATION:\n")
  cat("- Total rows:", nrow(data), "\n")
  cat("- Total columns:", ncol(data), "\n")
  cat("- Column names:", paste(names(data), collapse = ", "), "\n")
  cat("- Data types:", paste(sapply(data, class), collapse = ", "), "\n\n")
  
  # MISSING VALUES CHECK
  cat("MISSING VALUES ANALYSIS:\n")
  missing_summary <- sapply(data, function(x) {
    sum(is.na(x) | x == "" | x == "NULL" | x == "null" | is.null(x))
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
  } else {
    cat("⚠️ TOTAL MISSING VALUES:", total_missing, "\n")
  }
  
  cat("\n")
  
  # DUPLICATE CHECK
  cat("DUPLICATE ANALYSIS:\n")
  total_duplicates <- sum(duplicated(data))
  
  if (total_duplicates > 0) {
    cat("  ❌ DUPLICATE ROWS: Found", total_duplicates, "completely duplicate rows\n")
    issues_found <- issues_found + total_duplicates
  } else {
    cat("  ✅ NO DUPLICATE ROWS: All rows are unique\n")
  }
  
  # Contract ID specific checks
  if ("contractid" %in% names(data)) {
    duplicate_ids <- sum(duplicated(data$contractid))
    if (duplicate_ids > 0) {
      cat("  ❌ DUPLICATE CONTRACT IDs: Found", duplicate_ids, "duplicate IDs\n")
      dup_ids <- data$contractid[duplicated(data$contractid)]
      cat("    Examples:", paste(head(dup_ids, 3), collapse = ", "), "\n")
      issues_found <- issues_found + duplicate_ids
    } else {
      cat("  ✅ UNIQUE CONTRACT IDs: All contract IDs are unique\n")
    }
  }
  
  cat("\n")
  
  return(list(issues = issues_found, missing = missing_summary, data = data))
}

# SPECIFIC BUSINESS LOGIC CHECKS
perform_business_logic_checks <- function(data, table_name) {
  cat("BUSINESS LOGIC CHECKS FOR", toupper(table_name), ":\n")
  
  business_issues <- 0

  # Contract ID format check  
  if ("contractid" %in% names(data)) {
    cat("CONTRACT ID FORMAT:\n")
    
    # Remove NA values first, then check format
    valid_contractids <- data$contractid[!is.na(data$contractid) & data$contractid != ""]
    
    if (length(valid_contractids) > 0) {
      # Check V### pattern on non-NA values
      valid_format <- grepl("^V[0-9]+$", valid_contractids)
      invalid_count <- sum(!valid_format)
      
      if (invalid_count > 0) {
        cat("  ❌ INVALID FORMAT: Found", invalid_count, "IDs not matching V### pattern\n")
        invalid_examples <- valid_contractids[!valid_format]
        cat("    Examples:", paste(head(invalid_examples, 5), collapse = ", "), "\n")
        business_issues <- business_issues + invalid_count
        
        # Show what the invalid formats look like
        cat("    Common issues found:\n")
        if (any(grepl("\\^", invalid_examples))) {
          cat("      - Contains ^ characters\n")
        }
        if (any(grepl("\\(", invalid_examples))) {
          cat("      - Contains ( characters\n")
        }
        if (any(grepl(" ", invalid_examples))) {
          cat("      - Contains spaces\n")
        }
        
      } else {
        cat("  ✅ VALID FORMAT: All non-missing contract IDs follow V### pattern\n")
      }
      
      # Only try to extract numeric parts from VALID formatted IDs
      valid_ids_only <- valid_contractids[valid_format]
      if (length(valid_ids_only) > 0) {
        # Extract numeric parts safely
        tryCatch({
          numeric_parts <- gsub("V", "", valid_ids_only)
          # Convert to numeric, handle any conversion errors
          numeric_values <- suppressWarnings(as.numeric(numeric_parts))
          # Remove any NAs that resulted from conversion
          numeric_values <- numeric_values[!is.na(numeric_values)]
          
          if (length(numeric_values) > 0) {
            cat("  📊 VALID ID RANGE: V", sprintf("%03.0f", min(numeric_values)), 
                " to V", sprintf("%03.0f", max(numeric_values)), "\n")
            cat("  📊 Valid formatted IDs:", length(valid_ids_only), "out of", length(valid_contractids), "\n")
          }
        }, error = function(e) {
          cat("  ⚠️ Could not analyze ID range due to format issues\n")
        })
      } else {
        cat("  ❌ NO PROPERLY FORMATTED IDs: No contract IDs follow the V### pattern\n")
      }
      
    } else {
      cat("  ❌ NO VALID IDs: All contract IDs are missing or empty\n")
      business_issues <- business_issues + 1
    }
    
    # Report missing IDs
    missing_ids <- sum(is.na(data$contractid) | data$contractid == "")
    if (missing_ids > 0) {
      cat("  ⚠️ MISSING IDs:", missing_ids, "contract IDs are missing or empty\n")
    }

    # Show summary of all contract ID issues
    total_contracts <- nrow(data)

    # Count valid formatted IDs (handle NAs properly)
    non_na_contractids <- data$contractid[!is.na(data$contractid)]
    valid_formatted <- sum(grepl("^V[0-9]+$", non_na_contractids))

    # Count duplicates (excluding NAs from duplication check)
    duplicates_count <- sum(duplicated(data$contractid[!is.na(data$contractid)]))

    cat("  📋 CONTRACT ID SUMMARY:\n")
    cat("    - Total contracts:", total_contracts, "\n")
    cat("    - Properly formatted (V###):", valid_formatted, "\n")
    cat("    - Invalid format:", invalid_count, "\n")
    cat("    - Missing/empty:", missing_ids, "\n")
    cat("    - Duplicates found:", duplicates_count, "\n")
  }
  
  # Customer ID format check
  if ("customerid" %in% names(data)) {
    cat("CUSTOMER ID FORMAT:\n")
    
    # Remove NA values first, then check format
    valid_customerids <- data$customerid[!is.na(data$customerid) & data$customerid != ""]
    
    if (length(valid_customerids) > 0) {
      valid_format <- grepl("^K[0-9]+$", valid_customerids)
      invalid_count <- sum(!valid_format)
      
      if (invalid_count > 0) {
        cat("  ❌ INVALID FORMAT: Found", invalid_count, "IDs not matching K### pattern\n")
        business_issues <- business_issues + invalid_count
      } else {
        cat("  ✅ VALID FORMAT: All customer IDs follow K### pattern\n")
      }
    } else {
      cat("  ❌ NO VALID CUSTOMER IDs: All customer IDs are missing or empty\n")
    }
  }

  # Birthdate validation
  if ("birthdate" %in% names(data)) {
    cat("BIRTHDATE VALIDATION:\n")
    
    # Convert to Date if not already
    if (!inherits(data$birthdate, "Date")) {
      data$birthdate <- as.Date(data$birthdate)
    }
    
    # Future dates check
    future_dates <- sum(data$birthdate > Sys.Date(), na.rm = TRUE)
    if (future_dates > 0) {
      cat("  ❌ FUTURE BIRTHS: Found", future_dates, "birth dates in the future\n")
      business_issues <- business_issues + future_dates
    } else {
      cat("  ✅ DATE LOGIC: No future birth dates\n")
    }
    
    # Very old dates check (before 1900)
    old_dates <- sum(data$birthdate < as.Date("1900-01-01"), na.rm = TRUE)
    if (old_dates > 0) {
      cat("  ❌ UNREALISTIC AGES: Found", old_dates, "birth dates before 1900\n")
      business_issues <- business_issues + old_dates
    } else {
      cat("  ✅ REALISTIC AGES: No birth dates before 1900\n")
    }
    
    # Age statistics
    ages <- as.numeric(difftime(Sys.Date(), data$birthdate, units = "days")) / 365.25
    valid_ages <- ages[!is.na(ages) & ages >= 0 & ages <= 120]
    
    if (length(valid_ages) > 0) {
      cat("  📊 AGE STATISTICS:\n")
      cat("    - Youngest:", round(min(valid_ages), 1), "years\n")
      cat("    - Oldest:", round(max(valid_ages), 1), "years\n") 
      cat("    - Average:", round(mean(valid_ages), 1), "years\n")
      cat("    - Median:", round(median(valid_ages), 1), "years\n")
    }
  }
  
  # Mandant ID validation
  if ("mandantid" %in% names(data)) {
    cat("MANDANT ID VALIDATION:\n")
    unique_mandants <- unique(data$mandantid[!is.na(data$mandantid)])
    expected_mandants <- c("LV1", "LV2")
    
    unexpected <- setdiff(unique_mandants, expected_mandants)
    if (length(unexpected) > 0) {
      cat("  ❌ UNEXPECTED MANDANTS: Found", paste(unexpected, collapse = ", "), "\n")
      business_issues <- business_issues + length(unexpected)
    } else {
      cat("  ✅ VALID MANDANTS: Only expected values (LV1, LV2)\n")
    }
    
    # Distribution
    mandant_counts <- table(data$mandantid)
    cat("  📊 DISTRIBUTION:", paste(names(mandant_counts), "=", mandant_counts, collapse = ", "), "\n")
  }
  
  # Numeric value validation
  numeric_cols <- sapply(data, is.numeric)
  if (any(numeric_cols)) {
    cat("NUMERIC VALUE VALIDATION:\n")
    
    for (col in names(data)[numeric_cols]) {
      values <- data[[col]]
      
      # Check for negative values (may be inappropriate for insurance values)
      if (grepl("wert|value|amount", col, ignore.case = TRUE)) {
        negative_count <- sum(values < 0, na.rm = TRUE)
        if (negative_count > 0) {
          cat("  ❌", toupper(col), ": Found", negative_count, "negative values\n")
          cat("    Min value:", round(min(values, na.rm = TRUE), 2), "\n")
          business_issues <- business_issues + negative_count
        } else {
          cat("  ✅", toupper(col), ": No negative values\n")
        }
      }
      
      # Basic statistics
      if (sum(!is.na(values)) > 0) {
        cat("    📊 Range:", round(min(values, na.rm = TRUE), 2), 
            "to", round(max(values, na.rm = TRUE), 2), "\n")
        cat("    📊 Average:", round(mean(values, na.rm = TRUE), 2), "\n")
      }
    }
  }
  
  cat("\nBUSINESS LOGIC ISSUES FOUND:", business_issues, "\n\n")
  return(business_issues)
}

# RUN QUALITY CHECKS ON ALL TABLES
cat("EXECUTING COMPREHENSIVE QUALITY ASSESSMENT\n")
cat("==========================================\n\n")

# Initialize results
all_results <- list()
total_issues <- 0

# 1. Contracts Quality Assessment
if (!is.null(contracts_data)) {
  contracts_result <- perform_basic_quality_checks(contracts_data, "contracts")
  contracts_business <- perform_business_logic_checks(contracts_data, "contracts")
  all_results$contracts <- list(
    basic = contracts_result,
    business = contracts_business,
    total_issues = contracts_result$issues + contracts_business
  )
  total_issues <- total_issues + contracts_result$issues + contracts_business
}

# 2. Customers Quality Assessment  
if (!is.null(customers_data)) {
  customers_result <- perform_basic_quality_checks(customers_data, "customers")
  customers_business <- perform_business_logic_checks(customers_data, "customers")
  all_results$customers <- list(
    basic = customers_result,
    business = customers_business,
    total_issues = customers_result$issues + customers_business
  )
  total_issues <- total_issues + customers_result$issues + customers_business
}

# 3. Timeseries Quality Assessment
if (!is.null(timeseries_data)) {
  timeseries_result <- perform_basic_quality_checks(timeseries_data, "timeseries")
  timeseries_business <- perform_business_logic_checks(timeseries_data, "timeseries")
  all_results$timeseries <- list(
    basic = timeseries_result,
    business = timeseries_business,
    total_issues = timeseries_result$issues + timeseries_business
  )
  total_issues <- total_issues + timeseries_result$issues + timeseries_business
}

# REFERENTIAL INTEGRITY CHECKS
cat("REFERENTIAL INTEGRITY ASSESSMENT\n")
cat("=================================\n")

ref_issues <- 0

if (!is.null(contracts_data) && !is.null(customers_data) && !is.null(timeseries_data)) {
  
  if ("contractid" %in% names(contracts_data) && 
      "contractid" %in% names(customers_data) && 
      "contractid" %in% names(timeseries_data)) {
    
    # Get unique IDs from each table
    contracts_ids <- unique(contracts_data$contractid[!is.na(contracts_data$contractid)])
    customers_ids <- unique(customers_data$contractid[!is.na(customers_data$contractid)])
    timeseries_ids <- unique(timeseries_data$contractid[!is.na(timeseries_data$contractid)])
    
    cat("UNIQUE CONTRACT IDs BY TABLE:\n")
    cat("- Contracts table:", length(contracts_ids), "unique IDs\n")
    cat("- Customers table:", length(customers_ids), "unique IDs\n")
    cat("- Timeseries table:", length(timeseries_ids), "unique IDs\n\n")
    
    # Relationship analysis
    cat("RELATIONSHIP ANALYSIS:\n")
    
    # Contracts with customer data
    contracts_with_customers <- sum(contracts_ids %in% customers_ids)
    contracts_missing_customers <- length(contracts_ids) - contracts_with_customers
    
    if (contracts_missing_customers > 0) {
      cat("  ❌ MISSING CUSTOMERS: Found", contracts_missing_customers, 
          "contracts without customer data\n")
      ref_issues <- ref_issues + contracts_missing_customers
    } else {
      cat("  ✅ COMPLETE CUSTOMERS: All contracts have customer data\n")
    }
    
    cat("    Coverage:", contracts_with_customers, "of", length(contracts_ids),
        "(", round(100 * contracts_with_customers / length(contracts_ids), 1), "%)\n")
    
    # Contracts with timeseries data
    contracts_with_timeseries <- sum(contracts_ids %in% timeseries_ids)
    contracts_missing_timeseries <- length(contracts_ids) - contracts_with_timeseries
    
    if (contracts_missing_timeseries > 0) {
      cat("  ❌ MISSING TIMESERIES: Found", contracts_missing_timeseries, 
          "contracts without timeseries data\n")
      ref_issues <- ref_issues + contracts_missing_timeseries
    } else {
      cat("  ✅ COMPLETE TIMESERIES: All contracts have timeseries data\n")
    }
    
    cat("    Coverage:", contracts_with_timeseries, "of", length(contracts_ids),
        "(", round(100 * contracts_with_timeseries / length(contracts_ids), 1), "%)\n")
    
    # Orphaned records
    cat("\nORPHANED RECORDS:\n")
    
    orphaned_customers <- customers_ids[!customers_ids %in% contracts_ids]
    if (length(orphaned_customers) > 0) {
      cat("  ⚠️ ORPHANED CUSTOMERS:", length(orphaned_customers), 
          "customer records without corresponding contracts\n")
      cat("    Examples:", paste(head(orphaned_customers, 3), collapse = ", "), "\n")
    } else {
      cat("  ✅ NO ORPHANED CUSTOMERS: All customer records have contracts\n")
    }
    
    orphaned_timeseries <- timeseries_ids[!timeseries_ids %in% contracts_ids]
    if (length(orphaned_timeseries) > 0) {
      cat("  ⚠️ ORPHANED TIMESERIES:", length(orphaned_timeseries), 
          "timeseries records without corresponding contracts\n")
      cat("    Examples:", paste(head(orphaned_timeseries, 3), collapse = ", "), "\n")
    } else {
      cat("  ✅ NO ORPHANED TIMESERIES: All timeseries records have contracts\n")
    }
    
  } else {
    cat("❌ Cannot perform referential integrity checks - missing contractid columns\n")
    ref_issues <- 1
  }
  
} else {
  cat("❌ Cannot perform referential integrity checks - missing data tables\n")
  ref_issues <- 1
}

cat("\nREFERENTIAL INTEGRITY ISSUES FOUND:", ref_issues, "\n\n")
total_issues <- total_issues + ref_issues

# FINAL SUMMARY AND RECOMMENDATIONS
cat("==================================================\n")
cat("FINAL DATA QUALITY ASSESSMENT SUMMARY\n") 
cat("==================================================\n")

cat("OVERALL RESULTS:\n")
if (total_issues == 0) {
  cat("🎉 OUTSTANDING! No data quality issues detected.\n")
  cat("✅ Your data is ready for ETL processing.\n")
} else {
  cat("⚠️ ATTENTION REQUIRED: Found", total_issues, "data quality issues.\n")
  cat("❌ Recommend fixing issues before running ETL pipeline.\n")
}

cat("\nISSUES BY CATEGORY:\n")
if (!is.null(all_results$contracts)) {
  cat("- Contracts table:", all_results$contracts$total_issues, "issues\n")
}
if (!is.null(all_results$customers)) {
  cat("- Customers table:", all_results$customers$total_issues, "issues\n")
}
if (!is.null(all_results$timeseries)) {
  cat("- Timeseries table:", all_results$timeseries$total_issues, "issues\n")
}
cat("- Referential integrity:", ref_issues, "issues\n")

cat("\nRECOMMENDED ACTIONS:\n")
cat("1. 🔍 Review detailed findings above\n")
cat("2. 🛠️  Fix missing values in critical columns\n")
cat("3. 🗂️  Resolve duplicate contract IDs\n")
cat("4. 📅 Correct invalid dates and formats\n")
cat("5. 🔗 Address referential integrity issues\n")
cat("6. ✅ Re-run quality checks after fixes\n")

cat("\nNEXT STEPS:\n")
if (total_issues == 0) {
  cat("➡️  Proceed with ETL pipeline: source('main.R')\n")
  cat("➡️  Generate final report: quarto::quarto_render('pipeline.qmd')\n")
} else {
  cat("➡️  Fix data quality issues first\n")
  cat("➡️  Re-run quality assessment\n")
  cat("➡️  Then proceed with ETL pipeline\n")
}

# Save summary to file
summary_data <- data.frame(
  table_name = c("contracts", "customers", "timeseries", "referential"),
  issues_found = c(
    ifelse(is.null(all_results$contracts), 0, all_results$contracts$total_issues),
    ifelse(is.null(all_results$customers), 0, all_results$customers$total_issues),
    ifelse(is.null(all_results$timeseries), 0, all_results$timeseries$total_issues),
    ref_issues
  ),
  assessment_time = Sys.time()
)

write.csv(summary_data, "reports/quality_assessment_summary.csv", row.names = FALSE)

cat("\n📊 Summary saved to: reports/quality_assessment_summary.csv\n")
cat("📅 Assessment completed:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("==================================================\n")