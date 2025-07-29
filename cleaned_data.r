# =============================================================================
# COMPLETE DATA LOADING AND CLEANING EXAMPLE
# =============================================================================
# Purpose: Load insurance data from multiple sources and clean it comprehensively
# Author: Data Quality Assessment System
# Date: 2025-07-29

# =============================================================================
# SETUP AND INITIALIZATION
# =============================================================================

cat("=== INSURANCE DATA LOADING AND CLEANING PIPELINE ===\n")
cat("Started:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("R Version:", R.version.string, "\n\n")

# Create logging setup
if (!dir.exists("reports")) {
  dir.create("reports", recursive = TRUE)
}

log_file <- paste0("reports/data_cleaning_log_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".txt")
sink(log_file, append = TRUE, split = TRUE)

cat("=====================================\n")
cat("DATA LOADING AND CLEANING LOG\n") 
cat("=====================================\n")
cat("Started:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("Log file:", log_file, "\n")
cat("=====================================\n\n")

# Prevent automatic date parsing issues
options(stringsAsFactors = FALSE)
Sys.setenv(TZ = "UTC")

# Load required packages
required_packages <- c("DBI", "RSQLite", "readxl", "jsonlite", "dplyr")

for (pkg in required_packages) {
  if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
    cat("Installing", pkg, "...\n")
    install.packages(pkg)
    library(pkg, character.only = TRUE)
  }
}

cat("✓ All required packages loaded\n\n")

# =============================================================================
# DATA LOADING FUNCTIONS
# =============================================================================

load_insurance_data <- function() {
  cat("LOADING DATA FOR CLEANING PIPELINE\n")
  cat("==================================\n")
  
  data_sources <- list()
  
  # 1. Load SQLite data (Contracts)
  tryCatch({
    con <- dbConnect(RSQLite::SQLite(), "data/raw/contracts.sqlite")
    tables <- dbListTables(con)
    
    if (length(tables) > 0) {
      contracts_raw <- dbGetQuery(con, paste("SELECT * FROM", tables[1]))
      data_sources$contracts <- contracts_raw
      cat("✓ Loaded contracts from SQLite:", nrow(contracts_raw), "rows,", ncol(contracts_raw), "columns\n")
      cat("  Columns:", paste(names(contracts_raw), collapse = ", "), "\n")
    } else {
      stop("No tables found in SQLite database")
    }
    
    dbDisconnect(con)
  }, error = function(e) {
    cat("❌ Error loading SQLite data:", e$message, "\n")
    data_sources$contracts <- NULL
  })
  
  # 2. Load Excel data (Customers)
  tryCatch({
    customers_raw <- read_excel("data/raw/customers.xlsx")
    data_sources$customers <- customers_raw
    cat("✓ Loaded customers from Excel:", nrow(customers_raw), "rows,", ncol(customers_raw), "columns\n")
    cat("  Columns:", paste(names(customers_raw), collapse = ", "), "\n")
  }, error = function(e) {
    cat("❌ Error loading Excel data:", e$message, "\n")
    data_sources$customers <- NULL
  })
  
  # 3. Load JSON data (Timeseries)
  tryCatch({
    json_raw <- fromJSON("data/raw/contracts_timeseries.json")
    
    # Handle different JSON structures
    if (is.list(json_raw) && !is.data.frame(json_raw)) {
      # Array of objects - convert to dataframe
      timeseries_raw <- do.call(rbind, lapply(json_raw, function(x) {
        if (is.list(x)) {
          data.frame(x, stringsAsFactors = FALSE)
        } else {
          data.frame(value = x, stringsAsFactors = FALSE)
        }
      }))
    } else {
      timeseries_raw <- as.data.frame(json_raw)
    }
    
    data_sources$timeseries <- timeseries_raw
    cat("✓ Loaded timeseries from JSON:", nrow(timeseries_raw), "rows,", ncol(timeseries_raw), "columns\n")
    cat("  Columns:", paste(names(timeseries_raw), collapse = ", "), "\n")
  }, error = function(e) {
    cat("❌ Error loading JSON data:", e$message, "\n")
    data_sources$timeseries <- NULL
  })
  
  cat("\n")
  return(data_sources)
}

# =============================================================================
# COMPREHENSIVE DATA CLEANING FUNCTION
# =============================================================================

clean_data <- function(data, table_type = "unknown", verbose = TRUE) {
  
  if (verbose) {
    cat("=== CLEANING DATA FOR", toupper(table_type), "TABLE ===\n")
    cat("Original dimensions:", nrow(data), "rows x", ncol(data), "columns\n")
  }
  
  # Create cleaning log
  cleaning_log <- list(
    table_type = table_type,
    original_rows = nrow(data),
    original_cols = ncol(data),
    actions_taken = character(),
    dropped_rows = 0,
    imputed_values = 0,
    converted_columns = character(),
    renamed_columns = character()
  )
  
  # Make a copy to work with
  cleaned_data <- data
  
  # ==========================================================================
  # 1. COLUMN RENAMING AND STANDARDIZATION
  # ==========================================================================
  
  if (verbose) cat("\n1. COLUMN RENAMING AND STANDARDIZATION\n")
  
  # Store original column names
  original_names <- names(cleaned_data)
  
  # Standardize column names (lowercase, remove spaces, etc.)
  names(cleaned_data) <- tolower(trimws(names(cleaned_data)))
  names(cleaned_data) <- gsub("[^a-z0-9_]", "_", names(cleaned_data))
  names(cleaned_data) <- gsub("_{2,}", "_", names(cleaned_data))  # Remove multiple underscores
  names(cleaned_data) <- gsub("_$", "", names(cleaned_data))     # Remove trailing underscores
  
  # Table-specific column renaming
  if (table_type == "contracts") {
    # Standardize contract table columns
    column_mapping <- c(
      "contract_id" = "contractid",
      "contractid" = "contractid",
      "customer_id" = "customerid", 
      "customerid" = "customerid",
      "mandant_id" = "mandantid",
      "mandantid" = "mandantid"
    )
    
    for (old_name in names(column_mapping)) {
      if (old_name %in% names(cleaned_data)) {
        names(cleaned_data)[names(cleaned_data) == old_name] <- column_mapping[old_name]
        cleaning_log$renamed_columns <- c(cleaning_log$renamed_columns, 
                                        paste(old_name, "->", column_mapping[old_name]))
      }
    }
    
  } else if (table_type == "customers") {
    # Standardize customer table columns
    column_mapping <- c(
      "customer_id" = "customerid",
      "customerid" = "customerid", 
      "birth_date" = "birthdate",
      "birthdate" = "birthdate",
      "date_of_birth" = "birthdate",
      "dob" = "birthdate"
    )
    
    for (old_name in names(column_mapping)) {
      if (old_name %in% names(cleaned_data)) {
        names(cleaned_data)[names(cleaned_data) == old_name] <- column_mapping[old_name]
        cleaning_log$renamed_columns <- c(cleaning_log$renamed_columns, 
                                        paste(old_name, "->", column_mapping[old_name]))
      }
    }
    
  } else if (table_type == "timeseries") {
    # Standardize timeseries table columns
    column_mapping <- c(
      "contract_id" = "contractid",
      "contractid" = "contractid",
      "rueckkaufswert" = "surrender_value",
      "rückkaufswert" = "surrender_value",
      "surrender_value" = "surrender_value",
      "value" = "surrender_value",
      "date" = "date",
      "datum" = "date"
    )
    
    for (old_name in names(column_mapping)) {
      if (old_name %in% names(cleaned_data)) {
        names(cleaned_data)[names(cleaned_data) == old_name] <- column_mapping[old_name]
        cleaning_log$renamed_columns <- c(cleaning_log$renamed_columns, 
                                        paste(old_name, "->", column_mapping[old_name]))
      }
    }
  }
  
  if (verbose && length(cleaning_log$renamed_columns) > 0) {
    cat("✓ Renamed columns:", paste(cleaning_log$renamed_columns, collapse = ", "), "\n")
  }
  
  # ==========================================================================
  # 2. DATA TYPE CONVERSION
  # ==========================================================================
  
  if (verbose) cat("\n2. DATA TYPE CONVERSION\n")
  
  # Contract ID cleaning and validation
  if ("contractid" %in% names(cleaned_data)) {
    if (verbose) cat("Processing Contract IDs...\n")
    
    original_contractids <- as.character(cleaned_data$contractid)
    
    # Remove NA values for pattern matching
    non_na_original <- original_contractids[!is.na(original_contractids)]
    
    # Clean contract IDs - remove invalid characters and standardize format
    cleaned_contractids <- gsub("[^V0-9]", "", original_contractids)
    
    # Fix common formatting issues
    cleaned_contractids <- gsub("^V0*", "V", cleaned_contractids)  # Remove leading zeros after V
    cleaned_contractids <- ifelse(nchar(cleaned_contractids) < 2, NA, cleaned_contractids)
    
    # Count changes
    invalid_before <- sum(!grepl("^V[0-9]+$", non_na_original))
    non_na_cleaned <- cleaned_contractids[!is.na(cleaned_contractids)]
    invalid_after <- sum(!grepl("^V[0-9]+$", non_na_cleaned))
    
    cleaned_data$contractid <- cleaned_contractids
    
    if (verbose) {
      cat("  ✓ Contract IDs: Fixed", invalid_before - invalid_after, "formatting issues\n")
    }
    
    cleaning_log$converted_columns <- c(cleaning_log$converted_columns, "contractid")
    cleaning_log$actions_taken <- c(cleaning_log$actions_taken, 
                                  paste("Fixed", invalid_before - invalid_after, "contract ID formats"))
  }
  
  # Customer ID cleaning
  if ("customerid" %in% names(cleaned_data)) {
    if (verbose) cat("Processing Customer IDs...\n")
    
    original_customerids <- as.character(cleaned_data$customerid)
    
    # Remove NA values for pattern matching
    non_na_original <- original_customerids[!is.na(original_customerids)]
    
    # Clean customer IDs
    cleaned_customerids <- gsub("[^K0-9]", "", original_customerids)
    cleaned_customerids <- gsub("^K0*", "K", cleaned_customerids)
    cleaned_customerids <- ifelse(nchar(cleaned_customerids) < 2, NA, cleaned_customerids)
    
    invalid_before <- sum(!grepl("^K[0-9]+$", non_na_original))
    non_na_cleaned <- cleaned_customerids[!is.na(cleaned_customerids)]
    invalid_after <- sum(!grepl("^K[0-9]+$", non_na_cleaned))
    
    cleaned_data$customerid <- cleaned_customerids
    
    if (verbose) {
      cat("  ✓ Customer IDs: Fixed", invalid_before - invalid_after, "formatting issues\n")
    }
    
    cleaning_log$converted_columns <- c(cleaning_log$converted_columns, "customerid")
    cleaning_log$actions_taken <- c(cleaning_log$actions_taken, 
                                  paste("Fixed", invalid_before - invalid_after, "customer ID formats"))
  }
  
  # Mandant ID cleaning
  if ("mandantid" %in% names(cleaned_data)) {
    if (verbose) cat("Processing Mandant IDs...\n")
    
    original_mandantids <- as.character(cleaned_data$mandantid)
    
    # Clean mandant IDs - keep only LV1 or LV2
    cleaned_mandantids <- toupper(gsub("[^LV12]", "", original_mandantids))
    
    # Fix common issues
    cleaned_mandantids <- ifelse(grepl("LV.*1", cleaned_mandantids), "LV1", cleaned_mandantids)
    cleaned_mandantids <- ifelse(grepl("LV.*2", cleaned_mandantids), "LV2", cleaned_mandantids)
    cleaned_mandantids <- ifelse(cleaned_mandantids %in% c("LV1", "LV2"), cleaned_mandantids, NA)
    
    # Count invalid entries before and after
    non_na_original <- original_mandantids[!is.na(original_mandantids)]
    invalid_before <- sum(!non_na_original %in% c("LV1", "LV2"))
    non_na_cleaned <- cleaned_mandantids[!is.na(cleaned_mandantids)]
    invalid_after <- sum(!non_na_cleaned %in% c("LV1", "LV2"))
    
    cleaned_data$mandantid <- cleaned_mandantids
    
    if (verbose) {
      cat("  ✓ Mandant IDs: Fixed", invalid_before - invalid_after, "formatting issues\n")
    }
    
    cleaning_log$converted_columns <- c(cleaning_log$converted_columns, "mandantid")
    cleaning_log$actions_taken <- c(cleaning_log$actions_taken, 
                                  paste("Fixed", invalid_before - invalid_after, "mandant ID formats"))
  }
  
  # Date conversion
  if ("birthdate" %in% names(cleaned_data)) {
    if (verbose) cat("Processing Birth Dates...\n")
    
    tryCatch({
      original_dates <- cleaned_data$birthdate
      
      # Try multiple date formats
      cleaned_dates <- as.Date(original_dates)
      
      # If that fails, try other common formats
      if (sum(is.na(cleaned_dates)) > sum(is.na(original_dates))) {
        cleaned_dates <- as.Date(original_dates, format = "%d.%m.%Y")
      }
      if (sum(is.na(cleaned_dates)) > sum(is.na(original_dates))) {
        cleaned_dates <- as.Date(original_dates, format = "%d/%m/%Y")
      }
      
      # Validate dates (remove future dates and very old dates)
      today <- Sys.Date()
      min_date <- as.Date("1900-01-01")
      
      invalid_dates <- cleaned_dates > today | cleaned_dates < min_date
      cleaned_dates[invalid_dates & !is.na(invalid_dates)] <- NA
      
      cleaned_data$birthdate <- cleaned_dates
      
      if (verbose) {
        cat("  ✓ Birth Dates: Converted to Date format, removed", 
            sum(invalid_dates, na.rm = TRUE), "invalid dates\n")
      }
      
      cleaning_log$converted_columns <- c(cleaning_log$converted_columns, "birthdate")
      cleaning_log$actions_taken <- c(cleaning_log$actions_taken, 
                                    paste("Converted birthdate to Date, removed", 
                                          sum(invalid_dates, na.rm = TRUE), "invalid dates"))
      
    }, error = function(e) {
      if (verbose) cat("  ⚠️ Birth Date conversion failed:", e$message, "\n")
    })
  }
  
  # Date conversion for timeseries
  if ("date" %in% names(cleaned_data)) {
    if (verbose) cat("Processing Time Series Dates...\n")
    
    tryCatch({
      original_dates <- cleaned_data$date
      cleaned_dates <- as.Date(original_dates)
      
      # Validate dates
      today <- Sys.Date()
      min_date <- as.Date("1990-01-01")  # Insurance data shouldn't be too old
      
      invalid_dates <- cleaned_dates > today | cleaned_dates < min_date
      cleaned_dates[invalid_dates & !is.na(invalid_dates)] <- NA
      
      cleaned_data$date <- cleaned_dates
      
      if (verbose) {
        cat("  ✓ Time Series Dates: Converted to Date format, removed", 
            sum(invalid_dates, na.rm = TRUE), "invalid dates\n")
      }
      
      cleaning_log$converted_columns <- c(cleaning_log$converted_columns, "date")
      
    }, error = function(e) {
      if (verbose) cat("  ⚠️ Date conversion failed:", e$message, "\n")
    })
  }
  
  # Numeric conversion for surrender values
  if ("surrender_value" %in% names(cleaned_data)) {
    if (verbose) cat("Processing Surrender Values...\n")
    
    tryCatch({
      original_values <- cleaned_data$surrender_value
      
      # Clean numeric values - remove non-numeric characters except decimal points
      cleaned_values <- gsub("[^0-9.-]", "", as.character(original_values))
      cleaned_values <- as.numeric(cleaned_values)
      
      # Remove negative values (not logical for surrender values)
      negative_count <- sum(cleaned_values < 0, na.rm = TRUE)
      cleaned_values[cleaned_values < 0] <- NA
      
      cleaned_data$surrender_value <- cleaned_values
      
      if (verbose) {
        cat("  ✓ Surrender Values: Converted to numeric, removed", negative_count, "negative values\n")
      }
      
      cleaning_log$converted_columns <- c(cleaning_log$converted_columns, "surrender_value")
      cleaning_log$actions_taken <- c(cleaning_log$actions_taken, 
                                    paste("Converted surrender_value to numeric, removed", 
                                          negative_count, "negative values"))
      
    }, error = function(e) {
      if (verbose) cat("  ⚠️ Surrender value conversion failed:", e$message, "\n")
    })
  }
  
  # ==========================================================================
  # 3. HANDLING MISSING VALUES AND PROBLEMATIC ENTRIES
  # ==========================================================================
  
  if (verbose) cat("\n3. HANDLING MISSING VALUES AND PROBLEMATIC ENTRIES\n")
  
  initial_rows <- nrow(cleaned_data)
  
  if (table_type == "contracts") {
    # Contract table: Drop rows without contract ID (critical)
    before_rows <- nrow(cleaned_data)
    cleaned_data <- cleaned_data[!is.na(cleaned_data$contractid), ]
    dropped_contract_rows <- before_rows - nrow(cleaned_data)
    
    if (dropped_contract_rows > 0) {
      if (verbose) cat("  ❌ DROPPED", dropped_contract_rows, "rows without Contract ID (critical field)\n")
      cleaning_log$dropped_rows <- cleaning_log$dropped_rows + dropped_contract_rows
      cleaning_log$actions_taken <- c(cleaning_log$actions_taken, 
                                    paste("Dropped", dropped_contract_rows, "rows without Contract ID"))
    }
    
    # Impute missing Mandant IDs with most common value
    if ("mandantid" %in% names(cleaned_data)) {
      missing_mandant <- is.na(cleaned_data$mandantid)
      if (sum(missing_mandant) > 0) {
        # Find most common mandant
        mandant_table <- table(cleaned_data$mandantid[!missing_mandant])
        if (length(mandant_table) > 0) {
          most_common_mandant <- names(mandant_table)[which.max(mandant_table)]
          
          cleaned_data$mandantid[missing_mandant] <- most_common_mandant
          
          if (verbose) {
            cat("  🔧 IMPUTED", sum(missing_mandant), "missing Mandant IDs with '", 
                most_common_mandant, "' (most common value)\n")
          }
          
          cleaning_log$imputed_values <- cleaning_log$imputed_values + sum(missing_mandant)
          cleaning_log$actions_taken <- c(cleaning_log$actions_taken, 
                                        paste("Imputed", sum(missing_mandant), 
                                              "missing Mandant IDs with", most_common_mandant))
        }
      }
    }
    
  } else if (table_type == "customers") {
    # Customer table: Drop rows without customer ID
    before_rows <- nrow(cleaned_data)
    cleaned_data <- cleaned_data[!is.na(cleaned_data$customerid), ]
    dropped_customer_rows <- before_rows - nrow(cleaned_data)
    
    if (dropped_customer_rows > 0) {
      if (verbose) cat("  ❌ DROPPED", dropped_customer_rows, "rows without Customer ID (critical field)\n")
      cleaning_log$dropped_rows <- cleaning_log$dropped_rows + dropped_customer_rows
      cleaning_log$actions_taken <- c(cleaning_log$actions_taken, 
                                    paste("Dropped", dropped_customer_rows, "rows without Customer ID"))
    }
    
    # Keep rows with missing birth dates (not always critical)
    if ("birthdate" %in% names(cleaned_data)) {
      missing_birthdate <- sum(is.na(cleaned_data$birthdate))
      if (missing_birthdate > 0 && verbose) {
        cat("  ⚠️ KEPT", missing_birthdate, "rows with missing birth dates (may be legitimate)\n")
      }
    }
    
  } else if (table_type == "timeseries") {
    # Timeseries: Drop rows without contract ID or date (both critical for time series)
    before_rows <- nrow(cleaned_data)
    cleaned_data <- cleaned_data[!is.na(cleaned_data$contractid) & !is.na(cleaned_data$date), ]
    dropped_timeseries_rows <- before_rows - nrow(cleaned_data)
    
    if (dropped_timeseries_rows > 0) {
      if (verbose) cat("  ❌ DROPPED", dropped_timeseries_rows, "rows without Contract ID or Date (critical for timeseries)\n")
      cleaning_log$dropped_rows <- cleaning_log$dropped_rows + dropped_timeseries_rows
      cleaning_log$actions_taken <- c(cleaning_log$actions_taken, 
                                    paste("Dropped", dropped_timeseries_rows, 
                                          "rows without Contract ID or Date"))
    }
    
    # Keep rows with missing surrender values but flag them
    if ("surrender_value" %in% names(cleaned_data)) {
      missing_values <- sum(is.na(cleaned_data$surrender_value))
      if (missing_values > 0 && verbose) {
        cat("  ⚠️ KEPT", missing_values, "rows with missing surrender values (may indicate data gaps)\n")
      }
    }
  }
  
  # ==========================================================================
  # 4. REMOVE COMPLETE DUPLICATES
  # ==========================================================================
  
  if (verbose) cat("\n4. REMOVING COMPLETE DUPLICATES\n")
  
  before_dedup <- nrow(cleaned_data)
  cleaned_data <- unique(cleaned_data)
  duplicates_removed <- before_dedup - nrow(cleaned_data)
  
  if (duplicates_removed > 0) {
    if (verbose) cat("  ✓ REMOVED", duplicates_removed, "completely duplicate rows\n")
    cleaning_log$dropped_rows <- cleaning_log$dropped_rows + duplicates_removed
    cleaning_log$actions_taken <- c(cleaning_log$actions_taken, 
                                  paste("Removed", duplicates_removed, "duplicate rows"))
  } else {
    if (verbose) cat("  ✓ No duplicate rows found\n")
  }
  
  # ==========================================================================
  # 5. FINAL SUMMARY
  # ==========================================================================
  
  cleaning_log$final_rows <- nrow(cleaned_data)
  cleaning_log$final_cols <- ncol(cleaned_data)
  cleaning_log$rows_retained_percent <- round(100 * cleaning_log$final_rows / cleaning_log$original_rows, 1)
  
  if (verbose) {
    cat("\n=== CLEANING SUMMARY FOR", toupper(table_type), "===\n")
    cat("Original dimensions:", cleaning_log$original_rows, "rows x", cleaning_log$original_cols, "columns\n")
    cat("Final dimensions:", cleaning_log$final_rows, "rows x", cleaning_log$final_cols, "columns\n")
    cat("Rows retained:", cleaning_log$rows_retained_percent, "%\n")
    cat("Total rows dropped:", cleaning_log$dropped_rows, "\n")
    cat("Total values imputed:", cleaning_log$imputed_values, "\n")
    cat("Columns converted:", length(cleaning_log$converted_columns), "\n")
    
    if (length(cleaning_log$actions_taken) > 0) {
      cat("\nActions taken:\n")
      for (action in cleaning_log$actions_taken) {
        cat("  -", action, "\n")
      }
    }
    cat("========================================\n\n")
  }
  
  # Return cleaned data and log
  return(list(
    data = cleaned_data,
    log = cleaning_log
  ))
}

# =============================================================================
# HELPER FUNCTION: CLEAN ALL TABLES
# =============================================================================

clean_all_tables <- function(contracts_data = NULL, customers_data = NULL, 
                            timeseries_data = NULL, verbose = TRUE) {
  
  if (verbose) {
    cat("=======================================================\n")
    cat("COMPREHENSIVE DATA CLEANING FOR ALL TABLES\n")
    cat("=======================================================\n")
  }
  
  results <- list()
  all_logs <- list()
  
  # Clean contracts data
  if (!is.null(contracts_data)) {
    if (verbose) cat("Cleaning CONTRACTS table...\n")
    contracts_result <- clean_data(contracts_data, "contracts", verbose)
    results$contracts <- contracts_result$data
    all_logs$contracts <- contracts_result$log
  }
  
  # Clean customers data
  if (!is.null(customers_data)) {
    if (verbose) cat("Cleaning CUSTOMERS table...\n")
    customers_result <- clean_data(customers_data, "customers", verbose)
    results$customers <- customers_result$data
    all_logs$customers <- customers_result$log
  }
  
  # Clean timeseries data
  if (!is.null(timeseries_data)) {
    if (verbose) cat("Cleaning TIMESERIES table...\n")
    timeseries_result <- clean_data(timeseries_data, "timeseries", verbose)
    results$timeseries <- timeseries_result$data
    all_logs$timeseries <- timeseries_result$log
  }
  
  if (verbose) {
    cat("=======================================================\n")
    cat("ALL TABLE CLEANING COMPLETED\n")
    cat("=======================================================\n")
  }
  
  return(list(
    cleaned_data = results,
    cleaning_logs = all_logs
  ))
}

# =============================================================================
# SAVE CLEANED DATA FUNCTION
# =============================================================================

save_cleaned_data <- function(cleaned_data_list, cleaning_logs_list) {
  cat("SAVING CLEANED DATA\n")
  cat("===================\n")
  
  # Create cleaned data directory
  if (!dir.exists("data/cleaned")) {
    dir.create("data/cleaned", recursive = TRUE)
    cat("✓ Created data/cleaned directory\n")
  }
  
  # Save each cleaned dataset
  for (table_name in names(cleaned_data_list)) {
    if (!is.null(cleaned_data_list[[table_name]])) {
      filename <- paste0("data/cleaned/", table_name, "_cleaned.csv")
      write.csv(cleaned_data_list[[table_name]], filename, row.names = FALSE)
      cat("✓ Saved", table_name, "to:", filename, "\n")
    }
  }
  
  # Save cleaning logs
  log_filename <- paste0("reports/cleaning_logs_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".json")
  write(jsonlite::toJSON(cleaning_logs_list, pretty = TRUE), log_filename)
  cat("✓ Saved cleaning logs to:", log_filename, "\n")
  
  cat("===================\n\n")
}

# =============================================================================
# MAIN EXECUTION
# =============================================================================

cat("STARTING COMPLETE DATA LOADING AND CLEANING PIPELINE\n")
cat("====================================================\n\n")

# Step 1: Load raw data
cat("STEP 1: LOADING RAW DATA\n")
raw_data <- load_insurance_data()

# Step 2: Clean all data
cat("STEP 2: CLEANING ALL DATA\n")
cleaning_results <- clean_all_tables(
  contracts_data = raw_data$contracts,
  customers_data = raw_data$customers,
  timeseries_data = raw_data$timeseries,
  verbose = TRUE
)

# Extract cleaned data and logs
cleaned_data <- cleaning_results$cleaned_data
cleaning_logs <- cleaning_results$cleaning_logs

# Step 3: Save cleaned data
cat("STEP 3: SAVING CLEANED DATA\n")
save_cleaned_data(cleaned_data, cleaning_logs)

# Step 4: Final summary
cat("STEP 4: FINAL PIPELINE SUMMARY\n")
cat("==============================\n")

total_original_rows <- 0
total_final_rows <- 0

for (table_name in names(cleaning_logs)) {
  log <- cleaning_logs[[table_name]]
  cat("📊", toupper(table_name), "TABLE:\n")
  cat("  - Original:", log$original_rows, "rows\n")
  cat("  - Cleaned:", log$final_rows, "rows\n")
  cat("  - Retention:", log$rows_retained_percent, "%\n")
  cat("  - Dropped:", log$dropped_rows, "rows\n")
  cat("  - Imputed:", log$imputed_values, "values\n")
  
  total_original_rows <- total_original_rows + log$original_rows
  total_final_rows <- total_final_rows + log$final_rows
}

cat("\n🎯 OVERALL PIPELINE RESULTS:\n")
cat("  - Total original rows:", total_original_rows, "\n")
cat("  - Total cleaned rows:", total_final_rows, "\n")
cat("  - Overall retention:", round(100 * total_final_rows / total_original_rows, 1), "%\n")