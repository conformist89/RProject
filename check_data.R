# Insurance Data Checker Script
# File: check_data.R
# 
# Simple script to examine your insurance data files
# Usage: source("check_data.R")

cat("=== INSURANCE DATA CHECKER ===\n")
cat("Started:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")

# Load required packages
tryCatch({
  library(DBI)
  library(RSQLite)
  library(readxl)
  library(jsonlite)
  cat("✓ All packages loaded successfully\n\n")
}, error = function(e) {
  cat("❌ Error loading packages:", e$message, "\n")
  cat("Please install missing packages with: install.packages(c('DBI', 'RSQLite', 'readxl', 'jsonlite'))\n")
  stop()
})

log_file <- paste0("reports/primary_data_quality_log_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".txt")
sink(log_file, append = TRUE, split = TRUE)

# CHECK FILE 1: contracts.sqlite
cat("1. CHECKING contracts.sqlite\n")
cat("============================\n")

if (file.exists("data/raw/contracts.sqlite")) {
  cat("✓ File found\n")
  cat("File size:", file.size("data/raw/contracts.sqlite"), "bytes\n")
  
  tryCatch({
    con <- dbConnect(RSQLite::SQLite(), "data/raw/contracts.sqlite")
    tables <- dbListTables(con)
    
    if (length(tables) > 0) {
      cat("Tables found:", paste(tables, collapse = ", "), "\n\n")
      
      for (table_name in tables) {
        cat("--- TABLE:", table_name, "---\n")
        
        # Get table structure
        structure <- dbGetQuery(con, paste("PRAGMA table_info(", table_name, ")"))
        cat("Columns:", paste(structure$name, collapse = ", "), "\n")
        
        # Get row count
        count <- dbGetQuery(con, paste("SELECT COUNT(*) as count FROM", table_name))
        cat("Total rows:", count$count, "\n")
        
        # Show first 3 rows
        if (count$count > 0) {
          sample_data <- dbGetQuery(con, paste("SELECT * FROM", table_name, "LIMIT 3"))
          cat("First 3 rows:\n")
          print(sample_data)
        }
        cat("\n")
      }
    } else {
      cat("❌ No tables found in SQLite file\n")
    }
    
    dbDisconnect(con)
    
  }, error = function(e) {
    cat("❌ Error reading SQLite file:", e$message, "\n")
  })
  
} else {
  cat("❌ File not found: data/raw/contracts.sqlite\n")
}

cat("\n")

# CHECK FILE 2: customers.xlsx
cat("2. CHECKING customers.xlsx\n")
cat("==========================\n")

if (file.exists("data/raw/customers.xlsx")) {
  cat("✓ File found\n")
  cat("File size:", file.size("data/raw/customers.xlsx"), "bytes\n")
  
  tryCatch({
    sheets <- excel_sheets("data/raw/customers.xlsx")
    cat("Sheets found:", paste(sheets, collapse = ", "), "\n\n")
    
    for (sheet_name in sheets) {
      cat("--- SHEET:", sheet_name, "---\n")
      
      # Read first few rows to check structure
      sample_data <- read_excel("data/raw/customers.xlsx", sheet = sheet_name, n_max = 3)
      cat("Columns:", paste(names(sample_data), collapse = ", "), "\n")
      cat("Total columns:", ncol(sample_data), "\n")
      
      # Try to get total row count
      tryCatch({
        full_data <- read_excel("data/raw/customers.xlsx", sheet = sheet_name)
        cat("Total rows:", nrow(full_data), "\n")
      }, error = function(e) {
        cat("Total rows: Could not determine\n")
      })
      
      cat("First 3 rows:\n")
      print(sample_data)
      cat("\n")
    }
    
  }, error = function(e) {
    cat("❌ Error reading Excel file:", e$message, "\n")
  })
  
} else {
  cat("❌ File not found: data/raw/customers.xlsx\n")
}

cat("\n")

# CHECK FILE 3: contracts_timeseries.json
cat("3. CHECKING contracts_timeseries.json\n")
cat("=====================================\n")

if (file.exists("data/raw/contracts_timeseries.json")) {
  cat("✓ File found\n")
  cat("File size:", file.size("data/raw/contracts_timeseries.json"), "bytes\n")

  tryCatch({
    json_data <- fromJSON("data/raw/contracts_timeseries.json")
    
    cat("JSON type:", class(json_data), "\n")
    
    if (is.data.frame(json_data)) {
      cat("Rows:", nrow(json_data), "\n")
      cat("Columns:", paste(names(json_data), collapse = ", "), "\n")
      cat("First 3 rows:\n")
      print(head(json_data, 3))
      
    } else if (is.list(json_data)) {
      cat("List items:", length(json_data), "\n")
      
      if (length(json_data) > 0) {
        first_item <- json_data[[1]]
        if (is.list(first_item)) {
          cat("Fields in first item:", paste(names(first_item), collapse = ", "), "\n")
        }
        cat("First 3 items:\n")
        print(str(json_data[[1]], max.level = 1))
      
      }
      
    } else {
      cat("Unknown JSON structure\n")
      print(str(json_data))
    }
    
  }, error = function(e) {
    cat("❌ Error reading JSON file:", e$message, "\n")
  })
  
} else {
  cat("❌ File not found: data/raw/contracts_timeseries.json\n")
}


cat("\n")
cat("4. CHECKING EXTERNAL INTEREST RATES\n")
cat("===================================\n")

cat("Retrieving real German/US interest rates 2020-2024...\n")

interest_data <- NULL
source_info <- "none"

# Method: quantmod + Yahoo Finance (since your quick test worked)
tryCatch({
  # Install quantmod if needed
  if (!requireNamespace("quantmod", quietly = TRUE)) {
    cat("Installing quantmod package...\n")
    install.packages("quantmod")
  }
  
  library(quantmod)
  cat("✓ quantmod package loaded\n")
  
  # Download 10-Year Treasury rates (your test that worked)
  cat("Downloading ^TNX (10-Year Treasury) data...\n")
  
  rates_xts <- getSymbols("^TNX", src = "yahoo", 
                         from = "2020-01-01", 
                         to = "2024-12-31", 
                         auto.assign = FALSE)
  
  if (!is.null(rates_xts) && nrow(rates_xts) > 0) {
    cat("✓ Downloaded", nrow(rates_xts), "daily data points\n")
    
    # Convert to data frame
    rates_df <- data.frame(
      date = index(rates_xts),
      close = as.numeric(Cl(rates_xts))
    )
    
    # Remove NA values
    rates_df <- rates_df[!is.na(rates_df$close), ]
    
    # Create monthly aggregation
    library(dplyr)
    rates_df$year_month <- format(rates_df$date, "%Y-%m")
    
    interest_data <- rates_df %>%
      group_by(year_month) %>%
      summarise(
        date = max(date),
        interest_rate = round(last(close), 2),
        .groups = 'drop'
      ) %>%
      arrange(date)
    
    source_info <- "Yahoo Finance (^TNX - 10-Year Treasury)"
    cat("✓ Processed to", nrow(interest_data), "monthly rates\n")
    
  } else {
    stop("No data returned from Yahoo Finance")
  }
  
}, error = function(e) {
  cat("❌ Failed to retrieve real interest rates:", e$message, "\n")
  cat("Possible causes:\n")
  cat("- Internet connection issue\n") 
  cat("- Yahoo Finance temporarily unavailable\n")
  cat("- Package installation problem\n")
  interest_data <- NULL
})

# Display results
if (!is.null(interest_data) && nrow(interest_data) > 0) {
  cat("\n--- REAL INTEREST RATES RETRIEVED ---\n")
  cat("Source:", source_info, "\n")
  cat("Data points:", nrow(interest_data), "monthly rates\n")
  cat("Date range:", min(interest_data$date), "to", max(interest_data$date), "\n")
  cat("Rate range:", min(interest_data$interest_rate, na.rm = TRUE), "% to", 
      max(interest_data$interest_rate, na.rm = TRUE), "%\n")
  
  cat("\nFirst 5 rates:\n")
  print(head(interest_data, 5))
  
  cat("\nLast 5 rates:\n")
  print(tail(interest_data, 5))
}
# SUMMARY
cat("\n")
cat("5. SUMMARY\n")
cat("==========\n")

files_found <- 0
if (file.exists("data/raw/contracts.sqlite")) files_found <- files_found + 1
if (file.exists("data/raw/customers.xlsx")) files_found <- files_found + 1
if (file.exists("data/raw/contracts_timeseries.json")) files_found <- files_found + 1

cat("Files found:", files_found, "out of 3\n")

if (files_found == 3) {
  cat("✅ All data files are present!\n")
  cat("Next step: Run the ETL pipeline to process the data\n")
} else {
  cat("⚠️ Some data files are missing\n")
  cat("Please check that all files are in the data/raw/ directory\n")
}

cat("\nData check completed:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=== END OF DATA CHECK ===\n")

sink()
cat("📄 Complete assessment log saved to:", log_file, "\n")