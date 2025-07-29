# Data Quality Assessment with pointblank
# File: data_quality.R

cat("=== DATA QUALITY ASSESSMENT ===\n")
cat("Started:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")

# Install and load required packages
if (!require(pointblank)) {
  cat("Installing pointblank...\n")
  renv::install("pointblank")
}
if (!require(DBI)) renv::install("DBI")
if (!require(RSQLite)) renv::install("RSQLite")
if (!require(readxl)) renv::install("readxl")
if (!require(jsonlite)) renv::install("jsonlite")
if (!require(dplyr)) renv::install("dplyr")

library(pointblank)
library(DBI)
library(RSQLite)
library(readxl)
library(jsonlite)
library(dplyr)

cat("✓ All packages loaded\n\n")

# LOAD YOUR ACTUAL DATA
cat("LOADING DATA FOR QUALITY CHECKS\n")
cat("================================\n")

# 1. Load SQLite data
con <- dbConnect(RSQLite::SQLite(), "data/raw/contracts.sqlite")
tables <- dbListTables(con)
contracts_data <- dbGetQuery(con, paste("SELECT * FROM", tables[1]))
dbDisconnect(con)
cat("✓ Loaded contracts:", nrow(contracts_data), "rows\n")

# 2. Load Excel data
customers_data <- read_excel("data/raw/customers.xlsx")
names(customers_data) <- tolower(names(customers_data))
cat("✓ Loaded customers:", nrow(customers_data), "rows\n")

# 3. Load JSON data
json_raw <- fromJSON("data/raw/contracts_timeseries.json")
if (is.list(json_raw) && !is.data.frame(json_raw)) {
  timeseries_data <- do.call(rbind, lapply(json_raw, data.frame))
} else {
  timeseries_data <- json_raw
}
cat("✓ Loaded timeseries:", nrow(timeseries_data), "rows\n\n")

# POINTBLANK QUALITY CHECKS
cat("RUNNING POINTBLANK QUALITY CHECKS\n")
cat("==================================\n")

# 1. CONTRACTS DATA QUALITY
cat("1. Contracts Data Quality\n")
cat("-------------------------\n")

contracts_agent <- create_agent(
  tbl = contracts_data,
  tbl_name = "contracts",
  label = "Contracts Data Quality Assessment"
) %>%
  # Check for missing values in key columns
  col_vals_not_null(columns = vars(contractid)) %>%
  col_vals_not_null(columns = vars(customerid)) %>%
  
  # Check for unique contract IDs
  col_vals_unique(columns = vars(contractid)) %>%
  
  # Check data formats (if your data has these patterns)
  col_vals_regex(columns = vars(contractid), regex = "^V[0-9]+$", na_pass = TRUE) %>%
  col_vals_regex(columns = vars(customerid), regex = "^K[0-9]+$", na_pass = TRUE) %>%
  
  # Execute the checks
  interrogate()

# Display results
print(contracts_agent)
cat("\nContracts Summary:\n")
print(get_sundered_data(contracts_agent))

# 2. CUSTOMERS DATA QUALITY  
cat("\n2. Customers Data Quality\n")
cat("-------------------------\n")

customers_agent <- create_agent(
  tbl = customers_data,
  tbl_name = "customers",
  label = "Customers Data Quality Assessment"
) %>%
  # Check required columns exist and have values
  col_vals_not_null(columns = vars(contractid)) %>%
  
  # Check birthdate validity (if column exists)
  {
    if ("birthdate" %in% names(customers_data)) {
      . %>%
        col_vals_not_null(columns = vars(birthdate)) %>%
        col_vals_lt(columns = vars(birthdate), value = Sys.Date()) %>%
        col_vals_gt(columns = vars(birthdate), value = as.Date("1900-01-01"))
    } else {
      .
    }
  } %>%
  
  # Execute checks
  interrogate()

print(customers_agent)
cat("\nCustomers Summary:\n")
print(get_sundered_data(customers_agent))

# 3. TIMESERIES DATA QUALITY
cat("\n3. Timeseries Data Quality\n")
cat("--------------------------\n")

timeseries_agent <- create_agent(
  tbl = timeseries_data,
  tbl_name = "timeseries", 
  label = "Timeseries Data Quality Assessment"
) %>%
  # Check for required columns
  col_vals_not_null(columns = vars(contractid)) %>%
  
  # Check date validity (if date column exists)
  {
    date_col <- intersect(c("date", "Date", "DATE"), names(timeseries_data))
    if (length(date_col) > 0) {
      . %>% col_vals_not_null(columns = vars(!!sym(date_col[1])))
    } else {
      .
    }
  } %>%
  
  # Check numeric values (if value column exists)
  {
    value_col <- intersect(c("rueckkaufswert", "value", "amount"), names(timeseries_data))
    if (length(value_col) > 0) {
      . %>% 
        col_vals_not_null(columns = vars(!!sym(value_col[1]))) %>%
        col_vals_gte(columns = vars(!!sym(value_col[1])), value = 0)
    } else {
      .
    }
  } %>%
  
  # Execute checks
  interrogate()

print(timeseries_agent)
cat("\nTimeseries Summary:\n")
print(get_sundered_data(timeseries_agent))

# CROSS-TABLE REFERENTIAL INTEGRITY
cat("\n4. Referential Integrity Checks\n")
cat("--------------------------------\n")

# Check if contract IDs match between tables
contracts_ids <- unique(contracts_data$contractid)
customers_ids <- unique(customers_data$contractid)
timeseries_ids <- unique(timeseries_data$contractid)

cat("Contract IDs in contracts table:", length(contracts_ids), "\n")
cat("Contract IDs in customers table:", length(customers_ids), "\n")
cat("Contract IDs in timeseries table:", length(timeseries_ids), "\n")

# Overlap analysis
contracts_in_customers <- sum(contracts_ids %in% customers_ids)
contracts_in_timeseries <- sum(contracts_ids %in% timeseries_ids)
customers_in_timeseries <- sum(customers_ids %in% timeseries_ids)

cat("\nReferential Integrity:\n")
cat("- Contracts with customer data:", contracts_in_customers, "of", length(contracts_ids), "\n")
cat("- Contracts with timeseries data:", contracts_in_timeseries, "of", length(contracts_ids), "\n")
cat("- Customers with timeseries data:", customers_in_timeseries, "of", length(customers_ids), "\n")

# EXPORT QUALITY REPORTS
cat("\n5. Exporting Quality Reports\n")
cat("----------------------------\n")

if (!dir.exists("reports")) dir.create("reports", recursive = TRUE)

# Export HTML reports
tryCatch({
  export_report(contracts_agent, filename = "reports/contracts_quality_report.html")
  cat("✓ Contracts quality report saved to: reports/contracts_quality_report.html\n")
}, error = function(e) {
  cat("⚠️ Could not export contracts report:", e$message, "\n")
})

tryCatch({
  export_report(customers_agent, filename = "reports/customers_quality_report.html")
  cat("✓ Customers quality report saved to: reports/customers_quality_report.html\n")
}, error = function(e) {
  cat("⚠️ Could not export customers report:", e$message, "\n")
})

tryCatch({
  export_report(timeseries_agent, filename = "reports/timeseries_quality_report.html")
  cat("✓ Timeseries quality report saved to: reports/timeseries_quality_report.html\n")
}, error = function(e) {
  cat("⚠️ Could not export timeseries report:", e$message, "\n")
})

# SUMMARY
cat("\n=== QUALITY ASSESSMENT SUMMARY ===\n")
cat("Reports generated in reports/ directory\n")
cat("- contracts_quality_report.html\n")
cat("- customers_quality_report.html\n") 
cat("- timeseries_quality_report.html\n")

cat("\nNext steps:\n")
cat("1. Review the HTML reports in your browser\n")
cat("2. Fix any data quality issues found\n")
cat("3. Run the complete ETL pipeline\n")

cat("\nQuality assessment completed:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("=== END OF QUALITY ASSESSMENT ===\n")