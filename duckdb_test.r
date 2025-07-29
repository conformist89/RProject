# =============================================================================
# DUCKDB DATEN VERIFICATION - PRÜFUNG DER GESPEICHERTEN DATEN
# =============================================================================

# Load DuckDB if not already loaded
if (!require("duckdb", character.only = TRUE, quietly = TRUE)) {
  install.packages("duckdb")
  library(duckdb)
}

# =============================================================================
# METHODE 1: SCHNELLE DATEI-PRÜFUNG
# =============================================================================

check_duckdb_file <- function(db_path = "data/cleaned/insurance_data.duckdb") {
  cat("BASIC FILE CHECK\n")
  cat("================\n")
  
  if (file.exists(db_path)) {
    file_size <- file.size(db_path)
    file_size_mb <- round(file_size / (1024^2), 2)
    creation_time <- file.info(db_path)$mtime
    
    cat("✅ Database file exists:", db_path, "\n")
    cat("📁 File size:", file_size_mb, "MB\n")
    cat("📅 Last modified:", format(creation_time, "%Y-%m-%d %H:%M:%S"), "\n")
    
    if (file_size > 1000) {  # Größer als 1KB
      cat("✅ File size indicates data is stored\n")
      return(TRUE)
    } else {
      cat("⚠️ File is very small - might be empty\n")
      return(FALSE)
    }
  } else {
    cat("❌ Database file not found:", db_path, "\n")
    return(FALSE)
  }
}

# =============================================================================
# METHODE 2: DATABASE STRUKTUR PRÜFEN
# =============================================================================

inspect_duckdb_structure <- function(db_path = "data/cleaned/insurance_data.duckdb") {
  cat("DATABASE STRUCTURE INSPECTION\n")
  cat("=============================\n")
  
  if (!file.exists(db_path)) {
    cat("❌ Database file not found\n")
    return(NULL)
  }
  
  tryCatch({
    # Verbindung öffnen
    con <- dbConnect(duckdb::duckdb(), dbdir = db_path)
    
    # Alle Tabellen auflisten
    tables <- dbGetQuery(con, "SHOW TABLES")
    
    cat("📋 Total tables found:", nrow(tables), "\n\n")
    
    if (nrow(tables) > 0) {
      cat("TABLE LIST:\n")
      for (i in 1:nrow(tables)) {
        table_name <- tables$name[i]
        cat("🗂️ ", table_name, "\n")
      }
      
      cat("\nTABLE DETAILS:\n")
      for (i in 1:nrow(tables)) {
        table_name <- tables$name[i]
        
        # Row count
        tryCatch({
          row_count <- dbGetQuery(con, paste0("SELECT COUNT(*) as count FROM ", table_name))$count
          cat("   📊", table_name, ":", row_count, "rows\n")
        }, error = function(e) {
          cat("   ❌", table_name, ": Error counting rows\n")
        })
      }
    } else {
      cat("❌ No tables found in database\n")
    }
    
    # Verbindung schließen
    dbDisconnect(con)
    
    return(tables)
    
  }, error = function(e) {
    cat("❌ Error inspecting database:", e$message, "\n")
    return(NULL)
  })
}

# =============================================================================
# METHODE 3: VERSIONEN PRÜFEN (Ihre eingebaute Funktion verwenden)
# =============================================================================

check_data_versions <- function(db_path = "data/cleaned/insurance_data.duckdb") {
  cat("DATA VERSIONS CHECK\n")
  cat("===================\n")
  
  if (!file.exists(db_path)) {
    cat("❌ Database file not found\n")
    return(NULL)
  }
  
  tryCatch({
    con <- dbConnect(duckdb::duckdb(), dbdir = db_path)
    
    # Prüfen ob data_versions Tabelle existiert
    tables <- dbGetQuery(con, "SHOW TABLES")
    
    if ("data_versions" %in% tables$name) {
      versions <- dbGetQuery(con, "
        SELECT 
          version_id,
          created_at,
          contracts_rows,
          customers_rows,
          timeseries_rows,
          total_rows,
          notes
        FROM data_versions 
        ORDER BY created_at DESC
      ")
      
      if (nrow(versions) > 0) {
        cat("✅ Found", nrow(versions), "data versions:\n\n")
        
        for (i in 1:nrow(versions)) {
          v <- versions[i, ]
          cat("🔖 Version:", v$version_id, "\n")
          cat("   📅 Created:", v$created_at, "\n")
          cat("   📊 Total rows:", v$total_rows, "\n")
          cat("   📋 Details: Contracts(", v$contracts_rows, "), Customers(", 
              v$customers_rows, "), Timeseries(", v$timeseries_rows, ")\n")
          cat("   📝 Notes:", v$notes, "\n\n")
        }
        
        dbDisconnect(con)
        return(versions)
      } else {
        cat("⚠️ data_versions table exists but is empty\n")
      }
    } else {
      cat("❌ data_versions table not found\n")
    }
    
    dbDisconnect(con)
    return(NULL)
    
  }, error = function(e) {
    cat("❌ Error checking versions:", e$message, "\n")
    return(NULL)
  })
}

# =============================================================================
# METHODE 4: DATEN-SAMPLE PRÜFEN
# =============================================================================

sample_data_from_duckdb <- function(db_path = "data/cleaned/insurance_data.duckdb", 
                                   table_pattern = NULL, limit = 5) {
  cat("DATA SAMPLING FROM DUCKDB\n")
  cat("=========================\n")
  
  if (!file.exists(db_path)) {
    cat("❌ Database file not found\n")
    return(NULL)
  }
  
  tryCatch({
    con <- dbConnect(duckdb::duckdb(), dbdir = db_path)
    
    # Alle Tabellen finden
    tables <- dbGetQuery(con, "SHOW TABLES")
    
    # Filter tables if pattern provided
    if (!is.null(table_pattern)) {
      tables <- tables[grepl(table_pattern, tables$name), , drop = FALSE]
    }
    
    if (nrow(tables) == 0) {
      cat("❌ No matching tables found\n")
      dbDisconnect(con)
      return(NULL)
    }
    
    samples <- list()
    
    for (i in 1:nrow(tables)) {
      table_name <- tables$name[i]
      
      tryCatch({
        # Sample data
        sample_query <- paste0("SELECT * FROM ", table_name, " LIMIT ", limit)
        sample_data <- dbGetQuery(con, sample_query)
        
        cat("📋 Sample from", table_name, ":\n")
        print(head(sample_data))
        cat("\n")
        
        samples[[table_name]] <- sample_data
        
      }, error = function(e) {
        cat("❌ Error sampling from", table_name, ":", e$message, "\n")
      })
    }
    
    dbDisconnect(con)
    return(samples)
    
  }, error = function(e) {
    cat("❌ Error sampling data:", e$message, "\n")
    return(NULL)
  })
}

# =============================================================================
# METHODE 5: VOLLSTÄNDIGE VERIFICATION
# =============================================================================

complete_duckdb_verification <- function(db_path = "data/cleaned/insurance_data.duckdb") {
  cat("COMPLETE DUCKDB VERIFICATION\n")
  cat("============================\n")
  cat("Database path:", db_path, "\n\n")
  
  # 1. File check
  file_ok <- check_duckdb_file(db_path)
  cat("\n")
  
  if (!file_ok) {
    cat("❌ File check failed - stopping verification\n")
    return(FALSE)
  }
  
  # 2. Structure check
  tables <- inspect_duckdb_structure(db_path)
  cat("\n")
  
  if (is.null(tables) || nrow(tables) == 0) {
    cat("❌ No tables found - database might be corrupted\n")
    return(FALSE)
  }
  
  # 3. Versions check
  versions <- check_data_versions(db_path)
  cat("\n")
  
  # 4. Sample latest version data
  if (!is.null(versions) && nrow(versions) > 0) {
    latest_version <- versions$version_id[1]
    cat("SAMPLING LATEST VERSION DATA (", latest_version, "):\n")
    cat("================================================\n")
    samples <- sample_data_from_duckdb(db_path, latest_version, limit = 3)
  } else {
    cat("SAMPLING AVAILABLE DATA:\n")
    cat("========================\n")
    samples <- sample_data_from_duckdb(db_path, limit = 3)
  }
  
  # 5. Final verdict
  cat("FINAL VERIFICATION RESULT:\n")
  cat("==========================\n")
  
  if (file_ok && !is.null(tables) && nrow(tables) > 0) {
    cat("✅ SUCCESS: Data is properly stored in DuckDB!\n")
    cat("📊 Found", nrow(tables), "tables with data\n")
    
    if (!is.null(versions)) {
      cat("🔖 Found", nrow(versions), "data versions\n")
    }
    
    return(TRUE)
  } else {
    cat("❌ FAILURE: Data storage verification failed\n")
    return(FALSE)
  }
}

# =============================================================================
# METHODE 6: SCHNELL-CHECK FÜR COMMAND LINE
# =============================================================================

quick_check <- function() {
  cat("QUICK DUCKDB CHECK\n")
  cat("==================\n")
  
  db_path <- "data/cleaned/insurance_data.duckdb"
  
  if (file.exists(db_path)) {
    size_mb <- round(file.size(db_path) / (1024^2), 2)
    cat("✅ Database exists (", size_mb, "MB)\n")
    
    # Quick connection test
    tryCatch({
      con <- dbConnect(duckdb::duckdb(), dbdir = db_path)
      table_count <- nrow(dbGetQuery(con, "SHOW TABLES"))
      dbDisconnect(con)
      
      cat("✅ Connection successful -", table_count, "tables found\n")
      return(TRUE)
    }, error = function(e) {
      cat("❌ Connection failed:", e$message, "\n")
      return(FALSE)
    })
  } else {
    cat("❌ Database file not found\n")
    return(FALSE)
  }
}

# =============================================================================
# USAGE EXAMPLES
# =============================================================================

cat("DUCKDB VERIFICATION METHODS LOADED\n")
cat("===================================\n")
cat("Available functions:\n")
cat("1. quick_check()                     - Schnelle Überprüfung\n")
cat("2. check_duckdb_file()              - Datei-Details\n") 
cat("3. inspect_duckdb_structure()       - Tabellen-Struktur\n")
cat("4. check_data_versions()            - Versionen anzeigen\n")
cat("5. sample_data_from_duckdb()        - Daten-Samples\n")
cat("6. complete_duckdb_verification()   - Vollständige Prüfung\n")
cat("\nRun any of these functions to verify your data!\n")

# Automatische Schnell-Prüfung
cat("\nAUTOMATIC QUICK CHECK:\n")
quick_check()
check_duckdb_file()
inspect_duckdb_structure()
check_data_versions()
sample_data_from_duckdb() 

# Vollständige Prüfung

cat("\nFULL CHECK:\n")
complete_duckdb_verification()