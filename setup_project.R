# Komplettes Setup-Skript für Lebensversicherungs-ETL Projekt
# Datei: setup_project.R
# 
# Dieses Skript richtet das komplette Projekt mit renv ein

cat("=== LEBENSVERSICHERUNGS ETL-PROJEKT SETUP ===\n\n")

# 1. renv initialisieren (falls noch nicht geschehen)
if (!file.exists("renv.lock")) {
  cat("📦 Initialisiere renv...\n")
  renv::init()
} else {
  cat("📦 renv bereits initialisiert\n")
}

# 2. Alle benötigten Pakete definieren
required_packages <- c(
  # Datenbank und Import
  "DBI",
  "RSQLite", 
  "readxl",
  "jsonlite",
  
  # Externe Daten
  "yahoofinancer",
  
  # Datenqualität
  "pointblank",
  
  # Datenmanipulation
  "dplyr",
  "lubridate",
  "tidyr",
  
  # Visualisierung
  "ggplot2",
  
  # Utilities
  "here",
  "optparse",
  
  # Dokumentation
  "knitr",
  "rmarkdown",
  "quarto"
)

cat("📥 Installiere benötigte Pakete...\n")
cat("Pakete:", paste(required_packages, collapse = ", "), "\n\n")

# 3. Pakete installieren
for (pkg in required_packages) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    cat("  - Installiere:", pkg, "\n")
    tryCatch({
      install.packages(pkg)
    }, error = function(e) {
      cat("    ⚠️ Warnung bei", pkg, ":", e$message, "\n")
    })
  } else {
    cat("  ✓", pkg, "bereits installiert\n")
  }
}

# 4. Spezielle Pakete (falls Standard-Installation fehlschlägt)
tryCatch({
  if (!requireNamespace("yahoofinancer", quietly = TRUE)) {
    cat("🌐 Versuche yahoofinancer von GitHub...\n")
    if (!requireNamespace("remotes", quietly = TRUE)) {
      install.packages("remotes")
    }
    remotes::install_github("RSingh-Data/yahoofinancer")
  }
}, error = function(e) {
  cat("⚠️ yahoofinancer Installation fehlgeschlagen. Verwende Fallback-Daten.\n")
})

# 5. renv snapshot erstellen
cat("\n📸 Erstelle renv snapshot...\n")
renv::snapshot(confirm = FALSE)

# 6. Projektstruktur erstellen
cat("📁 Erstelle Projektstruktur...\n")

directories <- c(
  "data/raw",
  "data/processed", 
  "R",
  "reports",
  "tests"
)

for (dir in directories) {
  if (!dir.exists(dir)) {
    dir.create(dir, recursive = TRUE)
    cat("  ✓ Erstellt:", dir, "\n")
  } else {
    cat("  - Existiert bereits:", dir, "\n")
  }
}

# 7. README erstellen
readme_content <- '# Lebensversicherungs ETL-Pipeline

## Schnellstart

1. **Projekt öffnen**: Doppelklick auf `insurance_etl_project.Rproj`
2. **Pipeline ausführen**: 
   ```r
   source("main.R")
   ```
3. **Interaktiven Bericht erstellen**:
   ```r
   quarto::quarto_render("pipeline.qmd")
   ```

## Projektstruktur

- `data/raw/` - Eingangsdaten (SQLite, Excel, JSON)
- `data/processed/` - Verarbeitete Daten
- `R/` - Helper-Funktionen
- `reports/` - Generierte Berichte
- `pipeline.qmd` - Hauptdokumentation
- `main.R` - Einfache Ausführung

## Daten vorbereiten

Legen Sie folgende Dateien in `data/raw/`:
- `contracts.sqlite` - Vertragsdaten
- `customers.xlsx` - Kundendaten  
- `contracts_timeseries.json` - Zeitreihendaten

## Abhängigkeiten

Dieses Projekt verwendet `renv` für reproduzierbare Umgebungen.
Bei Problemen: `renv::restore()`
'

writeLines(readme_content, "README.md")
cat("  ✓ README.md erstellt\n")

# 8. .Rproj Datei erstellen (falls nicht vorhanden)
if (!file.exists(paste0(basename(getwd()), ".Rproj"))) {
  rproj_content <- 'Version: 1.0

RestoreWorkspace: Default
SaveWorkspace: Default
AlwaysSaveHistory: Default

EnableCodeIndexing: Yes
UseSpacesForTab: Yes
NumSpacesForTab: 2
Encoding: UTF-8

RnwWeave: Sweave
LaTeX: pdfLaTeX

AutoAppendNewline: Yes
StripTrailingWhitespace: Yes
'
  
  writeLines(rproj_content, paste0(basename(getwd()), ".Rproj"))
  cat("  ✓ .Rproj Datei erstellt\n")
}

# 9. Test der Installation
cat("\n🧪 Teste Installation...\n")
test_packages <- c("DBI", "dplyr", "ggplot2", "here")

all_good <- TRUE
for (pkg in test_packages) {
  tryCatch({
    library(pkg, character.only = TRUE)
    cat("  ✓", pkg, "funktioniert\n")
  }, error = function(e) {
    cat("  ❌", pkg, "Problem:", e$message, "\n")
    all_good <- FALSE
  })
}

# 10. Zusammenfassung
cat("\n=== SETUP ABGESCHLOSSEN ===\n")
if (all_good) {
  cat("✅ Alle Pakete erfolgreich installiert!\n")
} else {
  cat("⚠️ Einige Pakete haben Probleme. Prüfen Sie die Meldungen oben.\n")
}

cat("\n📋 NÄCHSTE SCHRITTE:\n")
cat("1. Kopieren Sie Ihre Daten nach data/raw/\n")
cat("2. Erstellen Sie die R-Skripte (etl_functions.R, main.R, pipeline.qmd)\n") 
cat("3. Führen Sie die Pipeline aus: source('main.R')\n")

cat("\n🔄 renv Status:\n")
renv::status()

cat("\n🎯 Projekt bereit! Happy coding! 🚀\n")