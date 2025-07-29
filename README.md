# Lebensversicherungs ETL-Pipeline

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

