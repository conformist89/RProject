# Lebensversicherungs ETL-Pipeline

# 🔧 Hauptfunktionen der Datenverarbeitungspipeline

Dieses Projekt beinhaltet eine automatisierte Datenverarbeitung und -bereinigung für Lebensversicherungsverträge. Die wichtigsten Funktionen sind unten dokumentiert.

---

## 🔁 1. Umbenennung und Standardisierung von Spaltennamen

- **Namensstandardisierung**  
  - Umwandlung aller Spaltennamen in Kleinbuchstaben  
  - Entfernen von Sonderzeichen und Leerzeichen  

- **Tabellenspezifisches Mapping**  
  - Spaltennamen werden je nach Tabellentyp in einheitliche Standards überführt  

- **Deutsch → Englisch Mapping**  
  - Beispiel: `rueckkaufswert` → `surrender_value`

---

## 🧮 2. Konvertierung von Datentypen

- **Vertragsnummern (`contract_id`)**  
  - Formatbereinigung zum Muster `V###`  
  - Entfernung ungültiger Zeichen  

- **Kundennummern (`customer_id`)**  
  - Standardisierung auf Format `K###`  

- **Mandantennummern (`mandant_id`)**  
  - Umwandlung in gültige Werte (`LV1`, `LV2`)  

- **Datumsfelder**  
  - Konvertierung in einheitliches Datumsformat  
  - Unterstützung mehrerer Eingabeformate (z. B. `dd.mm.yyyy`, `yyyy-mm-dd`)  

- **Numerische Werte**  
  - Umwandlung von Rückkaufswerten in numerisches Format  
  - Entfernung negativer Werte

---

## 🚫 3. Strategie für fehlende Werte

### 🔴 DROP-Strategie (kritische Felder)

- **Fehlende Vertragsnummer (`contract_id`)**  
  → Zeile wird **gelöscht** (wichtig für Joins)

- **Fehlende Kundennummer (`customer_id`) in Kundentabelle**  
  → Zeile wird **gelöscht** (Primärschlüssel fehlt)

- **Fehlendes Datum und Vertragsnummer in Zeitreihe**  
  → Zeile wird **gelöscht** (beides kritisch)

### 🟡 IMPUTE-Strategie (nicht-kritische Felder)

- **Fehlende Mandantennummer (`mandant_id`)**  
  → Ersetzung durch häufigsten Wert (`LV1` oder `LV2`)

### 🟢 KEEP-Strategie (legitim fehlend)

- **Fehlendes Geburtsdatum (`birth_date`)**  
  → **Beibehalten** (mögliche Datenschutzgründe)

- **Fehlender Rückkaufswert (`surrender_value`)**  
  → **Beibehalten** (mögliche echte Datenlücke)

---

## ✅ 4. Fehlerbehandlung & Validierung

- **Duplikaterkennung**  
  - Doppelte Zeilen werden vollständig entfernt

- **Validierung von Datumswerten**  
  - Keine Geburten in der Zukunft  
  - Plausible Zeiträume für Vertrags- und Ereignisdaten

- **Logging**  
  - Umfassende Protokollierung aller durchgeführten Schritte zur Nachvollziehbarkeit

---


## 📁 Projektstruktur (Beispiel)

- `data/raw/` - Eingangsdaten (SQLite, Excel, JSON)
- `data/processed/` - Verarbeitete Daten
- `R/` - Helper-Funktionen
- `reports/` - Generierte Berichte
- `pipeline.qmd` - Hauptdokumentation
- `simple_data_quality.R` - interne Daten laden und Datenqualität prüfen
- `remote_data_quality.R` - externe Zinssätze laden und Datenqualität prüfen
- `cleaned_data.R` - Rohdatenbasis Aufgabe und speichert die bereinigte Rohdatenbasis versioniert in DuckDB
- `duckdb_test.r` - Validierung der Ergebnisse aus der DuckDB-Ausgabedatei
- `check_data.R` - ein einfacher Datencheck ohne Details
- `vergleich.R` - Analyse & Business‑Logik Skript
- `analyse.r` - Das Skript `vergleich.R` ausführen

## Daten vorbereiten

Legen Sie folgende Dateien in `data/raw/`:
- `contracts.sqlite` - Vertragsdaten
- `customers.xlsx` - Kundendaten  
- `contracts_timeseries.json` - Zeitreihendaten

## Abhängigkeiten

Dieses Projekt verwendet `renv` für reproduzierbare Umgebungen.
Bei Problemen: `renv::restore()`


## Verwendete ChatGPT-Prompts

1. "Wie lese ich Daten aus SQLite, Excel und JSON in R ein?"
2. "Wie kann ich historische Zinssätze in R mit quantmod/yahoofinancer abrufen?"
3. "Wie prüfe ich Datenqualität mit pointblank?"
4. "Wie schreibe ich eine Funktion clean_data() in R zur Datenbereinigung?"
5. "Wie speichere ich DataFrames in DuckDB mit Zeitstempel?"
6. "Wie kann ich Altersklassen basierend auf Geburtsdatum berechnen?"

