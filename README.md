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

   ```

## 📁 Projektstruktur (Beispiel)

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

