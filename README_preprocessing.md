# Datenaufbereitung Kinderbetreuung (Preprocessing)

Dieses Verzeichnis enthält die Skripte zur Aufbereitung der Datenbasis für den Chatbot zur Kinderbetreuung in Oberösterreich. Die Pipeline kombiniert offene Verwaltungsdaten (OGD) mit ergänzenden Informationen aus Web-Scraping und einem kirchlichen PDF-Verzeichnis.

## Überblick

Die Preprocessing-Pipeline besteht aus vier Hauptschritten:

1. **Bereinigung des OGD-Datensatzes** (`ogd_preprocessing.py`)
2. **Web-Scraping von Trägerseiten** (`kbbe_web_scraper.py`)
3. **Extraktion kirchlicher Einrichtungen aus einem PDF** (`caritas_pdf_extraction.py`)
4. **Merge & Enrichment des OGD-Datensatzes** (`kbbe_merge.py`)

Alle Zwischenergebnisse und finalen Dateien werden im Ordner  
`preprocessing/outputs/` abgelegt.

---

## Verzeichnisstruktur

Vorausgesetzte Struktur im Projektroot:

```
agentic_rag_chatbot/
├── raw_data/
│   ├── kbbes.csv
│   └── Liste_der_kirchlichen_Kinderbildungs-_und_-betreuungseinrichtungen.pdf
├── preprocessing/
│   ├── ogd_preprocessing.py
│   ├── kbbe_web_scraper.py
│   ├── caritas_pdf_extraction.py
│   ├── kbbe_merge.py
│   └── outputs/
└── requirements.txt
```

## 1. OGD-Bereinigung (`ogd_preprocessing.py`)

**Zweck:**  
Bereinigung des OGD-Datensatzes `kbbes.csv` aus `raw_data/` und Vereinheitlichung zentraler Felder als Grundlage für das spätere Matching und Enrichment.

### Verarbeitungsschritte

- Vereinheitlichung der Spaltennamen (Trimmen, Kleinschreibung)
- Bereinigung der Telefonnummern (Entfernen von Leerzeichen, Normalisierung)
- Normalisierung der `weburl`-Spalte:
  - Trimmen
  - Kleinschreibung
  - Ersetzen von `www.` durch `https://www.` (einheitliches Format)
- Mapping der Kurzbezeichnungen in der Spalte `art` (z. B. `KG → Kindergarten`)
- Mapping der Bezirkskennzahlen (`bezirk`) auf deren Klartext-Bezeichnungen
- Aktualisierung einzelner veralteter URLs mittels manuell gepflegtem Mapping
- Entfernen von Testeinrichtungen (Zeilen, die „TEST“ im Namen enthalten)

### Output

- `preprocessing/outputs/ogd_preprocessed.csv`  
  → bereinigter OGD-Datensatz, der als Referenz für das Matching dient.

### Ausführung

Im Projektroot:

```bash
(.venv) python3 preprocessing/ogd_preprocessing.py
```

## 2. Web-Scraping (`kbbe_web_scraper.py`)

**Zweck:**  
Erweiterung des OGD-Datensatzes durch zusätzliche Informationen der wichtigsten Träger in Oberösterreich:

- Stadt Linz (Serviceguide)
- Kinderfreunde
- Familienbund

Die Webseiten dieser Träger enthalten oft aktuellere oder detailliertere Informationen, die im OGD-Datensatz fehlen — z. B. Öffnungszeiten, Beschreibungen, Anmeldeformulare oder Kontaktdetails.

---

### Vorgehen

Für jeden Träger existieren Python-Listen mit `(name, url)`-Tupeln.  
Jede URL wird mit einer spezifischen Parser-Funktion verarbeitet:

- **parse_linz_facility_page**
- **parse_kinderfreunde_page**
- **parse_familienbund_page**

Dabei werden u. a. folgende Felder extrahiert:

- Name der Einrichtung  
- Straße, PLZ, Ort  
- Telefon, E-Mail  
- Öffnungszeiten (heuristisch)  
- Beschreibung oder Besonderheiten (falls vorhanden)  
- Anmelde- oder Vormerk-Links  
- Trägerbezeichnung

Alle Ergebnisse werden in ein harmonisiertes Schema überführt.

---

### Output

Nach dem Scraping entstehen folgende Dateien:

- `preprocessing/outputs/linz_kinderbetreuung_stadt.csv`  
- `preprocessing/outputs/kinderfreunde_kinderbetreuung_ooe.csv`  
- `preprocessing/outputs/familienbund_kinderbetreuung_ooe.csv`

Diese dienen anschließend als Eingabe für den Merge-Prozess.

---

### Ausführung

Im Projektroot:

```bash
(.venv) python3 preprocessing/kbbe_web_scraper.py
```

## 3. Caritas-PDF-Extraktion (`caritas_pdf_extraction.py`)

**Zweck:**  
Extraktion und Strukturierung des kirchlichen Verzeichnisses aus dem PDF

```text
raw_data/Liste_der_kirchlichen_Kinderbildungs-_und_-betreuungseinrichtungen.pdf
```

Dieses PDF enthält eine Liste kirchlicher Kinderbildungs- und -betreuungseinrichtungen, die im OGD-Datensatz so nicht direkt verfügbar ist.

---

### Vorgehen

1. **Tabellenextraktion mit `tabula-py`**

   - Alle Seiten des PDFs werden mit `tabula.read_pdf(...)` eingelesen.
   - Tabellarische Inhalte werden in eine Liste von `pandas.DataFrame`-Objekten überführt.
   - Offensichtlich leere Zeilen und Kopfzeilen werden entfernt.

2. **Parsing & Standardisierung**

   Pro Tabellenzeile wird ein Datensatz in folgendes Schema überführt:

   - `name` – Name der Einrichtung  
   - `strasse` – Straßenadresse  
   - `plz` – Postleitzahl  
   - `ort` – Ort  
   - `telefon` – Telefonnummer  
   - `email` – E-Mail-Adresse  
   - `bezirk` – Bezirk (aus Kopfzeilen bzw. Abschnittstiteln)

   Zusätzlich werden offensichtliche Formatierungsartefakte (z. B. PLZ als Float) bereinigt.

3. **Ableitung von Einrichtungstyp und Träger**

   - `art` wird heuristisch aus dem Namen abgeleitet  
     (z. B. „Krabbelstube“, „Kindergarten“, „Hort“).  
   - `traeger` wird über die E-Mail-Domain und Namensbestandteile klassifiziert  
     (z. B. „Caritas“, „Pfarrcaritas“, „VFFB“, Orden).

4. **Bereinigung der Ortsnamen**

   - Fälle, in denen Straßenteile versehentlich im Ortsfeld landen, werden über heuristische Regeln korrigiert  
     (z. B. Tokens wie „Straße“, „Strasse“, „weg“, „gasse“, „platz“).

5. **Missingness-Analyse (optional)**

   - Fehlende Werte werden tabellarisch protokolliert.
   - Optional kann eine Missingness-Matrix über `missingno` erzeugt werden.

---

### Output

- `preprocessing/outputs/caritas_kinderbetreuung_ooe.csv`

Dieses CSV enthält die kirchlichen Einrichtungen in einem Schema, das mit den anderen Trägerdaten kompatibel ist und direkt für das Matching genutzt werden kann.

---

### Ausführung

Im Projektroot:

```bash
(.venv) python3 preprocessing/caritas_pdf_extraction.py
```

## 4. Merge & Enrichment (`kbbe_merge.py`)

**Zweck:**  
Zusammenführung des bereinigten OGD-Datensatzes mit allen Scraping-Quellen sowie dem Caritas-PDF, um zusätzliche Informationen (z. B. Träger, Öffnungszeiten, Anmeldelinks) in den OGD-Bestand zu integrieren.

---

### Datenbasis

Eingabedateien im Ordner `preprocessing/outputs/`:

- `ogd_preprocessed.csv`
- `linz_kinderbetreuung_stadt.csv`
- `kinderfreunde_kinderbetreuung_ooe.csv`
- `familienbund_kinderbetreuung_ooe.csv`
- `caritas_kinderbetreuung_ooe.csv`

Diese werden zunächst in einen gemeinsamen Scraping-Datensatz (`kbbes_scraped_merged.csv`) überführt und anschließend zur Anreicherung des OGD genutzt.

---

### Matching-Strategie

Die Zusammenführung erfolgt über einen mehrstufigen Matching-Prozess:

1. **Normalisierung zentraler Felder**
   - PLZ (`plz_norm`)
   - Straße (`strasse_norm`)
   - Ort (`ort_norm`)
   - Einrichtungstyp (`art_norm`)
   - Telefonnummer (`telefon_norm`, nur Ziffern)
   - E-Mail (`email_norm`, Kleinschreibung)
   - Name (`name_norm`, für Fuzzy-Matching)

2. **E-Mail-Matching (stark, 1:1)**
   - Exakte Übereinstimmung normalisierter E-Mails.
   - Nur akzeptiert, wenn E-Mail in beiden Datensätzen jeweils genau einmal vorkommt.

3. **Telefon-Matching (stark, 1:1)**
   - Exaktes Matching der normalisierten Telefonnummer.
   - Gleiche 1:1-Bedingung wie bei E-Mails.

4. **Fuzzy-Matching auf dem Namen (token_set_ratio)**
   - Blocking auf PLZ, Ort und Einrichtungstyp → geringere Fehl-Matches.
   - Score-Schwelle: **≥ 90**
   - Pro OGD-Eintrag wird maximal **ein** Scraping-Eintrag übernommen.

5. **Enrichment**
   Aus dem Scraping-Datensatz werden zusätzliche Felder an den OGD-Datensatz angehängt, u. a.:

   - `traeger`
   - `oeffnungszeiten`
   - `beschreibung`
   - `kosten`
   - `schliesstage`
   - `vormerkung_form_url` oder `anmeldung_url`
   - weitere strukturierte oder textuelle Zusatzinformationen

   Zusätzlich werden Matching-Regel (`match_rule`) und Score (`score`) dokumentiert.

---

### Output-Dateien

Alle Ergebnisse werden in `preprocessing/outputs/` gespeichert:

- **`kbbes_scraped_merged.csv`**  
  Zusammengeführte Scraping-Daten aller Träger (Kontrollbasis).

- **`ogd_enriched.csv`**  
  → Finaler, angereicherter Datensatz für RAG, SQL und den Chatbot.

- **`kbbes_links.csv`**  
  → Dokumentation aller Matches inkl. verwendeter Matching-Regel und Score.

---

### Ausführung

Im Projektroot:

```bash
(.venv) python3 preprocessing/kbbe_merge.py
```

## 5. Voraussetzungen & Installation

Für die komplette Preprocessing-Pipeline wird eine Python-Umgebung mit mehreren externen Bibliotheken benötigt.  
Alle Abhängigkeiten sind in der Datei `requirements.txt` hinterlegt.

### Benötigte Python-Version

- **Python 3.10 oder höher** (empfohlen)

---

### Notwendige Python-Pakete

**Kernbibliotheken:**
- `pandas`
- `numpy`

**Web-Scraping:**
- `requests`
- `beautifulsoup4`
- `tqdm`

**Datenqualität & Visualisierung:**
- `matplotlib`
- `missingno`

**PDF-Verarbeitung:**
- `tabula-py`  
  → benötigt eine funktionierende Java Runtime (JRE/JDK)

**Matching & Fuzzy-Matching:**
- `rapidfuzz`

**Sonstige Projektabhängigkeiten:**
- `openai`, `langchain` (nur relevant für spätere Pipeline-Schritte)

---

### Installation der Umgebung

1. Virtuelle Umgebung erstellen:

```bash
python3 -m venv .venv
```

2. Aktivieren

```bash
# macOS / Linux
source .venv/bin/activate

# Windows
.venv\Scripts\activate
```

3. Paketinstallation
```bash
(.venv) python3 -m pip install -r requirements.txt
```
