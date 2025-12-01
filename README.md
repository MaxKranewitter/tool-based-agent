# Agentic RAG Chatbot für Kinderbetreuung in Oberösterreich

Dieses Projekt entwickelt einen tool-basierten Single-Agent Chatbot ("LEO"), der Bürger*innen in Oberösterreich bei Fragen zur Kinderbetreuung unterstützt.  
Der Chatbot kombiniert Informationen aus:

- dem offiziellen OGD-Datensatz (`kbbes.csv`)
- gescrapten Webseiten der großen Träger (Stadt Linz, Kinderfreunde, Familienbund)
- einem kirchlichen PDF-Verzeichnis der Caritas/Pfarrcaritas
- einer SQL-Datenbank mit strukturierten Einrichtungseinträgen
- einem Vektorindex für unstrukturierte Dokumente (RAG)

Der Chatbot ist technisch so aufgebaut, dass SQL-Daten, Retrieval-Kontext und Websuche (Fallback) gemeinsam genutzt werden können.

---

## Funktionsübersicht

- Tool-basierter Agent mit SQL-, Vektorstore- und Websuch-Tools
- Extraktion und Kombination strukturierter und unstrukturierter Daten
- Streamlit-Frontend inkl.:
-   Chatoberfläche
-   Anzeige von Einrichtungen
-   Vormerkfunktion („unverbindlich vormerken“)
- Evaluierungs-Pipeline via LangSmith (LLM-as-a-Judge)
- Ca. 150 Testfragen in drei Evaluierungs-Buckets (SQL, RAG, Web)

## Projektübersicht

```text
agentic_rag_chatbot/
├── app.py                      # Streamlit-Frontend des Chatbots
├── backend/
│   ├── agent.py                # Tool-basierter Single-Agent (SQL + RAG + Web)
│   ├── sql_db.py               # SQL-Zugriff & Datenbank-Funktionen
│   └── utils/                  # Helper (z.B. Cleaning)
│
├── preprocessing/              # Preprocessing-Pipeline (OGD, Scraping, PDFs)
│   └── README_preprocessing.md
│
├── rag/                        # Vektorstore-Pipeline (Dokumente, Embeddings)
│
├── eval/                       # Testsets & LangSmith Evaluierungen
│   ├── extended_eval_langsmith.py
│   └── sql_search.csv, web_search_eval.csv, rag_eval.csv
│
├── data/                       # Statische Inhalte (optional)
├── requirements.txt            # Python Dependencies
└── README.md
```

---

## Komponenten

### 1. Datenvorverarbeitung (Preprocessing)

Alle Schritte zur Datenaufbereitung befinden sich im Ordner:

```bash
preprocessing
```

Die vollständige Dokumentation der Pipeline steht in:
```bash
preprocessing/README_preprocessing.md
```

Dort beschrieben sind:

- Bereinigung des OGD-Datensatzes  
- Web-Scraping der wichtigsten Träger  
- Extraktion kirchlicher Einrichtungen (Caritas/Pfarrcaritas)  
- Matching & Enrichment  
- Erstellung des finalen Datensatzes `ogd_enriched.csv`

Dieser Datensatz bildet die Grundlage für SQL, RAG und den Chatbot.

---

### 2. Backend / Agent

Das Backend besteht aus einem tool-basierten Single-Agent, implementiert in:

```bash
backend/agent.py
```

Der Agent nutzt drei Tools:
- SQL-Tool: Echtzeit-Abfragen auf den Einrichtungsdaten
- Vektorstore: RAG-Retrieval aus PDFs, Sozialratgeber, Leitfäden
- Websuche: Fallback bei fehlenden Datenquellen

Die Tools werden über OpenAI Responses automatisch vom Modell angesteuert.

---

### 3. Streamlit-Frontend 

Die App wird gestartet über:

```bash
streamlit run app.py
```

Features:
- Chatoberfläche
- Kontextanzeige im Backend
- Vormerkfunktion (SQL-Schreiboperation)
- visuelle Darstellung von Einrichtungen

---

### 4. Evaluierung (LangSmith)

Die Evaluierung besteht aus:
- 150 Testfragen
- Aufgeteilt in 3 Buckets:
-   SQL
-   Vektorstore (RAG)
-   Websuche

Die automatische Bewertung erfolgt mittels drei Metriken:
- Correctness
- Faithfulness
- Context Relevance
- Inconclusive Behavior

Ausführbar über
```bash
python eval/extended_eval_langsmith.py
```

Auswertung erfolgt in LangSmith.

---

## Hintergrund 

Dieses Projekt wurde im Rahmen des Care4Work-Forschungsprojekts entwickelt und dient als Prototyp zur Verbesserung des Informationszugangs für Eltern in Oberösterreich.
Die Ergebnisse wurden im Rahmen einer Masterarbeit an der FH Oberösterreich dokumentiert und evaluiert.

---

## Weiterentwicklung / Ausblick
- Multi-Agent-Architektur
- Größerer und strukturierterer Dokumentenkorpus
- Verbesserte SQL-Schicht
- Webscraping-Pipeline erweitern
- Live-Daten für Platzverfügbarkeiten
- Robustere Mehrsprachigkeit
- UX-Tests mit Eltern, Gemeinden und Trägern

