#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Extraktion der kirchlichen KBBEs aus dem Caritas-PDF.

Dieses Skript:

- liest das PDF
  "Liste_der_kirchlichen_Kinderbildungs-_und_-betreuungseinrichtungen.pdf"
  aus dem Ordner raw_data/,
- extrahiert alle Tabellen mit tabula-py,
- parst die Zeilen in ein einheitliches Schema,
- leitet Einrichtungstyp (art) und Träger (traeger) ab,
- bereinigt Ortsnamen,
- speichert das Ergebnis als
  preprocessing/outputs/caritas_kinderbetreuung_ooe.csv.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Dict, List, Optional

import matplotlib.pyplot as plt
import missingno as msno
import pandas as pd
import tabula

# ---------------------------------------------------------------------------
# Projektpfade & Konstanten
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_DIR = PROJECT_ROOT / "raw_data"
PREPROCESSING_DIR = PROJECT_ROOT / "preprocessing"
OUTPUT_DIR = PREPROCESSING_DIR / "outputs"

PDF_FILENAME = "Liste_der_kirchlichen_Kinderbildungs-_und_-betreuungseinrichtungen.pdf"
PDF_PATH = RAW_DATA_DIR / PDF_FILENAME

OUTPUT_CSV = OUTPUT_DIR / "caritas_kinderbetreuung_ooe.csv"

# ---------------------------------------------------------------------------
# Extraktions- und Bereinigungsfunktionen
# ---------------------------------------------------------------------------


def extract_tables_from_pdf(pdf_path: Path) -> List[pd.DataFrame]:
    """Liest alle Tabellentseiten aus dem PDF mit tabula-py.

    Args:
        pdf_path: Pfad zur PDF-Datei.

    Returns:
        Liste von DataFrames (eine oder mehrere Tabellen pro Seite).

    Raises:
        FileNotFoundError: Falls die PDF-Datei nicht existiert.
    """
    if not pdf_path.is_file():
        msg = f"PDF-Datei nicht gefunden: {pdf_path}"
        logging.error(msg)
        raise FileNotFoundError(msg)

    logging.info("Lese Tabellen aus PDF: %s", pdf_path)
    tables: List[pd.DataFrame] = tabula.read_pdf(
        str(pdf_path),
        pages="all",
        lattice=True,        # Tabellen mit Linien
        multiple_tables=True,
    )
    logging.info("Anzahl extrahierter Tabellen: %d", len(tables))
    return tables


def parse_caritas_tables(tables: List[pd.DataFrame]) -> pd.DataFrame:
    """Parst die extrahierten PDF-Tabellen in ein Rohschema.

    Das erwartete Schema pro Zeile:
    - name, ort, plz, strasse, email, telefon, bezirk

    Args:
        tables: Liste von Tabellen-DataFrames aus tabula.read_pdf.

    Returns:
        DataFrame mit Rohdaten.
    """
    records: List[Dict[str, Optional[str]]] = []

    for tbl in tables:
        # komplett leere Zeilen entfernen
        tbl = tbl.dropna(how="all")
        if tbl.empty:
            continue

        # 1) Bezirk aus Spaltenüberschrift (z.B. "Bezirk Braunau")
        current_bezirk: Optional[str] = None
        for col in tbl.columns:
            if isinstance(col, str) and col.strip().startswith("Bezirk"):
                current_bezirk = col.replace("Bezirk", "").strip()
                break

        # 2) Zeilenweise durchgehen
        for _, row in tbl.iterrows():
            vals = [v for v in row.tolist() if pd.notna(v)]
            if not vals:
                continue

            first = str(vals[0]).strip()

            # Überschrift des PDFs ignorieren
            if first.startswith("Kirchliche KBBEs"):
                continue

            # Bezirkszeile im Zeilentext -> Bezirk aktualisieren
            if first.startswith("Bezirk "):
                current_bezirk = first.replace("Bezirk", "").strip()
                continue

            # Kopfzeilen („Ort“, „PLZ“, …) überspringen
            if first in ("Ort", "PLZ", "Straße", "E-Mail", "Telefon"):
                continue

            # Auf 6 Felder begrenzen / auffüllen
            while len(vals) < 6:
                vals.append(None)

            name, ort, plz, strasse, email, telefon = vals[:6]

            # PLZ säubern
            plz_str: Optional[str] = None
            if plz is not None:
                try:
                    plz_str = str(int(float(plz)))
                except Exception:
                    plz_str = str(plz).strip()

            rec: Dict[str, Optional[str]] = {
                "name": str(name).strip(),
                "ort": str(ort).strip() if ort is not None else None,
                "plz": plz_str,
                "strasse": str(strasse).strip() if strasse is not None else None,
                "email": str(email).strip() if email is not None else None,
                "telefon": str(telefon).strip() if telefon is not None else None,
                "bezirk": current_bezirk,
            }
            records.append(rec)

    df_raw = pd.DataFrame(records)
    logging.info("Rohdaten aus PDF: %d Zeilen, %d Spalten", *df_raw.shape)
    return df_raw


def infer_type(name: str) -> str:
    """Leitet den Einrichtungstyp aus dem Namen ab."""
    n = str(name).lower()
    if "hort" in n:
        return "hort"
    if "krabbelstube" in n:
        return "krabbelstube"
    if "kindergarten" in n:
        return "kindergarten"
    return "unknown"


def infer_provider(email: str, name: str) -> str:
    """Leitet den Träger aus E-Mail-Domain bzw. Name ab."""
    e = (email or "").lower()
    n = (name or "").lower()
    if "caritas-ooe.at" in e:
        return "Caritas"
    if "pfarrcaritas-kita.at" in e:
        return "Pfarrcaritas"
    if "vffb.or.at" in e:
        return "VFFB"
    if "kreuzschwestern" in n or "kreuzschwestern" in e:
        return "Kreuzschwestern"
    if "ordens" in n:
        return "Ordens- bzw. Schulverein"
    return "Kirchlich (sonst.)"


def clean_city(city: Optional[str], street: Optional[str]) -> Optional[str]:
    """Bereinigt Ortsnamen, aus denen versehentlich Straßenteile ins Feld rutschten.

    Heuristik:
    - Wenn Tokens im Ortsnamen wie 'Straße', 'Strasse', 'weg', 'gasse', 'platz'
      vorkommen und diese bereits in der Straße enthalten sind,
      werden sie abgeschnitten.
    """
    if not isinstance(city, str) or not isinstance(street, str):
        return city

    tokens = city.split()
    street_lower = street.lower().replace("ß", "ss")

    cut_idx = None
    for i, tok in enumerate(tokens):
        low = tok.lower()
        if any(
            key in low for key in ["str.", "straße", "strasse", "weg", "gasse", "platz"]
        ):
            stem = re.sub(r"[^a-zäöüß]", "", low).replace("ß", "ss")
            if stem and stem in street_lower:
                cut_idx = i
                break

    if cut_idx is not None and cut_idx > 0:
        return " ".join(tokens[:cut_idx])

    return city


def build_caritas_dataframe(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Wandelt den Roh-DataFrame in das Standardschema um.

    Args:
        df_raw: Rohdaten aus parse_caritas_tables.

    Returns:
        DataFrame df_caritas mit Spalten:
        art, name, strasse, plz, ort, telefon, email, traeger, bezirk
    """
    df = df_raw.copy()

    # Einrichtungstyp & Träger ableiten
    df["art"] = df["name"].apply(infer_type)
    df["traeger"] = df.apply(
        lambda r: infer_provider(r.get("email", None), r.get("name", None)),
        axis=1,
    )

    df_caritas = pd.DataFrame(
        {
            "art": df["art"],
            "name": df["name"],
            "strasse": df["strasse"],
            "plz": df["plz"],
            "ort": df["ort"],
            "telefon": df["telefon"],
            "email": df["email"],
            "traeger": df["traeger"],
            "bezirk": df["bezirk"],
        }
    )

    # offensichtliche Schreibfehler / Zusammenschreibungen glätten
    df_caritas["name"] = (
        df_caritas["name"]
        .astype(str)
        .str.replace(
            "Pfarrcaritaskindergarten",
            "Pfarrcaritas Kindergarten",
            regex=False,
        )
        .str.replace(
            "Pfarrcaritaskrabbelstube",
            "Pfarrcaritas Krabbelstube",
            regex=False,
        )
        .str.replace("Pfarrcaritashort", "Pfarrcaritas Hort", regex=False)
    )

    # Ortsnamen bereinigen
    df_caritas["ort"] = df_caritas.apply(
        lambda r: clean_city(r["ort"], r["strasse"]),
        axis=1,
    )

    logging.info("df_caritas: %d Zeilen, %d Spalten", *df_caritas.shape)
    return df_caritas


def log_missingness(df: pd.DataFrame, name: str) -> None:
    """Loggt fehlende Werte (Anzahl und Prozentsatz) und optional eine Matrix."""
    logging.info("Missingness-Analyse für %s", name)

    missing_counts = df.isna().sum().sort_values(ascending=False)
    total_rows = len(df)
    missing_percent = (missing_counts / max(total_rows, 1) * 100).round(2)

    missing_df = pd.DataFrame(
        {"Anzahl": missing_counts, "Prozentsatz (%)": missing_percent}
    )
    logging.info("Fehlende Werte (%s):\n%s", name, missing_df)

    # Optional: Missingness-Matrix (kann bei Bedarf auskommentiert werden)
    try:
        msno.matrix(df)
        plt.tight_layout()
        plt.show()
    except Exception as exc:  # z.B. wenn kein Display verfügbar ist
        logging.warning("Missingno-Plot konnte nicht erzeugt werden: %s", exc)


# ---------------------------------------------------------------------------
# Hauptlogik
# ---------------------------------------------------------------------------


def main() -> int:
    """Hauptfunktion: PDF einlesen, extrahieren, bereinigen, CSV speichern."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    logging.info("Projektroot: %s", PROJECT_ROOT)
    logging.info("PDF-Pfad: %s", PDF_PATH)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logging.info("Output-Verzeichnis: %s", OUTPUT_DIR)

    # 1) Tabellen aus dem PDF extrahieren
    tables = extract_tables_from_pdf(PDF_PATH)

    if not tables:
        logging.error("Keine Tabellen im PDF gefunden.")
        return 1

    # 2) Rohdaten parsen
    df_raw = parse_caritas_tables(tables)

    # 3) In Standardschema mappen und bereinigen
    df_caritas = build_caritas_dataframe(df_raw)

    # 4) Deskriptive Missingness-Statistik
    log_missingness(df_caritas, "df_caritas")

    # 5) CSV speichern
    df_caritas.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    logging.info("Caritas-Daten gespeichert unter: %s", OUTPUT_CSV)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
