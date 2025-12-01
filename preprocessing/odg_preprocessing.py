#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Preprocess OGD childcare dataset (kbbes.csv) for the LEO chatbot.

Dieses Skript ist für die lokale VS-Code-Umgebung gedacht und
benötigt **kein** Google Colab / Google Drive.

Funktionen:
- lädt den OGD-Datensatz aus raw_data/kbbes.csv,
- bereinigt Spaltennamen, Telefonnummern und URLs,
- mappt 'art' und 'bezirk' auf sprechende Labels,
- entfernt Testzeilen,
- visualisiert Missingness (optional),
- speichert den bereinigten Datensatz nach preprocessing/outputs/.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import missingno as msno
import pandas as pd

# ---------------------------------------------------------------------------
# Pfade & Konfiguration
# ---------------------------------------------------------------------------

# Dieses File liegt in: <PROJECT_ROOT>/preprocessing/ogd_preprocessing.py
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "raw_data"
PREPROCESSING_DIR = PROJECT_ROOT / "preprocessing"
OUTPUT_DIR = PREPROCESSING_DIR / "outputs"

INPUT_FILE = DATA_DIR / "kbbes.csv"
OUTPUT_FILE = OUTPUT_DIR / "ogd_preprocessed.csv"

# ---------------------------------------------------------------------------
# URL-Mapping (manuell gepflegt)
# ---------------------------------------------------------------------------

URL_MAPPING = {
    "https://www.steyr.gv.at/einrichtungen/soziale_einrichtungen/"
    "kindergaerten_und_horte": (
        "https://www.steyr.at/Leben/Familie_Kinder/"
        "Krabbelstuben_Kindergaerten_und_Horte"
    ),
    "https://st-anna-steyr.at/start-hort": "https://hort.st-anna-steyr.at/",
    "http://st-anna-steyr.at/start-hort": "https://hort.st-anna-steyr.at/",
    "st-anna-steyr.at/start-hort": "https://hort.st-anna-steyr.at/",
    "https://www.fwsl.at/waldorfkindergarten-sued": "https://waldorf-linz.at/",
    "https://www.fwsl.at/waldorfkindergarten-nord": "https://waldorf-linz.at/",
    "https://www.junges-wohnen.at/hort": "https://www.junges-wohnen.at/",
    "https://www.kidsandcompany.at/wordpress": (
        "https://www.kidsandcompany-steyr.at/"
    ),
    "https://www.ooe.lebenshilfe.org/lebenshilfe": (
        "https://ooe.lebenshilfe.org/standorte/kindergaerten/"
        "kindergarten-kindergarten-steyr-gleink"
    ),
    "https://www.davinciakademie.at/home": "https://www.davinciakademie.at/",
    "https://www.ekiz-uttendorf.at/index.php/krabbelstube.html": (
        "http://www.ekiz-uttendorf.at/index.php/"
    ),
    "https://www.stpantaleon.at/gemeinde/kinderbetreuung": (
        "https://www.stpantaleon.at/Politik_Verwaltung/Schule_Bildung/"
        "Kindergarten_Krabbelstube"
    ),
    "https://www.rossbach.at/krabbelstube_rossbach_-_st_veit_8": (
        "https://www.rossbach.at/Unser_Rossbach/Kinderbetreuung..."
        "_Rossbach_-_St_Veit/Kontakt_und_Aufnahme/Kontakt_und_Aufnahme"
    ),
}

ART_MAPPING = {
    "KG": "Kindergarten",
    "KS": "Krabbelstube",
    "HO": "Hort",
    "SOF": "Sonstige Form der Kinderbetreuung",
}

BEZIRK_MAPPING = {
    401: "Linz (Stadt)",
    402: "Steyr (Stadt)",
    403: "Wels (Stadt)",
    404: "Braunau am Inn",
    405: "Eferding",
    406: "Freistadt",
    407: "Gmunden",
    408: "Grieskirchen",
    409: "Kirchdorf an der Krems",
    410: "Linz-Land",
    411: "Perg",
    412: "Ried im Innkreis",
    413: "Rohrbach",
    414: "Schärding",
    415: "Steyr-Land",
    416: "Urfahr-Umgebung",
    417: "Vöcklabruck",
    418: "Wels-Land",
}

# ---------------------------------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------------------------------


def plot_missingness(df: pd.DataFrame, title: Optional[str] = None) -> None:
    """Visualisiert fehlende Werte in einem DataFrame.

    Args:
        df: DataFrame, dessen Missingness visualisiert werden soll.
        title: Optionaler Titel für die Grafik.
    """
    if title:
        plt.title(title)
    msno.matrix(df)
    plt.show()


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Bereinigt Spaltennamen (Trimmen und Kleinschreibung).

    Args:
        df: Ursprünglicher DataFrame.

    Returns:
        DataFrame mit bereinigten Spaltennamen.
    """
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()
    return df


def clean_phone_column(df: pd.DataFrame, column: str = "telefon") -> pd.DataFrame:
    """Bereinigt die Telefonnummernspalte.

    - Entfernt Leerzeichen.
    - Wandelt 'nan' wieder in leere Strings um.

    Args:
        df: Eingangs-DataFrame.
        column: Name der Telefonspalte.

    Returns:
        DataFrame mit bereinigter Telefonspalte.
    """
    df = df.copy()
    df[column] = (
        df[column]
        .astype(str)
        .str.replace(r"\s+", "", regex=True)  # alle Leerzeichen entfernen
        .str.replace(r"^nan$", "", regex=True)  # 'nan' wieder zu leer
    )
    return df


def normalize_weburl_column(df: pd.DataFrame, column: str = "weburl") -> pd.DataFrame:
    """Normalisiert die Web-URLs (Trimmen, Kleinschreibung, www-Präfix).

    Args:
        df: Eingangs-DataFrame.
        column: Name der Spalte mit URLs.

    Returns:
        DataFrame mit bereinigter URL-Spalte.
    """
    df = df.copy()
    df[column] = (
        df[column]
        .astype(str)
        .str.strip()
        .str.replace(r"^nan$", "", regex=True)
        .str.lower()
        .str.replace(r"^www\.", "https://www.", regex=True)
    )
    return df


def map_art_column(df: pd.DataFrame, column: str = "art") -> pd.DataFrame:
    """Mappt Kurzbezeichnungen in der Spalte 'art' auf Langformen.

    Args:
        df: Eingangs-DataFrame.
        column: Name der Spalte mit der Art der Einrichtung.

    Returns:
        DataFrame mit gemappter 'art'-Spalte.
    """
    df = df.copy()
    df[column] = df[column].map(ART_MAPPING)
    return df


def map_bezirk_column(df: pd.DataFrame, column: str = "bezirk") -> pd.DataFrame:
    """Mappt Bezirkskennzahlen auf Bezirksnamen.

    Args:
        df: Eingangs-DataFrame.
        column: Name der Spalte mit der Bezirkskennzahl.

    Returns:
        DataFrame mit Bezirksnamen.
    """
    df = df.copy()
    # robust konvertieren, falls es doch mal NaNs gibt
    df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
    df[column] = df[column].map(BEZIRK_MAPPING)
    return df


def apply_url_mapping(df: pd.DataFrame, column: str = "weburl") -> pd.DataFrame:
    """Bereinigt und aktualisiert Web-URLs über ein manuell gepflegtes Mapping.

    Args:
        df: Eingangs-DataFrame.
        column: Name der Spalte mit URLs.

    Returns:
        DataFrame mit aktualisierten URLs.
    """
    df = df.copy()
    df[column] = df[column].astype(str).str.strip()
    df[column] = df[column].replace(URL_MAPPING)
    return df


def clean_ogd_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Führt alle Bereinigungsschritte für den OGD-Datensatz aus.

    Args:
        df: Roh-DataFrame aus 'kbbes.csv'.

    Returns:
        Bereinigter DataFrame.
    """
    df = clean_column_names(df)
    df = clean_phone_column(df, column="telefon")
    df = normalize_weburl_column(df, column="weburl")
    df = map_art_column(df, column="art")
    df = map_bezirk_column(df, column="bezirk")
    df = apply_url_mapping(df, column="weburl")

    # Zeilen mit "TEST" im Namen entfernen
    df = df[~df["name"].str.contains("TEST", case=False, na=False)].copy()

    return df


# ---------------------------------------------------------------------------
# Hauptlogik
# ---------------------------------------------------------------------------


def main() -> int:
    """Hauptfunktion: lädt, bereinigt und speichert den OGD-Datensatz."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    logging.info("Projektroot: %s", PROJECT_ROOT)
    logging.info("Rohdaten-Verzeichnis: %s", DATA_DIR)
    logging.info("Input-Datei: %s", INPUT_FILE)

    if not INPUT_FILE.is_file():
        logging.error("Input-Datei existiert nicht: %s", INPUT_FILE)
        return 1

    # Daten laden
    df_raw = pd.read_csv(INPUT_FILE)

    logging.info("Form (Zeilen, Spalten) vor Cleaning: %s", df_raw.shape)
    logging.info("Spaltennamen: %s", list(df_raw.columns))

    logging.info("Fehlende Werte pro Spalte (Top 10):\n%s",
                 df_raw.isna().sum().sort_values(ascending=False).head(10))

    # Missingness-Matrix vor dem Cleaning (optional; kann auskommentiert werden)
    # plot_missingness(df_raw, title="Missingness vor dem Cleaning")

    # Bereinigung
    df = clean_ogd_dataset(df_raw)

    logging.info("Form nach Cleaning: %s", df.shape)
    logging.info("Bezirkswerte nach Mapping (unique): %s",
                 sorted(df["bezirk"].dropna().unique().tolist()))

    # Missingness-Matrix nach dem Cleaning (optional)
    # plot_missingness(df, title="Missingness nach dem Cleaning")

    logging.info("Vorschau auf den bereinigten DataFrame:\n%s", df.head())

    # Output-Verzeichnis anlegen und CSV schreiben
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)

    logging.info("Bereinigter Datensatz gespeichert unter: %s", OUTPUT_FILE)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
