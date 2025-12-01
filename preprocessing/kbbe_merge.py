#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Merge & Enrichment von OGD-KBBE mit gescrapten Trägerdaten.

Dieses Skript:

1. Lädt die gescrapten CSV-Dateien:
   - linz_kinderbetreuung_stadt.csv
   - kinderfreunde_kinderbetreuung_ooe.csv
   - familienbund_kinderbetreuung_ooe.csv
   - caritas_kinderbetreuung_ooe.csv

2. Fügt sie zu einem gemeinsamen Scraping-Datensatz `df_all` zusammen
   und speichert ihn als:
   - kbbes_scraped_merged.csv

3. Lädt den bereinigten OGD-Datensatz:
   - ogd_preprocessed.csv

4. Führt ein mehrstufiges Matching durch:
   - exakte E-Mail-Matches (1:1)
   - exakte Telefon-Matches (1:1)
   - Fuzzy-Matching auf dem Namen mit Blocking (PLZ, Ort, Art)

5. Enriched den OGD-Datensatz mit Zusatzinformationen aus dem Scraping
   und speichert:
   - ogd_enriched.csv
   - optional: Links-Tabelle kbbes_links.csv für Debugging.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
from rapidfuzz import fuzz

# ---------------------------------------------------------------------------
# Projektpfade & Dateinamen
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PREPROCESSING_DIR = PROJECT_ROOT / "preprocessing"
OUTPUT_DIR = PREPROCESSING_DIR / "outputs"

SCRAPED_LINZ = OUTPUT_DIR / "linz_kinderbetreuung_stadt.csv"
SCRAPED_KF = OUTPUT_DIR / "kinderfreunde_kinderbetreuung_ooe.csv"
SCRAPED_FB = OUTPUT_DIR / "familienbund_kinderbetreuung_ooe.csv"
SCRAPED_CARITAS = OUTPUT_DIR / "caritas_kinderbetreuung_ooe.csv"

SCRAPED_MERGED = OUTPUT_DIR / "kbbes_scraped_merged.csv"
OGD_CLEAN = OUTPUT_DIR / "ogd_preprocessed.csv"
OGD_ENRICHED = OUTPUT_DIR / "ogd_enriched.csv"
LINKS_PATH = OUTPUT_DIR / "kbbes_links.csv"

# Spalten, die aus df_all an den OGD-Datensatz "angeflanscht" werden sollen
ENRICH_COLS: List[str] = [
    "traeger",
    "leiter_in",
    "contact_name",
    "vormerkung_form_url",
    "anmeldung_url",
    "anmeldung_krabbelstube_url",
    "anmeldung_kindergarten_url",
    "oeffnungszeiten",
    "gruppen",
    "plaetze",
    "schliesstage",
    "kosten",
    "beschreibung",
    "lage",
    "verkehrsanbindung",
    "gebaeude",
    "angebot_art_label",
]


# ---------------------------------------------------------------------------
# Hilfsfunktionen: Laden, Normalisierung
# ---------------------------------------------------------------------------


def load_csv(path: Path, name: str) -> pd.DataFrame:
    """Lädt ein CSV und wirft bei Nichtvorhandensein einen klaren Fehler.

    Args:
        path: Pfad zur CSV-Datei.
        name: Logischer Name (für Logging).

    Returns:
        Eingelesener DataFrame.
    """
    if not path.is_file():
        msg = f"{name}-Datei nicht gefunden unter: {path}"
        logging.error(msg)
        raise FileNotFoundError(msg)
    logging.info("Lade %s aus %s", name, path)
    return pd.read_csv(path)


def norm_plz(x: object) -> Optional[str]:
    """Normalisiert Postleitzahlen auf String ohne führende Nullen-Verlust."""
    if pd.isna(x):
        return None
    try:
        return str(int(float(x))).strip()
    except Exception:
        return str(x).strip()


def norm_strasse(s: object) -> Optional[str]:
    """Normalisiert Straßennamen (ß->ss, Straße->Strasse, Kleinbuchstaben)."""
    if not isinstance(s, str):
        return None
    s = s.strip()
    s = s.replace("ß", "ss")
    s = (
        s.replace("Straße", "Strasse")
        .replace("straße", "strasse")
        .replace("Str.", "Strasse")
        .replace("str.", "strasse")
    )
    s = re.sub(r"\s+", " ", s)
    return s.lower()


def norm_hausnr(h: object) -> Optional[str]:
    """Normalisiert Hausnummern (Whitespace entfernen, Kleinbuchstaben)."""
    if pd.isna(h):
        return None
    h_str = str(h).strip().replace(" ", "").lower()
    return h_str or None


def norm_city(c: object) -> Optional[str]:
    """Normalisiert Ortsnamen (Whitespace, Kleinbuchstaben)."""
    if not isinstance(c, str):
        return None
    return re.sub(r"\s+", " ", c.strip()).lower()


def norm_art(a: object) -> Optional[str]:
    """Normalisiert Einrichtungstypen auf {hort, krabbelstube, kindergarten}."""
    if not isinstance(a, str):
        return None
    a_low = a.lower()
    if "hort" in a_low:
        return "hort"
    if "krabbelstube" in a_low or "kleinkind" in a_low:
        return "krabbelstube"
    if "kindergarten" in a_low:
        return "kindergarten"
    return a_low


def norm_phone(p: object) -> Optional[str]:
    """Extrahiert nur Ziffern aus Telefonnummern (für exakte Telefon-Matches)."""
    if not isinstance(p, str):
        p = str(p) if not pd.isna(p) else None
    if p is None:
        return None
    digits = re.sub(r"\D", "", p)
    return digits or None


def norm_email(e: object) -> Optional[str]:
    """Normalisiert E-Mail-Adressen (Trim + Kleinbuchstaben)."""
    if not isinstance(e, str):
        return None
    return e.strip().lower() or None


def norm_url(u: object) -> Optional[str]:
    """Normalisiert URLs (ohne Schema, ohne Trailing Slash, Kleinbuchstaben)."""
    if not isinstance(u, str):
        return None
    u = u.strip().lower()
    u = re.sub(r"^https?://", "", u)
    u = u.rstrip("/")
    return u or None


def norm_name(s: object) -> Optional[str]:
    """Normalisiert Einrichtungsnamen für Fuzzy-Matching."""
    if not isinstance(s, str):
        return None
    s = s.lower().strip()
    return re.sub(r"\s+", " ", s) or None


# ---------------------------------------------------------------------------
# Matching-Schritte
# ---------------------------------------------------------------------------


def compute_normalized_keys(df: pd.DataFrame) -> pd.DataFrame:
    """Erzeugt Normalisierungs-Spalten für Matching.

    Args:
        df: Eingabedatensatz.

    Returns:
        DataFrame mit zusätzlichen *_norm-Spalten.
    """
    df = df.copy()
    df["plz_norm"] = df.get("plz").apply(norm_plz)
    df["strasse_norm"] = df.get("strasse").apply(norm_strasse)
    df["hausnr_norm"] = df.get("hausnr").apply(norm_hausnr) if "hausnr" in df else None
    df["ort_norm"] = df.get("ort").apply(norm_city)
    df["art_norm"] = df.get("art").apply(norm_art)
    df["telefon_norm"] = df.get("telefon").apply(norm_phone)
    df["email_norm"] = df.get("email").apply(norm_email)
    df["weburl_norm"] = df.get("weburl").apply(norm_url)
    df["name_norm"] = df.get("name").apply(norm_name)
    return df


def find_strong_email_matches(df_all: pd.DataFrame, df_ogd: pd.DataFrame) -> pd.DataFrame:
    """Findet eindeutige 1:1-E-Mail-Matches zwischen df_all und df_ogd.

    Args:
        df_all: Scraping-Datensatz.
        df_ogd: OGD-Datensatz.

    Returns:
        DataFrame mit Spalten idx_all, idx_ogd, email_norm, match_rule, score.
    """
    df_all_email = df_all[~df_all["email_norm"].isna()].copy()
    df_ogd_email = df_ogd[~df_ogd["email_norm"].isna()].copy()

    df_all_email["idx_all"] = df_all_email.index
    df_ogd_email["idx_ogd"] = df_ogd_email.index

    email_matches = df_all_email.merge(
        df_ogd_email,
        on="email_norm",
        how="inner",
        suffixes=("_all", "_ogd"),
    )

    logging.info("Rohzahl E-Mail-Matches: %s", len(email_matches))

    # Häufigkeit je E-Mail in beiden Datensätzen
    cnt_all = df_all_email.groupby("email_norm").size().rename("n_all")
    cnt_ogd = df_ogd_email.groupby("email_norm").size().rename("n_ogd")
    email_counts = pd.concat([cnt_all, cnt_ogd], axis=1).fillna(0).astype(int)

    email_matches = email_matches.merge(email_counts, on="email_norm", how="left")
    email_matches["email_unique_both"] = (email_matches["n_all"] == 1) & (
        email_matches["n_ogd"] == 1
    )

    email_strong = email_matches[email_matches["email_unique_both"]].copy()
    logging.info("Starke E-Mail-Matches (1:1): %s", len(email_strong))

    links_email = email_strong[["idx_ogd", "idx_all", "email_norm"]].copy()
    links_email["match_rule"] = "email_unique"
    links_email["score"] = np.nan
    return links_email


def find_strong_phone_matches(
    df_all_after_email: pd.DataFrame,
    df_ogd_after_email: pd.DataFrame,
) -> pd.DataFrame:
    """Findet eindeutige 1:1-Telefon-Matches im Rest nach E-Mail-Matching."""
    all_tel = df_all_after_email[~df_all_after_email["telefon_norm"].isna()].copy()
    ogd_tel = df_ogd_after_email[~df_ogd_after_email["telefon_norm"].isna()].copy()

    all_tel["idx_all"] = all_tel.index
    ogd_tel["idx_ogd"] = ogd_tel.index

    phone_matches = all_tel.merge(
        ogd_tel,
        on="telefon_norm",
        how="inner",
        suffixes=("_all", "_ogd"),
    )
    logging.info("Telefon-Matches (roh): %s", len(phone_matches))

    cnt_all_tel = all_tel.groupby("telefon_norm").size().rename("n_all_tel")
    cnt_ogd_tel = ogd_tel.groupby("telefon_norm").size().rename("n_ogd_tel")
    tel_counts = pd.concat([cnt_all_tel, cnt_ogd_tel], axis=1).fillna(0).astype(int)

    phone_matches = phone_matches.merge(tel_counts, on="telefon_norm", how="left")
    phone_matches["phone_unique_both"] = (phone_matches["n_all_tel"] == 1) & (
        phone_matches["n_ogd_tel"] == 1
    )

    phone_strong = phone_matches[phone_matches["phone_unique_both"]].copy()
    logging.info("Starke Telefon-Matches (1:1): %s", len(phone_strong))

    links_phone = phone_strong[["idx_ogd", "idx_all", "telefon_norm"]].copy()
    links_phone["match_rule"] = "phone_unique"
    links_phone["score"] = np.nan
    return links_phone


def fuzzy_match_blocked(
    df_left: pd.DataFrame,
    df_right: pd.DataFrame,
    block_cols: Iterable[str] = ("plz_norm", "ort_norm", "art_norm"),
    score_cutoff: int = 90,
) -> pd.DataFrame:
    """Fuzzy-Matching mit Blocking auf Name (token_set_ratio).

    Args:
        df_left: typischerweise df_ogd_after_phone.
        df_right: typischerweise df_all_after_phone.
        block_cols: Spalten für exact blocking (z.B. PLZ, Ort, Art).
        score_cutoff: Mindest-Score für akzeptierte Matches.

    Returns:
        DataFrame mit Spalten idx_ogd, idx_all, score, match_rule.
    """
    df_left = df_left.copy()
    df_right = df_right.copy()

    df_left["idx_left"] = df_left.index
    df_right["idx_right"] = df_right.index

    matches: List[Dict[str, object]] = []
    used_right: set[int] = set()

    for row in df_left.itertuples():
        cand = df_right

        # Blocking
        for col in block_cols:
            val = getattr(row, col)
            if val is None or (isinstance(val, float) and np.isnan(val)):
                continue
            cand = cand[cand[col] == val]
        if cand.empty:
            continue

        best_score = -1
        best_idx_right: Optional[int] = None

        for cand_row in cand.itertuples():
            if row.name_norm is None or cand_row.name_norm is None:
                continue
            score = fuzz.token_set_ratio(row.name_norm, cand_row.name_norm)
            if score > best_score:
                best_score = score
                best_idx_right = cand_row.idx_right

        if best_score >= score_cutoff and best_idx_right not in used_right:
            matches.append(
                {
                    "idx_ogd": row.idx_left,
                    "idx_all": best_idx_right,
                    "score": best_score,
                    "match_rule": "fuzzy_name_plz_ort_art",
                }
            )
            used_right.add(best_idx_right)

    fuzzy_matches = pd.DataFrame(matches)
    logging.info(
        "Fuzzy-Matches (Name + PLZ + Ort + Art, Score >= %d): %s",
        score_cutoff,
        len(fuzzy_matches),
    )
    return fuzzy_matches


# ---------------------------------------------------------------------------
# Enrichment
# ---------------------------------------------------------------------------


def build_enrichment(
    df_all: pd.DataFrame, df_ogd: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Führt alle Matching-Schritte aus und baut Enrichment- und Link-Tabelle.

    Args:
        df_all: Scraping-Datensatz (alle Träger zusammen).
        df_ogd: Bereinigter OGD-Datensatz.

    Returns:
        Tuple (df_ogd_enriched, links), wobei
        - df_ogd_enriched: OGD + angereicherte Spalten + Match-Metadaten
        - links: Tabelle aller Matches (idx_ogd, idx_all, match_rule, score)
    """
    # Normalisierte Keys
    df_all_norm = compute_normalized_keys(df_all)
    df_ogd_norm = compute_normalized_keys(df_ogd)

    # E-Mail-Matches
    links_email = find_strong_email_matches(df_all_norm, df_ogd_norm)

    # Rest nach E-Mail
    idx_all_email = links_email["idx_all"].unique()
    idx_ogd_email = links_email["idx_ogd"].unique()
    df_all_after_email = df_all_norm.loc[~df_all_norm.index.isin(idx_all_email)].copy()
    df_ogd_after_email = df_ogd_norm.loc[~df_ogd_norm.index.isin(idx_ogd_email)].copy()

    logging.info(
        "Rest nach E-Mail-Matching: df_all=%d, df_ogd=%d",
        len(df_all_after_email),
        len(df_ogd_after_email),
    )

    # Telefon-Matches
    links_phone = find_strong_phone_matches(df_all_after_email, df_ogd_after_email)

    # Rest nach Telefon
    idx_all_phone = links_phone["idx_all"].unique()
    idx_ogd_phone = links_phone["idx_ogd"].unique()
    df_all_after_phone = df_all_after_email.loc[
        ~df_all_after_email.index.isin(idx_all_phone)
    ].copy()
    df_ogd_after_phone = df_ogd_after_email.loc[
        ~df_ogd_after_email.index.isin(idx_ogd_phone)
    ].copy()

    logging.info(
        "Rest nach Email+Telefon: df_all=%d, df_ogd=%d",
        len(df_all_after_phone),
        len(df_ogd_after_phone),
    )

    # Fuzzy-Matches
    links_fuzzy = fuzzy_match_blocked(df_ogd_after_phone, df_all_after_phone)

    # Alle Links zusammenführen
    links = pd.concat(
        [
            links_email[["idx_ogd", "idx_all", "match_rule", "score"]],
            links_phone[["idx_ogd", "idx_all", "match_rule", "score"]],
            links_fuzzy[["idx_ogd", "idx_all", "match_rule", "score"]],
        ],
        ignore_index=True,
    )

    logging.info("Anzahl Link-Zeilen gesamt: %d", len(links))
    logging.info("Eindeutige OGD-Indizes: %d", links["idx_ogd"].nunique())
    logging.info("Eindeutige ALL-Indizes: %d", links["idx_all"].nunique())

    # Enrichment aus df_all holen
    missing_enrich_cols = [c for c in ENRICH_COLS if c not in df_all.columns]
    if missing_enrich_cols:
        logging.warning(
            "Folgende ENRICH_COLS fehlen in df_all und werden mit NaN gefüllt: %s",
            missing_enrich_cols,
        )
        for c in missing_enrich_cols:
            if c not in df_all.columns:
                df_all[c] = np.nan

    enrichment = links.merge(
        df_all[ENRICH_COLS],
        left_on="idx_all",
        right_index=True,
        how="left",
    )

    # pro OGD-Einrichtung nur einen Datensatz behalten (falls Dubletten)
    enrichment = enrichment[["idx_ogd", "match_rule", "score"] + ENRICH_COLS].copy()
    enrichment = enrichment.drop_duplicates(subset="idx_ogd")
    enrichment = enrichment.set_index("idx_ogd")

    # Enrichment an df_ogd anhängen (Index muss mit idx_ogd korrespondieren)
    df_ogd_enriched = df_ogd.join(enrichment, how="left")

    logging.info(
        "OGD angereichert: Zeilen=%d, Spalten=%d",
        len(df_ogd_enriched),
        len(df_ogd_enriched.columns),
    )

    return df_ogd_enriched, links


# ---------------------------------------------------------------------------
# Hauptlogik
# ---------------------------------------------------------------------------


def main() -> int:
    """Hauptfunktion: führt Merge & Enrichment aus und speichert CSVs."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logging.info("Projektroot: %s", PROJECT_ROOT)
    logging.info("Output-Verzeichnis: %s", OUTPUT_DIR)

    # 1) Scraping-Datensätze laden
    df_linz = load_csv(SCRAPED_LINZ, "Stadt Linz")
    df_kf = load_csv(SCRAPED_KF, "Kinderfreunde")
    df_fb = load_csv(SCRAPED_FB, "Familienbund")
    df_car = load_csv(SCRAPED_CARITAS, "Caritas/Pfarrcaritas")

    logging.info(
        "Zeilen: Linz=%d, Kinderfreunde=%d, Familienbund=%d, Caritas=%d",
        len(df_linz),
        len(df_kf),
        len(df_fb),
        len(df_car),
    )

    # 2) Scraping-Datensätze zusammenführen
    df_all = pd.concat([df_linz, df_kf, df_fb, df_car], ignore_index=True)
    logging.info("df_all (Scraping gesamt): %d Zeilen, %d Spalten", *df_all.shape)

    # Optional: leere Spalten melden
    for name, df_part in [
        ("Stadt Linz", df_linz),
        ("Kinderfreunde", df_kf),
        ("Familienbund", df_fb),
        ("Caritas/Pfarrcaritas", df_car),
    ]:
        empty_cols = df_part.columns[df_part.isna().all()].tolist()
        logging.info("%s: %d komplett leere Spalten", name, len(empty_cols))

    # 3) df_all speichern
    df_all.to_csv(SCRAPED_MERGED, index=False)
    logging.info("Gescrapte Trägerdaten gespeichert unter: %s", SCRAPED_MERGED)

    # 4) OGD-Kern laden
    df_ogd = load_csv(OGD_CLEAN, "OGD (bereinigt)")
    logging.info("df_ogd: %d Zeilen, %d Spalten", *df_ogd.shape)

    # 5) Enrichment durchführen
    df_ogd_enriched, links = build_enrichment(df_all=df_all, df_ogd=df_ogd)

    # 6) Ergebnisse speichern
    df_ogd_enriched.to_csv(OGD_ENRICHED, index=False)
    logging.info("Angereicherter OGD-Datensatz gespeichert unter: %s", OGD_ENRICHED)

    links.to_csv(LINKS_PATH, index=False)
    logging.info("Matching-Links gespeichert unter: %s", LINKS_PATH)

    # 7) Kurze Coverage-Statistiken
    scraped_with_traeger = df_all["traeger"].notna().sum()
    logging.info(
        "Scraping: %d Zeilen gesamt, %d mit traeger (%.1f %%)",
        len(df_all),
        scraped_with_traeger,
        scraped_with_traeger / max(len(df_all), 1) * 100,
    )

    ogd_with_traeger = df_ogd_enriched["traeger"].notna().sum()
    logging.info(
        "OGD angereichert: %d Zeilen gesamt, %d mit traeger (%.1f %%)",
        len(df_ogd_enriched),
        ogd_with_traeger,
        ogd_with_traeger / max(len(df_ogd_enriched), 1) * 100,
    )

    if scraped_with_traeger > 0:
        logging.info(
            "Anteil der gescrapten traeger, die im OGD gelandet sind: %.1f %%",
            ogd_with_traeger / scraped_with_traeger * 100,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
