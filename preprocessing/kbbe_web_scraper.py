#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Scraper für Kinderbetreuungseinrichtungen (Linz, Kinderfreunde, Familienbund).

Dieses Skript ist für die lokale VS-Code-Umgebung gedacht und benötigt
kein Google Colab / Google Drive.

Es:
- lädt definierte URL-Listen (Linz, Kinderfreunde, Familienbund),
- scraped die jeweiligen Detailseiten,
- harmonisiert die Felder,
- speichert CSVs nach: preprocessing/outputs/.
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Projektpfade
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PREPROCESSING_DIR = PROJECT_ROOT / "preprocessing"
OUTPUT_DIR = PREPROCESSING_DIR / "outputs"

OUTPUT_LINZ = OUTPUT_DIR / "linz_kinderbetreuung_stadt.csv"
OUTPUT_KF = OUTPUT_DIR / "kinderfreunde_kinderbetreuung_ooe.csv"
OUTPUT_FB = OUTPUT_DIR / "familienbund_kinderbetreuung_ooe.csv"

# ---------------------------------------------------------------------------
# Globale Konstanten (Header, Regex, Formular-URLs)
# ---------------------------------------------------------------------------

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0 Safari/537.36"
    )
}

FORM_URLS = {
    "hort": "https://www.linz.at/serviceguide/form.php?id=8746",
    "kindergarten": "https://www.linz.at/serviceguide/form.php?id=8753",
    "krabbelstube": "https://www.linz.at/serviceguide/form.php?id=8754",
}

EMAIL_PATTERN = re.compile(
    r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"
)

WEEKDAY_PATTERN = re.compile(
    r"(Montag|Dienstag|Mittwoch|Donnerstag|Freitag|Samstag|Sonntag)",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# URL-Listen (Platzhalter – hier deine echten Listen einfügen)
# ---------------------------------------------------------------------------

hort_urls: List[Tuple[str, str]] = [
    ("Hort Allendeplatz", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122477"),
    ("Hort Biesenfeld", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122479"),
    ("Hort Coulinstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123116"),
    ("Hort Dorfhalleschule", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122480"),
    ("Hort Edlbacherstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123117"),
    ("Hort Edmund Aigner", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122481"),
    ("Hort Fechterweg", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122482"),
    ("Hort Goetheschule", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122483"),
    ("Hort Harbach", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123142"),
    ("Hort Hauderweg", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122484"),
    ("Hort Keferfeld", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123267"),
    ("Hort Khevenhüllerstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122488"),
    ("Hort Koref", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122489"),
    ("Hort Löwenfeld", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122493"),
    ("Hort Mira-Lobe", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123431"),
    ("Hort Mozartschule", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122492"),
    ("Hort Pichlingschule", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122494"),
    ("Hort Raimundstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122495"),
    ("Hort Robinson", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122478"),
    ("Hort Rohrmayrstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122496"),
    ("Hort Römerbergschule", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122497"),
    ("Hort Schärfschule", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122498"),
    ("Hort Scharmühlwinkel", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122508"),
    ("Hort Siemensschule", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122499"),
    ("Hort Solar City", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122500"),
    ("Hort Sonnenstein", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122486"),
    ("Hort Spallerhofschule", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122501"),
    ("Hort Spaunstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123141"),
    ("Hort Stadlerstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123235"),
    ("Hort Straßlandweg", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122502"),
    ("Hort Weberschule", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122503"),
    ("Hort Wieningerstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122504"),
    ("Integrationshort Karlhofschule", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122506"),
    ("Integrationshort Rennerschule", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122507"),
]

kindergarten_urls: List[Tuple[str, str]] = [
    ("Kindergarten Allendeplatz", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122512"),
    ("Kindergarten Am Hartmayrgut", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123283"),
    ("Kindergarten Anastasius-Grün-Straße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122513"),
    ("Kindergarten Auwiesenstraße 130", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122514"),
    ("Kindergarten Auwiesenstraße 22-24", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122515"),
    ("Kindergarten Auwiesenstraße 60", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122516"),
    ("Kindergarten Breitwiesergutstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122517"),
    ("Kindergarten Brucknerstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123232"),
    ("Kindergarten Bürgerstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122518"),
    ("Kindergarten Commendastraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122519"),
    ("Kindergarten Cremeristraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122520"),
    ("Kindergarten Darrgutstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122521"),
    ("Kindergarten Dauphinestraße 216", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123040"),
    ("Kindergarten Dornacher Straße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122522"),
    ("Kindergarten Edeltraud-Hofer-Straße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123213"),
    ("Kindergarten Freistädter Straße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122524"),
    ("Kindergarten Garnisonstraße 33", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122525"),
    ("Kindergarten Garnisonstraße 38", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123096"),
    ("Kindergarten Glimpfingerstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123265"),
    ("Kindergarten Hauderweg", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122526"),
    ("Kindergarten Hebenstreitstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122527"),
    ("Kindergarten Heliosallee 181", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123311"),
    ("Kindergarten Helmholtzstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123176"),
    ("Kindergarten Hertzstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122529"),
    ("Kindergarten Hofmannsthalweg", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122530"),
    ("Kindergarten Hofmeindlweg", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122531"),
    ("Kindergarten Holzstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122807"),
    ("Kindergarten In der Auerpeint", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122532"),
    ("Kindergarten J.-W.-Klein-Straße 60", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122533"),
    ("Kindergarten Kraußstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122534"),
    ("Kindergarten Langgasse", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122536"),
    ("Kindergarten Leonfeldner Straße 102a", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122537"),
    ("Kindergarten Leonfeldner Straße 3a", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122538"),
    ("Kindergarten Leonfeldner Straße 99d", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122540"),
    ("Kindergarten Ludlgasse", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122541"),
    ("Kindergarten Marienberg", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122542"),
    ("Kindergarten Minnesängerplatz", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122543"),
    ("Kindergarten Neufelderstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122555"),
    ("Kindergarten Pestalozzistraße 84", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122544"),
    ("Kindergarten Pestalozzistraße 96", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123346"),
    ("Kindergarten Poschacherstraße – Bilingualer Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123049"),
    ("Kindergarten Posthofstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122545"),
    ("Kindergarten Reischekstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122547"),
    ("Kindergarten Rohrmayrstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122549"),
    ("Kindergarten Römerstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122550"),
    ("Kindergarten Scharmühlwinkel", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122551"),
    ("Kindergarten Schiedermayrweg", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122552"),
    ("Kindergarten Schiffmannstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123255"),
    ("Kindergarten Schnitzlerweg", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122553"),
    ("Kindergarten Sennweg", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122554"),
    ("Kindergarten Sintstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123273"),
    ("Kindergarten Traundorfer Straße 286", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123348"),
    ("Kindergarten Tungassingerstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122556"),
    ("Kindergarten Webergasse", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122557"),
    ("Kindergarten Weikerlseestraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122528"),
    ("Kindergarten Werndlstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122805"),
    ("Kindergarten Wieningerstraße 16", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122558"),
    ("Kindergarten Wieningerstraße 19", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123347"),
    ("Kindergarten Ziererfeldstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122559"),
]

krabbelstube_urls: List[Tuple[str, str]] = [
    ("Krabbelstube Am Hartmayrgut", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123048"),
    ("Krabbelstube Anastasius-Grün-Straße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122561"),
    ("Krabbelstube Auf der Wies", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123228"),
    ("Krabbelstube Dauphinestraße 56a", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123042"),
    ("Krabbelstube Don-Bosco-Weg", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122562"),
    ("Krabbelstube Freistädter Straße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122563"),
    ("Krabbelstube Hessenplatz", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123146"),
    ("Krabbelstube Humboldtstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123099"),
    ("Krabbelstube Kreßweg", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123043"),
    ("Krabbelstube Leonfeldner Straße 100a", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122564"),
    ("Krabbelstube Maidwieserstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123072"),
    ("Krabbelstube Rohrmayrstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122565"),
    ("Krabbelstube Scharmühlwinkel", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122566"),
    ("Krabbelstube Schiedermayrweg", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123262"),
    ("Krabbelstube Schubertstraße", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123229"),
    ("Krabbelstube Wallenbergstraße (vormals Tungassingerstraße)", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123073"),

    ("Krabbelstubengruppe Bürgerstraße im Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122567"),
    ("Krabbelstubengruppe Dauphinestraße 216 im Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123041"),
    ("Krabbelstubengruppe Helmholtzstraße im Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123177"),
    ("Krabbelstubengruppe Hofmannsthalweg im Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122568"),
    ("Krabbelstubengruppe Schnitzlerweg im Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122806"),
    ("Krabbelstubengruppe Sennweg im Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122571"),
    ("Krabbelstubengruppe Wieningerstraße 16 im Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122572"),

    ("Krabbelstubengruppen Allendeplatz im Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122560"),
    ("Krabbelstubengruppen Commendastraße im Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123225"),
    ("Krabbelstubengruppen Edeltraud-Hofer-Straße im Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123214"),
    ("Krabbelstubengruppen Garnisonstraße 38 im Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123095"),
    ("Krabbelstubengruppen Glimpfingerstraße im Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123291"),
    ("Krabbelstubengruppen Hauderweg im Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123312"),
    ("Krabbelstubengruppen Heliosallee im Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123044"),
    ("Krabbelstubengruppen Hertzstraße im Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123071"),
    ("Krabbelstubengruppen Hofmeindlweg im Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123272"),
    ("Krabbelstubengruppen Johann-Wilhelm-Klein-Straße im Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122569"),
    ("Krabbelstubengruppen Leonfeldnerstr. 99d im Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123212"),
    ("Krabbelstubengruppen Neufelderstraße im Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122573"),
    ("Krabbelstubengruppen Poschacherstraße im bilingualen Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123050"),
    ("Krabbelstubengruppen Reischekstraße im Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=122570"),
    ("Krabbelstubengruppen Schiffmannstraße im Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123254"),
    ("Krabbelstubengruppen Sintstraße im Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123274"),
    ("Krabbelstubengruppen Traundorfer Straße 286", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123358"),
    ("Krabbelstubengruppen Weikerlseestraße im Kindergarten", "https://www.linz.at/serviceguide/viewchapter.php?chapter_id=123139"),
]

kinderfreunde_hort_urls: List[Tuple[str, str]] = [
    ("Hort Ansfelden", "https://kinderfreunde.at/angebote/detail/hort-ansfelden"),
    ("Hort Braunau", "https://kinderfreunde.at/angebote/detail/hort-braunau"),

    ("Hort Eferding - Gruppe 1, 2 und 6",
     "https://kinderfreunde.at/angebote/detail/hort-eferding-gruppe-3-4-5-und-buro-leitung"), # hier sind tatsächlich die URLs vertauscht worden
    ("Hort Eferding - Gruppe 3, 4, 5 und Büro Leitung",
     "https://kinderfreunde.at/angebote/detail/hort-eferding-gruppe-1-2-und-6"), # hier sind tatsächlich die URLs vertauscht worden
    ("Hort Grünau", "https://kinderfreunde.at/angebote/detail/hort-grunau"),
    ("Hort Gutau", "https://kinderfreunde.at/angebote/detail/hort-gutau"),
    ("Hort Haid", "https://kinderfreunde.at/angebote/detail/hort-haid"),
    ("Hort Hallstatt", "https://kinderfreunde.at/angebote/detail/hort-hallstatt"),
    ("Hort Kirchberg-Thening", "https://kinderfreunde.at/angebote/detail/hort-kirchberg-thening"),
    ("Hort Kirchdorf", "https://kinderfreunde.at/angebote/detail/hort-kirchdorf"),
    ("Hort Kremsdorf", "https://kinderfreunde.at/angebote/detail/hort-kremsdorf"),
    ("Hort Langholzfeld", "https://kinderfreunde.at/angebote/detail/hort-langholzfeld"),

    ("Hort Lengau", "https://kinderfreunde.at/angebote/detail/hort-lengau"),
    ("Hort Mauerkirchen", "https://kinderfreunde.at/angebote/detail/hort-mauerkirchen"),
    ("Hort Neumarkt", "https://kinderfreunde.at/angebote/detail/hort-neumarkt"),
    ("Hort Oftering", "https://kinderfreunde.at/angebote/detail/hort-oftering"),
    ("Hort Riedersbach", "https://kinderfreunde.at/angebote/detail/hort-riedersbach"),
    ("Hort Schwertberg", "https://kinderfreunde.at/angebote/detail/hort-schwertberg"),
    ("Hort Sierning", "https://kinderfreunde.at/angebote/detail/hort-sierning"),
    ("Hort St. Georgen/Gusen", "https://kinderfreunde.at/angebote/detail/hort-st-georgen-gusen"),
    ("Hort St. Margarethen", "https://kinderfreunde.at/angebote/detail/hort-st-margarethen"),
    ("Hort Unterweitersdorf", "https://kinderfreunde.at/angebote/detail/hort-unterweitersdorf"),
    ("Hort Wartberg", "https://kinderfreunde.at/angebote/detail/hort-wartberg"),
    ("Hort Wilhering", "https://kinderfreunde.at/angebote/detail/hort-wilhering"),
    ("Linz - Hort Ziegeleistraße", "https://kinderfreunde.at/angebote/detail/linz-hort-ziegeleistrasse"),
]

kinderfreunde_kindergarten_urls: List[Tuple[str, str]] = [
    ("Kindergarten Eferding",
     "https://kinderfreunde.at/angebote/detail/kindergarten-eferding"),
    ("Kindergarten Langholzfeld",
     "https://kinderfreunde.at/angebote/detail/kindergarten-langholzfeld"),
    ("Kindergarten Mauthausen",
     "https://kinderfreunde.at/angebote/detail/kindergarten-mauthausen"),
    ("Kindergarten Neuhofen",
     "https://kinderfreunde.at/angebote/detail/kindergarten-neuhofen"),
    ("Kindergarten Obertraun",
     "https://kinderfreunde.at/angebote/detail/kindergarten-obertraun"),
    ("Kindergarten Pasching",
     "https://kinderfreunde.at/angebote/detail/kindergarten-pasching"),
    ("Kindergarten Plus City",
     "https://kinderfreunde.at/angebote/detail/kindergarten-plus-city"),
    ("Kindergarten Schwertberg",
     "https://kinderfreunde.at/angebote/detail/kindergarten-schwertberg"),
    ("Kinderzentrum Pasching",
     "https://kinderfreunde.at/angebote/detail/kinderzentrum-pasching"),
    ("Krabbelstube und Kindergarten Langholzfeld",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-und-kindergarten-langholzfeld"),
    ("Krabbelstube und Kindergarten Plus City",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-und-kindergarten-plus-city"),
    ("Linz - Kindergarten Edisonstraße",
     "https://kinderfreunde.at/angebote/detail/linz-kindergarten-edisonstrasse"),
    ("Linz - Kindergarten Einfaltstraße",
     "https://kinderfreunde.at/angebote/detail/linz-kindergarten-einfaltstrasse"),
    ("Linz - Kindergarten Ing. Stern-Straße",
     "https://kinderfreunde.at/angebote/detail/linz-kindergarten-ing-stern-strasse"),
    ("Linz - Kindergarten Zaunmüllerstraße",
     "https://kinderfreunde.at/angebote/detail/linz-kindergarten-zaunmullerstrasse"),
    ("Naturkindergarten St.Georgen/Gusen",
     "https://kinderfreunde.at/angebote/detail/naturkindergarten-st-georgen-gusen"),
    ("Steyr - Kindergarten Ennsleite",
     "https://kinderfreunde.at/angebote/detail/kindergarten-ennsleite"),
]

kinderfreunde_krabbelstube_urls: List[Tuple[str, str]] = [
    ("Betriebskrabbelstube Forensisch-Therapeutisches Zentrum Asten",
     "https://kinderfreunde.at/angebote/detail/betriebskrabbelstube-forensisch-therapeutisches-zentrum-asten"),
    ("Kinderzentrum Pasching",
     "https://kinderfreunde.at/angebote/detail/kinderzentrum-pasching"),
    ("Krabbelstube Asten",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-asten"),
    ("Krabbelstube Attnang-Puchheim",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-attnang-puchheim"),
    ("Krabbelstube Braunau",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-braunau"),
    ("Krabbelstube Braunau Expositur Neustadt",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-braunau-expositur-neustadt"),
    ("Krabbelstube Eferding",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-eferding"),
    ("Krabbelstube Gallneukirchen",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-gallneukirchen"),
    ("Krabbelstube Haid",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-haid"),
    ("Krabbelstube Haid - Expositur Ansfelden",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-haid-expositur-ansfelden"),

    ("Krabbelstube Haid - Haidpark",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-haid-haidpark"),
    ("Krabbelstube Haslach",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-haslach"),
    ("Krabbelstube Langholzfeld",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-langholzfeld"),
    ("Krabbelstube Lengau",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-lengau"),
    ("Krabbelstube Lengau II",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-lengau-ii"),
    ("Krabbelstube Mattighofen",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-mattighofen"),

    ("Krabbelstube Mattighofen KTM",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-mattighofen-ktm"),
    ("Krabbelstube Mauthausen",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-mauthausen"),
    ("Krabbelstube Neuhofen",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-neuhofen"),
    ("Krabbelstube Neumarkt",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-neumarkt"),

    ("Krabbelstube Pasching",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-pasching"),
    ("Krabbelstube Plus City",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-plus-city"),
    ("Krabbelstube Schörfling",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-schorfling"),
    ("Krabbelstube Seewalchen",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-seewalchen-1"),
    ("Krabbelstube St. Georgen / Gusen",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-st-georgen-1"),

    ("Krabbelstube und Kindergarten Langholzfeld",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-und-kindergarten-langholzfeld"),
    ("Krabbelstube und Kindergarten Plus City",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-und-kindergarten-plus-city"),
    ("Krabbelstube Wilhering",
     "https://kinderfreunde.at/angebote/detail/krabbelstube-wilhering-1"),

    ("Naturkindergarten St.Georgen/Gusen",
     "https://kinderfreunde.at/angebote/detail/naturkindergarten-st-georgen-gusen"),
    ("Steyr - Krabbelstube Kuschelbär",
     "https://kinderfreunde.at/angebote/detail/steyr-krabbelstube-kuschelbar"),

    ("Wels - Krabbelstube Purzelbaum",
     "https://kinderfreunde.at/angebote/detail/wels-krabbelstube-purzelbaum"),
    ("Wels - Krabbelstube Regenbogen",
     "https://kinderfreunde.at/angebote/detail/wels-krabbelstube-regenbogen"),
    ("Wels - Krabbelstube Sonnenschein",
     "https://kinderfreunde.at/angebote/detail/wels-krabbelstube-sonnenschein"),
    ("Wels - Krabbelstube Spatzennest",
     "https://kinderfreunde.at/angebote/detail/wels-krabbelstube-spatzennest"),
    ("Wels - Krabbelstube Wirbelwind",
     "https://kinderfreunde.at/angebote/detail/wels-krabbelstube-wirbelwind"),
]

familienbund_kindergarten_urls: List[Tuple[str, str]] = [
    ("Krabbelstube & Kindergarten Gmunden",
     "https://ooe.familienbund.at/betreuung/krabbelstube-kindergarten-gmunden/"),
    ("Kindergarten & Krabbelstube Hargelsberg",
     "https://ooe.familienbund.at/betreuung/kindergaerten-krabbelstuben-hargelsberg/"),
    ("Kindergarten Katsdorf – Reiser",
     "https://ooe.familienbund.at/betreuung/kindergaerten-katsdorf-reiser/"),
    ("Kindergarten & Krabbelstube Kematen/Krems",
     "https://ooe.familienbund.at/betreuung/krabbelstuben-kematen-krems/"),
    ("Kindergarten & Krabbelstube Pregarten Althauserstraße",
     "https://ooe.familienbund.at/betreuung/kindergaerten-krabbelstuben-pregarten-althauserstrasse/"),
    ("Kindergarten & Krabbelstube Pregarten Grünbichl",
     "https://ooe.familienbund.at/betreuung/kindergaerten-pregarten-gruenbichl/"),
]

familienbund_krabbelstube_urls: List[Tuple[str, str]] = [
    ("Krabbelstube Bad Hall",
     "https://ooe.familienbund.at/betreuung/krabbelstuben-bad-hall/"),
    ("Krabbelstube Dietach",
     "https://ooe.familienbund.at/betreuung/krabbelstuben-dietach/"),
    ("Krabbelstube & Kindergarten Gmunden",
     "https://ooe.familienbund.at/betreuung/krabbelstube-kindergarten-gmunden/"),
    ("Kindergarten & Krabbelstube Hargelsberg",
     "https://ooe.familienbund.at/betreuung/kindergaerten-krabbelstuben-hargelsberg/"),
    ("Kindergarten & Krabbelstube Kematen/Krems",
     "https://ooe.familienbund.at/betreuung/krabbelstuben-kematen-krems/"),
    ("Krabbelstube Kirchham",
     "https://ooe.familienbund.at/betreuung/krabbelstuben-kirchham/"),
    ("Krabbelstube Köckendorf",
     "https://ooe.familienbund.at/betreuung/krabbelstuben-koeckendorf/"),
    ("Krabbelstube Kronstorf",
     "https://ooe.familienbund.at/betreuung/krabbelstuben-kronstorf/"),
    ("Krabbelstube Mondseeland",
     "https://ooe.familienbund.at/betreuung/krabbelstuben-mondseeland/"),
    ("Kindergarten & Krabbelstube Pregarten Althauserstraße",
     "https://ooe.familienbund.at/betreuung/kindergaerten-krabbelstuben-pregarten-althauserstrasse/"),
    ("Kindergarten & Krabbelstube Pregarten Grünbichl",
     "https://ooe.familienbund.at/betreuung/kindergaerten-pregarten-gruenbichl/"),
    ("Krabbelstube Puchenau",
     "https://ooe.familienbund.at/betreuung/krabbelstuben-puchenau/"),
    ("Krabbelstube St. Florian Ort",
     "https://ooe.familienbund.at/betreuung/krabbelstuben-st-florian/"),
    ("Krabbelstube St. Florian Hausfeld",
     "https://ooe.familienbund.at/betreuung/krabbelstube-st-florian-hausfeld/"),
    ("Krabbelstube St. Marienkirchen",
     "https://ooe.familienbund.at/betreuung/krabbelstuben-st-marienkirchen/"),
]

familienbund_krabbelstube_betrieb_urls: List[Tuple[str, str]] = [
    ("Krabbelstube Rotax",
     "https://ooe.familienbund.at/betreuung/krabbelstube-rotax/"),
    ("Krabbelstube RoSiPez (Rosenbauer/Silhouette/PEZ)",
     "https://ooe.familienbund.at/betreuung/betriebliche-krabbelstuben-leonding-rosenbauer-silhouette-pez/"),
    ("Krabbelstube Ordensklinikum Elisabethinen",
     "https://ooe.familienbund.at/betreuung/krabbelstube-ordensklinikum-elisabethinen/"),
    ("Krabbelstube Energie AG",
     "https://ooe.familienbund.at/betreuung/betriebliche-krabbelstuben-linz-energie-ag/"),
    ("Krabbelstube Oberbank",
     "https://ooe.familienbund.at/betreuung/betriebliche-krabbelstuben-linz-oberbank/"),
    ("Krabbelstube WiKi",
     "https://ooe.familienbund.at/betreuung/betriebliche-krabbelstuben-linz-primetals-wifi-wko-siemens/"),
    ("Krabbelstube HABAU – BauZwerge",
     "https://ooe.familienbund.at/betreuung/betriebliche-krabbelstuben-perg-habau/"),
    ("Krabbelstube Engel GmbH",
     "https://ooe.familienbund.at/betreuung/betriebliche-krabbelstuben-schwertberg-engel-gmbh/"),
    ("Krabbelstube Felbermayr",
     "https://ooe.familienbund.at/betreuung/betriebliche-krabbelstuben-wels-felbermayr/"),
]

familienbund_flexible_urls: List[Tuple[str, str]] = [
    ("Flexible Kinderbetreuung Haid-Center",
     "https://ooe.familienbund.at/betreuung/flexible-kinderbetreuungen-ansfelden-haid-center-kinderland/"),
    ("Flexible Kleinkindbetreuung Pfarrwichtel",
     "https://ooe.familienbund.at/betreuung/flexible-kinderbetreuungen-ansfelden-pfarrwichtel/"),
]

familienbund_hort_urls: List[Tuple[str, str]] = [
    ("Hort Bad Hall",
     "https://ooe.familienbund.at/betreuung/hort-bad-hall/"),
    ("Hort Pregarten",
     "https://ooe.familienbund.at/betreuung/hort-pregarten/"),
]

# ---------------------------------------------------------------------------
# Hilfsfunktionen: HTTP & Textbereinigung
# ---------------------------------------------------------------------------


def fetch_html(url: str) -> BeautifulSoup:
    """Lädt eine Webseite und gibt ein BeautifulSoup-Objekt zurück."""
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def normalize_whitespace(text: Optional[str]) -> Optional[str]:
    """Reduziert mehrere Whitespaces/Zeilenumbrüche auf ein Leerzeichen."""
    if pd.isna(text):
        return text
    return re.sub(r"\s+", " ", str(text)).strip()


def clean_contact_name(text: Optional[str]) -> Optional[str]:
    """Bereinigt Kontakt-Namen (Doppelpunkte, doppelte Kommas, Mehrfach-Leerzeichen)."""
    if pd.isna(text):
        return text
    x = str(text)
    x = re.sub(r",\s*,", ",", x)
    x = re.sub(r"\s+", " ", x).strip()
    x = re.sub(r"\s*:\s*$", "", x)
    return x


def extract_first_email(text: str) -> Optional[str]:
    """Extrahiert die erste E-Mail-Adresse aus einem Textblock."""
    if not text:
        return None
    match = EMAIL_PATTERN.search(text)
    return match.group(0) if match else None


# ---------------------------------------------------------------------------
# Parsing Stadt Linz (Serviceguide-Seiten)
# ---------------------------------------------------------------------------


def parse_linz_facility_page(
    url: str,
    list_name: str,
    facility_type: str,
) -> Dict[str, Optional[str]]:
    """Parst eine Kinderbetreuungs-Seite der Stadt Linz (Serviceguide)."""
    soup = fetch_html(url)

    h1 = soup.find("h1")
    name = h1.get_text(strip=True) if h1 else list_name

    full_text = " ".join(soup.stripped_strings)

    street: Optional[str] = None
    plz: Optional[str] = None
    ort: Optional[str] = None
    telefon: Optional[str] = None
    email = extract_first_email(full_text)

    for p in soup.find_all(["p", "li"]):
        text = normalize_whitespace(p.get_text(" ", strip=True))

        if text and re.search(r"\b\d{4,5}\b", text) and "," in text and street is None:
            addr_street, rest = text.split(",", 1)
            addr_street = addr_street.strip()
            rest = rest.strip()
            parts = rest.split(maxsplit=1)
            if len(parts) == 2 and re.fullmatch(r"\d{4,5}", parts[0]):
                street = addr_street
                plz = parts[0]
                ort = parts[1]
                continue

        if telefon is None and re.search(r"\+?\d[\d\s/()-]{5,}", text or ""):
            telefon = text

    opening_lines: List[str] = []
    for p in soup.find_all(["p", "li", "div"]):
        txt = normalize_whitespace(p.get_text(" ", strip=True))
        if not txt:
            continue
        if "Öffnungszeiten" in txt or WEEKDAY_PATTERN.search(txt):
            opening_lines.append(txt)

    opening_hours = (
        " | ".join(dict.fromkeys(opening_lines)) if opening_lines else None
    )

    description: Optional[str] = None
    for p in soup.find_all("p"):
        txt = normalize_whitespace(p.get_text(" ", strip=True))
        if txt and len(txt) > 80 and "kind" in txt.lower():
            description = txt
            break

    form_url = FORM_URLS.get(facility_type.lower())

    return {
        "art": facility_type,
        "name": name,
        "weburl": url,
        "contact_name": None,
        "strasse": street,
        "plz": plz,
        "ort": ort,
        "telefon": telefon,
        "email": email,
        "oeffnungszeiten": opening_hours,
        "beschreibung": description,
        "anmeldung_url": form_url,
        "traeger": "Stadt Linz",
    }


# ---------------------------------------------------------------------------
# Parsing Kinderfreunde
# ---------------------------------------------------------------------------


def parse_kinderfreunde_page(
    url: str,
    list_name: str,
    facility_type: str,
) -> Dict[str, Optional[str]]:
    """Parst eine Kinderfreunde-Seite (kinderfreunde.at)."""
    soup = fetch_html(url)

    h1 = soup.find("h1")
    name = h1.get_text(strip=True) if h1 else list_name

    full_text = " ".join(soup.stripped_strings)
    email = extract_first_email(full_text)

    street: Optional[str] = None
    plz: Optional[str] = None
    ort: Optional[str] = None
    telefon: Optional[str] = None
    oeffnungszeiten: Optional[str] = None
    kosten: Optional[str] = None
    schliesstage: Optional[str] = None
    beschreibung: Optional[str] = None

    for p in soup.find_all(["p", "li"]):
        txt = normalize_whitespace(p.get_text(" ", strip=True))
        if not txt:
            continue

        if street is None and "," in txt and re.search(r"\b\d{4,5}\b", txt):
            addr_street, rest = txt.split(",", 1)
            addr_street = addr_street.strip()
            rest = rest.strip()
            parts = rest.split(maxsplit=1)
            if len(parts) == 2 and re.fullmatch(r"\d{4,5}", parts[0]):
                street = addr_street
                plz = parts[0]
                ort = parts[1]
                continue

        if telefon is None and "Tel" in txt:
            telefon = txt

        if ("Öffnungszeiten" in txt) or (
            "Uhr" in txt and WEEKDAY_PATTERN.search(txt)
        ):
            oeffnungszeiten = (
                txt if oeffnungszeiten is None else f"{oeffnungszeiten} | {txt}"
            )

        if "Kosten" in txt or "Beitrag" in txt:
            kosten = txt if kosten is None else f"{kosten} | {txt}"

        if "Schließtage" in txt or "geschlossen" in txt:
            schliesstage = (
                txt if schliesstage is None else f"{schliesstage} | {txt}"
            )

    for p in soup.find_all("p"):
        txt = normalize_whitespace(p.get_text(" ", strip=True))
        if txt and len(txt) > 80:
            beschreibung = txt
            break

    return {
        "art": facility_type,
        "name": name,
        "weburl": url,
        "contact_name": None,
        "strasse": street,
        "plz": plz,
        "ort": ort,
        "telefon": telefon,
        "email": email,
        "oeffnungszeiten": oeffnungszeiten,
        "angebot_art_label": None,
        "kosten": kosten,
        "schliesstage": schliesstage,
        "beschreibung": beschreibung,
        "traeger": "Kinderfreunde",
    }


# ---------------------------------------------------------------------------
# Parsing Familienbund
# ---------------------------------------------------------------------------


def parse_familienbund_page(
    url: str,
    list_name: str,
    traeger_label: str,
    facility_type: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    """Parst eine Familienbund-Seite (ooe.familienbund.at).

    Args:
        url: URL der Familienbund-Seite.
        list_name: Name aus der URL-Liste.
        traeger_label: Text, der den Träger beschreibt (z.B. „Familienbund OÖ“).
        facility_type: Art der Einrichtung (z.B. "kindergarten", "krabbelstube", "hort").

    Returns:
        Dictionary mit harmonisierten Feldern.
    """
    soup = fetch_html(url)

    h1 = soup.find("h1")
    name = h1.get_text(strip=True) if h1 else list_name

    full_text = " ".join(soup.stripped_strings)
    email = extract_first_email(full_text)

    street: Optional[str] = None
    plz: Optional[str] = None
    ort: Optional[str] = None
    telefon: Optional[str] = None

    for p in soup.find_all(["p", "li"]):
        txt = normalize_whitespace(p.get_text(" ", strip=True))
        if not txt:
            continue

        # Adresse
        if street is None and "," in txt and re.search(r"\b\d{4,5}\b", txt):
            addr_street, rest = txt.split(",", 1)
            addr_street = addr_street.strip()
            rest = rest.strip()
            parts = rest.split(maxsplit=1)
            if len(parts) == 2 and re.fullmatch(r"\d{4,5}", parts[0]):
                street = addr_street
                plz = parts[0]
                ort = parts[1]
                continue

        # Telefon
        if telefon is None and ("Tel" in txt or "Telefon" in txt):
            telefon = txt

    return {
        "art": facility_type,
        "name": name,
        "weburl": url,
        "contact_name": None,
        "strasse": street,
        "plz": plz,
        "ort": ort,
        "telefon": telefon,
        "email": email,
        "traeger": traeger_label,
        "anmeldung_url": None,
        "anmeldung_krabbelstube_url": None,
        "anmeldung_kindergarten_url": None,
    }


# ---------------------------------------------------------------------------
# Generische Scraping-Funktion
# ---------------------------------------------------------------------------


def scrape_facility_list(
    urls: List[Tuple[str, str]],
    parser_func: Callable[..., Dict[str, Optional[str]]],
    parser_kwargs: Optional[Dict[str, object]] = None,
    desc: str = "Scraping",
    sleep_seconds: float = 0.5,
) -> List[Dict[str, Optional[str]]]:
    """Scraped eine Liste von Einrichtungen mit einer Parser-Funktion."""
    parser_kwargs = parser_kwargs or {}
    records: List[Dict[str, Optional[str]]] = []

    for name, url in tqdm(urls, desc=desc):
        try:
            rec = parser_func(url=url, list_name=name, **parser_kwargs)
            records.append(rec)
        except Exception as exc:  # noqa: BLE001
            logging.warning("[ERROR] %s – %s: %s", name, url, exc)
        time.sleep(sleep_seconds)

    return records


# ---------------------------------------------------------------------------
# Hauptlogik
# ---------------------------------------------------------------------------


def main() -> int:
    """Hauptfunktion: führt das Scraping aus und speichert CSVs in outputs/."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    logging.info("Projektroot: %s", PROJECT_ROOT)
    logging.info("Output-Verzeichnis: %s", OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # -------------------------
    # 1) Stadt Linz
    # -------------------------
    linz_records: List[Dict[str, Optional[str]]] = []

    if hort_urls:
        linz_records += scrape_facility_list(
            urls=hort_urls,
            parser_func=parse_linz_facility_page,
            parser_kwargs={"facility_type": "hort"},
            desc="Linz: Horte scrapen",
        )

    if kindergarten_urls:
        linz_records += scrape_facility_list(
            urls=kindergarten_urls,
            parser_func=parse_linz_facility_page,
            parser_kwargs={"facility_type": "kindergarten"},
            desc="Linz: Kindergärten scrapen",
        )

    if krabbelstube_urls:
        linz_records += scrape_facility_list(
            urls=krabbelstube_urls,
            parser_func=parse_linz_facility_page,
            parser_kwargs={"facility_type": "krabbelstube"},
            desc="Linz: Krabbelstuben scrapen",
        )

    df_linz = pd.DataFrame(linz_records)
    logging.info("Linz – Anzahl Einrichtungen: %s", len(df_linz))
    if not df_linz.empty:
        df_linz.to_csv(OUTPUT_LINZ, index=False, encoding="utf-8-sig")
        logging.info("Linz-Daten gespeichert unter: %s", OUTPUT_LINZ)

    # -------------------------
    # 2) Kinderfreunde
    # -------------------------
    kf_records: List[Dict[str, Optional[str]]] = []

    if kinderfreunde_hort_urls:
        kf_records += scrape_facility_list(
            urls=kinderfreunde_hort_urls,
            parser_func=parse_kinderfreunde_page,
            parser_kwargs={"facility_type": "hort"},
            desc="Kinderfreunde: Horte scrapen",
        )

    if kinderfreunde_kindergarten_urls:
        kf_records += scrape_facility_list(
            urls=kinderfreunde_kindergarten_urls,
            parser_func=parse_kinderfreunde_page,
            parser_kwargs={"facility_type": "kindergarten"},
            desc="Kinderfreunde: Kindergärten scrapen",
        )

    if kinderfreunde_krabbelstube_urls:
        kf_records += scrape_facility_list(
            urls=kinderfreunde_krabbelstube_urls,
            parser_func=parse_kinderfreunde_page,
            parser_kwargs={"facility_type": "krabbelstube"},
            desc="Kinderfreunde: Krabbelstuben scrapen",
        )

    df_kf = pd.DataFrame(kf_records)

    kf_base_cols = [
        "art",
        "name",
        "weburl",
        "contact_name",
        "strasse",
        "plz",
        "ort",
        "telefon",
        "email",
        "oeffnungszeiten",
    ]
    kf_extra_cols = [
        "angebot_art_label",
        "kosten",
        "schliesstage",
        "beschreibung",
        "traeger",
    ]

    for col in kf_base_cols + kf_extra_cols:
        if col not in df_kf.columns:
            df_kf[col] = None

    if not df_kf.empty:
        df_kf["strasse"] = df_kf["strasse"].apply(normalize_whitespace)
        df_kf["oeffnungszeiten"] = df_kf["oeffnungszeiten"].apply(
            normalize_whitespace
        )
        df_kf["contact_name"] = df_kf["contact_name"].apply(clean_contact_name)
        df_kf = df_kf[kf_base_cols + kf_extra_cols]

    logging.info("Kinderfreunde – Anzahl Einrichtungen: %s", len(df_kf))
    if not df_kf.empty:
        df_kf.to_csv(OUTPUT_KF, index=False, encoding="utf-8-sig")
        logging.info("Kinderfreunde-Daten gespeichert unter: %s", OUTPUT_KF)

    # -------------------------
    # 3) Familienbund
    # -------------------------
    fb_records: List[Dict[str, Optional[str]]] = []

    if familienbund_kindergarten_urls:
        fb_records += scrape_facility_list(
            urls=familienbund_kindergarten_urls,
            parser_func=parse_familienbund_page,
            parser_kwargs={
                "traeger_label": "Familienbund OÖ",
                "facility_type": "kindergarten",
            },
            desc="Familienbund: Kindergärten scrapen",
        )

    if familienbund_krabbelstube_urls:
        fb_records += scrape_facility_list(
            urls=familienbund_krabbelstube_urls,
            parser_func=parse_familienbund_page,
            parser_kwargs={
                "traeger_label": "Familienbund OÖ",
                "facility_type": "krabbelstube",
            },
            desc="Familienbund: Krabbelstuben scrapen",
        )

    if familienbund_krabbelstube_betrieb_urls:
        fb_records += scrape_facility_list(
            urls=familienbund_krabbelstube_betrieb_urls,
            parser_func=parse_familienbund_page,
            parser_kwargs={
                "traeger_label": "Familienbund OÖ",
                "facility_type": "krabbelstube",
            },
            desc="Familienbund: Betriebliche Krabbelstuben scrapen",
        )

    if familienbund_hort_urls:
        fb_records += scrape_facility_list(
            urls=familienbund_hort_urls,
            parser_func=parse_familienbund_page,
            parser_kwargs={
                "traeger_label": "Familienbund OÖ",
                "facility_type": "hort",
            },
            desc="Familienbund: Horte scrapen",
        )

    if familienbund_flexible_urls:
        fb_records += scrape_facility_list(
            urls=familienbund_flexible_urls,
            parser_func=parse_familienbund_page,
            parser_kwargs={
                "traeger_label": "Familienbund OÖ",
                "facility_type": "flexible",
            },
            desc="Familienbund: Flexible Angebote scrapen",
        )

    df_fb = pd.DataFrame(fb_records)

    fb_base_cols = [
        "art",
        "name",
        "weburl",
        "contact_name",
        "strasse",
        "plz",
        "ort",
        "telefon",
        "email",
    ]
    fb_extra_cols = [
        "traeger",
        "anmeldung_url",
        "anmeldung_krabbelstube_url",
        "anmeldung_kindergarten_url",
    ]

    for col in fb_base_cols + fb_extra_cols:
        if col not in df_fb.columns:
            df_fb[col] = None

    if not df_fb.empty:
        df_fb["strasse"] = df_fb["strasse"].apply(normalize_whitespace)
        df_fb["contact_name"] = df_fb["contact_name"].apply(clean_contact_name)
        df_fb = df_fb[fb_base_cols + fb_extra_cols]

    logging.info("Familienbund – Anzahl Einrichtungen: %s", len(df_fb))
    if not df_fb.empty:
        df_fb.to_csv(OUTPUT_FB, index=False, encoding="utf-8-sig")
        logging.info("Familienbund-Daten gespeichert unter: %s", OUTPUT_FB)

    logging.info("Scraping abgeschlossen.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())