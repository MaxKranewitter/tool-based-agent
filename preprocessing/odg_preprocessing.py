# %% [markdown]
# # Setup & Konfiguration

# Optional: Google Drive in Google Colab einbinden
from google.colab import drive
drive.mount("/content/drive")

import os
from typing import Optional

# Basisverzeichnis im Google Drive (bei Bedarf anpassen)
BASE_DIR = r"/content/drive/My Drive/DSE_MSc"
DATA_DIR = os.path.join(BASE_DIR, "datasets")
NOTEBOOK_DIR = os.path.join(BASE_DIR, "notebooks")

# Pfad zu dieser Notebook-Datei (für den späteren HTML-Export)
NOTEBOOK_PATH = os.path.join(
    NOTEBOOK_DIR,
    "OGD_vorverarbeitung_exploration.ipynb",
)

# Arbeitsverzeichnis setzen
os.chdir(BASE_DIR)
print("Arbeitsverzeichnis:", os.getcwd())

# Überblick über Dateien im Dataset-Ordner
print("Dateien im Dataset-Ordner:")
print(os.listdir(DATA_DIR))


# %% [markdown]
# # Pakete laden

import pandas as pd
import matplotlib.pyplot as plt
import missingno as msno


# %% [markdown]
# # Hilfsfunktionen

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
    df.columns = (
        df.columns
        .str.strip()   # Leerzeichen am Rand entfernen
        .str.lower()   # alles klein schreiben
    )
    return df


def clean_phone_column(df: pd.DataFrame, column: str = "telefon") -> pd.DataFrame:
    """Bereinigt die Telefonnummernspalte.

    - Entfernt Leerzeichen.
    - Wandelt 'nan' wieder in leere Strings um.

    Args:
        df: Eingangs-DataFrame.
        column: Name der Telefons palte.

    Returns:
        DataFrame mit bereinigter Telefons palte.
    """
    df = df.copy()
    df[column] = (
        df[column]
        .astype(str)
        .str.replace(r"\s+", "", regex=True)    # alle Leerzeichen entfernen
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
    art_mapping = {
        "KG": "Kindergarten",
        "KS": "Krabbelstube",
        "HO": "Hort",
        "SOF": "Sonstige Form der Kinderbetreuung",
    }

    df = df.copy()
    df[column] = df[column].map(art_mapping)
    return df


def map_bezirk_column(df: pd.DataFrame, column: str = "bezirk") -> pd.DataFrame:
    """Mappt Bezirkskennzahlen auf Bezirksnamen.

    Args:
        df: Eingangs-DataFrame.
        column: Name der Spalte mit der Bezirkskennzahl.

    Returns:
        DataFrame mit Bezirksnamen.
    """
    bezirk_mapping = {
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

    df = df.copy()
    df[column] = df[column].astype(int)
    df[column] = df[column].map(bezirk_mapping)
    return df


# URLs sind teilweise veraltet (Datensatz von 2023)
# Musste manuell erfolgen
# Mapping alte -> neue URLs
URL_MAPPING = {
    "https://www.steyr.gv.at/einrichtungen/soziale_einrichtungen/kindergaerten_und_horte":
        "https://www.steyr.at/Leben/Familie_Kinder/Krabbelstuben_Kindergaerten_und_Horte",

    "https://st-anna-steyr.at/start-hort":
        "https://hort.st-anna-steyr.at/",
    "http://st-anna-steyr.at/start-hort":
        "https://hort.st-anna-steyr.at/",
    "st-anna-steyr.at/start-hort":
        "https://hort.st-anna-steyr.at/",

    "https://www.fwsl.at/waldorfkindergarten-sued":
        "https://waldorf-linz.at/",
    "https://www.fwsl.at/waldorfkindergarten-nord":
        "https://waldorf-linz.at/",

    "https://www.junges-wohnen.at/hort":
        "https://www.junges-wohnen.at/",

    "https://www.kidsandcompany.at/wordpress":
        "https://www.kidsandcompany-steyr.at/",

    "https://www.ooe.lebenshilfe.org/lebenshilfe":
        "https://ooe.lebenshilfe.org/standorte/kindergaerten/kindergarten-kindergarten-steyr-gleink",

    "https://www.davinciakademie.at/home":
        "https://www.davinciakademie.at/",

    "https://www.ekiz-uttendorf.at/index.php/krabbelstube.html":
        "http://www.ekiz-uttendorf.at/index.php/",

    "https://www.stpantaleon.at/gemeinde/kinderbetreuung":
        "https://www.stpantaleon.at/Politik_Verwaltung/Schule_Bildung/Kindergarten_Krabbelstube",

    "https://www.rossbach.at/krabbelstube_rossbach_-_st_veit_8":
        "https://www.rossbach.at/Unser_Rossbach/Kinderbetreuung..._Rossbach_-_St_Veit/Kontakt_und_Aufnahme/Kontakt_und_Aufnahme",
}


def apply_url_mapping(df: pd.DataFrame, column: str = "weburl") -> pd.DataFrame:
    """Bereinigt und aktualisiert Web-URLs über ein manuell gepflegtes Mapping.

    Args:
        df: Eingangs-DataFrame.
        column: Name der Spalte mit URLs.

    Returns:
        DataFrame mit aktualisierten URLs.
    """
    df = df.copy()
    # Whitespace säubern, damit das Mapping greift
    df[column] = df[column].astype(str).str.strip()
    # Mapping anwenden
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


# %% [markdown]
# # Daten laden und erste Exploration

# Pfad zur Datei
FILE_PATH = os.path.join(DATA_DIR, "kbbes.csv")

# CSV einlesen
df_raw = pd.read_csv(FILE_PATH)

# Überblick
print("Form (Zeilen, Spalten):", df_raw.shape)
print("\nSpaltennamen:")
print(df_raw.columns.tolist())

print("\nInfo zum ursprünglichen DataFrame:")
print(df_raw.info())

print("\nFehlende Werte pro Spalte:")
print(df_raw.isna().sum().sort_values(ascending=False))

# Missingness-Matrix vor dem Cleaning
plot_missingness(df_raw, title="Missingness vor dem Cleaning")


# %% [markdown]
# # Datenbereinigung

df = clean_ogd_dataset(df_raw)

# Plausibilitätsprüfung: Bezirkswerte, die nicht im Mapping enthalten wären
# (sollte im Normalfall eine leere Menge sein)
print("\nNicht gemappte Bezirkswerte (falls vorhanden):")
# Hier nutzen wir die numerischen Keys des Mappings als Referenz
bezirk_mapping_keys = {401, 402, 403, 404, 405, 406, 407, 408,
                       409, 410, 411, 412, 413, 414, 415, 416,
                       417, 418}
# Ursprüngliche Werte könnten schon gemappt sein; hier nur zur Kontrolle:
print(sorted(set(df["bezirk"])))


# Missingness-Matrix nach dem Cleaning
plot_missingness(df, title="Missingness nach dem Cleaning")

print("\nVorschau auf den bereinigten DataFrame:")
print(df.head())


# %% [markdown]
# # Bereinigten Datensatz speichern

FINAL_DIR = os.path.join(DATA_DIR, "datasets_final")
os.makedirs(FINAL_DIR, exist_ok=True)

OUTPUT_PATH = os.path.join(FINAL_DIR, "ogd_preprocessed.csv")
df.to_csv(OUTPUT_PATH, index=False)

print("Gespeichert unter:", OUTPUT_PATH)


# %% [markdown]
# # Notebook als HTML exportieren (optional)

# Hinweis: funktioniert in der Regel in Jupyter/Colab-Umgebungen
# und erzeugt eine HTML-Version des Notebooks im Zielordner.
!jupyter nbconvert --to html "$NOTEBOOK_PATH" --output-dir="$FINAL_DIR"