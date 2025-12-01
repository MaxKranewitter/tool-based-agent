import psycopg
import streamlit as st
from typing import List, Dict, Any, Optional

DB_URL = st.secrets["SUPABASE_DB_URL"]


def query_db(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    """
    Führt ein SELECT-Statement aus und gibt eine Liste von Dicts zurück.
    Nur für READ-Only gedacht.
    """
    rows: List[Dict[str, Any]] = []

    with psycopg.connect(DB_URL) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, params)
            for row in cur.fetchall():
                rows.append(dict(row))

    return rows


def clean_contact_field(value: str | None) -> str | None:
    """
    Entfernt Markdown-Reste wie '(...utm_source=openai)' aus Telefon/E-Mail/URL.
    """
    if not value:
        return None
    return value.split("(")[0].strip()


# ---------------------------------------------------------
# Einrichtungen nach Ort
# ---------------------------------------------------------
def get_facilities_by_query(query: str) -> List[Dict[str, Any]]:
    """
    Sucht Einrichtungen nach Ortsname ODER PLZ.

    - Wenn query nur aus Ziffern besteht → als PLZ interpretieren (Spalte plz).
    - Sonst → fuzzy-Ortsname: exakter Match ODER Ort ILIKE %query%.
    """
    q = (query or "").strip()
    if not q:
        return []

    rows: List[Dict[str, Any]] = []

    # Nur Ziffern → PLZ-Suche
    if q.replace(" ", "").isdigit():
        sql = """
            SELECT
                kennzahl,
                name,
                ort,
                plz,
                telefon,
                email,
                weburl,
                capacity_estimate,
                current_occupancy,
                pre_registrations
            FROM public."KBBEs"
            WHERE plz::text = %s
            ORDER BY ort, name
        """
        rows = query_db(sql, (q,))
    else:
        # Ortsname → exakter Match ODER fuzzy via ILIKE %q%
        like = f"%{q}%"
        sql = """
            SELECT
                kennzahl,
                name,
                ort,
                plz,
                telefon,
                email,
                weburl,
                capacity_estimate,
                current_occupancy,
                pre_registrations
            FROM public."KBBEs"
            WHERE lower(ort) = lower(%s)
               OR ort ILIKE %s
            ORDER BY ort, name
        """
        rows = query_db(sql, (q, like))

    # Kontakte säubern
    for r in rows:
        r["telefon"] = clean_contact_field(r.get("telefon"))
        r["email"] = clean_contact_field(r.get("email"))
        r["weburl"] = clean_contact_field(r.get("weburl"))

    return rows


def format_facilities(rows: List[Dict[str, Any]], city: str) -> str:
    """
    Baut aus den DB-Zeilen einen gut lesbaren Text für den Chatbot.
    """
    if not rows:
        return f"Ich habe in der Datenbank keine Kinderbetreuungseinrichtungen in {city} gefunden."

    lines = [f"Ich habe folgende Kinderbetreuungseinrichtungen in {city} gefunden:\n"]
    for r in rows:
        line = f"- **{r['name']}**"
        contact_parts = []
        if r.get("telefon"):
            contact_parts.append(f"Tel.: {r['telefon']}")
        if r.get("email"):
            contact_parts.append(f"E-Mail: {r['email']}")
        if r.get("weburl"):
            contact_parts.append(f"Web: {r['weburl']}")
        if contact_parts:
            line += " — " + " | ".join(contact_parts)
        lines.append(line)

    return "\n".join(lines)


# ---------------------------------------------------------
# Kapazität & Vormerkung
# ---------------------------------------------------------
def get_free_places(kennzahl: int) -> Optional[int]:
    sql = """
        SELECT capacity_estimate, current_occupancy, pre_registrations
        FROM public."KBBEs"
        WHERE kennzahl = %s
    """
    rows = query_db(sql, (kennzahl,))
    if not rows:
        return None

    r = rows[0]

    def to_int(x) -> int:
        if x is None:
            return 0
        if isinstance(x, str):
            x = x.strip()
            if x == "":
                return 0
        return int(x)

    cap = to_int(r.get("capacity_estimate"))
    occ = to_int(r.get("current_occupancy"))
    pre = to_int(r.get("pre_registrations"))

    free_places = cap - occ - pre
    return max(free_places, 0)


def reserve_place(
    kennzahl: int,
    parent_name: str,
    parent_email: str,
    child_name: str,
) -> bool:
    free = get_free_places(kennzahl)
    if free is None or free <= 0:
        return False

    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE public."KBBEs"
                SET pre_registrations = COALESCE(pre_registrations, 0) + 1
                WHERE kennzahl = %s
                """,
                (kennzahl,),
            )

            # optional: später Vormerkdetails in eigene Tabelle schreiben
        conn.commit()

    return True


def get_facility_by_kennzahl(kennzahl: int) -> Optional[Dict[str, Any]]:
    sql = """
        SELECT
            kennzahl,
            name,
            ort,
            telefon,
            email,
            weburl,
            capacity_estimate,
            current_occupancy,
            pre_registrations
        FROM public."KBBEs"
        WHERE kennzahl = %s
    """
    rows = query_db(sql, (kennzahl,))
    return rows[0] if rows else None

def reset_pre_registrations(city: Optional[str] = None) -> None:
    """
    Setzt pre_registrations entweder:
    - für alle Einrichtungen (city=None) oder
    - nur für eine bestimmte Stadt/Gemeinde
    wieder auf 0.
    Nur für Test-/Demo-Zwecke gedacht.
    """
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            if city:
                cur.execute(
                    """
                    UPDATE public."KBBEs"
                    SET pre_registrations = 0
                    WHERE lower(ort) = lower(%s)
                    """,
                    (city,),
                )
            else:
                cur.execute(
                    """
                    UPDATE public."KBBEs"
                    SET pre_registrations = 0
                    """
                )
        conn.commit()