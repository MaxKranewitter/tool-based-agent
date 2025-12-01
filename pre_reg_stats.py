import os
import sys
from datetime import datetime, timedelta

# Projekt-Root in den Suchpfad nehmen
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = CURRENT_DIR
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from backend.sql_db import query_db  # nutzt DB_URL & st.secrets intern


def summary_last_7_days():
    """
    Gibt eine Übersicht der Vormerkungen der letzten 7 Tage:
    - Gesamtzahl
    - Anzahl pro Einrichtung
    - Anzahl pro Ort
    """
    sql_total = """
        SELECT COUNT(*) AS total
        FROM public.pre_registrations
        WHERE created_at >= now() - interval '7 days'
    """
    total = query_db(sql_total)[0]["total"]

    sql_by_facility = """
        SELECT
            k.kennzahl,
            k.name,
            k.ort,
            COUNT(p.id) AS pre_reg_count
        FROM public.pre_registrations p
        JOIN public."KBBEs" k USING (kennzahl)
        WHERE p.created_at >= now() - interval '7 days'
        GROUP BY k.kennzahl, k.name, k.ort
        ORDER BY pre_reg_count DESC, k.ort, k.name
    """
    by_facility = query_db(sql_by_facility)

    sql_by_city = """
        SELECT
            k.ort,
            COUNT(p.id) AS pre_reg_count
        FROM public.pre_registrations p
        JOIN public."KBBEs" k USING (kennzahl)
        WHERE p.created_at >= now() - interval '7 days'
        GROUP BY k.ort
        ORDER BY pre_reg_count DESC, k.ort
    """
    by_city = query_db(sql_by_city)

    print("=== Vormerkungen (letzte 7 Tage) ===")
    print(f"Gesamtanzahl Vormerkungen: {total}\n")

    print("Top-Einrichtungen (nach Vormerkungen):")
    if not by_facility:
        print("  Keine Vormerkungen im gewählten Zeitraum.")
    else:
        for row in by_facility:
            print(
                f"  - {row['name']} (Kennzahl {row['kennzahl']}, {row['ort']}): "
                f"{row['pre_reg_count']} Vormerkung(en)"
            )

    print("\nVormerkungen nach Ort:")
    if not by_city:
        print("  Keine Vormerkungen im gewählten Zeitraum.")
    else:
        for row in by_city:
            print(f"  - {row['ort']}: {row['pre_reg_count']} Vormerkung(en)")


def list_recent_pre_regs(limit: int = 20):
    """
    Listet die letzten 'limit' Vormerkungen inkl. Datum, Kennzahl, Kind, Eltern.
    """
    sql = """
        SELECT
            p.created_at,
            p.kennzahl,
            k.name AS einrichtungsname,
            k.ort,
            p.child_name,
            p.parent_name,
            p.parent_email
        FROM public.pre_registrations p
        JOIN public."KBBEs" k USING (kennzahl)
        ORDER BY p.created_at DESC
        LIMIT %s
    """
    rows = query_db(sql, (limit,))

    print(f"\n=== Letzte {limit} Vormerkungen ===")
    if not rows:
        print("Keine Vormerkungen gefunden.")
        return

    for r in rows:
        ts = r["created_at"]
        ts_str = ts.strftime("%Y-%m-%d %H:%M") if isinstance(ts, datetime) else str(ts)
        print(
            f"- {ts_str}: {r['child_name']} (Eltern: {r['parent_name']}, {r['parent_email']}) "
            f"→ {r['einrichtungsname']} (Kennzahl {r['kennzahl']}, {r['ort']})"
        )


if __name__ == "__main__":
    summary_last_7_days()
    list_recent_pre_regs(limit=20)