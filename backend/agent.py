import json
import re
from typing import List, Dict, Any

import streamlit as st
from openai import OpenAI

from backend.sql_db import (
    get_facilities_by_query,
    format_facilities,
    get_free_places,
    reserve_place,
    get_facility_by_kennzahl,
)

# OpenAI-Client holt den Key aus Streamlit-Secrets
client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

# Vector Store ID aus setup_rag.py
VECTOR_STORE_ID = "vs_69266a51597c81919d1463fc2f95128e"


# ---------------------------------------------------------
# Helper: Zitationsmarker aus Antworten entfernen
# ---------------------------------------------------------
def _clean_citations(text: str) -> str:
    """
    Entfernt interne OpenAI-Zitationsmarker und private Sonderzeichen,
    lässt aber Zeilenumbrüche und normale Aufzählungen erhalten.
    """

    # Alles entfernen, was mit 'filecite' beginnt, bis zum nächsten Leerzeichen oder Zeilenende
    text = re.sub(r"filecite.*?(?=\s|$)", "", text)

    # Alle 'turnXfileY'-Marker entfernen
    text = re.sub(r"turn\d+file\d+", "", text)

    # Private-Use-Characters und Replacement-Char (�) entfernen
    def is_bad_char(ch: str) -> bool:
        code = ord(ch)
        if 0xE000 <= code <= 0xF8FF:
            return True
        if 0xF0000 <= code <= 0xFFFFD or 0x100000 <= code <= 0x10FFFD:
            return True
        if code == 0xFFFD:
            return True
        return False

    text = "".join(ch for ch in text if not is_bad_char(ch))

    # Mehrfache SPACES (nicht Zeilenumbrüche) reduzieren
    text = re.sub(r"[ ]{2,}", " ", text)

    # Am Zeilenende Leerzeichen entfernen, Zeilenumbrüche beibehalten
    lines = [line.rstrip() for line in text.splitlines()]
    text = "\n".join(lines)

    return text.strip()


# ---------------------------------------------------------
# Router: Entscheidet, ob eine SQL-Aktion nötig ist
# ---------------------------------------------------------
def decide_sql_action(messages: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Nutzt ein kleines Modell, um zu entscheiden, ob eine SQL-Aktion nötig ist.
    Gibt ein Dict zurück, z.B.:

      {
        "action": "none" | "list_facilities" | "check_free_places" | "reserve_place",
        "city": "...",
        "kennzahl": 401007,
        "parent_name": "...",
        "parent_email": "...",
        "child_name": "..."
      }
    """

    # letzte User-Nachricht extrahieren
    last_user = None
    for m in reversed(messages):
        if m["role"] == "user":
            last_user = m["content"]
            break

    if not last_user:
        return {"action": "none"}

    system_prompt = (
        "Du bist ein Routing-Assistent für einen Kinderbetreuungs-Chatbot in Oberösterreich. "
        "Analysiere die letzte Nutzerfrage und entscheide, ob eine SQL-Funktion auf der "
        "Kinderbetreuungsdatenbank aufgerufen werden soll.\n\n"
        "Gib deine Antwort ausschließlich als JSON im folgenden Format zurück:\n"
        "{\n"
        '  \"action\": \"none\" | \"list_facilities\" | \"check_free_places\" | \"reserve_place\",\n'
        '  \"city\": string oder null,\n'
        '  \"kennzahl\": number oder null,\n'
        '  \"parent_name\": string oder null,\n'
        '  \"parent_email\": string oder null,\n'
        '  \"child_name\": string oder null\n'
        "}\n\n"
        "Regeln:\n"
        "- Verwende action=\"list_facilities\", wenn nach Einrichtungen oder freien Plätzen in einer bestimmten Stadt/Gemeinde gefragt wird "
        "(z.B. \"Welche Kinderbetreuungseinrichtungen gibt es in Linz?\") oder \"Wie viele Plätze sind in Hagenberg noch frei?\").\n"
        "- Verwende action=\"check_free_places\", wenn nach freien Plätzen in einer bestimmten Einrichtung gefragt wird.\n"
        "- Verwende action=\"reserve_place\", wenn die Nutzerin ihr Kind vormerken/anmelden möchte.\n"
        "- Sonst action=\"none\".\n"
        "- city nur setzen, wenn eindeutig genannt (z.B. \"Linz\", \"Hagenberg\").\n"
        "- kennzahl nur setzen, wenn die Kennzahl explizit genannt wird.\n"
        "- parent_name, parent_email, child_name nur setzen, wenn sie in der Frage klar vorkommen.\n"
    )

    router_response = client.responses.create(
        model="gpt-4.1-mini",
        input=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": last_user},
        ],
    )

    # Text aus der Router-Antwort holen
    try:
        raw = getattr(router_response, "output_text", None)
        if not raw:
            first = router_response.output[0].content[0].text
            raw = getattr(first, "value", str(first))
    except Exception:
        return {"action": "none"}

    # JSON robust parsen
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if not match:
            return {"action": "none"}
        try:
            data = json.loads(match.group(0))
        except json.JSONDecodeError:
            return {"action": "none"}

    if "action" not in data:
        data["action"] = "none"

    return data


# ---------------------------------------------------------
# Hauptfunktion: Agent mit SQL + RAG + Web
# ---------------------------------------------------------
def run_agent(messages: List[Dict[str, Any]]) -> str:
    """
    Orchestriert:
    - SQL (Einrichtungen, freie Plätze, Vormerkung)
    - RAG (file_search)
    - Websuche (web_search_preview)
    und liefert die finale Antwort als Text.
    """

    # 1) Routing: Brauchen wir eine SQL-Aktion?
    sql_action = decide_sql_action(messages)

    sql_context_parts: List[str] = []

    action = sql_action.get("action", "none")
    city = sql_action.get("city")
    kennzahl = sql_action.get("kennzahl")
    parent_name = sql_action.get("parent_name")
    parent_email = sql_action.get("parent_email")
    child_name = sql_action.get("child_name")

    # 2) SQL-Aktionen ausführen und Kontext erzeugen
    if action == "list_facilities" and city:
        facilities = get_facilities_by_query(city)
        sql_context_parts.append(format_facilities(facilities, city))

    elif action == "check_free_places" and kennzahl is not None:
        kennzahl_int = int(kennzahl)
        fac = get_facility_by_kennzahl(kennzahl_int)
        free = get_free_places(kennzahl_int)
        if fac:
            name = fac.get("name", f"Einrichtung mit Kennzahl {kennzahl_int}")
        else:
            name = f"Einrichtung mit Kennzahl {kennzahl_int}"

        if free is None:
            txt = (
                f"Für die Einrichtung **{name}** (Kennzahl {kennzahl_int}) liegen "
                "keine Kapazitätsinformationen vor."
            )
        elif free > 0:
            txt = (
                f"Für die Einrichtung **{name}** (Kennzahl {kennzahl_int}) sind nach den aktuellen "
                f"Daten noch ungefähr **{free} Plätze** verfügbar."
            )
        else:
            txt = (
                f"Für die Einrichtung **{name}** (Kennzahl {kennzahl_int}) sind nach den aktuellen "
                "Daten derzeit keine Plätze mehr frei."
            )
        sql_context_parts.append(txt)

    elif (
        action == "reserve_place"
        and kennzahl is not None
        and parent_name
        and parent_email
        and child_name
    ):
        kennzahl_int = int(kennzahl)
        fac = get_facility_by_kennzahl(kennzahl_int)
        ok = reserve_place(kennzahl_int, parent_name, parent_email, child_name)

        if fac:
            name = fac.get("name", f"Einrichtung mit Kennzahl {kennzahl_int}")
        else:
            name = f"Einrichtung mit Kennzahl {kennzahl_int}"

        if ok:
            txt = (
                f"Die Vormerkung für das Kind **{child_name}** bei **{name}** (Kennzahl {kennzahl_int}) "
                "wurde in der Datenbank gespeichert. Die Einrichtung bzw. der Träger kann sich nun "
                "bei Bedarf mit den angegebenen Kontaktdaten melden.\n\n"
                "Hinweis: Die Vormerkung ist noch keine verbindliche Platzzusage."
            )
        else:
            txt = (
                f"Für die Einrichtung **{name}** (Kennzahl {kennzahl_int}) konnte keine Vormerkung "
                "mehr gespeichert werden (vermutlich keine freien Plätze mehr oder Einrichtung nicht gefunden)."
            )
        sql_context_parts.append(txt)

    # 3) SQL-Kontext als zusätzliche System-Nachricht anhängen
    final_messages = list(messages)
    if sql_context_parts:
        sql_context_text = "\n\n".join(sql_context_parts)
        final_messages.append(
            {
                "role": "system",
                "content": (
                    "Folgende Informationen wurden aus der strukturierten Kinderbetreuungsdatenbank ermittelt. "
                    "Du kannst sie für deine Antwort verwenden:\n\n"
                    f"{sql_context_text}"
                ),
            }
        )

    # 4) Großer Modellaufruf mit RAG + Websuche
    response = client.responses.create(
        model="gpt-5.1",
        input=final_messages,
        tools=[
            {"type": "web_search_preview"},
            {"type": "file_search", "vector_store_ids": [VECTOR_STORE_ID]},
        ],
    )

    try:
        raw = getattr(response, "output_text", None)
        if raw:
            return _clean_citations(raw)
    except Exception:
        pass

    first = response.output[0].content[0].text
    raw = getattr(first, "value", str(first))
    return _clean_citations(raw)
