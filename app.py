import os
import streamlit as st
import uuid

from backend.agent import run_agent  # <--- unser Agent
from backend.sql_db import (
    get_facilities_by_query,
    get_free_places,
    reserve_place,
    reset_pre_registrations
)

# -----------------------------
# Streamlit UI-Konfiguration
# -----------------------------
st.set_page_config(page_title="Kinderbetreuungs-Chatbot", page_icon="🦊")

st.title("🦊 LEO – Kinderbetreuungs-Chatbot")
st.write("Stelle Fragen zur Kinderbetreuung in Oberösterreich. Ich helfe dir gerne weiter!.")

# -----------------------------
# Chat-Verlauf im Session State
# -----------------------------
if "messages" not in st.session_state:
    st.session_state.messages = [
        {
            "role": "system",
            "content": (
                "Du bist ein sachlicher, hilfsbereiter Assistent für Fragen zur Kinderbetreuung "
                "in Oberösterreich (der schlaue Fuchs LEO). "
                "Standardmäßig antwortest du auf Deutsch, klar und verständlich. "
                "Wenn Nutzer*innen jedoch eindeutig in einer anderen Sprache schreiben, "
                "antwortest du in derselben Sprache (z.B. Englisch), ohne den Inhalt zu wechseln.\n\n"
                "Dir stehen mehrere Datenquellen zur Verfügung: eine strukturierte Datenbank mit Kinderbetreuungseinrichtungen "
                "und Platzkapazitäten, ein Dokumentenbestand (RAG) und eine Websuche. "
                "Du kannst Einrichtungen und freie Plätze beschreiben und konkrete nächste Schritte vorschlagen.\n\n"
                "WICHTIG: Du führst selbst keine verbindlichen Anmeldungen durch. "
                "Wenn Nutzer*innen ihr Kind vormerken oder anmelden möchten, erkläre ihnen, dass sie dafür "
                "die Vormerkfunktion in der Anwendung nutzen können (Bereich 'Einrichtungen & freie Plätze' in der Seitenleiste) "
                "oder sich direkt an die jeweilige Einrichtung bzw. Gemeinde wenden sollen."
            ),
        }
    ]

# Bisherige Nachrichten anzeigen (ohne system)
for msg in st.session_state.messages:
    if msg["role"] in ("user", "assistant"):
        with st.chat_message("user" if msg["role"] == "user" else "assistant"):
            st.markdown(msg["content"])

# -----------------------------
# Eingabefeld für neue Frage
# -----------------------------
user_input = st.chat_input("Frage zur Kinderbetreuung eingeben...")

if user_input:
    # Nutzer-Nachricht speichern
    st.session_state.messages.append({"role": "user", "content": user_input})

    # Agent aufrufen (Responses API + Websuche)
    assistant_message = run_agent(st.session_state.messages)

    # Antwort speichern
    st.session_state.messages.append(
        {"role": "assistant", "content": assistant_message}
    )

    # Antwort anzeigen
    with st.chat_message("assistant"):
        st.markdown(assistant_message)

# -----------------------------
# Sidebar: Einrichtungen & freie Plätze (Prototyp)
# -----------------------------
with st.sidebar:
    st.subheader("🔎 Einrichtungen & freie Plätze (Prototyp)")

    # Stadt/Gemeinde eingeben
    with st.form("facility_search_form"):
        city_query = st.text_input(
            "Stadt / Gemeinde eingeben (z.B. Linz, Steyr, Wels):",
            value=st.session_state.get("city_query", ""),
        )
        submitted_search = st.form_submit_button("Einrichtungen suchen")

    if submitted_search:
        st.session_state["city_query"] = city_query

    city = st.session_state.get("city_query", "").strip()

    facilities = []
    if city:
        facilities = get_facilities_by_query(city)

        if not facilities:
            st.info(f"Ich habe keine Einrichtungen in {city} gefunden.")
        else:
            st.write(f"Gefundene Einrichtungen in **{city}**:")

            for fac in facilities:
                kennzahl = fac["kennzahl"]
                name = fac["name"]
                ort = fac.get("ort")
                plz = fac.get("plz")
                phone = fac.get("telefon")
                email = fac.get("email")
                url = fac.get("weburl")

                label = f"{name}"
                if ort or plz:
                    label += " ("
                    parts = []
                    if ort:
                        parts.append(ort)
                    if plz:
                        parts.append(str(plz))
                    label += ", ".join(parts) + f", Kennzahl: {kennzahl})"
                else:
                    label += f" (Kennzahl: {kennzahl})"

                with st.expander(f"{name} (Kennzahl: {kennzahl})"):
                    # Kontaktdaten
                    contact_parts = []
                    if phone:
                        contact_parts.append(f"Tel.: {phone}")
                    if email:
                        contact_parts.append(f"E-Mail: {email}")
                    if url:
                        contact_parts.append(f"Web: {url}")
                    if contact_parts:
                        st.write(" | ".join(contact_parts))

                    # Freie Plätze
                    free = get_free_places(kennzahl)
                    if free is None:
                        st.info(
                            "Für diese Einrichtung liegen derzeit keine Angaben zur Platzkapazität vor. "
                            "Bitte wenden Sie sich bei Interesse direkt an die Einrichtung."
                        )
                    elif free > 0:
                        st.success(
                            f"Aktuell sind voraussichtlich noch **{free} Plätze** verfügbar. "
                            "Bitte beachten Sie, dass es sich um eine unverbindliche Einschätzung handelt."
                        )
                    else:
                        st.error(
                            "Derzeit sind nach den vorliegenden Daten **keine Plätze mehr frei**. "
                            "Sie können sich bei dringendem Bedarf dennoch direkt an die Einrichtung wenden."
                        )

                    # Vormerkformular nur, wenn noch Plätze frei
                    if free and free > 0:
                        st.write("### Kind vormerken")
                        with st.form(f"pre_reg_form_{kennzahl}"):
                            child_name = st.text_input(
                                "Name des Kindes",
                                key=f"child_{kennzahl}",
                            )
                            parent_name = st.text_input(
                                "Name der Eltern / Erziehungsberechtigten",
                                key=f"parent_{kennzahl}",
                            )
                            parent_email = st.text_input(
                                "E-Mail",
                                key=f"email_{kennzahl}",
                            )
                            submitted_pre = st.form_submit_button("Kind unverbindlich vormerken")

                        if submitted_pre:
                            ok = reserve_place(kennzahl, parent_name, parent_email, child_name)
                            if ok:
                                st.success(
                                    f"Ihre Vormerkung für **{child_name}** wurde gespeichert. "
                                    "Die Einrichtung bzw. der Träger wird sich bei Bedarf mit Ihnen in Verbindung setzen.\n\n"
                                    "_Hinweis: Die Vormerkung stellt noch keine verbindliche Platzzusage dar._"
                                )
                            else:
                                st.error(
                                    "Leider konnte derzeit keine Vormerkung mehr gespeichert werden. "
                                    "Vermutlich sind die verfügbaren Plätze inzwischen vergeben."
                                )

        # Reset-Button für diese Stadt (nur wenn city gesetzt ist)
        if facilities:
            if st.button(
                "Vormerkungen für diese Stadt zurücksetzen (Test)",
                help="Setzt alle Vormerkungen (pre_registrations) für Einrichtungen in dieser Stadt auf 0.",
            ):
                reset_pre_registrations(city)
                st.success(
                    f"Alle Vormerkungen für Einrichtungen in **{city}** wurden zurückgesetzt. "
                    "Bitte die Suche erneut ausführen, um die aktualisierten freien Plätze zu sehen."
                )


# -----------------------------
# Im Hauptfenster: Einrichtungen & freie Plätze (Prototyp)
# -----------------------------

# st.markdown("---")
# st.subheader("🔎 Einrichtungen & freie Plätze (Prototyp)")

# with st.form("facility_search_form"):
#     city_query = st.text_input("Stadt / Gemeinde eingeben (z.B. Linz, Steyr, Wels):")
#     submitted_search = st.form_submit_button("Einrichtungen suchen")

# if submitted_search and city_query:
#     facilities = get_facilities_by_city(city_query)

#     if not facilities:
#         st.info(f"Ich habe keine Einrichtungen in {city_query} gefunden.")
#     else:
#         st.write(f"**Gefundene Einrichtungen in {city_query}:**")

#         for fac in facilities:
#             kennzahl = fac["kennzahl"]
#             name = fac["name"]
#             phone = fac.get("telefon")
#             email = fac.get("email")
#             url = fac.get("weburl")

#             with st.expander(f"{name} (Kennzahl: {kennzahl})"):
#                 contact_parts = []
#                 if phone:
#                     contact_parts.append(f"Tel.: {phone}")
#                 if email:
#                     contact_parts.append(f"E-Mail: {email}")
#                 if url:
#                     contact_parts.append(f"Web: {url}")
#                 if contact_parts:
#                     st.write(" | ".join(contact_parts))

#                 # Freie Plätze anzeigen
#                 free = get_free_places(kennzahl)
#                 if free is None:
#                     st.info("Für diese Einrichtung liegen derzeit keine Angaben zur Platzkapazität vor. "
#                             "Bitte wenden Sie sich bei Interesse direkt an den Träger oder die Einrichtung.")
#                 elif free > 0:
#                     st.success(f"Aktuell sind voraussichtlich noch **{free} Plätze** verfügbar. "
#                                 "Bitte beachten Sie, dass es sich um eine unverbindliche Einschätzung handelt.")
#                 else:
#                     st.error("Derzeit sind nach den vorliegenden Daten **keine Plätze mehr frei**. "
#                             "Sie können sich bei dringendem Bedarf dennoch direkt an die Einrichtung wenden.")

#                 # Vormerkformular NUR wenn noch Plätze frei
#                 if free and free > 0:
#                     st.write("### Kind vormerken")
#                     st.caption("Mit der Vormerkung können Sie Ihr Kind unverbindlich für einen Platz in dieser Einrichtung registrieren.")
#                     with st.form(f"pre_reg_form_{kennzahl}"):
#                         child_name = st.text_input("Name des Kindes", key=f"child_{kennzahl}")
#                         parent_name = st.text_input("Name der Eltern / Erziehungsberechtigten", key=f"name_{kennzahl}")
#                         parent_email = st.text_input("E-Mail", key=f"email_{kennzahl}")
#                         submitted_pre = st.form_submit_button("Kind vormerken")

#                     if submitted_pre:
#                         ok = reserve_place(kennzahl, parent_name, parent_email)
#                         if ok:
#                             st.success(f"Ihre Vormerkung für **{child_name}** wurde gespeichert. "
#                                         "Die Einrichtung bzw. der Träger wird sich bei Bedarf mit Ihnen in Verbindung setzen.\n\n"
#                                         "_Hinweis: Die Vormerkung stellt noch keine verbindliche Platzzusage dar._")
#                         else:
#                             st.error("Leider konnte derzeit keine Vormerkung mehr gespeichert werden. "
#                                     "Vermutlich sind die verfügbaren Plätze inzwischen vergeben. "
#                                     "Bitte wenden Sie sich bei dringendem Bedarf direkt an die Einrichtung.")

