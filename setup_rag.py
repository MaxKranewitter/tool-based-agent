import os
from backend.rag import create_vector_store_if_not_exists, upload_files_to_vector_store

# Sicherstellen, dass der API-Key gesetzt ist
if not os.environ.get("OPENAI_API_KEY"):
    raise RuntimeError("Bitte OPENAI_API_KEY als Umgebungsvariable setzen.")

# 1) Vector Store erstellen
vector_store_id = create_vector_store_if_not_exists()
print(f"Erstellter Vector Store: {vector_store_id}")

# 2) Lokale Dateien definieren, die hochgeladen werden sollen
#    Passe diese Pfade an deine tatsächlichen Dateien an
file_paths = [
    # OÖ Sozialratgeber
    r"/Users/mkranewitter/Desktop/MSc DSE/4. Semester/Masterarbeit/Daten/Für RAG/Ratgeber/ooe_sozialratgeber_2025.pdf",

    # Merkblatt Kindergartenpflicht
    r"/Users/mkranewitter/Desktop/MSc DSE/4. Semester/Masterarbeit/Daten/Für RAG/Merkblätter, Leitfäden, Elterninformation/Merkblatt Kindergartenpflicht - Stand 07-2025.pdf",

    # Bildungsdirektion OÖ – verschiedene PDFs
    r"/Users/mkranewitter/Desktop/MSc DSE/4. Semester/Masterarbeit/Daten/Für RAG/Webseite Bildungsdirektion OÖ/anmeldung_kbbes.pdf",
    r"/Users/mkranewitter/Desktop/MSc DSE/4. Semester/Masterarbeit/Daten/Für RAG/Webseite Bildungsdirektion OÖ/arten_kbbes.pdf",
    r"/Users/mkranewitter/Desktop/MSc DSE/4. Semester/Masterarbeit/Daten/Für RAG/Webseite Bildungsdirektion OÖ/einstieg_kindergarten.pdf",
    r"/Users/mkranewitter/Desktop/MSc DSE/4. Semester/Masterarbeit/Daten/Für RAG/Webseite Bildungsdirektion OÖ/KBBE in OÖ Bildungsdirektion Oberösterreich.pdf",
    r"/Users/mkranewitter/Desktop/MSc DSE/4. Semester/Masterarbeit/Daten/Für RAG/Webseite Bildungsdirektion OÖ/Kindergartenpflicht , Bildungsdirektion Oberösterreich.pdf",
]

if not file_paths:
    print("Keine Dateien angegeben. Bitte file_paths in setup_rag.py anpassen.")
else:
    upload_files_to_vector_store(vector_store_id, file_paths)
    print("Dateien wurden hochgeladen.")