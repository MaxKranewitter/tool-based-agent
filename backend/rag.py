import os
from typing import List

from openai import OpenAI

client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

VECTOR_STORE_NAME = "kinderbetreuung_rag_store"


def create_vector_store_if_not_exists() -> str:
    vs = client.vector_stores.create(
        name=VECTOR_STORE_NAME,
        metadata={"project": "Kinderbetreuung RAG"},
    )
    return vs.id


def upload_files_to_vector_store(vector_store_id: str, file_paths: List[str]) -> None:
    """
    Lädt die angegebenen Dateien nacheinander in den Vector Store.
    In deiner openai-Version erwartet upload_and_poll ein Argument 'file'
    und keine Liste 'files'.
    """
    for path in file_paths:
        print(f"Lade Datei hoch: {path}")
        with open(path, "rb") as f:
            client.vector_stores.files.upload_and_poll(
                vector_store_id=vector_store_id,
                file=f,
            )
    print("Alle Dateien wurden hochgeladen.")