"""
LangSmith Evaluation Script für den Kinderbetreuungs-Chatbot (LEO).

- Lädt API Keys aus .streamlit/secrets.toml
- Erstellt oder verwendet ein LangSmith-Dataset
- Definiert den Chatbot als Target für die Evaluierung
- Führt mehrere LLM-as-a-Judge Evaluatoren aus:
  - correctness
  - context_relevance
  - faithfulness
  - inconclusive_behavior

WICHTIG: Alle Evaluatoren geben Scores im Bereich 0.0–1.0 zurück,
damit sie mit der LangSmith-Feedback-Konfiguration kompatibel sind.
"""

import os
import sys
from typing import Dict, Any, List

import streamlit as st
from langsmith import Client
from langsmith.utils import LangSmithConflictError

from openevals.llm import create_llm_as_judge

# ---------------------------------------------------------------------------
# Projektstruktur & Agent-Import
# ---------------------------------------------------------------------------

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from backend.agent import run_agent  # type: ignore


# ---------------------------------------------------------------------------
# Konfiguration
# ---------------------------------------------------------------------------

# DATASET_NAME = "kinderbetreuung-eval-sql-bucket"
DATASET_NAME = "kinderbetreuung-eval-rag-bucket"
# DATASET_NAME = "kinderbetreuung-eval-web-bucket"

examples: List[Dict[str, Any]] = [
    {
        "inputs": {
            "question": "Wer hat den Oö. Sozialratgeber 2025 herausgegeben?"
        },
        "outputs": {
            "expected": "Die Antwort soll die offizielle Herausgeberorganisation nennen (z. B. Land Oberösterreich bzw. die zuständige Abteilung/Sozialabteilung) und klar machen, dass es sich um eine Publikation des Landes handelt."
        },
    },
    {
        "inputs": {
            "question": "Welche Stellen oder Organisationen waren an der Erstellung des Oö. Sozialratgebers 2025 beteiligt?"
        },
        "outputs": {
            "expected": "Die Antwort soll die im Dokument genannten beteiligten Stellen (z. B. Fachabteilungen, Sozialorganisationen, Projektpartner) aufgreifen und kurz zusammenfassen, dass der Sozialratgeber in Zusammenarbeit mehrerer Akteure entstanden ist."
        },
    },
    {
        "inputs": {
            "question": "Wofür ist der Oö. Sozialratgeber 2025 gedacht?"
        },
        "outputs": {
            "expected": "Die Antwort soll erklären, dass der Sozialratgeber 2025 einen Überblick über soziale Angebote und Unterstützungsleistungen in Oberösterreich bietet und Bürgerinnen und Bürgern als Orientierungshilfe in sozialen Fragen dient."
        },
    },
    {
        "inputs": {
            "question": "Welche Inhalte deckt der Oö. Sozialratgeber 2025 in Bezug auf Kinderbetreuung ab?"
        },
        "outputs": {
            "expected": "Die Antwort soll darauf eingehen, dass der Sozialratgeber Informationen zu Kinderbetreuungsangeboten, Zuständigkeiten und Anlaufstellen in Oberösterreich enthält und diese im Kontext des gesamten sozialen Versorgungssystems einordnet."
        },
    },
]




# examples: List[Dict[str, Any]] = [
#     {
#         "inputs": {"question": "Wie melde ich mein Kind im Kindergarten Linz an?"},
#         "outputs": {
#             "expected": "Die Antwort soll auf die offizielle Website der Stadt Linz verweisen und das dort beschriebene Anmeldeverfahren erwähnen."
#         },
#     },
#     {
#         "inputs": {"question": "Wie melde ich mein Kind im Kindergarten Steyr an?"},
#         "outputs": {
#             "expected": "Die Antwort soll auf die offizielle Website der Stadt Steyr verweisen und die dort beschriebenen Schritte zur Anmeldung nennen."
#         },
#     },
#     {
#         "inputs": {"question": "Wie melde ich mein Kind im Kindergarten Wels an?"},
#         "outputs": {
#             "expected": "Die Antwort soll auf die offizielle Website der Stadt Wels verweisen und kurz das dortige Anmeldeformular bzw. Kontaktmöglichkeiten nennen."
#         },
#     },
# ]


# ---------------------------------------------------------------------------
# Secrets & Environment Variablen
# ---------------------------------------------------------------------------

OPENAI_KEY = st.secrets["OPENAI_API_KEY"]
LANGSMITH_KEY = st.secrets["LANGCHAIN_API_KEY"]
LANGSMITH_PROJECT = st.secrets.get("LANGCHAIN_PROJECT", "Kinderbetreuung-Chatbot")

os.environ["OPENAI_API_KEY"] = OPENAI_KEY
os.environ["LANGCHAIN_API_KEY"] = LANGSMITH_KEY
os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_PROJECT"] = LANGSMITH_PROJECT
os.environ["LANGCHAIN_ENDPOINT"] = "https://eu.api.smith.langchain.com"

# optional neue Namenskonvention, schadet nicht:
os.environ["LANGSMITH_API_KEY"] = LANGSMITH_KEY
os.environ["LANGSMITH_PROJECT"] = LANGSMITH_PROJECT
os.environ["LANGSMITH_ENDPOINT"] = "https://eu.api.smith.langchain.com"


# ---------------------------------------------------------------------------
# LangSmith Client & Dataset
# ---------------------------------------------------------------------------

client = Client()


def ensure_dataset() -> Any:
    """Erstellt das Dataset in LangSmith oder liest ein bestehendes ein."""
    try:
        dataset = client.create_dataset(
            dataset_name=DATASET_NAME,
            description="Testset für den Kinderbetreuungs-Chatbot (LEO)",
        )
        print(f"[LangSmith] Dataset neu erstellt: {DATASET_NAME}")
        client.create_examples(dataset_id=dataset.id, examples=examples)
        print(f"[LangSmith] {len(examples)} Beispiele zum Dataset hinzugefügt.")
    except LangSmithConflictError:
        dataset = client.read_dataset(dataset_name=DATASET_NAME)
        print(f"[LangSmith] Bestehendes Dataset verwendet: {DATASET_NAME}")
    return dataset


# ---------------------------------------------------------------------------
# Target: Chatbot-Funktion für LangSmith
# ---------------------------------------------------------------------------

def target(inputs: Dict[str, Any]) -> Dict[str, str]:
    """
    Wird von LangSmith für jedes Beispiel aufgerufen.
    inputs entspricht dem 'inputs'-Dict aus dem Dataset, z.B. {"question": "..."}.
    """

    question = inputs.get("question", "")
    messages = [{"role": "user", "content": question}]
    answer = run_agent(messages)

    # Optional: später contexts ergänzen (answer, contexts = run_agent(...))
    return {"answer": answer}


# ---------------------------------------------------------------------------
# LLM-as-a-Judge Prompts (Skala 0.0–1.0)
# ---------------------------------------------------------------------------

CORRECTNESS_PROMPT = """
Du bist ein sachlicher Gutachter.

Du erhältst:
- inputs: die Eingabe des Nutzers (z.B. die Frage),
- outputs: die Antwort des Chatbots,
- reference_outputs: eine Referenzbeschreibung, was inhaltlich erwartet wird.

inputs:
{inputs}

outputs:
{outputs}

reference_outputs:
{reference_outputs}

Bewerte auf einer Skala von 0.0 bis 1.0, wie gut die Antwort des Chatbots die
Referenz inhaltlich trifft.

0.0 = Antwort verfehlt den Kern weitgehend oder ist falsch.
0.5 = Antwort trifft Teile der Referenz, lässt aber Wichtiges aus.
1.0 = Antwort deckt die Referenz inhaltlich sehr gut ab.

Gib NUR eine Zahl zwischen 0.0 und 1.0 aus (z.B. 0.0, 0.4, 0.7, 1.0).
"""

CONTEXT_RELEVANCE_PROMPT = """
Du bist ein sachlicher Gutachter.

Du erhältst:
- inputs: die Nutzereingabe (z.B. Frage),
- outputs: die vom System erzeugten Inhalte (z.B. Antwort oder Kontexte).

inputs:
{inputs}

outputs:
{outputs}

Bewerte auf einer Skala von 0.0 bis 1.0, wie relevant outputs für die Beantwortung
von inputs ist.

0.0 = kaum relevant,
0.5 = teils relevant, teils irrelevant,
1.0 = sehr relevant und direkt hilfreich.

Gib NUR eine Zahl zwischen 0.0 und 1.0 aus.
"""

FAITHFULNESS_PROMPT = """
Du bist ein sachlicher Gutachter.

Du erhältst:
- inputs: die Frage,
- outputs: die Antwort des Chatbots,
- reference_outputs: zusätzliche Informationen (z.B. Referenztexte oder Kontext).

inputs (Frage):
{inputs}

outputs (Antwort des Chatbots):
{outputs}

reference_outputs (Kontext oder Referenz):
{reference_outputs}

Bewerte auf einer Skala von 0.0 bis 1.0, wie gut die Antwort durch reference_outputs
gestützt ist.

0.0 = größtenteils nicht gedeckt / wirkt halluziniert,
0.5 = teilweise gedeckt, aber mit Lücken oder spekulativen Anteilen,
1.0 = vollständig gedeckt, keine offensichtlichen Halluzinationen.

Gib NUR eine Zahl zwischen 0.0 und 1.0 aus.
"""

INCONCLUSIVE_PROMPT = """
Du bist ein sachlicher Gutachter.

Du erhältst:
- inputs: die Nutzerfrage,
- outputs: die Antwort des Chatbots,
- reference_outputs: eine Beschreibung, was im Idealfall passieren soll.

inputs (Frage):
{inputs}

outputs (Antwort des Chatbots):
{outputs}

reference_outputs (Erwartung):
{reference_outputs}

Bewerte auf einer Skala von 0.0 bis 1.0:

1.0 = Der Chatbot erklärt nachvollziehbar, dass keine ausreichenden Informationen
      vorliegen (oder verweist korrekt auf fehlende Zuständigkeit), und spekuliert nicht.
0.0 = Der Chatbot erfindet offensichtlich Inhalte oder gibt eine scheinbar
      selbstbewusste Antwort, obwohl klar ist, dass die Informationen nicht vorliegen.

Gib NUR eine Zahl zwischen 0.0 und 1.0 aus.
"""


evaluator_correctness = create_llm_as_judge(
    prompt=CORRECTNESS_PROMPT,
    model="gpt-4o-mini",
    feedback_key="correctness",
)

evaluator_context_relevance = create_llm_as_judge(
    prompt=CONTEXT_RELEVANCE_PROMPT,
    model="gpt-4o-mini",
    feedback_key="context_relevance",
)

evaluator_faithfulness = create_llm_as_judge(
    prompt=FAITHFULNESS_PROMPT,
    model="gpt-4o-mini",
    feedback_key="faithfulness",
)

evaluator_inconclusive = create_llm_as_judge(
    prompt=INCONCLUSIVE_PROMPT,
    model="gpt-4o-mini",
    feedback_key="inconclusive_behavior",
)


# ---------------------------------------------------------------------------
# Evaluator-Wrapper für LangSmith (run + example)
# ---------------------------------------------------------------------------

def correctness_evaluator(run, example, langsmith_extra=None):
    return evaluator_correctness(
        inputs=example.inputs or {},
        outputs=run.outputs or {},
        reference_outputs=example.outputs or {},
    )


def context_relevance_evaluator(run, example, langsmith_extra=None):
    return evaluator_context_relevance(
        inputs=example.inputs or {},
        outputs=run.outputs or {},
        reference_outputs=example.outputs or {},
    )


def faithfulness_evaluator(run, example, langsmith_extra=None):
    return evaluator_faithfulness(
        inputs=example.inputs or {},
        outputs=run.outputs or {},
        reference_outputs=example.outputs or {},
    )


def inconclusive_evaluator(run, example, langsmith_extra=None):
    return evaluator_inconclusive(
        inputs=example.inputs or {},
        outputs=run.outputs or {},
        reference_outputs=example.outputs or {},
    )


# ---------------------------------------------------------------------------
# Hauptfunktion
# ---------------------------------------------------------------------------

def main() -> None:
    dataset = ensure_dataset()

    print(f"[LangSmith] Starte Evaluation auf Dataset: {dataset.name}")

    _ = client.evaluate(
        target,
        data=DATASET_NAME,
        evaluators=[
            correctness_evaluator,
            context_relevance_evaluator,
            faithfulness_evaluator,
            inconclusive_evaluator,
        ],
        experiment_prefix="eval-run-",
    )

    print("[LangSmith] Evaluation gestartet. Ergebnisse im LangSmith-Dashboard sichtbar.")


if __name__ == "__main__":
    main()
