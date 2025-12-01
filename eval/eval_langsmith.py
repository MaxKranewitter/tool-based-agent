import os
import sys
import streamlit as st  # für secrets
from langsmith import Client
from langsmith.utils import LangSmithConflictError
from typing import Dict, Any

# Projektwurzel (agentic_rag_chatbot) zum Python-Suchpfad hinzufügen
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# Damit wir den Agenten importieren können
from backend.agent import run_agent

# Konfiguration: Name des Test-Datasets
# HIER ANPASSEN je nach Testset, z.B.:
# "kinderbetreuung-eval-v1-basis"
# "kinderbetreuung-eval-v2-beihilfen"
# "kinderbetreuung-eval-v3-linz-steyr-wels"
DATASET_NAME = "kinderbetreuung-eval-v3-linz-steyr-wels"

# --- Load secrets from .streamlit/secrets.toml ---
# (Streamlit lädt secrets sonst nur in der App)
OPENAI_KEY = st.secrets["OPENAI_API_KEY"]
LANGSMITH_KEY = st.secrets["LANGCHAIN_API_KEY"]
LANGSMITH_PROJECT = st.secrets.get("LANGCHAIN_PROJECT", "Kinderbetreuung-Chatbot")


# --- Set environment variables for LangSmith ---
os.environ["OPENAI_API_KEY"] = OPENAI_KEY
os.environ["LANGCHAIN_API_KEY"] = LANGSMITH_KEY
os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_PROJECT"] = LANGSMITH_PROJECT
os.environ["LANGCHAIN_ENDPOINT"] = "https://eu.api.smith.langchain.com"


# --- Create LangSmith client ---
client = Client()


# --- Test dataset ---
examples = [
    {
        "inputs": {"question": "Wie melde ich mein Kind im Kindergarten Linz an?"},
        "outputs": {"expected": "Antwort soll auf Stadt Linz verweisen."},
    },
    {
        "inputs": {"question": "Wie melde ich mein Kind im Kindergarten Steyr an?"},
        "outputs": {"expected": "Antwort soll auf Stadt Steyr verweisen."},
    },
    {
        "inputs": {"question": "Wie melde ich mein Kind im Kindergarten Wels an?"},
        "outputs": {"expected": "Antwort soll auf Stadt Wels verweisen."},
    },
]


# --- Create dataset in LangSmith ---
try:
    dataset = client.create_dataset(
        dataset_name=DATASET_NAME,
        description="Testset für den Kinderbetreuungs-Chatbot",
    )
    print(f"Dataset neu erstellt: {DATASET_NAME}")
    client.create_examples(
        dataset_id=dataset.id,
        examples=examples,
    )
    print("Beispiele zum Dataset hinzugefügt.")
except LangSmithConflictError:
    dataset = client.read_dataset(dataset_name=DATASET_NAME)
    print(f"Bestehendes Dataset verwendet: {DATASET_NAME}")


# --- Define model target (DEIN chatbot) ---
def target(inputs: Dict[str, Any]) -> Dict[str, str]:
    """
    Wird von LangSmith für jedes Beispiel aufgerufen.
    Inputs entspricht dem 'inputs'-Dict aus dem Dataset:
        {"question": "..."}
    """
    question = inputs["question"]
    messages = [{"role": "user", "content": question}]

    answer = run_agent(messages)
    return {"answer": answer}


# --- OPTIONAL: LLM-as-a-judge evaluator ---
from openevals.llm import create_llm_as_judge
from openevals.prompts import CORRECTNESS_PROMPT

def correctness_evaluator(inputs: Dict, outputs: Dict, reference_outputs: Dict):
    """
    Prüft: Wie gut deckt die Antwort die erwarteten Punkte ab?
    """
    evaluator = create_llm_as_judge(
        prompt=CORRECTNESS_PROMPT,
        model="gpt-4o-mini",   # billig & gut für Bewertung
        feedback_key="correctness",
    )

    return evaluator(
        inputs=inputs,
        outputs=outputs,
        reference_outputs=reference_outputs
    )


# --- Run experiment ---
results = client.evaluate(
    target,
    data=DATASET_NAME,
    evaluators=[correctness_evaluator],
    experiment_prefix="eval-run-",
)

print("Experiment gestartet. Ergebnisse in LangSmith sichtbar.")
