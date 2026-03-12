import os
from dotenv import load_dotenv

load_dotenv()
# --- Neo4j Config ---
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASS", "password123")

# --- LLM Config ---
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openai").lower()
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")

# Set a reasonable default embedding model if not provided
# 'all-minilm' is extremely lightweight (~23MB) and perfect for local labs.
_default_emb = "all-minilm" if LLM_PROVIDER == "ollama" else "text-embedding-3-small"
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", _default_emb)

# --- Chatbot Settings ---
MAX_CYPHER_RETRIES = 2
SUGGESTED_QUESTIONS = [
    "Who co-starred with Keanu Reeves in The Matrix?",
    "Which actors have appeared in more than 3 movies?",
    "Who directed the movie Cast Away?",
    "Find the co-actors of co-actors of Keanu Reeves (2-hop).",
    "Which person directed and acted in the same movie?"
]
