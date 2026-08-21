#!/usr/bin/env python3
"""
test_retrieval.py

Sanity check for the retrieval half of the pipeline. Takes a text query,
embeds it with the same model used by the consumer (all-MiniLM-L6-v2),
and runs a cosine-similarity search against `resolution_embeddings` in
pgvector. Prints the top-k matches with their similarity score.

Usage:
    python test_retrieval.py "customer was charged twice"
    python test_retrieval.py "customer was charged twice" --top-k 3

With no query argument, drops into an interactive loop so you can try
several queries without reloading the model each time.

Env vars (override with flags if you prefer):
    VECTORDB_HOST=localhost
    VECTORDB_PORT=5433
    VECTORDB_NAME=ragdb
    VECTORDB_USER=raguser
    VECTORDB_PASSWORD=ragpass
"""

import argparse
import os

import psycopg2
from sentence_transformers import SentenceTransformer

VECTORDB_HOST = os.getenv("VECTORDB_HOST", "localhost")
VECTORDB_PORT = os.getenv("VECTORDB_PORT", "5433")
VECTORDB_NAME = os.getenv("VECTORDB_NAME", "ragdb")
VECTORDB_USER = os.getenv("VECTORDB_USER", "raguser")
VECTORDB_PASSWORD = os.getenv("VECTORDB_PASSWORD", "ragpass")

# pgvector's <=> operator returns cosine DISTANCE (0 = identical, 2 = opposite).
# We convert to similarity (1 - distance) for a more intuitive 0-1-ish score.
SEARCH_SQL = """
    SELECT
        ticket_id,
        resolution_id,
        agent_name,
        ticket_subject,
        chunk_text,
        1 - (embedding <=> %s::vector) AS similarity
    FROM resolution_embeddings
    ORDER BY embedding <=> %s::vector
    LIMIT %s
"""


def get_db_connection():
    return psycopg2.connect(
        host=VECTORDB_HOST,
        port=VECTORDB_PORT,
        dbname=VECTORDB_NAME,
        user=VECTORDB_USER,
        password=VECTORDB_PASSWORD,
    )


def run_query(cur, model, query_text, top_k):
    query_embedding = model.encode(query_text).tolist()
    cur.execute(SEARCH_SQL, (query_embedding, query_embedding, top_k))
    results = cur.fetchall()

    print(f"\nQuery: \"{query_text}\"")
    print("-" * 60)
    if not results:
        print("No results -- is resolution_embeddings populated yet?")
        return

    for i, (ticket_id, resolution_id, agent_name, ticket_subject, chunk_text, similarity) in enumerate(results, start=1):
        print(f"{i}. similarity={similarity:.4f}  ticket_id={ticket_id}  resolution_id={resolution_id}  agent={agent_name}")
        print(f"   subject: {ticket_subject}")
        print(f"   {chunk_text}")
        print()


def main():
    parser = argparse.ArgumentParser(description="Test similarity search against resolution_embeddings")
    parser.add_argument("query", nargs="?", help="Query text. If omitted, starts an interactive loop.")
    parser.add_argument("--top-k", type=int, default=5)
    args = parser.parse_args()

    print("Loading embedding model (all-MiniLM-L6-v2)...")
    model = SentenceTransformer("all-MiniLM-L6-v2")
    print("Model loaded.")

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        if args.query:
            run_query(cur, model, args.query, args.top_k)
        else:
            print("\nInteractive mode -- type a query and press Enter. Ctrl+C to quit.\n")
            while True:
                try:
                    query_text = input("query> ").strip()
                    if query_text:
                        run_query(cur, model, query_text, args.top_k)
                except KeyboardInterrupt:
                    print("\nExiting.")
                    break
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()