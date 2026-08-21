#!/usr/bin/env python3
"""
embed_consumer.py

Reads Debezium CDC events from the `support.public.ticket_resolutions`
Kafka topic, embeds the resolution note text with a local sentence-
transformers model, and upserts the result into the pgvector store
(`resolution_embeddings` table).

Run this AFTER:
  - the Debezium connector is registered and RUNNING
  - the vectordb container is up with the schema applied

Usage:
    python embed_consumer.py

Runs forever, consuming new messages as they arrive (including the
initial snapshot Debezium produced on connector startup). Ctrl+C to stop.

Env vars (override with flags if you prefer):
    KAFKA_BOOTSTRAP=localhost:9092
    KAFKA_TOPIC=support.public.ticket_resolutions
    KAFKA_GROUP_ID=embed-consumer

    VECTORDB_HOST=localhost
    VECTORDB_PORT=5433
    VECTORDB_NAME=ragdb
    VECTORDB_USER=raguser
    VECTORDB_PASSWORD=ragpass

    SOURCE_DB_HOST=localhost
    SOURCE_DB_PORT=5432
    SOURCE_DB_NAME=supportdb
    SOURCE_DB_USER=appuser
    SOURCE_DB_PASSWORD=apppass
"""

import json
import os

import psycopg2
from kafka import KafkaConsumer
from sentence_transformers import SentenceTransformer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "support.public.ticket_resolutions")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "embed-consumer")

VECTORDB_HOST = os.getenv("VECTORDB_HOST", "localhost")
VECTORDB_PORT = os.getenv("VECTORDB_PORT", "5433")
VECTORDB_NAME = os.getenv("VECTORDB_NAME", "ragdb")
VECTORDB_USER = os.getenv("VECTORDB_USER", "raguser")
VECTORDB_PASSWORD = os.getenv("VECTORDB_PASSWORD", "ragpass")

SOURCE_DB_HOST = os.getenv("SOURCE_DB_HOST", "localhost")
SOURCE_DB_PORT = os.getenv("SOURCE_DB_PORT", "5432")
SOURCE_DB_NAME = os.getenv("SOURCE_DB_NAME", "supportdb")
SOURCE_DB_USER = os.getenv("SOURCE_DB_USER", "appuser")
SOURCE_DB_PASSWORD = os.getenv("SOURCE_DB_PASSWORD", "apppass")

# Debezium "op" codes worth acting on:
#   c = create, u = update, r = read (initial snapshot)
# "d" (delete) is intentionally not handled here yet -- see note at bottom.
UPSERT_OPS = {"c", "u", "r"}

TICKET_LOOKUP_SQL = """
    SELECT subject, body FROM tickets WHERE ticket_id = %s
"""

UPSERT_SQL = """
    INSERT INTO resolution_embeddings
        (ticket_id, resolution_id, agent_name, resolved_at, ticket_subject, ticket_body, chunk_text, chunk_index, embedding)
    VALUES (%s, %s, %s, %s::timestamptz, %s, %s, %s, 0, %s)
    ON CONFLICT (resolution_id, chunk_index)
    DO UPDATE SET
        chunk_text     = EXCLUDED.chunk_text,
        embedding      = EXCLUDED.embedding,
        agent_name     = EXCLUDED.agent_name,
        resolved_at    = EXCLUDED.resolved_at,
        ticket_subject = EXCLUDED.ticket_subject,
        ticket_body    = EXCLUDED.ticket_body
"""


def get_vectordb_connection():
    return psycopg2.connect(
        host=VECTORDB_HOST,
        port=VECTORDB_PORT,
        dbname=VECTORDB_NAME,
        user=VECTORDB_USER,
        password=VECTORDB_PASSWORD,
    )


def get_source_db_connection():
    return psycopg2.connect(
        host=SOURCE_DB_HOST,
        port=SOURCE_DB_PORT,
        dbname=SOURCE_DB_NAME,
        user=SOURCE_DB_USER,
        password=SOURCE_DB_PASSWORD,
    )


def build_embedding_text(subject, body, note):
    """Combine ticket context with the resolution note. Embedding the note
    alone loses the vocabulary that actually distinguishes one issue
    category from another (e.g. billing vs. login vs. shipping)."""
    return f"Ticket: {subject}\n{body}\n\nResolution: {note}"


def main():
    print(f"Loading embedding model (all-MiniLM-L6-v2)...")
    model = SentenceTransformer("all-MiniLM-L6-v2")
    print("Model loaded.")

    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP}, topic '{KAFKA_TOPIC}'...")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",   # pick up the initial snapshot too
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
    )

    vconn = get_vectordb_connection()
    vconn.autocommit = True
    vcur = vconn.cursor()

    sconn = get_source_db_connection()
    sconn.autocommit = True
    scur = sconn.cursor()

    print("Listening for messages... (Ctrl+C to stop)")
    try:
        for message in consumer:
            envelope = message.value
            if envelope is None:
                continue  # tombstone message, nothing to do

            op = envelope.get("op")
            after = envelope.get("after")

            if op not in UPSERT_OPS or after is None:
                continue

            resolution_id = after["resolution_id"]
            ticket_id = after["ticket_id"]
            agent_name = after.get("agent_name")
            note = after["note"]
            resolved_at = after.get("resolved_at")  # Debezium sends ISO 8601 string for timestamptz columns

            scur.execute(TICKET_LOOKUP_SQL, (ticket_id,))
            row = scur.fetchone()
            if row is None:
                print(f"WARNING: ticket_id={ticket_id} not found in source DB, skipping resolution_id={resolution_id}")
                continue
            subject, body = row

            embedding_text = build_embedding_text(subject, body, note)
            embedding = model.encode(embedding_text).tolist()

            vcur.execute(
                UPSERT_SQL,
                (ticket_id, resolution_id, agent_name, resolved_at, subject, body, embedding_text, embedding),
            )
            print(f"Upserted resolution_id={resolution_id} (ticket_id={ticket_id})")

    except KeyboardInterrupt:
        print("\nStopping consumer.")
    finally:
        vcur.close()
        vconn.close()
        scur.close()
        sconn.close()
        consumer.close()


if __name__ == "__main__":
    main()

# ---------------------------------------------------------------------------
# Note on deletes: this consumer does not currently remove embeddings when a
# ticket_resolutions row is deleted upstream (op == "d"). Since your
# REPLICA IDENTITY FULL setup means the "before" image is available on
# deletes, you could handle this by adding:
#   if op == "d":
#       before = envelope.get("before")
#       cur.execute("DELETE FROM resolution_embeddings WHERE resolution_id = %s", (before["resolution_id"],))
# Left out for now since your data generator doesn't delete resolutions.
# ---------------------------------------------------------------------------