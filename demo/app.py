"""
Demo UI: simulates two agents.
  - "Resolve a Ticket" panel writes a resolution note into the SOURCE Postgres
    (ticket_resolutions table). That write triggers Debezium -> Kafka -> your
    consumer -> embedding -> insert into pgvector, completely outside this app.
  - "Search Resolutions" panel embeds a query and searches the VECTOR DB
    directly, so you can watch a just-written resolution become retrievable
    within seconds.

Run:
    streamlit run app.py
"""

import os
import time
import datetime

import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor
from sentence_transformers import SentenceTransformer

# ---------------------------------------------------------------------------
# Config (override via env vars if your ports/creds differ)
# ---------------------------------------------------------------------------
SOURCE_DB = dict(
    host=os.getenv("SOURCE_DB_HOST", "localhost"),
    port=os.getenv("SOURCE_DB_PORT", "5432"),
    user=os.getenv("SOURCE_DB_USER", "appuser"),
    password=os.getenv("SOURCE_DB_PASSWORD", "apppass"),
    dbname=os.getenv("SOURCE_DB_NAME", "supportdb"),
)

VECTOR_DB = dict(
    host=os.getenv("VECTOR_DB_HOST", "localhost"),
    port=os.getenv("VECTOR_DB_PORT", "5433"),
    user=os.getenv("VECTOR_DB_USER", "vectoruser"),
    password=os.getenv("VECTOR_DB_PASSWORD", "vectorpass"),
    dbname=os.getenv("VECTOR_DB_NAME", "ragdb"),
)

EMBED_MODEL_NAME = "all-MiniLM-L6-v2"


@st.cache_resource
def load_model():
    return SentenceTransformer(EMBED_MODEL_NAME)


def get_source_conn():
    return psycopg2.connect(**SOURCE_DB)


def get_vector_conn():
    return psycopg2.connect(**VECTOR_DB)


def fetch_open_tickets():
    with get_source_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT ticket_id, subject, body
                FROM tickets
                WHERE status = 'open'
                ORDER BY created_at DESC
                LIMIT 50
                """
            )
            return cur.fetchall()


def resolve_ticket(ticket_id: int, agent_name: str, note: str):
    """Writes the resolution + flips ticket status. This is the write that
    Debezium picks up and turns into a CDC event."""
    with get_source_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ticket_resolutions (ticket_id, agent_name, note, resolved_at)
                VALUES (%s, %s, %s, now())
                RETURNING resolution_id
                """,
                (ticket_id, agent_name, note),
            )
            resolution_id = cur.fetchone()[0]
            cur.execute(
                "UPDATE tickets SET status = 'resolved', updated_at = now() WHERE ticket_id = %s",
                (ticket_id,),
            )
        conn.commit()
    return resolution_id


def search_resolutions(query_text: str, top_k: int = 5):
    model = load_model()
    query_vec = model.encode(query_text).tolist()
    with get_vector_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT resolution_id, ticket_id, chunk_text, agent_name,
                       resolved_at, embedding <=> %s::vector AS distance
                FROM resolution_embeddings
                ORDER BY embedding <=> %s::vector
                LIMIT %s
                """,
                (query_vec, query_vec, top_k),
            )
            return cur.fetchall()


def check_resolution_indexed(resolution_id: int) -> bool:
    with get_vector_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM resolution_embeddings WHERE resolution_id = %s LIMIT 1",
                (resolution_id,),
            )
            return cur.fetchone() is not None


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
st.set_page_config(page_title="Real-Time RAG Demo", layout="wide")
st.title("Real-Time RAG Data Pipeline — Live Demo")
st.caption(
    "Resolve a ticket on the left. It flows Postgres -> Debezium -> Kafka -> "
    "embedding consumer -> pgvector, outside this app. Search on the right "
    "to see how fast it becomes retrievable."
)

col1, col2 = st.columns(2)

# --- Panel 1: Agent resolves a ticket -------------------------------------
with col1:
    st.header("Agent — Resolve a Ticket")

    try:
        tickets = fetch_open_tickets()
    except Exception as e:
        st.error(f"Could not reach source DB: {e}")
        tickets = []

    if not tickets:
        st.info("No open tickets found. Seed some tickets first.")
    else:
        ticket_options = {f"#{t['ticket_id']} — {t['subject']}": t["ticket_id"] for t in tickets}
        selected_label = st.selectbox("Open ticket", list(ticket_options.keys()))
        selected_ticket_id = ticket_options[selected_label]

        agent_name = st.text_input("Agent name", value="Sarah")
        note = st.text_area(
            "Resolution note",
            placeholder="Carrier XYZ outage, package delayed not lost, advise 48hr wait.",
            height=100,
        )

        if st.button("Submit resolution", type="primary"):
            if not note.strip():
                st.warning("Write a resolution note first.")
            else:
                t0 = time.time()
                resolution_id = resolve_ticket(selected_ticket_id, agent_name, note)
                st.success(f"Resolution #{resolution_id} written to source DB.")

                with st.spinner("Waiting for it to appear in the vector DB..."):
                    indexed = False
                    for _ in range(30):  # poll up to ~15s
                        if check_resolution_indexed(resolution_id):
                            indexed = True
                            break
                        time.sleep(0.5)

                elapsed = time.time() - t0
                if indexed:
                    st.success(f"Searchable in vector DB after **{elapsed:.1f}s**.")
                else:
                    st.warning(
                        "Not indexed yet after 15s — check the consumer is running "
                        "and the connector status in the console UI."
                    )

# --- Panel 2: Search resolutions -------------------------------------------
with col2:
    st.header("Search — Find Similar Resolutions")

    query = st.text_input(
        "What's the customer asking?",
        placeholder="my package says delivered but I never got it",
    )

    if st.button("Search"):
        if not query.strip():
            st.warning("Type a query first.")
        else:
            try:
                results = search_resolutions(query, top_k=5)
            except Exception as e:
                st.error(f"Could not reach vector DB: {e}")
                results = []

            if not results:
                st.info("No results found.")
            for r in results:
                similarity = 1 - r["distance"]  # cosine distance -> similarity
                with st.container(border=True):
                    st.markdown(f"**Resolution #{r['resolution_id']}** (ticket #{r['ticket_id']})")
                    st.write(r["chunk_text"])
                    st.caption(
                        f"Agent: {r['agent_name']} · Resolved: {r['resolved_at']} · "
                        f"Similarity: {similarity:.3f}"
                    )