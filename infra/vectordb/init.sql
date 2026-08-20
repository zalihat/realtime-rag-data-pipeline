-- ---------------------------------------------------------------------------
-- RAG vector store schema
-- Runs on first boot of the `vectordb` container (docker-entrypoint-initdb.d)
-- ---------------------------------------------------------------------------

CREATE EXTENSION IF NOT EXISTS vector;

-- One row per embedded chunk of a ticket resolution note.
-- Kept separate from the source `ticket_resolutions` table (which lives in
-- the other Postgres instance) -- this table is populated by a downstream
-- consumer reading the `support.public.ticket_resolutions` Kafka topic,
-- embedding the note text, and upserting here.
CREATE TABLE resolution_embeddings (
    id              BIGSERIAL PRIMARY KEY,

    -- Foreign reference back to the source system. Not a real FK since
    -- it points at a different database -- enforced at the application
    -- level by the consumer.
    ticket_id       INTEGER NOT NULL,
    resolution_id   INTEGER NOT NULL,

    -- Denormalized fields useful for filtering/display without a join
    -- back to Postgres at query time.
    agent_name      TEXT,
    resolved_at     TIMESTAMPTZ,

    -- The actual text that was embedded. Kept alongside the vector so
    -- retrieval results are self-contained (no round-trip needed to
    -- fetch the source text for display / LLM context).
    chunk_text      TEXT NOT NULL,
    chunk_index     INTEGER NOT NULL DEFAULT 0,  -- for future chunking of long notes

    -- all-MiniLM-L6-v2 produces 384-dimensional embeddings.
    embedding       VECTOR(384) NOT NULL,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- One embedding per (resolution, chunk) -- lets the consumer safely
    -- re-run/upsert without creating duplicates.
    UNIQUE (resolution_id, chunk_index)
);

CREATE INDEX idx_resolution_embeddings_ticket_id
    ON resolution_embeddings (ticket_id);

-- HNSW index for approximate nearest-neighbor search.
-- vector_cosine_ops -- cosine distance, the standard choice for
-- sentence-transformer embeddings (they're typically compared by
-- cosine similarity, not raw L2 distance).
CREATE INDEX idx_resolution_embeddings_hnsw
    ON resolution_embeddings
    USING hnsw (embedding vector_cosine_ops);