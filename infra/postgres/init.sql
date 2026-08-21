-- Schema
CREATE TABLE customers (
    customer_id     SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    email           TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE tickets (
    ticket_id       SERIAL PRIMARY KEY,
    customer_id     INTEGER REFERENCES customers(customer_id),
    subject         TEXT NOT NULL,
    body            TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'open',   -- open | resolved
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ticket_resolutions (
    resolution_id   SERIAL PRIMARY KEY,
    ticket_id       INTEGER REFERENCES tickets(ticket_id),
    agent_name      TEXT NOT NULL,
    note            TEXT NOT NULL,        -- the resolution text you want in the RAG index
    resolved_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- CDC essentials
-- ---------------------------------------------------------------------------
-- REPLICA IDENTITY FULL: Postgres logs the ENTIRE row (all columns, old + new)
-- to the WAL on UPDATE/DELETE, not just the primary key. Without this, Debezium's
-- "before" image on an UPDATE/DELETE only contains the PK, so your note text
-- would be missing from delete events and unreliable on updates.
ALTER TABLE ticket_resolutions REPLICA IDENTITY FULL;
ALTER TABLE tickets REPLICA IDENTITY FULL;

-- Publication: Debezium's pgoutput plugin reads from a publication, not the
-- whole database by default. Only tables listed here are ever CDC'd.
CREATE PUBLICATION dbz_publication FOR TABLE tickets, ticket_resolutions;