# Realtime RAG Data Pipeline — CDC Setup Notes

Postgres → Debezium → Kafka → (future: RAG indexing) pipeline for support
tickets and resolution notes. This doc captures what's running, why, and
every gotcha hit while standing it up — for future-me.

## Stack

| Service     | Image                              | Port | Purpose                                  |
|-------------|-------------------------------------|------|-------------------------------------------|
| `postgres`  | `debezium/postgres:16`             | 5432 | Source DB, logical replication enabled    |
| `zookeeper` | `quay.io/debezium/zookeeper:2.7`   | 2181 | Kafka coordination                        |
| `kafka`     | `quay.io/debezium/kafka:2.7`       | 9092 | Broker, CDC event stream                  |
| `connect`   | `quay.io/debezium/connect:2.7`     | 8083 | Kafka Connect worker running Debezium     |
| `console`   | `redpandadata/console:v2.7.2`      | 8080 | UI for topics + connector status          |

Bring everything up:
```powershell
docker compose up -d
docker compose ps
```

## Schema / CDC prerequisites (`infra/postgres/init.sql`)

- `tickets` and `ticket_resolutions` both have `REPLICA IDENTITY FULL` —
  without this, Debezium's "before" image on UPDATE/DELETE only contains
  the PK, so resolution note text would be missing from delete events and
  unreliable on updates.
- `CREATE PUBLICATION dbz_publication FOR TABLE tickets, ticket_resolutions;`
  — Debezium's `pgoutput` plugin reads from a named publication, not the
  whole DB by default. Only these two tables are ever CDC'd.
- This only runs on a **fresh** `pgdata` volume. If you change `init.sql`
  after the volume already exists, it won't re-run — `docker compose down -v`
  to wipe and re-init if needed.

## Registering the Debezium connector

Nothing flows until this is POSTed to Connect (`infra/debezium/register-connector.json`):

```powershell
curl.exe -X POST -H "Content-Type: application/json" `
  --data "@infra\debezium\register-connector.json" `
  http://localhost:8083/connectors
```

Check status:
```powershell
curl.exe http://localhost:8083/connectors/support-pg-connector/status
```
Both `connector.state` and `tasks[0].state` should read `RUNNING`.

Key config choices:
- `database.hostname: "postgres"` — must match the **Compose service name**,
  not `container_name` (`pg`). Compose's internal DNS resolves service names.
- `plugin.name: "pgoutput"` — Postgres's built-in logical decoding plugin,
  no extra install needed on the `debezium/postgres` image.
- `publication.autocreate.mode: "disabled"` — publication is created
  manually in `init.sql`, don't want Debezium creating its own.

## Data generation

`generate_data.py` — synthetic customers/tickets/resolutions across 5
categories (billing, login, bug, feature_request, shipping) with matched
subject/body/resolution templates so ticket ↔ resolution pairs are
topically consistent. Configurable `--resolve-pct` controls what fraction
of tickets get a resolution note (rest stay `open`).

```powershell
pip install -r requirements.txt
python generate_data.py --customers 50 --tickets 200 --resolve-pct 0.7
```

## Topics produced

- `support.public.tickets` — **all** tickets, open and resolved (inserts +
  updates, e.g. status changes). Not filtered.
- `support.public.ticket_resolutions` — only rows for tickets that got
  resolved; structurally this **is** the "resolved" stream already.
- Debezium internal topics: `connect_configs`, `connect_offsets`,
  `connect_statuses`, plus a schema history topic.

**Decision: no CDC-level filtering.** Keep both topics unfiltered/raw and
join by `ticket_id` downstream — `tickets` gives context (subject, body,
customer), `ticket_resolutions` gives the actual content to index. Filtering
at the connector level would lose flexibility later (e.g. open-ticket
dashboards, resolution-time tracking).

## Gotchas hit while setting this up (all fixed, kept for reference)

1. **Compose file is `docker-compose.yml`, not a Dockerfile.** `version:
   "3.8"` key is deprecated in current Compose — dropped it.

2. **Docker Hub pull TLS timeout** (`failed to do request: ... TLS handshake
   timeout`) — transient network issue, not a config problem. Retry, check
   Docker Desktop networking/DNS settings, or try a different network if
   persistent.

3. **Zookeeper/Kafka healthchecks failing despite the service being fine.**
   `nc` and `kafka-broker-api-versions.sh` weren't reliably available/on
   PATH in these images. Fixed by using bash's built-in TCP check instead:
   ```yaml
   test: ["CMD-SHELL", "bash -c 'echo > /dev/tcp/localhost/<port>' || exit 1"]
   ```

4. **Kafka healthcheck: "Connection refused" even with the `/dev/tcp`
   check.** Real bug, not tooling — Kafka's `KAFKA_LISTENERS` defaulted to
   binding only the container's specific IP (e.g. `172.20.0.4:9092`), not
   `localhost`. Fixed by explicitly setting:
   ```yaml
   KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092
   KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
   ```
   (listeners = what Kafka binds to inside the container; advertised
   listeners = what other containers use to reach it.)

5. **`curl` in PowerShell isn't real curl** — it's aliased to
   `Invoke-WebRequest`, which chokes on `-H`/`--data` flag syntax. Use
   `curl.exe` explicitly to get the real binary, or switch to
   `Invoke-RestMethod`.

6. **Redpanda Console crash-looping**: `"a cluster name must be set to
   identify the connect cluster"` even though `CONNECT_CLUSTERS_0_NAME` was
   set via environment vars. Console v2.7's env-var parsing for nested
   Connect cluster config wasn't picking up the flat env var names reliably.
   Fixed by mounting a proper YAML config file instead:
   ```yaml
   # infra/console/config.yml
   kafka:
     brokers:
       - kafka:9092
   connect:
     enabled: true
     clusters:
       - name: local-connect
         url: http://connect:8083
   ```
   ```yaml
   # docker-compose.yml, console service
   volumes:
     - ./infra/console/config.yml:/tmp/config.yml
   command: ["--config.filepath=/tmp/config.yml"]
   ```
   Note: needs **double-dash** `--config.filepath`, not single-dash — the
   binary silently ignored the single-dash form with no error, which is
   why it looked like the volume mount wasn't working at first.

## Quick verification checklist

```powershell
docker compose ps                                          # all healthy?
curl.exe http://localhost:8083/connectors/support-pg-connector/status
curl.exe http://localhost:8080                              # Console up?
docker exec -it pg psql -U appuser -d supportdb -c "\dRp"    # publication exists?
```

Tail a topic directly without the UI:
```powershell
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic support.public.tickets --from-beginning
```

## Credentials (local dev only — not for anything shared)

- Host/port: `localhost:5432`
- DB: `supportdb`
- User/pass: `appuser` / `apppass`
- JDBC: `jdbc:postgresql://localhost:5432/supportdb`

## Not done yet

- RAG indexing/embedding pipeline consuming from `support.public.ticket_resolutions`
- A real Kafka sink connector (currently only the Postgres source connector is registered)
- Auth/secrets hardening (currently hardcoded creds, fine for local only)