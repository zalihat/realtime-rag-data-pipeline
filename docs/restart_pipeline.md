# Restart / Reset Procedure

Quick reference for wiping and restarting the pipeline from scratch —
useful when you want to see CDC flow from a clean state again, or
after changing `init.sql` (which only runs on a fresh `pgdata` volume).

## Full reset (Postgres data + Kafka topics wiped)

**1. Delete the connector first** (cleaner than letting it error against a wiped DB):
```powershell
curl.exe -X DELETE http://localhost:8083/connectors/support-pg-connector
```

**2. Tear everything down, including volumes:**
```powershell
docker compose down -v
```
The `-v` flag removes the `pgdata` named volume (wiping Postgres) along
with the containers, which also wipes Kafka's in-container topic data
since it isn't on a named volume.

**3. Bring the stack back up:**
```powershell
docker compose up -d
docker compose ps
```
This re-runs `init.sql` on the fresh `pgdata` volume — recreating the
schema, `REPLICA IDENTITY FULL`, and `dbz_publication` from scratch.
Wait for all services to show `Healthy` before continuing.

**4. Re-register the connector** (needs a fresh replication slot against the new DB):
```powershell
curl.exe -X POST -H "Content-Type: application/json" `
  --data "@infra\debezium\register-connector.json" `
  http://localhost:8083/connectors

curl.exe http://localhost:8083/connectors/support-pg-connector/status
```
Confirm both `connector.state` and `tasks[0].state` show `RUNNING`.

**5. Regenerate data:**
```powershell
python generate_data.py --customers 50 --tickets 200 --resolve-pct 0.7
```

**6. Verify:** check Console at `http://localhost:8080` — topics should
reappear and populate with the new snapshot + live inserts.

## Note

`docker compose down -v` removes **all** named volumes in the compose
file — currently just `pgdata`, so safe. Revisit this if more persistent
volumes get added later that shouldn't be wiped alongside it.