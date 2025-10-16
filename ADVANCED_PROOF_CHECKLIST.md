# Advanced Proof Checklist

This checklist provides commands to prove the system's resilience and observability features.

## 1) Adaptive Rate Limiting

Goal: Verify per-source quotas and throttling metrics.

Trigger Run & Observe Logs (PowerShell / Bash):

```bash
# Trigger a run
curl -X POST -H "Authorization: Bearer <your_token>" http://localhost:3000/api/refresh

# Tail logs for throttle/retry notes (Docker)
docker logs -f kasparro_api
```

Verify Metrics (PowerShell):

```powershell
curl -s http://localhost:3000/metrics | findstr "throttle_events_total"
```

Acceptance: Logs show `[RATE LIMIT]` warnings, and the `/metrics` endpoint exposes a `throttle_events_total` counter.

---

## 2) Transactional Resume

Goal: Prove that the system resumes from the last checkpoint after a crash.

Induce Failure:

```bash
# Trigger a run
curl -X POST -H "Authorization: Bearer <your_token>" http://localhost:3000/api/refresh
# Immediately kill the process to simulate a crash. Example (Linux/Mac):
# pkill -f node
# Or run the included helper that triggers a failing run (if present):
node fail-run.js
```

Restart and Resume:

```bash
docker compose up -d
curl -X POST -H "Authorization: Bearer <your_token>" http://localhost:3000/api/refresh
```

Verify in Logs (PowerShell):

```powershell
docker logs kasparro_api | findstr "Resuming from"
```

Acceptance: Logs show `Resuming from last successful checkpoint...` or similar resume message and no duplicate data is created.

---

## 3) Automated Schema Drift Mapping

Goal: Prove the system auto-maps column name changes.

Seed Drift:

```bash
node seed-drift.js
```

Trigger Run:

```bash
curl -X POST -H "Authorization: Bearer <your_token>" http://localhost:3000/api/refresh
```

Verify in Logs (PowerShell):

```powershell
docker logs kasparro_api | findstr "[SCHEMA DRIFT]"
```

Acceptance: Logs show `[SCHEMA DRIFT] Detected schema drift... Applying mappings` and a new document is created in the `schemaversions` collection (or the equivalent collection used by your app).

---

Notes and Troubleshooting

- If `docker logs -f kasparro_api` returns `No such container`, check the container name with `docker ps` and substitute the correct container name.
- If metrics are not exposed, ensure the API is running and `prom-client` metrics are enabled in the app.
- When running locally (outside Docker), some hostnames like `mongo` are only reachable from inside Docker Compose; see `README.md` for local fallback instructions.

If you'd like, I can also insert a short pointer to this file into `README.md` linking it, or attempt to update the README directly if you prefer an inline checklist there.
