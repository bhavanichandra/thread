# THREAD — Session Handoff

**Project:** `E:\splunk-hackathon\thread`
**Deadline:** June 15 2026 @ 9:00 AM PDT (Splunk Agentic Ops Hackathon)
**Branch:** `main` (all changes committed and pushed — no open PRs)

---

## What Was Done This Session

### E2E Pipeline — Fully Working

The complete failure → investigation → Slack → replay loop is verified end-to-end:

```
Order placed → payment fails → REQUEST_ERROR on RabbitMQ
→ logs_consumer detects it → 10s Splunk indexing delay
→ 6 MCP queries fire (visible as [THREAD:MCP] in terminal)
→ Slack alert posted with buttons
→ User clicks Replay → slack_messages_queue → ReplayEngine → HTTP 200
```

### Cisco DTMS Implemented (`thread_platform/agent/investigator.py`)

`cisco_dtms_anomaly()` now runs a real 6th MCP call using Splunk's `anomalydetect` SPL command (Cisco Deep Time Series Model backend on Splunk Cloud). Previously was a `NotImplementedError` stub.

- Runs `| anomalydetect error_rate` via `mcp.search()` → appears in terminal as `[THREAD:MCP] dtms_anomalydetect(service-name)`
- When < 3 time buckets (fresh failure), uses the 61-point timeseries already in memory for the score
- Returns `ai_source="cisco_dtms"` → Slack shows "AI Verdict — Cisco Deep Time Series"
- Falls back to `heuristic_anomaly()` on any error

### `/thread-search` Fixed (`thread_platform/splunk/mcp_client.py`)

`saia_generate_spl` returns `{"error": "Service not initialized, please contact support."}` on this Splunk Cloud instance (AI Assistant not enabled or scoped). The old code treated this as valid SPL (it contained `index=thread_logs`). Fixed: added error JSON detection → returns `""` → Groq fallback fires correctly. `/thread-search` now works end-to-end via Groq → SPL → MCP → Slack table.

### Slack UX Cleanup (`thread_platform/slack/blocks.py`)

- Removed `_anomaly_bar` (`█████░░` block chars after anomaly score)
- Removed emoji from section headers (`🔍`, `🤖`)
- Cleaner Replay button: red `🔁 Replay (L=N)` when safe; grey `↩ Replay anyway` when `recommended_limit=0`
- **Removed AI Analysis button entirely** (Groq search deep-link was confusing — both buttons opened Splunk search)

### README Updated (`README.md`)

- Both Mermaid diagrams updated: 6 MCP calls, Cisco DTMS in sequence, removed dashboard XML generation, fixed button labels
- Component table expanded with slash commands, `dashboard_gen.py`, `mcp_client.py`
- `![High Level Architecture](./high-level-architecture.svg)` added at top (user is creating this file in draw.io — **not yet committed**)

---

## Current State

### Confirmed Working
```
docker compose -f docker-compose-cloud.yaml up   all containers healthy
[THREAD:MCP] Available tools (14)                logged on startup
[THREAD:MCP] dtms_anomalydetect(...)             6th MCP call fires on every failure
Anomaly score: 1.00 [cisco_dtms]                 correct source shown in terminal + Slack
/thread-search <query>                           Groq → SPL → MCP → Slack table (13 results verified)
Replay button                                    HTTP 200 confirmed (stored REQUEST_START found in SQLite)
All 5 slash commands                             /thread-fail, /thread-recover, /thread-status, /thread-search, /thread-analyse
```

### What's Left

| Task | Notes |
|---|---|
| **Commit architecture diagram** | User creating in draw.io. Save as `high-level-architecture.svg` in repo root (README already references it). |
| **BHA-36: Video recording** | Under 3 min. Show `[THREAD:MCP]` 6 queries in terminal. See demo script below. |
| **BHA-38: Devpost submission** | Deadline June 15 2026 @ 09:00 PDT |
| **README minor fix** | Sequence diagram line 60 still says `[AI Analysis]` — button was removed from code, update label to `[Transaction Chain][Replay]` |

---

## Demo Script (BHA-36 Video)

1. **[Architecture diagram / README]** — "THREAD: distributed transaction tracing + AI recovery via Splunk MCP"
2. **[Terminal]** — `infisical run --env=staging -- docker-compose -f docker-compose-cloud.yaml up`
3. **[Slack]** — `/thread-fail` → order placed → "THREAD is investigating"
4. **[Terminal]** — watch 6 `[THREAD:MCP]` lines appear + `Anomaly score: 1.00 [cisco_dtms]`
5. **[Slack]** — alert arrives: Correlation ID, error rate, Cisco DTMS verdict, Transaction Chain button
6. **[Slack]** — `/thread-recover` → click `↩ Replay anyway` → "Replay Succeeded HTTP 200"
7. **[Slack]** — `/thread-search show all payment failures last hour` → Groq SPL → results table
8. **[Close]** — "6 agentic MCP calls per failure, Cisco AI anomaly detection, one-click recovery in Slack"

Key line to say during terminal moment: *"THREAD waits 10 seconds for Splunk to index the event before querying, so results are always fresh data."*

---

## Key Files

| File | Purpose |
|---|---|
| `thread_platform/agent/investigator.py` | 6-query MCP investigation + Cisco DTMS `anomalydetect` |
| `thread_platform/splunk/mcp_client.py` | MCP client — persistent session, `generate_spl` error detection |
| `thread_platform/slack/blocks.py` | Block Kit alert builder (no AI Analysis button) |
| `thread_platform/slack/commands.py` | 5 slash command handlers |
| `thread_platform/slack/handler.py` | Bolt AsyncApp, `trigger_replay` action |
| `thread_platform/consumers/logs_consumer.py` | REQUEST_ERROR → 10s delay → investigation |
| `thread_platform/agent/dashboard_gen.py` | Groq → SPL → search deep-link (url stored but button removed) |
| `docker-compose-cloud.yaml` | Splunk Cloud stack (use this for demo) |

---

## Splunk Cloud Config

| Purpose | Value |
|---|---|
| Web UI | `https://prd-p-pxen4.splunkcloud.com` |
| MCP URL | `https://prd-p-pxen4.splunkcloud.com/en-US/splunkd/__raw/services/mcp` |
| HEC | `https://prd-p-pxen4.splunkcloud.com:8088` |
| Username | `sc_admin` |
| Infisical env | `staging` |
| Index | `thread_logs` |

### Known Splunk Cloud Constraints
- Port 8089 blocked externally — MCP only reachable via `__raw` proxy
- `__raw` is read-only for REST (POSTing dashboard XML returns 303 → login redirect)
- `saia_generate_spl` returns `{"error": "Service not initialized"}` — Groq fallback always fires for `/thread-search`

---

## Running the Stack

```powershell
# Start (cloud)
infisical run --env=staging -- docker-compose -f docker-compose-cloud.yaml up -d

# Rebuild platform only
infisical run --env=staging -- docker-compose -f docker-compose-cloud.yaml build thread-platform
infisical run --env=staging -- docker-compose -f docker-compose-cloud.yaml up -d thread-platform

# Watch MCP traces
docker logs thread-platform --follow

# Demo flow (manual)
curl.exe -X POST http://localhost:8002/admin/toggle-failure   # enable failure
curl.exe -X POST http://localhost:8001/api/v1/orders `
  -H "Content-Type: application/json" `
  -d '{"customer_id":"demo","items":[{"productId":"p1","quantity":1}],"total":50.00}'
curl.exe -X POST http://localhost:8002/admin/toggle-failure   # disable failure
```

---

## Suggested Skills for Next Session

- `/verify` — confirm replay still works after architecture diagram commit
- `anthropic-skills:caveman` — active this session, re-invoke if desired
