# THREAD — Claude Code Context

> Read this before touching any file. It contains every architectural decision
> made during design, the full tech stack, conventions, and current build state.

---

## What THREAD Is

THREAD (Traceable Header Relay, Execution Audit & Distributed-recovery) is a
distributed transaction tracing framework and AI-powered recovery platform built
for the Splunk Agentic Ops Hackathon 2026 (Observability track, deadline June 15 2026).

Services publish a 5-field contract to RabbitMQ and log to Splunk HEC. When a
transaction fails, an AI agent investigates via Splunk MCP Server, calculates a
dynamic replay limit using heuristic analysis (Cisco DTMS when Splunk Cloud is
active), and delivers one-click recovery to the ops team in Slack.

---

## Architecture (Final — do not change without updating this file)

```
Services (any language)
  ├── log 5-field contract ──→ Splunk HEC     [investigation]
  └── publish to RabbitMQ ──→ Thread Service → SQLite  [replay]

Splunk detects REQUEST_ERROR ──→ webhook ──→ Thread Service
Thread Service ──→ Splunk MCP (5 queries) + heuristic anomaly ──→ InvestigationResult
Thread Service ──→ Slack #thread-alerts (Block Kit + buttons)

User clicks Replay ──→ Slack handler ack() ──→ slack_messages_queue
Thread Service consumer ──→ SQLite lookup ──→ re-execute original request
```

### Key Architectural Decisions (from design sessions — do NOT revisit)

1. **No SDK package** — `ThreadMessage` model lives inline in
   `demo_services/thread_publisher.py` AND `thread_platform/models/contract.py`.
   Two copies, 15 lines each. Intentional — separate processes, no packaging needed.

2. **No middleware** — services explicitly call `publish_thread_event()` and
   `log_to_splunk()`. This makes the contract visible and language-portable.

3. **RabbitMQ not Kafka** — task queue pattern, not log streaming. Kafka is
   documented in README as the production scale-up path.

4. **SQLite not Redis** — replay request bodies stored in SQLite with 24h TTL.
   Survives restarts. Simple. Right for this scale.

5. **Slack as UI** — no custom dashboard. Slack is where ops teams live.
   Block Kit messages + buttons = the entire ops interface.

6. **MCP is the star** — Splunk MCP Server runs 5 SPL queries per investigation.
   Every query is logged with timing (`[THREAD:MCP]` prefix) so judges can see
   it working in the terminal during the demo.

7. **Cisco DTMS is cloud-only** — local Docker Splunk doesn't have hosted models.
   Use `SPLUNK_AI_ENABLED=false` for local dev (heuristic fallback).
   Set `SPLUNK_AI_ENABLED=true` when Splunk Cloud trial is active.

8. **AI-generated Splunk dashboards** — THREAD auto-creates a Splunk dashboard
   per failure investigation using AI-generated SPL (Groq/Claude). No manually
   built dashboard XML anywhere.

---

## The THREAD Contract (5 mandatory fields)

Every service in every language must include these in every log line AND
every RabbitMQ message:

```json
{
  "correlationId":  "abc-123",
  "transactionId":  "def-456",
  "sourceService":  "order-service",
  "targetService":  "payment-service",
  "traceEvent":     "REQUEST_START | REQUEST_END | REQUEST_ERROR",
  "timestamp":      "2026-06-10T10:00:00Z",

  // Optional — for replay capture (REQUEST_START only needs these):
  "method":         "POST",
  "url":            "http://payment-service:8002/api/v1/payments",
  "body":           { ... },
  "statusCode":     null,
  "durationMs":     null,
  "errorMessage":   null,
  "replayAttempt":  0
}
```

**SPL field names are camelCase** — `correlationId`, `traceEvent`, `sourceService`,
`durationMs` etc. NOT snake_case. All Splunk queries must use these names.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Demo services | Python 3.11+, FastAPI, uv |
| Message bus | RabbitMQ 3.13 (`aio_pika`) |
| Request storage | SQLite (via `sqlite3` stdlib) |
| Log ingestion | Splunk HEC port 8088 |
| Data store | Splunk Enterprise (Docker: `splunk/splunk:latest`) |
| AI queries | Splunk MCP Server port 7700 |
| AI analytics | Heuristic (local) / Cisco DTMS (Splunk Cloud) |
| Ops interface | Slack (Socket Mode, Block Kit, `slack-bolt`) |
| Secret management | Infisical CLI (`infisical run -- <command>`) |
| Containers | Docker Compose |
| Package manager | uv + pyproject.toml (NO requirements.txt anywhere) |

---

## Repository Structure

```
thread/
├── CLAUDE.md                        ← you are here
├── CONTEXT.md                       ← architecture decisions (detailed)
├── architecture_diagram.png         ← required by hackathon rules
├── LICENSE                          ← MIT
├── docker-compose.yml
├── infisical.json                   ← committed, no secrets
├── .env                             ← gitignored, non-secret config
├── .env.example                     ← committed reference
│
├── demo_services/
│   ├── thread_publisher.py          ← THREAD contract + RabbitMQ publisher + HEC logger
│   ├── order_service/
│   │   ├── main.py
│   │   ├── pyproject.toml
│   │   └── Dockerfile
│   ├── payment_service/
│   │   ├── main.py                  ← has SIMULATE_FAILURE env var
│   │   ├── pyproject.toml
│   │   └── Dockerfile
│   └── inventory_service/
│       ├── main.py
│       ├── pyproject.toml
│       └── Dockerfile
│
└── thread_platform/
    ├── main.py                      ← FastAPI app + lifespan startup
    ├── pyproject.toml
    ├── Dockerfile
    ├── setup_queues.py              ← declares RabbitMQ queues on startup
    ├── models/
    │   ├── __init__.py
    │   └── contract.py              ← ThreadMessage + TraceEvent (copy of demo_services version)
    ├── agent/
    │   ├── investigator.py          ← InvestigationAgent, heuristic_anomaly, cisco_dtms_anomaly
    │   └── models.py                ← InvestigationResult, FailureClass, ForecastTrend
    ├── consumers/
    │   ├── logs_consumer.py         ← consumes thread_logs_queue → SQLite
    │   └── slack_consumer.py        ← consumes slack_messages_queue → replay/skip/escalate
    ├── replay/
    │   └── engine.py                ← ReplayEngine reads from SQLite
    ├── slack/
    │   ├── handler.py               ← ack() → publish to slack_messages_queue
    │   └── blocks.py                ← Block Kit message builders
    ├── splunk/
    │   ├── mcp_client.py            ← SplunkMCPClient with timing logs
    │   └── queries.py               ← all SPL query templates
    └── store/
        └── database.py              ← SQLite schema, save/get/cleanup
```

---

## Environment Variables

### Infisical (6 secrets — never in .env or code)
```
SPLUNK_HEC_TOKEN
SPLUNK_MCP_TOKEN
SPLUNK_PASSWORD
SLACK_BOT_TOKEN
SLACK_SIGNING_SECRET
SLACK_APP_TOKEN
RABBITMQ_PASSWORD
```

### .env (non-secret config)
```bash
SPLUNK_HOST=localhost
SPLUNK_PORT=8089
SPLUNK_HEC_PORT=8088
SPLUNK_INDEX=thread_logs
SPLUNK_BASE_URL=http://localhost:8000
SPLUNK_MCP_URL=http://localhost:7700
SPLUNK_USERNAME=admin
SLACK_SOCKET_MODE=true
SLACK_ALERT_CHANNEL=#thread-alerts
ORDER_SERVICE_URL=http://localhost:8001
PAYMENT_SERVICE_URL=http://localhost:8002
INVENTORY_SERVICE_URL=http://localhost:8003
PLATFORM_PORT=9000
PLATFORM_ENV=development
SIMULATE_FAILURE=false
FAILURE_RATE=0.0
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_USER=thread
RABBITMQ_VHOST=/
THREAD_LOGS_QUEUE=thread_logs_queue
SLACK_MESSAGES_QUEUE=slack_messages_queue
SQLITE_PATH=thread_store.db
SPLUNK_AI_ENABLED=false        # true when Splunk Cloud trial active
```

### Running Commands
```powershell
# Start everything
infisical run -- docker-compose up --build

# Local dev — single service
infisical run -- uv run uvicorn main:app --port 8001 --reload

# Thread Platform
infisical run -- uv run uvicorn thread_platform.main:app --port 9000 --reload

# Watch Thread Service logs (shows [THREAD:MCP] traces)
docker logs thread-platform --follow
```

---

## Key Patterns — Follow These Exactly

### 1. Fire-and-forget async tasks
```python
# Correct — never await these directly, never block request processing
asyncio.create_task(publish_thread_event(...))
asyncio.create_task(log_to_splunk(...))
```

### 2. Slack ack() — must complete in < 3 seconds
```python
@app.action("replay_transaction")
async def handle_replay(ack, body, respond):
    await ack()                    # FIRST — always
    await respond({...})           # update message immediately
    await _publish_action(...)     # then publish to queue
```

### 3. MCP query logging — always include timing
```python
# Every MCP query must print [THREAD:MCP] with timing
print(f"[THREAD:MCP] {label} → {len(results)} results ({elapsed:.0f}ms)")
```

### 4. RabbitMQ messages — always PERSISTENT
```python
aio_pika.Message(
    body=json.dumps(payload).encode(),
    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,  # always
)
```

### 5. Never break the service on THREAD errors
```python
try:
    await publish_thread_event(...)
except Exception as e:
    print(f"[THREAD] Publish failed (non-fatal): {e}")
    # never raise — THREAD must never impact service behaviour
```

### 6. Pydantic version
```toml
# Always use >=2.9.0 — earlier versions fail on Python 3.13
"pydantic>=2.9.0"
```

### 7. Dockerfiles — always use uv
```dockerfile
FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml ./
RUN uv sync --no-dev
COPY . .
CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]
```

---

## Current Build State

### Done ✅
- BHA-21: Splunk Enterprise running in Docker with HEC configured
- BHA-22: GitHub repo initialised with MIT licence, Infisical, docker-compose
- BHA-24: 3 demo microservices (order/payment/inventory) scaffolded and running
- BHA-26: SDK cancelled — ThreadMessage lives inline in thread_publisher.py
- BHA-41: RabbitMQ in docker-compose, both queues created

### In Progress 🔄
- BHA-28: Splunk HEC logging, MCP trace visibility, failure alert, AI-generated dashboards

### Up Next (in order)
- BHA-30: SplunkMCPClient with timing logs on all 5 queries
- BHA-31: InvestigationAgent with SPLUNK_AI_ENABLED flag + mcp_trace field
- BHA-42: thread_logs_queue consumer + SQLite store
- BHA-32: ReplayEngine reading from SQLite
- BHA-33: Slack bot — Block Kit messages + ack() → queue
- BHA-43: slack_messages_queue consumer
- BHA-35: E2E testing
- BHA-36: Video recording
- BHA-37: README + architecture diagram
- BHA-38: Devpost submission

### Cancelled ❌
- BHA-34: Superseded by BHA-43

### Optional (do only if time after BHA-38)
- BHA-40: LLM summary in Slack message (Groq or Claude)

---

## BHA-28 — Current Task

The active issue. Three things left to implement:

### 1. `log_to_splunk()` in `demo_services/thread_publisher.py`
Add alongside `publish_thread_event()`. Both are called in every route.
Uses `SPLUNK_HEC_URL` and `SPLUNK_HEC_TOKEN` from env.
Sends structured JSON to `http://splunk:8088/services/collector/event`.
Must include `replayAttempt` field.

### 2. MCP timing logs in `thread_platform/splunk/mcp_client.py`
Every `search()` call prints:
```
[THREAD:MCP] {label} → {len(results)} results ({elapsed:.0f}ms)
```

### 3. AI-generated Splunk dashboard (replaces hardcoded panels)
After investigation completes, call Groq (free) or Claude to generate
3 SPL queries as JSON based on failure context. Then create the dashboard
in Splunk via REST API. Add dashboard URL to InvestigationResult.
Show as "📊 View Generated Dashboard" button in Slack message.

**AI dashboard generation flow:**
```python
# 1. Call LLM with failure context
prompt = f"""
Generate 3 Splunk SPL dashboard panels for this failure.
Respond ONLY with a JSON array, no markdown.

Context:
- Failed service: {failed_service}
- Error type: {error_type}
- Correlation ID: {correlation_id}
- Time range: last 1 hour

Format: [{{"title": "...", "spl": "...", "viz": "table|timechart|single_value"}}]

Use these field names: correlationId, sourceService, targetService,
traceEvent, statusCode, durationMs, errorMessage, replayAttempt.
Index is: thread_logs
"""

# 2. Create dashboard via Splunk REST API
# POST https://splunk:8089/servicesNS/admin/search/data/ui/views
# Content-Type: application/x-www-form-urlencoded
# Body: name=thread-<correlationId>&eai:data=<dashboard-xml>

# 3. Return dashboard URL
dashboard_url = f"{SPLUNK_BASE_URL}/en-US/app/search/thread-{correlation_id[:8]}"
```

Use Groq (free, fast) as the LLM:
```python
from groq import Groq
client = Groq(api_key=os.getenv("GROQ_API_KEY"))
# Add GROQ_API_KEY to Infisical
```

---

## Splunk Dashboard XML Template

When creating the dashboard via REST API, wrap the generated SPL in this XML:

```xml
<dashboard version="1.1">
  <label>THREAD - {correlation_id}</label>
  <description>Auto-generated by THREAD AI Agent</description>
  {panels}
</dashboard>
```

Each panel (table):
```xml
<row>
  <panel>
    <title>{title}</title>
    <table>
      <search><query>{spl}</query><earliest>-1h</earliest><latest>now</latest></search>
    </table>
  </panel>
</row>
```

Each panel (timechart):
```xml
<row>
  <panel>
    <title>{title}</title>
    <chart>
      <search><query>{spl}</query><earliest>-1h</earliest><latest>now</latest></search>
      <option name="charting.chart">line</option>
    </chart>
  </panel>
</row>
```

---

## Hackathon Context

- **Event:** Splunk Agentic Ops Hackathon 2026
- **Track:** Observability
- **Deadline:** June 15, 2026 @ 9:00 AM PDT
- **Target prizes:** Observability ($3k) + Best MCP Server ($1k) + Grand Prize ($7k)
- **Max one bonus prize** — targeting Best MCP Server
- **Judges:** Splunk practitioners — they know Splunk, they'll check MCP usage
- **Demo video:** Under 3 minutes, must show MCP queries in terminal

### Prize-winning angle
THREAD is not just a chatbot on top of Splunk. The MCP Server is the core
AI integration — 5 queries per investigation, timing logged, results visible
in Slack. The AI-generated dashboards add a second layer of agentic behaviour:
THREAD doesn't just read Splunk, it writes back to it.

---

## Windows Notes (dev machine is Windows)

- Use `curl.exe` not `curl` in PowerShell (curl is an alias for Invoke-WebRequest)
- Line continuation in PowerShell: backtick `` ` `` not `\`
- Env vars in PowerShell: `$env:KEY="value"` not `export KEY=value`
- For loops in PowerShell: `1..12 | ForEach-Object { ... }`
- Docker runs Linux containers via Docker Desktop — this is fine

---

## Do Not

- Do not create `requirements.txt` files — use `pyproject.toml` + uv only
- Do not hardcode secrets — use `os.getenv()` and Infisical
- Do not use `BackgroundTasks` in Slack handler — use `slack_messages_queue`
- Do not use `asyncio.sleep()` to poll — use RabbitMQ consumers
- Do not add middleware to services — explicit `publish_thread_event()` calls only
- Do not create a separate `thread-sdk` package — ThreadMessage lives inline
- Do not build Splunk dashboard XML manually — AI generates it
- Do not use old SPL field names: `event_type`, `correlation_id`, `service_name`
