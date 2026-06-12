# THREAD
### Traceable Header Relay, Execution Audit & Distributed-recovery

> Built for the Splunk Agentic Ops Hackathon 2026 — Observability Track

THREAD is a distributed transaction tracing framework and AI-powered recovery platform.
Services publish a 5-field contract to RabbitMQ and log to Splunk. When something fails,
an AI agent investigates via **Splunk MCP Server**, the **Cisco Deep Time Series Model**
calculates a dynamic replay limit, and one-click recovery lands in Slack — in under 30 seconds.

![Architecture](architecture_diagram.png)

## Demo Video

[▶ Watch the 3-minute demo on YouTube](YOUR_YOUTUBE_URL_HERE)

---

## The Problem

In distributed systems, a failed request requires engineers to manually trace logs across
multiple services (30–90 min), decide whether to retry by intuition, and execute the replay
manually. THREAD automates all three steps.

---

## Architecture

```
Services (any language)
  ├── log 5-field contract ──→ Splunk HEC      [investigation data]
  └── publish to RabbitMQ ──→ Thread Service → SQLite  [replay capture]

Splunk detects REQUEST_ERROR ──→ webhook ──→ Thread Service
Thread Service ──→ Splunk MCP Server (5 queries) + Cisco DTMS ──→ InvestigationResult
Thread Service ──→ Slack #thread-alerts (Block Kit message + buttons)

User clicks Replay ──→ Slack handler ack() ──→ slack_messages_queue
Thread Service consumer ──→ SQLite lookup ──→ re-execute original request
```

### Key Components

| Component | Purpose |
|-----------|---------|
| `demo_services/` | 3 FastAPI microservices (order/payment/inventory) |
| `demo_services/thread_publisher.py` | THREAD contract: publish to RabbitMQ + Splunk HEC |
| `thread_platform/agent/` | AI Investigation Agent (Splunk MCP + Cisco DTMS) |
| `thread_platform/consumers/` | RabbitMQ consumers (logs + Slack messages) |
| `thread_platform/replay/` | Replay engine (reads from SQLite) |
| `thread_platform/slack/` | Slack bot (Socket Mode, Block Kit) |
| `thread_platform/splunk/` | Splunk MCP client + 5 SPL queries |
| `thread_platform/store/` | SQLite schema and CRUD |

---

## The THREAD Contract

Any service in any language participates by including these fields in every log line
and every RabbitMQ message:

```json
{
  "correlationId": "abc-123",
  "transactionId": "def-456",
  "sourceService": "order-service",
  "targetService": "payment-service",
  "traceEvent":    "REQUEST_START | REQUEST_END | REQUEST_ERROR",
  "timestamp":     "2026-06-10T10:00:00Z",
  "replayAttempt": 0,

  // REQUEST_START only — captured for one-click replay:
  "method":        "POST",
  "url":           "http://payment-service:8002/api/v1/payments",
  "body":          { },
  "statusCode":    null,
  "durationMs":    null,
  "errorMessage":  null
}
```

**All SPL field names are camelCase** — `correlationId`, `traceEvent`, `sourceService`, `durationMs`, etc.

---

## Prerequisites

- Python 3.11+
- Docker and Docker Compose
- Splunk Enterprise (free trial or developer licence from [dev.splunk.com](https://dev.splunk.com))
- Slack workspace with permission to add apps
- [Infisical](https://app.infisical.com) account (free tier)

---

## Quick Start

### 1. Clone the repo

```bash
git clone https://github.com/YOUR_USERNAME/thread.git
cd thread
```

### 2. Set up Infisical (7 secrets)

```bash
# macOS/Linux
brew install infisical/get-cli/infisical

# Windows (PowerShell)
winget install Infisical.Infisical

infisical login && infisical init
```

Add these secrets to your Infisical dev environment:

| Secret | Description |
|--------|-------------|
| `SPLUNK_HEC_TOKEN` | Splunk HTTP Event Collector token |
| `SPLUNK_MCP_TOKEN` | Splunk API token for MCP queries |
| `SPLUNK_PASSWORD` | Splunk admin password |
| `SLACK_BOT_TOKEN` | Slack bot OAuth token (`xoxb-...`) |
| `SLACK_SIGNING_SECRET` | Slack app signing secret |
| `SLACK_APP_TOKEN` | Slack app-level token for Socket Mode (`xapp-...`) |
| `RABBITMQ_PASSWORD` | RabbitMQ password for the `thread` user |

### 3. Configure environment

```bash
cp .env.example .env
# Edit .env — all values are non-sensitive config (ports, URLs, flags)
```

### 4. Run everything

```bash
infisical run -- docker-compose up --build
```

Services start on:
- Order service: `http://localhost:8001`
- Payment service: `http://localhost:8002`
- Inventory service: `http://localhost:8003`
- Thread Platform: `http://localhost:9000`
- Splunk Web: `http://localhost:8000`
- RabbitMQ Management: `http://localhost:15672`

### 5. Configure Splunk alert

In Splunk Web (`http://localhost:8000`), create a real-time alert:
- Search: `index=thread_logs traceEvent=REQUEST_ERROR | dedup correlationId`
- Action: Webhook → `http://thread-platform:9000/splunk/alert`

### 6. Test it

```powershell
# Happy path — order succeeds
curl.exe -X POST http://localhost:8001/api/v1/orders `
  -H "Content-Type: application/json" `
  -d '{"customer_id": "c1", "items": [{"sku": "A1", "quantity": 1}], "total": 29.99}'

# Inject failure — set payment service to fail, resend order, watch Slack
```

---

## Slack App Setup

1. Go to [api.slack.com/apps](https://api.slack.com/apps) → **Create New App → From Scratch**
2. App Name: `THREAD Bot`
3. Enable **Socket Mode** → generate App-Level Token → add to Infisical as `SLACK_APP_TOKEN`
4. **OAuth & Permissions → Bot Token Scopes:** `chat:write`, `chat:write.public`, `channels:read`
5. **Interactivity & Shortcuts → Enable**
6. Install to workspace → copy Bot Token → add to Infisical as `SLACK_BOT_TOKEN`
7. **Basic Information → Signing Secret** → add to Infisical as `SLACK_SIGNING_SECRET`
8. Create `#thread-alerts` channel, invite the bot: `/invite @THREAD Bot`

---

## AI Investigation Pipeline

When a `REQUEST_ERROR` hits Splunk and the webhook fires, THREAD runs 5 MCP queries:

```
[THREAD:MCP] get_transaction_chain(abc-123...)      →   6 results (234ms)
[THREAD:MCP] get_failure_details(abc-123...)        →   1 results (187ms)
[THREAD:MCP] get_service_health(payment-service)    →   1 results (203ms)
[THREAD:MCP] get_system_errors()                    →   2 results (198ms)
[THREAD:MCP] get_error_rate_timeseries(payment...)  →  12 results (241ms)
[THREAD:MCP] Investigation complete for abc-123...
[THREAD:MCP]   Failed service:  payment-service
[THREAD:MCP]   Error rate:      4.2%
[THREAD:MCP]   Anomaly score:   0.23
[THREAD:MCP]   Forecast trend:  RECOVERING
[THREAD:MCP]   Replay limit L:  2
```

Results are posted to Slack as a Block Kit message with Replay / Skip / Escalate buttons.

### AI Backends

| Flag | Backend |
|------|---------|
| `SPLUNK_AI_ENABLED=false` | Heuristic anomaly detection (local dev, default) |
| `SPLUNK_AI_ENABLED=true` | Cisco Deep Time Series Model (Splunk Cloud trial only) |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Demo services | Python 3.11, FastAPI |
| Message bus | RabbitMQ (task queue) |
| Request storage | SQLite (replay capture, 24h TTL) |
| Log ingestion | Splunk HEC (port 8088) |
| Data store | Splunk Enterprise |
| AI queries | Splunk MCP Server |
| AI analytics | Cisco Deep Time Series Model (Splunk Hosted) |
| Ops interface | Slack (Socket Mode, Block Kit) |
| Secret management | Infisical CLI |
| Containers | Docker, Docker Compose |
| Package manager | uv |

> **Scale path:** Replace RabbitMQ with Apache Kafka for >10k req/s.
> The producer/consumer interface is identical — only the client library changes.

---

## REST API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/splunk/alert` | POST | Splunk failure webhook (triggers investigation) |
| `/replay/{correlation_id}` | POST | Manually trigger replay |
| `/investigation/{correlation_id}` | GET | Run investigation on demand |

---

## Hackathon

- **Track:** Observability
- **Event:** Splunk Agentic Ops Hackathon 2026
- **Splunk capabilities used:** MCP Server, Cisco Deep Time Series Model (hosted), HEC, Alerts, REST API

---

## Licence

MIT — see [LICENSE](LICENSE)
