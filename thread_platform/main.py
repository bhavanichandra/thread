import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request

from .agent.investigator import InvestigationAgent
from .consumers.logs_consumer import start_logs_consumer
from .consumers.slack_consumer import start_slack_consumer
from .replay.engine import ReplayEngine, ReplayNotFoundError
from .setup_queues import setup_queues
from .slack.handler import post_investigation_result, start_slack_socket_mode
from .store.database import cleanup_old_messages, init_db

_investigation_agent = InvestigationAgent()
_replay_engine       = ReplayEngine()


async def _cleanup_loop() -> None:
    while True:
        await asyncio.sleep(3600)
        deleted = cleanup_old_messages(hours=24)
        print(f"[THREAD] Cleanup: removed {deleted} old messages")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await asyncio.to_thread(init_db)
    print("[THREAD] SQLite initialised")

    await setup_queues()

    asyncio.create_task(start_logs_consumer())
    asyncio.create_task(start_slack_consumer())
    asyncio.create_task(start_slack_socket_mode())
    asyncio.create_task(_cleanup_loop())

    yield


app = FastAPI(title="THREAD Platform", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "thread-platform"}


@app.post("/splunk/alert")
async def splunk_alert(request: Request):
    """Splunk failure alert webhook — triggers investigation and posts to Slack."""
    try:
        body = await request.json()
    except Exception:
        body = {}

    correlation_id = body.get("correlation_id") or body.get("correlationId", "")
    service_name   = body.get("service_name",   "unknown")
    error_message  = body.get("error_message",  "")

    if not correlation_id:
        return {"status": "ignored", "reason": "no correlation_id"}

    print(
        f"[THREAD] Alert received: correlationId={correlation_id} "
        f"service={service_name}"
    )

    asyncio.create_task(_run_investigation(correlation_id))
    return {"status": "investigating", "correlation_id": correlation_id}


async def _run_investigation(correlation_id: str) -> None:
    try:
        result = await _investigation_agent.investigate(correlation_id)
        await post_investigation_result(result)
    except Exception as e:
        print(f"[THREAD] Investigation failed for {correlation_id}: {e}")


@app.post("/replay/{correlation_id}")
async def trigger_replay(correlation_id: str, request: Request):
    """Re-execute the original request stored in SQLite."""
    try:
        body = await request.json()
    except Exception:
        body = {}

    attempt = int(body.get("attempt", 1))
    try:
        result = await _replay_engine.execute(correlation_id, attempt)
        return result.model_dump()
    except ReplayNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/investigation/{correlation_id}")
async def run_investigation(correlation_id: str):
    """Manually trigger an investigation (useful for demos)."""
    try:
        result = await _investigation_agent.investigate(correlation_id)
        return result.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
