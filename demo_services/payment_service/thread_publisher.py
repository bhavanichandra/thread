import os
import time as _time
import urllib.parse
import aio_pika
import httpx
from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from pydantic import BaseModel, ConfigDict


class TraceEvent(str, Enum):
    REQUEST_START = "REQUEST_START"
    REQUEST_END   = "REQUEST_END"
    REQUEST_ERROR = "REQUEST_ERROR"


class ThreadMessage(BaseModel):
    correlationId:  str
    transactionId:  str
    sourceService:  str
    targetService:  str
    traceEvent:     TraceEvent
    timestamp:      datetime
    method:         Optional[str]   = None
    url:            Optional[str]   = None
    body:           Optional[dict]  = None
    statusCode:     Optional[int]   = None
    durationMs:     Optional[float] = None
    errorMessage:   Optional[str]   = None

    model_config = ConfigDict(use_enum_values=True)


# ── RabbitMQ ──────────────────────────────────────────────────────────────────

def get_rabbitmq_url() -> str:
    user = os.getenv('RABBITMQ_USER', 'thread')
    password = os.getenv('RABBITMQ_PASSWORD', '')
    host = os.getenv('RABBITMQ_HOST', 'localhost')
    port = os.getenv('RABBITMQ_PORT', '5672')
    vhost = os.getenv('RABBITMQ_VHOST', '/')

    auth = f"{urllib.parse.quote(user)}:{urllib.parse.quote(password)}" if password else urllib.parse.quote(user)
    vhost_encoded = urllib.parse.quote(vhost, safe='')

    return f"amqp://{auth}@{host}:{port}/{vhost_encoded}"

THREAD_LOGS_QUEUE = os.getenv("THREAD_LOGS_QUEUE", "thread_logs_queue")

async def publish_thread_event(
    correlation_id: str,
    transaction_id: str,
    source_service: str,
    target_service: str,
    trace_event: str,
    method: str = None,
    url: str = None,
    body: dict = None,
    status_code: int = None,
    duration_ms: float = None,
    error_message: str = None,
):
    """Publish a THREAD contract message to RabbitMQ. Fire-and-forget."""
    msg = ThreadMessage(
        correlationId=correlation_id,
        transactionId=transaction_id,
        sourceService=source_service,
        targetService=target_service,
        traceEvent=trace_event,
        timestamp=datetime.now(timezone.utc),
        method=method,
        url=url,
        body=body,
        statusCode=status_code,
        durationMs=duration_ms,
        errorMessage=error_message,
    )

    rabbitmq_url = get_rabbitmq_url()
    try:
        connection = await aio_pika.connect_robust(rabbitmq_url, timeout=3)
        async with connection:
            channel = await connection.channel()
            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=msg.model_dump_json().encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key=THREAD_LOGS_QUEUE,
            )
    except Exception as e:
        print(f"[THREAD] Publish failed (non-fatal): {e}")


# ── Splunk HEC ────────────────────────────────────────────────────────────────

SPLUNK_HEC_URL   = os.getenv("SPLUNK_HEC_URL", "http://localhost:8088")
SPLUNK_HEC_TOKEN = os.getenv("SPLUNK_HEC_TOKEN", "")
SPLUNK_INDEX     = os.getenv("SPLUNK_INDEX", "thread_logs")

async def log_to_splunk(
    correlation_id: str,
    transaction_id: str,
    source_service: str,
    target_service: str,
    trace_event: str,
    status_code: int = None,
    duration_ms: float = None,
    error_message: str = None,
    replay_attempt: int = 0,
) -> None:
    """Log structured event to Splunk HEC. Fire-and-forget."""
    trace_event_str = trace_event.value if hasattr(trace_event, "value") else str(trace_event)
    ts = datetime.now(timezone.utc).isoformat()

    payload = {
        "time": _time.time(),
        "source": "thread",
        "sourcetype": "thread:transaction",
        "index": SPLUNK_INDEX,
        "event": {
            "correlationId":  correlation_id,
            "transactionId":  transaction_id,
            "sourceService":  source_service,
            "targetService":  target_service,
            "traceEvent":     trace_event_str,
            "timestamp":      ts,
            "statusCode":     status_code,
            "durationMs":     duration_ms,
            "errorMessage":   error_message,
            "replayAttempt":  replay_attempt,
        },
    }
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            await client.post(
                f"{SPLUNK_HEC_URL}/services/collector/event",
                headers={"Authorization": f"Splunk {SPLUNK_HEC_TOKEN}"},
                json=payload,
            )
    except Exception as e:
        print(f"[THREAD] HEC log failed (non-fatal): {e}")
