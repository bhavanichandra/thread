import os
import json
import urllib.parse
import aio_pika
from datetime import datetime, timezone

def get_rabbitmq_url() -> str:
    user = os.getenv('RABBITMQ_USER', 'thread')
    password = os.getenv('RABBITMQ_PASSWORD', '')
    host = os.getenv('RABBITMQ_HOST', 'localhost')
    port = os.getenv('RABBITMQ_PORT', '5672')
    vhost = os.getenv('RABBITMQ_VHOST', '/')
    
    # If password exists, URL-encode it to handle special characters
    auth = f"{urllib.parse.quote(user)}:{urllib.parse.quote(password)}" if password else urllib.parse.quote(user)
    vhost_encoded = urllib.parse.quote(vhost, safe='')
    
    return f"amqp://{auth}@{host}:{port}/{vhost_encoded}"

THREAD_LOGS_QUEUE = os.getenv("THREAD_LOGS_QUEUE", "thread_logs_queue")

async def publish_thread_event(
    correlation_id: str,
    transaction_id: str,
    source_service: str,
    target_service: str,
    trace_event: str,       # REQUEST_START | REQUEST_END | REQUEST_ERROR
    method: str = None,
    url: str = None,
    body: dict = None,
    status_code: int = None,
    duration_ms: float = None,
    error_message: str = None,
):
    """Publish a THREAD contract message to RabbitMQ. Fire-and-forget."""
    msg = {
        "correlationId": correlation_id,
        "transactionId": transaction_id,
        "sourceService": source_service,
        "targetService": target_service,
        "traceEvent":    trace_event,
        "timestamp":     datetime.now(timezone.utc).isoformat(),
    }
    if method:        msg["method"]       = method
    if url:           msg["url"]          = url
    if body:          msg["body"]         = body
    if status_code is not None:   msg["statusCode"]   = status_code
    if duration_ms is not None:   msg["durationMs"]   = duration_ms
    if error_message: msg["errorMessage"] = error_message

    rabbitmq_url = get_rabbitmq_url()
    try:
        connection = await aio_pika.connect_robust(rabbitmq_url, timeout=3)
        async with connection:
            channel = await connection.channel()
            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(msg).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key=THREAD_LOGS_QUEUE,
            )
    except Exception as e:
        # Never let THREAD publishing break the service
        print(f"[THREAD] Publish failed (non-fatal): {e}")
