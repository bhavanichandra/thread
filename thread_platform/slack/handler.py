"""
Slack bot — Socket Mode, Block Kit failure alerts, ack() → slack_messages_queue.

ack() must complete in < 3 seconds. All processing happens in slack_consumer.
"""

import json
import os
from datetime import datetime, timezone

import aio_pika
from slack_bolt.async_app import AsyncApp
from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler

SLACK_BOT_TOKEN      = os.getenv("SLACK_BOT_TOKEN", "")
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET", "")
SLACK_APP_TOKEN      = os.getenv("SLACK_APP_TOKEN", "")
SLACK_ALERT_CHANNEL  = os.getenv("SLACK_ALERT_CHANNEL", "#thread-alerts")

SLACK_MESSAGES_QUEUE = os.getenv("SLACK_MESSAGES_QUEUE", "slack_messages_queue")


def _rabbitmq_url() -> str:
    from ..setup_queues import get_rabbitmq_url
    return get_rabbitmq_url()


app = AsyncApp(token=SLACK_BOT_TOKEN, signing_secret=SLACK_SIGNING_SECRET)


async def post_investigation_result(result) -> None:
    """Post a Block Kit failure alert to #thread-alerts."""
    from .blocks import build_failure_alert_blocks
    blocks = build_failure_alert_blocks(result)
    try:
        await app.client.chat_postMessage(
            channel=SLACK_ALERT_CHANNEL,
            text=f"Transaction failure detected: {result.correlation_id}",
            blocks=blocks,
        )
    except Exception as e:
        print(f"[THREAD] Slack alert failed for {result.correlation_id} (non-fatal): {e}")


async def _publish_action(action: str, correlation_id: str, response_url: str) -> None:
    try:
        connection = await aio_pika.connect_robust(_rabbitmq_url())
        async with connection:
            channel = await connection.channel()
            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps({
                        "action":        action,
                        "correlationId": correlation_id,
                        "responseUrl":   response_url,
                        "timestamp":     datetime.now(timezone.utc).isoformat(),
                    }).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key=SLACK_MESSAGES_QUEUE,
            )
    except Exception as e:
        print(f"[THREAD] _publish_action failed (non-fatal): {e}")


@app.action("trigger_replay")
async def handle_replay(ack, body, respond):
    await ack()
    value          = body["actions"][0]["value"]
    correlation_id = value.split(":")[0]
    response_url   = body.get("response_url", "")
    await respond({"replace_original": True,
                   "text": f"⏳ Queuing replay for `{correlation_id}`..."})
    await _publish_action("REPLAY", correlation_id, response_url)


@app.action("skip_transaction")
async def handle_skip(ack, body):
    await ack()
    await _publish_action(
        "SKIP",
        body["actions"][0]["value"],
        body.get("response_url", ""),
    )


@app.action("escalate_transaction")
async def handle_escalate(ack, body):
    await ack()
    await _publish_action(
        "ESCALATE",
        body["actions"][0]["value"],
        body.get("response_url", ""),
    )


@app.action("view_in_splunk")
async def handle_view_splunk(ack):
    await ack()


async def start_slack_socket_mode() -> None:
    if not SLACK_APP_TOKEN:
        print("[THREAD] SLACK_APP_TOKEN not set — Slack Socket Mode disabled")
        return
    handler = AsyncSocketModeHandler(app, SLACK_APP_TOKEN)
    await handler.start_async()
