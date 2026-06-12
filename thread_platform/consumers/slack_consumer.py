"""
slack_messages_queue consumer — processes Slack button actions after ack().

Handles REPLAY, SKIP, ESCALATE. Posts results back to Slack via response_url.
"""

import asyncio
import json
import os

import aio_pika
import httpx

from ..replay.engine import ReplayEngine, ReplayNotFoundError
from ..setup_queues import get_rabbitmq_url

SLACK_MESSAGES_QUEUE = os.getenv("SLACK_MESSAGES_QUEUE", "slack_messages_queue")

_replay_engine = ReplayEngine()


async def start_slack_consumer() -> None:
    """Long-running consumer. Call as asyncio.create_task() on startup."""
    rabbitmq_url = get_rabbitmq_url()
    while True:
        try:
            print("[THREAD] slack_consumer connecting...")
            connection = await aio_pika.connect_robust(rabbitmq_url, timeout=10)
            async with connection:
                channel = await connection.channel()
                await channel.set_qos(prefetch_count=5)
                queue = await channel.declare_queue(SLACK_MESSAGES_QUEUE, durable=True)
                print(f"[THREAD] slack_consumer started — listening on {SLACK_MESSAGES_QUEUE}")

                async with queue.iterator() as q:
                    async for message in q:
                        async with message.process():
                            try:
                                msg = json.loads(message.body.decode())
                                await _handle_action(msg)
                            except Exception as e:
                                print(f"[THREAD] slack_consumer error: {e}")

        except asyncio.CancelledError:
            print("[THREAD] slack_consumer stopped.")
            break
        except Exception as e:
            print(f"[THREAD] slack_consumer lost connection: {e} — retrying in 5s")
            await asyncio.sleep(5)


async def _handle_action(msg: dict) -> None:
    action         = msg.get("action")
    correlation_id = msg.get("correlationId", "unknown")
    response_url   = msg.get("responseUrl", "")

    if action == "REPLAY":
        await _handle_replay(correlation_id, response_url)
    elif action == "SKIP":
        await _post(response_url, {
            "replace_original": True,
            "text": f"⏭️ Transaction `{correlation_id}` skipped by ops team.",
        })
    elif action == "ESCALATE":
        await _post(response_url, {
            "replace_original": True,
            "text": f"🚨 Transaction `{correlation_id}` escalated to on-call.",
        })
    else:
        print(f"[THREAD] slack_consumer: unknown action {action!r}")


async def _handle_replay(correlation_id: str, response_url: str) -> None:
    await _post(response_url, {
        "replace_original": True,
        "text": f"⏳ Replaying transaction `{correlation_id}`...",
    })

    try:
        result = await _replay_engine.execute(correlation_id)
    except ReplayNotFoundError:
        await _post(response_url, {
            "replace_original": True,
            "text": f"❌ Cannot replay — no stored request found for `{correlation_id}`.",
        })
        return

    if result.success:
        await _post(response_url, {
            "replace_original": True,
            "blocks": [
                {"type": "header",
                 "text": {"type": "plain_text", "text": "✅ Replay Succeeded"}},
                {"type": "section",
                 "fields": [
                     {"type": "mrkdwn", "text": f"*Correlation ID*\n`{correlation_id}`"},
                     {"type": "mrkdwn", "text": f"*Status*\n`HTTP {result.http_status}`"},
                     {"type": "mrkdwn", "text": f"*Duration*\n{result.duration_ms:.0f}ms"},
                     {"type": "mrkdwn", "text": f"*Attempt*\n{result.attempt_number}"},
                 ]},
            ],
        })
    else:
        await _post(response_url, {
            "replace_original": True,
            "text": (
                f"🚫 Replay failed for `{correlation_id}`\n"
                f"Error: {result.error or f'HTTP {result.http_status}'}\n"
                f"Consider escalating to on-call."
            ),
        })


async def _post(url: str, payload: dict) -> None:
    if not url:
        return
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json=payload)
            if resp.status_code != 200:
                print(f"[THREAD] Slack response_url failed: {resp.status_code}")
    except Exception as e:
        print(f"[THREAD] Slack post error: {e}")
