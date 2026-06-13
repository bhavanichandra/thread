"""
thread_logs_queue consumer — validates THREAD contract messages and stores them in SQLite.
On REQUEST_ERROR, triggers investigation and posts to Slack automatically.
"""

import asyncio
import json
import os

import aio_pika

from ..agent.investigator import InvestigationAgent
from ..setup_queues import get_rabbitmq_url
from ..slack.commands import set_last_failed
from ..slack.handler import post_investigation_result
from ..store.database import mark_failed, save_message

THREAD_LOGS_QUEUE = os.getenv("THREAD_LOGS_QUEUE", "thread_logs_queue")

_investigation_agent = InvestigationAgent()
# Dedup set — prevent double-investigation when multiple hops publish REQUEST_ERROR
_investigated: set[str] = set()


async def _investigate_and_alert(correlation_id: str, context: dict) -> None:
    try:
        result = await _investigation_agent.investigate(correlation_id, context=context)
        await post_investigation_result(result)
    except Exception as e:
        print(f"[THREAD] Investigation/alert failed for {correlation_id}: {e}")


async def start_logs_consumer() -> None:
    """Long-running consumer. Call as asyncio.create_task() on startup."""
    rabbitmq_url = get_rabbitmq_url()
    while True:
        try:
            print(f"[THREAD] logs_consumer connecting...")
            connection = await aio_pika.connect_robust(rabbitmq_url, timeout=10)
            async with connection:
                channel = await connection.channel()
                await channel.set_qos(prefetch_count=10)
                # passive=True avoids redeclaring with mismatched args (setup_queues owns the declaration)
                queue = await channel.declare_queue(THREAD_LOGS_QUEUE, durable=True, passive=True)
                print(f"[THREAD] logs_consumer started — listening on {THREAD_LOGS_QUEUE}")

                async with queue.iterator() as q:
                    async for message in q:
                        try:
                            async with message.process():
                                msg = json.loads(message.body.decode())
                                await asyncio.to_thread(save_message, msg)

                                if msg.get("traceEvent") == "REQUEST_ERROR":
                                    correlation_id = msg.get("correlationId", "unknown")
                                    await asyncio.to_thread(mark_failed, correlation_id, msg)
                                    set_last_failed(correlation_id)
                                    if correlation_id not in _investigated:
                                        _investigated.add(correlation_id)
                                        print(
                                            f"[THREAD] Failure detected: {correlation_id} "
                                            f"({msg.get('sourceService')} → {msg.get('targetService')}) "
                                            f"— triggering investigation"
                                        )
                                        asyncio.create_task(_investigate_and_alert(correlation_id, msg))
                                    else:
                                        print(
                                            f"[THREAD] Failure already investigated: {correlation_id} "
                                            f"({msg.get('sourceService')} → {msg.get('targetService')}) — skipping"
                                        )
                        except Exception as e:
                            # Exception propagated through message.process() → nack already sent
                            print(f"[THREAD] logs_consumer error (message nacked): {e}")

        except asyncio.CancelledError:
            print("[THREAD] logs_consumer stopped.")
            break
        except Exception as e:
            print(f"[THREAD] logs_consumer lost connection: {e} — retrying in 5s")
            await asyncio.sleep(5)
