"""
thread_logs_queue consumer — validates THREAD contract messages and stores them in SQLite.
"""

import asyncio
import json
import os

import aio_pika

from ..setup_queues import get_rabbitmq_url
from ..store.database import mark_failed, save_message

THREAD_LOGS_QUEUE = os.getenv("THREAD_LOGS_QUEUE", "thread_logs_queue")


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
                queue = await channel.declare_queue(THREAD_LOGS_QUEUE, durable=True)
                print(f"[THREAD] logs_consumer started — listening on {THREAD_LOGS_QUEUE}")

                async with queue.iterator() as q:
                    async for message in q:
                        async with message.process():
                            try:
                                msg = json.loads(message.body.decode())
                                await asyncio.to_thread(save_message, msg)

                                if msg.get("traceEvent") == "REQUEST_ERROR":
                                    correlation_id = msg.get("correlationId", "unknown")
                                    await asyncio.to_thread(mark_failed, correlation_id, msg)
                                    print(
                                        f"[THREAD] Failure recorded: {correlation_id} "
                                        f"({msg.get('sourceService')} → {msg.get('targetService')})"
                                    )

                            except Exception as e:
                                print(f"[THREAD] logs_consumer error: {e}")

        except asyncio.CancelledError:
            print("[THREAD] logs_consumer stopped.")
            break
        except Exception as e:
            print(f"[THREAD] logs_consumer lost connection: {e} — retrying in 5s")
            await asyncio.sleep(5)
