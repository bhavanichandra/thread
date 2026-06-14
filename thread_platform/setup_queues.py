import os
import aio_pika
import asyncio
import urllib.parse

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

RABBITMQ_URL = get_rabbitmq_url()
THREAD_LOGS_QUEUE = os.getenv("THREAD_LOGS_QUEUE", "thread_logs_queue")
SLACK_MESSAGES_QUEUE = os.getenv("SLACK_MESSAGES_QUEUE", "slack_messages_queue")

async def setup_queues():
    """Declare queues on startup. Retries with backoff until RabbitMQ is ready."""
    url = get_rabbitmq_url()
    last_err = None
    for attempt in range(12):
        try:
            connection = await aio_pika.connect_robust(url, timeout=10)
            async with connection:
                channel = await connection.channel()
                await channel.declare_queue(
                    THREAD_LOGS_QUEUE,
                    durable=True,
                    arguments={"x-message-ttl": 86400000},
                )
                await channel.declare_queue(SLACK_MESSAGES_QUEUE, durable=True)
            print(f"[THREAD] Queues ready: {THREAD_LOGS_QUEUE}, {SLACK_MESSAGES_QUEUE}")
            return
        except Exception as e:
            last_err = e
            wait = min(2 ** attempt, 30)
            print(f"[THREAD] RabbitMQ not ready (attempt {attempt + 1}/12), retry in {wait}s: {e}")
            await asyncio.sleep(wait)
    raise RuntimeError(f"RabbitMQ unreachable after 12 attempts: {last_err}")

if __name__ == "__main__":
    asyncio.run(setup_queues())
