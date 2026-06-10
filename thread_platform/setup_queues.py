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
    """Declare queues on startup. Idempotent — safe to run multiple times."""
    url = get_rabbitmq_url()
    try:
        connection = await aio_pika.connect_robust(url, timeout=5)
        async with connection:
            channel = await connection.channel()

            # thread_logs_queue — durable, survives restarts
            await channel.declare_queue(
                THREAD_LOGS_QUEUE,
                durable=True,
                arguments={
                    "x-message-ttl": 86400000,  # 24h TTL in ms
                }
            )

            # slack_messages_queue — durable
            await channel.declare_queue(
                SLACK_MESSAGES_QUEUE,
                durable=True,
            )

            print(f"[THREAD] Queues ready: {THREAD_LOGS_QUEUE}, {SLACK_MESSAGES_QUEUE}")
    except Exception as e:
        print(f"[THREAD] Queue setup failed: {e}")
        raise e

if __name__ == "__main__":
    asyncio.run(setup_queues())
