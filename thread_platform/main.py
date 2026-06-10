import os
import json
import sqlite3
import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
import aio_pika

from setup_queues import setup_queues, get_rabbitmq_url

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("thread-platform")

DB_PATH = os.getenv("SQLITE_DB_PATH", "thread.db")
THREAD_LOGS_QUEUE = os.getenv("THREAD_LOGS_QUEUE", "thread_logs_queue")

def init_db():
    """Create the SQLite database and schema if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS thread_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                correlationId TEXT NOT NULL,
                transactionId TEXT NOT NULL,
                sourceService TEXT NOT NULL,
                targetService TEXT NOT NULL,
                traceEvent TEXT NOT NULL,
                method TEXT,
                url TEXT,
                body TEXT,
                statusCode INTEGER,
                durationMs REAL,
                errorMessage TEXT,
                timestamp TEXT NOT NULL
            )
        """)
        conn.commit()
        logger.info(f"Database initialized at {DB_PATH}. Table thread_messages is ready.")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise e
    finally:
        conn.close()

def save_message_to_db(data: dict):
    """Write log event to SQLite table thread_messages."""
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO thread_messages (
                correlationId, transactionId, sourceService, targetService,
                traceEvent, method, url, body, statusCode, durationMs,
                errorMessage, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get("correlationId"),
            data.get("transactionId"),
            data.get("sourceService"),
            data.get("targetService"),
            data.get("traceEvent"),
            data.get("method"),
            data.get("url"),
            json.dumps(data.get("body")) if data.get("body") is not None else None,
            data.get("statusCode"),
            data.get("durationMs"),
            data.get("errorMessage"),
            data.get("timestamp")
        ))
        conn.commit()
        logger.info(f"Saved message from {data.get('sourceService')} to {data.get('targetService')} - event: {data.get('traceEvent')}")
    except Exception as e:
        logger.error(f"Failed to save message to SQLite: {e}")
    finally:
        conn.close()

async def consumer_loop():
    """Background task consuming messages from RabbitMQ and writing them to SQLite."""
    rabbitmq_url = get_rabbitmq_url()
    while True:
        try:
            logger.info(f"Consumer connecting to RabbitMQ at {rabbitmq_url.split('@')[-1]}...")
            connection = await aio_pika.connect_robust(rabbitmq_url, timeout=5)
            async with connection:
                channel = await connection.channel()
                # Set prefetch count to 10
                await channel.set_qos(prefetch_count=10)
                
                # Make sure queue is declared
                queue = await channel.declare_queue(THREAD_LOGS_QUEUE, durable=True)
                logger.info(f"Consumer connected. Subscribed to {THREAD_LOGS_QUEUE}")
                
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        async with message.process():
                            try:
                                body_str = message.body.decode()
                                data = json.loads(body_str)
                                # Write to DB in a separate thread to avoid blocking loop
                                await asyncio.to_thread(save_message_to_db, data)
                            except Exception as e:
                                logger.error(f"Error processing consumer message: {e}")
        except asyncio.CancelledError:
            logger.info("Consumer loop stopped (cancelled).")
            break
        except Exception as e:
            logger.error(f"Consumer connection lost or error: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize DB schema
    init_db()
    
    # Run setup queues to declare all queues on startup
    await setup_queues()
    
    # Start RabbitMQ log consumer as background task
    consumer_task = asyncio.create_task(consumer_loop())
    
    yield
    
    # Clean up background task on shutdown
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

app = FastAPI(title="Thread Platform", lifespan=lifespan)

@app.get("/health")
async def health():
    return {"status": "ok", "service": "thread-platform"}
