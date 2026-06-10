import os
import json
import sqlite3
import asyncio
import logging
import threading
from contextlib import asynccontextmanager
from fastapi import FastAPI
import aio_pika

from setup_queues import setup_queues, get_rabbitmq_url

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("thread-platform")

DB_PATH = os.getenv("SQLITE_DB_PATH", "thread.db")
THREAD_LOGS_QUEUE = os.getenv("THREAD_LOGS_QUEUE", "thread_logs_queue")

# Database concurrency lock and persistent connection
db_lock = threading.Lock()
_db_conn = None

def get_db_conn():
    global _db_conn
    with db_lock:
        if _db_conn is None:
            _db_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        return _db_conn

def init_db():
    """Create the SQLite database, schema, and indexes if they don't exist."""
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
        # Indexes for query performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_thread_messages_correlationId ON thread_messages (correlationId)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_thread_messages_transactionId ON thread_messages (transactionId)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_thread_messages_sourceService ON thread_messages (sourceService)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_thread_messages_targetService ON thread_messages (targetService)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_thread_messages_timestamp ON thread_messages (timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_thread_messages_corr_time ON thread_messages (correlationId, timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_thread_messages_trans_time ON thread_messages (transactionId, timestamp)")
        
        conn.commit()
        logger.info(f"Database initialized at {DB_PATH}. Table thread_messages and indexes are ready.")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise e
    finally:
        conn.close()

def save_message_to_db(data: dict):
    """Write log event to SQLite table thread_messages using the shared connection."""
    body_val = data.get("body")
    body_serialized = None
    if body_val is not None:
        if isinstance(body_val, (dict, list)):
            try:
                body_serialized = json.dumps(body_val)
            except (TypeError, ValueError):
                body_serialized = str(body_val)
        else:
            body_serialized = str(body_val)

    conn = get_db_conn()
    with db_lock:
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
                body_serialized,
                data.get("statusCode"),
                data.get("durationMs"),
                data.get("errorMessage"),
                data.get("timestamp")
            ))
            conn.commit()
            logger.info(f"Saved message from {data.get('sourceService')} to {data.get('targetService')} - event: {data.get('traceEvent')}")
        except Exception as e:
            logger.error(f"Failed to save message to SQLite: {e}")
            raise e

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
                
                queue = await channel.declare_queue(
                    THREAD_LOGS_QUEUE,
                    durable=True,
                    arguments={
                        "x-message-ttl": 86400000,  # 24h TTL in ms
                    }
                )
                logger.info(f"Consumer connected. Subscribed to {THREAD_LOGS_QUEUE}")
                
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        try:
                            # Use requeue=True so it is requeued on failure
                            async with message.process(requeue=True):
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
    # Initialize DB schema and indexes in a background thread to prevent blocking
    await asyncio.to_thread(init_db)
    
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
    
    # Close persistent DB connection if open
    global _db_conn
    if _db_conn is not None:
        try:
            _db_conn.close()
            logger.info("Closed persistent database connection.")
        except Exception as e:
            logger.error(f"Error closing DB connection on shutdown: {e}")

app = FastAPI(title="Thread Platform", lifespan=lifespan)

@app.get("/health")
async def health():
    return {"status": "ok", "service": "thread-platform"}
