"""
SQLite store — schema, CRUD, and TTL cleanup for THREAD messages.

Column names use snake_case at the DB level; callers pass camelCase dicts.
"""

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

DB_PATH = os.getenv("SQLITE_PATH", "thread_store.db")


def init_db() -> None:
    """Create tables and indexes. Safe to call multiple times."""
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS thread_messages (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                correlation_id  TEXT NOT NULL,
                transaction_id  TEXT NOT NULL,
                source_service  TEXT NOT NULL,
                target_service  TEXT NOT NULL,
                trace_event     TEXT NOT NULL,
                method          TEXT,
                url             TEXT,
                body            TEXT,
                status_code     INTEGER,
                duration_ms     REAL,
                error_message   TEXT,
                timestamp       TEXT NOT NULL,
                created_at      TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_correlation_id
                ON thread_messages(correlation_id);

            CREATE INDEX IF NOT EXISTS idx_trace_event
                ON thread_messages(trace_event);

            CREATE INDEX IF NOT EXISTS idx_corr_event
                ON thread_messages(correlation_id, trace_event);

            CREATE TABLE IF NOT EXISTS failed_transactions (
                correlation_id  TEXT PRIMARY KEY,
                failed_at       TEXT NOT NULL,
                source_service  TEXT NOT NULL,
                target_service  TEXT NOT NULL,
                error_message   TEXT,
                replay_count    INTEGER DEFAULT 0,
                resolved        INTEGER DEFAULT 0
            );
        """)


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def save_message(msg: dict) -> None:
    body = msg.get("body")
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO thread_messages
              (correlation_id, transaction_id, source_service, target_service,
               trace_event, method, url, body, status_code, duration_ms,
               error_message, timestamp)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                msg["correlationId"],
                msg["transactionId"],
                msg["sourceService"],
                msg["targetService"],
                msg["traceEvent"],
                msg.get("method"),
                msg.get("url"),
                json.dumps(body) if isinstance(body, (dict, list)) else body,
                msg.get("statusCode"),
                msg.get("durationMs"),
                msg.get("errorMessage"),
                msg.get("timestamp", datetime.now(timezone.utc).isoformat()),
            ),
        )


def mark_failed(correlation_id: str, msg: dict) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO failed_transactions
              (correlation_id, failed_at, source_service, target_service, error_message)
            VALUES (?,?,?,?,?)
            """,
            (
                correlation_id,
                datetime.now(timezone.utc).isoformat(),
                msg["sourceService"],
                msg["targetService"],
                msg.get("errorMessage", ""),
            ),
        )


def get_replay_request(correlation_id: str) -> Optional[dict]:
    """Return the original REQUEST_START payload for replay."""
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT method, url, body
            FROM thread_messages
            WHERE correlation_id = ?
              AND trace_event = 'REQUEST_START'
            ORDER BY created_at ASC
            LIMIT 1
            """,
            (correlation_id,),
        ).fetchone()

        if not row:
            return None
        return {
            "method": row["method"],
            "url":    row["url"],
            "body":   json.loads(row["body"]) if row["body"] else None,
        }


def cleanup_old_messages(hours: int = 24) -> int:
    """Delete messages older than N hours. Returns rows deleted."""
    with get_conn() as conn:
        result = conn.execute(
            "DELETE FROM thread_messages WHERE created_at < datetime('now', ? || ' hours')",
            (f"-{hours}",),
        )
        return result.rowcount
