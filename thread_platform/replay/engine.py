"""
ReplayEngine — re-executes the original entry-point request stored in SQLite.
"""

import time
from datetime import datetime, timezone
from typing import Optional

import httpx
from pydantic import BaseModel

from ..store.database import get_replay_request


class ReplayResult(BaseModel):
    correlation_id: str
    attempt_number: int
    success:        bool
    http_status:    Optional[int]   = None
    duration_ms:    Optional[float] = None
    response_body:  Optional[dict]  = None
    error:          Optional[str]   = None
    replayed_at:    datetime        = datetime.now(timezone.utc)


class ReplayNotFoundError(Exception):
    pass


class ReplayEngine:

    async def execute(self, correlation_id: str, attempt_number: int = 1) -> ReplayResult:
        stored = get_replay_request(correlation_id)
        if not stored:
            raise ReplayNotFoundError(
                f"No stored request for correlation_id: {correlation_id}"
            )

        method = stored["method"]
        url    = stored["url"]
        body   = stored["body"]

        if not method or not url:
            raise ReplayNotFoundError(
                f"Stored request for {correlation_id} is missing method or url"
            )

        headers = {
            "x-correlation-id": correlation_id,
            "x-replay-attempt": str(attempt_number),
            "x-thread-replay":  "true",
            "Content-Type":     "application/json",
        }

        start = time.monotonic()
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=body,
                )

            duration_ms = (time.monotonic() - start) * 1000
            success     = response.status_code < 400

            try:
                resp_body = response.json()
            except Exception:
                resp_body = {"raw": response.text}

            print(
                f"[THREAD] Replay {correlation_id[:8]}... attempt={attempt_number} "
                f"status={response.status_code} ({duration_ms:.0f}ms)"
            )
            return ReplayResult(
                correlation_id=correlation_id,
                attempt_number=attempt_number,
                success=success,
                http_status=response.status_code,
                duration_ms=round(duration_ms, 2),
                response_body=resp_body,
            )

        except httpx.TimeoutException:
            duration_ms = (time.monotonic() - start) * 1000
            return ReplayResult(
                correlation_id=correlation_id,
                attempt_number=attempt_number,
                success=False,
                duration_ms=round(duration_ms, 2),
                error=f"Timeout after {duration_ms:.0f}ms",
            )

        except Exception as e:
            duration_ms = (time.monotonic() - start) * 1000
            return ReplayResult(
                correlation_id=correlation_id,
                attempt_number=attempt_number,
                success=False,
                duration_ms=round(duration_ms, 2),
                error=str(e),
            )


replay_engine = ReplayEngine()
