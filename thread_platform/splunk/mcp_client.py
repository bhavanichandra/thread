"""
SplunkMCPClient — executes SPL queries via the Splunk MCP Server.

Uses real MCP protocol: SSE transport + JSON-RPC 2.0, calling the
splunk_run_query tool exposed by the official Splunkbase MCP Server app.

Every query is timed and printed with the [THREAD:MCP] prefix so judges
can see real MCP activity in the terminal during the demo recording.

Connection: HTTPS to https://splunk:8089/services/mcp
Auth:       Bearer encrypted token (SPLUNK_MCP_TOKEN from Infisical)
Transport:  Server-Sent Events (SSE) + JSON-RPC 2.0
Tool:       splunk_run_query
"""

import os
import time as _time
import logging
import json

from mcp.client.session import ClientSession
from mcp.client.sse import sse_client

from .queries import (
    transaction_chain_query,
    failure_details_query,
    service_health_query,
    system_errors_query,
    error_rate_timeseries_query,
)

logger = logging.getLogger("thread-platform")

SPLUNK_MCP_URL   = os.getenv("SPLUNK_MCP_URL", "https://localhost:8089/services/mcp")
SPLUNK_MCP_TOKEN = os.getenv("SPLUNK_MCP_TOKEN", "")
SPLUNK_INDEX     = os.getenv("SPLUNK_INDEX", "thread_logs")


def _parse_tool_result(response) -> list[dict]:
    """Extract result rows from an MCP tool call response."""
    rows: list[dict] = []
    for item in response.content:
        text = getattr(item, "text", None)
        if not text:
            continue
        try:
            data = json.loads(text)
            if isinstance(data, list):
                rows.extend(data)
            elif isinstance(data, dict):
                rows.append(data)
        except (json.JSONDecodeError, TypeError):
            pass
    return rows


class SplunkMCPClient:
    """Async MCP client for Splunk — real SSE + JSON-RPC 2.0 protocol."""

    def __init__(self):
        self._url = SPLUNK_MCP_URL
        self._headers = {"Authorization": f"Bearer {SPLUNK_MCP_TOKEN}"}

    # ── core ──────────────────────────────────────────────────────────────────

    async def search(
        self,
        spl: str,
        earliest: str = "-15m",
        latest: str = "now",
        max_results: int = 100,
        _label: str = "search",
    ) -> list[dict]:
        """Call splunk_run_query via MCP and return rows as a list of dicts.

        Prints [THREAD:MCP] timing so judges see every query fire.
        """
        t0 = _time.monotonic()
        results: list[dict] = []

        try:
            async with sse_client(self._url, headers=self._headers) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    response = await session.call_tool(
                        "splunk_run_query",
                        {
                            "query":        f"search {spl}",
                            "earliest_time": earliest,
                            "latest_time":   latest,
                            "max_count":     max_results,
                        },
                    )
                    results = _parse_tool_result(response)

        except Exception as exc:
            logger.warning(f"[THREAD:MCP] {_label} failed: {exc}")

        elapsed = (_time.monotonic() - t0) * 1000
        print(f"[THREAD:MCP] {_label:<55} → {len(results):>3} results ({elapsed:.0f}ms)")
        return results

    # ── named queries ─────────────────────────────────────────────────────────

    async def get_transaction_chain(self, correlation_id: str) -> list[dict]:
        return await self.search(
            transaction_chain_query(correlation_id),
            earliest="-24h",
            _label=f"get_transaction_chain({correlation_id[:8]}...)",
        )

    async def get_failure_details(self, correlation_id: str) -> list[dict]:
        return await self.search(
            failure_details_query(correlation_id),
            earliest="-24h",
            _label=f"get_failure_details({correlation_id[:8]}...)",
        )

    async def get_service_health(
        self, service_name: str, window: str = "-15m"
    ) -> dict:
        results = await self.search(
            service_health_query(service_name, window),
            earliest=window,
            _label=f"get_service_health({service_name})",
        )
        if results:
            r = results[0]
            return {
                "ok":         int(r.get("ok", 0)),
                "total":      int(r.get("total", 0)),
                "error_rate": float(r.get("error_rate", 0.0)),
                "health_pct": float(r.get("health_pct", 100.0)),
            }
        return {"ok": 0, "total": 0, "error_rate": 100.0, "health_pct": 0.0}

    async def get_system_errors(self, window: str = "-15m") -> list[dict]:
        return await self.search(
            system_errors_query(window),
            earliest=window,
            _label="get_system_errors()",
        )

    async def get_error_rate_timeseries(
        self, service_name: str, window: str = "-1h"
    ) -> list[dict]:
        return await self.search(
            error_rate_timeseries_query(service_name, window),
            earliest=window,
            _label=f"get_error_rate_timeseries({service_name})",
        )
