"""
SplunkMCPClient — executes SPL queries via the Splunk MCP Server.

Uses real MCP protocol: Streamable HTTP transport + JSON-RPC 2.0, calling the
splunk_run_query tool exposed by the official Splunkbase MCP Server app.

Every query is timed and printed with the [THREAD:MCP] prefix so judges
can see real MCP activity in the terminal during the demo recording.

Connection: HTTPS to https://splunk:8089/services/mcp
Auth:       Bearer encrypted token (SPLUNK_MCP_TOKEN from Infisical)
Transport:  Streamable HTTP (POST) + JSON-RPC 2.0  ← NOT SSE
Tool:       splunk_run_query
"""

import os
import time as _time
import logging
import json

import httpx
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from .queries import (
    transaction_chain_query,
    failure_details_query,
    service_health_query,
    system_errors_query,
    error_rate_timeseries_query,
)

logger = logging.getLogger("thread-platform")

SPLUNK_MCP_URL      = os.getenv("SPLUNK_MCP_URL", "https://localhost:8089/services/mcp")
SPLUNK_MCP_TOKEN    = os.getenv("SPLUNK_MCP_TOKEN", "")
SPLUNK_INDEX        = os.getenv("SPLUNK_INDEX", "thread_logs")
SPLUNK_MCP_INSECURE = os.getenv("SPLUNK_MCP_INSECURE", "false").lower() == "true"


def _make_httpx_client(
    headers: dict | None = None,
    timeout: httpx.Timeout | None = None,
    auth: httpx.Auth | None = None,
) -> httpx.AsyncClient:
    """httpx client factory for MCP transport."""
    return httpx.AsyncClient(
        headers=headers or {},
        timeout=timeout or httpx.Timeout(10.0, connect=5.0),
        auth=auth,
        verify=not SPLUNK_MCP_INSECURE,
    )


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
                # Splunk MCP wraps rows: {"results": [...], "truncated": bool, "total_rows": int}
                if "results" in data and isinstance(data["results"], list):
                    rows.extend(data["results"])
                else:
                    rows.append(data)
        except (json.JSONDecodeError, TypeError):
            pass
    return rows


class SplunkMCPClient:
    """Async MCP client for Splunk — Streamable HTTP transport.

    Use as an async context manager to hold one persistent ClientSession across
    multiple tool calls (e.g. all 5 investigation queries).  Falls back to a
    one-shot connection when methods are called without the context manager.

        async with SplunkMCPClient() as mcp:
            results = await mcp.search(...)   # reuses session
            more    = await mcp.search(...)   # same connection
    """

    def __init__(self):
        self._url = SPLUNK_MCP_URL
        self._headers = {"Authorization": f"Bearer {SPLUNK_MCP_TOKEN}"}
        self._session: ClientSession | None = None
        self._transport_cm = None
        self._session_cm = None

    # ── context manager ───────────────────────────────────────────────────────

    async def __aenter__(self) -> "SplunkMCPClient":
        self._transport_cm = streamablehttp_client(
            self._url,
            headers=self._headers,
            httpx_client_factory=_make_httpx_client,
        )
        read, write, _ = await self._transport_cm.__aenter__()
        self._session_cm = ClientSession(read, write)
        self._session = await self._session_cm.__aenter__()
        await self._session.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session_cm:
            await self._session_cm.__aexit__(exc_type, exc_val, exc_tb)
        if self._transport_cm:
            await self._transport_cm.__aexit__(exc_type, exc_val, exc_tb)
        self._session = None
        self._session_cm = None
        self._transport_cm = None

    # ── internal dispatcher ───────────────────────────────────────────────────

    async def _call_tool(self, name: str, args: dict):
        """Call an MCP tool.  Uses the persistent session when inside the
        context manager; otherwise opens a one-shot connection."""
        if self._session:
            return await self._session.call_tool(name, args)
        async with streamablehttp_client(
            self._url,
            headers=self._headers,
            httpx_client_factory=_make_httpx_client,
        ) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                return await session.call_tool(name, args)

    # ── public methods ────────────────────────────────────────────────────────

    async def list_tools(self) -> list[str]:
        """Print and return all tool names exposed by the Splunk MCP Server."""
        try:
            if self._session:
                result = await self._session.list_tools()
            else:
                async with streamablehttp_client(
                    self._url,
                    headers=self._headers,
                    httpx_client_factory=_make_httpx_client,
                ) as (read, write, _):
                    async with ClientSession(read, write) as session:
                        await session.initialize()
                        result = await session.list_tools()
            names = [t.name for t in result.tools]
            print(f"[THREAD:MCP] Available tools ({len(names)}): {', '.join(names)}")
            for t in result.tools:
                schema = getattr(t, "inputSchema", None) or getattr(t, "input_schema", None)
                props = (schema or {}).get("properties", {})
                param_names = list(props.keys())
                print(f"[THREAD:MCP]   {t.name}({', '.join(param_names)}): {t.description or ''}")
            return names
        except Exception as exc:
            cause = exc
            if hasattr(exc, "exceptions") and exc.exceptions:
                cause = exc.exceptions[0]
            cause = getattr(cause, "__cause__", cause) or cause
            print(f"[THREAD:MCP] list_tools failed: {type(cause).__name__}: {cause}")
            return []

    async def generate_spl(self, natural_language: str) -> str:
        """Call saia_generate_spl to turn plain English into SPL via Splunk AI Assistant.

        Returns a clean SPL string without a leading 'search' keyword,
        guaranteed to scope to SPLUNK_INDEX.  Returns empty string on failure.
        """
        t0 = _time.monotonic()
        spl = ""
        try:
            response = await self._call_tool(
                "saia_generate_spl",
                {
                    "prompt": (
                        f"{natural_language}. "
                        f"Only search index={SPLUNK_INDEX}. "
                        f"Use camelCase fields: correlationId, transactionId, sourceService, "
                        f"targetService, traceEvent, statusCode, durationMs, errorMessage, replayAttempt. "
                        f"traceEvent values: REQUEST_START, REQUEST_END, REQUEST_ERROR."
                    ),
                    "spl_only": True,
                },
            )
            # Try structured rows first
            rows = _parse_tool_result(response)
            if rows:
                r = rows[0]
                spl = r.get("spl") or r.get("query") or r.get("generated_spl") or r.get("result") or ""
                if not spl:
                    # Some versions return the SPL as a top-level string value
                    spl = next((v for v in r.values() if isinstance(v, str) and "index=" in v), "")
            # Fallback: raw text content
            if not spl:
                for item in response.content:
                    text = getattr(item, "text", None)
                    if text and text.strip():
                        spl = text.strip()
                        break
        except Exception as exc:
            cause = exc
            if hasattr(exc, "exceptions") and exc.exceptions:
                cause = exc.exceptions[0]
            cause = getattr(cause, "__cause__", cause) or cause
            print(f"[THREAD:MCP] saia_generate_spl failed: {type(cause).__name__}: {cause}")

        # Strip accidental leading "search" keyword — search() adds it back
        spl = spl.strip()
        if spl.lower().startswith("search "):
            spl = spl[7:].strip()

        # Ensure the query is scoped to our index (safety net)
        if spl and f"index={SPLUNK_INDEX}" not in spl:
            spl = f"index={SPLUNK_INDEX} {spl}"

        elapsed = (_time.monotonic() - t0) * 1000
        print(f"[THREAD:MCP] saia_generate_spl                                       → ({elapsed:.0f}ms): {spl[:120]}")
        return spl

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

        # Strip a leading "search" keyword — MCP adds it automatically via
        # the query prefix below, and saia_generate_spl sometimes returns it.
        spl_clean = spl.strip()
        if spl_clean.lower().startswith("search "):
            spl_clean = spl_clean[7:].strip()

        try:
            response = await self._call_tool(
                "splunk_run_query",
                {
                    "query":         f"search {spl_clean}",
                    "earliest_time": earliest,
                    "latest_time":   latest,
                    "row_limit":     max_results,
                },
            )
            results = _parse_tool_result(response)
        except Exception as exc:
            cause = exc
            if hasattr(exc, "exceptions") and exc.exceptions:
                cause = exc.exceptions[0]
            cause = getattr(cause, "__cause__", cause) or cause
            print(f"[THREAD:MCP] {_label} failed: {type(cause).__name__}: {cause}")

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
