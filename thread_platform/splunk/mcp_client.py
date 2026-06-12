"""
SplunkMCPClient — executes SPL queries against the Splunk REST API.

Every query is timed and printed with the [THREAD:MCP] prefix so judges can
see real MCP activity in the terminal during the demo recording.

Connection: HTTPS to Splunk REST API on port 8089.
Auth:       username/password (Basic) — token auth not available on all tiers.
"""

import os
import time as _time
import logging
import httpx

from .queries import (
    transaction_chain_query,
    failure_details_query,
    service_health_query,
    system_errors_query,
    error_rate_timeseries_query,
)

logger = logging.getLogger("thread-platform")

SPLUNK_HOST     = os.getenv("SPLUNK_HOST", "localhost")
SPLUNK_PORT     = int(os.getenv("SPLUNK_PORT", "8089"))
SPLUNK_USERNAME = os.getenv("SPLUNK_USERNAME", "admin")
SPLUNK_PASSWORD = os.getenv("SPLUNK_PASSWORD", "")
SPLUNK_INDEX    = os.getenv("SPLUNK_INDEX", "thread_logs")


class SplunkMCPClient:
    """Thin async client over the Splunk REST search/jobs API."""

    def __init__(self):
        self._base = f"https://{SPLUNK_HOST}:{SPLUNK_PORT}"
        self._auth = (SPLUNK_USERNAME, SPLUNK_PASSWORD)

    # ── core ──────────────────────────────────────────────────────────────────

    async def search(
        self,
        spl: str,
        earliest: str = "-15m",
        latest: str = "now",
        max_results: int = 100,
        _label: str = "search",
    ) -> list[dict]:
        """Run a blocking SPL search and return rows as a list of dicts.

        Prints [THREAD:MCP] timing so judges see every query fire.
        """
        t0 = _time.monotonic()
        results: list[dict] = []

        try:
            async with httpx.AsyncClient(
                verify=False,          # self-signed cert in Docker
                timeout=30.0,
            ) as client:
                # 1. Submit the search job
                resp = await client.post(
                    f"{self._base}/services/search/jobs",
                    auth=self._auth,
                    data={
                        "search":       f"search {spl}",
                        "earliest_time": earliest,
                        "latest_time":   latest,
                        "output_mode":   "json",
                        "exec_mode":     "blocking",   # wait for results inline
                    },
                )
                resp.raise_for_status()
                sid = resp.json()["sid"]

                # 2. Fetch results
                res = await client.get(
                    f"{self._base}/services/search/jobs/{sid}/results",
                    auth=self._auth,
                    params={"output_mode": "json", "count": max_results},
                )
                res.raise_for_status()
                results = res.json().get("results", [])

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
                "ok":          int(r.get("ok", 0)),
                "total":       int(r.get("total", 0)),
                "error_rate":  float(r.get("error_rate", 0.0)),
                "health_pct":  float(r.get("health_pct", 100.0)),
            }
        # No results — service not seen in logs (treat as unknown, not healthy)
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
