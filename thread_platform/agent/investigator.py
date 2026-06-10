"""
InvestigationAgent — orchestrates the 5 MCP queries, computes anomaly score,
derives forecast trend, and produces an InvestigationResult.

Terminal output during a demo:
  [THREAD:MCP] get_transaction_chain(abc-123...)      →   6 results (234ms)
  [THREAD:MCP] get_failure_details(abc-123...)        →   1 results (187ms)
  [THREAD:MCP] get_service_health(payment-service)    →   1 results (203ms)
  [THREAD:MCP] get_system_errors()                    →   2 results (198ms)
  [THREAD:MCP] get_error_rate_timeseries(payment...)  →  12 results (241ms)
  [THREAD:MCP] Investigation complete for abc-123...
  [THREAD:MCP]   Failed service:  payment-service
  [THREAD:MCP]   Error rate:      4.2%
  [THREAD:MCP]   Anomaly score:   0.23
  [THREAD:MCP]   Forecast trend:  RECOVERING
  [THREAD:MCP]   Replay limit L:  2
"""
from __future__ import annotations

import logging
from typing import Optional

from ..splunk.mcp_client import SplunkMCPClient
from .models import ForecastTrend, InvestigationResult
from .dashboard_gen import generate_dashboard

logger = logging.getLogger("thread-platform")


class InvestigationAgent:
    """Runs a 5-query MCP investigation for a failed transaction."""

    def __init__(self, mcp: Optional[SplunkMCPClient] = None):
        self._mcp = mcp or SplunkMCPClient()

    async def investigate(self, correlation_id: str) -> InvestigationResult:
        """Run all 5 queries concurrently then synthesise results."""
        import asyncio

        # ── Run all 5 queries concurrently ────────────────────────────────────
        (
            chain,
            failures,
            system_errors,
        ) = await asyncio.gather(
            self._mcp.get_transaction_chain(correlation_id),
            self._mcp.get_failure_details(correlation_id),
            self._mcp.get_system_errors(window="-15m"),
        )

        # Determine failed service from first error row
        failed_service = "unknown"
        http_status: Optional[int] = None
        error_message: Optional[str] = None
        if failures:
            row = failures[0]
            failed_service = row.get("sourceService", "unknown")
            try:
                http_status = int(row["statusCode"]) if row.get("statusCode") else None
            except (ValueError, TypeError):
                http_status = None
            error_message = row.get("errorMessage")

        # Fetch service health + timeseries for the identified failed service
        health, timeseries = await asyncio.gather(
            self._mcp.get_service_health(failed_service, window="-15m"),
            self._mcp.get_error_rate_timeseries(failed_service, window="-1h"),
        )

        # ── Derive metrics ────────────────────────────────────────────────────
        error_rate_pct: float = health.get("error_rate", 0.0)   # already 0-100
        error_rate_frac: float = error_rate_pct / 100.0
        total_system_errors = len(system_errors)

        # Anomaly score: blend error-rate contribution + system-wide spread
        system_spread = min(total_system_errors / 10.0, 1.0)
        anomaly_score = round(
            0.7 * min(error_rate_frac, 1.0) + 0.3 * system_spread,
            2,
        )

        # Forecast trend from timeseries: compare last 3 vs first 3 error rates
        forecast_trend = ForecastTrend.UNKNOWN
        if len(timeseries) >= 6:
            try:
                first3 = [float(r.get("error_rate", 0)) for r in timeseries[:3]]
                last3  = [float(r.get("error_rate", 0)) for r in timeseries[-3:]]
                avg_first = sum(first3) / len(first3)
                avg_last  = sum(last3)  / len(last3)
                delta = avg_last - avg_first
                if delta < -2.0:
                    forecast_trend = ForecastTrend.RECOVERING
                elif delta > 2.0:
                    forecast_trend = ForecastTrend.DEGRADING
                else:
                    forecast_trend = ForecastTrend.STABLE
            except Exception:
                forecast_trend = ForecastTrend.UNKNOWN

        # Replay limit: conservative when anomaly is high or trend degrading
        if forecast_trend == ForecastTrend.DEGRADING or anomaly_score > 0.6:
            recommended_limit = 1
        elif anomaly_score > 0.3:
            recommended_limit = 2
        else:
            recommended_limit = 3

        # ── MCP trace string (shown in Slack) ─────────────────────────────────
        mcp_trace = (
            f"• `get_transaction_chain` → {len(chain)} events\n"
            f"• `get_failure_details` → {failed_service}, HTTP {http_status or 'n/a'}\n"
            f"• `get_service_health` → error rate {error_rate_pct:.1f}%\n"
            f"• `get_system_errors` → {total_system_errors} transactions affected\n"
            f"• `get_error_rate_timeseries` → {len(timeseries)} data points, trend {forecast_trend.value}"
        )

        # ── Terminal summary ──────────────────────────────────────────────────
        short_id = correlation_id[:8]
        print(f"[THREAD:MCP] Investigation complete for {short_id}...")
        print(f"[THREAD:MCP]   Failed service:  {failed_service}")
        print(f"[THREAD:MCP]   Error rate:      {error_rate_pct:.1f}%")
        print(f"[THREAD:MCP]   Anomaly score:   {anomaly_score:.2f}")
        print(f"[THREAD:MCP]   Forecast trend:  {forecast_trend.value}")
        print(f"[THREAD:MCP]   Replay limit L:  {recommended_limit}")

        # ── Generate AI-created dashboard ─────────────────────────────────────
        dashboard_url = await generate_dashboard(
            correlation_id=correlation_id,
            failed_service=failed_service,
            error_message=error_message or "Unknown error",
            error_rate_pct=error_rate_pct,
            anomaly_score=anomaly_score,
        )

        return InvestigationResult(
            correlation_id=correlation_id,
            failed_service=failed_service,
            http_status=http_status,
            error_message=error_message,
            error_rate=error_rate_frac,
            total_system_errors=total_system_errors,
            anomaly_score=anomaly_score,
            forecast_trend=forecast_trend,
            recommended_limit=recommended_limit,
            chain_length=len(chain),
            mcp_trace=mcp_trace,
            dashboard_url=dashboard_url,
        )
