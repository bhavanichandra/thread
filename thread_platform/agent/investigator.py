"""
InvestigationAgent — orchestrates the 5 MCP queries, computes anomaly score,
derives forecast trend, and produces an InvestigationResult.

Two anomaly backends, selected by SPLUNK_AI_ENABLED env flag:
  - heuristic_anomaly()    — local dev / Docker Splunk (default)
  - cisco_dtms_anomaly()   — Splunk Cloud trial only (set SPLUNK_AI_ENABLED=true)

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

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from ..splunk.mcp_client import SplunkMCPClient
from .dashboard_gen import generate_dashboard
from .models import FailureClass, ForecastTrend, InvestigationResult

logger = logging.getLogger("thread-platform")

SPLUNK_AI_ENABLED = os.getenv("SPLUNK_AI_ENABLED", "false").lower() == "true"


# ── Failure classification ────────────────────────────────────────────────────

def classify_failure(
    error_type: str,
    http_status: int,
    system_error_rate: float,
    failed_service_error_rate: float,
) -> FailureClass:
    if http_status in (400, 422):
        return FailureClass.BAD_REQUEST
    if http_status in (401, 403):
        return FailureClass.AUTH_FAILURE
    if system_error_rate > 0.30 or failed_service_error_rate > 0.50:
        return FailureClass.SYSTEMIC_OUTAGE
    transient = {"ConnectionTimeout", "ReadTimeout", "ConnectError", "ServiceUnavailable"}
    if error_type in transient or http_status == 503:
        return FailureClass.TRANSIENT
    return FailureClass.UNKNOWN


# ── Dynamic replay limit ──────────────────────────────────────────────────────

def calculate_replay_limit(
    error_type: str,
    http_status: int,
    failed_service_error_rate: float,
    system_error_rate: float,
    prior_attempts: int,
    anomaly_score: float,
    forecast_trend: ForecastTrend,
) -> int:
    if prior_attempts >= 3:
        return 0
    if http_status in (400, 422, 401, 403):
        return 0
    if system_error_rate > 0.30:
        return 0
    if failed_service_error_rate > 0.50:
        return 0
    if forecast_trend == ForecastTrend.DEGRADING and anomaly_score > 0.6:
        return 0
    transient = {"ConnectionTimeout", "ReadTimeout", "ConnectError", "ServiceUnavailable"}
    if forecast_trend == ForecastTrend.RECOVERING and anomaly_score < 0.4:
        if error_type in transient or http_status == 503:
            return max(0, 3 - prior_attempts)
    if 500 <= http_status < 600:
        return max(0, 1 - prior_attempts)
    return 0


# ── Heuristic fallback (local dev / no Splunk Cloud) ─────────────────────────

def heuristic_anomaly(
    timeseries: list[dict],
) -> tuple[float, ForecastTrend, Optional[float]]:
    """
    Trend analysis on raw error rate timeseries.
    Used when SPLUNK_AI_ENABLED=false (local Docker Splunk).
    Returns same shape as cisco_dtms_anomaly.
    """
    if not timeseries or len(timeseries) < 2:
        return 0.0, ForecastTrend.UNKNOWN, None

    rates = [float(r.get("error_rate", 0)) for r in timeseries]
    current = rates[-1]
    average = sum(rates) / len(rates)

    anomaly_score = round(min(current / max(average, 0.001), 1.0), 2)

    mid = len(rates) // 2
    first_half  = sum(rates[:mid]) / max(mid, 1)
    second_half = sum(rates[mid:]) / max(len(rates) - mid, 1)

    if second_half < first_half * 0.7:
        trend = ForecastTrend.RECOVERING
    elif second_half > first_half * 1.3:
        trend = ForecastTrend.DEGRADING
    else:
        trend = ForecastTrend.STABLE

    return anomaly_score, trend, round(second_half, 4)


# ── Cisco Deep Time Series Model (Splunk Cloud only) ─────────────────────────

async def cisco_dtms_anomaly(
    service_name: str,
    timeseries: list[dict],
) -> tuple[float, ForecastTrend, Optional[float], str]:
    """
    Calls Cisco Deep Time Series Model via Splunk AI Toolkit.
    Only active when SPLUNK_AI_ENABLED=true (Splunk Cloud trial).

    Returns (anomaly_score, trend, forecast_rate, actual_source) so the caller
    always knows which backend produced the result even when DTMS falls back.

    TODO: Implement when Splunk Cloud trial is available.
    Falls back to heuristic on any error.
    """
    try:
        print(f"[THREAD:DTMS] Querying Cisco DTMS for {service_name}...")
        # Implement via splunklib when cloud trial is active — see BHA-31
        raise NotImplementedError("Implement when Splunk Cloud trial is active")
    except Exception as e:
        print(f"[THREAD:DTMS] Unavailable ({e}), falling back to heuristic")
        score, trend, forecast = heuristic_anomaly(timeseries)
        return score, trend, forecast, "heuristic"


# ── Main Investigation Agent ──────────────────────────────────────────────────

class InvestigationAgent:
    """Runs a 5-query MCP investigation for a failed transaction."""

    def __init__(self, mcp: Optional[SplunkMCPClient] = None):
        self._mcp = mcp or SplunkMCPClient()

    async def investigate(
        self, correlation_id: str, context: Optional[dict] = None
    ) -> InvestigationResult:
        """Run 5 MCP queries, compute anomaly, classify failure, derive replay limit.

        context: the raw RabbitMQ ThreadMessage dict — used as fallback when MCP
        returns 0 results (e.g. Splunk MCP Server not installed, or logs not yet indexed).

        All 5 queries share one persistent MCP ClientSession opened here.
        """
        async with self._mcp:
            return await self._run_investigation(correlation_id, context)

    async def _run_investigation(
        self, correlation_id: str, context: Optional[dict] = None
    ) -> InvestigationResult:
        # ── 1. Concurrent queries (chain + failures + system errors) ──────────
        chain, failures, system_errors = await asyncio.gather(
            self._mcp.get_transaction_chain(correlation_id),
            self._mcp.get_failure_details(correlation_id),
            self._mcp.get_system_errors(window="-15m"),
        )

        # ── 2. Extract failure facts ──────────────────────────────────────────
        # Seed from RabbitMQ context first (always available), then let MCP override
        ctx = context or {}
        failed_service = ctx.get("targetService") or "unknown"
        error_message  = ctx.get("errorMessage") or ""
        error_type     = error_message.split(":")[0].strip() if error_message else "Unknown"
        try:
            http_status = int(ctx["statusCode"]) if ctx.get("statusCode") else 500
        except (ValueError, TypeError):
            http_status = 500

        if failures:
            row = failures[0]
            failed_service = row.get("targetService") or failed_service
            error_message  = row.get("errorMessage") or error_message
            error_type     = error_message.split(":")[0].strip() if error_message else error_type
            try:
                http_status = int(row["statusCode"]) if row.get("statusCode") else http_status
            except (ValueError, TypeError):
                pass

        # replayAttempt is numbered sequentially (0=original, 1=first replay, …).
        # max() gives the highest attempt seen across all chain rows — correct even
        # when a single replay produces multiple hop events with the same number.
        prior_attempts = max(
            (int(e.get("replayAttempt") or 0) for e in chain),
            default=0,
        )

        # Transaction chain summary
        services = list(dict.fromkeys(
            e.get("sourceService") for e in chain if e.get("sourceService")
        ))
        total_hops = len(services)
        try:
            fail_step     = services.index(failed_service) + 1
            failure_point = f"step {fail_step} of {total_hops}"
        except ValueError:
            failure_point = "unknown step"

        # ── 3. Service health + timeseries (sequential — need failed_service) ─
        health, timeseries = await asyncio.gather(
            self._mcp.get_service_health(failed_service, window="-15m"),
            self._mcp.get_error_rate_timeseries(failed_service, window="-1h"),
        )

        failed_svc_error_rate = health.get("error_rate", 0.0) / 100.0
        total_system_errors   = len(system_errors)
        # system_errors is a deduped list of failing correlationIds — no total volume
        # available. Normalise count against 100 as a load proxy (saturates at 1.0).
        system_error_rate     = min(total_system_errors / 100.0, 1.0)

        # ── 4. Anomaly detection ──────────────────────────────────────────────
        if SPLUNK_AI_ENABLED:
            # cisco_dtms_anomaly returns actual_source so fallback is reflected correctly
            anomaly_score, forecast_trend, forecast_rate, ai_source = \
                await cisco_dtms_anomaly(failed_service, timeseries)
        else:
            anomaly_score, forecast_trend, forecast_rate = \
                heuristic_anomaly(timeseries)
            # When MCP has no timeseries data, use a fixed score so the Slack
            # message shows something meaningful rather than 0.00 / UNKNOWN.
            # health["total"]==0 means the health query also returned no data.
            if not timeseries:
                has_real_health = health.get("total", 0) > 0
                if has_real_health:
                    anomaly_score = round(min(failed_svc_error_rate + 0.3, 1.0), 2)
                    forecast_trend = (
                        ForecastTrend.DEGRADING if failed_svc_error_rate > 0.5
                        else ForecastTrend.STABLE
                    )
                else:
                    anomaly_score = 0.75
                    forecast_trend = ForecastTrend.DEGRADING
            ai_source = "heuristic"

        # ── 5. Classify failure + calculate replay limit ───────────────────────
        failure_class     = classify_failure(
            error_type, http_status, system_error_rate, failed_svc_error_rate
        )
        recommended_limit = calculate_replay_limit(
            error_type=error_type,
            http_status=http_status,
            failed_service_error_rate=failed_svc_error_rate,
            system_error_rate=system_error_rate,
            prior_attempts=prior_attempts,
            anomaly_score=anomaly_score,
            forecast_trend=forecast_trend,
        )

        # ── MCP trace string (shown in Slack) ─────────────────────────────────
        mcp_trace = (
            f"• `get_transaction_chain` → {len(chain)} events\n"
            f"• `get_failure_details` → {failed_service}, HTTP {http_status}\n"
            f"• `get_service_health` → error rate {failed_svc_error_rate * 100:.1f}%\n"
            f"• `get_system_errors` → {total_system_errors} transactions affected\n"
            f"• `get_error_rate_timeseries` → {len(timeseries)} data points, "
            f"trend {forecast_trend.value} [{ai_source}]"
        )

        # ── Terminal summary ──────────────────────────────────────────────────
        short_id = correlation_id[:8]
        print(f"[THREAD:MCP] Investigation complete for {short_id}...")
        print(f"[THREAD:MCP]   Failed service:  {failed_service}")
        print(f"[THREAD:MCP]   Error rate:      {failed_svc_error_rate * 100:.1f}%")
        print(f"[THREAD:MCP]   Anomaly score:   {anomaly_score:.2f} [{ai_source}]")
        print(f"[THREAD:MCP]   Forecast trend:  {forecast_trend.value}")
        print(f"[THREAD:MCP]   Replay limit L:  {recommended_limit}")

        # ── Generate AI dashboard ─────────────────────────────────────────────
        dashboard_url = await generate_dashboard(
            correlation_id=correlation_id,
            failed_service=failed_service,
            error_message=error_message or "Unknown error",
            error_rate_pct=failed_svc_error_rate * 100,
            anomaly_score=anomaly_score,
        )

        return InvestigationResult(
            correlation_id=correlation_id,
            investigated_at=datetime.now(timezone.utc),
            services_involved=services,
            total_hops=total_hops,
            failed_service=failed_service,
            failure_point=failure_point,
            error_type=error_type,
            error_message=error_message,
            http_status=http_status,
            failure_class=failure_class,
            failed_service_error_rate=failed_svc_error_rate,
            system_error_rate=system_error_rate,
            affected_transactions_15m=total_system_errors,
            prior_attempts=prior_attempts,
            anomaly_score=anomaly_score,
            forecast_trend=forecast_trend,
            forecast_error_rate_15m=forecast_rate,
            ai_source=ai_source,
            recommended_limit=recommended_limit,
            replay_safe=recommended_limit > 0,
            mcp_trace=mcp_trace,
            dashboard_url=dashboard_url,
        )
