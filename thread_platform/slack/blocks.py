"""
Slack Block Kit message builder for THREAD failure alerts.

Produces a rich message that shows:
  1. Failure summary (service, error, status code)
  2. MCP investigation trace (5 Splunk queries + results)
  3. AI verdict (anomaly score, forecast trend, replay recommendation)
"""
from __future__ import annotations

import os
from typing import Optional

from ..agent.models import ForecastTrend, InvestigationResult

SPLUNK_BASE_URL = os.getenv("SPLUNK_BASE_URL", "http://localhost:8000")
SPLUNK_INDEX    = os.getenv("SPLUNK_INDEX", "thread_logs")


def _escape_mrkdwn(text: str) -> str:
    """Escape Slack mrkdwn special characters so they render as literals."""
    for ch in ("*", "_", "`", "~", ">", "|"):
        text = text.replace(ch, f"\\{ch}")
    return text


def _splunk_link(correlation_id: str) -> str:
    """Deep-link to the THREAD Transaction Chain saved search filtered by correlationId."""
    query = (
        f'index={SPLUNK_INDEX} correlationId="{correlation_id}" '
        f'| sort by _time asc '
        f'| table _time, correlationId, sourceService, targetService, '
        f'traceEvent, statusCode, durationMs, errorMessage, replayAttempt'
    )
    import urllib.parse
    encoded = urllib.parse.quote(query)
    return f"{SPLUNK_BASE_URL}/en-US/app/search/search?q={encoded}&earliest=-24h&latest=now"


def _trend_emoji(trend: ForecastTrend) -> str:
    return {
        ForecastTrend.RECOVERING: "📈",
        ForecastTrend.STABLE:     "➡️",
        ForecastTrend.DEGRADING:  "📉",
        ForecastTrend.UNKNOWN:    "❓",
    }.get(trend, "❓")



def build_failure_alert_blocks(result: InvestigationResult) -> list[dict]:
    """Return Slack Block Kit blocks for a THREAD failure alert."""

    short_id = result.correlation_id[:8]
    trend_emoji = _trend_emoji(result.forecast_trend)
    splunk_url  = _splunk_link(result.correlation_id)

    # ── Header ────────────────────────────────────────────────────────────────
    blocks: list[dict] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"🔴 THREAD — Transaction Failure Detected",
                "emoji": True,
            },
        },
        {"type": "divider"},
        # ── Failure summary ───────────────────────────────────────────────────
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Correlation ID*\n`{result.correlation_id}`",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Failed Service*\n`{result.failed_service}`",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*HTTP Status*\n`{result.http_status or 'n/a'}`",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Error Rate*\n`{result.failed_service_error_rate * 100:.1f}%`",
                },
            ],
        },
    ]

    # Error message (if present)
    if result.error_message:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Error:* {_escape_mrkdwn(result.error_message)}",
                },
            }
        )

    blocks.append({"type": "divider"})

    # ── MCP investigation trace ───────────────────────────────────────────────
    if result.mcp_trace:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*Splunk MCP Investigation*\n"
                        f"{result.mcp_trace}"
                    ),
                },
            }
        )
        blocks.append({"type": "divider"})

    # ── AI verdict ────────────────────────────────────────────────────────────
    ai_label = (
        "Cisco Deep Time Series" if result.ai_source == "cisco_dtms"
        else "Heuristic (local dev)"
    )
    blocks.append(
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*AI Verdict — {ai_label}*\n"
                    f"Anomaly score: `{result.anomaly_score:.2f}`\n"
                    f"Forecast: {trend_emoji} *{result.forecast_trend.value}*\n"
                    f"Replay: *{'safe — L=' + str(result.recommended_limit) if result.recommended_limit > 0 else 'not recommended'}*\n"
                    f"System-wide failures: *{result.affected_transactions_15m}* transactions affected"
                ),
            },
        }
    )

    # ── Actions ───────────────────────────────────────────────────────────────
    actions = [
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "🔎 Transaction Chain", "emoji": True},
            "url": splunk_url,
            "style": "primary",
        },
    ]

    # Add AI-generated search button if available
    if result.dashboard_url:
        actions.insert(
            1,
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "🤖 AI Analysis", "emoji": True},
                "url": result.dashboard_url,
            },
        )

    if result.recommended_limit > 0:
        actions.append({
            "type": "button",
            "text": {"type": "plain_text", "text": f"🔁 Replay (L={result.recommended_limit})", "emoji": True},
            "action_id": "trigger_replay",
            "value": f"{result.correlation_id}:{result.recommended_limit}",
            "style": "danger",
        })
    else:
        actions.append({
            "type": "button",
            "text": {"type": "plain_text", "text": "↩ Replay anyway", "emoji": True},
            "action_id": "trigger_replay",
            "value": f"{result.correlation_id}:0",
        })

    blocks.append({"type": "actions", "elements": actions})

    return blocks
