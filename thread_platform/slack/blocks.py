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


def _anomaly_bar(score: float) -> str:
    """Visual 5-block bar representing anomaly score 0-1."""
    filled = round(score * 5)
    return "█" * filled + "░" * (5 - filled)


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
                    "text": f"*Correlation ID*\n`{short_id}...`",
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
                    "text": f"*Error Rate*\n`{result.error_rate * 100:.1f}%`",
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
                    "text": f"*Error:* {result.error_message}",
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
                        f"*🔍 Splunk MCP Investigation*\n"
                        f"{result.mcp_trace}"
                    ),
                },
            }
        )
        blocks.append({"type": "divider"})

    # ── AI verdict ────────────────────────────────────────────────────────────
    blocks.append(
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*🤖 AI Verdict*\n"
                    f"Anomaly score: `{result.anomaly_score:.2f}` {_anomaly_bar(result.anomaly_score)}\n"
                    f"Forecast: {trend_emoji} *{result.forecast_trend.value}*\n"
                    f"Replay recommendation: *replay L={result.recommended_limit}*\n"
                    f"System-wide failures: *{result.total_system_errors}* transactions affected"
                ),
            },
        }
    )

    # ── Actions ───────────────────────────────────────────────────────────────
    actions = [
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "🔎 View in Splunk", "emoji": True},
            "url": splunk_url,
            "style": "primary",
        },
    ]

    # Add dashboard button if available
    if result.dashboard_url:
        actions.insert(
            1,
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "📊 AI Dashboard", "emoji": True},
                "url": result.dashboard_url,
                "style": "primary",
            },
        )

    actions.append(
        {
            "type": "button",
            "text": {"type": "plain_text", "text": f"🔁 Replay (L={result.recommended_limit})", "emoji": True},
            "action_id": "trigger_replay",
            "value": f"{result.correlation_id}:{result.recommended_limit}",
            "style": "danger",
        }
    )

    blocks.append({"type": "actions", "elements": actions})

    return blocks
