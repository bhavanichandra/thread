"""
Data models for the THREAD investigation agent.
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel


class FailureClass(str, Enum):
    TRANSIENT       = "TRANSIENT"
    SYSTEMIC_OUTAGE = "SYSTEMIC_OUTAGE"
    BAD_REQUEST     = "BAD_REQUEST"
    AUTH_FAILURE    = "AUTH_FAILURE"
    UNKNOWN         = "UNKNOWN"


class ForecastTrend(str, Enum):
    RECOVERING = "RECOVERING"
    STABLE     = "STABLE"
    DEGRADING  = "DEGRADING"
    UNKNOWN    = "UNKNOWN"


class InvestigationResult(BaseModel):
    # Input
    correlation_id:             str
    investigated_at:            datetime

    # Transaction chain
    services_involved:          list[str]
    total_hops:                 int
    failed_service:             str
    failure_point:              str

    # Failure details
    error_type:                 str
    error_message:              str
    http_status:                int
    failure_class:              FailureClass

    # Health context (from MCP queries)
    failed_service_error_rate:  float           # 0-1 fraction
    system_error_rate:          float           # 0-1 fraction
    affected_transactions_15m:  int
    prior_attempts:             int

    # Anomaly / forecast — Cisco DTMS (cloud) or heuristic fallback (local)
    anomaly_score:              float
    forecast_trend:             ForecastTrend
    forecast_error_rate_15m:    Optional[float] = None
    ai_source:                  str             = "heuristic"  # "cisco_dtms" | "heuristic"

    # Replay decision
    recommended_limit:          int
    replay_safe:                bool

    # Human-readable MCP trace for Slack
    mcp_trace:                  Optional[str]   = None

    # AI-generated dashboard URL (BHA-28)
    dashboard_url:              Optional[str]   = None

    # Optional LLM summary (BHA-40)
    llm_summary:                Optional[str]   = None
