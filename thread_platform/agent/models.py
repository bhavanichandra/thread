"""
Data models for the THREAD investigation agent.
"""
from __future__ import annotations

from enum import Enum
from typing import Optional
from pydantic import BaseModel


class ForecastTrend(str, Enum):
    RECOVERING  = "RECOVERING"
    STABLE      = "STABLE"
    DEGRADING   = "DEGRADING"
    UNKNOWN     = "UNKNOWN"


class InvestigationResult(BaseModel):
    """Structured output of one MCP-driven investigation run."""

    # Input
    correlation_id:     str

    # What went wrong
    failed_service:     str
    http_status:        Optional[int]   = None
    error_message:      Optional[str]   = None

    # Health metrics
    error_rate:         float           = 0.0   # 0-1 fraction
    total_system_errors: int            = 0     # unique failing correlationIds in window

    # AI-computed values
    anomaly_score:      float           = 0.0   # 0-1; higher = more unusual
    forecast_trend:     ForecastTrend   = ForecastTrend.UNKNOWN
    recommended_limit:  int             = 3     # replay L parameter

    # Chain summary
    chain_length:       int             = 0     # total events in transaction chain

    # Human-readable MCP trace for Slack (populated by investigator)
    mcp_trace:          Optional[str]   = None

    # AI-generated dashboard
    dashboard_url:      Optional[str]   = None
