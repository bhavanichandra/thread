"""
Splunk SPL query strings for the THREAD investigation pipeline.
Each function returns a ready-to-run SPL string.
Field names match the camelCase ThreadMessage contract.
"""


def _escape_spl(value: str) -> str:
    """Escape a value for safe interpolation inside a double-quoted SPL string."""
    return value.replace("\\", "\\\\").replace('"', '\\"')


def transaction_chain_query(correlation_id: str) -> str:
    """Full event chain for a single correlationId, ordered by time."""
    cid = _escape_spl(correlation_id)
    return (
        f'index=thread_logs correlationId="{cid}" '
        f'| sort by _time asc '
        f'| table _time, correlationId, sourceService, targetService, '
        f'traceEvent, statusCode, durationMs, errorMessage, replayAttempt'
    )


def failure_details_query(correlation_id: str) -> str:
    """REQUEST_ERROR events for a correlationId — one row per failure."""
    cid = _escape_spl(correlation_id)
    return (
        f'index=thread_logs correlationId="{cid}" traceEvent=REQUEST_ERROR '
        f'| table _time, sourceService, targetService, statusCode, errorMessage, durationMs'
    )


def service_health_query(service_name: str, window: str = "-15m") -> str:
    """Error rate and success ratio for a specific targetService.

    Uses targetService so results reflect errors hitting the service itself
    (each service logs events with sourceService=caller, targetService=self).
    """
    svc = _escape_spl(service_name)
    return (
        f'index=thread_logs earliest={window} '
        f'targetService="{svc}" '
        f'traceEvent IN (REQUEST_END, REQUEST_ERROR) '
        f'| eval success=if(traceEvent="REQUEST_END",1,0) '
        f'| stats sum(success) as ok, count as total by targetService '
        f'| eval error_rate=round((total-ok)/total*100,2) '
        f'| eval health_pct=round(ok/total*100,2) '
        f'| table targetService, ok, total, error_rate, health_pct'
    )


def system_errors_query(window: str = "-15m") -> str:
    """All REQUEST_ERROR events across all services, deduplicated by correlationId."""
    return (
        f'index=thread_logs earliest={window} traceEvent=REQUEST_ERROR '
        f'| dedup correlationId '
        f'| table _time, correlationId, sourceService, targetService, errorMessage, statusCode'
    )


def error_rate_timeseries_query(service_name: str, window: str = "-1h") -> str:
    """1-minute bucketed error rate timeseries for a service."""
    svc = _escape_spl(service_name)
    return (
        f'index=thread_logs earliest={window} '
        f'sourceService="{svc}" '
        f'traceEvent IN (REQUEST_END, REQUEST_ERROR) '
        f'| timechart span=1m '
        f'  count(eval(traceEvent="REQUEST_ERROR")) as errors, '
        f'  count as total '
        f'| eval error_rate=round(errors/total*100,2) '
        f'| fillnull value=0 error_rate errors total'
    )


# ── Saved search (Step 5 — replaces dashboard, opens in Splunk for judges) ────

TRANSACTION_CHAIN_SAVED_SEARCH = (
    'index=thread_logs correlationId="*" '
    '| sort by _time asc '
    '| table _time, correlationId, sourceService, targetService, '
    '  traceEvent, statusCode, durationMs, errorMessage, replayAttempt'
)
