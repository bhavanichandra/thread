from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel


class TraceEvent(str, Enum):
    REQUEST_START = "REQUEST_START"
    REQUEST_END   = "REQUEST_END"
    REQUEST_ERROR = "REQUEST_ERROR"


class ThreadMessage(BaseModel):
    """
    The THREAD contract — 5 mandatory fields for every trace event.
    Intentionally duplicated from demo_services/thread_publisher.py;
    these run in separate processes and must not share an import path.
    """
    correlationId:  str
    transactionId:  str
    sourceService:  str
    targetService:  str
    traceEvent:     TraceEvent
    timestamp:      datetime
    method:         Optional[str]   = None
    url:            Optional[str]   = None
    body:           Optional[dict]  = None
    statusCode:     Optional[int]   = None
    durationMs:     Optional[float] = None
    errorMessage:   Optional[str]   = None

    class Config:
        use_enum_values = True
