import os
import uuid
import time
import asyncio
import logging
import traceback
from fastapi import FastAPI, Request, BackgroundTasks
from pydantic import BaseModel
from thread_publisher import publish_thread_event

# Thread Logger custom filter to prevent KeyErrors on default logs
class ThreadLogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, "correlationId"):
            record.correlationId = "-"
        if not hasattr(record, "transactionId"):
            record.transactionId = "-"
        if not hasattr(record, "sourceService"):
            record.sourceService = "-"
        if not hasattr(record, "targetService"):
            record.targetService = "-"
        if not hasattr(record, "traceEvent"):
            record.traceEvent = "-"
        return True

# Configure logging to match the 5-field THREAD contract requirement
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    '%(asctime)s correlationId=%(correlationId)s '
    'transactionId=%(transactionId)s '
    'sourceService=%(sourceService)s '
    'targetService=%(targetService)s '
    'traceEvent=%(traceEvent)s %(message)s'
))
handler.addFilter(ThreadLogFilter())
logging.getLogger().handlers = [handler]
logging.getLogger().setLevel(logging.INFO)

app = FastAPI(title="Inventory Service")
SERVICE_NAME = "inventory-service"

class ReserveRequest(BaseModel):
    order_id: str
    items: list[dict]

@app.get("/health")
async def health():
    return {"status": "ok", "service": SERVICE_NAME}

@app.post("/api/v1/reserve")
async def reserve_inventory(reserve: ReserveRequest, request: Request, background_tasks: BackgroundTasks):
    correlation_id = request.headers.get("x-correlation-id", str(uuid.uuid4()))
    transaction_id = request.headers.get("x-transaction-id", str(uuid.uuid4()))
    start_time = time.monotonic()

    # Log + publish REQUEST_START
    logging.info("Inventory reservation started",
        extra={
            "correlationId": correlation_id,
            "transactionId": transaction_id,
            "sourceService": "payment-service",
            "targetService": SERVICE_NAME,
            "traceEvent": "REQUEST_START"
        }
    )

    # Use BackgroundTasks so FastAPI awaits completion before shutdown
    background_tasks.add_task(
        publish_thread_event,
        correlation_id=correlation_id,
        transaction_id=transaction_id,
        source_service="payment-service",
        target_service=SERVICE_NAME,
        trace_event="REQUEST_START",
        method="POST",
        url=str(request.url),
        body=reserve.model_dump(),
    )

    try:
        # Simulate some inventory check work
        await asyncio.sleep(0.05)

        duration_ms = (time.monotonic() - start_time) * 1000

        # Log + publish REQUEST_END
        logging.info("Inventory reservation completed",
            extra={
                "correlationId": correlation_id,
                "transactionId": transaction_id,
                "sourceService": "payment-service",
                "targetService": SERVICE_NAME,
                "traceEvent": "REQUEST_END"
            }
        )

        background_tasks.add_task(
            publish_thread_event,
            correlation_id=correlation_id,
            transaction_id=transaction_id,
            source_service="payment-service",
            target_service=SERVICE_NAME,
            trace_event="REQUEST_END",
            status_code=200,
            duration_ms=duration_ms,
        )

        return {
            "reservation_id": f"res_{reserve.order_id}",
            "status": "reserved",
            "order_id": reserve.order_id,
        }

    except Exception as exc:
        duration_ms = (time.monotonic() - start_time) * 1000
        error_msg = f"{type(exc).__name__}: {exc}"

        logging.error("Inventory reservation failed",
            extra={
                "correlationId": correlation_id,
                "transactionId": transaction_id,
                "sourceService": "payment-service",
                "targetService": SERVICE_NAME,
                "traceEvent": "REQUEST_ERROR"
            },
            exc_info=True,
        )

        background_tasks.add_task(
            publish_thread_event,
            correlation_id=correlation_id,
            transaction_id=transaction_id,
            source_service="payment-service",
            target_service=SERVICE_NAME,
            trace_event="REQUEST_ERROR",
            status_code=500,
            duration_ms=duration_ms,
            error_message=error_msg,
        )
        raise
