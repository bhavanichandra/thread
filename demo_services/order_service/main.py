import os
import uuid
import time
import asyncio
import logging
import httpx
from fastapi import FastAPI, HTTPException, Request
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

app = FastAPI(title="Order Service")
SERVICE_NAME = "order-service"
PAYMENT_URL = os.getenv("PAYMENT_SERVICE_URL", "http://localhost:8002")

class OrderRequest(BaseModel):
    customer_id: str
    items: list[dict]
    total: float

class OrderResponse(BaseModel):
    order_id: str
    status: str
    message: str
    correlation_id: str

@app.get("/health")
async def health():
    return {"status": "ok", "service": SERVICE_NAME}

@app.post("/api/v1/orders", response_model=OrderResponse)
async def create_order(order: OrderRequest, request: Request):
    # Extract or generate correlation ID
    correlation_id = request.headers.get("x-correlation-id", str(uuid.uuid4()))
    transaction_id = str(uuid.uuid4())
    start_time = time.monotonic()

    # Log + publish REQUEST_START
    logging.info("Order request received",
        extra={
            "correlationId": correlation_id,
            "transactionId": transaction_id,
            "sourceService": "client",
            "targetService": SERVICE_NAME,
            "traceEvent": "REQUEST_START"
        }
    )

    asyncio.create_task(publish_thread_event(
        correlation_id=correlation_id,
        transaction_id=transaction_id,
        source_service="client",
        target_service=SERVICE_NAME,
        trace_event="REQUEST_START",
        method="POST",
        url=str(request.url),
        body=order.model_dump(),
    ))

    order_id = str(uuid.uuid4())[:8]

    # Call payment service — forward correlation ID and transaction ID
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.post(
                f"{PAYMENT_URL}/api/v1/payments",
                json={
                    "order_id": order_id,
                    "amount": order.total,
                    "customer_id": order.customer_id,
                    "items": order.items,
                },
                headers={
                    "x-correlation-id": correlation_id,
                    "x-transaction-id": transaction_id
                }
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            duration_ms = (time.monotonic() - start_time) * 1000
            error_msg = f"Payment failed: {e.response.text}"
            
            logging.error(f"Payment failed with status {e.response.status_code}",
                extra={
                    "correlationId": correlation_id,
                    "transactionId": transaction_id,
                    "sourceService": "client",
                    "targetService": SERVICE_NAME,
                    "traceEvent": "REQUEST_ERROR"
                }
            )
            
            asyncio.create_task(publish_thread_event(
                correlation_id=correlation_id,
                transaction_id=transaction_id,
                source_service="client",
                target_service=SERVICE_NAME,
                trace_event="REQUEST_ERROR",
                duration_ms=duration_ms,
                status_code=502,
                error_message=error_msg,
            ))
            raise HTTPException(status_code=502, detail=error_msg)
        except httpx.RequestError as e:
            duration_ms = (time.monotonic() - start_time) * 1000
            error_msg = f"Payment unreachable: {str(e)}"
            
            logging.error(error_msg,
                extra={
                    "correlationId": correlation_id,
                    "transactionId": transaction_id,
                    "sourceService": "client",
                    "targetService": SERVICE_NAME,
                    "traceEvent": "REQUEST_ERROR"
                }
            )
            
            asyncio.create_task(publish_thread_event(
                correlation_id=correlation_id,
                transaction_id=transaction_id,
                source_service="client",
                target_service=SERVICE_NAME,
                trace_event="REQUEST_ERROR",
                duration_ms=duration_ms,
                status_code=503,
                error_message=error_msg,
            ))
            raise HTTPException(status_code=503, detail=error_msg)

    duration_ms = (time.monotonic() - start_time) * 1000

    # Log + publish REQUEST_END
    logging.info("Order request processed successfully",
        extra={
            "correlationId": correlation_id,
            "transactionId": transaction_id,
            "sourceService": "client",
            "targetService": SERVICE_NAME,
            "traceEvent": "REQUEST_END"
        }
    )

    asyncio.create_task(publish_thread_event(
        correlation_id=correlation_id,
        transaction_id=transaction_id,
        source_service="client",
        target_service=SERVICE_NAME,
        trace_event="REQUEST_END",
        status_code=200,
        duration_ms=duration_ms,
    ))

    return OrderResponse(
        order_id=order_id,
        status="confirmed",
        message="Order placed successfully",
        correlation_id=correlation_id
    )
