import os
import uuid
import time
import asyncio
import logging
import httpx
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
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

app = FastAPI(title="Payment Service")
SERVICE_NAME = "payment-service"
INVENTORY_URL = os.getenv("INVENTORY_SERVICE_URL", "http://localhost:8003")

class PaymentRequest(BaseModel):
    order_id: str
    amount: float = Field(gt=0)
    customer_id: str
    items: list[dict]

@app.get("/health")
async def health():
    return {"status": "ok", "service": SERVICE_NAME}

@app.post("/api/v1/payments")
async def process_payment(payment: PaymentRequest, request: Request):
    correlation_id = request.headers.get("x-correlation-id", str(uuid.uuid4()))
    # Extract or generate a transaction ID
    transaction_id = request.headers.get("x-transaction-id", str(uuid.uuid4()))
    start_time = time.monotonic()

    # Log + publish REQUEST_START
    logging.info("Payment processing started",
        extra={
            "correlationId": correlation_id,
            "transactionId": transaction_id,
            "sourceService": "order-service",
            "targetService": SERVICE_NAME,
            "traceEvent": "REQUEST_START"
        }
    )

    asyncio.create_task(publish_thread_event(
        correlation_id=correlation_id,
        transaction_id=transaction_id,
        source_service="order-service",
        target_service=SERVICE_NAME,
        trace_event="REQUEST_START",
        method="POST",
        url=str(request.url),
        body=payment.model_dump(),
    ))

    # Failure injection
    if os.getenv("SIMULATE_FAILURE", "false").lower() == "true":
        duration_ms = (time.monotonic() - start_time) * 1000
        error_msg = "Payment gateway timeout — simulated failure"
        
        logging.error(error_msg,
            extra={
                "correlationId": correlation_id,
                "transactionId": transaction_id,
                "sourceService": "order-service",
                "targetService": SERVICE_NAME,
                "traceEvent": "REQUEST_ERROR"
            }
        )
        
        asyncio.create_task(publish_thread_event(
            correlation_id=correlation_id,
            transaction_id=transaction_id,
            source_service="order-service",
            target_service=SERVICE_NAME,
            trace_event="REQUEST_ERROR",
            status_code=503,
            duration_ms=duration_ms,
            error_message=error_msg,
        ))
        raise HTTPException(status_code=503, detail=error_msg)

    # Call inventory service — forward correlation ID and transaction ID
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.post(
                f"{INVENTORY_URL}/api/v1/reserve",
                json={"order_id": payment.order_id, "items": payment.items},
                headers={
                    "x-correlation-id": correlation_id,
                    "x-transaction-id": transaction_id
                }
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            duration_ms = (time.monotonic() - start_time) * 1000
            error_msg = f"Inventory failed: {e.response.text}"
            
            logging.error(f"Inventory reservation failed with status {e.response.status_code}",
                extra={
                    "correlationId": correlation_id,
                    "transactionId": transaction_id,
                    "sourceService": "order-service",
                    "targetService": SERVICE_NAME,
                    "traceEvent": "REQUEST_ERROR"
                }
            )
            
            asyncio.create_task(publish_thread_event(
                correlation_id=correlation_id,
                transaction_id=transaction_id,
                source_service="order-service",
                target_service=SERVICE_NAME,
                trace_event="REQUEST_ERROR",
                duration_ms=duration_ms,
                status_code=502,
                error_message=error_msg,
            ))
            raise HTTPException(status_code=502, detail=error_msg)
        except httpx.RequestError as e:
            duration_ms = (time.monotonic() - start_time) * 1000
            error_msg = f"Inventory unreachable: {str(e)}"
            
            logging.error(error_msg,
                extra={
                    "correlationId": correlation_id,
                    "transactionId": transaction_id,
                    "sourceService": "order-service",
                    "targetService": SERVICE_NAME,
                    "traceEvent": "REQUEST_ERROR"
                }
            )
            
            asyncio.create_task(publish_thread_event(
                correlation_id=correlation_id,
                transaction_id=transaction_id,
                source_service="order-service",
                target_service=SERVICE_NAME,
                trace_event="REQUEST_ERROR",
                duration_ms=duration_ms,
                status_code=503,
                error_message=error_msg,
            ))
            raise HTTPException(status_code=503, detail=error_msg)

    duration_ms = (time.monotonic() - start_time) * 1000

    # Log + publish REQUEST_END
    logging.info("Payment processed successfully",
        extra={
            "correlationId": correlation_id,
            "transactionId": transaction_id,
            "sourceService": "order-service",
            "targetService": SERVICE_NAME,
            "traceEvent": "REQUEST_END"
        }
    )

    asyncio.create_task(publish_thread_event(
        correlation_id=correlation_id,
        transaction_id=transaction_id,
        source_service="order-service",
        target_service=SERVICE_NAME,
        trace_event="REQUEST_END",
        status_code=200,
        duration_ms=duration_ms,
    ))

    return {
        "payment_id": f"pay_{payment.order_id}",
        "status": "charged",
        "amount": payment.amount,
    }
