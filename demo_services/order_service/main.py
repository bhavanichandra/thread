import os
import asyncio
import httpx

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Order Service")

PAYMENT_SERVICE_URL = os.getenv("PAYMENT_SERVICE_URL", "http://localhost:8002")


class OrderRequest(BaseModel):

    customer_id: str
    items: list[dict]
    total: float


class OrderResponse(BaseModel):
    order_id: str
    status: str
    message: str


@app.get("/health")
async def health():

    return {"status": "ok", "service": "order-service"}


@app.post("/api/v1/orders", response_model=OrderResponse)
async def create_order(order: OrderRequest):
    import uuid

    order_id = str(uuid.uuid4())[:8]
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.post(
                f"{PAYMENT_SERVICE_URL}/api/v1/payments",
                json={
                    "order_id": order_id,
                    "amount": order.total,
                    "customer_id": order.customer_id,
                    "items": order.items,
                },
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=502, detail=f"Payment failed: {e.response.text}"
            )
        except httpx.RequestError as e:
            raise HTTPException(
                status_code=503, detail=f"Payment unreachable: {str(e)}"
            )
    return OrderResponse(
        order_id=order_id, status="confirmed", message="Order placed successfully"
    )
