import os
import asyncio
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
app = FastAPI(title="Payment Service")
INVENTORY_SERVICE_URL = os.getenv("INVENTORY_SERVICE_URL", "http://localhost:8003")

class PaymentRequest(BaseModel):
    order_id: str
    amount: float
    customer_id: str


@app.get("/health")
async def health():
    return {"status": "ok", "service": "payment-service"}


@app.post("/api/v1/payments")
async def process_payment(payment: PaymentRequest):
    if os.getenv("SIMULATE_FAILURE", "false").lower() == "true":
        raise HTTPException(
            status_code=503, detail="Payment gateway timeout — simulated failure"
        )
    await asyncio.sleep(0.1)
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.post(
                f"{INVENTORY_SERVICE_URL}/api/v1/reserve",
                json={"order_id": payment.order_id, "items": []},
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=502, detail=f"Inventory failed: {e.response.text}"
            )
    return {
        "payment_id": f"pay_{payment.order_id}",
        "status": "charged",
        "amount": payment.amount,
    }
