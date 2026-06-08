import os
import asyncio
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="Inventory Service")


class ReserveRequest(BaseModel):
    order_id: str
    items: list[dict]


@app.get("/health")
async def health():
    return {"status": "ok", "service": "inventory-service"}


@app.post("/api/v1/reserve")
async def reserve_inventory(request: ReserveRequest):
    await asyncio.sleep(0.05)
    return {
        "reservation_id": f"res_{request.order_id}",
        "status": "reserved",
        "order_id": request.order_id,
    }
