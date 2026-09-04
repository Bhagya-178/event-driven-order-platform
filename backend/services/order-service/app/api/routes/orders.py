from typing import Optional
from uuid import UUID
import json
import asyncio
from fastapi import APIRouter, Depends, Header, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.api.dependencies import get_db, get_current_user
from app.schemas.order import OrderCreate, OrderResponse
from app.services.order_service import OrderService
from app.core.redis import redis_manager
from app.db.session import AsyncSessionLocal
from app.models.order import Order

router = APIRouter()

@router.post("/", response_model=OrderResponse, status_code=status.HTTP_201_CREATED)
async def create_order(
    data: OrderCreate,
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user)
):
    service = OrderService(db)
    order = await service.create_order(data, idempotency_key=idempotency_key)

    # Ensure cache is fresh
    await redis_manager.delete(f"order:{order.id}")
    return order

@router.get("/{order_id}", response_model=OrderResponse)
async def get_order(
    order_id: UUID,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user)
):
    cache_key = f"order:{order_id}"
    cached_data = await redis_manager.get(cache_key)
    if cached_data:
        try:
            return json.loads(cached_data)
        except Exception:
            pass

    service = OrderService(db)
    order = await service.get_order(order_id)

    try:
        serialized = OrderResponse.model_validate(order).model_dump_json()
        await redis_manager.set(cache_key, serialized, expire=60)
    except Exception:
        pass

    return order
