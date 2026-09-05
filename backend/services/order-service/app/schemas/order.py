from uuid import UUID
from datetime import datetime
from decimal import Decimal
from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field, ConfigDict

class OrderItemCreate(BaseModel):
    product_id: UUID
    quantity: int = Field(gt=0)

class OrderCreate(BaseModel):
    customer_id: UUID
    items: List[OrderItemCreate] = Field(min_length=1)

class OrderItemResponse(BaseModel):
    id: UUID
    product_id: UUID
    quantity: int
    unit_price: Decimal
    total_price: Decimal

    model_config = ConfigDict(from_attributes=True)

class OrderResponse(BaseModel):
    id: UUID
    customer_id: UUID
    status: str
    payment_status: Optional[str] = "PENDING"
    inventory_status: Optional[str] = "PENDING"
    idempotency_key: Optional[str] = None
    currency: str
    total_amount: Decimal
    created_at: datetime
    updated_at: datetime
    items: List[OrderItemResponse]

    model_config = ConfigDict(from_attributes=True)

class OutboxEventTrace(BaseModel):
    id: UUID
    event_type: str
    status: str
    retry_count: int
    created_at: datetime
    published_at: Optional[datetime] = None
    correlation_id: Optional[UUID] = None
    payload: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(from_attributes=True)

class SagaMilestone(BaseModel):
    step: str
    service: str
    status: str
    timestamp: Optional[datetime] = None
    detail: str

class OrderTraceResponse(BaseModel):
    order: OrderResponse
    saga_timeline: List[SagaMilestone]
    outbox_events: List[OutboxEventTrace]
