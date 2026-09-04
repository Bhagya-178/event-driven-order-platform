from typing import Any, Dict, Optional
from datetime import datetime, timezone
from uuid import UUID, uuid4
from pydantic import BaseModel, Field

class EventEnvelope(BaseModel):
    """
    Standard event envelope for all Kafka messages in the platform.
    Ensures traceability and consistency across domains.
    """
    event_id: UUID = Field(default_factory=uuid4)
    event_type: str
    event_version: int = 1
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    aggregate_type: str
    aggregate_id: UUID
    correlation_id: UUID
    causation_id: Optional[UUID] = None
    payload: Dict[str, Any]

# --- Payload Schemas ---

class OrderItemPayload(BaseModel):
    product_id: UUID
    quantity: int
    unit_price: str

class OrderCreatedPayload(BaseModel):
    customer_id: UUID
    total_amount: str
    currency: str
    items: list[OrderItemPayload]

class PaymentOutcomePayload(BaseModel):
    amount: str
    currency: str
    status: str  # "SUCCEEDED" or "FAILED"
    provider_payment_id: Optional[str] = None
    error_message: Optional[str] = None

class InventoryReservedPayload(BaseModel):
    items: list[OrderItemPayload]

class InventoryFailedPayload(BaseModel):
    product_id: UUID
    requested_quantity: int
    available_quantity: int
    error_message: str

class InventoryReleasedPayload(BaseModel):
    items: list[OrderItemPayload]
    reason: str

class DeadLetterEnvelope(BaseModel):
    """
    Standard forensic envelope for dead-lettered messages.
    Captures complete context of failure, partition coordinates, and payload.
    """
    dlq_id: UUID = Field(default_factory=uuid4)
    original_topic: str
    original_partition: int
    original_offset: int
    original_key: Optional[str] = None
    error_type: str
    error_message: str
    stack_trace: Optional[str] = None
    retry_count: int
    failed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: Any

class DeadLetterEnvelope(BaseModel):
    """
    Standard forensic envelope for dead-lettered messages.
    Captures complete context of failure, partition coordinates, and payload.
    """
    dlq_id: UUID = Field(default_factory=uuid4)
    original_topic: str
    original_partition: int
    original_offset: int
    original_key: Optional[str] = None
    error_type: str
    error_message: str
    stack_trace: Optional[str] = None
    retry_count: int
    failed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: Any

class DeadLetterEnvelope(BaseModel):
    """
    Standard forensic envelope for dead-lettered messages.
    Captures complete context of failure, partition coordinates, and payload.
    """
    dlq_id: UUID = Field(default_factory=uuid4)
    original_topic: str
    original_partition: int
    original_offset: int
    original_key: Optional[str] = None
    error_type: str
    error_message: str
    stack_trace: Optional[str] = None
    retry_count: int
    failed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: Any

