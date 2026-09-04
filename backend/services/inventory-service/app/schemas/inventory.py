from uuid import UUID
from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field

class InventoryResponse(BaseModel):
    product_id: UUID
    available_quantity: int
    reserved_quantity: int
    version: int

    model_config = ConfigDict(from_attributes=True)

class ReservationCreate(BaseModel):
    order_id: UUID
    product_id: UUID
    quantity: int = Field(gt=0)

class ReservationResponse(BaseModel):
    id: UUID
    order_id: UUID
    product_id: UUID
    quantity: int
    status: str
    created_at: datetime
    expires_at: datetime | None = None

    model_config = ConfigDict(from_attributes=True)
