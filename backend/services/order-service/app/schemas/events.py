from uuid import UUID
from datetime import datetime
from pydantic import BaseModel
from typing import Dict, Any

class EventEnvelope(BaseModel):
    event_id: UUID
    event_type: str
    aggregate_id: UUID
    occurred_at: datetime
    correlation_id: UUID
    version: int = 1
    payload: Dict[str, Any]
