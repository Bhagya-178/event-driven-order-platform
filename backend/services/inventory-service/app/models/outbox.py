import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, Integer, DateTime, Text, Uuid, JSON, Index
from app.db.base import Base

class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id = Column(Uuid, primary_key=True, default=uuid.uuid4)
    aggregate_type = Column(String(100), nullable=False)
    aggregate_id = Column(Uuid, nullable=False)
    event_type = Column(String(100), nullable=False)
    payload = Column(JSON, nullable=False)

    correlation_id = Column(Uuid, nullable=True)
    causation_id = Column(Uuid, nullable=True)

    status = Column(String(50), nullable=False, default="PENDING", index=True)
    retry_count = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False, index=True)
    published_at = Column(DateTime(timezone=True), nullable=True)
    last_error = Column(Text, nullable=True)

    __table_args__ = (
        Index("ix_inventory_outbox_status_created", "status", "created_at"),
    )
