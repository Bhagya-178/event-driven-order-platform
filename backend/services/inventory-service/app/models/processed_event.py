from datetime import datetime, timezone
from sqlalchemy import Column, String, DateTime, Boolean, Uuid
import uuid
from app.db.base import Base

class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    event_id = Column(Uuid, primary_key=True)
    event_type = Column(String(255), nullable=False)
    consumer_name = Column(String(255), nullable=False)
    processed_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

class OrderPaymentState(Base):
    __tablename__ = "order_payment_states"

    order_id = Column(Uuid, primary_key=True)
    payment_failed = Column(Boolean, default=False, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
