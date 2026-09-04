import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, DateTime, Numeric, Uuid, ForeignKey
from sqlalchemy.orm import relationship
from app.db.base import Base

class Order(Base):
    __tablename__ = "orders"

    id = Column(Uuid, primary_key=True, default=uuid.uuid4)
    customer_id = Column(Uuid, nullable=False)
    idempotency_key = Column(String(255), unique=True, nullable=True, index=True)

    # State tracking
    payment_status = Column(String(50), nullable=False, default="PENDING")
    inventory_status = Column(String(50), nullable=False, default="PENDING")
    status = Column(String(50), nullable=False, default="CREATED")
    
    total_amount = Column(Numeric(12, 2), nullable=False)
    currency = Column(String(3), nullable=False, default="USD")
    
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    items = relationship("OrderItem", back_populates="order", cascade="all, delete-orphan")
