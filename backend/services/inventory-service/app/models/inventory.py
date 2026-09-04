import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, Integer, Uuid, DateTime, String, CheckConstraint
from app.db.base import Base

class Inventory(Base):
    __tablename__ = "inventory"

    id = Column(Uuid, primary_key=True, default=uuid.uuid4)
    product_id = Column(Uuid, unique=True, nullable=False)
    available_quantity = Column(Integer, nullable=False, default=0)
    reserved_quantity = Column(Integer, nullable=False, default=0)
    version = Column(Integer, nullable=False, default=1)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    __table_args__ = (
        CheckConstraint('available_quantity >= 0', name='check_available_quantity_positive'),
        CheckConstraint('reserved_quantity >= 0', name='check_reserved_quantity_positive'),
    )
    
    __mapper_args__ = {
        "version_id_col": version
    }

class InventoryReservation(Base):
    __tablename__ = "inventory_reservations"

    id = Column(Uuid, primary_key=True, default=uuid.uuid4)
    order_id = Column(Uuid, nullable=False)
    product_id = Column(Uuid, nullable=False)
    quantity = Column(Integer, nullable=False)
    status = Column(String(50), nullable=False, default="RESERVED") # RESERVED, CONFIRMED, RELEASED
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        CheckConstraint('quantity > 0', name='check_reservation_quantity_positive'),
    )
