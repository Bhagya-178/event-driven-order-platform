from uuid import UUID
from decimal import Decimal
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timezone
import uuid
import asyncio
from typing import Optional
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.schemas.order import OrderCreate
from app.models.order import Order
from app.models.order_item import OrderItem
from app.models.outbox import OutboxEvent
from app.repositories.order_repository import OrderRepository
from app.repositories.outbox_repository import OutboxRepository
from shared.exceptions.errors import NotFoundError, ValidationError
from shared.events.schemas import OrderCreatedPayload, OrderItemPayload

from sqlalchemy.orm import selectinload

class OrderService:
    _sqlite_lock = asyncio.Lock()

    def __init__(self, session: AsyncSession):
        self.session = session
        self.order_repo = OrderRepository(session)
        self.outbox_repo = OutboxRepository(session)

    async def create_order(self, data: OrderCreate, idempotency_key: Optional[str] = None) -> Order:
        """
        Creates an order, its items, and an outbox event transactionally.
        If an idempotency_key is provided and an order already exists, returns the existing order.
        """
        is_postgres = self.session.bind and self.session.bind.dialect.name == "postgresql"
        if not is_postgres:
            await self._sqlite_lock.acquire()
        try:
            if idempotency_key:
                stmt = select(Order).options(selectinload(Order.items)).where(Order.idempotency_key == idempotency_key)
                existing = (await self.session.execute(stmt)).scalars().first()
                if existing:
                    return existing

            order = Order(
                id=uuid.uuid4(),
                customer_id=data.customer_id,
                idempotency_key=idempotency_key,
                status="CREATED",
                payment_status="PENDING",
                inventory_status="PENDING",
                currency="USD",
                total_amount=Decimal("0.00")
            )

            total_amount = Decimal("0.00")
            
            for item_data in data.items:
                unit_price = Decimal("10.00")
                total_price = unit_price * item_data.quantity
                total_amount += total_price

                order_item = OrderItem(
                    id=uuid.uuid4(),
                    product_id=item_data.product_id,
                    quantity=item_data.quantity,
                    unit_price=unit_price,
                    total_price=total_price
                )
                order.items.append(order_item)

            order.total_amount = total_amount

            # Prepare Outbox Event
            payload = OrderCreatedPayload(
                customer_id=order.customer_id,
                total_amount=str(order.total_amount),
                currency=order.currency,
                items=[
                    OrderItemPayload(
                        product_id=item.product_id,
                        quantity=int(item.quantity),
                        unit_price=str(item.unit_price)
                    ) for item in order.items
                ]
            )

            corr_id = uuid.uuid4()
            event_id = uuid.uuid4()

            outbox_event = OutboxEvent(
                id=event_id,
                aggregate_type="order",
                aggregate_id=order.id,
                event_type="OrderCreated",
                correlation_id=corr_id,
                causation_id=None,
                payload=payload.model_dump(mode="json"),
                status="PENDING",
                created_at=datetime.now(timezone.utc)
            )

            try:
                self.order_repo.add(order)
                self.outbox_repo.add(outbox_event)
                await self.session.commit()
                return order
            except IntegrityError:
                await self.session.rollback()
                if idempotency_key:
                    for _ in range(10):
                        await asyncio.sleep(0.05)
                        stmt = select(Order).options(selectinload(Order.items)).where(Order.idempotency_key == idempotency_key)
                        existing = (await self.session.execute(stmt)).scalars().first()
                        if existing:
                            return existing
                raise
            except Exception as e:
                await self.session.rollback()
                raise e
        finally:
            if not is_postgres:
                self._sqlite_lock.release()

    async def get_order(self, order_id: UUID) -> Order:
        order = await self.order_repo.get_by_id(order_id)
        if not order:
            raise NotFoundError(f"Order {order_id} not found")
        return order

    async def list_orders(self, limit: int = 50, offset: int = 0) -> list[Order]:
        """Lists recent orders for the customer and business portals."""
        return await self.order_repo.list_recent(limit=limit, offset=offset)

    async def get_order_trace(self, order_id: UUID) -> dict:
        """Constructs detailed technical trace for distributed saga visualization."""
        order = await self.get_order(order_id)
        outbox_events = await self.outbox_repo.get_by_aggregate_id(order_id)

        milestones = [
            {
                "step": "ORDER_CREATED",
                "service": "order-service",
                "status": "COMPLETED",
                "timestamp": order.created_at,
                "detail": f"Order persisted with {len(order.items)} item(s) and outbox event staged atomically."
            }
        ]

        if order.payment_status == "SUCCEEDED":
            milestones.append({
                "step": "PAYMENT_PROCESSING",
                "service": "payment-service",
                "status": "COMPLETED",
                "timestamp": order.updated_at,
                "detail": f"Payment of ${order.total_amount} {order.currency} captured via Payment Service."
            })
        elif order.payment_status == "FAILED":
            milestones.append({
                "step": "PAYMENT_PROCESSING",
                "service": "payment-service",
                "status": "FAILED",
                "timestamp": order.updated_at,
                "detail": "Payment transaction failed."
            })
        else:
            milestones.append({
                "step": "PAYMENT_PROCESSING",
                "service": "payment-service",
                "status": "IN_PROGRESS",
                "timestamp": None,
                "detail": "Awaiting PaymentSucceeded event from payments.events topic."
            })

        if order.inventory_status == "RESERVED":
            milestones.append({
                "step": "INVENTORY_RESERVATION",
                "service": "inventory-service",
                "status": "COMPLETED",
                "timestamp": order.updated_at,
                "detail": "Stock reserved using row locks and version increment."
            })
        elif order.inventory_status == "FAILED":
            milestones.append({
                "step": "INVENTORY_RESERVATION",
                "service": "inventory-service",
                "status": "FAILED",
                "timestamp": order.updated_at,
                "detail": "Stock reservation failed due to insufficient stock."
            })
        else:
            milestones.append({
                "step": "INVENTORY_RESERVATION",
                "service": "inventory-service",
                "status": "IN_PROGRESS",
                "timestamp": None,
                "detail": "Awaiting InventoryReserved event from inventory.events topic."
            })

        if order.status == "CONFIRMED":
            milestones.append({
                "step": "SAGA_COMPLETION",
                "service": "order-service",
                "status": "COMPLETED",
                "timestamp": order.updated_at,
                "detail": "Distributed saga completed successfully! Order is CONFIRMED."
            })
        elif order.status == "FAILED":
            milestones.append({
                "step": "SAGA_COMPENSATION",
                "service": "order-service",
                "status": "FAILED",
                "timestamp": order.updated_at,
                "detail": "Order marked FAILED; compensations applied."
            })
        else:
            milestones.append({
                "step": "SAGA_COORDINATION",
                "service": "order-service",
                "status": "PENDING",
                "timestamp": None,
                "detail": "State machine awaiting convergence of events."
            })

        return {
            "order": order,
            "saga_timeline": milestones,
            "outbox_events": outbox_events
        }
