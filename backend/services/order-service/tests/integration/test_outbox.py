import pytest
import asyncio
from uuid import uuid4
from decimal import Decimal
from unittest.mock import AsyncMock, patch

from sqlalchemy import select
from app.db.session import AsyncSessionLocal
from app.models.order import Order
from app.models.outbox import OutboxEvent
from app.schemas.order import OrderCreate, OrderItemCreate
from app.services.order_service import OrderService
from app.messaging.outbox_publisher import order_outbox_publisher
from app.messaging.producer import order_producer

@pytest.mark.asyncio
async def test_order_creation_creates_pending_outbox_event():
    """
    Verifies that create_order atomically writes the order, items,
    and a PENDING OutboxEvent in the same DB transaction without calling Kafka directly.
    """
    customer_id = uuid4()
    product_id = uuid4()
    order_data = OrderCreate(
        customer_id=customer_id,
        items=[OrderItemCreate(product_id=product_id, quantity=3)]
    )

    async with AsyncSessionLocal() as session:
        service = OrderService(session)
        created_order = await service.create_order(order_data)
        order_id = created_order.id

    # Verify order and outbox record exist in DB
    async with AsyncSessionLocal() as session:
        order = await session.get(Order, order_id)
        assert order is not None
        assert order.total_amount == Decimal("30.00")

        stmt = select(OutboxEvent).where(OutboxEvent.aggregate_id == order_id)
        outbox = (await session.execute(stmt)).scalars().first()
        assert outbox is not None
        assert outbox.event_type == "OrderCreated"
        assert outbox.status == "PENDING"
        assert outbox.retry_count == 0
        assert outbox.correlation_id is not None
        assert outbox.payload["customer_id"] == str(customer_id)
        assert len(outbox.payload["items"]) == 1
        assert outbox.payload["items"][0]["quantity"] == 3

@pytest.mark.asyncio
async def test_outbox_publisher_relays_to_kafka_and_marks_published():
    """
    Verifies that the OutboxPublisher reads PENDING events, relays them to Kafka,
    and updates their status to PUBLISHED with published_at set.
    """
    customer_id = uuid4()
    product_id = uuid4()
    order_data = OrderCreate(
        customer_id=customer_id,
        items=[OrderItemCreate(product_id=product_id, quantity=1)]
    )

    async with AsyncSessionLocal() as session:
        service = OrderService(session)
        created_order = await service.create_order(order_data)
        order_id = created_order.id

    # Mock producer to verify publisher behavior without requiring live Kafka daemon in test runner
    with patch.object(order_producer, "publish_event", new_callable=AsyncMock) as mock_publish:
        processed = await order_outbox_publisher.publish_pending_batch()
        assert processed >= 1
        assert mock_publish.called

        # Verify the envelope passed to publish_event
        called_envelope = mock_publish.call_args[0][0]
        assert called_envelope.event_type == "OrderCreated"
        assert called_envelope.aggregate_id == order_id
        assert called_envelope.payload["customer_id"] == str(customer_id)

    # Verify status in DB is now PUBLISHED
    async with AsyncSessionLocal() as session:
        stmt = select(OutboxEvent).where(OutboxEvent.aggregate_id == order_id)
        outbox = (await session.execute(stmt)).scalars().first()
        assert outbox.status == "PUBLISHED"
        assert outbox.published_at is not None
        assert outbox.last_error is None

@pytest.mark.asyncio
async def test_outbox_publisher_kafka_failure_retry():
    """
    Verifies that if Kafka publishing raises an exception, the event remains PENDING,
    retry_count is incremented, and last_error is captured.
    """
    customer_id = uuid4()
    product_id = uuid4()
    order_data = OrderCreate(
        customer_id=customer_id,
        items=[OrderItemCreate(product_id=product_id, quantity=2)]
    )

    async with AsyncSessionLocal() as session:
        service = OrderService(session)
        created_order = await service.create_order(order_data)
        order_id = created_order.id

    # Mock producer to simulate a temporary Kafka connection failure
    with patch.object(order_producer, "publish_event", side_effect=ConnectionError("Kafka broker unreachable")):
        processed = await order_outbox_publisher.publish_pending_batch()
        assert processed >= 1

    # Verify event remains PENDING with incremented retry_count and error message
    async with AsyncSessionLocal() as session:
        stmt = select(OutboxEvent).where(OutboxEvent.aggregate_id == order_id)
        outbox = (await session.execute(stmt)).scalars().first()
        assert outbox.status == "PENDING"
        assert outbox.retry_count == 1
        assert "Kafka broker unreachable" in outbox.last_error
        assert outbox.published_at is None
