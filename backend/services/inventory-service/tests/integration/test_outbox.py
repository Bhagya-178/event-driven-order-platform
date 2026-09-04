import pytest
import asyncio
from uuid import uuid4
from unittest.mock import patch, AsyncMock

from sqlalchemy import select
from app.db.session import AsyncSessionLocal
from app.models.inventory import Inventory, InventoryReservation
from app.models.outbox import OutboxEvent
from app.models.processed_event import ProcessedEvent
from app.messaging.consumer import inventory_consumer
from app.messaging.outbox_publisher import inventory_outbox_publisher
from app.messaging.producer import inventory_producer
from shared.events.schemas import EventEnvelope

@pytest.mark.asyncio
async def test_inventory_consumer_creates_pending_outbox_event():
    """
    Verifies that processing OrderCreated creates the inventory reservation,
    processed_event, and outbox_event in a single atomic DB transaction.
    """
    order_id = uuid4()
    event_id = uuid4()
    corr_id = uuid4()
    product_id = uuid4()

    # Pre-seed inventory
    async with AsyncSessionLocal() as session:
        inv = Inventory(product_id=product_id, available_quantity=50, reserved_quantity=0)
        session.add(inv)
        await session.commit()

    event = EventEnvelope(
        event_id=event_id,
        event_type="OrderCreated",
        aggregate_type="order",
        aggregate_id=order_id,
        correlation_id=corr_id,
        payload={
            "customer_id": str(uuid4()),
            "total_amount": "100.00",
            "currency": "USD",
            "items": [{"product_id": str(product_id), "quantity": 5, "unit_price": "20.00"}]
        }
    )

    await inventory_consumer.process_event(event)

    async with AsyncSessionLocal() as session:
        # Check reservation
        stmt_res = select(InventoryReservation).where(InventoryReservation.order_id == order_id)
        reservation = (await session.execute(stmt_res)).scalars().first()
        assert reservation is not None
        assert reservation.quantity == 5

        # Check inventory quantities
        stmt_inv = select(Inventory).where(Inventory.product_id == product_id)
        inv = (await session.execute(stmt_inv)).scalars().first()
        assert inv.available_quantity == 45
        assert inv.reserved_quantity == 5

        # Check processed_events
        stmt_proc = select(ProcessedEvent).where(ProcessedEvent.event_id == event_id)
        processed = (await session.execute(stmt_proc)).scalars().first()
        assert processed is not None

        # Check outbox_events
        stmt_out = select(OutboxEvent).where(OutboxEvent.aggregate_id == order_id)
        outbox = (await session.execute(stmt_out)).scalars().first()
        assert outbox is not None
        assert outbox.event_type == "InventoryReserved"
        assert outbox.status == "PENDING"
        assert outbox.correlation_id == corr_id
        assert outbox.causation_id == event_id

@pytest.mark.asyncio
async def test_inventory_outbox_publisher_relays_to_kafka():
    """
    Verifies that the InventoryOutboxPublisher relays pending InventoryReserved
    events to Kafka and transitions them to PUBLISHED.
    """
    order_id = uuid4()
    event_id = uuid4()
    corr_id = uuid4()
    product_id = uuid4()

    async with AsyncSessionLocal() as session:
        inv = Inventory(product_id=product_id, available_quantity=20, reserved_quantity=0)
        session.add(inv)
        await session.commit()

    event = EventEnvelope(
        event_id=event_id,
        event_type="OrderCreated",
        aggregate_type="order",
        aggregate_id=order_id,
        correlation_id=corr_id,
        payload={
            "customer_id": str(uuid4()),
            "total_amount": "40.00",
            "currency": "USD",
            "items": [{"product_id": str(product_id), "quantity": 2, "unit_price": "20.00"}]
        }
    )

    await inventory_consumer.process_event(event)

    with patch.object(inventory_producer, "publish_event", new_callable=AsyncMock) as mock_publish:
        processed = await inventory_outbox_publisher.publish_pending_batch()
        assert processed >= 1
        assert mock_publish.called

        called_envelope = mock_publish.call_args[0][0]
        assert called_envelope.event_type == "InventoryReserved"
        assert called_envelope.aggregate_id == order_id

    async with AsyncSessionLocal() as session:
        stmt = select(OutboxEvent).where(OutboxEvent.aggregate_id == order_id)
        outbox = (await session.execute(stmt)).scalars().first()
        assert outbox.status == "PUBLISHED"
        assert outbox.published_at is not None

@pytest.mark.asyncio
async def test_inventory_outbox_publisher_failure_retry():
    """
    Verifies that if Kafka publishing fails in inventory outbox publisher,
    the event remains PENDING and retry_count is incremented.
    """
    order_id = uuid4()
    event_id = uuid4()
    corr_id = uuid4()
    product_id = uuid4()

    async with AsyncSessionLocal() as session:
        inv = Inventory(product_id=product_id, available_quantity=10, reserved_quantity=0)
        session.add(inv)
        await session.commit()

    event = EventEnvelope(
        event_id=event_id,
        event_type="OrderCreated",
        aggregate_type="order",
        aggregate_id=order_id,
        correlation_id=corr_id,
        payload={
            "customer_id": str(uuid4()),
            "total_amount": "20.00",
            "currency": "USD",
            "items": [{"product_id": str(product_id), "quantity": 1, "unit_price": "20.00"}]
        }
    )

    await inventory_consumer.process_event(event)

    with patch.object(inventory_producer, "publish_event", side_effect=RuntimeError("Kafka broker down")):
        processed = await inventory_outbox_publisher.publish_pending_batch()
        assert processed >= 1

    async with AsyncSessionLocal() as session:
        stmt = select(OutboxEvent).where(OutboxEvent.aggregate_id == order_id)
        outbox = (await session.execute(stmt)).scalars().first()
        assert outbox.status == "PENDING"
        assert outbox.retry_count == 1
        assert "Kafka broker down" in outbox.last_error
