import pytest
import asyncio
from uuid import uuid4
from unittest.mock import patch, AsyncMock
from sqlalchemy import select

from app.messaging.consumer import inventory_consumer
from app.messaging.producer import inventory_producer
from app.models.inventory import Inventory, InventoryReservation
from app.models.processed_event import ProcessedEvent
from app.db.session import AsyncSessionLocal
from shared.events.schemas import EventEnvelope

@pytest.mark.asyncio
async def test_poison_pill_unblocks_inventory_consumer_partition():
    poison_msg = type("MockMsg", (), {
        "topic": "orders.events",
        "partition": 0,
        "offset": 300,
        "key": b"poison",
        "value": {"bad": "data"}
    })()

    order_id = uuid4()
    product_id = uuid4()

    # Pre-seed inventory
    async with AsyncSessionLocal() as session:
        inv = Inventory(product_id=product_id, available_quantity=10, reserved_quantity=0)
        session.add(inv)
        await session.commit()

    valid_msg = type("MockMsg", (), {
        "topic": "orders.events",
        "partition": 0,
        "offset": 301,
        "key": str(order_id).encode("utf-8"),
        "value": {
            "event_id": str(uuid4()),
            "event_type": "OrderCreated",
            "aggregate_type": "order",
            "aggregate_id": str(order_id),
            "correlation_id": str(uuid4()),
            "payload": {
                "customer_id": str(uuid4()),
                "total_amount": "20.00",
                "currency": "USD",
                "items": [{"product_id": str(product_id), "quantity": 2, "unit_price": "10.00"}]
            }
        }
    })()

    committed_offsets = []

    with patch.object(inventory_producer, "publish_to_dlq", new_callable=AsyncMock) as mock_dlq:
        for msg in [poison_msg, valid_msg]:
            await inventory_consumer.handle_message(msg)
            committed_offsets.append(msg.offset)

    assert mock_dlq.call_count == 1
    dlq_topic, dlq_env = mock_dlq.call_args[0]
    assert dlq_topic == "inventory.events.dlq"
    assert dlq_env.original_offset == 300

    # Both offsets committed -> partition was unblocked
    assert committed_offsets == [300, 301]

    # Valid event was processed
    async with AsyncSessionLocal() as session:
        stmt = select(InventoryReservation).where(InventoryReservation.order_id == order_id)
        reservation = (await session.execute(stmt)).scalars().first()
        assert reservation is not None
        assert reservation.quantity == 2

@pytest.mark.asyncio
async def test_inventory_at_least_once_redelivery_idempotency():
    order_id = uuid4()
    event_id = uuid4()
    product_id = uuid4()

    # Pre-seed processed event and reservation
    async with AsyncSessionLocal() as session:
        inv = Inventory(product_id=product_id, available_quantity=8, reserved_quantity=2)
        session.add(inv)
        proc = ProcessedEvent(
            event_id=event_id,
            event_type="OrderCreated",
            consumer_name="inventory-service"
        )
        session.add(proc)
        res = InventoryReservation(
            order_id=order_id,
            product_id=product_id,
            quantity=2,
            status="RESERVED"
        )
        session.add(res)
        await session.commit()

    redelivered_msg = type("MockMsg", (), {
        "topic": "orders.events",
        "partition": 0,
        "offset": 90,
        "key": str(order_id).encode("utf-8"),
        "value": {
            "event_id": str(event_id),
            "event_type": "OrderCreated",
            "aggregate_type": "order",
            "aggregate_id": str(order_id),
            "correlation_id": str(uuid4()),
            "payload": {
                "customer_id": str(uuid4()),
                "total_amount": "20.00",
                "currency": "USD",
                "items": [{"product_id": str(product_id), "quantity": 2, "unit_price": "10.00"}]
            }
        }
    })()

    handled = await inventory_consumer.handle_message(redelivered_msg)
    assert handled is True

    # Verify inventory was not deducted again
    async with AsyncSessionLocal() as session:
        inv = (await session.execute(select(Inventory).where(Inventory.product_id == product_id))).scalars().first()
        assert inv.available_quantity == 8
        assert inv.reserved_quantity == 2
