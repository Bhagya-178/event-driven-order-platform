import pytest
import asyncio
from uuid import uuid4
import json
from aiokafka import AIOKafkaProducer

from app.db.session import AsyncSessionLocal
from app.models.order import Order
from app.models.processed_event import ProcessedEvent
from sqlalchemy import select
from shared.events.schemas import EventEnvelope
from app.messaging.consumer import order_consumer
from app.messaging.producer import order_producer


import socket

def is_kafka_running():
    try:
        with socket.create_connection(("localhost", 9092), timeout=0.5):
            return True
    except OSError:
        return False

@pytest.mark.asyncio
@pytest.mark.skipif(not is_kafka_running(), reason="Live Kafka broker not running on localhost:9092")
async def test_order_state_machine_updates():
    await order_producer.start()
    await order_consumer.start()
    
    order_id = uuid4()
    payment_success_id = uuid4()
    inventory_reserved_id = uuid4()
    
    # Setup Order
    async with AsyncSessionLocal() as session:
        order = Order(
            id=order_id,
            customer_id=uuid4(),
            total_amount="100.00",
            currency="USD"
        )
        session.add(order)
        await session.commit()
    
    payment_event = EventEnvelope(
        event_id=payment_success_id,
        event_type="PaymentSucceeded",
        aggregate_type="order",
        aggregate_id=order_id,
        correlation_id=uuid4(),
        payload={"amount": "100.00", "currency": "USD", "status": "SUCCEEDED"}
    )
    
    inventory_event = EventEnvelope(
        event_id=inventory_reserved_id,
        event_type="InventoryReserved",
        aggregate_type="order",
        aggregate_id=order_id,
        correlation_id=uuid4(),
        payload={"items": []}
    )
    
    producer = AIOKafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8')
    )
    await producer.start()
    
    # Send PaymentSucceeded
    await producer.send_and_wait("payments.events", key=str(order_id), value=payment_event.model_dump(mode="json"))
    
    # Send InventoryReserved
    await producer.send_and_wait("inventory.events", key=str(order_id), value=inventory_event.model_dump(mode="json"))
    
    # Wait for consumer to process both
    for _ in range(10):
        await asyncio.sleep(1)
        async with AsyncSessionLocal() as session:
            stmt = select(Order).where(Order.id == order_id)
            order = (await session.execute(stmt)).scalars().first()
            if order and order.status == "CONFIRMED":
                break
                
    assert order.payment_status == "SUCCEEDED"
    assert order.inventory_status == "RESERVED"
    assert order.status == "CONFIRMED"

    await producer.stop()
    await order_consumer.stop()
    await order_producer.stop()

