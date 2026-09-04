import pytest
import asyncio
from uuid import uuid4
from unittest.mock import patch, AsyncMock

from shared.events.schemas import EventEnvelope
from app.messaging.consumer import payment_consumer
from app.messaging.producer import payment_producer

@pytest.mark.asyncio
async def test_payment_consumer_retries_transient_failure():
    """
    Simulates a transient error on attempt 1 that succeeds on attempt 2.
    Verifies that the consumer retries and succeeds without publishing to DLQ.
    """
    order_id = uuid4()
    event_payload = {
        "event_id": str(uuid4()),
        "event_type": "OrderCreated",
        "aggregate_type": "order",
        "aggregate_id": str(order_id),
        "correlation_id": str(uuid4()),
        "payload": {
            "customer_id": str(uuid4()),
            "total_amount": "100.00",
            "currency": "USD",
            "items": []
        }
    }

    msg = type("MockKafkaMessage", (), {
        "topic": "orders.events",
        "partition": 0,
        "offset": 55,
        "key": str(order_id).encode("utf-8"),
        "value": event_payload
    })()

    calls = 0

    async def transient_process(event):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError("Payment gateway timed out")

    with patch.object(payment_consumer, "process_order_created", side_effect=transient_process) as mock_proc, \
         patch.object(payment_producer, "publish_to_dlq", new_callable=AsyncMock) as mock_dlq:
        result = await payment_consumer.handle_message(msg)
        assert result is True
        assert calls == 2
        assert mock_dlq.call_count == 0

@pytest.mark.asyncio
async def test_payment_consumer_exhausts_retries_and_routes_to_dlq():
    """
    Simulates a permanent error. Verifies that the consumer exhausts 3 attempts
    and routes the DeadLetterEnvelope to payments.events.dlq.
    """
    order_id = uuid4()
    event_payload = {
        "event_id": str(uuid4()),
        "event_type": "OrderCreated",
        "aggregate_type": "order",
        "aggregate_id": str(order_id),
        "correlation_id": str(uuid4()),
        "payload": {"corrupted": True}
    }

    msg = type("MockKafkaMessage", (), {
        "topic": "orders.events",
        "partition": 2,
        "offset": 99,
        "key": str(order_id).encode("utf-8"),
        "value": event_payload
    })()

    with patch.object(payment_consumer, "process_order_created", side_effect=ValueError("Corrupted order payload")), \
         patch.object(payment_producer, "publish_to_dlq", new_callable=AsyncMock) as mock_dlq:
        result = await payment_consumer.handle_message(msg)
        assert result is False
        assert mock_dlq.call_count == 1

        topic, dlq_envelope = mock_dlq.call_args[0]
        assert topic == "payments.events.dlq"
        assert dlq_envelope.error_type == "ValueError"
        assert dlq_envelope.retry_count == 3
        assert dlq_envelope.original_topic == "orders.events"
        assert dlq_envelope.original_offset == 99
