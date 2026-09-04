import pytest
import asyncio
from uuid import uuid4
from unittest.mock import patch, AsyncMock

from shared.events.schemas import EventEnvelope
from app.messaging.consumer import order_consumer
from app.messaging.producer import order_producer

@pytest.mark.asyncio
async def test_order_consumer_retries_transient_failure():
    """
    Simulates a transient error on attempt 1 that succeeds on attempt 2.
    Verifies that the consumer retries and succeeds without publishing to DLQ.
    """
    order_id = uuid4()
    event_payload = {
        "event_id": str(uuid4()),
        "event_type": "PaymentSucceeded",
        "aggregate_type": "order",
        "aggregate_id": str(order_id),
        "correlation_id": str(uuid4()),
        "payload": {"amount": "100.00", "currency": "USD", "status": "SUCCEEDED"}
    }

    msg = type("MockKafkaMessage", (), {
        "topic": "payments.events",
        "partition": 0,
        "offset": 105,
        "key": str(order_id).encode("utf-8"),
        "value": event_payload
    })()

    calls = 0

    async def transient_process(event):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionError("Transient DB connection drop")

    with patch.object(order_consumer, "process_event", side_effect=transient_process) as mock_proc, \
         patch.object(order_producer, "publish_to_dlq", new_callable=AsyncMock) as mock_dlq:
        result = await order_consumer.handle_message(msg)
        assert result is True
        assert calls == 2
        assert mock_dlq.call_count == 0

@pytest.mark.asyncio
async def test_order_consumer_exhausts_retries_and_routes_to_dlq():
    """
    Simulates a permanent error. Verifies that the consumer exhausts 3 attempts
    and routes the DeadLetterEnvelope to orders.events.dlq.
    """
    order_id = uuid4()
    event_payload = {
        "event_id": str(uuid4()),
        "event_type": "PaymentSucceeded",
        "aggregate_type": "order",
        "aggregate_id": str(order_id),
        "correlation_id": str(uuid4()),
        "payload": {"invalid": "payload"}
    }

    msg = type("MockKafkaMessage", (), {
        "topic": "payments.events",
        "partition": 1,
        "offset": 204,
        "key": str(order_id).encode("utf-8"),
        "value": event_payload
    })()

    with patch.object(order_consumer, "process_event", side_effect=ValueError("Unrecoverable data error")), \
         patch.object(order_producer, "publish_to_dlq", new_callable=AsyncMock) as mock_dlq:
        result = await order_consumer.handle_message(msg)
        assert result is False
        assert mock_dlq.call_count == 1

        topic, dlq_envelope = mock_dlq.call_args[0]
        assert topic == "orders.events.dlq"
        assert dlq_envelope.error_type == "ValueError"
        assert dlq_envelope.retry_count == 3
        assert dlq_envelope.original_topic == "payments.events"
        assert dlq_envelope.original_offset == 204
