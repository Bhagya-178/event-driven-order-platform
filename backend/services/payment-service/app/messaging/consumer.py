import json
import asyncio
import logging
from uuid import uuid4
from aiokafka import AIOKafkaConsumer
from shared.events.schemas import EventEnvelope, PaymentOutcomePayload
from shared.events.consumer_handler import process_with_retry_and_dlq
from app.core.config import settings
from app.db.session import AsyncSessionLocal
from datetime import datetime, timezone
from app.models.payment import Payment
from app.models.processed_event import ProcessedEvent
from app.models.outbox import OutboxEvent
from app.services.payment_service import PaymentService
from app.messaging.producer import payment_producer
from sqlalchemy import select

logger = logging.getLogger(__name__)

class PaymentConsumer:
    _sqlite_lock = asyncio.Lock()

    def __init__(self):
        self.consumer = None
        self.task = None

    async def start(self):
        bootstrap_servers = getattr(settings, "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.consumer = AIOKafkaConsumer(
            "orders.events",
            bootstrap_servers=bootstrap_servers,
            group_id="payment-service",
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        await self.consumer.start()
        self.task = asyncio.create_task(self.consume())
        logger.info("PaymentConsumer started")

    async def stop(self):
        if self.task:
            self.task.cancel()
        if self.consumer:
            await self.consumer.stop()
            logger.info("PaymentConsumer stopped")

    async def consume(self):
        try:
            async for msg in self.consumer:
                await self.handle_message(msg)
                await self.consumer.commit()
        except asyncio.CancelledError:
            pass

    async def handle_message(self, msg) -> bool:
        async def _process(m):
            val = m.value if hasattr(m, "value") else m
            if isinstance(val, dict):
                event = EventEnvelope.model_validate(val)
            elif isinstance(val, str):
                event = EventEnvelope.model_validate(json.loads(val))
            elif isinstance(val, EventEnvelope):
                event = val
            else:
                raise ValueError(f"Unparseable message: {val}")
            if event.event_type == "OrderCreated":
                await self.process_order_created(event)

        return await process_with_retry_and_dlq(
            msg=msg,
            process_func=_process,
            dlq_publish_func=lambda topic, env: payment_producer.publish_to_dlq(topic, env),
            dlq_topic="payments.events.dlq",
            max_retries=3,
            initial_delay=0.01,
            backoff_factor=2.0
        )

    async def process_order_created(self, event: EventEnvelope):
        async with AsyncSessionLocal() as session:
            is_postgres = session.bind and session.bind.dialect.name == "postgresql"
            if not is_postgres:
                await self._sqlite_lock.acquire()
            try:
                stmt = select(ProcessedEvent).where(ProcessedEvent.event_id == event.event_id)
                result = await session.execute(stmt)
                if result.scalars().first():
                    logger.info(f"Event {event.event_id} already processed, skipping.")
                    return

                # Note: Payment processing might take time, but we do it in transaction
                # In a real system, you might separate the external API call from the DB transaction.
                # For this learning progression, we do it in one atomic block.
                
                # Create payment record
                payload = event.payload
                payment = Payment(
                    order_id=event.aggregate_id,
                    idempotency_key=str(event.event_id), # Using event_id as idempotency key
                    amount=payload["total_amount"],
                    currency=payload["currency"],
                    status="PENDING"
                )
                session.add(payment)
                await session.flush() # flush to get payment.id if needed, or just let service handle it
                
                # Since PaymentService expects a dict, let's adapt
                # Wait, our existing PaymentService expects an API request. Let's just do it directly.
                payment.status = "SUCCEEDED" # Simulated successful payment for now

                # Mark processed
                processed = ProcessedEvent(
                    event_id=event.event_id,
                    event_type=event.event_type,
                    consumer_name="payment-service"
                )
                session.add(processed)

                # Insert Outbox Event transactionally (Phase 4: Transactional Outbox)
                event_type = "PaymentSucceeded" if payment.status == "SUCCEEDED" else "PaymentFailed"
                outcome_payload = PaymentOutcomePayload(
                    amount=str(payment.amount),
                    currency=payment.currency,
                    status=payment.status,
                    provider_payment_id="sim-123"
                )

                outbox_event = OutboxEvent(
                    id=uuid4(),
                    aggregate_type="order",
                    aggregate_id=event.aggregate_id,
                    event_type=event_type,
                    correlation_id=event.correlation_id,
                    causation_id=event.event_id,
                    payload=outcome_payload.model_dump(mode="json"),
                    status="PENDING",
                    created_at=datetime.now(timezone.utc)
                )
                session.add(outbox_event)

                await session.commit()
                logger.info(f"Payment processed and {event_type} outbox event created for order {event.aggregate_id}")
            finally:
                if not is_postgres:
                    self._sqlite_lock.release()

payment_consumer = PaymentConsumer()

