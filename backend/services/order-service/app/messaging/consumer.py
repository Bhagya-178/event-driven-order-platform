import asyncio
import logging
from sqlalchemy import select
from app.db.session import AsyncSessionLocal
from app.models.order import Order
from app.models.processed_event import ProcessedEvent
from app.messaging.producer import order_producer
from shared.events.schemas import EventEnvelope
from shared.events.consumer_handler import ConsumerEventHandler
from app.core.redis import redis_manager

import json
from aiokafka import AIOKafkaConsumer
from app.core.config import settings

logger = logging.getLogger("app.messaging.consumer")

class OrderConsumer:
    """
    Consumes events from payment and inventory topics to drive the Order state machine.
    Uses pessimistic row-locking on PostgreSQL and session locks on SQLite to prevent lost-update races.
    """
    _sqlite_lock = asyncio.Lock()

    def __init__(self):
        self.consumer = None
        self.task = None
        self.handler = ConsumerEventHandler(
            consumer_name="order-service",
            dlq_publish_func=lambda topic, env: order_producer.publish_to_dlq(topic, env),
            dlq_topic="orders.events.dlq",
            max_retries=3,
            initial_delay=0.01,
            backoff_factor=2.0
        )

    async def start(self):
        bootstrap_servers = getattr(settings, "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.consumer = AIOKafkaConsumer(
            "payments.events", "inventory.events",
            bootstrap_servers=bootstrap_servers,
            group_id="order-service",
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        await self.consumer.start()
        self.task = asyncio.create_task(self.consume())
        logger.info("OrderConsumer started")

    async def stop(self):
        if self.task:
            self.task.cancel()
        if self.consumer:
            await self.consumer.stop()
            logger.info("OrderConsumer stopped")

    async def consume(self):
        try:
            async for msg in self.consumer:
                await self.handle_message(msg)
                await self.consumer.commit()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Unexpected error in OrderConsumer loop: {e}", exc_info=True)

    async def handle_message(self, msg) -> bool:
        async def _process_wrapper(m):
            val = m.value if hasattr(m, "value") else m
            if isinstance(val, dict):
                event = EventEnvelope.model_validate(val)
            elif isinstance(val, str):
                import json
                event = EventEnvelope.model_validate(json.loads(val))
            elif isinstance(val, EventEnvelope):
                event = val
            else:
                raise ValueError(f"Unparseable message: {val}")
            await self.process_event(event)

        return await self.handler.handle(msg, _process_wrapper)

    async def process_event(self, event: EventEnvelope):
        async with AsyncSessionLocal() as session:
            is_postgres = session.bind and session.bind.dialect.name == "postgresql"
            if not is_postgres:
                await self._sqlite_lock.acquire()
            try:
                # Rule 4 & 5: Consumers MUST be idempotent
                stmt = select(ProcessedEvent).where(ProcessedEvent.event_id == event.event_id)
                result = await session.execute(stmt)
                if result.scalars().first():
                    logger.info(f"Event {event.event_id} already processed, skipping.")
                    return

                # Add to processed events
                processed = ProcessedEvent(
                    event_id=event.event_id,
                    event_type=event.event_type,
                    consumer_name="order-service"
                )
                session.add(processed)

                # Rule 8 & 11: Cross-topic ordering handled by state machine with row-lock to prevent lost updates
                stmt_order = select(Order).where(Order.id == event.aggregate_id)
                if is_postgres:
                    stmt_order = stmt_order.with_for_update()

                order = (await session.execute(stmt_order)).scalars().first()
                if order:
                    if event.event_type == "PaymentSucceeded":
                        order.payment_status = "SUCCEEDED"
                    elif event.event_type == "PaymentFailed":
                        order.payment_status = "FAILED"
                    elif event.event_type == "InventoryReserved":
                        order.inventory_status = "RESERVED"
                    elif event.event_type == "InventoryFailed":
                        order.inventory_status = "FAILED"
                    elif event.event_type == "InventoryReleased":
                        order.inventory_status = "RELEASED"

                    # Derive final state
                    if order.payment_status == "SUCCEEDED" and order.inventory_status == "RESERVED":
                        order.status = "CONFIRMED"
                    elif order.payment_status == "FAILED" or order.inventory_status == "FAILED":
                        order.status = "FAILED"

                # Rule 3: processed_events insertion and business logic commit together
                await session.commit()

                # Invalidate Redis cache for order
                if order:
                    await redis_manager.delete(f"order:{order.id}")

                logger.info(f"Processed {event.event_type} for order {event.aggregate_id}")
            finally:
                if not is_postgres:
                    self._sqlite_lock.release()

order_consumer = OrderConsumer()
