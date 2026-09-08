import json
import asyncio
import logging
from uuid import uuid4, UUID
from aiokafka import AIOKafkaConsumer
from shared.events.schemas import EventEnvelope, InventoryReservedPayload, InventoryFailedPayload
from shared.events.consumer_handler import process_with_retry_and_dlq
from app.core.config import settings
from app.db.session import AsyncSessionLocal
from datetime import datetime, timezone
from app.models.inventory import Inventory, InventoryReservation
from app.models.processed_event import ProcessedEvent, OrderPaymentState
from app.models.outbox import OutboxEvent
from app.messaging.producer import inventory_producer
from sqlalchemy import select

logger = logging.getLogger(__name__)

class InventoryConsumer:
    _sqlite_lock = asyncio.Lock()

    def __init__(self):
        self.consumer = None
        self.task = None

    async def start(self):
        bootstrap_servers = getattr(settings, "KAFKA_BOOTSTRAP_SERVERS", None) or "localhost:9092"
        self.consumer = AIOKafkaConsumer(
            "orders.events", "payments.events",
            bootstrap_servers=bootstrap_servers,
            group_id="inventory-service",
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        for attempt in range(1, 6):
            try:
                await self.consumer.start()
                self.task = asyncio.create_task(self.consume())
                logger.info("InventoryConsumer started")
                return
            except Exception as e:
                logger.warning(f"InventoryConsumer start attempt {attempt}/5 failed ({e}), retrying in 2s...")
                await asyncio.sleep(2)
        await self.consumer.start()
        self.task = asyncio.create_task(self.consume())
        logger.info("InventoryConsumer started")

    async def stop(self):
        if self.task:
            self.task.cancel()
        if self.consumer:
            await self.consumer.stop()
            logger.info("InventoryConsumer stopped")

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
            await self.process_event(event)

        return await process_with_retry_and_dlq(
            msg=msg,
            process_func=_process,
            dlq_publish_func=lambda topic, env: inventory_producer.publish_to_dlq(topic, env),
            dlq_topic="inventory.events.dlq",
            max_retries=3,
            initial_delay=0.01,
            backoff_factor=2.0
        )

    async def process_event(self, event: EventEnvelope):
        async with AsyncSessionLocal() as session:
            is_postgres = session.bind and session.bind.dialect.name == "postgresql"
            if not is_postgres:
                await self._sqlite_lock.acquire()
            try:
                stmt = select(ProcessedEvent).where(ProcessedEvent.event_id == event.event_id)
                result = await session.execute(stmt)
                if result.scalars().first():
                    return

                if event.event_type == "OrderCreated":
                    # Check payment failure state (Rule 8: Cross topic ordering)
                    state = await session.get(OrderPaymentState, event.aggregate_id)
                    if state and state.payment_failed:
                        logger.info("OrderCreated arrived but PaymentFailed was already processed. Skipping reservation.")
                        return
                    await self._handle_order_created(session, event)
                
                elif event.event_type == "PaymentFailed":
                    state = await session.get(OrderPaymentState, event.aggregate_id)
                    if not state:
                        state = OrderPaymentState(order_id=event.aggregate_id, payment_failed=True)
                        session.add(state)
                    else:
                        state.payment_failed = True
                    
                    # Release existing reservations if any
                    stmt = select(InventoryReservation).where(InventoryReservation.order_id == event.aggregate_id)
                    reservations = (await session.execute(stmt)).scalars().all()
                    for res in reservations:
                        # simplistic release
                        stmt = select(Inventory).where(Inventory.product_id == res.product_id)
                        inv = (await session.execute(stmt)).scalars().first()
                        if inv:
                            inv.available_quantity += res.quantity
                            inv.reserved_quantity -= res.quantity
                        await session.delete(res)

                    if reservations:
                        released_outbox = OutboxEvent(
                            id=uuid4(),
                            event_type="InventoryReleased",
                            aggregate_type="order",
                            aggregate_id=event.aggregate_id,
                            correlation_id=event.correlation_id,
                            causation_id=event.event_id,
                            payload={"reason": "PaymentFailed", "order_id": str(event.aggregate_id)},
                            status="PENDING",
                            created_at=datetime.now(timezone.utc)
                        )
                        session.add(released_outbox)

                # Mark processed
                processed = ProcessedEvent(event_id=event.event_id, event_type=event.event_type, consumer_name="inventory-service")
                session.add(processed)
                await session.commit()
            finally:
                if not is_postgres:
                    self._sqlite_lock.release()

    async def _handle_order_created(self, session, event: EventEnvelope):
        # Reservation logic
        payload = event.payload
        success = True
        for item in payload.get("items", []):
            product_id = UUID(item["product_id"])
            qty = item["quantity"]
            stmt = select(Inventory).where(Inventory.product_id == product_id)
            inv = (await session.execute(stmt)).scalars().first()
            if inv and inv.available_quantity >= qty:
                inv.available_quantity -= qty
                inv.reserved_quantity += qty
                res = InventoryReservation(order_id=event.aggregate_id, product_id=product_id, quantity=qty)
                session.add(res)
            else:
                success = False
                break
        
        event_type = "InventoryReserved" if success else "InventoryFailed"
        outbox_event = OutboxEvent(
            id=uuid4(),
            event_type=event_type,
            aggregate_type="order",
            aggregate_id=event.aggregate_id,
            correlation_id=event.correlation_id,
            causation_id=event.event_id,
            payload={"items": payload.get("items", [])} if success else {"error": "Insufficient stock"},
            status="PENDING",
            created_at=datetime.now(timezone.utc)
        )
        session.add(outbox_event)
        logger.info(f"Created {event_type} outbox event for order {event.aggregate_id}")

inventory_consumer = InventoryConsumer()

