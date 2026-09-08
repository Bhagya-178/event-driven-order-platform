import json
from aiokafka import AIOKafkaProducer
from shared.events.schemas import EventEnvelope, DeadLetterEnvelope
from app.core.config import settings
import logging

logger = logging.getLogger(__name__)

class PaymentProducer:
    def __init__(self):
        self.producer = None

    async def start(self):
        bootstrap_servers = getattr(settings, "KAFKA_BOOTSTRAP_SERVERS", None) or "localhost:9092"
        self.producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',
            enable_idempotence=True
        )
        import asyncio
        for attempt in range(1, 6):
            try:
                await self.producer.start()
                logger.info("PaymentProducer started")
                return
            except Exception as e:
                logger.warning(f"PaymentProducer start attempt {attempt}/5 failed ({e}), retrying in 2s...")
                await asyncio.sleep(2)
        await self.producer.start()
        logger.info("PaymentProducer started")

    async def stop(self):
        if self.producer:
            await self.producer.stop()
            logger.info("PaymentProducer stopped")
    async def publish_event(self, event: EventEnvelope):
        await self.producer.send_and_wait(
            topic="payments.events",
            key=event.aggregate_id,
            value=event.model_dump(mode="json")
        )
        logger.info(f"Published event {event.event_type} for order {event.aggregate_id}")

    async def publish_to_dlq(self, topic: str, envelope: DeadLetterEnvelope):
        key = envelope.original_key or str(envelope.dlq_id)
        await self.producer.send_and_wait(
            topic=topic,
            key=key,
            value=envelope.model_dump(mode="json")
        )
        logger.warning(f"Published dead letter envelope {envelope.dlq_id} to DLQ {topic}")

payment_producer = PaymentProducer()

