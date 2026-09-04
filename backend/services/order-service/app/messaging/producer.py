import json
from aiokafka import AIOKafkaProducer
from shared.events.schemas import EventEnvelope, DeadLetterEnvelope
from app.core.config import settings
import logging

logger = logging.getLogger(__name__)

class OrderProducer:
    def __init__(self):
        self.producer = None

    async def start(self):
        # We assume Kafka is reachable via KAFKA_BOOTSTRAP_SERVERS, default to localhost:9092
        bootstrap_servers = getattr(settings, "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',  # Rule 9: Producers MUST use acknowledgements (acks=all)
            enable_idempotence=True
        )
        await self.producer.start()
        logger.info("OrderProducer started")

    async def stop(self):
        if self.producer:
            await self.producer.stop()
            logger.info("OrderProducer stopped")

    async def publish_event(self, event: EventEnvelope):
        # Rule 6: Kafka message key MUST use aggregate_id
        await self.producer.send_and_wait(
            topic="orders.events",
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

order_producer = OrderProducer()
