import asyncio
import logging
import traceback
from typing import Any, Awaitable, Callable, Optional
from shared.events.schemas import DeadLetterEnvelope

logger = logging.getLogger(__name__)

async def process_with_retry_and_dlq(
    msg: Any,
    process_func: Callable[[Any], Awaitable[None]],
    dlq_publish_func: Callable[[str, DeadLetterEnvelope], Awaitable[None]],
    dlq_topic: str,
    max_retries: int = 3,
    initial_delay: float = 0.5,
    backoff_factor: float = 2.0,
) -> bool:
    """
    Executes process_func with exponential backoff retries.
    If all retries fail, routes the message to the DLQ topic with full forensic metadata
    so the partition can be safely committed and unblocked.

    Returns:
        bool: True if processed successfully, False if routed to DLQ.
    """
    last_exception = None
    delay = initial_delay

    for attempt in range(1, max_retries + 1):
        try:
            await process_func(msg)
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_exception = exc
            logger.warning(
                f"Attempt {attempt}/{max_retries} failed for message at {getattr(msg, 'topic', 'unknown')}:"
                f"{getattr(msg, 'partition', 0)}:{getattr(msg, 'offset', 0)}: {exc}"
            )
            if attempt < max_retries:
                await asyncio.sleep(delay)
                delay *= backoff_factor

    # Retries exhausted -> route to DLQ
    logger.error(
        f"Retries exhausted ({max_retries}/{max_retries}). Routing message to DLQ topic '{dlq_topic}'."
    )

    try:
        topic = getattr(msg, "topic", "unknown")
        partition = getattr(msg, "partition", 0)
        offset = getattr(msg, "offset", 0)
        key = getattr(msg, "key", None)
        if isinstance(key, bytes):
            key = key.decode("utf-8", errors="replace")
        elif key is not None:
            key = str(key)

        value = getattr(msg, "value", str(msg))

        dlq_envelope = DeadLetterEnvelope(
            original_topic=topic,
            original_partition=partition,
            original_offset=offset,
            original_key=key,
            error_type=type(last_exception).__name__ if last_exception else "UnknownError",
            error_message=str(last_exception),
            stack_trace=traceback.format_exc(),
            retry_count=max_retries,
            payload=value,
        )

        await dlq_publish_func(dlq_topic, dlq_envelope)
        logger.info(f"Successfully routed message {topic}:{partition}:{offset} to DLQ {dlq_topic}")
        return False
    except Exception as dlq_err:
        logger.critical(f"FATAL: Failed to route message to DLQ {dlq_topic}: {dlq_err}", exc_info=True)
        raise dlq_err

class ConsumerEventHandler:
    """
    Reusable event consumer wrapper supporting exponential backoff retries and DLQ routing.
    """
    def __init__(
        self,
        consumer_name: str,
        dlq_publish_func: Callable[[str, DeadLetterEnvelope], Awaitable[None]],
        dlq_topic: str,
        max_retries: int = 3,
        initial_delay: float = 0.5,
        backoff_factor: float = 2.0,
    ):
        self.consumer_name = consumer_name
        self.dlq_publish_func = dlq_publish_func
        self.dlq_topic = dlq_topic
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.backoff_factor = backoff_factor

    async def handle(self, msg: Any, process_func: Callable[[Any], Awaitable[None]]) -> bool:
        return await process_with_retry_and_dlq(
            msg=msg,
            process_func=process_func,
            dlq_publish_func=self.dlq_publish_func,
            dlq_topic=self.dlq_topic,
            max_retries=self.max_retries,
            initial_delay=self.initial_delay,
            backoff_factor=self.backoff_factor,
        )
