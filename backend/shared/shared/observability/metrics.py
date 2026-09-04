import time
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    generate_latest,
    CONTENT_TYPE_LATEST,
    REGISTRY
)

# Standard Metrics
http_requests_total = Counter(
    "http_requests_total",
    "Total count of HTTP requests",
    ["method", "endpoint", "status_code"]
)

http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "Duration of HTTP requests in seconds",
    ["method", "endpoint"]
)

kafka_events_published_total = Counter(
    "kafka_events_published_total",
    "Total events published to Kafka",
    ["topic", "event_type"]
)

kafka_events_consumed_total = Counter(
    "kafka_events_consumed_total",
    "Total events consumed from Kafka",
    ["topic", "event_type", "status"]
)

outbox_events_pending_gauge = Gauge(
    "outbox_events_pending",
    "Current number of pending outbox events",
    ["service"]
)

dlq_events_total = Counter(
    "dlq_events_routed_total",
    "Total messages routed to dead letter queues",
    ["topic", "error_type"]
)

class PrometheusMiddleware(BaseHTTPMiddleware):
    """
    Middleware that records metrics for every HTTP request passing through FastAPI.
    """
    async def dispatch(self, request: Request, call_next):
        # Exclude metrics endpoint itself from metrics recording
        if request.url.path == "/metrics":
            return await call_next(request)

        start_time = time.perf_counter()
        response = None
        status_code = 500

        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        except Exception:
            status_code = 500
            raise
        finally:
            duration = time.perf_counter() - start_time
            endpoint = request.url.path
            # Group dynamic ID paths (e.g. /orders/1234 -> /orders/{id})
            method = request.method

            http_requests_total.labels(
                method=method,
                endpoint=endpoint,
                status_code=str(status_code)
            ).inc()

            http_request_duration_seconds.labels(
                method=method,
                endpoint=endpoint
            ).observe(duration)

async def metrics_response() -> Response:
    """
    Returns Prometheus formatted metrics.
    """
    return Response(
        content=generate_latest(REGISTRY),
        media_type=CONTENT_TYPE_LATEST
    )
