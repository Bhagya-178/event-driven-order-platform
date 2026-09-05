from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
from shared.observability.logging import setup_structured_logging
from shared.observability.metrics import PrometheusMiddleware, metrics_response
from shared.redis.rate_limiter import RateLimiter, RateLimiterMiddleware

import app.models.payment
import app.models.outbox
import app.models.processed_event
from app.db.base import Base
from app.db.session import engine

from app.api.routes import payments, health
from app.core.config import settings
from app.core.redis import redis_manager
from app.messaging.producer import payment_producer
from app.messaging.consumer import payment_consumer
from app.messaging.outbox_publisher import payment_outbox_publisher

setup_structured_logging("payment-service", level=settings.LOG_LEVEL)

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    except Exception as e:
        print(f"Warning: Payment DB init: {e}")
    await redis_manager.connect()
    await payment_producer.start()
    await payment_consumer.start()
    await payment_outbox_publisher.start()
    yield
    await payment_outbox_publisher.stop()
    await payment_consumer.stop()
    await payment_producer.stop()
    await redis_manager.close()


app = FastAPI(title="Payment Service", version="0.1.0", lifespan=lifespan)

# CORS Middleware for Frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Observability & Rate Limiting Middleware
app.add_middleware(PrometheusMiddleware)
if getattr(settings, "ENABLE_RATE_LIMITING", True):
    rate_limiter = RateLimiter(redis_manager, max_requests=100, window_seconds=60)
    app.add_middleware(RateLimiterMiddleware, rate_limiter=rate_limiter)

@app.get("/metrics", include_in_schema=False)
async def prometheus_metrics():
    return await metrics_response()

app.include_router(health.router, prefix="/health", tags=["health"])
app.include_router(payments.router, prefix="/payments", tags=["payments"])
