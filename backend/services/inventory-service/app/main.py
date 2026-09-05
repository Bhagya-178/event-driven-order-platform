from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
from shared.observability.logging import setup_structured_logging
from shared.observability.metrics import PrometheusMiddleware, metrics_response
from shared.redis.rate_limiter import RateLimiter, RateLimiterMiddleware

import uuid
from sqlalchemy import select
import app.models.inventory
import app.models.outbox
import app.models.processed_event
from app.db.base import Base
from app.db.session import engine, AsyncSessionLocal
from app.models.inventory import Inventory

from app.api.routes import inventory, health
from app.core.config import settings
from app.core.redis import redis_manager
from app.messaging.producer import inventory_producer
from app.messaging.consumer import inventory_consumer
from app.messaging.outbox_publisher import inventory_outbox_publisher

setup_structured_logging("inventory-service", level=settings.LOG_LEVEL)

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        async with AsyncSessionLocal() as session:
            res = await session.execute(select(Inventory))
            if not res.scalars().first():
                catalog_ids = [
                    uuid.UUID("11111111-1111-1111-1111-111111111111"),
                    uuid.UUID("22222222-2222-2222-2222-222222222222"),
                    uuid.UUID("33333333-3333-3333-3333-333333333333"),
                    uuid.UUID("44444444-4444-4444-4444-444444444444"),
                ]
                for cid in catalog_ids:
                    session.add(Inventory(product_id=cid, available_quantity=100, reserved_quantity=0))
                await session.commit()
    except Exception as e:
        print(f"Warning: Inventory DB init / seed: {e}")
    await redis_manager.connect()
    await inventory_producer.start()
    await inventory_consumer.start()
    await inventory_outbox_publisher.start()
    yield
    await inventory_outbox_publisher.stop()
    await inventory_consumer.stop()
    await inventory_producer.stop()
    await redis_manager.close()


app = FastAPI(title="Inventory Service", version="0.1.0", lifespan=lifespan)

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
app.include_router(inventory.router, prefix="/inventory", tags=["inventory"])
