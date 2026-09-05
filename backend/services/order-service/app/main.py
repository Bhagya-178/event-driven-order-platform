from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from app.api.routes import orders, health
from app.core.config import settings
from app.core.redis import redis_manager
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
from shared.exceptions.errors import ApplicationError, NotFoundError, ValidationError
from shared.observability.logging import setup_structured_logging
from shared.observability.metrics import PrometheusMiddleware, metrics_response
from shared.redis.rate_limiter import RateLimiter, RateLimiterMiddleware

import app.models.order
import app.models.order_item
import app.models.outbox
import app.models.processed_event
from app.db.base import Base
from app.db.session import engine

from contextlib import asynccontextmanager
from app.messaging.producer import order_producer
from app.messaging.consumer import order_consumer
from app.messaging.outbox_publisher import order_outbox_publisher

setup_structured_logging("order-service", level=settings.LOG_LEVEL)

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    except Exception as e:
        print(f"Warning: Order DB init: {e}")
    await redis_manager.connect()
    await order_producer.start()
    await order_consumer.start()
    await order_outbox_publisher.start()
    yield
    await order_outbox_publisher.stop()
    await order_consumer.stop()
    await order_producer.stop()
    await redis_manager.close()


app = FastAPI(title="Order Service", version="0.1.0", lifespan=lifespan)

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
app.include_router(orders.router, prefix="/orders", tags=["orders"])

@app.exception_handler(NotFoundError)
async def not_found_exception_handler(request: Request, exc: NotFoundError):
    return JSONResponse(
        status_code=404,
        content={"error": exc.code, "message": exc.message},
    )

@app.exception_handler(ValidationError)
async def validation_exception_handler(request: Request, exc: ValidationError):
    return JSONResponse(
        status_code=422,
        content={"error": exc.code, "message": exc.message},
    )

@app.exception_handler(ApplicationError)
async def application_exception_handler(request: Request, exc: ApplicationError):
    return JSONResponse(
        status_code=500,
        content={"error": exc.code, "message": exc.message},
    )

