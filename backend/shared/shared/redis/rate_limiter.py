import logging
from typing import Optional
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from shared.redis.client import RedisManager

logger = logging.getLogger(__name__)

class RateLimiter:
    """
    Sliding window rate limiter using Redis.
    Guarantees fail-open behavior if Redis is unavailable.
    """
    def __init__(self, redis_manager: RedisManager, max_requests: int = 100, window_seconds: int = 60):
        self.redis = redis_manager
        self.max_requests = max_requests
        self.window_seconds = window_seconds

    async def is_rate_limited(self, identifier: str) -> tuple[bool, int, int]:
        """
        Returns (is_limited, current_count, retry_after_seconds)
        """
        if not self.redis.client:
            # Redis not available -> fail open
            return False, 1, 0

        key = f"rate_limit:{identifier}"
        try:
            count = await self.redis.increment(key, ttl=self.window_seconds)
            if count > self.max_requests:
                ttl = await self.redis.client.ttl(key)
                retry_after = max(1, ttl) if ttl > 0 else self.window_seconds
                return True, count, retry_after
            return False, count, 0
        except Exception as e:
            logger.warning(f"Rate limiting check failed (fail-open): {e}")
            return False, 1, 0

class RateLimiterMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, rate_limiter: RateLimiter):
        super().__init__(app)
        self.rate_limiter = rate_limiter

    async def dispatch(self, request: Request, call_next):
        # Exclude metrics and health endpoints from rate limits
        if request.url.path in ["/metrics", "/health", "/health/live", "/health/ready"]:
            return await call_next(request)

        # Identify client by IP or X-Forwarded-For
        client_ip = request.headers.get("X-Forwarded-For") or (request.client.host if request.client else "unknown")
        
        is_limited, count, retry_after = await self.rate_limiter.is_rate_limited(client_ip)
        if is_limited:
            logger.warning(f"Rate limit exceeded for client {client_ip} ({count}/{self.rate_limiter.max_requests})")
            return JSONResponse(
                status_code=429,
                content={"detail": "Too many requests. Please slow down."},
                headers={"Retry-After": str(retry_after)}
            )

        return await call_next(request)
